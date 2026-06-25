"""Operator workflow for ingestion DLQ rows.

This module backs the `scripts/manage_ingestion_dlq.py` CLI. It intentionally
operates on `ingestion_failures`, not raw Kafka DLQ messages: by the time an
operator triages a failure, the DLQ writer has already persisted the bounded
failure record into Postgres.
"""
from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timezone
from typing import Any, get_args
from uuid import UUID

import asyncpg
import orjson

from lib.shared.http_headers import redact_log_mapping
from lib.shared.ids import uuid7
from services.ingest.ingestion.kafka.producer import (
    IdempotentProducer,
    ProducerConfig,
)
from services.ingest.ingestion.kafka.topics import topic_for
from services.ingest.ingestion.raw_tier.envelope import (
    IngressKindLiteral,
    RawEnvelope,
)
from services.platform.operator_auth import require_tenant_operator


_HASH_RE = re.compile(r"^[0-9a-f]{40}$")
_MAX_LIMIT = 100
_REASON_CHARS = 500
_INGRESS_KINDS = frozenset(get_args(IngressKindLiteral))


class IngestionDLQOperatorError(ValueError):
    """Operator-facing validation or workflow error."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage Fyralis ingestion_failures replay and quarantine.",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN. Defaults to $DATABASE_URL.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers for replay.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List ingestion DLQ rows.")
    _add_common_args(list_parser)
    list_parser.add_argument("--source", help="Optional source filter.")
    list_parser.add_argument("--limit", type=int, default=50)
    list_parser.add_argument(
        "--include-quarantined",
        action="store_true",
        help="Include quarantined rows.",
    )
    list_parser.add_argument(
        "--include-resolved",
        action="store_true",
        help="Include already resolved rows.",
    )

    replay_parser = subparsers.add_parser(
        "replay",
        help="Republish a raw envelope and mark the failure replayed.",
    )
    _add_common_args(replay_parser)
    _add_failure_arg(replay_parser)
    replay_parser.add_argument(
        "--ingress-kind",
        choices=sorted(_INGRESS_KINDS),
        help=(
            "Ingress kind for the replayed RawEnvelope. Required unless the "
            "failure error_context contains a valid ingress_kind."
        ),
    )
    replay_parser.add_argument("--reason", help="Operator reason for replay.")

    quarantine_parser = subparsers.add_parser(
        "quarantine",
        help="Quarantine an ingestion DLQ row without deleting it.",
    )
    _add_common_args(quarantine_parser)
    _add_failure_arg(quarantine_parser)
    quarantine_parser.add_argument("--reason", required=True)

    return parser


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--tenant", required=True, help="Tenant UUID.")
    parser.add_argument(
        "--operator-actor",
        required=True,
        help="Actor UUID performing the operator action.",
    )


def _add_failure_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--failure-id", required=True, help="ingestion_failures.id")


async def run_command(
    args: argparse.Namespace,
    *,
    conn: asyncpg.Connection,
    producer: Any | None = None,
) -> dict[str, Any]:
    tenant_id = _parse_uuid(args.tenant, field="tenant")
    actor_id = _parse_uuid(args.operator_actor, field="operator_actor")
    assert tenant_id is not None
    assert actor_id is not None

    await _bind_tenant(conn, tenant_id)
    await _ensure_operator_actor(conn, tenant_id=tenant_id, actor_id=actor_id)

    if args.command == "list":
        return await list_failures(
            conn,
            tenant_id=tenant_id,
            actor_id=actor_id,
            source=getattr(args, "source", None),
            limit=getattr(args, "limit", 50),
            include_quarantined=bool(getattr(args, "include_quarantined", False)),
            include_resolved=bool(getattr(args, "include_resolved", False)),
        )

    failure_id = _parse_uuid(args.failure_id, field="failure_id")
    assert failure_id is not None

    if args.command == "quarantine":
        return await quarantine_failure(
            conn,
            tenant_id=tenant_id,
            actor_id=actor_id,
            failure_id=failure_id,
            reason=_bounded_reason(args.reason),
        )

    if args.command == "replay":
        owned_producer = producer is None
        if producer is None:
            producer = IdempotentProducer(
                ProducerConfig(
                    bootstrap_servers=args.bootstrap_servers,
                    client_id=f"ingestion-dlq-replay-{failure_id}",
                )
            )
        await producer.start()
        try:
            return await replay_failure(
                conn,
                tenant_id=tenant_id,
                actor_id=actor_id,
                failure_id=failure_id,
                producer=producer,
                ingress_kind=getattr(args, "ingress_kind", None),
                reason=_bounded_reason(getattr(args, "reason", None)),
            )
        finally:
            if owned_producer:
                await producer.stop()

    raise IngestionDLQOperatorError(f"unknown command {args.command!r}")


async def list_failures(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    source: str | None,
    limit: int,
    include_quarantined: bool,
    include_resolved: bool,
) -> dict[str, Any]:
    limit = max(1, min(_MAX_LIMIT, int(limit or 1)))
    source_clause = "" if not source else "AND source = $3"
    resolved_clause = "" if include_resolved else "AND resolved_at IS NULL"
    quarantine_clause = "" if include_quarantined else "AND quarantined_at IS NULL"
    params: list[Any] = [tenant_id, limit]
    if source:
        params.append(source)
    rows = await conn.fetch(
        f"""
        SELECT id, source, failure_kind, raw_s3_key, error_summary,
               attempt_count, first_seen_at, last_seen_at, resolved_at,
               resolution_kind, resolved_by, quarantined_at, quarantine_reason
        FROM ingestion_failures
        WHERE tenant_id = $1
          {source_clause}
          {resolved_clause}
          {quarantine_clause}
        ORDER BY last_seen_at DESC
        LIMIT $2
        """,
        *params,
    )
    items = [_failure_row_to_item(row) for row in rows]
    await _record_operator_action(
        conn,
        tenant_id=tenant_id,
        actor_id=actor_id,
        action="dead_letter.list",
        resource_type="ingestion_failure_collection",
        resource_id=None,
        metadata={
            "source": source,
            "limit": limit,
            "include_quarantined": include_quarantined,
            "include_resolved": include_resolved,
            "item_count": len(items),
        },
    )
    return {
        "ok": True,
        "action": "list",
        "tenant_id": str(tenant_id),
        "items": items,
    }


async def quarantine_failure(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    failure_id: UUID,
    reason: str,
) -> dict[str, Any]:
    if not reason:
        raise IngestionDLQOperatorError("quarantine reason is required")
    async with conn.transaction():
        row = await conn.fetchrow(
            """
            UPDATE ingestion_failures
            SET quarantined_at = COALESCE(quarantined_at, now()),
                quarantined_by = $3,
                quarantine_reason = $4
            WHERE tenant_id = $1
              AND id = $2
              AND resolved_at IS NULL
            RETURNING id, source, failure_kind, quarantined_at
            """,
            tenant_id,
            failure_id,
            actor_id,
            reason,
        )
        if row is None:
            raise IngestionDLQOperatorError(
                "failure not found or already resolved"
            )
        await _record_operator_action(
            conn,
            tenant_id=tenant_id,
            actor_id=actor_id,
            action="dead_letter.quarantine",
            resource_type="ingestion_failure",
            resource_id=failure_id,
            metadata={
                "source": row["source"],
                "failure_kind": row["failure_kind"],
                "reason": reason,
            },
        )
    return {
        "ok": True,
        "action": "quarantine",
        "status": "quarantined",
        "tenant_id": str(tenant_id),
        "failure_id": str(failure_id),
        "source": row["source"],
        "failure_kind": row["failure_kind"],
        "quarantined_at": _iso(row["quarantined_at"]),
    }


async def replay_failure(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    failure_id: UUID,
    producer: Any,
    ingress_kind: str | None,
    reason: str | None,
) -> dict[str, Any]:
    lock_id = _advisory_lock_id(failure_id)
    await conn.execute("SELECT pg_advisory_lock($1)", lock_id)
    try:
        row = await conn.fetchrow(
            """
            SELECT id, source, failure_kind, raw_s3_key, error_context,
                   resolved_at, quarantined_at
            FROM ingestion_failures
            WHERE tenant_id = $1 AND id = $2
            """,
            tenant_id,
            failure_id,
        )
        if row is None:
            raise IngestionDLQOperatorError("failure not found")
        if row["resolved_at"] is not None:
            raise IngestionDLQOperatorError("failure is already resolved")
        if row["quarantined_at"] is not None:
            raise IngestionDLQOperatorError("failure is quarantined")
        raw_s3_key = row["raw_s3_key"]
        if not raw_s3_key:
            raise IngestionDLQOperatorError(
                "failure has no raw_s3_key and cannot be replayed"
            )
        selected_ingress_kind = _select_ingress_kind(
            supplied=ingress_kind,
            error_context=row["error_context"],
        )
        content_hash = content_hash_from_raw_s3_key(raw_s3_key)
        source = str(row["source"])
        topic = topic_for("raw", source)
        envelope = RawEnvelope(
            source=source,  # type: ignore[arg-type]
            tenant_id=tenant_id,
            raw_s3_key=raw_s3_key,
            content_hash=content_hash,
            ingested_at=datetime.now(tz=timezone.utc),
            ingress_kind=selected_ingress_kind,  # type: ignore[arg-type]
            ingress_metadata={
                "operator_replay_failure_id": str(failure_id),
                "operator_replay": True,
            },
        )
        await producer.produce(
            topic=topic,
            value=orjson.dumps(envelope.model_dump(mode="json")),
            key=str(tenant_id).encode("utf-8"),
        )
        remaining = await producer.flush(timeout_seconds=10.0)
        if remaining:
            raise IngestionDLQOperatorError(
                f"replay publish did not flush {remaining} message(s)"
            )

        async with conn.transaction():
            updated = await conn.fetchrow(
                """
                UPDATE ingestion_failures
                SET resolved_at = now(),
                    resolution_kind = 'replayed',
                    resolved_by = $3
                WHERE tenant_id = $1
                  AND id = $2
                  AND resolved_at IS NULL
                  AND quarantined_at IS NULL
                RETURNING resolved_at
                """,
                tenant_id,
                failure_id,
                f"operator:{actor_id}",
            )
            if updated is None:
                raise IngestionDLQOperatorError(
                    "failure changed state after replay publish"
                )
            await _record_operator_action(
                conn,
                tenant_id=tenant_id,
                actor_id=actor_id,
                action="dead_letter.retry",
                resource_type="ingestion_failure",
                resource_id=failure_id,
                metadata={
                    "source": source,
                    "failure_kind": row["failure_kind"],
                    "ingress_kind": selected_ingress_kind,
                    "replay_topic": topic,
                    "reason": reason,
                },
            )
    finally:
        await conn.execute("SELECT pg_advisory_unlock($1)", lock_id)
    return {
        "ok": True,
        "action": "replay",
        "status": "replayed",
        "tenant_id": str(tenant_id),
        "failure_id": str(failure_id),
        "source": source,
        "failure_kind": row["failure_kind"],
        "ingress_kind": selected_ingress_kind,
        "topic": topic,
        "resolved_at": _iso(updated["resolved_at"]),
    }


def content_hash_from_raw_s3_key(raw_s3_key: str) -> str:
    leaf = raw_s3_key.rsplit("/", 1)[-1]
    if leaf.endswith(".json.zst"):
        content_hash = leaf.removesuffix(".json.zst")
    elif leaf.endswith(".json"):
        content_hash = leaf.removesuffix(".json")
    else:
        raise IngestionDLQOperatorError(
            "raw_s3_key must end with .json or .json.zst"
        )
    if not _HASH_RE.match(content_hash):
        raise IngestionDLQOperatorError(
            "raw_s3_key does not contain a 40-character lowercase content hash"
        )
    prefix = raw_s3_key.split("/")[-2] if "/" in raw_s3_key else ""
    if prefix != content_hash[:2]:
        raise IngestionDLQOperatorError(
            "raw_s3_key hash prefix does not match content hash"
        )
    return content_hash


def _select_ingress_kind(
    *,
    supplied: str | None,
    error_context: Any,
) -> str:
    if supplied:
        if supplied not in _INGRESS_KINDS:
            raise IngestionDLQOperatorError("invalid ingress_kind")
        return supplied
    if isinstance(error_context, dict):
        value = error_context.get("ingress_kind")
        if value in _INGRESS_KINDS:
            return str(value)
    raise IngestionDLQOperatorError(
        "ingress kind is required for replay; pass --ingress-kind"
    )


async def _bind_tenant(conn: asyncpg.Connection, tenant_id: UUID) -> None:
    await conn.execute(
        "SELECT set_config('app.current_tenant', $1, false)",
        str(tenant_id),
    )


async def _ensure_operator_actor(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
) -> None:
    await require_tenant_operator(
        conn,
        tenant_id=tenant_id,
        actor_id=actor_id,
        error_type=IngestionDLQOperatorError,
    )


async def _record_operator_action(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    action: str,
    resource_type: str,
    resource_id: UUID | None,
    metadata: dict[str, Any],
) -> None:
    await conn.execute(
        """
        INSERT INTO operator_action_log (
          id, tenant_id, actor_id, action, resource_type, resource_id, metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        """,
        uuid7(),
        tenant_id,
        actor_id,
        action,
        resource_type,
        resource_id,
        orjson.dumps(metadata).decode("utf-8"),
    )


def _failure_row_to_item(row: asyncpg.Record) -> dict[str, Any]:
    state = "open"
    if row["resolved_at"] is not None:
        state = "resolved"
    elif row["quarantined_at"] is not None:
        state = "quarantined"
    return {
        "id": str(row["id"]),
        "source": row["source"],
        "failure_kind": row["failure_kind"],
        "state": state,
        "attempt_count": row["attempt_count"],
        "raw_s3_key": row["raw_s3_key"],
        "error_preview": _redacted_preview(row["error_summary"]),
        "first_seen_at": _iso(row["first_seen_at"]),
        "last_seen_at": _iso(row["last_seen_at"]),
        "resolved_at": _iso(row["resolved_at"]),
        "resolution_kind": row["resolution_kind"],
        "resolved_by": row["resolved_by"],
        "quarantined_at": _iso(row["quarantined_at"]),
        "quarantine_reason": _redacted_preview(row["quarantine_reason"]),
    }


def _redacted_preview(value: Any) -> str | None:
    if value is None:
        return None
    redacted = redact_log_mapping({"message": str(value)}).get("message")
    return str(redacted)[:_REASON_CHARS]


def _bounded_reason(value: Any) -> str | None:
    if value is None:
        return None
    reason = str(value).strip()
    return reason[:_REASON_CHARS] if reason else None


def _parse_uuid(value: str | None, *, field: str) -> UUID | None:
    if value is None:
        raise IngestionDLQOperatorError(f"{field} is required")
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise IngestionDLQOperatorError(f"{field} must be a UUID") from exc


def _advisory_lock_id(failure_id: UUID) -> int:
    return failure_id.int & ((1 << 63) - 1)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


__all__ = [
    "IngestionDLQOperatorError",
    "build_parser",
    "content_hash_from_raw_s3_key",
    "list_failures",
    "quarantine_failure",
    "replay_failure",
    "run_command",
]
