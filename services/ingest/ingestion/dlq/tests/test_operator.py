from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
import orjson
import pytest

from lib.shared.ids import uuid7
from services.ingest.ingestion.dlq.operator import (
    IngestionDLQOperatorError,
    build_parser,
    content_hash_from_raw_s3_key,
    run_command,
)
from services.ingest.ingestion.raw_tier.envelope import RawEnvelope


pytestmark = pytest.mark.integration


class _FakeProducer:
    def __init__(self) -> None:
        self.started = False
        self.published: list[dict[str, Any]] = []
        self.flushes = 0

    async def start(self) -> None:
        self.started = True

    async def produce(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None = None,
        **_kw: Any,
    ) -> None:
        self.published.append({"topic": topic, "value": value, "key": key})

    async def flush(self, timeout_seconds: float = 10.0) -> int:
        self.flushes += 1
        return 0


def _parse(argv: list[str]) -> argparse.Namespace:
    return build_parser().parse_args(argv)


async def _insert_actor(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    name: str = "Operator",
    operator_role: bool = True,
) -> UUID:
    actor_id = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (
          id, tenant_id, type, display_name, status, metadata, created_at
        )
        VALUES ($1, $2, 'human_internal', $3, 'active', '{}'::jsonb, now())
        """,
        actor_id,
        tenant_id,
        name,
    )
    if operator_role:
        await conn.execute(
            """
            INSERT INTO actor_roles (
                tenant_id, actor_id, entity_type, entity_id, role,
                granted_by, granted_at, revoked_at
            )
            VALUES ($1, $2, 'tenant', NULL, 'admin', $2, now(), NULL)
            ON CONFLICT ON CONSTRAINT actor_roles_dedup DO NOTHING
            """,
            tenant_id,
            actor_id,
        )
    return actor_id


async def _insert_tenant(conn: asyncpg.Connection) -> UUID:
    tenant_id = uuid7()
    await conn.execute(
        """
        INSERT INTO tenants (id, name, is_demo)
        VALUES ($1, 'ingestion dlq operator test', FALSE)
        """,
        tenant_id,
    )
    return tenant_id


async def _insert_failure(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    raw_s3_key: str | None,
    error_summary: str = "parser failed",
    error_context: dict[str, Any] | None = None,
) -> UUID:
    failure_id = uuid7()
    await conn.execute(
        """
        INSERT INTO ingestion_failures (
          id, tenant_id, source, failure_kind, raw_s3_key, error_summary,
          error_context, attempt_count, first_seen_at, last_seen_at
        )
        VALUES (
          $1, $2, 'slack', 'normalizer_parse_error', $3, $4,
          $5::jsonb, 2, now(), now()
        )
        """,
        failure_id,
        tenant_id,
        raw_s3_key,
        error_summary,
        orjson.dumps(error_context or {}).decode("utf-8"),
    )
    return failure_id


def _raw_key(tenant_id: UUID, content_hash: str = "a" * 40) -> str:
    month = datetime.now(tz=timezone.utc).strftime("%Y-%m")
    return f"dev/slack/{tenant_id}/{month}/{content_hash[:2]}/{content_hash}.json"


def _metadata(row: asyncpg.Record) -> dict[str, Any]:
    value = row["metadata"]
    if isinstance(value, str):
        return orjson.loads(value)
    return value


def test_content_hash_from_raw_s3_key_validates_shape() -> None:
    assert content_hash_from_raw_s3_key(
        "dev/slack/tenant/2026-06/aa/" + "a" * 40 + ".json.zst"
    ) == "a" * 40
    with pytest.raises(IngestionDLQOperatorError, match="hash prefix"):
        content_hash_from_raw_s3_key(
            "dev/slack/tenant/2026-06/bb/" + "a" * 40 + ".json"
        )


async def test_list_redacts_error_preview_and_audits(
    fresh_db: asyncpg.Pool,
) -> None:
    async with fresh_db.acquire() as conn:
        tenant_id = await _insert_tenant(conn)
        actor_id = await _insert_actor(conn, tenant_id)
        await _insert_failure(
            conn,
            tenant_id=tenant_id,
            raw_s3_key=_raw_key(tenant_id),
            error_summary=(
                "failed for admin@example.com with Authorization: "
                "Bearer sk-test-secret"
            ),
        )

        result = await run_command(
            _parse(
                [
                    "list",
                    "--tenant",
                    str(tenant_id),
                    "--operator-actor",
                    str(actor_id),
                ]
            ),
            conn=conn,
        )

        assert result["ok"] is True
        assert len(result["items"]) == 1
        preview = result["items"][0]["error_preview"]
        assert "admin@example.com" not in preview
        assert "sk-test-secret" not in preview
        assert "[redacted-email]" in preview

        audit_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM operator_action_log
            WHERE tenant_id = $1
              AND actor_id = $2
              AND action = 'dead_letter.list'
              AND resource_type = 'ingestion_failure_collection'
            """,
            tenant_id,
            actor_id,
        )
        assert audit_count == 1


async def test_quarantine_marks_failure_and_audits(
    fresh_db: asyncpg.Pool,
) -> None:
    async with fresh_db.acquire() as conn:
        tenant_id = await _insert_tenant(conn)
        actor_id = await _insert_actor(conn, tenant_id)
        failure_id = await _insert_failure(
            conn,
            tenant_id=tenant_id,
            raw_s3_key=_raw_key(tenant_id),
        )

        result = await run_command(
            _parse(
                [
                    "quarantine",
                    "--tenant",
                    str(tenant_id),
                    "--operator-actor",
                    str(actor_id),
                    "--failure-id",
                    str(failure_id),
                    "--reason",
                    "non-retryable malformed source payload",
                ]
            ),
            conn=conn,
        )

        assert result["status"] == "quarantined"
        row = await conn.fetchrow(
            """
            SELECT resolved_at, quarantined_at, quarantined_by, quarantine_reason
            FROM ingestion_failures
            WHERE id = $1
            """,
            failure_id,
        )
        assert row["resolved_at"] is None
        assert row["quarantined_at"] is not None
        assert row["quarantined_by"] == actor_id
        assert row["quarantine_reason"] == "non-retryable malformed source payload"

        audit = await conn.fetchrow(
            """
            SELECT action, resource_type, resource_id, metadata
            FROM operator_action_log
            WHERE tenant_id = $1 AND actor_id = $2
            """,
            tenant_id,
            actor_id,
        )
        assert audit["action"] == "dead_letter.quarantine"
        assert audit["resource_type"] == "ingestion_failure"
        assert audit["resource_id"] == failure_id
        assert _metadata(audit)["failure_kind"] == "normalizer_parse_error"


async def test_replay_publishes_raw_envelope_marks_resolved_and_audits(
    fresh_db: asyncpg.Pool,
) -> None:
    async with fresh_db.acquire() as conn:
        tenant_id = await _insert_tenant(conn)
        actor_id = await _insert_actor(conn, tenant_id)
        content_hash = "b" * 40
        raw_s3_key = _raw_key(tenant_id, content_hash)
        failure_id = await _insert_failure(
            conn,
            tenant_id=tenant_id,
            raw_s3_key=raw_s3_key,
        )
        producer = _FakeProducer()

        result = await run_command(
            _parse(
                [
                    "replay",
                    "--tenant",
                    str(tenant_id),
                    "--operator-actor",
                    str(actor_id),
                    "--failure-id",
                    str(failure_id),
                    "--ingress-kind",
                    "webhook",
                    "--reason",
                    "normalizer parser fixed",
                ]
            ),
            conn=conn,
            producer=producer,
        )

        assert result["status"] == "replayed"
        assert producer.started is True
        assert producer.flushes == 1
        assert len(producer.published) == 1
        published = producer.published[0]
        assert published["topic"] == "ingestion.raw.slack"
        assert published["key"] == str(tenant_id).encode("utf-8")
        envelope = RawEnvelope.model_validate(orjson.loads(published["value"]))
        assert envelope.tenant_id == tenant_id
        assert envelope.source == "slack"
        assert envelope.raw_s3_key == raw_s3_key
        assert envelope.content_hash == content_hash
        assert envelope.ingress_kind == "webhook"
        assert envelope.ingress_metadata["operator_replay_failure_id"] == str(
            failure_id
        )

        row = await conn.fetchrow(
            """
            SELECT resolved_at, resolution_kind, resolved_by
            FROM ingestion_failures
            WHERE id = $1
            """,
            failure_id,
        )
        assert row["resolved_at"] is not None
        assert row["resolution_kind"] == "replayed"
        assert row["resolved_by"] == f"operator:{actor_id}"

        audit = await conn.fetchrow(
            """
            SELECT action, resource_type, resource_id, metadata
            FROM operator_action_log
            WHERE tenant_id = $1 AND actor_id = $2
            """,
            tenant_id,
            actor_id,
        )
        assert audit["action"] == "dead_letter.retry"
        assert audit["resource_type"] == "ingestion_failure"
        assert audit["resource_id"] == failure_id
        metadata = _metadata(audit)
        assert metadata["replay_topic"] == "ingestion.raw.slack"
        assert metadata["ingress_kind"] == "webhook"


async def test_replay_rejects_missing_raw_key_without_publishing(
    fresh_db: asyncpg.Pool,
) -> None:
    async with fresh_db.acquire() as conn:
        tenant_id = await _insert_tenant(conn)
        actor_id = await _insert_actor(conn, tenant_id)
        failure_id = await _insert_failure(
            conn,
            tenant_id=tenant_id,
            raw_s3_key=None,
        )
        producer = _FakeProducer()

        with pytest.raises(IngestionDLQOperatorError, match="no raw_s3_key"):
            await run_command(
                _parse(
                    [
                        "replay",
                        "--tenant",
                        str(tenant_id),
                        "--operator-actor",
                        str(actor_id),
                        "--failure-id",
                        str(failure_id),
                        "--ingress-kind",
                        "webhook",
                    ]
                ),
                conn=conn,
                producer=producer,
            )

        assert producer.published == []
        resolved_at = await conn.fetchval(
            "SELECT resolved_at FROM ingestion_failures WHERE id = $1",
            failure_id,
        )
        assert resolved_at is None
