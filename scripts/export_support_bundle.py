#!/usr/bin/env python3
"""Export a sanitized tenant support bundle.

The bundle is for first-line support and incident triage. It intentionally
contains counts and bounded operational states only; it must not include raw
payloads, object keys, prompts, tokens, webhook signatures, or user content.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lib.shared.ids import uuid7  # noqa: E402
from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.platform.operator_auth import require_tenant_operator  # noqa: E402


class SupportBundleError(ValueError):
    """Operator-facing validation error."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export a sanitized Fyralis tenant support bundle.",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN. Defaults to $DATABASE_URL.",
    )
    parser.add_argument("--tenant", required=True, help="Tenant UUID.")
    parser.add_argument(
        "--operator-actor",
        required=True,
        help="Actor UUID performing this operator action.",
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=24,
        help="Recent-window size for operator action counts.",
    )
    parser.add_argument(
        "--deployment-sha",
        default=os.environ.get("GITHUB_SHA") or os.environ.get("DEPLOYMENT_SHA"),
        help="Optional deployment SHA to include in the bundle.",
    )
    return parser


async def export_support_bundle(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    operator_actor_id: UUID,
    window_hours: int = 24,
    deployment_sha: str | None = None,
) -> dict[str, Any]:
    await require_tenant_operator(
        conn,
        tenant_id=tenant_id,
        actor_id=operator_actor_id,
        error_type=SupportBundleError,
    )
    bundle = await fetch_support_bundle(
        conn,
        tenant_id=tenant_id,
        window_hours=window_hours,
        deployment_sha=deployment_sha,
    )
    await _record_operator_action(
        conn,
        tenant_id=tenant_id,
        actor_id=operator_actor_id,
        metadata={
            "window_hours": window_hours,
            "deployment_sha_present": bool(deployment_sha),
            "schema_version": bundle["schema_version"],
            "privacy_contract": bundle["privacy_contract"],
        },
    )
    return bundle


async def fetch_support_bundle(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    window_hours: int = 24,
    deployment_sha: str | None = None,
) -> dict[str, Any]:
    if window_hours <= 0 or window_hours > 24 * 31:
        raise SupportBundleError("window_hours must be between 1 and 744")

    tenant = await conn.fetchrow(
        "SELECT id, name, created_at FROM tenants WHERE id = $1",
        tenant_id,
    )
    if tenant is None:
        raise SupportBundleError("tenant not found")

    return {
        "schema_version": 1,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "tenant": {
            "id": str(tenant["id"]),
            "name": tenant["name"],
            "created_at": _iso(tenant["created_at"]),
        },
        "deployment": {
            "sha": deployment_sha,
        },
        "window_hours": window_hours,
        "actor_role_counts": await _actor_role_counts(conn, tenant_id),
        "operator_action_counts": await _operator_action_counts(
            conn,
            tenant_id,
            window_hours,
        ),
        "ingestion_failure_counts": await _ingestion_failure_counts(
            conn,
            tenant_id,
        ),
        "dead_letter_counts": await _dead_letter_counts(conn, tenant_id),
        "backup_recovery_status": await _backup_recovery_status(conn),
        "privacy_contract": {
            "contains_raw_payloads": False,
            "contains_object_keys": False,
            "contains_prompts_or_completions": False,
            "contains_tokens_or_secrets": False,
            "contains_operator_metadata_blobs": False,
        },
    }


async def _actor_role_counts(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> list[dict[str, Any]]:
    rows = await _fetch(
        conn,
        """
        SELECT entity_type,
               role,
               COUNT(*) FILTER (WHERE revoked_at IS NULL)::int AS active_count,
               COUNT(*) FILTER (WHERE revoked_at IS NOT NULL)::int AS revoked_count
        FROM actor_roles
        WHERE tenant_id = $1
        GROUP BY entity_type, role
        ORDER BY entity_type, role
        """,
        tenant_id,
    )
    return [_record(row) for row in rows]


async def _operator_action_counts(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    window_hours: int,
) -> list[dict[str, Any]]:
    rows = await _fetch(
        conn,
        """
        SELECT action,
               resource_type,
               COUNT(*)::int AS count,
               MAX(occurred_at) AS last_seen_at
        FROM operator_action_log
        WHERE tenant_id = $1
          AND occurred_at >= now() - make_interval(hours => $2)
        GROUP BY action, resource_type
        ORDER BY action, resource_type
        """,
        tenant_id,
        window_hours,
    )
    return [_record(row) for row in rows]


async def _ingestion_failure_counts(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> list[dict[str, Any]]:
    rows = await _fetch(
        conn,
        """
        SELECT source,
               failure_kind,
               CASE
                 WHEN quarantined_at IS NOT NULL THEN 'quarantined'
                 WHEN resolved_at IS NOT NULL THEN 'resolved'
                 ELSE 'open'
               END AS state,
               COUNT(*)::int AS count,
               COALESCE(MAX(attempt_count), 0)::int AS max_attempt_count,
               MAX(last_seen_at) AS last_seen_at
        FROM ingestion_failures
        WHERE tenant_id = $1
        GROUP BY source, failure_kind, state
        ORDER BY source, failure_kind, state
        """,
        tenant_id,
    )
    return [_record(row) for row in rows]


async def _dead_letter_counts(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> dict[str, list[dict[str, Any]]]:
    post_commit = await _fetch(
        conn,
        """
        SELECT action_kind,
               CASE
                 WHEN quarantined_at IS NOT NULL THEN 'quarantined'
                 WHEN dead_lettered_at IS NOT NULL THEN 'dead_lettered'
                 WHEN processed_at IS NOT NULL THEN 'processed'
                 ELSE 'pending'
               END AS state,
               COUNT(*)::int AS count
        FROM pending_post_commit_actions
        WHERE tenant_id = $1
        GROUP BY action_kind, state
        ORDER BY action_kind, state
        """,
        tenant_id,
    )
    model_reeval = await _fetch(
        conn,
        """
        SELECT cause_kind,
               CASE
                 WHEN quarantined_at IS NOT NULL THEN 'quarantined'
                 WHEN retried_at IS NOT NULL THEN 'retried'
                 ELSE 'dead_lettered'
               END AS state,
               COUNT(*)::int AS count
        FROM model_reeval_dead_letter
        WHERE tenant_id = $1
        GROUP BY cause_kind, state
        ORDER BY cause_kind, state
        """,
        tenant_id,
    )
    think_trigger = await _fetch(
        conn,
        """
        SELECT trigger_kind,
               COALESCE(trigger_subkind, 'unknown') AS trigger_subkind,
               CASE
                 WHEN quarantined_at IS NOT NULL THEN 'quarantined'
                 WHEN completed_at IS NOT NULL AND last_error IS NOT NULL
                   THEN 'dead_lettered'
                 WHEN completed_at IS NOT NULL THEN 'completed'
                 WHEN locked_at IS NOT NULL THEN 'leased'
                 ELSE 'ready'
               END AS state,
               COUNT(*)::int AS count
        FROM think_trigger_queue
        WHERE tenant_id = $1
        GROUP BY trigger_kind, trigger_subkind, state
        ORDER BY trigger_kind, trigger_subkind, state
        """,
        tenant_id,
    )
    return {
        "post_commit": [_record(row) for row in post_commit],
        "model_reeval": [_record(row) for row in model_reeval],
        "think_trigger": [_record(row) for row in think_trigger],
    }


async def _backup_recovery_status(
    conn: asyncpg.Connection,
) -> list[dict[str, Any]]:
    rows = await _fetch(
        conn,
        """
        SELECT component,
               check_name,
               status,
               last_success_at,
               last_attempt_at,
               freshness_slo_seconds,
               updated_at
        FROM backup_recovery_status
        ORDER BY component, check_name
        """,
    )
    return [_record(row) for row in rows]


async def _record_operator_action(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    metadata: dict[str, Any],
) -> None:
    await conn.execute(
        """
        INSERT INTO operator_action_log (
            id, tenant_id, actor_id, action, resource_type, resource_id,
            metadata, occurred_at
        ) VALUES (
            $1, $2, $3, 'support_bundle.export', 'support_bundle', NULL,
            $4::jsonb, now()
        )
        """,
        uuid7(),
        tenant_id,
        actor_id,
        json.dumps(metadata, sort_keys=True),
    )


async def _fetch(
    conn: asyncpg.Connection,
    sql: str,
    *args: object,
) -> list[asyncpg.Record]:
    try:
        return list(await conn.fetch(sql, *args))
    except (
        asyncpg.UndefinedTableError,
        asyncpg.UndefinedColumnError,
    ):
        return []


def _record(row: asyncpg.Record) -> dict[str, Any]:
    return {key: _jsonable(row[key]) for key in row.keys()}


def _jsonable(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


async def _main_async(args: argparse.Namespace) -> int:
    if not args.dsn:
        print(
            json.dumps({"ok": False, "error": "DATABASE_URL is not set"}),
            file=sys.stderr,
        )
        return 2
    try:
        tenant_id = UUID(str(args.tenant))
        operator_actor_id = UUID(str(args.operator_actor))
    except ValueError:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "tenant and operator_actor must be UUIDs",
                }
            ),
            file=sys.stderr,
        )
        return 2

    conn = await asyncpg.connect(dsn=args.dsn)
    try:
        await _register_codecs(conn)
        async with conn.transaction():
            bundle = await export_support_bundle(
                conn,
                tenant_id=tenant_id,
                operator_actor_id=operator_actor_id,
                window_hours=args.window_hours,
                deployment_sha=args.deployment_sha,
            )
    except SupportBundleError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2
    finally:
        await conn.close()
    print(json.dumps(bundle, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
