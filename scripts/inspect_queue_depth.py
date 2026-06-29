#!/usr/bin/env python3
"""Inspect tenant-scoped durable queue depth from an operator shell."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
from typing import Any
from uuid import UUID

import asyncpg

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lib.shared.ids import uuid7  # noqa: E402
from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.platform.operator_auth import require_tenant_operator  # noqa: E402


class QueueDepthCliError(ValueError):
    """Operator-facing validation error."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect Fyralis queue depth for one tenant.",
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
    return parser


def _parse_uuid(value: str | None, *, field: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise QueueDepthCliError(f"{field} must be a UUID") from exc


async def run_command(
    args: argparse.Namespace,
    *,
    conn: asyncpg.Connection,
) -> dict[str, Any]:
    tenant_id = _parse_uuid(args.tenant, field="tenant")
    operator_actor_id = _parse_uuid(args.operator_actor, field="operator_actor")
    await require_tenant_operator(
        conn,
        tenant_id=tenant_id,
        actor_id=operator_actor_id,
        error_type=QueueDepthCliError,
    )

    queues = await inspect_queue_depth(conn, tenant_id=tenant_id)
    await _record_operator_action(
        conn,
        tenant_id=tenant_id,
        actor_id=operator_actor_id,
        metadata={"queues": queues},
    )
    return {
        "ok": True,
        "action": "queue_depth.inspect",
        "tenant_id": str(tenant_id),
        "queues": queues,
    }


async def inspect_queue_depth(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
) -> dict[str, dict[str, int]]:
    row = await conn.fetchrow(
        """
        SELECT
          (SELECT COUNT(*)::bigint
             FROM think_trigger_queue
            WHERE tenant_id = $1 AND completed_at IS NULL)
            AS think_pending,
          (SELECT COUNT(*)::bigint
             FROM think_trigger_queue
            WHERE tenant_id = $1
              AND completed_at IS NULL
              AND locked_by IS NULL
              AND scheduled_for <= now())
            AS think_ready,
          (SELECT COUNT(*)::bigint
             FROM think_trigger_queue
            WHERE tenant_id = $1
              AND completed_at IS NULL
              AND locked_by IS NOT NULL)
            AS think_locked,
          (SELECT COUNT(*)::bigint
             FROM model_reeval_queue
            WHERE tenant_id = $1 AND processed_at IS NULL)
            AS model_reeval_pending,
          (SELECT COUNT(*)::bigint
             FROM pending_post_commit_actions
            WHERE tenant_id = $1
              AND processed_at IS NULL
              AND dead_lettered_at IS NULL)
            AS post_commit_pending,
          (SELECT COUNT(*)::bigint
             FROM pending_post_commit_actions
            WHERE tenant_id = $1 AND dead_lettered_at IS NOT NULL)
            AS post_commit_dead_lettered,
          (SELECT COUNT(*)::bigint
             FROM ingestion_failures
            WHERE tenant_id = $1 AND resolved_at IS NULL)
            AS ingestion_failures_unresolved,
          (SELECT COUNT(*)::bigint
             FROM ingestion_failures
            WHERE tenant_id = $1 AND quarantined_at IS NOT NULL)
            AS ingestion_failures_quarantined,
          (SELECT COUNT(*)::bigint
             FROM source_onboarding_runs
            WHERE tenant_id = $1 AND status = 'pending')
            AS source_onboarding_pending,
          (SELECT COUNT(*)::bigint
             FROM source_onboarding_runs
            WHERE tenant_id = $1 AND status = 'in_progress')
            AS source_onboarding_in_progress,
          (SELECT COUNT(*)::bigint
             FROM source_onboarding_runs
            WHERE tenant_id = $1 AND status = 'failed')
            AS source_onboarding_failed
        """,
        tenant_id,
    )
    assert row is not None
    return {
        "think_trigger_queue": {
            "pending": int(row["think_pending"] or 0),
            "ready": int(row["think_ready"] or 0),
            "locked": int(row["think_locked"] or 0),
        },
        "model_reeval_queue": {
            "pending": int(row["model_reeval_pending"] or 0),
        },
        "pending_post_commit_actions": {
            "pending": int(row["post_commit_pending"] or 0),
            "dead_lettered": int(row["post_commit_dead_lettered"] or 0),
        },
        "ingestion_failures": {
            "unresolved": int(row["ingestion_failures_unresolved"] or 0),
            "quarantined": int(row["ingestion_failures_quarantined"] or 0),
        },
        "source_onboarding_runs": {
            "pending": int(row["source_onboarding_pending"] or 0),
            "in_progress": int(row["source_onboarding_in_progress"] or 0),
            "failed": int(row["source_onboarding_failed"] or 0),
        },
    }


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
            $1, $2, $3, 'queue_depth.inspect', 'queue_depth', NULL, $4::jsonb,
            now()
        )
        """,
        uuid7(),
        tenant_id,
        actor_id,
        json.dumps(metadata, sort_keys=True),
    )


async def _main_async(args: argparse.Namespace) -> int:
    if not args.dsn:
        print(
            json.dumps({"ok": False, "error": "DATABASE_URL is not set"}),
            file=sys.stderr,
        )
        return 2
    conn = await asyncpg.connect(dsn=args.dsn)
    try:
        await _register_codecs(conn)
        async with conn.transaction():
            result = await run_command(args, conn=conn)
    finally:
        await conn.close()
    print(json.dumps(result, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_main_async(args))
    except QueueDepthCliError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
