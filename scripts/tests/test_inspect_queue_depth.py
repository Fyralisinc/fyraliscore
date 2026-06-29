from __future__ import annotations

import argparse
import json

import asyncpg
import pytest

from lib.shared.ids import uuid7
from scripts.inspect_queue_depth import build_parser, run_command
from scripts.tests.conftest import insert_actor
from services.platform.access_control.roles import grant_role


pytestmark = pytest.mark.integration


def _parse(argv: list[str]) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _metadata(row: asyncpg.Record) -> dict[str, object]:
    value = row["metadata"]
    return json.loads(value) if isinstance(value, str) else value


async def _grant_operator_role(
    conn: asyncpg.Connection,
    *,
    tenant,
    actor_id,
) -> None:
    await grant_role(
        actor_id,
        "tenant",
        None,
        "admin",
        actor_id,
        conn=conn,
        tenant_id=tenant,
    )


@pytest.mark.asyncio
async def test_inspect_queue_depth_reports_bounded_counts_and_audits(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Queue operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        ready_trigger = uuid7()
        locked_trigger = uuid7()
        completed_trigger = uuid7()
        await conn.execute(
            """
            INSERT INTO think_trigger_queue
              (id, tenant_id, trigger_kind, trigger_subkind, payload)
            VALUES
              ($1, $4, 'T1', 'event_arrival', '{}'::jsonb),
              ($2, $4, 'T1', 'event_arrival', '{}'::jsonb),
              ($3, $4, 'T1', 'event_arrival', '{}'::jsonb)
            """,
            ready_trigger,
            locked_trigger,
            completed_trigger,
            tenant,
        )
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET locked_by = 'worker-1', locked_at = now()
            WHERE id = $1
            """,
            locked_trigger,
        )
        await conn.execute(
            "UPDATE think_trigger_queue SET completed_at = now() WHERE id = $1",
            completed_trigger,
        )
        await conn.execute(
            """
            INSERT INTO pending_post_commit_actions
              (id, tenant_id, trigger_id, action_kind, action_payload)
            VALUES
              ($1, $3, $4, 'broadcast_realtime', '{}'::jsonb),
              ($2, $3, $4, 'invalidate_metrics', '{}'::jsonb)
            """,
            uuid7(),
            uuid7(),
            tenant,
            ready_trigger,
        )
        await conn.execute(
            """
            UPDATE pending_post_commit_actions
            SET dead_lettered_at = now()
            WHERE action_kind = 'invalidate_metrics' AND tenant_id = $1
            """,
            tenant,
        )
        await conn.execute(
            """
            INSERT INTO ingestion_failures (
              id, tenant_id, source, failure_kind, error_summary
            ) VALUES
              ($1, $3, 'slack', 'normalizer_parse_error', 'bad payload'),
              ($2, $3, 'slack', 'kafka_publish_failure', 'broker down')
            """,
            uuid7(),
            uuid7(),
            tenant,
        )
        await conn.execute(
            """
            UPDATE ingestion_failures
            SET quarantined_at = now(), quarantine_reason = 'operator hold'
            WHERE tenant_id = $1 AND failure_kind = 'kafka_publish_failure'
            """,
            tenant,
        )

        result = await run_command(
            _parse(
                [
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                ]
            ),
            conn=conn,
        )

        assert result["ok"] is True
        assert result["action"] == "queue_depth.inspect"
        assert result["queues"]["think_trigger_queue"] == {
            "pending": 2,
            "ready": 1,
            "locked": 1,
        }
        assert result["queues"]["pending_post_commit_actions"] == {
            "pending": 1,
            "dead_lettered": 1,
        }
        assert result["queues"]["ingestion_failures"] == {
            "unresolved": 2,
            "quarantined": 1,
        }

        audit_row = await conn.fetchrow(
            """
            SELECT actor_id, action, resource_type, resource_id, metadata
            FROM operator_action_log
            WHERE tenant_id = $1 AND action = 'queue_depth.inspect'
            """,
            tenant,
        )
        assert audit_row is not None
        assert audit_row["actor_id"] == operator_actor
        assert audit_row["resource_type"] == "queue_depth"
        assert audit_row["resource_id"] is None
        assert _metadata(audit_row)["queues"] == result["queues"]
