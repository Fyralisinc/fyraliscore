from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest

from lib.shared.migrations import apply_migration


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "db/migrations/0205_entity_mention_detection_protocol.sql"


def test_mention_detection_migration_has_one_durable_fate_and_head() -> None:
    sql = MIGRATION.read_text()
    for table in (
        "entity_mention_detections",
        "entity_mention_detection_heads",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
        assert f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY" in sql
        assert f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY" in sql
    for fate in (
        "detected",
        "rejected_not_anchored",
        "rejected_not_entity",
        "unsupported_implicit",
    ):
        assert f"'{fate}'" in sql


def test_mention_detection_history_is_immutable_and_linked_downstream() -> None:
    sql = MIGRATION.read_text()
    assert "entity_mention_detections_immutable" in sql
    assert "entity_mention_detection_id" in sql
    assert "entity_candidate_generation_requests" in sql
    assert "grounding_traces" in sql
    assert "GroundingAnnotationAppender" in sql


@pytest.mark.asyncio
@pytest.mark.integration
async def test_mention_detection_migration_is_safe_to_reapply(
    fresh_db: asyncpg.Pool,
) -> None:
    sql = MIGRATION.read_text()
    async with fresh_db.acquire() as conn:
        await apply_migration(conn, sql, name=MIGRATION.name)
        await apply_migration(conn, sql, name=MIGRATION.name)
        assert await conn.fetchval(
            "SELECT to_regclass('public.entity_mention_detections') IS NOT NULL"
        )
        assert await conn.fetchval(
            "SELECT to_regclass('public.entity_mention_detection_heads') IS NOT NULL"
        )
        for table in (
            "entity_mention_detections",
            "entity_mention_detection_heads",
        ):
            rls = await conn.fetchrow(
                """
                SELECT relrowsecurity, relforcerowsecurity
                FROM pg_class
                WHERE oid = $1::regclass
                """,
                table,
            )
            assert rls is not None
            assert rls["relrowsecurity"] is True
            assert rls["relforcerowsecurity"] is True

        assert await conn.fetchval(
            """
            SELECT EXISTS (
              SELECT 1
              FROM pg_trigger
              WHERE tgrelid = 'entity_mention_detections'::regclass
                AND tgname = 'entity_mention_detections_immutable'
                AND NOT tgisinternal
            )
            """
        )
        for table, constraint_name in (
            (
                "entity_candidate_generation_requests",
                "candidate_request_mention_detection_fkey",
            ),
            ("grounding_traces", "grounding_trace_mention_detection_fkey"),
        ):
            assert await conn.fetchval(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM pg_constraint
                  WHERE conrelid = $1::regclass AND conname = $2
                )
                """,
                table,
                constraint_name,
            )
            columns = {
                row["column_name"]
                for row in await conn.fetch(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = $1
                    """,
                    table,
                )
            }
            assert {
                "entity_mention_detection_id",
                "entity_mention_id",
            } <= columns

        writer_constraint = await conn.fetchval(
            """
            SELECT pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conrelid = 'agency_command_results'::regclass
              AND conname = 'agency_command_results_writer_id_check'
            """
        )
        assert writer_constraint is not None
        assert "GroundingAnnotationAppender" in writer_constraint
