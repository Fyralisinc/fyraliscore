from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest

from lib.shared.migrations import apply_migration


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "db/migrations/0204_conversation_context_selection_protocol.sql"


def test_context_selection_migration_extends_snapshot_without_second_truth() -> None:
    sql = MIGRATION.read_text()
    assert "ALTER TABLE interpretation_context_snapshots" in sql
    assert "CREATE TABLE IF NOT EXISTS interpretation_context_heads" in sql
    assert "CREATE TABLE IF NOT EXISTS conversation_context_candidate_records" in sql
    assert "CREATE TABLE IF NOT EXISTS conversation_episode" not in sql
    assert "'GroundingAnnotationAppender'" in sql
    assert "conversation-context-selection-v1" in sql


def test_context_selection_history_and_candidates_are_append_only_and_isolated() -> None:
    sql = MIGRATION.read_text()
    assert "interpretation_context_snapshots_immutable" in sql
    assert "conversation_context_candidate_records_immutable" in sql
    for table in (
        "interpretation_context_heads",
        "conversation_context_candidate_records",
    ):
        assert f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY" in sql
        assert f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY" in sql


def test_context_selection_v2_shape_requires_proof_manifests_and_protocol() -> None:
    sql = MIGRATION.read_text()
    for field in (
        "selection_dependency",
        "candidate_manifest_digest",
        "probe_manifest_digest",
        "selection_decision_digest",
        "command_result_id",
    ):
        assert field in sql


@pytest.mark.asyncio
@pytest.mark.integration
async def test_context_selection_migration_is_safe_to_reapply(
    fresh_db: asyncpg.Pool,
) -> None:
    sql = MIGRATION.read_text()
    async with fresh_db.acquire() as conn:
        await apply_migration(conn, sql, name=MIGRATION.name)
        await apply_migration(conn, sql, name=MIGRATION.name)
        assert await conn.fetchval(
            "SELECT to_regclass('public.interpretation_context_heads') IS NOT NULL"
        )
        assert await conn.fetchval(
            "SELECT to_regclass('public.conversation_context_candidate_records') IS NOT NULL"
        )
