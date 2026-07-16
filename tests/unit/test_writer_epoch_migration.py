from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest

from lib.shared.migrations import apply_migration


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "db/migrations/0203_writer_scope_epoch_registry.sql"


def test_writer_epoch_migration_has_canonical_tables_and_writer() -> None:
    sql = MIGRATION.read_text()
    for table in (
        "writer_scope_heads",
        "writer_scope_versions",
        "writer_scope_partition_claims",
        "writer_scope_transition_proofs",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
        assert f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY" in sql
        assert f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY" in sql
    assert "'WriterEpochApplier'" in sql
    assert "PRIMARY KEY (tenant_id, semantic_responsibility, source_partition)" in sql


def test_writer_epoch_history_is_guarded_and_cutover_states_are_explicit() -> None:
    sql = MIGRATION.read_text()
    assert "reject_writer_scope_versions_mutation" in sql
    assert "reject_writer_scope_transition_proofs_mutation" in sql
    for state in (
        "legacy",
        "adapter_enforced",
        "backfilling",
        "catch_up",
        "verified",
        "writer_fenced",
        "new_canonical",
        "retired",
    ):
        assert f"'{state}'" in sql


def test_writer_epoch_proof_vocabulary_covers_transfer_and_retirement() -> None:
    sql = MIGRATION.read_text()
    for proof_kind in (
        "partition_coverage",
        "catch_up_complete",
        "semantic_equivalence",
        "authority_equivalence",
        "representability",
        "fence_acknowledged",
        "consumer_drain",
        "repair_residue_closed",
    ):
        assert f"'{proof_kind}'" in sql


@pytest.mark.asyncio
@pytest.mark.integration
async def test_writer_epoch_migration_is_safe_to_reapply(
    fresh_db: asyncpg.Pool,
) -> None:
    sql = MIGRATION.read_text()
    async with fresh_db.acquire() as conn:
        await apply_migration(conn, sql, name=MIGRATION.name)
        await apply_migration(conn, sql, name=MIGRATION.name)
        assert await conn.fetchval(
            "SELECT to_regclass('public.writer_scope_heads') IS NOT NULL"
        )
