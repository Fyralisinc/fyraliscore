from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "db/migrations/0222_canonical_referent_transitions.sql"


def test_referent_transition_migration_is_additive_and_tenant_scoped() -> None:
    sql = MIGRATION.read_text()
    for table in (
        "canonical_referent_transitions",
        "canonical_referent_transition_members",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
        assert f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY" in " ".join(
            sql.split()
        )
        assert f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY" in " ".join(
            sql.split()
        )
        assert f"ON {table};" in sql

    assert "UNIQUE (tenant_id, operation_ref)" in sql
    assert "FOREIGN KEY (tenant_id, transition_id)" in sql
    assert "REFERENCES canonical_referent_transitions (tenant_id, id)" in sql
    assert "current_setting('app.current_tenant', true)" in sql


def test_referent_transition_migration_preserves_physical_ids() -> None:
    sql = MIGRATION.read_text()

    assert "canonical_ref JSONB NOT NULL" in sql
    assert "member_role IN ('predecessor', 'successor')" in sql
    assert "expected_predecessor_version" in sql
    assert "canonical_referent_transition_member_lookup_idx" in sql
    assert "canonical_referent_transition_member_unique_ref_idx" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS" in sql
    assert "ALTER TABLE actors" not in sql
    assert "ALTER TABLE resources" not in sql
    assert "ALTER TABLE models" not in sql


def test_referent_transition_history_is_immutable_and_idempotent() -> None:
    sql = MIGRATION.read_text()

    assert "request_fingerprint ~ '^[0-9a-f]{64}$'" in sql
    for transition_kind in (
        "replacement",
        "merge",
        "split",
        "resurrection",
        "retire",
    ):
        assert f"'{transition_kind}'" in sql
    assert "trigger_name := 'reject_' || table_name || '_mutation'" in sql
    assert "BEFORE UPDATE OR DELETE" in sql
    assert "IF NOT EXISTS" in sql
    assert "DROP POLICY IF EXISTS tenant_isolation" in sql
