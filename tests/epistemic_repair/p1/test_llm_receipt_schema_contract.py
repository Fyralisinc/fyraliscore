from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MIGRATION = ROOT / "db" / "migrations" / "0224_llm_call_attempt_receipts.sql"


def test_receipt_migration_is_tenant_scoped_and_reconcilable() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS llm_logical_call_receipts" in sql
    assert "CREATE TABLE IF NOT EXISTS llm_provider_attempt_receipts" in sql
    assert sql.count("tenant_id UUID NOT NULL REFERENCES tenants(id)") == 2
    assert "PRIMARY KEY (tenant_id, logical_call_id)" in sql
    assert "PRIMARY KEY (tenant_id, physical_attempt_id)" in sql
    assert "UNIQUE (tenant_id, logical_call_id, ordinal)" in sql
    assert "DEFERRABLE INITIALLY DEFERRED" in sql


def test_receipt_migration_separates_actual_estimated_and_unavailable_usage() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")

    assert "usage_exactness IN ('reported', 'estimated', 'unavailable')" in sql
    assert "pricing_version TEXT NOT NULL" in sql
    assert "physical_attempt_count INTEGER NOT NULL" in sql
    assert "outcome = 'cache_hit' AND physical_attempt_count = 0" in sql


def test_receipt_tables_enforce_tenant_isolation() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")

    for table in (
        "llm_logical_call_receipts",
        "llm_provider_attempt_receipts",
    ):
        assert f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY" in sql
        assert f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY" in sql
        assert f"CREATE POLICY tenant_isolation ON {table}" in sql
