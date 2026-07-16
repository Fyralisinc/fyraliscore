from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (
    ROOT
    / "db/migrations/0223_source_identity_binding_overlap_guard.sql"
)


def test_overlap_guard_is_database_enforced_and_idempotent() -> None:
    sql = " ".join(MIGRATION.read_text().split())

    assert "CREATE EXTENSION IF NOT EXISTS btree_gist" in sql
    assert (
        "ADD CONSTRAINT source_identity_bindings_no_valid_time_overlap"
        in sql
    )
    assert "EXCLUDE USING gist" in sql
    assert "tenant_id WITH =" in sql
    assert "source_system WITH =" in sql
    assert "source_native_identifier WITH =" in sql
    assert "tstzrange(valid_from, valid_to, '[)') WITH &&" in sql
    assert "WHERE (transaction_to IS NULL)" in sql
    assert "IF NOT EXISTS" in sql


def test_overlap_guard_preserves_adjacent_half_open_intervals() -> None:
    sql = MIGRATION.read_text()

    assert "tstzrange(valid_from, valid_to, '[)')" in sql
    assert "transaction_to IS NULL" in sql
    assert "SourceIdentityBindingRepo" in sql
