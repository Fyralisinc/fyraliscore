from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_attention_governance_concern_migration_has_atomic_protocol() -> None:
    sql = (
        ROOT / "db/migrations/0211_attention_governance_concerns.sql"
    ).read_text()
    for table in (
        "attention_governance_bindings",
        "concern_command_results",
        "concern_heads",
        "concern_versions",
        "concern_transitions",
        "concern_identity_corrections",
        "concern_canonical_events",
        "concern_outbox_records",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
    assert "UNIQUE (tenant_id, semantic_idempotency_key)" in sql
    assert "processing_authority_fingerprint TEXT NOT NULL" in sql
    assert "consumption_authority_fingerprint TEXT NOT NULL" in sql
    assert "effective_binding_envelope JSONB NOT NULL" in sql
    assert "predecessor_concern_id" in sql
    assert "successor_concern_id" in sql
    assert "CHECK (to_version = from_version + 1)" in sql
    assert "ALTER TABLE %I FORCE ROW LEVEL SECURITY" in sql
