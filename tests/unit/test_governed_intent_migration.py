from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_governed_intent_migration_has_exact_acceptance_and_atomic_protocol() -> None:
    sql = (ROOT / "db/migrations/0208_governed_intent_protocol.sql").read_text()
    for table in (
        "intent_proposals",
        "intent_proposal_fate_events",
        "intent_exact_acceptances",
        "intent_aggregate_heads",
        "intent_command_results",
        "intent_versions",
        "intent_canonical_events",
        "intent_outbox_records",
        "intent_basis_change_events",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
    assert "UNIQUE (tenant_id, semantic_idempotency_key)" in sql
    assert "proposal_digest TEXT NOT NULL" in sql
    assert "normalized_payload_digest TEXT NOT NULL" in sql
    assert "proposal_acceptance_id UUID" in sql
    assert "authority_basis_snapshot JSONB NOT NULL" in sql
    assert "grounding_dependencies JSONB NOT NULL" in sql
    assert "intent_canonical_events" in sql
    assert "intent_outbox_records" in sql
    assert "ALTER TABLE %I FORCE ROW LEVEL SECURITY" in sql


def test_intent_legacy_cutover_never_invents_historical_authority() -> None:
    sql = (
        ROOT / "db/migrations/0209_intent_legacy_cutover_baselines.sql"
    ).read_text()
    assert "CREATE TABLE IF NOT EXISTS intent_legacy_baselines" in sql
    assert "legacy_unknown_review_required" in sql
    assert "baseline_payload_digest" in sql
    assert "source_snapshot JSONB NOT NULL" in sql
    assert "FORCE ROW LEVEL SECURITY" in sql


def test_intent_command_reconstructability_is_explicit_for_legacy_rows() -> None:
    sql = (
        ROOT / "db/migrations/0210_intent_command_reconstructability.sql"
    ).read_text()
    assert "ADD COLUMN IF NOT EXISTS command JSONB" in sql
    assert "processing_authority_fingerprint" in sql
    assert "consumption_authority_fingerprint" in sql
    assert "authority_capture_status = 'complete'" in sql
    assert "authority_capture_status = 'legacy_missing'" in sql
