from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_consequential_agency_migration_separates_semantic_writers() -> None:
    sql = (ROOT / "db/migrations/0194_consequential_agency_protocol.sql").read_text()
    for table in (
        "agency_command_results",
        "intervention_episode_heads",
        "intervention_episode_versions",
        "consequential_intervention_specs",
        "consequential_proposals",
        "consequential_proposal_reviews",
        "consequential_predictions",
        "consequential_authorization_decisions",
        "consequential_outcomes",
        "consequential_settlements",
        "consequential_attributions",
        "agency_canonical_events",
        "agency_outbox_records",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
    for writer in (
        "ProposalAppender",
        "EpisodeCoordinator",
        "PredictionWriter",
        "AuthorizationApplier",
        "OutcomeRecorder",
        "SettlementApplier",
        "AttributionApplier",
    ):
        assert f"'{writer}'" in sql
    assert "DEFERRABLE INITIALLY DEFERRED" in sql
    assert "CHECK (independent_of_execution_claim)" in sql
    assert "CHECK (to_fate_version = from_fate_version + 1)" in sql
    assert "UNIQUE (tenant_id, prediction_id)" in sql
    assert "reject_consequential_immutable_mutation" in sql
    assert "ALTER TABLE %I FORCE ROW LEVEL SECURITY" in sql


def test_consequential_immutability_is_a_forward_safe_migration() -> None:
    sql = (
        ROOT / "db/migrations/0195_consequential_agency_immutability.sql"
    ).read_text()
    assert "reject_consequential_immutable_mutation" in sql
    assert "BEFORE UPDATE OR DELETE" in sql
    assert "to_regclass('public.' || t) IS NOT NULL" in sql
