from __future__ import annotations

from pathlib import Path

from scripts.check_schema_drift import EXPECTED_TABLES


ROOT = Path(__file__).resolve().parents[2]


def test_shell_migration_scripts_do_not_record_failed_migrations() -> None:
    for rel in ("scripts/docker-migrate.sh", "scripts/start.sh"):
        text = (ROOT / rel).read_text(encoding="utf-8")
        assert "ON_ERROR_STOP=1" in text
        assert "Recording" not in text
        assert "failed; the schema may already include" not in text


def test_model_metabolism_tables_are_in_schema_drift_contract() -> None:
    residual = EXPECTED_TABLES["model_residual_evidence"]
    latent_gap = EXPECTED_TABLES["sage_latent_gap_hypotheses"]

    assert {
        "id",
        "tenant_id",
        "source_observation_id",
        "residual_kind",
        "compact_summary",
        "reason",
        "status",
        "metadata",
        "resolved_at",
    } <= set(residual.columns)
    assert {
        "model_residual_evidence_open_dedup_idx",
        "model_residual_evidence_observation_idx",
    } <= residual.indexes
    assert residual.columns["metadata"].has_default is True

    assert {
        "id",
        "tenant_id",
        "gap_kind",
        "status",
        "residual_cluster_hash",
        "supporting_residual_ids",
        "supporting_observation_ids",
        "missing_evidence_statement",
        "falsifier",
        "next_evidence_needed",
        "confidence",
        "hypothesis_text",
        "resolved_at",
    } <= set(latent_gap.columns)
    assert {
        "sage_latent_gap_hypotheses_active_dedup_idx",
        "sage_latent_gap_hypotheses_residual_ids_idx",
        "sage_latent_gap_hypotheses_observation_ids_idx",
    } <= latent_gap.indexes
    assert latent_gap.columns["supporting_residual_ids"].data_type == "ARRAY"


def test_model_metabolism_migrations_preserve_lifecycle_and_rls_invariants() -> None:
    residual_sql = (
        ROOT / "db" / "migrations" / "0212_model_residual_evidence.sql"
    ).read_text(encoding="utf-8")
    latent_gap_sql = (
        ROOT / "db" / "migrations" / "0213_sage_latent_gap_hypotheses.sql"
    ).read_text(encoding="utf-8")

    assert "status IN ('open', 'absorbed', 'rejected', 'expired')" in residual_sql
    assert "model_residual_evidence_open_dedup_idx" in residual_sql
    assert "ALTER TABLE model_residual_evidence ENABLE ROW LEVEL SECURITY" in residual_sql
    assert "ALTER TABLE model_residual_evidence FORCE ROW LEVEL SECURITY" in residual_sql

    assert (
        "status IN ('candidate', 'confirmed', 'rejected', 'expired', 'superseded')"
        in latent_gap_sql
    )
    assert "sage_latent_gap_hypotheses_active_dedup_idx" in latent_gap_sql
    assert (
        "ALTER TABLE sage_latent_gap_hypotheses ENABLE ROW LEVEL SECURITY"
        in latent_gap_sql
    )
    assert (
        "ALTER TABLE sage_latent_gap_hypotheses FORCE ROW LEVEL SECURITY"
        in latent_gap_sql
    )
