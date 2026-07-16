from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_grounding_migration_preserves_semantic_separation_and_total_fate() -> None:
    sql = (ROOT / "db/migrations/0188_entity_grounding_trace.sql").read_text()

    for table in (
        "interpretation_context_snapshots",
        "entity_candidate_generation_requests",
        "entity_candidate_sets",
        "resolution_assessments",
        "grounding_admission_decisions",
        "grounding_traces",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
        assert "ALTER TABLE %I FORCE ROW LEVEL SECURITY" in sql
    assert "identity_registry_mutated BOOLEAN NOT NULL DEFAULT FALSE" in sql
    assert "source_observation_mutated BOOLEAN NOT NULL DEFAULT FALSE" in sql
    assert "'resolved_for_consumer', 'review', 'unresolved', 'abstained'" in sql
    assert "UNIQUE (tenant_id, request_digest)" in sql


def test_grounding_work_lifecycle_persists_retry_and_terminal_fates() -> None:
    sql = (
        ROOT / "db/migrations/0189_entity_grounding_work_lifecycle.sql"
    ).read_text()

    assert "CREATE TABLE IF NOT EXISTS entity_grounding_work_items" in sql
    assert "'pending', 'retry_scheduled', 'resolved_for_consumer', 'review'" in sql
    assert "current_trace_id UUID REFERENCES grounding_traces(id)" in sql
    assert "useful_safe_fate JSONB NOT NULL" in sql
    assert "UNIQUE (tenant_id, source_observation_id, phrase, processing_generation)" in sql
    assert "ALTER TABLE entity_grounding_work_items FORCE ROW LEVEL SECURITY" in sql
