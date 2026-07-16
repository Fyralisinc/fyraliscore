from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_intervention_manifest_work_migration_has_fenced_terminal_contract() -> None:
    sql = (
        ROOT
        / "db"
        / "migrations"
        / "0215_intervention_episode_manifest_work.sql"
    ).read_text(encoding="utf-8")

    assert "UNIQUE (tenant_id, source_event_id)" in sql
    assert "UNIQUE (tenant_id, episode_id, stage)" in sql
    assert "claim_token UUID" in sql
    assert "lease_expires_at TIMESTAMPTZ" in sql
    assert "status = 'processing'" in sql
    assert "status = 'applied' AND applied_episode_version IS NOT NULL" in sql
    assert "status NOT IN ('retry_scheduled', 'failed_terminal')" in sql
    assert "intervention_episode_manifest_work_due_idx" in sql
    assert (
        "ALTER TABLE intervention_episode_manifest_work_items "
        "FORCE ROW LEVEL SECURITY"
    ) in sql
