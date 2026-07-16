from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_agency_activation_migration_has_fenced_explicit_fates() -> None:
    sql = (
        ROOT
        / "db"
        / "migrations"
        / "0216_authorized_agency_activation_work.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS authorized_agency_activation_work_items" in sql
    assert "authorization_decision_version = 1" in sql
    assert "UNIQUE (tenant_id, source_event_id)" in sql
    assert "UNIQUE (tenant_id, authorization_decision_id)" in sql
    assert "UNIQUE (tenant_id, workflow_run_id)" in sql
    assert "UNIQUE (tenant_id, task_id)" in sql
    for status in (
        "pending",
        "processing",
        "retry_scheduled",
        "activated",
        "authorization_expired",
        "failed_terminal",
    ):
        assert f"'{status}'" in sql
    assert "claim_token UUID" in sql
    assert "lease_expires_at TIMESTAMPTZ" in sql
    assert "status = 'activated'" in sql
    assert "status = 'authorization_expired'" in sql
    assert "authorized_agency_activation_work_due_idx" in sql
    assert (
        "ALTER TABLE authorized_agency_activation_work_items "
        "FORCE ROW LEVEL SECURITY"
    ) in sql
