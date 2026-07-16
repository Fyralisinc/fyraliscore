from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_work_scheduling_migration_has_fenced_explicit_fates() -> None:
    sql = (
        ROOT / "db/migrations/0217_registered_work_scheduling.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS registered_work_scheduling_items" in sql
    assert "source_obligation_version = 1" in sql
    assert "UNIQUE (tenant_id, source_event_id)" in sql
    assert "UNIQUE (tenant_id, obligation_id)" in sql
    assert "UNIQUE (tenant_id, decision_id)" in sql
    assert "UNIQUE (tenant_id, lease_token_id)" in sql
    for status in (
        "pending",
        "processing",
        "retry_scheduled",
        "leased",
        "work_expired",
        "authorization_expired",
        "failed_terminal",
    ):
        assert f"'{status}'" in sql
    assert "claim_token UUID" in sql
    assert "lease_expires_at TIMESTAMPTZ" in sql
    assert "status = 'leased'" in sql
    assert "status IN ('work_expired', 'authorization_expired')" in sql
    assert "registered_work_scheduling_due_idx" in sql
    assert (
        "ALTER TABLE registered_work_scheduling_items FORCE ROW LEVEL SECURITY"
        in sql
    )
