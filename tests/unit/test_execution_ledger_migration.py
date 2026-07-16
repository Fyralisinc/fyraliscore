from pathlib import Path


MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "db/migrations/0212_workflow_work_and_external_effect_ledgers.sql"
)
COMPENSATION_MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "db/migrations/0214_external_effect_compensation_linkage.sql"
)


def test_execution_migration_separates_three_semantic_ledgers() -> None:
    sql = MIGRATION.read_text()
    for writer in (
        "AgencyStateApplier",
        "WorkLedgerApplier",
        "ExecutionLedgerApplier",
    ):
        assert f"'{writer}'" in sql
    for table in (
        "agency_workflow_run_heads",
        "agency_workflow_run_versions",
        "agency_task_heads",
        "agency_task_versions",
        "work_obligation_specs",
        "work_obligation_lineage_heads",
        "work_obligation_heads",
        "work_obligation_versions",
        "work_decisions",
        "work_lease_token_heads",
        "work_lease_token_versions",
        "action_adapter_capability_heads",
        "action_adapter_capability_versions",
        "external_effect_provider_keys",
        "external_effect_attempt_lineage_heads",
        "external_effect_attempt_heads",
        "external_effect_attempt_versions",
        "execution_receipts",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql


def test_execution_migration_preserves_fencing_and_unknown_effect_states() -> None:
    sql = MIGRATION.read_text()
    assert "UNIQUE (tenant_id, obligation_id, fence)" in sql
    assert "work_one_active_lease_per_obligation_idx" in sql
    assert "provider_idempotency_key" in sql
    assert "dispatch_intent_recorded" in sql
    assert "reconciliation_required" in sql
    assert "compensation_reconciling" in sql
    assert "external_effect_reconciliation_idx" in sql
    assert "CHECK (requested)" in sql


def test_execution_histories_are_append_only_and_tenant_scoped() -> None:
    sql = MIGRATION.read_text()
    immutable_tables = {
        "action_adapter_capability_versions",
        "agency_workflow_run_versions",
        "agency_task_versions",
        "work_obligation_specs",
        "work_obligation_versions",
        "work_decisions",
        "work_lease_token_versions",
        "external_effect_provider_keys",
        "external_effect_attempt_versions",
        "execution_receipts",
    }
    immutable_block = sql.split("FOREACH t IN ARRAY ARRAY[", 1)[1].split("]", 1)[0]
    for table in immutable_tables:
        assert f"'{table}'" in immutable_block
    assert "ALTER TABLE %I FORCE ROW LEVEL SECURITY" in sql
    assert "CREATE POLICY tenant_isolation" in sql


def test_compensation_migration_binds_spec_authorization_attempt_and_index() -> None:
    sql = COMPENSATION_MIGRATION.read_text()
    for column in (
        "current_compensation_spec_digest",
        "current_compensation_authorization_decision_id",
        "current_compensation_attempt_id",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {column}" in sql
    for constraint in (
        "external_effect_compensation_state_binding_check",
        "external_effect_compensation_spec_fk",
        "external_effect_compensation_authorization_fk",
        "external_effect_compensation_attempt_fk",
    ):
        assert constraint in sql
    assert "DROP INDEX IF EXISTS external_effect_reconciliation_idx" in sql
    assert "'compensation_unknown', 'compensation_reconciling'" in sql
