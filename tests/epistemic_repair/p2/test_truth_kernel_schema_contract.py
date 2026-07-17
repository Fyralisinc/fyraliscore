from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[3]
MIGRATIONS = ROOT / "db" / "migrations"
MIGRATION = MIGRATIONS / "0225_epistemic_truth_kernel.sql"


def _sql() -> str:
    return MIGRATION.read_text()


def test_migration_number_is_unique_and_follows_live_head() -> None:
    numbered = sorted(
        (int(path.name[:4]), path.name)
        for path in MIGRATIONS.glob("[0-9][0-9][0-9][0-9]_*.sql")
    )
    matching = [name for number, name in numbered if number == 225]

    assert matching == [MIGRATION.name]
    assert numbered[-1] == (225, MIGRATION.name)


def test_schema_defines_every_p2_truth_surface() -> None:
    sql = _sql()
    expected_tables = {
        "truth_candidates",
        "truth_admission_decisions",
        "truth_command_receipts",
        "model_truth_versions",
        "model_truth_lifecycle_events",
        "model_truth_heads",
        "model_truth_evidence_references",
        "model_truth_scope_bindings",
        "model_truth_scope_evidence",
        "relation_truth_admission_decisions",
        "relation_truth_versions",
        "relation_truth_heads",
        "relation_truth_participants",
        "relation_truth_evidence",
        "truth_repair_obligations",
        "model_activity_sidecar",
    }

    for table in expected_tables:
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
    assert "CREATE OR REPLACE VIEW accepted_current_models" in sql
    assert "CREATE OR REPLACE VIEW accepted_current_relations" in sql


def test_admission_is_immutable_and_cannot_target_truth_when_nonaccepted() -> None:
    sql = _sql()

    assert "disposition IN ('accepted', 'rejected', 'needs_review')" in sql
    assert "disposition <> 'accepted' AND admitted_model_id IS NULL" in sql
    assert "truth_admission_decisions_admitted_version_fk" in sql
    assert "FOREIGN KEY (tenant_id, admitted_model_id, admitted_version_id)" in sql
    assert "DEFERRABLE INITIALLY DEFERRED" in sql
    assert "'truth_candidates'" in sql
    assert "'truth_admission_decisions'" in sql


def test_model_version_binds_representation_lifecycle_and_exact_head() -> None:
    sql = _sql()

    for token in ("natural_text TEXT NOT NULL", "proposition JSONB NOT NULL", "semantic_digest TEXT NOT NULL"):
        assert token in sql
    assert "lifecycle IN ('active', 'disputed', 'falsified', 'superseded', 'archived')" in sql
    assert "FOREIGN KEY (tenant_id, model_id, version_id, version, semantic_digest, lifecycle)" in sql
    assert "terminal Model truth head cannot advance" in sql
    assert "NEW.version <= OLD.version" in sql
    version_body = sql.split("CREATE TABLE IF NOT EXISTS model_truth_versions", 1)[1].split(
        "ALTER TABLE truth_admission_decisions", 1
    )[0]
    assert "REFERENCES models(id)" not in version_body


def test_commands_and_lifecycle_events_are_idempotent_immutable_receipts() -> None:
    sql = _sql()

    assert "UNIQUE (tenant_id, idempotency_key)" in sql
    assert "request_digest TEXT NOT NULL" in sql
    assert "truth_command_receipts_model_version_fk" in sql
    assert "truth_command_receipts_relation_version_fk" in sql
    assert "model_truth_lifecycle_events" in sql
    assert "UNIQUE (tenant_id, command_id)" in sql
    assert "from_version_id UUID NOT NULL" in sql
    assert "to_version_id UUID NOT NULL" in sql
    assert "from_version_id <> to_version_id" in sql
    assert "'truth_command_receipts'" in sql
    assert "'model_truth_lifecycle_events'" in sql


def test_evidence_contract_is_typed_coordinate_authority_and_cutoff_bound() -> None:
    sql = _sql()

    assert "evidence_kind IN ('observation', 'model_version', 'registered')" in sql
    assert "evidence_role IN ('support', 'counterevidence', 'context', 'derivation', 'authority')" in sql
    for column in (
        "source_system TEXT NOT NULL",
        "source_object_id TEXT NOT NULL",
        "source_revision TEXT NOT NULL",
        "authority_ref TEXT NOT NULL",
        "policy_version TEXT NOT NULL",
        "authority_epoch INTEGER NOT NULL",
        "occurred_at TIMESTAMPTZ NOT NULL",
        "recorded_at TIMESTAMPTZ NOT NULL",
        "cutoff_at TIMESTAMPTZ NOT NULL",
    ):
        assert column in sql
    assert "occurred_at <= recorded_at AND recorded_at <= cutoff_at" in sql
    assert "authority_expires_at > cutoff_at" in sql


def test_scope_is_version_owned_typed_and_claim_local() -> None:
    sql = _sql()

    assert "model_truth_scope_bindings" in sql
    assert "scope_role TEXT NOT NULL" in sql
    assert "model_truth_scope_evidence" in sql
    assert "REFERENCES model_truth_evidence_references (tenant_id, model_version_id, reference_id)" in sql
    assert "enforce_scope_subject_kind_coherence" in sql
    assert "conflicting entity types" in sql


def test_relations_are_immutable_versioned_role_bearing_and_signed() -> None:
    sql = _sql()

    assert "relation_kind IN (" in sql
    for kind in (
        "causal_influence",
        "dependency_constraint",
        "enablement",
        "predictive_indicator",
    ):
        assert f"'{kind}'" in sql
    assert "role TEXT NOT NULL" in sql
    assert "model_version_id UUID NOT NULL" in sql
    assert "FOREIGN KEY (tenant_id, model_id, model_version_id)" in sql
    assert "polarity SMALLINT NOT NULL CHECK (polarity IN (-1, 1))" in sql
    assert "UNIQUE (tenant_id, relation_version_id, evidence_reference_id)" in sql
    assert "retired relation truth head cannot advance" in sql


def test_repair_obligation_is_version_bound_and_cause_idempotent() -> None:
    sql = _sql()

    assert "invalidated_model_version_id UUID NOT NULL" in sql
    assert re.search(
        r"UNIQUE \(\s*tenant_id, invalidated_model_version_id, affected_kind, affected_id, cause_code\s*\)",
        sql,
    )
    assert "REFERENCES model_truth_versions (tenant_id, version_id)" in sql


def test_accepted_views_require_decision_and_active_exact_head() -> None:
    sql = _sql()
    models_view = sql.split("CREATE OR REPLACE VIEW accepted_current_models AS", 1)[1].split(
        "CREATE OR REPLACE VIEW accepted_current_relations AS", 1
    )[0]
    relations_view = sql.split("CREATE OR REPLACE VIEW accepted_current_relations AS", 1)[1].split(
        "CREATE INDEX", 1
    )[0]

    for view in (models_view, relations_view):
        assert "disposition = 'accepted'" in view
        assert "WHERE h.lifecycle = 'active'" in view
        assert "semantic_digest" in view
    assert "model_truth_evidence_references" in models_view
    assert "relation_truth_participants" in relations_view
    assert "relation_truth_evidence" in relations_view
    assert "model_head.lifecycle = 'active'" in relations_view
    assert "evidence_head.lifecycle = 'active'" in relations_view


def test_activity_is_separate_from_semantic_versions() -> None:
    sql = _sql()
    version_body = sql.split("CREATE TABLE IF NOT EXISTS model_truth_versions", 1)[1].split(
        "ALTER TABLE truth_admission_decisions", 1
    )[0]
    sidecar_body = sql.split("CREATE TABLE IF NOT EXISTS model_activity_sidecar", 1)[1].split(
        "CREATE OR REPLACE VIEW", 1
    )[0]

    assert "retrieval_count" not in version_body
    assert "activation" not in version_body
    assert "last_retrieved_at" not in version_body
    assert "retrieval_count BIGINT" in sidecar_body
    assert "activation DOUBLE PRECISION" in sidecar_body
    assert "last_retrieved_at TIMESTAMPTZ" in sidecar_body


def test_every_tenant_table_has_rls_and_all_history_tables_are_immutable() -> None:
    sql = _sql()
    rls_tables = re.search(
        r"FOREACH tenant_table IN ARRAY ARRAY\[(.*?)\]\s*LOOP",
        sql,
        re.DOTALL,
    )
    immutable_tables = re.search(
        r"FOREACH immutable_table IN ARRAY ARRAY\[(.*?)\]\s*LOOP",
        sql,
        re.DOTALL,
    )

    assert rls_tables is not None
    assert immutable_tables is not None
    assert rls_tables.group(1).count("'") // 2 == 16
    assert immutable_tables.group(1).count("'") // 2 == 12
    assert "ENABLE ROW LEVEL SECURITY" in sql
    assert "FORCE ROW LEVEL SECURITY" in sql
    assert "reject_consequential_immutable_mutation()" in sql
