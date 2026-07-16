from pathlib import Path


MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "db/migrations/0213_failure_and_owner_terminalization_protocol.sql"
)


def test_failure_protocol_has_lineage_history_and_exact_owner_handshake() -> None:
    sql = MIGRATION.read_text()
    for table in (
        "failure_record_specs",
        "failure_record_lineage_heads",
        "failure_record_heads",
        "failure_record_versions",
        "owner_terminalization_requests",
        "owner_terminalization_resolutions",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
    assert "owner_terminalization_pending" in sql
    assert "owner_command_result_id" in sql
    assert "UNIQUE (tenant_id, owner_command_result_id)" in sql
    assert "current_failure_id" in sql


def test_failure_history_and_handshake_records_are_append_only_and_scoped() -> None:
    sql = MIGRATION.read_text()
    immutable_block = sql.split("FOREACH t IN ARRAY ARRAY[", 1)[1].split("]", 1)[0]
    for table in (
        "failure_record_specs",
        "failure_record_versions",
        "owner_terminalization_requests",
        "owner_terminalization_resolutions",
    ):
        assert f"'{table}'" in immutable_block
    assert "ALTER TABLE %I FORCE ROW LEVEL SECURITY" in sql
    assert "CREATE POLICY tenant_isolation" in sql
