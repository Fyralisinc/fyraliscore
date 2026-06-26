from __future__ import annotations

from pathlib import Path

from scripts.check_architecture_ratchets import (
    find_access_read_without_override_audit_violations,
    find_browser_token_storage_violations,
    find_byoc_agent_contract_privacy_violations,
    find_byoc_manifest_privacy_violations,
    find_destructive_migration_without_approval_violations,
    find_forbidden_metric_label_violations,
    find_import_linter_allowlist_violations,
    find_migration_filename_violations,
    find_network_call_in_transaction_violations,
    find_new_permissive_rls_policy_violations,
    find_plaintext_secret_column_migration_violations,
    find_product_default_tenant_without_production_guard_violations,
    find_raw_secret_ref_argument_violations,
    find_rollback_data_deletion_violations,
    find_raw_model_reeval_insert_violations,
    find_raw_pending_post_commit_action_insert_violations,
    find_raw_think_trigger_insert_violations,
    find_raw_think_obligation_insert_violations,
)


def test_migration_filename_check_flags_duplicate_prefixes(tmp_path: Path) -> None:
    migrations = tmp_path / "db" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0001_foundation.sql").write_text("SELECT 1;\n", encoding="utf-8")
    (migrations / "0001_duplicate.sql").write_text("SELECT 1;\n", encoding="utf-8")

    violations = find_migration_filename_violations(repo_root=tmp_path)

    assert len(violations) == 2
    assert {v.path for v in violations} == {
        Path("db/migrations/0001_foundation.sql"),
        Path("db/migrations/0001_duplicate.sql"),
    }
    assert {v.check for v in violations} == {"migration-filename-ratchet"}


def test_migration_filename_check_flags_malformed_names(tmp_path: Path) -> None:
    migrations = tmp_path / "db" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "next_bad.sql").write_text("SELECT 1;\n", encoding="utf-8")

    violations = find_migration_filename_violations(repo_root=tmp_path)

    assert len(violations) == 1
    assert violations[0].path == Path("db/migrations/next_bad.sql")
    assert "four-digit prefix" in violations[0].message


def test_migration_filename_check_allows_unique_prefixes(tmp_path: Path) -> None:
    migrations = tmp_path / "db" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0001_foundation.sql").write_text("SELECT 1;\n", encoding="utf-8")
    (migrations / "0002_add_models.sql").write_text("SELECT 1;\n", encoding="utf-8")

    violations = find_migration_filename_violations(repo_root=tmp_path)

    assert violations == []


def test_destructive_migration_check_flags_new_drop_without_approval(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "db" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0001_drop_customer_data.sql").write_text(
        "DROP TABLE customer_payloads;\n",
        encoding="utf-8",
    )

    violations = find_destructive_migration_without_approval_violations(
        repo_root=tmp_path
    )

    assert len(violations) == 1
    assert violations[0].path == Path("db/migrations/0001_drop_customer_data.sql")
    assert violations[0].check == "destructive-migration-approval"
    assert "backup verification" in violations[0].message


def test_destructive_migration_check_requires_complete_marker(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "db" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0001_drop_customer_data.sql").write_text(
        """
-- destructive-migration-approved: backup=snapshot-123 owner=platform
ALTER TABLE observations DROP COLUMN raw_text;
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_destructive_migration_without_approval_violations(
        repo_root=tmp_path
    )

    assert len(violations) == 1
    assert violations[0].line_number == 1
    assert "rollback=" in violations[0].message


def test_destructive_migration_check_allows_complete_marker(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "db" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0001_retire_shadow_index.sql").write_text(
        (
            "-- destructive-migration-approved: backup=snapshot-123 "
            "rollback=release-runbook owner=platform\n"
            "DROP INDEX IF EXISTS observations_shadow_idx;\n"
        ),
        encoding="utf-8",
    )

    violations = find_destructive_migration_without_approval_violations(
        repo_root=tmp_path
    )

    assert violations == []


def test_destructive_migration_check_ignores_commented_rollback_sql(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "db" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0001_add_table.sql").write_text(
        """
-- rollback:
-- DROP TABLE IF EXISTS new_table;
CREATE TABLE new_table (id uuid PRIMARY KEY);
/* DROP INDEX IF EXISTS old_idx; */
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_destructive_migration_without_approval_violations(
        repo_root=tmp_path
    )

    assert violations == []


def test_new_permissive_rls_policy_check_flags_post_baseline_migration(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "db" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0166_bad_rls_policy.sql").write_text(
        """
CREATE POLICY tenant_isolation ON customer_rows
USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
);
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_new_permissive_rls_policy_violations(repo_root=tmp_path)

    assert len(violations) == 1
    assert violations[0].path == Path("db/migrations/0166_bad_rls_policy.sql")
    assert violations[0].check == "new-permissive-rls-policy"


def test_new_permissive_rls_policy_check_allows_historical_baseline(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "db" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0164_legacy_policy_baseline.sql").write_text(
        """
CREATE POLICY tenant_isolation ON customer_rows
USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_new_permissive_rls_policy_violations(repo_root=tmp_path)

    assert violations == []


def test_new_permissive_rls_policy_check_ignores_comments(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "db" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0166_strict_policy.sql").write_text(
        """
-- rollback used to include current_setting('app.current_tenant', true) IS NULL
CREATE POLICY tenant_isolation ON customer_rows
USING (tenant_id = current_setting('app.current_tenant', true)::uuid)
WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::uuid);
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_new_permissive_rls_policy_violations(repo_root=tmp_path)

    assert violations == []


def test_plaintext_secret_column_check_flags_post_baseline_migration(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "db" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0167_bad_provider_secret.sql").write_text(
        """
ALTER TABLE provider_installations
  ADD COLUMN access_token TEXT;
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_plaintext_secret_column_migration_violations(
        repo_root=tmp_path,
    )

    assert len(violations) == 1
    assert violations[0].path == Path("db/migrations/0167_bad_provider_secret.sql")
    assert violations[0].check == "plaintext-secret-column-migration"


def test_plaintext_secret_column_check_allows_ref_hash_and_metadata(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "db" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0167_safe_provider_secret_refs.sql").write_text(
        """
ALTER TABLE provider_installations
  ADD COLUMN access_token_ref TEXT,
  ADD COLUMN webhook_secret_hash TEXT,
  ADD COLUMN token_type TEXT;
CREATE TABLE provider_token_metadata (
  id UUID PRIMARY KEY,
  token_status TEXT
);
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_plaintext_secret_column_migration_violations(
        repo_root=tmp_path,
    )

    assert violations == []


def test_plaintext_secret_column_check_allows_baseline_and_comments(
    tmp_path: Path,
) -> None:
    migrations = tmp_path / "db" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "0166_whatsapp_secret_refs.sql").write_text(
        "ALTER TABLE whatsapp_installations ADD COLUMN app_secret TEXT;\n",
        encoding="utf-8",
    )
    (migrations / "0167_comment_only.sql").write_text(
        """
-- ALTER TABLE provider_installations ADD COLUMN refresh_token TEXT;
ALTER TABLE provider_installations ADD COLUMN refresh_token_ref TEXT;
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_plaintext_secret_column_migration_violations(
        repo_root=tmp_path,
    )

    assert violations == []


def test_raw_secret_ref_argument_check_flags_raw_credentials(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "ingest" / "integrations" / "bad"
    source.mkdir(parents=True)
    (source / "oauth.py").write_text(
        """
async def finalize(api_token, webhook_secret):
    await finalize_install(
        pool,
        tenant_id=tenant_id,
        secret_ref=api_token,
        webhook_secret_ref=webhook_secret,
    )
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_raw_secret_ref_argument_violations(repo_root=tmp_path)

    assert len(violations) == 2
    assert {v.check for v in violations} == {"raw-secret-ref-argument"}
    assert {v.line_number for v in violations} == {5, 6}


def test_raw_secret_ref_argument_check_allows_ref_values(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "ingest" / "integrations" / "safe"
    source.mkdir(parents=True)
    (source / "oauth.py").write_text(
        """
async def finalize(store, api_token, webhook_secret):
    secret_ref = await store.put(api_token, label="api", tenant_id=tenant_id)
    webhook_secret_ref = await store.put(
        webhook_secret, label="webhook", tenant_id=tenant_id
    )
    await finalize_install(
        pool,
        tenant_id=tenant_id,
        secret_ref=secret_ref,
        webhook_secret_ref=webhook_secret_ref,
    )
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_raw_secret_ref_argument_violations(repo_root=tmp_path)

    assert violations == []


def test_raw_think_trigger_insert_check_flags_production_code(tmp_path: Path) -> None:
    source = tmp_path / "services" / "reasoning"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        'SQL = """\nINSERT INTO think_trigger_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_think_trigger_insert_violations(repo_root=tmp_path)

    assert [v.path for v in violations] == [Path("services/reasoning/bad.py")]


def test_raw_think_trigger_insert_check_allows_helper_and_tests(tmp_path: Path) -> None:
    helper = tmp_path / "services" / "domain"
    helper.mkdir(parents=True)
    (helper / "triggers.py").write_text(
        'SQL = """\nINSERT INTO think_trigger_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )
    tests = tmp_path / "services" / "reasoning" / "think" / "tests"
    tests.mkdir(parents=True)
    (tests / "test_worker.py").write_text(
        'SQL = """\nINSERT INTO think_trigger_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_think_trigger_insert_violations(repo_root=tmp_path)

    assert violations == []


def test_raw_model_reeval_insert_check_flags_production_code(tmp_path: Path) -> None:
    source = tmp_path / "services" / "reasoning"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        'SQL = """\nINSERT INTO model_reeval_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_model_reeval_insert_violations(repo_root=tmp_path)

    assert [v.path for v in violations] == [Path("services/reasoning/bad.py")]


def test_raw_model_reeval_insert_check_allows_owners_and_tests(
    tmp_path: Path,
) -> None:
    helper = tmp_path / "services" / "domain"
    helper.mkdir(parents=True)
    (helper / "triggers.py").write_text(
        'SQL = """\nINSERT INTO model_reeval_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )
    registry = tmp_path / "lib" / "shared"
    registry.mkdir(parents=True)
    (registry / "edge_registry.py").write_text(
        'SQL = """\nINSERT INTO model_reeval_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )
    tests = tmp_path / "services" / "reasoning" / "think" / "tests"
    tests.mkdir(parents=True)
    (tests / "test_worker.py").write_text(
        'SQL = """\nINSERT INTO model_reeval_queue (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_model_reeval_insert_violations(repo_root=tmp_path)

    assert violations == []


def test_raw_pending_post_commit_action_insert_check_flags_production_code(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "product"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        'SQL = """\nINSERT INTO pending_post_commit_actions (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_pending_post_commit_action_insert_violations(
        repo_root=tmp_path
    )

    assert [v.path for v in violations] == [Path("services/product/bad.py")]


def test_raw_pending_post_commit_action_insert_check_allows_owner_and_tests(
    tmp_path: Path,
) -> None:
    owner = tmp_path / "services" / "reasoning" / "think"
    owner.mkdir(parents=True)
    (owner / "post_commit.py").write_text(
        'SQL = """\nINSERT INTO pending_post_commit_actions (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )
    tests = tmp_path / "services" / "reasoning" / "think" / "tests"
    tests.mkdir(parents=True)
    (tests / "test_post_commit.py").write_text(
        'SQL = """\nINSERT INTO pending_post_commit_actions (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_pending_post_commit_action_insert_violations(
        repo_root=tmp_path
    )

    assert violations == []


def test_raw_think_obligation_insert_check_flags_production_code(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "reasoning"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        'SQL = """\nINSERT INTO think_obligations (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_think_obligation_insert_violations(repo_root=tmp_path)

    assert [v.path for v in violations] == [Path("services/reasoning/bad.py")]


def test_raw_think_obligation_insert_check_allows_owner_and_tests(
    tmp_path: Path,
) -> None:
    owner = tmp_path / "services" / "domain"
    owner.mkdir(parents=True)
    (owner / "obligations.py").write_text(
        'SQL = """\nINSERT INTO think_obligations (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )
    tests = tmp_path / "services" / "domain" / "tests"
    tests.mkdir(parents=True)
    (tests / "test_obligations.py").write_text(
        'SQL = """\nINSERT INTO think_obligations (id) VALUES ($1)\n"""\n',
        encoding="utf-8",
    )

    violations = find_raw_think_obligation_insert_violations(repo_root=tmp_path)

    assert violations == []


def test_network_call_in_transaction_check_flags_embed_inside_tx(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "reasoning"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        """
async def run(conn, embedder):
    async with conn.transaction():
        await embedder.embed("customer text")
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_network_call_in_transaction_violations(repo_root=tmp_path)

    assert len(violations) == 1
    assert violations[0].path == Path("services/reasoning/bad.py")


def test_access_read_audit_check_flags_unaudited_reader(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "product"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        """
from services.platform.access_control.checks import can_read_by_id

async def run(conn, actor_id, tenant_id, model_id):
    return await can_read_by_id(
        actor_id, "model", model_id, conn=conn, tenant_id=tenant_id
    )
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_access_read_without_override_audit_violations(
        repo_root=tmp_path
    )

    assert len(violations) == 1
    assert violations[0].path == Path("services/product/bad.py")


def test_access_read_audit_check_allows_audited_reader_and_tests(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "product"
    source.mkdir(parents=True)
    (source / "good.py").write_text(
        """
from services.platform.access_control.audit import record_override_if_needed
from services.platform.access_control.checks import can_read

async def run(conn, actor_id, tenant_id, entity):
    decision = await can_read(actor_id, entity, conn=conn, tenant_id=tenant_id)
    await record_override_if_needed(
        decision,
        actor_id=actor_id,
        entity_type=entity["kind"],
        entity_id=entity["id"],
        conn=conn,
        tenant_id=tenant_id,
    )
    return decision.allowed
""".lstrip(),
        encoding="utf-8",
    )
    tests = tmp_path / "services" / "product" / "tests"
    tests.mkdir(parents=True)
    (tests / "test_access.py").write_text(
        """
from services.platform.access_control.checks import can_read

async def test_reader(conn):
    await can_read("actor", {"kind": "model"}, conn=conn, tenant_id="tenant")
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_access_read_without_override_audit_violations(
        repo_root=tmp_path
    )

    assert violations == []


def test_forbidden_metric_label_check_flags_tenant_label(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "app"
    source.mkdir(parents=True)
    (source / "metrics.py").write_text(
        """
from lib.observability import counter

REQUESTS = counter("bad_total", "Bad.", ("tenant_id", "status"))
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_forbidden_metric_label_violations(repo_root=tmp_path)

    assert len(violations) == 1
    assert violations[0].path == Path("services/app/metrics.py")
    assert violations[0].check == "forbidden-metric-label"


def test_forbidden_metric_label_check_flags_source_channel_label(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "app"
    source.mkdir(parents=True)
    (source / "metrics.py").write_text(
        """
from lib.observability import counter

REQUESTS = counter("bad_total", "Bad.", ("source_channel", "status"))
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_forbidden_metric_label_violations(repo_root=tmp_path)

    assert len(violations) == 1
    assert violations[0].path == Path("services/app/metrics.py")
    assert violations[0].check == "forbidden-metric-label"


def test_forbidden_metric_label_check_allows_bounded_labels(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "app"
    source.mkdir(parents=True)
    (source / "metrics.py").write_text(
        """
from lib.observability import histogram

LATENCY = histogram(
    "good_seconds",
    "Good.",
    label_names=("method", "route", "status"),
)
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_forbidden_metric_label_violations(repo_root=tmp_path)

    assert violations == []


def test_browser_token_storage_check_flags_local_storage(tmp_path: Path) -> None:
    source = tmp_path / "ui"
    source.mkdir(parents=True)
    (source / "client.ts").write_text(
        """
export function saveToken(token: string) {
  window.localStorage.setItem("fyralis_token", token)
}
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_browser_token_storage_violations(repo_root=tmp_path)

    assert len(violations) == 1
    assert violations[0].path == Path("ui/client.ts")


def test_browser_token_storage_check_flags_query_token_url(tmp_path: Path) -> None:
    source = tmp_path / "ui"
    source.mkdir(parents=True)
    (source / "stream.ts").write_text(
        """
export function openStream(token: string) {
  return new WebSocket(`/stream?token=${encodeURIComponent(token)}`)
}
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_browser_token_storage_violations(repo_root=tmp_path)

    assert len(violations) == 1
    assert violations[0].check == "browser-token-storage"


def test_browser_token_storage_check_allows_cookie_stream(tmp_path: Path) -> None:
    source = tmp_path / "ui"
    source.mkdir(parents=True)
    (source / "stream.ts").write_text(
        """
export function openStream() {
  return new WebSocket("/stream")
}
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_browser_token_storage_violations(repo_root=tmp_path)

    assert violations == []


def test_product_default_tenant_check_flags_unguarded_fallback(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "product" / "example"
    source.mkdir(parents=True)
    (source / "api.py").write_text(
        """
def build_router(default_tenant_id=None):
    def auth_dep():
        if default_tenant_id is not None:
            return default_tenant_id
        raise PermissionError
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_product_default_tenant_without_production_guard_violations(
        repo_root=tmp_path
    )

    assert len(violations) == 1
    assert violations[0].check == "product-default-tenant-production-guard"
    assert violations[0].path == Path("services/product/example/api.py")


def test_product_default_tenant_check_allows_production_guard(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "product" / "example"
    source.mkdir(parents=True)
    (source / "api.py").write_text(
        """
def _request_is_production(request):
    return request.app.state.gateway_settings.is_production

def build_router(default_tenant_id=None):
    def auth_dep(request):
        if _request_is_production(request):
            raise PermissionError
        if default_tenant_id is not None:
            return default_tenant_id
        raise PermissionError
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_product_default_tenant_without_production_guard_violations(
        repo_root=tmp_path
    )

    assert violations == []


def test_network_call_in_transaction_check_flags_http_inside_tenant_tx(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "ingest"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        """
import httpx

async def run(tenant_id):
    async with tenant_transaction(tenant_id):
        async with httpx.AsyncClient() as client:
            await client.get("https://example.invalid")
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_network_call_in_transaction_violations(repo_root=tmp_path)

    assert len(violations) >= 1
    assert violations[0].path == Path("services/ingest/bad.py")


def test_network_call_in_transaction_check_flags_batch_api_inside_tx(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "ingest"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        """
async def run(conn, client):
    async with conn.transaction():
        await client.submit_jsonl("{}", metadata={})
        await client.retrieve("batch_123")
        await client.file_text("file_123")
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_network_call_in_transaction_violations(repo_root=tmp_path)

    assert {v.line_number for v in violations} == {3, 4, 5}
    assert {v.path for v in violations} == {Path("services/ingest/bad.py")}


def test_network_call_in_transaction_check_flags_object_storage_inside_tx(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "ingest"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        """
async def run(conn, s3_client):
    async with conn.transaction():
        await s3_client.put_if_absent("key", b"body")
        await s3_client.get("key")
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_network_call_in_transaction_violations(repo_root=tmp_path)

    assert {v.line_number for v in violations} == {3, 4}
    assert {v.path for v in violations} == {Path("services/ingest/bad.py")}


def test_network_call_in_transaction_check_flags_source_fetcher_and_publish_inside_tx(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "ingest"
    source.mkdir(parents=True)
    (source / "bad.py").write_text(
        """
async def run(conn, producer):
    async with conn.transaction():
        await fetch_page_gmail("cursor")
        await publish_embedding_request(producer=producer)
        await producer.produce("topic", key=b"k", value=b"v")
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_network_call_in_transaction_violations(repo_root=tmp_path)

    assert {v.line_number for v in violations} == {3, 4, 5}
    assert {v.path for v in violations} == {Path("services/ingest/bad.py")}


def test_network_call_in_transaction_check_allows_network_outside_tx(
    tmp_path: Path,
) -> None:
    source = tmp_path / "services" / "ingest"
    source.mkdir(parents=True)
    (source / "ok.py").write_text(
        """
async def run(conn, embedder):
    vec = await embedder.embed("customer text")
    async with conn.transaction():
        await conn.execute("SELECT 1", vec)
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_network_call_in_transaction_violations(repo_root=tmp_path)

    assert violations == []


def test_import_linter_allowlist_check_flags_growth(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.importlinter]
root_packages = ["lib", "services"]

[[tool.importlinter.contracts]]
name = "demo contract"
type = "forbidden"
source_modules = ["lib"]
forbidden_modules = ["services"]
ignore_imports = ["a -> b", "c -> d"]
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_import_linter_allowlist_violations(
        repo_root=tmp_path,
        limits={"demo contract": 1},
    )

    assert len(violations) == 1
    assert violations[0].check == "import-linter-allowlist-ratchet"


def test_import_linter_allowlist_check_allows_equal_or_lower_counts(
    tmp_path: Path,
) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.importlinter]
root_packages = ["lib", "services"]

[[tool.importlinter.contracts]]
name = "demo contract"
type = "forbidden"
source_modules = ["lib"]
forbidden_modules = ["services"]
ignore_imports = ["a -> b"]
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_import_linter_allowlist_violations(
        repo_root=tmp_path,
        limits={"demo contract": 1},
    )

    assert violations == []


def test_rollback_data_deletion_check_flags_volume_wipe(tmp_path: Path) -> None:
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "deploy-production.yml").write_text(
        "script: |\n  docker compose down --volumes\n",
        encoding="utf-8",
    )

    violations = find_rollback_data_deletion_violations(repo_root=tmp_path)

    assert len(violations) == 1
    assert violations[0].check == "rollback-data-deletion"
    assert "volume wipe" in violations[0].message


def test_rollback_data_deletion_check_allows_code_only_rollback(
    tmp_path: Path,
) -> None:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "deploy_compose_release.sh").write_text(
        "# docker compose down -v is forbidden\n"
        'git reset --hard "${PREVIOUS_SHA}"\n'
        "docker compose up -d --build --remove-orphans\n",
        encoding="utf-8",
    )

    violations = find_rollback_data_deletion_violations(repo_root=tmp_path)

    assert violations == []


def test_byoc_manifest_privacy_check_flags_public_or_raw_egress(
    tmp_path: Path,
) -> None:
    manifests = tmp_path / "deploy" / "byoc"
    manifests.mkdir(parents=True)
    (manifests / "bad.yaml").write_text(
        """
connectivity:
  direction: inbound
network:
  endpoint_exposure:
    - component: postgres
      exposure: public
telemetry:
  raw_payloads_allowed: true
data_residency:
  prompts_leave_boundary: true
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_byoc_manifest_privacy_violations(repo_root=tmp_path)

    assert [violation.check for violation in violations] == [
        "byoc-manifest-privacy",
        "byoc-manifest-privacy",
        "byoc-manifest-privacy",
        "byoc-manifest-privacy",
    ]
    assert {violation.line_number for violation in violations} == {2, 6, 8, 10}


def test_byoc_manifest_privacy_check_allows_checked_in_manifest() -> None:
    assert find_byoc_manifest_privacy_violations() == []


def test_byoc_agent_contract_privacy_check_flags_raw_token_or_telemetry(
    tmp_path: Path,
) -> None:
    contract = tmp_path / "services" / "platform" / "runtime"
    contract.mkdir(parents=True)
    (contract / "byoc_agent_contract.py").write_text(
        """
from typing import Literal

class ByocAgentTelemetryState:
    raw_logs_allowed: bool = True
    raw_payloads_allowed: Literal[False] = False
    raw_prompts_allowed: Literal[False] = False
    pii_allowed: Literal[False] = False

class ByocAgentEnrollmentPayload:
    install_token: str

class ByocAgentEnrollmentRequest:
    pass

class ByocAgentHeartbeat:
    pass
""".lstrip(),
        encoding="utf-8",
    )

    violations = find_byoc_agent_contract_privacy_violations(repo_root=tmp_path)

    assert [violation.check for violation in violations] == [
        "byoc-agent-contract-privacy",
        "byoc-agent-contract-privacy",
    ]
    assert {violation.line_number for violation in violations} == {4, 10}


def test_byoc_agent_contract_privacy_check_allows_checked_in_contract() -> None:
    assert find_byoc_agent_contract_privacy_violations() == []


def test_production_rollback_automation_does_not_delete_data() -> None:
    assert find_rollback_data_deletion_violations() == []
