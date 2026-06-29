from __future__ import annotations

import argparse
import json
from uuid import UUID

import asyncpg
import pytest
from cryptography.fernet import Fernet

from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore
from scripts.manage_dedicated_source_installations import (
    SPECS,
    DedicatedSourceInstallationCliError,
    _installation_projection_sql,
    _uninstall_installation,
    _webhook_cleanup_complete,
    _webhook_cleanup_status,
    build_parser,
    run_command,
)
from scripts.tests.conftest import insert_actor
from services.platform.access_control.roles import grant_role


pytestmark = pytest.mark.integration


def _parse(argv: list[str]) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _metadata(row: asyncpg.Record) -> dict[str, object]:
    value = row["metadata"]
    return json.loads(value) if isinstance(value, str) else value


async def _grant_operator_role(
    conn: asyncpg.Connection,
    *,
    tenant,
    actor_id: UUID,
) -> None:
    await grant_role(
        actor_id,
        "tenant",
        None,
        "admin",
        actor_id,
        conn=conn,
        tenant_id=tenant,
    )


async def _insert_installation(
    conn: asyncpg.Connection,
    *,
    tenant,
    source: str,
    scope_id: str,
    secret_store: FernetSecretStore,
    include_webhook: bool = False,
    region: str = "us-east-1",
) -> dict[str, str | UUID | None]:
    spec = SPECS[source]
    install_id = uuid7()
    refs: dict[str, str | None] = {}
    ref_labels = {
        "secret_ref": "access-token",
        "refresh_secret_ref": "refresh-token",
        "webhook_secret_ref": "webhook-secret",
        "api_hash_secret_ref": "api-hash",
        "session_secret_ref": "session",
        "backfill_session_secret_ref": "backfill-session",
        "app_secret_ref": "app-secret",
        "verify_token_ref": "verify-token",
        "access_token_ref": "access-token",
    }
    for column in spec.ref_columns:
        if column == "webhook_secret_ref" and not include_webhook:
            refs[column] = None
            continue
        suffix = ref_labels[column]
        refs[column] = await secret_store.put(
            f"{source}-{suffix}",
            label=f"{source}_{suffix}:{scope_id}",
            tenant_id=tenant,
        )

    columns = [
        "id",
        "tenant_id",
        spec.scope_column,
    ]
    values: list[object] = [
        install_id,
        tenant,
        scope_id,
    ]
    if spec.scope_column != "base_url":
        if spec.base_url_column is not None:
            columns.append(spec.base_url_column)
            values.append("https://api.example.test")
    if source == "jira":
        columns.append("account_email")
        values.append("operator@example.test")
    if source == "hibob":
        columns.append("service_user_id")
        values.append("service-user")
    if source == "aws":
        columns.extend(["region", "credential_kind"])
        values.extend([region, "assume_role"])
    if source == "telegram":
        columns.extend(["api_id"])
        values.extend(["12345"])
    for column in spec.ref_columns:
        if refs[column] is None and column == "webhook_secret_ref":
            continue
        columns.append(column)
        values.append(refs[column])

    placeholders = ", ".join(f"${idx}" for idx in range(1, len(values) + 1))
    await conn.execute(
        f"""
        INSERT INTO {spec.table} ({', '.join(columns)})
        VALUES ({placeholders})
        """,
        *values,
    )
    entity_key_columns = {
        "quickbooks": ("entity_type", "'test_entity'"),
        "gusto": ("entity_type", "'test_entity'"),
        "ramp": ("entity_type", "'test_entity'"),
        "carta": ("entity_type", "'test_entity'"),
        "linkedin": ("entity_type", "'test_entity'"),
        "ashby": ("entity_type", "'test_entity'"),
        "hibob": ("entity_type", "'test_entity'"),
        "jira": ("project_key", "'TEST'"),
        "mercury": ("account_id", "'account-1'"),
        "brex": ("account_id", "'account-1'"),
        "deel": ("contract_id", "'contract-1'"),
        "miro": ("board_id", "'board-1'"),
        "figma": ("file_key", "'file-1'"),
        "telegram": ("dialog_id, dialog_kind", "1001, 'chat'"),
        "signal": ("thread_id, thread_kind", "1001, 'group'"),
    }
    if (
        spec.entity_table is not None
        and spec.entity_install_column is not None
        and source in entity_key_columns
    ):
        key_columns, key_values = entity_key_columns[source]
        await conn.execute(
            f"""
            INSERT INTO {spec.entity_table} (
                id, tenant_id, {spec.entity_install_column}, {key_columns}, state
            ) VALUES ($1, $2, $3, {key_values}, 'active')
            """,
            uuid7(),
            tenant,
            install_id,
        )
    webhook_ref = refs.get("webhook_secret_ref")
    if webhook_ref:
        provider_installation_id = scope_id
        if spec.webhook_installation_id_transform == "host":
            provider_installation_id = (
                scope_id.replace("https://", "")
                .replace("http://", "")
                .rstrip("/")
                .split("/")[0]
            )
        await conn.execute(
            """
            INSERT INTO provider_installations
              (id, tenant_id, provider, installation_id, secret_ref, enabled)
            VALUES ($1, $2, $3, $4, $5, true)
            """,
            uuid7(),
            tenant,
            source,
            provider_installation_id,
            webhook_ref,
        )

    return {
        "id": install_id,
        "access_ref": refs.get("secret_ref"),
        "refresh_ref": refs.get("refresh_secret_ref"),
        "webhook_ref": webhook_ref,
        "api_hash_ref": refs.get("api_hash_secret_ref"),
        "session_ref": refs.get("session_secret_ref"),
        "backfill_session_ref": refs.get("backfill_session_secret_ref"),
        "app_secret_ref": refs.get("app_secret_ref"),
        "verify_token_ref": refs.get("verify_token_ref"),
        "access_token_ref": refs.get("access_token_ref"),
    }


@pytest.mark.asyncio
async def test_dedicated_status_pause_resume_updates_webhook_provider_row(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Source operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
        inserted = await _insert_installation(
            conn,
            tenant=tenant,
            source="quickbooks",
            scope_id="realm-123",
            secret_store=secret_store,
            include_webhook=True,
        )

        status_result = await run_command(
            _parse(
                [
                    "status",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "quickbooks",
                    "--scope-id",
                    "realm-123",
                ]
            ),
            conn=conn,
        )
        assert status_result["ok"] is True
        assert status_result["installations"][0]["id"] == str(inserted["id"])
        assert status_result["installations"][0]["enabled"] is True
        assert status_result["installations"][0]["entity_count"] == 1
        assert status_result["installations"][0]["has_secret_ref"] is True
        assert status_result["installations"][0]["has_refresh_secret_ref"] is True
        assert status_result["installations"][0]["has_webhook_secret_ref"] is True

        pause_result = await run_command(
            _parse(
                [
                    "pause",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "quickbooks",
                    "--scope-id",
                    "realm-123",
                    "--reason",
                    "provider outage",
                ]
            ),
            conn=conn,
        )
        assert pause_result["installation"]["enabled"] is False
        assert pause_result["installation"]["enabled_before"] is True
        assert pause_result["installation"]["webhook_provider_row_updated"] is True
        assert await conn.fetchval(
            """
            SELECT enabled
            FROM provider_installations
            WHERE tenant_id = $1 AND provider = 'quickbooks'
            """,
            tenant,
        ) is False

        resume_result = await run_command(
            _parse(
                [
                    "resume",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "quickbooks",
                    "--installation-row-id",
                    str(inserted["id"]),
                    "--reason",
                    "provider recovered",
                ]
            ),
            conn=conn,
        )
        assert resume_result["installation"]["enabled"] is True
        assert resume_result["installation"]["enabled_before"] is False
        assert await conn.fetchval(
            """
            SELECT enabled
            FROM provider_installations
            WHERE tenant_id = $1 AND provider = 'quickbooks'
            """,
            tenant,
        ) is True

        audit_rows = await conn.fetch(
            """
            SELECT action, metadata
            FROM operator_action_log
            WHERE tenant_id = $1
              AND resource_type = 'quickbooks_installations'
            ORDER BY action
            """,
            tenant,
        )
        assert [row["action"] for row in audit_rows] == [
            "source_installation.pause",
            "source_installation.resume",
            "source_installation.status",
        ]
        serialized_metadata = json.dumps(
            [_metadata(row) for row in audit_rows], sort_keys=True,
        )
        assert "realm-123" not in serialized_metadata
        assert "scope_id_hash" in serialized_metadata


@pytest.mark.asyncio
async def test_dedicated_rotate_secret_preserves_ref_and_sanitizes_audit(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Source operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
        inserted = await _insert_installation(
            conn,
            tenant=tenant,
            source="linkedin",
            scope_id="urn:li:organization:123",
            secret_store=secret_store,
        )
        monkeypatch.setenv("ROTATED_LINKEDIN_REFRESH", "new-refresh-secret")

        result = await run_command(
            _parse(
                [
                    "rotate-secret",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "linkedin",
                    "--scope-id",
                    "urn:li:organization:123",
                    "--secret-field",
                    "refresh",
                    "--new-secret-env",
                    "ROTATED_LINKEDIN_REFRESH",
                    "--reason",
                    "customer token rotation",
                ]
            ),
            conn=conn,
            secret_store=secret_store,
        )

        assert result["ok"] is True
        assert result["installation"]["secret_ref_rotated"] is True
        assert result["installation"]["rotated_secret_field"] == "refresh"
        assert (
            await conn.fetchval(
                """
                SELECT refresh_secret_ref
                FROM linkedin_installations
                WHERE id = $1
                """,
                inserted["id"],
            )
            == inserted["refresh_ref"]
        )
        assert (
            await secret_store.get(str(inserted["refresh_ref"]), tenant_id=tenant)
        ).decode() == "new-refresh-secret"

        serialized_result = json.dumps(result, sort_keys=True)
        assert "new-refresh-secret" not in serialized_result
        assert "linkedin-refresh-token" not in serialized_result
        metadata = _metadata(
            await conn.fetchrow(
                """
                SELECT metadata
                FROM operator_action_log
                WHERE tenant_id = $1
                  AND action = 'source_installation.secret.rotate'
                """,
                tenant,
            )
        )
        assert metadata["source"] == "linkedin"
        assert metadata["secret_field"] == "refresh"
        assert "urn:li:organization:123" not in json.dumps(metadata, sort_keys=True)


@pytest.mark.asyncio
async def test_dedicated_uninstall_disables_rows_deletes_refs_and_audits_safely(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Source operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
        inserted = await _insert_installation(
            conn,
            tenant=tenant,
            source="gusto",
            scope_id="company-123",
            secret_store=secret_store,
            include_webhook=True,
        )

        result = await run_command(
            _parse(
                [
                    "uninstall",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "gusto",
                    "--scope-id",
                    "company-123",
                    "--reason",
                    "customer requested uninstall",
                ]
            ),
            conn=conn,
            secret_store=secret_store,
        )

        assert result["ok"] is True
        assert result["installation"]["enabled"] is False
        assert result["installation"]["refs_seen"] == 3
        assert result["installation"]["refs_deleted"] == 3
        assert result["installation"]["secret_delete_errors"] == 0
        assert result["installation"]["has_secret_ref"] is False
        assert result["installation"]["has_refresh_secret_ref"] is False
        assert result["installation"]["has_webhook_secret_ref"] is False
        assert result["installation"]["webhook_provider_row_updated"] is True
        assert (
            result["installation"]["webhook_cleanup_status"]
            == "local_resolver_disabled"
        )
        assert result["installation"]["webhook_cleanup_complete"] is True

        install_row = await conn.fetchrow(
            """
            SELECT disabled_at, secret_ref, refresh_secret_ref, webhook_secret_ref
            FROM gusto_installations
            WHERE id = $1
            """,
            inserted["id"],
        )
        assert install_row["disabled_at"] is not None
        assert install_row["secret_ref"] is None
        assert install_row["refresh_secret_ref"] is None
        assert install_row["webhook_secret_ref"] is None
        provider_row = await conn.fetchrow(
            """
            SELECT enabled, secret_ref
            FROM provider_installations
            WHERE tenant_id = $1 AND provider = 'gusto'
            """,
            tenant,
        )
        assert provider_row["enabled"] is False
        assert provider_row["secret_ref"] is None

        for ref_name in ("access_ref", "refresh_ref", "webhook_ref"):
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM encrypted_secrets WHERE id = $1::uuid",
                    inserted[ref_name],
                )
                == 0
            )

        operator_metadata = _metadata(
            await conn.fetchrow(
                """
                SELECT metadata
                FROM operator_action_log
                WHERE tenant_id = $1
                  AND action = 'source_installation.uninstall'
                """,
                tenant,
            )
        )
        assert operator_metadata["source"] == "gusto"
        assert operator_metadata["refs_deleted"] == 3
        assert operator_metadata["webhook_provider_row_updated"] is True
        assert operator_metadata["webhook_cleanup_status"] == "local_resolver_disabled"
        assert operator_metadata["webhook_cleanup_complete"] is True
        install_audit_context = _metadata(
            await conn.fetchrow(
                """
                SELECT context AS metadata
                FROM installation_audit_log
                WHERE tenant_id = $1
                  AND provider = 'gusto'
                  AND action = 'uninstall'
                """,
                tenant,
            )
        )
        assert install_audit_context["refs_deleted"] == 3
        assert install_audit_context["webhook_cleanup_status"] == (
            "local_resolver_disabled"
        )
        assert install_audit_context["webhook_cleanup_complete"] is True
        combined = json.dumps(
            {
                "result": result,
                "operator_metadata": operator_metadata,
                "install_audit_context": install_audit_context,
            },
            sort_keys=True,
        )
        assert "company-123" not in json.dumps(
            {
                "operator_metadata": operator_metadata,
                "install_audit_context": install_audit_context,
            },
            sort_keys=True,
        )
        assert "gusto-access-token" not in combined
        assert "gusto-refresh-token" not in combined
        assert "gusto-webhook-secret" not in combined


@pytest.mark.asyncio
async def test_dedicated_api_key_source_rotates_and_uninstalls_without_refresh(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Source operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
        inserted = await _insert_installation(
            conn,
            tenant=tenant,
            source="ashby",
            scope_id="ashby-org-123",
            secret_store=secret_store,
            include_webhook=True,
        )
        monkeypatch.setenv("ROTATED_ASHBY_KEY", "new-ashby-api-key")

        status_result = await run_command(
            _parse(
                [
                    "status",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "ashby",
                    "--scope-id",
                    "ashby-org-123",
                ]
            ),
            conn=conn,
        )
        assert status_result["installations"][0]["entity_count"] == 1
        assert status_result["installations"][0]["has_secret_ref"] is True
        assert "has_refresh_secret_ref" not in status_result["installations"][0]

        rotate_result = await run_command(
            _parse(
                [
                    "rotate-secret",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "ashby",
                    "--scope-id",
                    "ashby-org-123",
                    "--secret-field",
                    "access",
                    "--new-secret-env",
                    "ROTATED_ASHBY_KEY",
                    "--reason",
                    "customer api key rotation",
                ]
            ),
            conn=conn,
            secret_store=secret_store,
        )
        assert rotate_result["installation"]["rotated_secret_field"] == "access"
        assert (
            await secret_store.get(str(inserted["access_ref"]), tenant_id=tenant)
        ).decode() == "new-ashby-api-key"

        uninstall_result = await run_command(
            _parse(
                [
                    "uninstall",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "ashby",
                    "--scope-id",
                    "ashby-org-123",
                    "--reason",
                    "customer requested uninstall",
                ]
            ),
            conn=conn,
            secret_store=secret_store,
        )
        assert uninstall_result["installation"]["enabled"] is False
        assert uninstall_result["installation"]["refs_seen"] == 2
        assert uninstall_result["installation"]["refs_deleted"] == 2
        assert uninstall_result["installation"]["has_secret_ref"] is False
        assert uninstall_result["installation"]["has_webhook_secret_ref"] is False
        provider_row = await conn.fetchrow(
            """
            SELECT enabled, secret_ref
            FROM provider_installations
            WHERE tenant_id = $1 AND provider = 'ashby'
            """,
            tenant,
        )
        assert provider_row["enabled"] is False
        assert provider_row["secret_ref"] is None
        assert "new-ashby-api-key" not in json.dumps(uninstall_result, sort_keys=True)


@pytest.mark.asyncio
async def test_dedicated_aws_uses_account_and_region_selector(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Source operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
        inserted = await _insert_installation(
            conn,
            tenant=tenant,
            source="aws",
            scope_id="111122223333",
            region="us-west-2",
            secret_store=secret_store,
        )
        monkeypatch.setenv("ROTATED_AWS_ROLE", "arn:aws:iam::111122223333:role/Fyralis")

        status_result = await run_command(
            _parse(
                [
                    "status",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "aws",
                    "--scope-id",
                    "111122223333",
                    "--region",
                    "us-west-2",
                ]
            ),
            conn=conn,
        )
        assert status_result["installations"][0]["region"] == "us-west-2"
        assert status_result["installations"][0]["credential_kind"] == "assume_role"
        assert status_result["installations"][0]["base_url"] is None

        rotate_result = await run_command(
            _parse(
                [
                    "rotate-secret",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "aws",
                    "--scope-id",
                    "111122223333",
                    "--region",
                    "us-west-2",
                    "--secret-field",
                    "access",
                    "--new-secret-env",
                    "ROTATED_AWS_ROLE",
                    "--reason",
                    "customer role rotation",
                ]
            ),
            conn=conn,
            secret_store=secret_store,
        )
        assert rotate_result["installation"]["rotated_secret_field"] == "access"
        assert (
            await secret_store.get(str(inserted["access_ref"]), tenant_id=tenant)
        ).decode() == "arn:aws:iam::111122223333:role/Fyralis"

        uninstall_result = await run_command(
            _parse(
                [
                    "uninstall",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "aws",
                    "--scope-id",
                    "111122223333",
                    "--region",
                    "us-west-2",
                    "--reason",
                    "customer requested uninstall",
                ]
            ),
            conn=conn,
            secret_store=secret_store,
        )
        assert uninstall_result["installation"]["enabled"] is False
        assert uninstall_result["installation"]["refs_seen"] == 1
        assert uninstall_result["installation"]["refs_deleted"] == 1
        assert uninstall_result["installation"]["has_secret_ref"] is False
        assert uninstall_result["installation"]["region"] == "us-west-2"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "scope_id", "expected_refs"),
    [
        ("telegram", "@fyralis-ops", 3),
        ("signal", "+15551234567", 2),
    ],
)
async def test_dedicated_session_sources_rotate_and_uninstall_session_refs(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    scope_id: str,
    expected_refs: int,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Source operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
        inserted = await _insert_installation(
            conn,
            tenant=tenant,
            source=source,
            scope_id=scope_id,
            secret_store=secret_store,
        )
        monkeypatch.setenv("ROTATED_SESSION", f"{source}-new-session")

        rotate_result = await run_command(
            _parse(
                [
                    "rotate-secret",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    source,
                    "--scope-id",
                    scope_id,
                    "--secret-field",
                    "session",
                    "--new-secret-env",
                    "ROTATED_SESSION",
                    "--reason",
                    "linked-device session rotation",
                ]
            ),
            conn=conn,
            secret_store=secret_store,
        )
        assert rotate_result["installation"]["rotated_secret_field"] == "session"
        assert rotate_result["installation"]["base_url"] is None
        assert (
            await secret_store.get(str(inserted["session_ref"]), tenant_id=tenant)
        ).decode() == f"{source}-new-session"

        uninstall_result = await run_command(
            _parse(
                [
                    "uninstall",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    source,
                    "--scope-id",
                    scope_id,
                    "--reason",
                    "customer requested uninstall",
                ]
            ),
            conn=conn,
            secret_store=secret_store,
        )
        assert uninstall_result["installation"]["enabled"] is False
        assert uninstall_result["installation"]["refs_seen"] == expected_refs
        assert uninstall_result["installation"]["refs_deleted"] == expected_refs
        assert uninstall_result["installation"]["has_session_secret_ref"] is False
        assert uninstall_result["installation"]["has_backfill_session_secret_ref"] is False


@pytest.mark.asyncio
async def test_dedicated_whatsapp_enabled_table_lifecycle(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with fresh_db.acquire() as conn:
        phone_number_id = f"1555{tenant.hex[:10]}"
        operator_actor = await insert_actor(conn, tenant, "Source operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
        inserted = await _insert_installation(
            conn,
            tenant=tenant,
            source="whatsapp",
            scope_id=phone_number_id,
            secret_store=secret_store,
        )
        monkeypatch.setenv("ROTATED_WHATSAPP_APP_SECRET", "new-whatsapp-app-secret")

        status_result = await run_command(
            _parse(
                [
                    "status",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "whatsapp",
                    "--scope-id",
                    phone_number_id,
                ]
            ),
            conn=conn,
        )
        assert status_result["installations"][0]["enabled"] is True
        assert status_result["installations"][0]["has_app_secret_ref"] is True
        assert status_result["installations"][0]["has_verify_token_ref"] is True
        assert status_result["installations"][0]["has_access_token_ref"] is True

        pause_result = await run_command(
            _parse(
                [
                    "pause",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "whatsapp",
                    "--scope-id",
                    phone_number_id,
                    "--reason",
                    "customer pause",
                ]
            ),
            conn=conn,
        )
        assert pause_result["installation"]["enabled"] is False
        assert pause_result["installation"]["enabled_before"] is True
        assert await conn.fetchval(
            "SELECT enabled FROM whatsapp_installations WHERE id = $1",
            inserted["id"],
        ) is False

        resume_result = await run_command(
            _parse(
                [
                    "resume",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "whatsapp",
                    "--scope-id",
                    phone_number_id,
                    "--reason",
                    "customer resume",
                ]
            ),
            conn=conn,
        )
        assert resume_result["installation"]["enabled"] is True

        rotate_result = await run_command(
            _parse(
                [
                    "rotate-secret",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "whatsapp",
                    "--scope-id",
                    phone_number_id,
                    "--secret-field",
                    "app-secret",
                    "--new-secret-env",
                    "ROTATED_WHATSAPP_APP_SECRET",
                    "--reason",
                    "customer app secret rotation",
                ]
            ),
            conn=conn,
            secret_store=secret_store,
        )
        assert rotate_result["installation"]["rotated_secret_field"] == "app-secret"
        assert (
            await secret_store.get(str(inserted["app_secret_ref"]), tenant_id=tenant)
        ).decode() == "new-whatsapp-app-secret"

        uninstall_result = await run_command(
            _parse(
                [
                    "uninstall",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "whatsapp",
                    "--scope-id",
                    phone_number_id,
                    "--reason",
                    "customer requested uninstall",
                ]
            ),
            conn=conn,
            secret_store=secret_store,
        )
        assert uninstall_result["installation"]["enabled"] is False
        assert uninstall_result["installation"]["refs_seen"] == 3
        assert uninstall_result["installation"]["refs_deleted"] == 3
        assert uninstall_result["installation"]["has_app_secret_ref"] is False
        assert uninstall_result["installation"]["has_verify_token_ref"] is False
        assert uninstall_result["installation"]["has_access_token_ref"] is False
        row = await conn.fetchrow(
            """
            SELECT enabled, app_secret_ref, verify_token_ref, access_token_ref
            FROM whatsapp_installations
            WHERE id = $1
            """,
            inserted["id"],
        )
        assert row["enabled"] is False
        assert row["app_secret_ref"] is None
        assert row["verify_token_ref"] is None
        assert row["access_token_ref"] is None


def test_dedicated_rotate_webhook_rejects_poll_only_source() -> None:
    args = _parse(
        [
            "rotate-secret",
            "--tenant",
            str(uuid7()),
            "--operator-actor",
            str(uuid7()),
            "--source",
            "linkedin",
            "--scope-id",
            "urn:li:organization:123",
            "--secret-field",
            "webhook",
            "--new-secret-env",
            "IGNORED",
            "--reason",
            "not supported",
        ]
    )

    from scripts.manage_dedicated_source_installations import _column_for_secret_field

    with pytest.raises(DedicatedSourceInstallationCliError, match="not supported"):
        _column_for_secret_field(SPECS["linkedin"], args.secret_field)


def test_google_workspace_sources_have_no_secret_ref_lifecycle_specs() -> None:
    expected = {
        "gmail": ("gmail_installations", "gmail_mailbox_watches"),
        "google_calendar": (
            "google_calendar_installations",
            "google_calendar_calendars",
        ),
        "google_drive": ("google_drive_installations", "google_drive_targets"),
    }

    for source, (table, entity_table) in expected.items():
        spec = SPECS[source]
        assert spec.table == table
        assert spec.scope_column == "workspace_domain"
        assert spec.ref_columns == ()
        assert spec.entity_table == entity_table
        assert spec.base_url_column is None
        assert spec.native_google_watch_table is (
            source in {"google_calendar", "google_drive"}
        )


def test_webhook_cleanup_status_requires_local_resolver_disable() -> None:
    assert (
        _webhook_cleanup_status(
            spec=SPECS["gusto"],
            provider_row_updated=True,
        )
        == "local_resolver_disabled"
    )
    missing_status = _webhook_cleanup_status(
        spec=SPECS["gusto"],
        provider_row_updated=False,
    )

    assert missing_status == "provider_row_missing"
    assert _webhook_cleanup_complete(missing_status) is False
    assert (
        _webhook_cleanup_status(
            spec=SPECS["google_calendar"],
            provider_row_updated=False,
        )
        == "not_applicable"
    )


def test_installation_projection_supports_sources_without_ref_columns() -> None:
    projection = _installation_projection_sql(
        SPECS["google_drive"],
        entity_count_sql="0::int",
    )

    assert "i.id" in projection
    assert "NULL::text AS base_url" in projection
    assert "secret_ref" not in projection
    assert ",\n               ,\n" not in projection


class _CaptureFetchrowConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.args: list[tuple[object, ...]] = []

    async def fetchrow(self, query: str, *args: object) -> dict[str, object]:
        self.queries.append(query)
        self.args.append(args)
        return {"id": args[1]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source",
    sorted(source for source, spec in SPECS.items() if spec.ref_columns),
)
async def test_uninstall_sql_clears_every_secret_ref_column(source: str) -> None:
    spec = SPECS[source]
    conn = _CaptureFetchrowConnection()
    tenant_id = uuid7()
    row_id = uuid7()

    await _uninstall_installation(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        spec=spec,
        row_id=row_id,
        clear_columns=spec.ref_columns,
    )

    query = conn.queries[-1]
    assert f"UPDATE {spec.table} i" in query
    for column in spec.ref_columns:
        assert f"{column} = NULL" in query
    assert conn.args[-1] == (tenant_id, row_id)


@pytest.mark.asyncio
async def test_uninstall_sql_rejects_unknown_secret_ref_column() -> None:
    with pytest.raises(DedicatedSourceInstallationCliError):
        await _uninstall_installation(
            _CaptureFetchrowConnection(),  # type: ignore[arg-type]
            tenant_id=uuid7(),
            spec=SPECS["ashby"],
            row_id=uuid7(),
            clear_columns=("refresh_secret_ref",),
        )


@pytest.mark.asyncio
async def test_google_calendar_uninstall_clears_native_watch_state(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Calendar operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
        installation_id = uuid7()
        calendar_id = uuid7()
        await conn.execute(
            """
            INSERT INTO google_calendar_installations (
                id, tenant_id, workspace_domain, service_account_email, scope,
                inclusion_spec, resolved_calendar_count, resolved_at
            ) VALUES (
                $1, $2, 'acme.test', 'svc@acme.test',
                'calendar.readonly', '{}'::jsonb, 1, now()
            )
            """,
            installation_id,
            tenant,
        )
        await conn.execute(
            """
            INSERT INTO google_calendar_calendars (
                id, tenant_id, google_calendar_installation_id, calendar_id,
                owner_email, sync_token, state, watch_channel_id,
                watch_resource_id, watch_token, watch_expiration, watch_state
            ) VALUES (
                $1, $2, $3, 'primary', 'alice@acme.test', 'sync-1',
                'active', 'chan-1', 'resource-1', 'watch-token-1',
                now() + interval '1 day', 'active'
            )
            """,
            calendar_id,
            tenant,
            installation_id,
        )

        result = await run_command(
            _parse(
                [
                    "uninstall",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--source",
                    "google_calendar",
                    "--installation-row-id",
                    str(installation_id),
                    "--reason",
                    "customer requested uninstall",
                ]
            ),
            conn=conn,
            secret_store=secret_store,
        )

        assert result["installation"]["enabled"] is False
        assert result["installation"]["refs_seen"] == 0
        assert result["installation"]["refs_deleted"] == 0
        assert result["installation"]["webhook_cleanup_status"] == "not_applicable"
        assert result["installation"]["webhook_cleanup_complete"] is True
        assert result["installation"]["native_google_watch_rows_cleared"] == 1
        watch_row = await conn.fetchrow(
            """
            SELECT watch_state, watch_channel_id, watch_resource_id,
                   watch_token, watch_expiration
              FROM google_calendar_calendars
             WHERE id = $1
            """,
            calendar_id,
        )
        assert watch_row["watch_state"] == "inactive"
        assert watch_row["watch_channel_id"] is None
        assert watch_row["watch_resource_id"] is None
        assert watch_row["watch_token"] is None
        assert watch_row["watch_expiration"] is None

        action = await conn.fetchrow(
            """
            SELECT metadata
              FROM operator_action_log
             WHERE tenant_id = $1
               AND action = 'source_installation.uninstall'
               AND resource_type = 'google_calendar_installations'
             ORDER BY occurred_at DESC
             LIMIT 1
            """,
            tenant,
        )
        assert _metadata(action)["native_google_watch_rows_cleared"] == 1
