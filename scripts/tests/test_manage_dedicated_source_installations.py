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
) -> dict[str, str | UUID | None]:
    spec = SPECS[source]
    install_id = uuid7()
    access_ref = await secret_store.put(
        f"{source}-access-token",
        label=f"{source}_access:{scope_id}",
        tenant_id=tenant,
    )
    refresh_ref = (
        await secret_store.put(
            f"{source}-refresh-token",
            label=f"{source}_refresh:{scope_id}",
            tenant_id=tenant,
        )
        if "refresh_secret_ref" in spec.ref_columns
        else None
    )
    webhook_ref = (
        await secret_store.put(
            f"{source}-webhook-secret",
            label=f"{source}_webhook:{scope_id}",
            tenant_id=tenant,
        )
        if include_webhook and "webhook_secret_ref" in spec.ref_columns
        else None
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
        columns.append("base_url")
        values.append("https://api.example.test")
    if source == "jira":
        columns.append("account_email")
        values.append("operator@example.test")
    if source == "hibob":
        columns.append("service_user_id")
        values.append("service-user")
    if "secret_ref" in spec.ref_columns:
        columns.append("secret_ref")
        values.append(access_ref)
    if "refresh_secret_ref" in spec.ref_columns:
        columns.append("refresh_secret_ref")
        values.append(refresh_ref)
    if "webhook_secret_ref" in spec.ref_columns:
        columns.append("webhook_secret_ref")
        values.append(webhook_ref)

    placeholders = ", ".join(f"${idx}" for idx in range(1, len(values) + 1))
    await conn.execute(
        f"""
        INSERT INTO {spec.table} ({', '.join(columns)})
        VALUES ({placeholders})
        """,
        *values,
    )
    if spec.entity_table is not None and spec.entity_install_column is not None:
        await conn.execute(
            f"""
            INSERT INTO {spec.entity_table} (
                id, tenant_id, {spec.entity_install_column}, entity_type, state
            ) VALUES ($1, $2, $3, 'test_entity', 'active')
            """,
            uuid7(),
            tenant,
            install_id,
        )
    if webhook_ref:
        await conn.execute(
            """
            INSERT INTO provider_installations
              (id, tenant_id, provider, installation_id, secret_ref, enabled)
            VALUES ($1, $2, $3, $4, $5, true)
            """,
            uuid7(),
            tenant,
            source,
            scope_id,
            webhook_ref,
        )

    return {
        "id": install_id,
        "access_ref": access_ref,
        "refresh_ref": refresh_ref,
        "webhook_ref": webhook_ref,
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
