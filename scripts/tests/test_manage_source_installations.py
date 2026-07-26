from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest
from cryptography.fernet import Fernet

from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore
from scripts.manage_source_installations import (
    SourceInstallationCliError,
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


async def _insert_provider_installation(
    conn: asyncpg.Connection,
    *,
    tenant,
    provider: str = "slack",
    installation_id: str | None = None,
    secret_ref: str | None = "secret://slack/T012345",
) -> UUID:
    row_id = uuid7()
    resolved_installation_id = installation_id or f"T{tenant.hex[:12]}"
    await conn.execute(
        """
        INSERT INTO provider_installations
          (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, $3, $4, $5, true)
        """,
        row_id,
        tenant,
        provider,
        resolved_installation_id,
        secret_ref,
    )
    return row_id


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


async def _insert_source_onboarding_run(
    conn: asyncpg.Connection,
    *,
    tenant,
    source: str,
    installation_row_id: UUID | None,
    status: str,
    age_minutes: int,
) -> UUID:
    run_id = uuid7()
    event_at = datetime.now(timezone.utc) - timedelta(minutes=age_minutes)
    completed_at = event_at if status in {"completed", "failed"} else None
    failure_reason = "provider failure" if status == "failed" else None
    await conn.execute(
        """
        INSERT INTO onboarding_runs (
            id, tenant_id, trigger_kind, workflow_id, status,
            sources_enabled, started_at, completed_at
        )
        VALUES ($1, $2, 'install', $3, 'complete', $4, $5, $6)
        """,
        run_id,
        tenant,
        f"install:{run_id}",
        [source],
        event_at,
        completed_at,
    )
    await conn.execute(
        """
        INSERT INTO source_onboarding_runs (
            onboarding_run_id, source, tenant_id, installation_row_id,
            status, started_at, completed_at, reconciled_at, failure_reason
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8)
        """,
        run_id,
        source,
        tenant,
        installation_row_id,
        status,
        event_at,
        completed_at,
        failure_reason,
    )
    return run_id


def test_manage_source_installations_requires_provider_for_native_id() -> None:
    args = _parse(
        [
            "status",
            "--tenant",
            str(uuid7()),
            "--operator-actor",
            str(uuid7()),
            "--installation-id",
            "T012345",
        ]
    )

    with pytest.raises(SourceInstallationCliError, match="requires --provider"):
        from scripts.manage_source_installations import _select_installations

        # The selector validates before issuing a query; passing None keeps the
        # test DB-free for this branch.
        import asyncio

        asyncio.run(_select_installations(None, tenant_id=uuid7(), args=args))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_manage_source_installations_status_pause_resume_cycle(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Source operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        installation_row_id = await _insert_provider_installation(conn, tenant=tenant)
        await _insert_source_onboarding_run(
            conn,
            tenant=tenant,
            source="slack",
            installation_row_id=installation_row_id,
            status="completed",
            age_minutes=5,
        )

        status_result = await run_command(
            _parse(
                [
                    "status",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--provider",
                    "slack",
                ]
            ),
            conn=conn,
        )
        assert status_result["ok"] is True
        assert status_result["action"] == "status"
        assert len(status_result["installations"]) == 1
        installation = status_result["installations"][0]
        assert installation["id"] == str(installation_row_id)
        assert installation["tenant_id"] == str(tenant)
        assert installation["provider"] == "slack"
        assert installation["installation_id"] == f"T{tenant.hex[:12]}"
        assert installation["enabled"] is True
        assert installation["installed_at"]
        assert installation["has_secret_ref"] is True
        assert installation["has_selected_repositories"] is False
        assert installation["latest_onboarding_status"] == "completed"
        assert installation["latest_onboarding_completed_at"]
        assert installation["latest_onboarding_reconciled_at"]
        assert installation["latest_onboarding_has_failure_reason"] is False
        assert installation["last_successful_sync_at"]
        assert installation["source_health"] == "healthy"

        pause_result = await run_command(
            _parse(
                [
                    "pause",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--provider",
                    "slack",
                    "--installation-id",
                    f"T{tenant.hex[:12]}",
                    "--reason",
                    "provider outage",
                ]
            ),
            conn=conn,
        )
        assert pause_result["ok"] is True
        assert pause_result["installation"]["enabled"] is False
        assert pause_result["installation"]["enabled_before"] is True

        resume_result = await run_command(
            _parse(
                [
                    "resume",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--installation-row-id",
                    str(installation_row_id),
                    "--reason",
                    "provider recovered",
                ]
            ),
            conn=conn,
        )
        assert resume_result["ok"] is True
        assert resume_result["installation"]["enabled"] is True
        assert resume_result["installation"]["enabled_before"] is False

        enabled = await conn.fetchval(
            "SELECT enabled FROM provider_installations WHERE id = $1",
            installation_row_id,
        )
        assert enabled is True

        audit_rows = await conn.fetch(
            """
            SELECT actor_id, action, resource_type, resource_id, metadata
            FROM operator_action_log
            WHERE tenant_id = $1
              AND action LIKE 'source_installation.%'
            ORDER BY action
            """,
            tenant,
        )
        assert sorted(row["action"] for row in audit_rows) == [
            "source_installation.pause",
            "source_installation.resume",
            "source_installation.status",
        ]
        assert {row["actor_id"] for row in audit_rows} == {operator_actor}
        assert {row["resource_type"] for row in audit_rows} == {
            "provider_installation"
        }
        by_action = {row["action"]: row for row in audit_rows}
        assert by_action["source_installation.status"]["resource_id"] == (
            installation_row_id
        )
        pause_metadata = _metadata(by_action["source_installation.pause"])
        assert pause_metadata["provider"] == "slack"
        assert pause_metadata["installation_id"] == f"T{tenant.hex[:12]}"
        assert pause_metadata["reason"] == "provider outage"
        assert pause_metadata["enabled_before"] is True
        assert pause_metadata["enabled_after"] is False
        assert (
            _metadata(by_action["source_installation.resume"])["reason"]
            == "provider recovered"
        )


@pytest.mark.asyncio
async def test_status_attributes_runs_to_exact_sibling_installation(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Source operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        completed_install = await _insert_provider_installation(
            conn,
            tenant=tenant,
            installation_id="T-COMPLETED",
        )
        failed_install = await _insert_provider_installation(
            conn,
            tenant=tenant,
            installation_id="T-FAILED",
        )
        no_run_install = await _insert_provider_installation(
            conn,
            tenant=tenant,
            installation_id="T-NO-RUN",
        )

        await _insert_source_onboarding_run(
            conn,
            tenant=tenant,
            source="slack",
            installation_row_id=completed_install,
            status="completed",
            age_minutes=30,
        )
        await _insert_source_onboarding_run(
            conn,
            tenant=tenant,
            source="slack",
            installation_row_id=failed_install,
            status="failed",
            age_minutes=5,
        )
        # A newer pre-contract row has no authoritative installation identity.
        # It must not be guessed onto any of the three sibling installations.
        await _insert_source_onboarding_run(
            conn,
            tenant=tenant,
            source="slack",
            installation_row_id=None,
            status="failed",
            age_minutes=1,
        )

        result = await run_command(
            _parse(
                [
                    "status",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--provider",
                    "slack",
                ]
            ),
            conn=conn,
        )

        by_native_id = {
            installation["installation_id"]: installation
            for installation in result["installations"]
        }
        assert set(by_native_id) == {"T-COMPLETED", "T-FAILED", "T-NO-RUN"}

        completed = by_native_id["T-COMPLETED"]
        assert completed["id"] == str(completed_install)
        assert completed["latest_onboarding_status"] == "completed"
        assert completed["last_successful_sync_at"] is not None
        assert completed["source_health"] == "healthy"

        failed = by_native_id["T-FAILED"]
        assert failed["id"] == str(failed_install)
        assert failed["latest_onboarding_status"] == "failed"
        assert failed["last_successful_sync_at"] is None
        assert failed["source_health"] == "degraded"

        no_run = by_native_id["T-NO-RUN"]
        assert no_run["id"] == str(no_run_install)
        assert no_run["latest_onboarding_status"] is None
        assert no_run["last_successful_sync_at"] is None
        assert no_run["source_health"] == "installed_no_sync"


@pytest.mark.asyncio
async def test_manage_source_installations_rotate_secret_preserves_ref_and_audits(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Source operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
        secret_ref = await secret_store.put(
            b"old-token",
            label=f"slack_bot_token:{tenant.hex}",
            tenant_id=tenant,
        )
        installation_id = f"T{tenant.hex[:12]}"
        installation_row_id = await _insert_provider_installation(
            conn,
            tenant=tenant,
            installation_id=installation_id,
            secret_ref=secret_ref,
        )
        monkeypatch.setenv("ROTATED_SLACK_TOKEN", "rotated-token")

        result = await run_command(
            _parse(
                [
                    "rotate-secret",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--provider",
                    "slack",
                    "--installation-id",
                    installation_id,
                    "--new-secret-env",
                    "ROTATED_SLACK_TOKEN",
                    "--reason",
                    "customer token rotation",
                ]
            ),
            conn=conn,
            secret_store=secret_store,
        )

        assert result["ok"] is True
        assert result["action"] == "rotate-secret"
        assert result["installation"]["id"] == str(installation_row_id)
        assert result["installation"]["secret_ref_rotated"] is True
        assert result["installation"]["has_secret_ref"] is True
        serialized_result = json.dumps(result, sort_keys=True)
        assert "old-token" not in serialized_result
        assert "rotated-token" not in serialized_result

        stored_ref = await conn.fetchval(
            "SELECT secret_ref FROM provider_installations WHERE id = $1",
            installation_row_id,
        )
        assert stored_ref == secret_ref
        assert await secret_store.get(secret_ref, tenant_id=tenant) == b"rotated-token"
        assert await conn.fetchval(
            "SELECT rotated_at IS NOT NULL FROM encrypted_secrets WHERE id = $1::uuid",
            secret_ref,
        )

        audit_row = await conn.fetchrow(
            """
            SELECT actor_id, action, resource_type, resource_id, metadata
            FROM operator_action_log
            WHERE tenant_id = $1
              AND action = 'source_installation.secret.rotate'
            """,
            tenant,
        )
        assert audit_row is not None
        assert audit_row["actor_id"] == operator_actor
        assert audit_row["resource_type"] == "provider_installation"
        assert audit_row["resource_id"] == installation_row_id
        metadata = _metadata(audit_row)
        assert metadata["provider"] == "slack"
        assert metadata["installation_id"] == installation_id
        assert metadata["reason"] == "customer token rotation"
        assert metadata["secret_source"] == "env"
        assert metadata["secret_ref_rotated"] is True
        serialized_metadata = json.dumps(metadata, sort_keys=True)
        assert "old-token" not in serialized_metadata
        assert "rotated-token" not in serialized_metadata


@pytest.mark.asyncio
async def test_manage_source_installations_uninstall_disables_clears_secret_and_audits(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Source operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
        secret_ref = await secret_store.put(
            b"delete-me-token",
            label=f"slack_bot_token:{tenant.hex}",
            tenant_id=tenant,
        )
        installation_id = f"T{tenant.hex[:12]}"
        installation_row_id = await _insert_provider_installation(
            conn,
            tenant=tenant,
            installation_id=installation_id,
            secret_ref=secret_ref,
        )

        result = await run_command(
            _parse(
                [
                    "uninstall",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--provider",
                    "slack",
                    "--installation-id",
                    installation_id,
                    "--reason",
                    "customer requested uninstall",
                ]
            ),
            conn=conn,
            secret_store=secret_store,
        )

        assert result["ok"] is True
        assert result["action"] == "uninstall"
        assert result["installation"]["id"] == str(installation_row_id)
        assert result["installation"]["enabled"] is False
        assert result["installation"]["enabled_before"] is True
        assert result["installation"]["has_secret_ref"] is False
        assert result["installation"]["secret_ref_deleted"] is True
        assert result["installation"]["provider_specific_cleanup_required"] is True
        assert "delete-me-token" not in json.dumps(result, sort_keys=True)

        row = await conn.fetchrow(
            """
            SELECT enabled, secret_ref
            FROM provider_installations
            WHERE id = $1
            """,
            installation_row_id,
        )
        assert row is not None
        assert row["enabled"] is False
        assert row["secret_ref"] is None
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM encrypted_secrets WHERE id = $1::uuid",
                secret_ref,
            )
            == 0
        )

        audit_row = await conn.fetchrow(
            """
            SELECT actor_id, action, resource_type, resource_id, metadata
            FROM operator_action_log
            WHERE tenant_id = $1
              AND action = 'source_installation.uninstall'
            """,
            tenant,
        )
        assert audit_row is not None
        assert audit_row["actor_id"] == operator_actor
        assert audit_row["resource_type"] == "provider_installation"
        assert audit_row["resource_id"] == installation_row_id
        metadata = _metadata(audit_row)
        assert metadata["provider"] == "slack"
        assert metadata["installation_id"] == installation_id
        assert metadata["reason"] == "customer requested uninstall"
        assert metadata["enabled_before"] is True
        assert metadata["enabled_after"] is False
        assert metadata["had_secret_ref"] is True
        assert metadata["secret_ref_deleted"] is True
        assert metadata["secret_ref_cleared"] is True
        assert metadata["provider_specific_cleanup_required"] is True
        assert metadata["data_deletion_required_separately"] is True
        serialized_metadata = json.dumps(metadata, sort_keys=True)
        assert secret_ref not in serialized_metadata
        assert "delete-me-token" not in serialized_metadata


@pytest.mark.asyncio
async def test_manage_source_installations_uninstall_can_keep_shared_secret_ref(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Source operator")
        await _grant_operator_role(conn, tenant=tenant, actor_id=operator_actor)
        secret_ref = "secret://shared/app-webhook"
        installation_row_id = await _insert_provider_installation(
            conn,
            tenant=tenant,
            provider="github",
            installation_id="gh-install",
            secret_ref=secret_ref,
        )

        result = await run_command(
            _parse(
                [
                    "uninstall",
                    "--tenant",
                    str(tenant),
                    "--operator-actor",
                    str(operator_actor),
                    "--provider",
                    "github",
                    "--installation-id",
                    "gh-install",
                    "--reason",
                    "app installation removed",
                    "--keep-secret-ref",
                ]
            ),
            conn=conn,
        )

        assert result["ok"] is True
        assert result["installation"]["id"] == str(installation_row_id)
        assert result["installation"]["enabled"] is False
        assert result["installation"]["has_secret_ref"] is True
        assert result["installation"]["secret_ref_deleted"] is False
        assert secret_ref not in json.dumps(result, sort_keys=True)

        row = await conn.fetchrow(
            "SELECT enabled, secret_ref FROM provider_installations WHERE id = $1",
            installation_row_id,
        )
        assert row is not None
        assert row["enabled"] is False
        assert row["secret_ref"] == secret_ref

        metadata = _metadata(
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
        assert metadata["secret_ref_deleted"] is False
        assert metadata["secret_ref_cleared"] is False
        assert secret_ref not in json.dumps(metadata, sort_keys=True)


@pytest.mark.asyncio
async def test_manage_source_installations_requires_operator_role(
    fresh_db: asyncpg.Pool,
    tenant,
    tenant_cleanup,
) -> None:
    async with fresh_db.acquire() as conn:
        operator_actor = await insert_actor(conn, tenant, "Unprivileged actor")
        await _insert_provider_installation(conn, tenant=tenant)

        with pytest.raises(SourceInstallationCliError, match="requires tenant role"):
            await run_command(
                _parse(
                    [
                        "status",
                        "--tenant",
                        str(tenant),
                        "--operator-actor",
                        str(operator_actor),
                    ]
                ),
                conn=conn,
            )
