from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import pytest
from cryptography.fernet import Fernet

from lib.shared.secrets import FernetSecretStore
from scripts.uninstall_whatsapp_installation import uninstall_whatsapp_installation


pytestmark = pytest.mark.integration


async def _seed_installation_with_refs(
    pool: asyncpg.Pool,
    store: FernetSecretStore,
) -> tuple[UUID, str, tuple[str, str, str]]:
    tenant_id = uuid4()
    phone_number_id = f"phone-{uuid4().hex}"
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"whatsapp-uninstall-{tenant_id.hex[:8]}",
    )
    app_ref = await store.put(
        "app-secret",
        label=f"whatsapp_app_secret:{phone_number_id}",
        tenant_id=tenant_id,
    )
    verify_ref = await store.put(
        "verify-token",
        label=f"whatsapp_verify_token:{phone_number_id}",
        tenant_id=tenant_id,
    )
    access_ref = await store.put(
        "access-token",
        label=f"whatsapp_access_token:{phone_number_id}",
        tenant_id=tenant_id,
    )
    await pool.execute(
        """
        INSERT INTO whatsapp_installations
            (tenant_id, phone_number_id, app_secret_ref, verify_token_ref,
             access_token_ref, app_secret, verify_token, access_token, enabled)
        VALUES ($1, $2, $3, $4, $5, 'legacy-app', 'legacy-verify',
                'legacy-access', true)
        """,
        tenant_id,
        phone_number_id,
        app_ref,
        verify_ref,
        access_ref,
    )
    return tenant_id, phone_number_id, (app_ref, verify_ref, access_ref)


async def test_uninstall_whatsapp_dry_run_does_not_mutate(
    gateway_pool: asyncpg.Pool,
) -> None:
    store = FernetSecretStore(gateway_pool, master_kek=Fernet.generate_key())
    _tenant_id, phone_number_id, _refs = await _seed_installation_with_refs(
        gateway_pool,
        store,
    )

    result = await uninstall_whatsapp_installation(
        gateway_pool,
        phone_number_id=phone_number_id,
        secret_store=store,
        dry_run=True,
    )

    row = await gateway_pool.fetchrow(
        """
        SELECT enabled, app_secret, app_secret_ref
          FROM whatsapp_installations
         WHERE phone_number_id = $1
        """,
        phone_number_id,
    )
    assert result.found is True
    assert result.dry_run is True
    assert result.refs_seen == 3
    assert row["enabled"] is True
    assert row["app_secret"] == "legacy-app"
    assert row["app_secret_ref"] is not None


async def test_uninstall_whatsapp_zeroizes_refs_and_writes_audit(
    gateway_pool: asyncpg.Pool,
) -> None:
    store = FernetSecretStore(gateway_pool, master_kek=Fernet.generate_key())
    tenant_id, phone_number_id, refs = await _seed_installation_with_refs(
        gateway_pool,
        store,
    )

    result = await uninstall_whatsapp_installation(
        gateway_pool,
        phone_number_id=phone_number_id,
        secret_store=store,
    )

    row = await gateway_pool.fetchrow(
        """
        SELECT enabled, app_secret, verify_token, access_token,
               app_secret_ref, verify_token_ref, access_token_ref
          FROM whatsapp_installations
         WHERE phone_number_id = $1
        """,
        phone_number_id,
    )
    remaining = await gateway_pool.fetchval(
        "SELECT count(*) FROM encrypted_secrets WHERE id = ANY($1::uuid[])",
        [UUID(ref) for ref in refs],
    )
    audit = await gateway_pool.fetchrow(
        """
        SELECT provider, action, status, context
          FROM installation_audit_log
         WHERE tenant_id = $1
         ORDER BY created_at DESC
         LIMIT 1
        """,
        tenant_id,
    )

    assert result.found is True
    assert result.enabled_before is True
    assert result.refs_seen == 3
    assert result.refs_deleted == 3
    assert result.secret_delete_errors == 0
    assert result.audit_written is True
    assert row["enabled"] is False
    assert row["app_secret"] is None
    assert row["verify_token"] is None
    assert row["access_token"] is None
    assert row["app_secret_ref"] is None
    assert row["verify_token_ref"] is None
    assert row["access_token_ref"] is None
    assert remaining == 0
    assert audit["provider"] == "whatsapp"
    assert audit["action"] == "uninstall"
    assert audit["status"] == "ok"
    assert "phone_number_id_hash" in audit["context"]
    assert audit["context"]["refs_deleted"] == 3


async def test_uninstall_whatsapp_is_idempotent(
    gateway_pool: asyncpg.Pool,
) -> None:
    store = FernetSecretStore(gateway_pool, master_kek=Fernet.generate_key())
    _tenant_id, phone_number_id, _refs = await _seed_installation_with_refs(
        gateway_pool,
        store,
    )

    await uninstall_whatsapp_installation(
        gateway_pool,
        phone_number_id=phone_number_id,
        secret_store=store,
    )
    rerun = await uninstall_whatsapp_installation(
        gateway_pool,
        phone_number_id=phone_number_id,
        secret_store=store,
    )

    assert rerun.found is True
    assert rerun.enabled_before is False
    assert rerun.refs_seen == 0
    assert rerun.refs_deleted == 0
    assert rerun.secret_delete_errors == 0


async def test_uninstall_whatsapp_missing_installation_is_noop(
    gateway_pool: asyncpg.Pool,
) -> None:
    store = FernetSecretStore(gateway_pool, master_kek=Fernet.generate_key())

    result = await uninstall_whatsapp_installation(
        gateway_pool,
        phone_number_id=f"missing-{uuid4().hex}",
        secret_store=store,
    )

    assert result.found is False
    assert result.refs_seen == 0
    assert result.refs_deleted == 0
