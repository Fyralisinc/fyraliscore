from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import pytest
from cryptography.fernet import Fernet

from lib.shared.secrets import FernetSecretStore
from scripts.migrate_whatsapp_secret_refs import migrate_whatsapp_secret_refs


pytestmark = pytest.mark.integration


async def _seed_legacy_installation(
    pool: asyncpg.Pool,
    *,
    phone_number_id: str,
) -> tuple[UUID, str]:
    tenant_id = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"whatsapp-migrate-{tenant_id.hex[:8]}",
    )
    await pool.execute(
        """
        INSERT INTO whatsapp_installations
            (tenant_id, phone_number_id, app_secret, verify_token, access_token)
        VALUES ($1, $2, $3, $4, $5)
        """,
        tenant_id,
        phone_number_id,
        "legacy-app-secret",
        "legacy-verify-token",
        "legacy-access-token",
    )
    return tenant_id, phone_number_id


async def test_migrate_whatsapp_secret_refs_dry_run_does_not_mutate(
    gateway_pool: asyncpg.Pool,
) -> None:
    phone_number_id = f"phone-{uuid4().hex}"
    await _seed_legacy_installation(gateway_pool, phone_number_id=phone_number_id)
    store = FernetSecretStore(gateway_pool, master_kek=Fernet.generate_key())

    result = await migrate_whatsapp_secret_refs(
        gateway_pool,
        secret_store=store,
        dry_run=True,
    )

    row = await gateway_pool.fetchrow(
        """
        SELECT app_secret, app_secret_ref, verify_token, verify_token_ref,
               access_token, access_token_ref
          FROM whatsapp_installations
         WHERE phone_number_id = $1
        """,
        phone_number_id,
    )
    assert result.rows_seen == 1
    assert result.rows_updated == 0
    assert result.refs_created == 0
    assert result.dry_run is True
    assert row["app_secret"] == "legacy-app-secret"
    assert row["app_secret_ref"] is None
    assert row["verify_token"] == "legacy-verify-token"
    assert row["verify_token_ref"] is None
    assert row["access_token"] == "legacy-access-token"
    assert row["access_token_ref"] is None


async def test_migrate_whatsapp_secret_refs_clears_plaintext_and_is_idempotent(
    gateway_pool: asyncpg.Pool,
) -> None:
    phone_number_id = f"phone-{uuid4().hex}"
    tenant_id, _phone_number_id = await _seed_legacy_installation(
        gateway_pool,
        phone_number_id=phone_number_id,
    )
    store = FernetSecretStore(gateway_pool, master_kek=Fernet.generate_key())

    result = await migrate_whatsapp_secret_refs(
        gateway_pool,
        secret_store=store,
    )

    row = await gateway_pool.fetchrow(
        """
        SELECT app_secret, app_secret_ref, verify_token, verify_token_ref,
               access_token, access_token_ref
          FROM whatsapp_installations
         WHERE phone_number_id = $1
        """,
        phone_number_id,
    )
    assert result.rows_seen == 1
    assert result.rows_updated == 1
    assert result.refs_created == 3
    assert row["app_secret"] is None
    assert row["verify_token"] is None
    assert row["access_token"] is None
    assert await store.get(row["app_secret_ref"], tenant_id=tenant_id) == (
        b"legacy-app-secret"
    )
    assert await store.get(row["verify_token_ref"], tenant_id=tenant_id) == (
        b"legacy-verify-token"
    )
    assert await store.get(row["access_token_ref"], tenant_id=tenant_id) == (
        b"legacy-access-token"
    )

    rerun = await migrate_whatsapp_secret_refs(
        gateway_pool,
        secret_store=store,
    )

    assert rerun.rows_seen == 0
    assert rerun.rows_updated == 0
    assert rerun.refs_created == 0
