from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.errors import InstallationCollisionError
from services.app.webhooks.provider_installations import (
    upsert_provider_installation_for_tenant,
)


pytestmark = pytest.mark.integration


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tenant_id = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"provider-install-{tenant_id.hex[:8]}",
    )
    return tenant_id


async def test_provider_installation_upsert_rejects_cross_tenant_rebind(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_a = await _seed_tenant(fresh_db)
    tenant_b = await _seed_tenant(fresh_db)

    row_id = await upsert_provider_installation_for_tenant(
        fresh_db,
        provider="brex",
        tenant_id=tenant_a,
        installation_id="org-shared",
        secret_ref="secret-a",
    )
    refreshed_row_id = await upsert_provider_installation_for_tenant(
        fresh_db,
        provider="brex",
        tenant_id=tenant_a,
        installation_id="org-shared",
        secret_ref="secret-a2",
    )

    assert refreshed_row_id == row_id

    with pytest.raises(InstallationCollisionError):
        await upsert_provider_installation_for_tenant(
            fresh_db,
            provider="brex",
            tenant_id=tenant_b,
            installation_id="org-shared",
            secret_ref="secret-b",
        )

    row = await fresh_db.fetchrow(
        """
        SELECT tenant_id, secret_ref
          FROM provider_installations
         WHERE provider = 'brex'
           AND installation_id = 'org-shared'
        """
    )

    assert row is not None
    assert row["tenant_id"] == tenant_a
    assert row["secret_ref"] == "secret-a2"
