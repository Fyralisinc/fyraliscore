"""Tenant-safe shared provider installation persistence."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from lib.shared.errors import InstallationCollisionError
from lib.shared.ids import uuid7


async def upsert_provider_installation_for_tenant(
    executor: Any,
    *,
    provider: str,
    tenant_id: UUID,
    installation_id: str,
    secret_ref: str | None,
) -> UUID:
    """Upsert only when an existing provider identity belongs to this tenant."""

    row = await executor.fetchrow(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, $3, $4, $5, TRUE)
        ON CONFLICT (provider, installation_id) DO UPDATE
            SET secret_ref = EXCLUDED.secret_ref,
                enabled = TRUE
            WHERE provider_installations.tenant_id = EXCLUDED.tenant_id
        RETURNING id
        """,
        uuid7(),
        tenant_id,
        provider,
        installation_id,
        secret_ref,
    )
    if row is None:
        raise InstallationCollisionError(
            f"{provider} installation is already bound to a different tenant"
        )
    return row["id"]


__all__ = ["upsert_provider_installation_for_tenant"]
