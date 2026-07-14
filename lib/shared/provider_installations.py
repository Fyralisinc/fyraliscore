"""Shared provider_installations write helpers.

The webhook resolver is intentionally cross-tenant: a provider-native
installation id must map to exactly one Fyralis tenant. All onboarding paths
that seed provider_installations must therefore refuse cross-tenant rebinding.
"""
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
    """Create or refresh one provider_installations row for the same tenant.

    If the `(provider, installation_id)` row already belongs to another tenant,
    the guarded `ON CONFLICT ... WHERE tenant_id = EXCLUDED.tenant_id` update
    returns no row. Treat that as an installation collision and never disclose
    the foreign tenant or native installation id.
    """
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
