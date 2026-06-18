"""services.platform.extensions.grants — the extension_grants repo.

Read/write the per-(tenant, extension) capability grant (migration 0127). The
stored ``capabilities`` is the effective set (intersection of manifest-declared
and operator-approved); see ``access.resolve_capabilities`` for how it's combined
with manifest defaults + tenant enablement.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from lib.extensions.host_api.v1 import Capabilities
from lib.shared.tenant_context import tenant_transaction


@dataclass(frozen=True)
class ExtensionGrant:
    tenant_id: UUID
    extension_id: str
    granted_version: str
    capabilities: Capabilities
    trust_ceiling: str
    granted_by: str
    granted_at: datetime
    revoked_at: datetime | None = None

    @property
    def active(self) -> bool:
        return self.revoked_at is None


def _caps_from_db(value: Any) -> Capabilities:
    if isinstance(value, (str, bytes, bytearray)):
        value = json.loads(value)
    return Capabilities.from_dict(value or {})


def _row_to_grant(row: Any) -> ExtensionGrant:
    return ExtensionGrant(
        tenant_id=row["tenant_id"],
        extension_id=row["extension_id"],
        granted_version=row["granted_version"],
        capabilities=_caps_from_db(row["capabilities"]),
        trust_ceiling=row["trust_ceiling"],
        granted_by=row["granted_by"],
        granted_at=row["granted_at"],
        revoked_at=row["revoked_at"],
    )


class ExtensionGrantsRepo:
    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def grant(
        self,
        *,
        tenant_id: UUID,
        extension_id: str,
        granted_version: str,
        capabilities: Capabilities,
        trust_ceiling: str = "inferential_external",
        granted_by: str,
    ) -> None:
        """Upsert an active grant (re-grant clears any prior revocation)."""
        async with tenant_transaction(tenant_id, pool=self._pool) as ctx:
            await ctx.execute(
                """
                INSERT INTO extension_grants
                  (tenant_id, extension_id, granted_version, capabilities,
                   trust_ceiling, granted_by)
                VALUES ($1, $2, $3, $4::jsonb, $5, $6)
                ON CONFLICT (tenant_id, extension_id) DO UPDATE SET
                  granted_version = EXCLUDED.granted_version,
                  capabilities    = EXCLUDED.capabilities,
                  trust_ceiling   = EXCLUDED.trust_ceiling,
                  granted_by      = EXCLUDED.granted_by,
                  granted_at      = now(),
                  revoked_at      = NULL
                """,
                tenant_id, extension_id, granted_version,
                json.dumps(capabilities.to_dict()), trust_ceiling, granted_by,
            )

    async def revoke(self, *, tenant_id: UUID, extension_id: str) -> None:
        async with tenant_transaction(tenant_id, pool=self._pool) as ctx:
            await ctx.execute(
                """
                UPDATE extension_grants SET revoked_at = now()
                WHERE tenant_id = $1 AND extension_id = $2 AND revoked_at IS NULL
                """,
                tenant_id, extension_id,
            )

    async def get(self, *, tenant_id: UUID, extension_id: str) -> ExtensionGrant | None:
        """The ACTIVE grant for (tenant, extension), or None."""
        async with tenant_transaction(tenant_id, pool=self._pool) as ctx:
            row = await ctx.fetchrow(
                """
                SELECT * FROM extension_grants
                WHERE tenant_id = $1 AND extension_id = $2 AND revoked_at IS NULL
                """,
                tenant_id, extension_id,
            )
        return _row_to_grant(row) if row is not None else None

    async def list_for_tenant(self, tenant_id: UUID) -> list[ExtensionGrant]:
        async with tenant_transaction(tenant_id, pool=self._pool) as ctx:
            rows = await ctx.fetch(
                "SELECT * FROM extension_grants WHERE tenant_id = $1 "
                "ORDER BY extension_id",
                tenant_id,
            )
        return [_row_to_grant(r) for r in rows]


__all__ = ["ExtensionGrant", "ExtensionGrantsRepo"]
