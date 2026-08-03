"""PostgreSQL persistence for installation authority and provenance."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

from services.ingest.connector_runtime.authority import InstallationAuthority


class PostgresAuthorityRepository:
    def __init__(self, executor: Any) -> None:
        self._executor = executor

    async def load(self, installation_id: UUID) -> InstallationAuthority | None:
        row = await self._executor.fetchrow(
            """
            SELECT installation_id, tenant_id, connector_id,
                   authority_generation, credential_owner,
                   granted_slot_names, granted_scopes,
                   granted_outbound_hosts, maximum_trust_tier,
                   provenance, granted_at, revoked_at
              FROM source_connector_authority_grants
             WHERE installation_id = $1
            """,
            installation_id,
        )
        if row is None:
            return None
        return InstallationAuthority(
            installation_id=row["installation_id"],
            tenant_id=row["tenant_id"],
            connector_id=row["connector_id"],
            generation=int(row["authority_generation"]),
            credential_owner=row["credential_owner"],
            secret_slots=frozenset(row["granted_slot_names"] or ()),
            scopes=frozenset(row["granted_scopes"] or ()),
            outbound_hosts=frozenset(row["granted_outbound_hosts"] or ()),
            maximum_trust_tier=row["maximum_trust_tier"],
            provenance=dict(row["provenance"] or {}),
            granted_at=row["granted_at"],
            revoked_at=row["revoked_at"],
        )

    async def grant(self, authority: InstallationAuthority) -> None:
        await self._executor.execute(
            """
            INSERT INTO source_connector_authority_grants (
                installation_id, tenant_id, connector_id,
                authority_generation, credential_owner,
                granted_slot_names, granted_scopes,
                granted_outbound_hosts, maximum_trust_tier,
                provenance, granted_at, revoked_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6::text[], $7::text[], $8::text[],
                $9, $10::jsonb, COALESCE($11, now()), $12
            )
            ON CONFLICT (installation_id) DO UPDATE SET
                tenant_id = EXCLUDED.tenant_id,
                connector_id = EXCLUDED.connector_id,
                authority_generation = EXCLUDED.authority_generation,
                credential_owner = EXCLUDED.credential_owner,
                granted_slot_names = EXCLUDED.granted_slot_names,
                granted_scopes = EXCLUDED.granted_scopes,
                granted_outbound_hosts = EXCLUDED.granted_outbound_hosts,
                maximum_trust_tier = EXCLUDED.maximum_trust_tier,
                provenance = EXCLUDED.provenance,
                updated_at = now(),
                revoked_at = EXCLUDED.revoked_at
            WHERE source_connector_authority_grants.tenant_id = EXCLUDED.tenant_id
              AND source_connector_authority_grants.connector_id = EXCLUDED.connector_id
              AND source_connector_authority_grants.authority_generation <= EXCLUDED.authority_generation
            """,
            authority.installation_id,
            authority.tenant_id,
            authority.connector_id,
            authority.generation,
            authority.credential_owner,
            sorted(authority.secret_slots),
            sorted(authority.scopes),
            sorted(authority.outbound_hosts),
            authority.maximum_trust_tier,
            json.dumps(authority.provenance),
            authority.granted_at,
            authority.revoked_at,
        )

    async def revoke(
        self,
        installation_id: UUID,
        *,
        revoked_at: datetime,
        reason: str,
    ) -> None:
        await self._executor.execute(
            """
            UPDATE source_connector_authority_grants
               SET revoked_at = $2,
                   updated_at = now(),
                   provenance = provenance || jsonb_build_object(
                       'revocation_reason', $3
                   )
             WHERE installation_id = $1
               AND revoked_at IS NULL
            """,
            installation_id,
            revoked_at,
            reason,
        )


__all__ = ["PostgresAuthorityRepository"]
