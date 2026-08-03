"""Durable installation authority used by connector binding."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from services.ingest.source_contract.connector import GrantedAuthority
from services.ingest.source_contract.errors import BindingError
from services.ingest.source_contract.models import InstallationRef


@dataclass(frozen=True)
class InstallationAuthority:
    installation_id: UUID
    tenant_id: UUID
    connector_id: str
    generation: int
    credential_owner: str
    secret_slots: frozenset[str] = frozenset()
    scopes: frozenset[str] = frozenset()
    outbound_hosts: frozenset[str] = frozenset()
    maximum_trust_tier: str = "untrusted"
    provenance: dict[str, Any] = field(default_factory=dict)
    granted_at: datetime | None = None
    revoked_at: datetime | None = None

    @property
    def active(self) -> bool:
        return self.revoked_at is None

    def validate_for(self, installation: InstallationRef) -> GrantedAuthority:
        if not self.active:
            raise BindingError(
                "installation authority has been revoked",
                details={"installation_id": str(installation.id)},
            )
        if (
            self.installation_id != installation.id
            or self.tenant_id != installation.tenant_id
            or self.connector_id != installation.connector_id
        ):
            raise BindingError(
                "durable authority does not belong to this installation",
                details={"installation_id": str(installation.id)},
            )
        if self.generation < installation.generation:
            raise BindingError(
                "durable authority predates the installation generation",
                details={
                    "installation_generation": installation.generation,
                    "authority_generation": self.generation,
                },
            )
        return GrantedAuthority(
            secret_slots=self.secret_slots,
            outbound_hosts=self.outbound_hosts,
            scopes=self.scopes,
            maximum_trust_tier=self.maximum_trust_tier,
        )


class AuthorityRepository(Protocol):
    async def load(self, installation_id: UUID) -> InstallationAuthority | None: ...

    async def grant(self, authority: InstallationAuthority) -> None: ...

    async def revoke(
        self,
        installation_id: UUID,
        *,
        revoked_at: datetime,
        reason: str,
    ) -> None: ...


__all__ = ["AuthorityRepository", "InstallationAuthority"]
