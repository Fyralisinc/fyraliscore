"""Durable installation authority used by connector binding."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from services.ingest.source_contract.connector import GrantedAuthority
from services.ingest.source_contract.errors import BindingError
from services.ingest.source_contract.manifest import ConnectorManifest
from services.ingest.source_contract.models import InstallationRef


_TRUST_ORDER = (
    "authoritative",
    "attested_agent",
    "authoritative_external",
    "reputable",
    "inferential",
    "inferential_external",
    "unvetted",
    "untrusted",
)
_TRUST_RANK = {value: index for index, value in enumerate(_TRUST_ORDER)}


def scope_authority(
    manifest: ConnectorManifest,
    granted: GrantedAuthority,
) -> GrantedAuthority:
    """Validate a grant and reduce it to the manifest's permission ceiling.

    Grants are intentionally partial and name only current credential-backed
    slots. Installation/configuration and pure normalization can bind before
    provider credentials exist. Capability factories expose only operations
    configured by the resulting scoped slots, while host ports enforce the same
    reduced hosts, scopes, and secrets.
    """

    requested = manifest.spec.permissions
    manifest_trust = manifest.spec.trust.maximum_tier
    if (
        manifest_trust not in _TRUST_RANK
        or granted.maximum_trust_tier not in _TRUST_RANK
    ):
        raise BindingError(
            "binding authority contains an unsupported trust tier",
            details={"connector_id": manifest.connector_id},
        )
    trust_ceiling = max(
        (manifest_trust, granted.maximum_trust_tier),
        key=_TRUST_RANK.__getitem__,
    )
    return GrantedAuthority(
        secret_slots=frozenset(requested.secret_slots) & granted.secret_slots,
        outbound_hosts=frozenset(requested.outbound_hosts) & granted.outbound_hosts,
        scopes=frozenset(requested.requested_scopes) & granted.scopes,
        maximum_trust_tier=trust_ceiling,
    )


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


__all__ = ["AuthorityRepository", "InstallationAuthority", "scope_authority"]
