"""Root connector, binding context, and capability-resolution contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Generic, Mapping, Protocol, TypeVar, runtime_checkable
from uuid import UUID

from services.ingest.source_contract.errors import (
    BindingError,
    CapabilityMismatchError,
    CapabilityUnavailableError,
)
from services.ingest.source_contract.host_services import HostServices
from services.ingest.source_contract.manifest import CapabilityRef, ConnectorManifest
from services.ingest.source_contract.models import InstallationRef


T_co = TypeVar("T_co", covariant=True)


@dataclass(frozen=True)
class CapabilityKey(Generic[T_co]):
    ref: CapabilityRef
    interface: type[T_co]


@dataclass(frozen=True)
class GrantedAuthority:
    secret_slots: frozenset[str] = frozenset()
    outbound_hosts: frozenset[str] = frozenset()
    scopes: frozenset[str] = frozenset()
    maximum_trust_tier: str = "untrusted"


@dataclass(frozen=True)
class BindingContext:
    installation: InstallationRef
    authority: GrantedAuthority
    services: HostServices


@dataclass(frozen=True)
class OperationContext:
    invocation_id: UUID
    deadline: datetime
    services: HostServices


@runtime_checkable
class BoundConnector(Protocol):
    @property
    def installation(self) -> InstallationRef: ...

    def capability(self, key: CapabilityKey[T_co]) -> T_co | None: ...


@runtime_checkable
class SourceConnector(Protocol):
    @property
    def manifest(self) -> ConnectorManifest: ...

    def bind(self, context: BindingContext) -> BoundConnector: ...


class StaticBoundConnector:
    """Immutable bound connector useful for first-party and legacy adapters."""

    def __init__(
        self,
        installation: InstallationRef,
        capabilities: Mapping[CapabilityRef, object],
    ) -> None:
        self._installation = installation
        self._capabilities = MappingProxyType(dict(capabilities))

    @property
    def installation(self) -> InstallationRef:
        return self._installation

    @property
    def capability_refs(self) -> tuple[CapabilityRef, ...]:
        return tuple(self._capabilities)

    def capability(self, key: CapabilityKey[T_co]) -> T_co | None:
        implementation = self._capabilities.get(key.ref)
        if implementation is None:
            return None
        if not isinstance(implementation, key.interface):
            raise CapabilityMismatchError(
                f"capability {key.ref.id}/v{key.ref.version} does not satisfy "
                f"{key.interface.__name__}",
                details={
                    "capability": key.ref.id,
                    "version": key.ref.version,
                    "interface": key.interface.__name__,
                },
            )
        return implementation

    def require(self, key: CapabilityKey[T_co]) -> T_co:
        implementation = self.capability(key)
        if implementation is None:
            raise CapabilityUnavailableError(
                f"installation {self.installation.id} does not expose "
                f"{key.ref.id}/v{key.ref.version}",
                details={
                    "installation_id": str(self.installation.id),
                    "capability": key.ref.id,
                    "version": key.ref.version,
                },
            )
        return implementation


def validate_binding_identity(
    *,
    manifest: ConnectorManifest,
    context: BindingContext,
    binding: BoundConnector,
) -> None:
    if context.installation.connector_id != manifest.connector_id:
        raise BindingError(
            "binding context connector ID does not match the connector manifest",
            details={
                "manifest_connector_id": manifest.connector_id,
                "installation_connector_id": context.installation.connector_id,
            },
        )
    if binding.installation != context.installation:
        raise BindingError(
            "connector returned a binding for a different installation",
            details={
                "expected_installation_id": str(context.installation.id),
                "actual_installation_id": str(binding.installation.id),
            },
        )


__all__ = [
    "BindingContext",
    "BoundConnector",
    "CapabilityKey",
    "GrantedAuthority",
    "OperationContext",
    "SourceConnector",
    "StaticBoundConnector",
    "validate_binding_identity",
]
