"""Side-by-side connector runtime composition root."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from services.ingest.connector_runtime.definitions import (
    DEFAULT_HOST_COMPATIBILITY,
    ConnectorCandidate,
    HostCompatibility,
    ManifestValidator,
)
from services.ingest.connector_runtime.policy import AtomicRoutingPolicy, RoutingPolicy
from services.ingest.connector_runtime.registry import (
    ConnectorRegistry,
    ConnectorRegistryBuilder,
)


@dataclass(frozen=True)
class ConnectorRuntimeComposition:
    """One immutable registry plus a replaceable routing policy snapshot."""

    registry: ConnectorRegistry
    routing: AtomicRoutingPolicy

    @property
    def registry_fingerprint(self) -> str:
        return self.registry.health().fingerprint


def build_runtime_composition(
    candidates: Iterable[ConnectorCandidate],
    *,
    host: HostCompatibility = DEFAULT_HOST_COMPATIBILITY,
    validators: Iterable[ManifestValidator] = (),
    policy: RoutingPolicy | None = None,
) -> ConnectorRuntimeComposition:
    """Validate then freeze a registry without touching legacy registries."""

    registry = ConnectorRegistryBuilder(host, validators=validators).extend(
        candidates
    ).build()
    return ConnectorRuntimeComposition(
        registry=registry,
        routing=AtomicRoutingPolicy(policy),
    )


__all__ = ["ConnectorRuntimeComposition", "build_runtime_composition"]
