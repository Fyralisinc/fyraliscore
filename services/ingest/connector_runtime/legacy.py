"""Explicit compatibility adapter for staged coexistence with legacy sources.

This module intentionally imports no legacy registry. Phase 2 can inject current
planner/fetcher/handler adapters source by source, while the old runtime remains
unchanged during Phase 1.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import TypeAlias

from services.ingest.connector_runtime.registry import ConnectorCandidate
from services.ingest.source_contract.connector import (
    BindingContext,
    CapabilityKey,
    StaticBoundConnector,
)
from services.ingest.source_contract.manifest import CapabilityRef, ConnectorManifest


LegacyCapabilityProvider: TypeAlias = Callable[[BindingContext], object]


class LegacyConnectorAdapter:
    """Compose injected legacy behavior into the new connector shape.

    Providers are called at bind time and therefore can adapt installation rows
    or clients without storing tenant state globally. Nothing is discovered or
    registered at import time.
    """

    def __init__(
        self,
        manifest: ConnectorManifest,
        providers: Mapping[CapabilityRef, LegacyCapabilityProvider],
    ) -> None:
        self._manifest = manifest
        self._providers = MappingProxyType(dict(providers))

    @property
    def manifest(self) -> ConnectorManifest:
        return self._manifest

    def bind(self, context: BindingContext) -> StaticBoundConnector:
        capabilities = {
            ref: provider(context) for ref, provider in self._providers.items()
        }
        return StaticBoundConnector(context.installation, capabilities)

    def candidate(
        self,
        capability_keys: tuple[CapabilityKey[object], ...],
        *,
        origin: str | None = None,
        conformance_fingerprint: str | None = None,
    ) -> ConnectorCandidate:
        return ConnectorCandidate(
            manifest=self.manifest,
            factory=lambda: self,
            capability_keys=capability_keys,
            origin=origin or f"legacy-adapter:{self.manifest.connector_id}",
            conformance_fingerprint=conformance_fingerprint,
        )


__all__ = ["LegacyCapabilityProvider", "LegacyConnectorAdapter"]
