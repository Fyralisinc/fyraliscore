"""Immutable registry inputs and negotiated connector descriptions."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TypeAlias

from services.ingest.connector_runtime.diagnostics import RegistryDiagnostic
from services.ingest.source_contract.capabilities import CAPABILITY_CATALOG
from services.ingest.source_contract.connector import CapabilityKey, SourceConnector
from services.ingest.source_contract.manifest import (
    CapabilityRef,
    ConnectorManifest,
    IsolationMode,
)
from services.ingest.source_contract.versioning import SemanticVersion


ConnectorFactory: TypeAlias = Callable[[], SourceConnector]
ManifestValidator: TypeAlias = Callable[
    [ConnectorManifest], Sequence[RegistryDiagnostic]
]


def _validate_fingerprint(fingerprint: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None:
        raise ValueError("conformance fingerprint must be a lowercase SHA-256")


@dataclass(frozen=True)
class HostCompatibility:
    contract_versions: tuple[SemanticVersion, ...]
    capability_catalog: Mapping[CapabilityRef, CapabilityKey[object]] = field(
        default_factory=lambda: CAPABILITY_CATALOG
    )
    isolation_modes: frozenset[IsolationMode] = frozenset(
        {IsolationMode.IN_PROCESS_TRUSTED}
    )
    require_conformance_fingerprint: bool = False
    approved_conformance_fingerprints: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not self.contract_versions:
            raise ValueError("host compatibility requires a contract version")
        if len(self.contract_versions) != len(set(self.contract_versions)):
            raise ValueError("host contract versions must be unique")
        object.__setattr__(
            self,
            "contract_versions",
            tuple(sorted(self.contract_versions)),
        )
        object.__setattr__(
            self,
            "capability_catalog",
            MappingProxyType(dict(self.capability_catalog)),
        )
        object.__setattr__(
            self,
            "isolation_modes",
            frozenset(self.isolation_modes),
        )
        object.__setattr__(
            self,
            "approved_conformance_fingerprints",
            frozenset(self.approved_conformance_fingerprints),
        )
        for ref, key in self.capability_catalog.items():
            if ref != key.ref:
                raise ValueError("host capability catalog key does not match value")
        for fingerprint in self.approved_conformance_fingerprints:
            _validate_fingerprint(fingerprint)


DEFAULT_HOST_COMPATIBILITY = HostCompatibility(
    contract_versions=(SemanticVersion.parse("1.0.0"),)
)


@dataclass(frozen=True)
class ConnectorCandidate:
    manifest: ConnectorManifest
    factory: ConnectorFactory
    capability_keys: tuple[CapabilityKey[object], ...]
    origin: str = "explicit"
    conformance_fingerprint: str | None = None

    def __post_init__(self) -> None:
        refs = [key.ref for key in self.capability_keys]
        if len(refs) != len(set(refs)):
            raise ValueError("candidate capability keys must be unique")
        if self.conformance_fingerprint is not None:
            _validate_fingerprint(self.conformance_fingerprint)
        object.__setattr__(
            self,
            "capability_keys",
            tuple(
                sorted(
                    self.capability_keys,
                    key=lambda key: (key.ref.id, key.ref.version),
                )
            ),
        )


@dataclass(frozen=True)
class ConnectorDescription:
    connector_id: str
    source: str
    connector_version: str
    negotiated_contract_version: str
    capabilities: tuple[CapabilityRef, ...]
    origin: str
    conformance_fingerprint: str | None


__all__ = [
    "ConnectorCandidate",
    "ConnectorDescription",
    "ConnectorFactory",
    "DEFAULT_HOST_COMPATIBILITY",
    "HostCompatibility",
    "ManifestValidator",
]
