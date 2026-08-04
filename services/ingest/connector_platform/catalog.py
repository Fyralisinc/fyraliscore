"""Manifest-derived inventory for all Fyralis ingestion source families."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from services.ingest.connector_conformance import (
    ConnectorConformanceSuite,
    assert_connector_conforms,
)
from services.ingest.connector_runtime.composition import (
    ConnectorRuntimeComposition,
    build_runtime_composition as compose_runtime,
)
from services.ingest.connector_runtime.discovery import candidate_from_manifest
from services.ingest.connector_runtime.policy import RoutingPolicy
from services.ingest.connector_runtime.registry import ConnectorCandidate, HostCompatibility
from services.ingest.connector_runtime.release_evidence import (
    ReleaseEvidenceCatalog,
    load_release_evidence,
)
from services.ingest.source_contract.manifest import load_connector_manifests
from services.ingest.source_contract.versioning import SemanticVersion


@dataclass(frozen=True)
class ConnectorCatalogEntry:
    source: str
    ingress_kinds: tuple[str, ...]
    connector_id: str
    connector_version: str
    implementation: str


_MANIFEST_DIRECTORY = Path(__file__).resolve().parents[1] / "connectors" / "manifests"
_RELEASE_EVIDENCE_PATH = _MANIFEST_DIRECTORY.parent / "release-evidence.json"


def _load_catalog() -> tuple[ConnectorCatalogEntry, ...]:
    return tuple(
        ConnectorCatalogEntry(
            source=manifest.source,
            ingress_kinds=manifest.spec.ingress_kinds,
            connector_id=manifest.connector_id,
            connector_version=manifest.metadata.version,
            implementation=manifest.spec.implementation,
        )
        for manifest in load_connector_manifests(_MANIFEST_DIRECTORY)
    )


CONNECTOR_CATALOG = _load_catalog()
_MANIFESTS = load_connector_manifests(_MANIFEST_DIRECTORY)
_RELEASE_EVIDENCE = load_release_evidence(_RELEASE_EVIDENCE_PATH)


def catalog_by_source() -> dict[str, ConnectorCatalogEntry]:
    return {entry.source: entry for entry in CONNECTOR_CATALOG}


def _candidate(manifest: Any) -> ConnectorCandidate:
    raw = candidate_from_manifest(
        manifest,
        origin=f"first-party-native:{manifest.source}",
    )
    report = ConnectorConformanceSuite().run(raw)
    assert_connector_conforms(report)
    evidence = _RELEASE_EVIDENCE.require(
        manifest.connector_id,
        manifest.metadata.version,
    )
    if report.fingerprint != evidence.structural_fingerprint:
        raise ValueError(
            f"release evidence mismatch for {manifest.connector_id}@"
            f"{manifest.metadata.version}"
        )
    if evidence.behavioral_fingerprint is None:
        raise ValueError(
            f"behavioral release evidence is missing for {manifest.connector_id}@"
            f"{manifest.metadata.version}"
        )
    return replace(raw, conformance_fingerprint=evidence.admission_fingerprint)


def build_runtime_candidates() -> tuple[ConnectorCandidate, ...]:
    candidates = tuple(
        _candidate(manifest)
        for manifest in sorted(_MANIFESTS, key=lambda item: item.connector_id)
    )
    _RELEASE_EVIDENCE.validate(candidates)
    return candidates


def default_routing_policy(*, revision: int = 1) -> RoutingPolicy:
    return RoutingPolicy(revision=revision)


def build_connector_runtime(
    policy: RoutingPolicy | None = None,
) -> ConnectorRuntimeComposition:
    candidates = build_runtime_candidates()
    host = HostCompatibility(
        contract_versions=(SemanticVersion.parse("1.0.0"),),
        require_conformance_fingerprint=True,
        approved_conformance_fingerprints=_RELEASE_EVIDENCE.approved_fingerprints,
    )
    return compose_runtime(
        candidates,
        host=host,
        policy=policy or default_routing_policy(),
    )


def release_evidence_catalog() -> ReleaseEvidenceCatalog:
    return _RELEASE_EVIDENCE


__all__ = [
    "CONNECTOR_CATALOG",
    "ConnectorCatalogEntry",
    "build_connector_runtime",
    "build_runtime_candidates",
    "catalog_by_source",
    "default_routing_policy",
    "release_evidence_catalog",
]
