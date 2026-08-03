"""Manifest-discovered connector catalog and immutable runtime composition."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from services.ingest.connector_conformance import (
    ConnectorConformanceSuite,
    assert_connector_conforms,
)
from services.ingest.connector_platform.catalog import build_compatibility_candidates
from services.ingest.connector_runtime.composition import (
    ConnectorRuntimeComposition,
    build_runtime_composition,
)
from services.ingest.connector_runtime.discovery import candidate_from_manifest
from services.ingest.connector_runtime.policy import RoutingPolicy
from services.ingest.connector_runtime.registry import (
    ConnectorCandidate,
    HostCompatibility,
)
from services.ingest.connector_runtime.release_evidence import (
    ReleaseEvidenceCatalog,
    load_release_evidence,
)
from services.ingest.source_contract.manifest import (
    ConnectorManifest,
    load_connector_manifests,
)
from services.ingest.source_contract.versioning import SemanticVersion


SLACK_CONNECTOR_ID = "fyralis/slack"
NOTION_CONNECTOR_ID = "fyralis/notion"
WHATSAPP_CONNECTOR_ID = "fyralis/whatsapp"
_CONNECTORS_DIRECTORY = Path(__file__).resolve().parents[1] / "connectors"
_MANIFEST_DIRECTORY = _CONNECTORS_DIRECTORY / "manifests"
_RELEASE_EVIDENCE_PATH = _CONNECTORS_DIRECTORY / "release-evidence.json"


def _native_manifests() -> dict[str, ConnectorManifest]:
    manifests = load_connector_manifests(_MANIFEST_DIRECTORY)
    return {manifest.connector_id: manifest for manifest in manifests}


_NATIVE_MANIFESTS = _native_manifests()
SLACK_MANIFEST = _NATIVE_MANIFESTS[SLACK_CONNECTOR_ID]
NOTION_MANIFEST = _NATIVE_MANIFESTS[NOTION_CONNECTOR_ID]
WHATSAPP_MANIFEST = _NATIVE_MANIFESTS[WHATSAPP_CONNECTOR_ID]
_RELEASE_EVIDENCE = load_release_evidence(_RELEASE_EVIDENCE_PATH)
SLACK_CONFORMANCE_FINGERPRINT = _RELEASE_EVIDENCE.require(
    SLACK_CONNECTOR_ID, SLACK_MANIFEST.metadata.version
).structural_fingerprint
NOTION_CONFORMANCE_FINGERPRINT = _RELEASE_EVIDENCE.require(
    NOTION_CONNECTOR_ID, NOTION_MANIFEST.metadata.version
).structural_fingerprint
WHATSAPP_CONFORMANCE_FINGERPRINT = _RELEASE_EVIDENCE.require(
    WHATSAPP_CONNECTOR_ID, WHATSAPP_MANIFEST.metadata.version
).structural_fingerprint


def _conformed_native_candidate(manifest: ConnectorManifest) -> ConnectorCandidate:
    raw = candidate_from_manifest(
        manifest,
        origin=f"first-party-native:{manifest.source}",
    )
    report = ConnectorConformanceSuite().run(raw)
    assert_connector_conforms(report)
    expected = _RELEASE_EVIDENCE.require(
        manifest.connector_id, manifest.metadata.version
    ).structural_fingerprint
    if report.fingerprint != expected:
        raise ValueError(
            f"release evidence mismatch for {manifest.connector_id}@"
            f"{manifest.metadata.version}: expected {expected}, "
            f"computed {report.fingerprint}"
        )
    return replace(raw, conformance_fingerprint=report.fingerprint)


def build_slack_candidate() -> ConnectorCandidate:
    return _conformed_native_candidate(SLACK_MANIFEST)


def build_notion_candidate() -> ConnectorCandidate:
    return _conformed_native_candidate(NOTION_MANIFEST)


def build_whatsapp_candidate() -> ConnectorCandidate:
    return _conformed_native_candidate(WHATSAPP_MANIFEST)


def build_pilot_candidates() -> tuple[ConnectorCandidate, ...]:
    return (
        build_slack_candidate(),
        build_notion_candidate(),
        build_whatsapp_candidate(),
    )


def build_runtime_candidates() -> tuple[ConnectorCandidate, ...]:
    """Return the admitted native and compatibility catalog candidates."""

    candidates = build_pilot_candidates() + build_compatibility_candidates()
    _RELEASE_EVIDENCE.validate(candidates)
    return candidates


def build_pilot_composition(
    policy: RoutingPolicy | None = None,
) -> ConnectorRuntimeComposition:
    """Build the complete catalog; execution remains legacy unless opted in."""

    candidates = build_runtime_candidates()
    host = HostCompatibility(
        contract_versions=(SemanticVersion.parse("1.0.0"),),
        require_conformance_fingerprint=True,
        approved_conformance_fingerprints=_RELEASE_EVIDENCE.approved_fingerprints,
    )
    return build_runtime_composition(
        candidates,
        host=host,
        policy=policy or default_migrated_routing_policy(),
    )


def default_migrated_routing_policy(*, revision: int = 1) -> RoutingPolicy:
    """Return the safe bootstrap policy; durable policy must opt into connector mode."""

    return RoutingPolicy(revision=revision)


def release_evidence_catalog() -> ReleaseEvidenceCatalog:
    return _RELEASE_EVIDENCE


__all__ = [
    "NOTION_CONNECTOR_ID",
    "NOTION_CONFORMANCE_FINGERPRINT",
    "NOTION_MANIFEST",
    "SLACK_CONNECTOR_ID",
    "SLACK_CONFORMANCE_FINGERPRINT",
    "SLACK_MANIFEST",
    "WHATSAPP_CONNECTOR_ID",
    "WHATSAPP_CONFORMANCE_FINGERPRINT",
    "WHATSAPP_MANIFEST",
    "build_notion_candidate",
    "build_pilot_candidates",
    "build_pilot_composition",
    "build_runtime_candidates",
    "build_slack_candidate",
    "build_whatsapp_candidate",
    "default_migrated_routing_policy",
    "release_evidence_catalog",
]
