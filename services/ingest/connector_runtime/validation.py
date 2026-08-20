"""Side-effect-free validation and compatibility negotiation."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from typing import TypeAlias

from services.ingest.connector_runtime.definitions import (
    ConnectorCandidate,
    HostCompatibility,
    ManifestValidator,
)
from services.ingest.connector_runtime.diagnostics import (
    DiagnosticSeverity,
    RegistryDiagnostic,
)
from services.ingest.source_contract.manifest import CapabilityRef
from services.ingest.source_contract.versioning import SemanticVersion


NegotiatedCapabilities: TypeAlias = dict[
    str, tuple[SemanticVersion, frozenset[CapabilityRef]]
]


def validate_candidates(
    candidates: tuple[ConnectorCandidate, ...],
    host: HostCompatibility,
    validators: tuple[ManifestValidator, ...],
) -> tuple[list[RegistryDiagnostic], NegotiatedCapabilities]:
    diagnostics: list[RegistryDiagnostic] = []
    negotiated: NegotiatedCapabilities = {}

    connector_ids: dict[str, list[ConnectorCandidate]] = defaultdict(list)
    sources: dict[str, list[ConnectorCandidate]] = defaultdict(list)
    for candidate in candidates:
        connector_ids[candidate.manifest.connector_id].append(candidate)
        sources[candidate.manifest.source].append(candidate)
        for alias in candidate.manifest.metadata.aliases:
            sources[alias].append(candidate)
    _report_duplicates(diagnostics, connector_ids, "duplicate_connector_id")
    _report_duplicates(diagnostics, sources, "duplicate_source")

    for candidate in candidates:
        manifest = candidate.manifest
        connector_id = manifest.connector_id
        contract = manifest.spec.contract_range.select_highest(
            host.contract_versions
        )
        if contract is None:
            diagnostics.append(
                RegistryDiagnostic(
                    severity=DiagnosticSeverity.ERROR,
                    code="contract_incompatible",
                    message=(
                        f"connector contract {manifest.spec.contract!r} has no "
                        "version in common with the host"
                    ),
                    connector_id=connector_id,
                    origin=candidate.origin,
                )
            )

        if manifest.spec.runtime.isolation not in host.isolation_modes:
            diagnostics.append(
                RegistryDiagnostic(
                    severity=DiagnosticSeverity.ERROR,
                    code="isolation_unsupported",
                    message=(
                        f"host does not support isolation mode "
                        f"{manifest.spec.runtime.isolation.value}"
                    ),
                    connector_id=connector_id,
                    origin=candidate.origin,
                )
            )

        _validate_conformance_evidence(
            diagnostics,
            candidate,
            host,
        )
        candidate_keys = {key.ref: key for key in candidate.capability_keys}
        manifest_refs = set(manifest.capability_refs)
        missing_implementations = manifest_refs - set(candidate_keys)
        undeclared_implementations = set(candidate_keys) - manifest_refs
        for ref in sorted(
            missing_implementations, key=lambda item: (item.id, item.version)
        ):
            diagnostics.append(
                RegistryDiagnostic(
                    severity=DiagnosticSeverity.ERROR,
                    code="declared_capability_missing",
                    message="manifest capability has no candidate implementation",
                    connector_id=connector_id,
                    capability=ref,
                    origin=candidate.origin,
                )
            )
        for ref in sorted(
            undeclared_implementations, key=lambda item: (item.id, item.version)
        ):
            diagnostics.append(
                RegistryDiagnostic(
                    severity=DiagnosticSeverity.ERROR,
                    code="undeclared_capability",
                    message=(
                        "candidate exposes a capability absent from its manifest"
                    ),
                    connector_id=connector_id,
                    capability=ref,
                    origin=candidate.origin,
                )
            )

        supported = _validate_capabilities(
            diagnostics,
            candidate,
            host,
        )
        _run_manifest_validators(diagnostics, candidate, validators)
        if contract is not None:
            negotiated[connector_id] = (contract, frozenset(supported))

    return diagnostics, negotiated


def _validate_conformance_evidence(
    diagnostics: list[RegistryDiagnostic],
    candidate: ConnectorCandidate,
    host: HostCompatibility,
) -> None:
    fingerprint = candidate.conformance_fingerprint
    if host.require_conformance_fingerprint and fingerprint is None:
        diagnostics.append(
            RegistryDiagnostic(
                severity=DiagnosticSeverity.ERROR,
                code="conformance_evidence_missing",
                message="connector has no conformance fingerprint",
                connector_id=candidate.manifest.connector_id,
                origin=candidate.origin,
            )
        )
    elif (
        fingerprint is not None
        and host.approved_conformance_fingerprints
        and fingerprint not in host.approved_conformance_fingerprints
    ):
        diagnostics.append(
            RegistryDiagnostic(
                severity=DiagnosticSeverity.ERROR,
                code="conformance_fingerprint_unapproved",
                message=(
                    "connector conformance fingerprint is not approved by the host"
                ),
                connector_id=candidate.manifest.connector_id,
                origin=candidate.origin,
            )
        )


def _validate_capabilities(
    diagnostics: list[RegistryDiagnostic],
    candidate: ConnectorCandidate,
    host: HostCompatibility,
) -> set[CapabilityRef]:
    manifest = candidate.manifest
    candidate_keys = {key.ref: key for key in candidate.capability_keys}
    declarations = {
        declaration.ref: declaration
        for declaration in manifest.spec.capabilities
    }
    supported: set[CapabilityRef] = set()
    for ref in manifest.capability_refs:
        host_key = host.capability_catalog.get(ref)
        if host_key is None:
            declaration = declarations[ref]
            severity = (
                DiagnosticSeverity.ERROR
                if declaration.required
                else DiagnosticSeverity.WARNING
            )
            diagnostics.append(
                RegistryDiagnostic(
                    severity=severity,
                    code=(
                        "required_capability_unsupported"
                        if declaration.required
                        else "optional_capability_omitted"
                    ),
                    message="host does not support this capability version",
                    connector_id=manifest.connector_id,
                    capability=ref,
                    origin=candidate.origin,
                )
            )
            continue
        candidate_key = candidate_keys.get(ref)
        if (
            candidate_key is not None
            and candidate_key.interface is not host_key.interface
        ):
            diagnostics.append(
                RegistryDiagnostic(
                    severity=DiagnosticSeverity.ERROR,
                    code="capability_interface_mismatch",
                    message=(
                        "candidate and host associate different interfaces "
                        "with the same capability"
                    ),
                    connector_id=manifest.connector_id,
                    capability=ref,
                    origin=candidate.origin,
                )
            )
            continue
        supported.add(ref)
    return supported


def _run_manifest_validators(
    diagnostics: list[RegistryDiagnostic],
    candidate: ConnectorCandidate,
    validators: tuple[ManifestValidator, ...],
) -> None:
    for validator in validators:
        try:
            diagnostics.extend(validator(candidate.manifest))
        except Exception as exc:  # noqa: BLE001 - diagnostic boundary
            diagnostics.append(
                RegistryDiagnostic(
                    severity=DiagnosticSeverity.ERROR,
                    code="manifest_validator_failed",
                    message=(
                        "manifest policy validator raised "
                        f"{type(exc).__name__}"
                    ),
                    connector_id=candidate.manifest.connector_id,
                    origin=candidate.origin,
                )
            )


def _report_duplicates(
    diagnostics: list[RegistryDiagnostic],
    values: Mapping[str, list[ConnectorCandidate]],
    code: str,
) -> None:
    for value, candidates in sorted(values.items()):
        if len(candidates) < 2:
            continue
        for candidate in candidates:
            diagnostics.append(
                RegistryDiagnostic(
                    severity=DiagnosticSeverity.ERROR,
                    code=code,
                    message=f"registry value {value!r} is declared more than once",
                    connector_id=candidate.manifest.connector_id,
                    origin=candidate.origin,
                )
            )


__all__ = ["NegotiatedCapabilities", "validate_candidates"]
