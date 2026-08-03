"""Explicit, deterministic, immutable Source Connector registry."""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import TypeAlias

from services.ingest.connector_runtime.diagnostics import (
    DiagnosticSeverity,
    RegistryDiagnostic,
    has_errors,
)
from services.ingest.source_contract.capabilities import CAPABILITY_CATALOG
from services.ingest.source_contract.connector import (
    BindingContext,
    BoundConnector,
    CapabilityKey,
    SourceConnector,
    StaticBoundConnector,
    validate_binding_identity,
)
from services.ingest.source_contract.errors import (
    BindingError,
    CapabilityMismatchError,
    ConnectorError,
    ConnectorNotFoundError,
    RegistryBuildError,
)
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


class RegistryStatus(StrEnum):
    READY = "ready"
    DEGRADED = "degraded"


@dataclass(frozen=True)
class RegistryHealth:
    status: RegistryStatus
    fingerprint: str
    connector_count: int
    connectors: tuple[ConnectorDescription, ...]
    diagnostics: tuple[RegistryDiagnostic, ...]

    @property
    def healthy(self) -> bool:
        return self.status is RegistryStatus.READY


@dataclass(frozen=True)
class RegisteredConnector:
    manifest: ConnectorManifest
    connector: SourceConnector
    negotiated_contract: SemanticVersion
    capability_keys: tuple[CapabilityKey[object], ...]
    origin: str
    conformance_fingerprint: str | None = None

    @property
    def connector_id(self) -> str:
        return self.manifest.connector_id

    @property
    def source(self) -> str:
        return self.manifest.source

    def bind(self, context: BindingContext) -> StaticBoundConnector:
        _validate_authority(self.manifest, context)
        try:
            raw_binding = self.connector.bind(context)
        except ConnectorError:
            raise
        except Exception as exc:  # noqa: BLE001 - normalize the boundary
            raise BindingError(
                f"connector {self.connector_id} failed while binding",
                details={
                    "connector_id": self.connector_id,
                    "exception_type": type(exc).__name__,
                },
            ) from exc
        validate_binding_identity(
            manifest=self.manifest,
            context=context,
            binding=raw_binding,
        )

        capabilities: dict[CapabilityRef, object] = {}
        for key in self.capability_keys:
            try:
                implementation = raw_binding.capability(key)
            except CapabilityMismatchError as exc:
                raise BindingError(
                    f"connector {self.connector_id} returned an invalid "
                    f"implementation for {key.ref.id}/v{key.ref.version}",
                    details=exc.details,
                ) from exc
            except ConnectorError:
                raise
            except Exception as exc:  # noqa: BLE001 - normalize the boundary
                raise BindingError(
                    f"connector {self.connector_id} failed capability resolution",
                    details={
                        "connector_id": self.connector_id,
                        "capability": key.ref.id,
                        "version": key.ref.version,
                        "exception_type": type(exc).__name__,
                    },
                ) from exc
            if implementation is None:
                raise BindingError(
                    f"connector {self.connector_id} declared but did not bind "
                    f"{key.ref.id}/v{key.ref.version}",
                    details={
                        "connector_id": self.connector_id,
                        "capability": key.ref.id,
                        "version": key.ref.version,
                    },
                )
            capabilities[key.ref] = implementation
        return StaticBoundConnector(context.installation, capabilities)

    def describe(self) -> ConnectorDescription:
        return ConnectorDescription(
            connector_id=self.connector_id,
            source=self.source,
            connector_version=self.manifest.metadata.version,
            negotiated_contract_version=str(self.negotiated_contract),
            capabilities=tuple(key.ref for key in self.capability_keys),
            origin=self.origin,
            conformance_fingerprint=self.conformance_fingerprint,
        )


class ConnectorRegistry:
    """An immutable snapshot of compatible connector definitions."""

    def __init__(
        self,
        registrations: Mapping[str, RegisteredConnector],
        diagnostics: tuple[RegistryDiagnostic, ...],
    ) -> None:
        by_id = dict(registrations)
        self._by_id = MappingProxyType(by_id)
        by_source: dict[str, RegisteredConnector] = {}
        for registration in by_id.values():
            by_source[registration.source] = registration
            for alias in registration.manifest.metadata.aliases:
                by_source[alias] = registration
        self._by_source = MappingProxyType(by_source)
        by_capability: dict[CapabilityRef, list[RegisteredConnector]] = defaultdict(
            list
        )
        for registration in by_id.values():
            for key in registration.capability_keys:
                by_capability[key.ref].append(registration)
        self._by_capability = MappingProxyType(
            {
                ref: tuple(sorted(values, key=lambda value: value.connector_id))
                for ref, values in by_capability.items()
            }
        )
        self._diagnostics = diagnostics
        self._fingerprint = _registry_fingerprint(by_id)

    @property
    def diagnostics(self) -> tuple[RegistryDiagnostic, ...]:
        return self._diagnostics

    def __len__(self) -> int:
        return len(self._by_id)

    def connector_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._by_id))

    def require(self, connector_id: str) -> RegisteredConnector:
        registration = self._by_id.get(connector_id)
        if registration is None:
            raise ConnectorNotFoundError(
                f"connector {connector_id!r} is not registered",
                details={"connector_id": connector_id},
            )
        return registration

    def for_source(self, source: str) -> RegisteredConnector:
        registration = self._by_source.get(source)
        if registration is None:
            raise ConnectorNotFoundError(
                f"no connector is registered for source {source!r}",
                details={"source": source},
            )
        return registration

    def list_by_capability(
        self, capability: CapabilityRef
    ) -> tuple[RegisteredConnector, ...]:
        return self._by_capability.get(capability, ())

    def resolve_for_install(self, context: BindingContext) -> StaticBoundConnector:
        return self.require(context.installation.connector_id).bind(context)

    def describe(self, connector_id: str) -> ConnectorDescription:
        return self.require(connector_id).describe()

    def descriptions(self) -> tuple[ConnectorDescription, ...]:
        return tuple(self._by_id[key].describe() for key in sorted(self._by_id))

    def health(self) -> RegistryHealth:
        status = (
            RegistryStatus.DEGRADED
            if any(
                diagnostic.severity is DiagnosticSeverity.WARNING
                for diagnostic in self._diagnostics
            )
            else RegistryStatus.READY
        )
        return RegistryHealth(
            status=status,
            fingerprint=self._fingerprint,
            connector_count=len(self),
            connectors=self.descriptions(),
            diagnostics=self._diagnostics,
        )


@dataclass(frozen=True)
class RegistryBuildResult:
    registry: ConnectorRegistry | None
    diagnostics: tuple[RegistryDiagnostic, ...]

    @property
    def succeeded(self) -> bool:
        return self.registry is not None and not has_errors(self.diagnostics)

    def require_registry(self) -> ConnectorRegistry:
        if self.registry is None:
            raise RegistryBuildError(
                "connector registry validation failed",
                details={
                    "errors": tuple(
                        diagnostic.message
                        for diagnostic in self.diagnostics
                        if diagnostic.severity is DiagnosticSeverity.ERROR
                    )
                },
            )
        return self.registry


class ConnectorRegistryBuilder:
    """Build a registry in two phases: static validation, then activation.

    No connector factory is invoked if any static validation error exists. This
    keeps invalid manifests and conflicts side-effect free.
    """

    def __init__(
        self,
        host: HostCompatibility = DEFAULT_HOST_COMPATIBILITY,
        *,
        validators: Iterable[ManifestValidator] = (),
    ) -> None:
        self._host = host
        self._validators = tuple(validators)
        self._candidates: list[ConnectorCandidate] = []

    def add(self, candidate: ConnectorCandidate) -> "ConnectorRegistryBuilder":
        self._candidates.append(candidate)
        return self

    def extend(
        self, candidates: Iterable[ConnectorCandidate]
    ) -> "ConnectorRegistryBuilder":
        self._candidates.extend(candidates)
        return self

    def build_result(self) -> RegistryBuildResult:
        candidates = tuple(
            sorted(
                self._candidates,
                key=lambda candidate: (
                    candidate.manifest.connector_id,
                    candidate.origin,
                ),
            )
        )
        diagnostics, negotiated = self._validate_static(candidates)
        if has_errors(tuple(diagnostics)):
            return RegistryBuildResult(None, tuple(diagnostics))

        registrations: dict[str, RegisteredConnector] = {}
        for candidate in candidates:
            connector_id = candidate.manifest.connector_id
            try:
                connector = candidate.factory()
            except Exception as exc:  # noqa: BLE001 - converted to diagnostics
                diagnostics.append(
                    RegistryDiagnostic(
                        severity=DiagnosticSeverity.ERROR,
                        code="factory_failed",
                        message=(
                            f"connector factory raised {type(exc).__name__}"
                        ),
                        connector_id=connector_id,
                        origin=candidate.origin,
                    )
                )
                continue
            if not isinstance(connector, SourceConnector):
                diagnostics.append(
                    RegistryDiagnostic(
                        severity=DiagnosticSeverity.ERROR,
                        code="invalid_connector_object",
                        message="factory result does not satisfy SourceConnector",
                        connector_id=connector_id,
                        origin=candidate.origin,
                    )
                )
                continue
            if connector.manifest != candidate.manifest:
                diagnostics.append(
                    RegistryDiagnostic(
                        severity=DiagnosticSeverity.ERROR,
                        code="manifest_implementation_mismatch",
                        message="factory manifest does not match candidate manifest",
                        connector_id=connector_id,
                        origin=candidate.origin,
                    )
                )
                continue

            keys = tuple(
                key
                for key in candidate.capability_keys
                if key.ref in negotiated[connector_id][1]
            )
            registrations[connector_id] = RegisteredConnector(
                manifest=candidate.manifest,
                connector=connector,
                negotiated_contract=negotiated[connector_id][0],
                capability_keys=keys,
                origin=candidate.origin,
                conformance_fingerprint=candidate.conformance_fingerprint,
            )
            diagnostics.append(
                RegistryDiagnostic(
                    severity=DiagnosticSeverity.INFO,
                    code="connector_registered",
                    message=(
                        f"registered {connector_id} with {len(keys)} capabilities"
                    ),
                    connector_id=connector_id,
                    origin=candidate.origin,
                )
            )

        final_diagnostics = tuple(diagnostics)
        if has_errors(final_diagnostics):
            return RegistryBuildResult(None, final_diagnostics)
        return RegistryBuildResult(
            ConnectorRegistry(registrations, final_diagnostics), final_diagnostics
        )

    def build(self) -> ConnectorRegistry:
        return self.build_result().require_registry()

    def _validate_static(
        self,
        candidates: tuple[ConnectorCandidate, ...],
    ) -> tuple[
        list[RegistryDiagnostic],
        dict[str, tuple[SemanticVersion, frozenset[CapabilityRef]]],
    ]:
        diagnostics: list[RegistryDiagnostic] = []
        negotiated: dict[
            str, tuple[SemanticVersion, frozenset[CapabilityRef]]
        ] = {}

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
                self._host.contract_versions
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

            if manifest.spec.runtime.isolation not in self._host.isolation_modes:
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

            fingerprint = candidate.conformance_fingerprint
            if (
                self._host.require_conformance_fingerprint
                and fingerprint is None
            ):
                diagnostics.append(
                    RegistryDiagnostic(
                        severity=DiagnosticSeverity.ERROR,
                        code="conformance_evidence_missing",
                        message="connector has no conformance fingerprint",
                        connector_id=connector_id,
                        origin=candidate.origin,
                    )
                )
            elif (
                fingerprint is not None
                and self._host.approved_conformance_fingerprints
                and fingerprint
                not in self._host.approved_conformance_fingerprints
            ):
                diagnostics.append(
                    RegistryDiagnostic(
                        severity=DiagnosticSeverity.ERROR,
                        code="conformance_fingerprint_unapproved",
                        message=(
                            "connector conformance fingerprint is not approved "
                            "by the host"
                        ),
                        connector_id=connector_id,
                        origin=candidate.origin,
                    )
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

            supported: set[CapabilityRef] = set()
            declarations = {
                declaration.ref: declaration
                for declaration in manifest.spec.capabilities
            }
            for ref in manifest.capability_refs:
                host_key = self._host.capability_catalog.get(ref)
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
                            connector_id=connector_id,
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
                            connector_id=connector_id,
                            capability=ref,
                            origin=candidate.origin,
                        )
                    )
                    continue
                supported.add(ref)

            for validator in self._validators:
                try:
                    diagnostics.extend(validator(manifest))
                except Exception as exc:  # noqa: BLE001 - diagnostic boundary
                    diagnostics.append(
                        RegistryDiagnostic(
                            severity=DiagnosticSeverity.ERROR,
                            code="manifest_validator_failed",
                            message=(
                                "manifest policy validator raised "
                                f"{type(exc).__name__}"
                            ),
                            connector_id=connector_id,
                            origin=candidate.origin,
                        )
                    )

            if contract is not None:
                negotiated[connector_id] = (contract, frozenset(supported))

        return diagnostics, negotiated


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


def _validate_authority(
    manifest: ConnectorManifest,
    context: BindingContext,
) -> None:
    if context.installation.connector_id != manifest.connector_id:
        raise BindingError(
            "installation connector ID does not match registry definition",
            details={
                "installation_connector_id": context.installation.connector_id,
                "registry_connector_id": manifest.connector_id,
            },
        )
    requested = manifest.spec.permissions
    missing_secrets = set(requested.secret_slots) - set(
        context.authority.secret_slots
    )
    missing_hosts = set(requested.outbound_hosts) - set(
        context.authority.outbound_hosts
    )
    missing_scopes = set(requested.requested_scopes) - set(
        context.authority.scopes
    )
    if missing_secrets or missing_hosts or missing_scopes:
        raise BindingError(
            "binding authority does not satisfy connector manifest permissions",
            details={
                "connector_id": manifest.connector_id,
                "missing_secret_slots": tuple(sorted(missing_secrets)),
                "missing_outbound_hosts": tuple(sorted(missing_hosts)),
                "missing_scopes": tuple(sorted(missing_scopes)),
            },
        )


def _registry_fingerprint(
    registrations: Mapping[str, RegisteredConnector],
) -> str:
    snapshot = [
        {
            "connector_id": registration.connector_id,
            "source": registration.source,
            "aliases": list(registration.manifest.metadata.aliases),
            "connector_version": registration.manifest.metadata.version,
            "contract_version": str(registration.negotiated_contract),
            "capabilities": [
                {"id": key.ref.id, "version": key.ref.version}
                for key in registration.capability_keys
            ],
            "origin": registration.origin,
            "conformance_fingerprint": registration.conformance_fingerprint,
        }
        for registration in (
            registrations[key] for key in sorted(registrations)
        )
    ]
    encoded = json.dumps(
        snapshot,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "ConnectorCandidate",
    "ConnectorDescription",
    "ConnectorFactory",
    "ConnectorRegistry",
    "ConnectorRegistryBuilder",
    "DEFAULT_HOST_COMPATIBILITY",
    "HostCompatibility",
    "ManifestValidator",
    "RegisteredConnector",
    "RegistryBuildResult",
    "RegistryHealth",
    "RegistryStatus",
]
