"""Explicit, deterministic, immutable Source Connector registry."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from services.ingest.connector_runtime.binding import RegisteredConnector
from services.ingest.connector_runtime.definitions import (
    DEFAULT_HOST_COMPATIBILITY,
    ConnectorCandidate,
    ConnectorDescription,
    ConnectorFactory,
    HostCompatibility,
    ManifestValidator,
)
from services.ingest.connector_runtime.diagnostics import (
    DiagnosticSeverity,
    RegistryDiagnostic,
    has_errors,
)
from services.ingest.connector_runtime.health import (
    RegistryHealth,
    RegistryStatus,
    registry_fingerprint,
)
from services.ingest.connector_runtime.validation import validate_candidates
from services.ingest.source_contract.connector import (
    BindingContext,
    SourceConnector,
    StaticBoundConnector,
)
from services.ingest.source_contract.errors import (
    ConnectorNotFoundError,
    RegistryBuildError,
)
from services.ingest.source_contract.manifest import CapabilityRef


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
        self._fingerprint = registry_fingerprint(by_id)

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
        diagnostics, negotiated = validate_candidates(
            candidates,
            self._host,
            self._validators,
        )
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
