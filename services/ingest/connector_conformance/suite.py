"""Baseline manifest, activation, binding, and resolution conformance checks."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable

from services.ingest.connector_conformance.fakes import make_binding_context
from services.ingest.connector_conformance.models import (
    ConformanceCheck,
    ConformanceReport,
    ConformanceStatus,
)
from services.ingest.connector_runtime.diagnostics import (
    DiagnosticSeverity,
    RegistryDiagnostic,
)
from services.ingest.connector_runtime.registry import (
    DEFAULT_HOST_COMPATIBILITY,
    ConnectorCandidate,
    ConnectorRegistryBuilder,
    HostCompatibility,
    ManifestValidator,
)
from services.ingest.source_contract.connector import BindingContext
from services.ingest.source_contract.errors import ConnectorError


CONFORMANCE_SUITE_VERSION = "source-connector-v1alpha1/1"


class ConnectorConformanceSuite:
    """Exercise foundation invariants without touching production services."""

    def __init__(
        self,
        host: HostCompatibility = DEFAULT_HOST_COMPATIBILITY,
        *,
        validators: Iterable[ManifestValidator] = (),
    ) -> None:
        self._host = host
        self._validators = tuple(validators)

    def run(
        self,
        candidate: ConnectorCandidate,
        *,
        binding_context: BindingContext | None = None,
    ) -> ConformanceReport:
        result = ConnectorRegistryBuilder(
            self._host,
            validators=self._validators,
        ).add(candidate).build_result()
        checks = [_diagnostic_check(item) for item in result.diagnostics]

        if result.registry is not None:
            checks.append(
                ConformanceCheck(
                    name="registry.snapshot",
                    status=ConformanceStatus.PASSED,
                    message="candidate produced an immutable registry snapshot",
                )
            )
            context = binding_context or make_binding_context(candidate.manifest)
            try:
                binding = result.registry.resolve_for_install(context)
            except Exception as exc:  # noqa: BLE001 - reported as conformance
                failure = (
                    str(exc)
                    if isinstance(exc, ConnectorError)
                    else type(exc).__name__
                )
                checks.append(
                    ConformanceCheck(
                        name="connector.binding",
                        status=ConformanceStatus.FAILED,
                        message=(
                            f"binding failed: {type(exc).__name__}: {failure}"
                        ),
                    )
                )
            else:
                checks.append(
                    ConformanceCheck(
                        name="connector.binding",
                        status=ConformanceStatus.PASSED,
                        message="connector bound to the requested installation",
                    )
                )
                for key in result.registry.require(
                    candidate.manifest.connector_id
                ).capability_keys:
                    implementation = binding.capability(key)
                    status = (
                        ConformanceStatus.PASSED
                        if implementation is not None
                        else ConformanceStatus.FAILED
                    )
                    checks.append(
                        ConformanceCheck(
                            name=(
                                f"capability.{key.ref.id}.v{key.ref.version}"
                            ),
                            status=status,
                            message=(
                                "declared capability resolved from the binding"
                                if implementation is not None
                                else "declared capability was absent from the binding"
                            ),
                        )
                    )

        if not checks:
            checks.append(
                ConformanceCheck(
                    name="registry.validation",
                    status=ConformanceStatus.FAILED,
                    message="registry validation produced no result",
                )
            )

        fingerprint = _fingerprint(candidate, tuple(checks))
        return ConformanceReport(
            suite_version=CONFORMANCE_SUITE_VERSION,
            connector_id=candidate.manifest.connector_id,
            connector_version=candidate.manifest.metadata.version,
            fingerprint=fingerprint,
            checks=tuple(checks),
        )


def assert_connector_conforms(report: ConformanceReport) -> None:
    if report.passed:
        return
    failures = "; ".join(
        (
            f"{check.name}"
            f"[{check.diagnostic_code}]" if check.diagnostic_code else check.name
        )
        + f": {check.message}"
        for check in report.failures
    )
    raise AssertionError(
        f"connector {report.connector_id} failed conformance: {failures}"
    )


def _diagnostic_check(diagnostic: RegistryDiagnostic) -> ConformanceCheck:
    status = {
        DiagnosticSeverity.INFO: ConformanceStatus.PASSED,
        DiagnosticSeverity.WARNING: ConformanceStatus.WARNING,
        DiagnosticSeverity.ERROR: ConformanceStatus.FAILED,
    }[diagnostic.severity]
    return ConformanceCheck(
        name="registry.validation",
        status=status,
        message=diagnostic.message,
        diagnostic_code=diagnostic.code,
    )


def _fingerprint(
    candidate: ConnectorCandidate,
    checks: tuple[ConformanceCheck, ...],
) -> str:
    payload = {
        "suite_version": CONFORMANCE_SUITE_VERSION,
        "manifest": candidate.manifest.model_dump(mode="json", by_alias=True),
        "capabilities": [
            {
                "id": key.ref.id,
                "version": key.ref.version,
                "interface": (
                    f"{key.interface.__module__}.{key.interface.__qualname__}"
                ),
            }
            for key in candidate.capability_keys
        ],
        "checks": [check.model_dump(mode="json") for check in checks],
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "CONFORMANCE_SUITE_VERSION",
    "ConnectorConformanceSuite",
    "assert_connector_conforms",
]
