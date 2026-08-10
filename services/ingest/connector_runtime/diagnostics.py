"""Structured diagnostics emitted while building and inspecting a registry."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from services.ingest.source_contract.manifest import CapabilityRef


class DiagnosticSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True)
class RegistryDiagnostic:
    severity: DiagnosticSeverity
    code: str
    message: str
    connector_id: str | None = None
    capability: CapabilityRef | None = None
    origin: str | None = None


def has_errors(diagnostics: tuple[RegistryDiagnostic, ...]) -> bool:
    return any(
        diagnostic.severity is DiagnosticSeverity.ERROR
        for diagnostic in diagnostics
    )


__all__ = ["DiagnosticSeverity", "RegistryDiagnostic", "has_errors"]
