"""Host-side registry and binding support for Source Connectors."""

from services.ingest.connector_runtime.diagnostics import (
    DiagnosticSeverity,
    RegistryDiagnostic,
)
from services.ingest.connector_runtime.legacy import LegacyConnectorAdapter
from services.ingest.connector_runtime.registry import (
    ConnectorCandidate,
    ConnectorRegistry,
    ConnectorRegistryBuilder,
    DEFAULT_HOST_COMPATIBILITY,
    HostCompatibility,
    RegisteredConnector,
    RegistryBuildResult,
    RegistryHealth,
    RegistryStatus,
)


__all__ = [
    "ConnectorCandidate",
    "ConnectorRegistry",
    "ConnectorRegistryBuilder",
    "DEFAULT_HOST_COMPATIBILITY",
    "DiagnosticSeverity",
    "HostCompatibility",
    "LegacyConnectorAdapter",
    "RegisteredConnector",
    "RegistryBuildResult",
    "RegistryDiagnostic",
    "RegistryHealth",
    "RegistryStatus",
]
