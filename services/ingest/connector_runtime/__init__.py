"""Host-side registry and binding support for Source Connectors."""

from services.ingest.connector_runtime.composition import (
    ConnectorRuntimeComposition,
    build_runtime_composition,
)
from services.ingest.connector_runtime.diagnostics import (
    DiagnosticSeverity,
    RegistryDiagnostic,
)
from services.ingest.connector_runtime.legacy import LegacyConnectorAdapter
from services.ingest.connector_runtime.policy import (
    AtomicRoutingPolicy,
    ExecutionMode,
    RouteDecision,
    RouteRequest,
    RoutingPolicy,
)
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
    "AtomicRoutingPolicy",
    "ConnectorCandidate",
    "ConnectorRegistry",
    "ConnectorRegistryBuilder",
    "ConnectorRuntimeComposition",
    "DEFAULT_HOST_COMPATIBILITY",
    "DiagnosticSeverity",
    "ExecutionMode",
    "HostCompatibility",
    "LegacyConnectorAdapter",
    "RegisteredConnector",
    "RegistryBuildResult",
    "RegistryDiagnostic",
    "RegistryHealth",
    "RegistryStatus",
    "RouteDecision",
    "RouteRequest",
    "RoutingPolicy",
    "build_runtime_composition",
]
