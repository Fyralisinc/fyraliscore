"""Reusable conformance harness for Source Connector candidates."""

from services.ingest.connector_conformance.fakes import (
    FakeHostEnvironment,
    make_binding_context,
)
from services.ingest.connector_conformance.models import (
    ConformanceCheck,
    ConformanceReport,
    ConformanceStatus,
)
from services.ingest.connector_conformance.suite import (
    CONFORMANCE_SUITE_VERSION,
    ConnectorConformanceSuite,
    assert_connector_conforms,
)


__all__ = [
    "CONFORMANCE_SUITE_VERSION",
    "ConformanceCheck",
    "ConformanceReport",
    "ConformanceStatus",
    "ConnectorConformanceSuite",
    "FakeHostEnvironment",
    "assert_connector_conforms",
    "make_binding_context",
]
