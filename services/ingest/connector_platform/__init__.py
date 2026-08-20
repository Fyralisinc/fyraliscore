"""Production platform composition for native Source Connectors."""

from services.ingest.connector_platform.catalog import (
    build_connector_runtime,
    build_runtime_candidates,
)

__all__ = ["build_connector_runtime", "build_runtime_candidates"]
