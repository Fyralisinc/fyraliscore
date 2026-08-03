"""Production platform composition for native Source Connectors."""

from services.ingest.connector_platform.pilots import (
    NOTION_CONNECTOR_ID,
    SLACK_CONNECTOR_ID,
    WHATSAPP_CONNECTOR_ID,
    build_fleet_candidates,
    build_fleet_composition,
    build_pilot_candidates,
    build_pilot_composition,
    build_runtime_candidates,
)

__all__ = [
    "NOTION_CONNECTOR_ID",
    "SLACK_CONNECTOR_ID",
    "WHATSAPP_CONNECTOR_ID",
    "build_fleet_candidates",
    "build_fleet_composition",
    "build_pilot_candidates",
    "build_pilot_composition",
    "build_runtime_candidates",
]
