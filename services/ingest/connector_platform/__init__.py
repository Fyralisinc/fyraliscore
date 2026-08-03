"""Platform composition and compatibility adapters for Source Connectors."""

from services.ingest.connector_platform.pilots import (
    NOTION_CONNECTOR_ID,
    SLACK_CONNECTOR_ID,
    build_pilot_candidates,
    build_pilot_composition,
)


__all__ = [
    "NOTION_CONNECTOR_ID",
    "SLACK_CONNECTOR_ID",
    "build_pilot_candidates",
    "build_pilot_composition",
]
