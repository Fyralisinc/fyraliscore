from __future__ import annotations

from services.ingest.connector_platform.pilots import (
    NOTION_CONNECTOR_ID,
    SLACK_CONNECTOR_ID,
    build_pilot_composition,
)
from services.ingest.connector_runtime.policy import ExecutionMode, RouteRequest
from services.ingest.source_contract.capabilities import (
    HISTORICAL_PULL_V1,
    INCREMENTAL_POLL_V1,
    WEBHOOK_V1,
)
from uuid import uuid4


def test_pilot_composition_is_immutable_and_native_pilots_are_authoritative() -> None:
    composition = build_pilot_composition()

    assert composition.registry.connector_ids() == (
        NOTION_CONNECTOR_ID,
        SLACK_CONNECTOR_ID,
    )
    assert composition.registry.list_by_capability(WEBHOOK_V1.ref)[0].source == "slack"
    assert (
        composition.registry.list_by_capability(INCREMENTAL_POLL_V1.ref)[0].source
        == "notion"
    )
    assert len(composition.registry.list_by_capability(HISTORICAL_PULL_V1.ref)) == 2
    assert composition.routing.resolve(
        RouteRequest(
            tenant_id=uuid4(),
            connector_id=SLACK_CONNECTOR_ID,
            source="slack",
            capability=HISTORICAL_PULL_V1.ref.id,
        )
    ).mode is ExecutionMode.CONNECTOR
