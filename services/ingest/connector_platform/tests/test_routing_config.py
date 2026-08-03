from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

from services.ingest.connector_platform.startup import wire_source_connector_runtime
from services.ingest.connector_runtime.policy import (
    ExecutionMode,
    RouteRequest,
)


def test_startup_publishes_registry_alongside_legacy_and_configures_scopes() -> None:
    tenant_id = uuid4()
    state = SimpleNamespace(legacy_dispatch="unchanged")
    config = {
        "revision": 3,
        "global": "shadow",
        "connectors": {"fyralis/notion": "connector"},
        "tenant_capabilities": [
            {
                "tenant_id": str(tenant_id),
                "connector_id": "fyralis/slack",
                "capability": "semantic.identity",
                "mode": "legacy",
            }
        ],
    }

    wiring = wire_source_connector_runtime(
        state, routing_config=json.dumps(config)
    )

    assert state.legacy_dispatch == "unchanged"
    assert len(state.source_connector_registry.connector_ids()) == 26
    identity = RouteRequest(
        tenant_id,
        "fyralis/slack",
        "slack",
        "semantic.identity",
    )
    notion = RouteRequest(
        tenant_id,
        "fyralis/notion",
        "notion",
        "ingestion.incremental_poll",
    )
    assert wiring.composition.routing.resolve(identity).mode is ExecutionMode.LEGACY
    assert wiring.composition.routing.resolve(notion).mode is ExecutionMode.CONNECTOR


def test_configuration_only_rollback_is_immediate() -> None:
    state = SimpleNamespace()
    wiring = wire_source_connector_runtime(
        state,
        routing_config='{"revision": 1, "global": "connector"}',
    )
    request = RouteRequest(
        uuid4(), "fyralis/slack", "slack", "ingestion.historical_pull"
    )
    assert wiring.composition.routing.resolve(request).mode is ExecutionMode.CONNECTOR

    wiring.configuration.rollback()

    assert wiring.composition.routing.resolve(request).mode is ExecutionMode.LEGACY
