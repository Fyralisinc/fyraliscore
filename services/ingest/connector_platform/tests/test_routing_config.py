from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.ingest.connector_platform.routing_config import parse_routing_policy
from services.ingest.connector_platform.startup import wire_source_connector_runtime
from services.ingest.connector_runtime.policy import ExecutionMode, RouteRequest


def test_startup_publishes_complete_contract_registry() -> None:
    state = SimpleNamespace()
    wiring = wire_source_connector_runtime(
        state, routing_config='{"revision":3,"global":"connector"}'
    )
    assert len(state.source_connector_registry.connector_ids()) == 26
    decision = wiring.composition.routing.resolve(
        RouteRequest(uuid4(), "fyralis/slack", "slack", "semantic.identity")
    )
    assert decision.mode is ExecutionMode.CONNECTOR
    assert decision.policy_revision == 3


@pytest.mark.parametrize(
    "value",
    [
        '{"global":"legacy"}',
        '{"global":"shadow"}',
        '{"connectors":{"fyralis/slack":"connector"}}',
        '{"tenant_capabilities":[]}',
    ],
)
def test_retired_source_routing_shapes_are_rejected(value: str) -> None:
    with pytest.raises(ValueError):
        parse_routing_policy(value)
