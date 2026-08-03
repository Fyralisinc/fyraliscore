"""Gateway/service startup wiring for the side-by-side connector runtime."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from services.ingest.connector_platform.pilots import build_pilot_composition
from services.ingest.connector_platform.routing_config import (
    RoutingConfigurationController,
    parse_routing_policy,
)
from services.ingest.connector_runtime.composition import ConnectorRuntimeComposition


ROUTING_CONFIG_ENV = "SOURCE_CONNECTOR_ROUTING_JSON"


@dataclass(frozen=True)
class SourceConnectorRuntimeWiring:
    composition: ConnectorRuntimeComposition
    configuration: RoutingConfigurationController


def wire_source_connector_runtime(
    state: Any,
    *,
    routing_config: str | None = None,
) -> SourceConnectorRuntimeWiring:
    """Construct and publish one immutable snapshot alongside legacy state."""

    raw_config = (
        os.environ.get(ROUTING_CONFIG_ENV)
        if routing_config is None
        else routing_config
    )
    policy = parse_routing_policy(raw_config)
    composition = build_pilot_composition(policy)
    controller = RoutingConfigurationController(composition.routing)
    wiring = SourceConnectorRuntimeWiring(composition, controller)
    state.source_connector_runtime = composition
    state.source_connector_registry = composition.registry
    state.source_connector_routing = controller
    return wiring


__all__ = [
    "ROUTING_CONFIG_ENV",
    "SourceConnectorRuntimeWiring",
    "wire_source_connector_runtime",
]
