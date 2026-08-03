"""Connector runtime wiring shared by legacy workflow processes."""

from __future__ import annotations

import os
from dataclasses import dataclass

import httpx

from services.ingest.connector_platform.execution import LegacyExecutionRouter
from services.ingest.connector_platform.pilots import build_pilot_composition
from services.ingest.connector_platform.routing_config import parse_routing_policy
from services.ingest.connector_platform.startup import ROUTING_CONFIG_ENV
from services.ingest.connector_runtime.composition import ConnectorRuntimeComposition
from services.ingest.connector_runtime.host_services import HostServicesFactory
from services.ingest.connector_runtime.shadow import ShadowReportSink


@dataclass(frozen=True)
class WorkflowConnectorWiring:
    composition: ConnectorRuntimeComposition
    router: LegacyExecutionRouter
    http_client: httpx.AsyncClient

    async def close(self) -> None:
        await self.http_client.aclose()


def build_workflow_connector_wiring(
    *,
    routing_config: str | None = None,
    shadow_sink: ShadowReportSink | None = None,
) -> WorkflowConnectorWiring:
    raw_config = (
        os.environ.get(ROUTING_CONFIG_ENV)
        if routing_config is None
        else routing_config
    )
    composition = build_pilot_composition(parse_routing_policy(raw_config))
    client = httpx.AsyncClient(follow_redirects=False)
    router = LegacyExecutionRouter(
        composition,
        HostServicesFactory(http_client=client),
        shadow_sink=shadow_sink,
    )
    return WorkflowConnectorWiring(composition, router, client)


__all__ = ["WorkflowConnectorWiring", "build_workflow_connector_wiring"]
