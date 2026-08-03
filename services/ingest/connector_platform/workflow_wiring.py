"""Connector runtime wiring shared by legacy workflow processes."""

from __future__ import annotations

import os
from dataclasses import dataclass

import httpx

from services.ingest.connector_platform.execution import LegacyExecutionRouter
from services.ingest.connector_platform.authority_store import (
    PostgresAuthorityRepository,
)
from services.ingest.connector_platform.pilots import (
    build_pilot_composition,
    default_migrated_routing_policy,
)
from services.ingest.connector_platform.routing_config import (
    RoutingConfigurationController,
    parse_routing_policy,
)
from services.ingest.connector_platform.startup import ROUTING_CONFIG_ENV
from services.ingest.connector_platform.production_host_services import (
    ProductionHostBackends,
    build_production_host_services_factory,
)
from services.ingest.connector_platform.rollout_store import (
    PostgresRolloutRepository,
)
from services.ingest.connector_runtime.composition import ConnectorRuntimeComposition
from services.ingest.connector_runtime.host_services import HostServicesFactory
from services.ingest.connector_runtime.shadow import ShadowReportSink
from services.ingest.connector_runtime.rollout import FleetRoutingController


@dataclass(frozen=True)
class WorkflowConnectorWiring:
    composition: ConnectorRuntimeComposition
    router: LegacyExecutionRouter
    http_client: httpx.AsyncClient
    rollout: FleetRoutingController | None = None

    async def refresh_routing(self) -> None:
        if self.rollout is not None:
            await self.rollout.refresh_once()

    async def watch_routing(self, stop_event: object) -> None:
        if self.rollout is not None:
            await self.rollout.run(stop_event)  # type: ignore[arg-type]

    async def close(self) -> None:
        await self.http_client.aclose()


def build_workflow_connector_wiring(
    *,
    routing_config: str | None = None,
    shadow_sink: ShadowReportSink | None = None,
    pool: object | None = None,
    secret_store: object | None = None,
    s3_raw_client: object | None = None,
    kafka_producer: object | None = None,
) -> WorkflowConnectorWiring:
    raw_config = (
        os.environ.get(ROUTING_CONFIG_ENV)
        if routing_config is None
        else routing_config
    )
    policy = (
        parse_routing_policy(raw_config)
        if raw_config
        else default_migrated_routing_policy()
    )
    composition = build_pilot_composition(policy)
    client = httpx.AsyncClient(follow_redirects=False)
    authority_repository = None
    host_services = HostServicesFactory(http_client=client)
    if pool is not None:
        if secret_store is None:
            from lib.shared.secrets import build_secret_store

            secret_store = build_secret_store(pool)  # type: ignore[arg-type]
        authority_repository = PostgresAuthorityRepository(pool)
        host_services = build_production_host_services_factory(
            ProductionHostBackends(
                pool=pool,
                secret_store=secret_store,  # type: ignore[arg-type]
                http_client=client,
                s3_raw_client=s3_raw_client,
                kafka_producer=kafka_producer,
                callback_base_url=os.environ.get("CONNECTOR_CALLBACK_BASE_URL"),
            )
        )
    router = LegacyExecutionRouter(
        composition,
        host_services,
        shadow_sink=shadow_sink,
        authority_repository=authority_repository,
        require_durable_authority=authority_repository is not None,
    )
    rollout = None
    if pool is not None:
        rollout_repository = PostgresRolloutRepository(pool)
        rollout = FleetRoutingController(
            rollout_repository,
            RoutingConfigurationController(composition.routing),
            actor=f"workflow:{os.getpid()}",
        )
    return WorkflowConnectorWiring(composition, router, client, rollout)


__all__ = ["WorkflowConnectorWiring", "build_workflow_connector_wiring"]
