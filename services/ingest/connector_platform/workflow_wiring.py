"""Contract-only connector runtime wiring shared by ingestion workflows."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

import httpx

from services.ingest.connector_platform.artifact_store import (
    PostgresArtifactRepository,
)
from services.ingest.connector_platform.authority_store import (
    PostgresAuthorityRepository,
)
from services.ingest.connector_platform.deployment import (
    ArtifactAdmissionController,
    ArtifactAdmissionSettings,
)
from services.ingest.connector_platform.execution import ConnectorExecutionRouter
from services.ingest.connector_platform.catalog import (
    build_connector_runtime,
    build_runtime_candidates,
    default_routing_policy,
)
from services.ingest.connector_platform.production_host_services import (
    ProductionHostBackends,
    build_production_host_services_factory,
)
from services.ingest.connector_platform.rollout_evidence import (
    PostgresRolloutEvidenceSink,
)
from services.ingest.connector_platform.rollout_store import (
    PostgresRolloutRepository,
)
from services.ingest.connector_platform.routing_config import (
    RoutingConfigurationController,
    parse_routing_policy,
)
from services.ingest.connector_platform.startup import ROUTING_CONFIG_ENV
from services.ingest.connector_runtime.composition import ConnectorRuntimeComposition
from services.ingest.connector_runtime.host_services import HostServicesFactory
from services.ingest.connector_runtime.rollout import FleetRoutingController


@dataclass(frozen=True)
class WorkflowConnectorWiring:
    composition: ConnectorRuntimeComposition
    router: ConnectorExecutionRouter
    http_client: httpx.AsyncClient
    rollout: FleetRoutingController | None = None
    artifact_admission: ArtifactAdmissionController | None = None
    evidence_sink: PostgresRolloutEvidenceSink | None = None

    async def refresh_routing(self) -> None:
        if self.rollout is not None:
            await self.rollout.refresh_once()
        if self.artifact_admission is not None:
            await self.artifact_admission.refresh()

    async def watch_routing(self, stop_event: object) -> None:
        while not stop_event.is_set():  # type: ignore[union-attr]
            await self.refresh_routing()
            try:
                await asyncio.wait_for(
                    stop_event.wait(),  # type: ignore[union-attr]
                    timeout=float(
                        os.environ.get("CONNECTOR_CONTROL_REFRESH_SECONDS", "5")
                    ),
                )
            except TimeoutError:
                continue

    async def close(self) -> None:
        if self.evidence_sink is not None:
            await self.evidence_sink.flush()
        await self.http_client.aclose()


def build_workflow_connector_wiring(
    *,
    routing_config: str | None = None,
    pool: object | None = None,
    secret_store: object | None = None,
    s3_raw_client: object | None = None,
    kafka_producer: object | None = None,
) -> WorkflowConnectorWiring:
    raw_config = (
        os.environ.get(ROUTING_CONFIG_ENV) if routing_config is None else routing_config
    )
    policy = (
        parse_routing_policy(raw_config)
        if raw_config
        else default_routing_policy()
    )
    composition = build_connector_runtime(policy)
    admission_settings = ArtifactAdmissionSettings.from_env()
    if pool is None and admission_settings.require_signed:
        composition.routing.replace_quarantine(
            {
                candidate.manifest.connector_id: (
                    "durable artifact admission is unavailable in this process"
                )
                for candidate in build_runtime_candidates()
                if candidate.origin.startswith("first-party-native:")
            }
        )
    client = httpx.AsyncClient(follow_redirects=False)
    authority_repository = None
    evidence_sink = None
    host_services = HostServicesFactory(http_client=client)
    if pool is not None:
        if secret_store is None:
            from lib.shared.secrets import build_secret_store

            secret_store = build_secret_store(pool)  # type: ignore[arg-type]
        authority_repository = PostgresAuthorityRepository(pool)
        evidence_sink = PostgresRolloutEvidenceSink(
            pool,
            lambda: rollout.active_revision if rollout is not None else None,
        )
        host_services = build_production_host_services_factory(
            ProductionHostBackends(
                pool=pool,
                secret_store=secret_store,  # type: ignore[arg-type]
                http_client=client,
                s3_raw_client=s3_raw_client,
                kafka_producer=kafka_producer,
                callback_base_url=os.environ.get("CONNECTOR_CALLBACK_BASE_URL"),
                metric_incrementer=evidence_sink.increment,
                metric_observer=evidence_sink.observe,
            )
        )
    router = ConnectorExecutionRouter(
        composition,
        host_services,
        authority_repository=authority_repository,
        require_durable_authority=authority_repository is not None,
    )
    rollout = None
    artifact_admission = None
    if pool is not None:
        rollout_repository = PostgresRolloutRepository(pool)
        rollout = FleetRoutingController(
            rollout_repository,
            RoutingConfigurationController(composition.routing),
            actor=f"workflow:{os.getpid()}",
            metric_reader=rollout_repository.read_metrics,
        )
        artifact_admission = ArtifactAdmissionController(
            PostgresArtifactRepository(pool),
            composition.routing,
            build_runtime_candidates(),
            admission_settings,
        )
    return WorkflowConnectorWiring(
        composition,
        router,
        client,
        rollout,
        artifact_admission,
        evidence_sink,
    )


__all__ = ["WorkflowConnectorWiring", "build_workflow_connector_wiring"]
