"""Gateway/service startup wiring for the side-by-side connector runtime."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any

from services.ingest.connector_platform.pilots import (
    build_pilot_composition,
    build_runtime_candidates,
    default_migrated_routing_policy,
)
from services.ingest.connector_platform.artifact_store import (
    PostgresArtifactRepository,
)
from services.ingest.connector_platform.deployment import (
    ArtifactAdmissionController,
    ArtifactAdmissionSettings,
)
from services.ingest.connector_platform.operational_health import (
    PostgresConnectorHealthReader,
)
from services.ingest.connector_platform.routing_config import (
    RoutingConfigurationController,
    parse_routing_policy,
)
from services.ingest.connector_platform.rollout_store import (
    PostgresRolloutRepository,
)
from services.ingest.connector_platform.rollout_evidence import (
    PostgresRolloutEvidenceSink,
)
from services.ingest.connector_runtime.composition import ConnectorRuntimeComposition
from services.ingest.connector_runtime.rollout import FleetRoutingController


ROUTING_CONFIG_ENV = "SOURCE_CONNECTOR_ROUTING_JSON"


@dataclass(frozen=True)
class SourceConnectorRuntimeWiring:
    composition: ConnectorRuntimeComposition
    configuration: RoutingConfigurationController
    rollout: FleetRoutingController | None = None
    artifact_admission: ArtifactAdmissionController | None = None
    evidence_sink: PostgresRolloutEvidenceSink | None = None

    async def refresh_routing(self) -> None:
        if self.rollout is not None:
            await self.rollout.evaluate_once()

    async def refresh_artifact_admission(self) -> tuple[int, int] | None:
        if self.artifact_admission is None:
            return None
        admission = await self.artifact_admission.refresh()
        return len(admission.candidates), len(admission.quarantined)

    async def run_control_loop(
        self, stop_event: asyncio.Event, *, interval_seconds: float = 5.0
    ) -> None:
        """Continuously propagate rollout and artifact quarantine state."""

        while not stop_event.is_set():
            await self.refresh_routing()
            await self.refresh_artifact_admission()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue

    async def close(self) -> None:
        if self.evidence_sink is not None:
            await self.evidence_sink.flush()


def wire_source_connector_runtime(
    state: Any,
    *,
    routing_config: str | None = None,
    pool: object | None = None,
    actor: str = "gateway",
) -> SourceConnectorRuntimeWiring:
    """Construct and publish one immutable snapshot alongside legacy state."""

    raw_config = (
        os.environ.get(ROUTING_CONFIG_ENV) if routing_config is None else routing_config
    )
    policy = (
        parse_routing_policy(raw_config)
        if raw_config
        else default_migrated_routing_policy()
    )
    composition = build_pilot_composition(policy)
    controller = RoutingConfigurationController(composition.routing)
    rollout = None
    artifact_admission = None
    evidence_sink = None
    if pool is not None and hasattr(pool, "fetchrow"):
        repository = PostgresRolloutRepository(pool)
        rollout = FleetRoutingController(
            repository,
            controller,
            actor=actor,
            metric_reader=repository.read_metrics,
        )
    if pool is not None and hasattr(pool, "fetch") and hasattr(pool, "execute"):
        evidence_sink = PostgresRolloutEvidenceSink(
            pool,
            lambda: rollout.active_revision if rollout is not None else None,
        )
        artifact_admission = ArtifactAdmissionController(
            PostgresArtifactRepository(pool),
            composition.routing,
            build_runtime_candidates(),
            ArtifactAdmissionSettings.from_env(),
        )
    wiring = SourceConnectorRuntimeWiring(
        composition,
        controller,
        rollout,
        artifact_admission,
        evidence_sink,
    )
    state.source_connector_runtime = composition
    state.source_connector_registry = composition.registry
    state.source_connector_routing = controller
    state.source_connector_rollout_evidence = evidence_sink
    if pool is not None and hasattr(pool, "fetch"):
        state.source_connector_health_reader = PostgresConnectorHealthReader(
            pool,
            composition,
        )
    return wiring


__all__ = [
    "ROUTING_CONFIG_ENV",
    "SourceConnectorRuntimeWiring",
    "wire_source_connector_runtime",
]
