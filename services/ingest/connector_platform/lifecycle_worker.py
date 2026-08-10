"""Dedicated continuous installation lifecycle controller process."""

from __future__ import annotations

import asyncio
import os

import httpx

from lib.shared.secrets import build_secret_store
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
from services.ingest.connector_platform.lifecycle_controller import (
    ContinuousInstallationController,
    PostgresInstallationLifecycleRepository,
)
from services.ingest.connector_platform.catalog import (
    build_connector_runtime,
    build_runtime_candidates,
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
from services.ingest.ingestion.observability import (
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)
from services.ingest.ingestion.workflows.runtime import make_workflow_pool


async def run_lifecycle_worker(stop_event: asyncio.Event | None = None) -> None:
    stop = stop_event or asyncio.Event()
    pool = await make_workflow_pool(os.environ["DATABASE_URL"])
    client = httpx.AsyncClient(follow_redirects=False)
    heartbeat = Heartbeat()
    metrics: dict[str, float] = {
        "source_connector_lifecycle.control_refreshes": 0,
        "source_connector_lifecycle.control_failures": 0,
        "source_connector_lifecycle.admitted": 0,
        "source_connector_lifecycle.quarantined": 0,
    }
    health = start_health_server(get_metrics=lambda: dict(metrics), heartbeat=heartbeat)
    try:
        secret_store = build_secret_store(pool)
        composition = build_connector_runtime()
        host_services = build_production_host_services_factory(
            ProductionHostBackends(
                pool=pool,
                secret_store=secret_store,
                http_client=client,
                callback_base_url=os.environ.get("CONNECTOR_CALLBACK_BASE_URL"),
            )
        )
        artifact_controller = ArtifactAdmissionController(
            PostgresArtifactRepository(pool),
            composition.routing,
            build_runtime_candidates(),
            ArtifactAdmissionSettings.from_env(),
        )
        artifact_admission = await artifact_controller.refresh()
        active_revision: list[int | None] = [None]
        rollout_repository = PostgresRolloutRepository(pool)
        active = await rollout_repository.load_active()
        if active is not None:
            active_revision[0] = active.revision
        evidence_sink = PostgresRolloutEvidenceSink(pool, lambda: active_revision[0])
        controller = ContinuousInstallationController(
            composition.registry,
            PostgresAuthorityRepository(pool),
            PostgresInstallationLifecycleRepository(pool),
            host_services,
            admitted_connector_ids=frozenset(
                candidate.manifest.connector_id
                for candidate in artifact_admission.candidates
            ),
            evidence_sink=evidence_sink,
        )

        async def refresh_control_plane() -> None:
            interval = float(os.environ.get("CONNECTOR_CONTROL_REFRESH_SECONDS", "5"))
            prune_counter = 0
            while not stop.is_set():
                try:
                    revision = await rollout_repository.load_active()
                    active_revision[0] = revision.revision if revision else None
                    admission = await artifact_controller.refresh()
                    admitted = frozenset(
                        candidate.manifest.connector_id
                        for candidate in admission.candidates
                    )
                    controller.replace_admitted_connectors(admitted)
                    metrics["source_connector_lifecycle.admitted"] = float(
                        len(admitted)
                    )
                    metrics["source_connector_lifecycle.quarantined"] = float(
                        len(admission.quarantined)
                    )
                    metrics["source_connector_lifecycle.control_refreshes"] += 1
                    prune_counter += 1
                    if prune_counter >= 720:
                        await evidence_sink.prune()
                        prune_counter = 0
                except Exception:  # noqa: BLE001 - control loop must keep refreshing
                    metrics["source_connector_lifecycle.control_failures"] += 1
                heartbeat.touch()
                try:
                    await asyncio.wait_for(stop.wait(), timeout=interval)
                except TimeoutError:
                    continue

        await asyncio.gather(
            controller.run(
                stop,
                interval_seconds=float(
                    os.environ.get("CONNECTOR_LIFECYCLE_INTERVAL_SECONDS", "5")
                ),
            ),
            refresh_control_plane(),
            run_heartbeat_ticker(heartbeat, stop),
        )
        await evidence_sink.flush()
    finally:
        if health is not None:
            health.shutdown()
            health.server_close()
        await client.aclose()
        await pool.close()


def main() -> None:
    asyncio.run(run_lifecycle_worker())


if __name__ == "__main__":
    main()


__all__ = ["main", "run_lifecycle_worker"]
