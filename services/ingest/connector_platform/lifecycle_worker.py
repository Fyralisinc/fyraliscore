"""Dedicated continuous installation lifecycle controller process."""

from __future__ import annotations

import asyncio
import os

import httpx

from lib.shared.secrets import build_secret_store
from services.ingest.connector_platform.authority_store import (
    PostgresAuthorityRepository,
)
from services.ingest.connector_platform.lifecycle_controller import (
    ContinuousInstallationController,
    PostgresInstallationLifecycleRepository,
)
from services.ingest.connector_platform.pilots import build_pilot_composition
from services.ingest.connector_platform.production_host_services import (
    ProductionHostBackends,
    build_production_host_services_factory,
)
from services.ingest.ingestion.workflows.runtime import make_workflow_pool


async def run_lifecycle_worker(stop_event: asyncio.Event | None = None) -> None:
    stop = stop_event or asyncio.Event()
    pool = await make_workflow_pool(os.environ["DATABASE_URL"])
    client = httpx.AsyncClient(follow_redirects=False)
    try:
        secret_store = build_secret_store(pool)
        composition = build_pilot_composition()
        host_services = build_production_host_services_factory(
            ProductionHostBackends(
                pool=pool,
                secret_store=secret_store,
                http_client=client,
                callback_base_url=os.environ.get("CONNECTOR_CALLBACK_BASE_URL"),
            )
        )
        controller = ContinuousInstallationController(
            composition.registry,
            PostgresAuthorityRepository(pool),
            PostgresInstallationLifecycleRepository(pool),
            host_services,
        )
        await controller.run(
            stop,
            interval_seconds=float(
                os.environ.get("CONNECTOR_LIFECYCLE_INTERVAL_SECONDS", "5")
            ),
        )
    finally:
        await client.aclose()
        await pool.close()


def main() -> None:
    asyncio.run(run_lifecycle_worker())


if __name__ == "__main__":
    main()


__all__ = ["main", "run_lifecycle_worker"]
