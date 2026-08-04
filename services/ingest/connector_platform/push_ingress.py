"""Host-owned push trigger that invokes contract incremental polling."""

from __future__ import annotations

from typing import Any

import httpx

from services.ingest.connector_platform.authority_store import (
    PostgresAuthorityRepository,
)
from services.ingest.connector_platform.execution import ConnectorExecutionRouter
from services.ingest.connector_platform.production_host_services import (
    ProductionHostBackends,
    build_production_host_services_factory,
)
from services.ingest.source_contract.errors import SourceUnavailableError


async def execute_connector_push_poll(
    *,
    app_state: Any,
    source: str,
    installation_id: Any,
) -> tuple[int, bool]:
    runtime = getattr(app_state, "integration_runtime", None)
    composition = getattr(app_state, "source_connector_runtime", None)
    if runtime is None or composition is None:
        raise SourceUnavailableError("connector push runtime is unavailable")
    install = await runtime.pool.fetchrow(
        """
        SELECT *, TRUE AS enabled
          FROM source_connector_installations
         WHERE id = $1
           AND connector_id = $2
           AND desired_state = 'Ready'
           AND observed_phase IN ('Ready', 'Degraded')
           AND removed_at IS NULL
        """,
        installation_id,
        f"fyralis/{source}",
    )
    if install is None:
        raise SourceUnavailableError("push installation is unavailable")
    async with httpx.AsyncClient(follow_redirects=False) as client:
        evidence = getattr(app_state, "source_connector_rollout_evidence", None)
        host = build_production_host_services_factory(
            ProductionHostBackends(
                pool=runtime.pool,
                secret_store=runtime.secret_store,
                http_client=client,
                s3_raw_client=getattr(app_state, "s3_raw_client", None),
                kafka_producer=getattr(app_state, "kafka_producer", None),
                metric_incrementer=evidence.increment if evidence else None,
                metric_observer=evidence.observe if evidence else None,
            )
        )
        execution = ConnectorExecutionRouter(
            composition,
            host,
            authority_repository=PostgresAuthorityRepository(runtime.pool),
            require_durable_authority=True,
        )
        return await execution.poll_and_emit(source, install)


__all__ = ["execute_connector_push_poll"]
