"""Production launcher for contract-derived credential renewal.

The process has no source list.  At startup it validates every source that
declares the ``credential_renewal`` worker, then the scheduler derives exact
tenant/installation work from the same contract.  Provider calls remain inside
the source-owned bounded invokers and their ProviderTransport bindings.
"""
from __future__ import annotations

import asyncio
import os

import asyncpg
import httpx
import structlog

from worker_observability import (
    install_signal_handlers,
    register_pool,
    start_worker_health,
)
from lib.shared.db import asyncpg_pool_runtime_kwargs, positive_int_env
from lib.shared.secrets import build_secret_store
from services.app.gateway.db_bootstrap import _register_codecs
from services.ingest.ingestion.workflows.credential_renewal_scheduler import (
    CredentialRenewalSchedulerConfig,
    WORKER_COMPONENT_ID,
    credential_renewal_sources,
    run_forever,
)
from services.ingest.integrations.provider_transport_runtime import (
    close_provider_transport_runtime,
    get_provider_transport_runtime,
)
from services.ingest.source_contract.runtime import validate_live_worker_startup


async def _main() -> None:
    sources = credential_renewal_sources()
    for source in sources:
        validate_live_worker_startup(source.source_id, WORKER_COMPONENT_ID)

    log = structlog.get_logger("fyralis.credential_renewal_scheduler")
    dsn = os.environ["DATABASE_URL"]
    pool_max = positive_int_env("SOURCE_SCHEDULER_POSTGRES_POOL_SIZE", default=8)
    runtime_kwargs = asyncpg_pool_runtime_kwargs(
        dsn=dsn,
        process_env_var="SOURCE_SCHEDULER_POSTGRES_PGBOUNCER_COMPATIBLE",
    )
    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,
        max_size=pool_max,
        init=_register_codecs,
        **runtime_kwargs,
    )
    register_pool("credential_renewal_scheduler", pool)
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)
    health_shutdown = start_worker_health("credential_renewal_scheduler", stop_event)

    provider_runtime = None
    try:
        # Credential renewal always has outbound requests.  Refuse to launch
        # without the shared distributed quota/runtime binding even outside a
        # web gateway process.
        provider_runtime = get_provider_transport_runtime(required=True)
        secret_store = build_secret_store(pool)
        config = CredentialRenewalSchedulerConfig(
            batch_size=positive_int_env("CREDENTIAL_RENEWAL_BATCH_SIZE", default=64),
            candidate_scan_limit=positive_int_env(
                "CREDENTIAL_RENEWAL_CANDIDATE_SCAN_LIMIT",
                default=512,
            ),
            max_concurrency=positive_int_env(
                "CREDENTIAL_RENEWAL_MAX_CONCURRENCY",
                default=8,
            ),
            lease_timeout_seconds=float(
                os.environ.get(
                    "CREDENTIAL_RENEWAL_LEASE_TIMEOUT_SECONDS",
                    "60",
                ),
            ),
            instance_name=os.environ.get(
                "CREDENTIAL_RENEWAL_INSTANCE",
                "credential_renewal_scheduler",
            ),
        )
        log.info(
            "credential_renewal_scheduler.starting",
            source_count=len(sources),
            cadence_seconds=min(
                source.renewal.cadence_seconds
                for source in sources
                if source.renewal is not None
            ),
        )
        async with httpx.AsyncClient(timeout=30.0) as http:
            await run_forever(
                pool,
                secret_store=secret_store,
                http=http,
                config=config,
                stop_event=stop_event,
            )
    finally:
        await close_provider_transport_runtime(provider_runtime)
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
