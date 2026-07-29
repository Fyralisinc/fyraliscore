"""Launcher: gmail watch-renewal scheduler.

Mirrors scripts/run_think_worker.py shape.
"""
from __future__ import annotations

import asyncio
import os

import asyncpg
import structlog

from worker_observability import (
    install_signal_handlers,
    register_pool,
    start_worker_health,
)
from lib.shared.db import asyncpg_pool_runtime_kwargs, positive_int_env
from services.app.gateway.db_bootstrap import _register_codecs
from services.ingest.integrations.gmail.watch_scheduler import run_forever
from services.ingest.integrations.provider_transport_runtime import (
    close_provider_transport_runtime,
    get_provider_transport_runtime,
)


async def _main() -> None:
    from services.ingest.source_contract.runtime import (
        validate_live_worker_startup,
    )

    validate_live_worker_startup("gmail", "gmail_watch_scheduler")
    log = structlog.get_logger("dogfood.gmail_watch_scheduler")
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
    register_pool("gmail_watch_scheduler", pool)
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)
    health_shutdown = start_worker_health("gmail_watch_scheduler", stop_event)

    provider_runtime = None
    try:
        # Watch creation may mint a DWD credential and always performs a
        # provider request. Treat missing distributed quota configuration as a
        # process-start failure, never as a per-watch manual-repair outcome.
        provider_runtime = get_provider_transport_runtime(required=True)
        log.info("gmail_watch_scheduler.starting")
        await run_forever(pool, stop_event=stop_event)
    finally:
        await close_provider_transport_runtime(provider_runtime)
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
