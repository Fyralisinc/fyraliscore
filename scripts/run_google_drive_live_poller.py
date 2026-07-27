"""Launcher: Google Drive live delta poller.

Near-real-time safety-net poller (the gmail_history_poller analog). Mirrors
scripts/run_gmail_history_poller.py shape.
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
from services.ingest.integrations.google_drive.live_poller import run_forever


async def _main() -> None:
    from services.ingest.source_contract.runtime import (
        validate_live_worker_startup,
    )

    validate_live_worker_startup("google_drive", "google_drive_live_poller")
    log = structlog.get_logger("dogfood.google_drive_live_poller")
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
    register_pool("google_drive_live_poller", pool)
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)
    health_shutdown = start_worker_health("google_drive_live_poller", stop_event)

    log.info("google_drive_live_poller.starting")
    try:
        await run_forever(pool, stop_event=stop_event)
    finally:
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
