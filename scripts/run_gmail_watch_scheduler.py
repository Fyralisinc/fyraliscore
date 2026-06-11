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
from services.app.gateway.db_bootstrap import _register_codecs
from services.ingest.integrations.gmail.watch_scheduler import run_forever


async def _main() -> None:
    log = structlog.get_logger("dogfood.gmail_watch_scheduler")
    dsn = os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(
        dsn=dsn, min_size=2, max_size=8, init=_register_codecs,
    )
    register_pool("gmail_watch_scheduler", pool)
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)
    health_shutdown = start_worker_health("gmail_watch_scheduler", stop_event)

    log.info("gmail_watch_scheduler.starting")
    try:
        await run_forever(pool, stop_event=stop_event)
    finally:
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
