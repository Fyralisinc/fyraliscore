"""Launcher: Google Drive push-channel watch scheduler.

Registers + renews changes.watch channels (the gmail_watch_scheduler analog).
Idles when GOOGLE_PUSH_WEBHOOK_BASE is unset (the live poller is then the
liveness path). Mirrors scripts/run_gmail_watch_scheduler.py shape.
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
from services.ingest.integrations.google_drive.watch import run_forever


async def _main() -> None:
    log = structlog.get_logger("dogfood.google_drive_watch_scheduler")
    dsn = os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(
        dsn=dsn, min_size=2, max_size=8, init=_register_codecs,
    )
    register_pool("google_drive_watch_scheduler", pool)
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)
    health_shutdown = start_worker_health(
        "google_drive_watch_scheduler", stop_event
    )

    log.info("google_drive_watch_scheduler.starting")
    try:
        await run_forever(pool, stop_event=stop_event)
    finally:
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
