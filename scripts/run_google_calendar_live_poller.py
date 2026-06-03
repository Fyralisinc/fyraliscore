"""Launcher: Google Calendar live delta poller.

Near-real-time safety-net poller (the gmail_history_poller analog). Mirrors
scripts/run_gmail_history_poller.py shape.
"""
from __future__ import annotations

import asyncio
import os
import signal

import asyncpg
import structlog

from services.app.gateway.db_bootstrap import _register_codecs
from services.ingest.integrations.google_calendar.live_poller import run_forever


async def _main() -> None:
    log = structlog.get_logger("dogfood.google_calendar_live_poller")
    dsn = os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(
        dsn=dsn, min_size=2, max_size=8, init=_register_codecs,
    )
    stop_event = asyncio.Event()

    def _stop(*_a: object) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    log.info("google_calendar_live_poller.starting")
    try:
        await run_forever(pool, stop_event=stop_event)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
