"""Launcher for services.think.worker.ThinkWorker — one worker process.

Bridges `ThinkWorker(pool).run()` to an asyncio-driven CLI. Kept minimal
on purpose: the worker owns its own poll/dispatch loop and graceful
shutdown via SIGTERM/SIGINT.
"""
from __future__ import annotations

import asyncio
import os
import signal

import asyncpg
import structlog

from lib.llm.provider import build_provider
from services.gateway.db_bootstrap import _register_codecs
from services.think.worker import ThinkWorker


async def _main() -> None:
    log = structlog.get_logger("dogfood.think_worker")
    dsn = os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(
        dsn=dsn, min_size=2, max_size=8, init=_register_codecs,
    )
    llm = build_provider()
    try:
        worker = ThinkWorker(pool, llm_provider=llm)
        worker.install_signal_handlers()
        log.info(
            "think_worker.starting",
            llm_provider=llm.config.provider,
            llm_model=llm.config.model,
        )
        await worker.run()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
