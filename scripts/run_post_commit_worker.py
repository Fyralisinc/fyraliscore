"""Launcher for services.think.post_commit.process_batch — polling loop.

Runs `process_batch` every POST_COMMIT_WORKER_POLL_INTERVAL_S seconds
and exits cleanly on SIGTERM/SIGINT.
"""
from __future__ import annotations

import asyncio
import os
import signal

import asyncpg
import structlog

from services.think.post_commit import WorkerStats, process_batch


async def _main() -> None:
    log = structlog.get_logger("dogfood.post_commit_worker")
    dsn = os.environ["DATABASE_URL"]
    poll_s = float(os.environ.get("POST_COMMIT_WORKER_POLL_INTERVAL_S", "5"))
    pool = await asyncpg.create_pool(dsn=dsn, min_size=2, max_size=4)

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            pass

    stats = WorkerStats()
    log.info("post_commit_worker.starting", poll_s=poll_s)
    try:
        while not shutdown.is_set():
            try:
                await process_batch(pool, stats=stats)
            except Exception as e:  # noqa: BLE001
                log.exception("post_commit.loop_error", error=str(e))
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=poll_s)
                break
            except asyncio.TimeoutError:
                pass
    finally:
        log.info(
            "post_commit_worker.stopping",
            processed=stats.processed,
            failed=stats.failed,
            dead_lettered=stats.dead_lettered,
            iterations=stats.iterations,
        )
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
