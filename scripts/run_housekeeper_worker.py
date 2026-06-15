"""Launcher for the Housekeeper scheduled-job worker."""
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
from services.workers.housekeeper.worker import (
    build_housekeeper_descriptors,
    run_forever,
    run_once_all,
)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


async def _main() -> None:
    log = structlog.get_logger("dogfood.housekeeper")
    dsn = os.environ["DATABASE_URL"]
    once = _env_bool("HOUSEKEEPER_ONCE", False)

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=4,
        init=_register_codecs,
    )
    register_pool("housekeeper_worker", pool)
    shutdown = asyncio.Event()
    install_signal_handlers(shutdown)
    health_shutdown = start_worker_health("housekeeper_worker", shutdown)

    descriptors = build_housekeeper_descriptors()
    log.info(
        "housekeeper.starting",
        once=once,
        jobs=[d.name for d in descriptors if d.enabled],
        disabled_jobs=[d.name for d in descriptors if not d.enabled],
    )
    try:
        if once:
            report = await run_once_all(pool, descriptors=descriptors)
            log.info(
                "housekeeper.once_done",
                completed=report.completed,
                failed=report.failed,
                errors=report.errors,
            )
        else:
            await run_forever(pool, descriptors=descriptors, shutdown=shutdown)
    finally:
        log.info("housekeeper.stopping")
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
