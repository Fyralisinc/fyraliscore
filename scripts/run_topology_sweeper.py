"""Launcher for the latent topology sweeper worker.

Runs the bounded relationship-field sweep on an interval and exits
cleanly on SIGTERM/SIGINT.
"""
from __future__ import annotations

import asyncio
import os
import signal

import asyncpg
import structlog

from services.app.gateway.db_bootstrap import _register_codecs
from services.workers.topology_sweeper.worker import (
    DEFAULT_INTERVAL_S,
    DEFAULT_LIMIT_PER_TENANT,
    DEFAULT_MIN_ACTIVATION,
    run_forever,
    run_once,
)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


async def _main() -> None:
    log = structlog.get_logger("dogfood.topology_sweeper")
    dsn = os.environ["DATABASE_URL"]
    interval_s = float(
        os.environ.get("TOPOLOGY_SWEEPER_INTERVAL_S", str(DEFAULT_INTERVAL_S))
    )
    limit_per_tenant = int(
        os.environ.get(
            "TOPOLOGY_SWEEPER_LIMIT_PER_TENANT",
            str(DEFAULT_LIMIT_PER_TENANT),
        )
    )
    min_activation = float(
        os.environ.get(
            "TOPOLOGY_SWEEPER_MIN_ACTIVATION",
            str(DEFAULT_MIN_ACTIVATION),
        )
    )
    once = _env_bool("TOPOLOGY_SWEEPER_ONCE", False)

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=3,
        init=_register_codecs,
    )
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            pass

    log.info(
        "topology_sweeper.starting",
        interval_s=interval_s,
        limit_per_tenant=limit_per_tenant,
        min_activation=min_activation,
        once=once,
    )
    try:
        if once:
            report = await run_once(
                pool,
                limit_per_tenant=limit_per_tenant,
                min_activation=min_activation,
            )
            log.info(
                "topology_sweeper.once_done",
                tenants=len(report.tenant_reports),
                candidates_inserted=report.candidates_inserted,
                think_triggers_enqueued=report.think_triggers_enqueued,
            )
        else:
            await run_forever(
                pool,
                interval_s=interval_s,
                limit_per_tenant=limit_per_tenant,
                min_activation=min_activation,
                shutdown=shutdown,
            )
    finally:
        log.info("topology_sweeper.stopping")
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
