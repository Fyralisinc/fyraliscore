"""Launcher for the anomaly processor background worker."""
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
from services.workers.anomaly_processor.worker import (
    AnomalyProcessor,
    AnomalyProcessorConfig,
)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


async def _main() -> None:
    log = structlog.get_logger("dogfood.anomaly_processor")
    dsn = os.environ["DATABASE_URL"]
    once = _env_bool("ANOMALY_PROCESSOR_ONCE", False)

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=4,
        init=_register_codecs,
    )
    register_pool("anomaly_processor_worker", pool)
    shutdown = asyncio.Event()
    install_signal_handlers(shutdown)
    health_shutdown = start_worker_health("anomaly_processor_worker", shutdown)

    config = AnomalyProcessorConfig.from_env()
    worker = AnomalyProcessor(pool, config=config)
    log.info(
        "anomaly_processor.starting",
        once=once,
        poll_interval_s=config.poll_interval_s,
        promote_every_n_cycles=config.promote_every_n_cycles,
        t3_budget_per_tenant_per_min=config.t3_budget_per_tenant_per_min,
    )
    try:
        if once:
            tenants = await worker._list_active_tenants()
            counters = await worker.process_once(tenants, force_promote=True)
            log.info(
                "anomaly_processor.once_done",
                tenants=len(tenants),
                **counters,
            )
        else:
            await worker.run(shutdown)
    finally:
        log.info("anomaly_processor.stopping")
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
