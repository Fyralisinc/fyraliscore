"""Launcher for the anomaly processor worker."""
from __future__ import annotations

import asyncio
import os
from collections import Counter

import asyncpg
import structlog

from worker_observability import (
    install_signal_handlers,
    register_pool,
    start_worker_health,
)
from lib.observability.metrics import render_default
from lib.shared.db import asyncpg_pool_runtime_kwargs, positive_int_env
from services.app.gateway.db_bootstrap import _register_codecs
from services.workers.anomaly_processor.worker import (
    AnomalyProcessor,
    AnomalyProcessorConfig,
)


_COUNTER_KEYS = (
    "detected",
    "enqueued_t3",
    "debounced",
    "suppressed",
    "escalated",
    "subthreshold",
    "promoted",
    "rate_limited",
)


def _render_stats(stats: Counter[str]) -> str:
    lines = [
        "# HELP anomaly_processor_cycles_total Anomaly processor cycles with active tenants.",
        "# TYPE anomaly_processor_cycles_total counter",
        f"anomaly_processor_cycles_total {stats['cycles']}",
    ]
    for key in _COUNTER_KEYS:
        metric = f"anomaly_processor_{key}_total"
        lines.extend(
            [
                f"# HELP {metric} Anomaly processor {key} counter.",
                f"# TYPE {metric} counter",
                f"{metric} {stats[key]}",
            ]
        )
    return "\n".join(lines) + "\n" + render_default()


async def _main() -> None:
    log = structlog.get_logger("dogfood.anomaly_processor")
    dsn = os.environ["DATABASE_URL"]
    pool_max = positive_int_env("ANOMALY_PROCESSOR_POSTGRES_POOL_SIZE", default=4)
    runtime_kwargs = asyncpg_pool_runtime_kwargs(
        dsn=dsn,
        process_env_var="ANOMALY_PROCESSOR_POSTGRES_PGBOUNCER_COMPATIBLE",
    )

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=pool_max,
        init=_register_codecs,
        **runtime_kwargs,
    )
    register_pool("anomaly_processor_worker", pool)

    shutdown = asyncio.Event()
    install_signal_handlers(shutdown)
    stats: Counter[str] = Counter()
    health_shutdown = start_worker_health(
        "anomaly_processor_worker",
        shutdown,
        render_metrics=lambda: _render_stats(stats),
    )

    config = AnomalyProcessorConfig.from_env()

    def _record_cycle(counters: dict[str, int]) -> None:
        stats["cycles"] += 1
        for key in _COUNTER_KEYS:
            stats[key] += int(counters.get(key, 0))

    log.info(
        "anomaly_processor.starting",
        poll_interval_s=config.poll_interval_s,
        t3_budget_per_tenant_per_min=config.t3_budget_per_tenant_per_min,
        promote_every_n_cycles=config.promote_every_n_cycles,
    )
    try:
        processor = AnomalyProcessor(pool, config=config)
        await processor.run(shutdown, on_cycle=_record_cycle)
    finally:
        log.info("anomaly_processor.stopping")
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
