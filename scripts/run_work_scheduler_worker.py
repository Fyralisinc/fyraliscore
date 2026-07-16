"""Launcher for durable planned-work scheduling."""
from __future__ import annotations

import asyncio
import os
import socket
from dataclasses import dataclass
from datetime import timedelta

import asyncpg
import structlog

from worker_observability import (
    install_signal_handlers,
    register_pool,
    start_worker_health,
)
from lib.observability.metrics import render_default
from services.app.gateway.db_bootstrap import _register_codecs
from services.workers.work_scheduler_worker.worker import WorkSchedulerWorker


@dataclass
class _LauncherStats:
    batches: int = 0
    processed: int = 0
    loop_errors: int = 0


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def _render_stats(stats: _LauncherStats) -> str:
    values = {
        "batches": stats.batches,
        "processed": stats.processed,
        "loop_errors": stats.loop_errors,
    }
    lines: list[str] = []
    for metric, value in values.items():
        name = f"work_scheduler_worker_{metric}_total"
        lines.extend(
            [
                f"# HELP {name} Work scheduler worker "
                f"{metric.replace('_', ' ')}.",
                f"# TYPE {name} counter",
                f"{name} {value}",
            ]
        )
    return "\n".join(lines) + "\n" + render_default()


async def _main() -> None:
    log = structlog.get_logger("fyralis.work_scheduler_worker")
    dsn = os.environ["DATABASE_URL"]
    poll_s = float(os.environ.get("WORK_SCHEDULER_POLL_INTERVAL_S", "5"))
    batch_size = int(os.environ.get("WORK_SCHEDULER_BATCH_SIZE", "100"))
    lease_s = float(os.environ.get("WORK_SCHEDULER_LEASE_S", "120"))
    retry_s = float(os.environ.get("WORK_SCHEDULER_RETRY_S", "30"))
    max_attempts = int(os.environ.get("WORK_SCHEDULER_MAX_ATTEMPTS", "5"))
    worker_id = os.environ.get(
        "WORK_SCHEDULER_WORKER_ID",
        f"work-scheduler:{socket.gethostname()}:{os.getpid()}",
    )
    once = _env_bool("WORK_SCHEDULER_ONCE", False)

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=4,
        init=_register_codecs,
    )
    register_pool("work_scheduler_worker", pool)
    shutdown = asyncio.Event()
    install_signal_handlers(shutdown)
    stats = _LauncherStats()
    health_shutdown = start_worker_health(
        "work_scheduler_worker",
        shutdown,
        render_metrics=lambda: _render_stats(stats),
    )
    worker = WorkSchedulerWorker(
        pool=pool,
        worker_id=worker_id,
        lease_duration=timedelta(seconds=lease_s),
        retry_delay=timedelta(seconds=retry_s),
        max_attempts=max_attempts,
    )

    log.info(
        "work_scheduler_worker.starting",
        worker_id=worker_id,
        once=once,
        poll_s=poll_s,
        batch_size=batch_size,
        lease_s=lease_s,
        retry_s=retry_s,
        max_attempts=max_attempts,
    )
    try:
        while not shutdown.is_set():
            try:
                processed = await worker.process_batch(limit=batch_size)
                stats.batches += 1
                stats.processed += processed
                log.info(
                    "work_scheduler_worker.poll_done",
                    processed=processed,
                )
            except Exception as exc:  # noqa: BLE001
                stats.loop_errors += 1
                log.exception(
                    "work_scheduler_worker.loop_error",
                    error=str(exc),
                )
            if once:
                break
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=poll_s)
                break
            except asyncio.TimeoutError:
                continue
    finally:
        log.info(
            "work_scheduler_worker.stopping",
            batches=stats.batches,
            processed=stats.processed,
            loop_errors=stats.loop_errors,
        )
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
