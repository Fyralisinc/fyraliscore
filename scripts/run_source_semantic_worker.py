"""Launcher for durable grounding-to-source-semantics work."""
from __future__ import annotations

import asyncio
import os
import socket
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
from services.workers.source_semantic_worker import (
    SourceSemanticWorker,
    SourceSemanticWorkerStats,
)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def _render_stats(stats: SourceSemanticWorkerStats) -> str:
    values = {
        "batches": stats.batches,
        "claimed": stats.claimed,
        "belief_applied": stats.belief_applied,
        "no_admission": stats.no_admission,
        "retries_scheduled": stats.retries_scheduled,
        "terminal_failures": stats.terminal_failures,
        "stale_claims": stats.stale_claims,
    }
    lines: list[str] = []
    for metric, value in values.items():
        name = f"source_semantic_worker_{metric}_total"
        lines.extend(
            [
                f"# HELP {name} Source semantic worker {metric.replace('_', ' ')}.",
                f"# TYPE {name} counter",
                f"{name} {value}",
            ]
        )
    return "\n".join(lines) + "\n" + render_default()


async def _main() -> None:
    log = structlog.get_logger("fyralis.source_semantic_worker")
    dsn = os.environ["DATABASE_URL"]
    poll_s = float(os.environ.get("SOURCE_SEMANTIC_POLL_INTERVAL_S", "5"))
    batch_size = int(os.environ.get("SOURCE_SEMANTIC_BATCH_SIZE", "25"))
    lease_s = float(os.environ.get("SOURCE_SEMANTIC_LEASE_S", "120"))
    retry_s = float(os.environ.get("SOURCE_SEMANTIC_RETRY_S", "30"))
    max_attempts = int(os.environ.get("SOURCE_SEMANTIC_MAX_ATTEMPTS", "5"))
    worker_id = os.environ.get(
        "SOURCE_SEMANTIC_WORKER_ID",
        f"source-semantic:{socket.gethostname()}:{os.getpid()}",
    )
    once = _env_bool("SOURCE_SEMANTIC_ONCE", False)

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=4,
        init=_register_codecs,
    )
    register_pool("source_semantic_worker", pool)
    shutdown = asyncio.Event()
    install_signal_handlers(shutdown)
    stats = SourceSemanticWorkerStats()
    health_shutdown = start_worker_health(
        "source_semantic_worker",
        shutdown,
        render_metrics=lambda: _render_stats(stats),
    )
    worker = SourceSemanticWorker(
        pool=pool,
        worker_id=worker_id,
        lease_duration=timedelta(seconds=lease_s),
        retry_delay=timedelta(seconds=retry_s),
        max_attempts=max_attempts,
    )

    log.info(
        "source_semantic_worker.starting",
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
                processed = await worker.process_batch(
                    limit=batch_size,
                    stats=stats,
                )
                log.info("source_semantic_worker.poll_done", processed=processed)
            except Exception as exc:  # noqa: BLE001
                log.exception("source_semantic_worker.loop_error", error=str(exc))
            if once:
                break
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=poll_s)
                break
            except asyncio.TimeoutError:
                continue
    finally:
        log.info(
            "source_semantic_worker.stopping",
            batches=stats.batches,
            claimed=stats.claimed,
            belief_applied=stats.belief_applied,
            no_admission=stats.no_admission,
            retries_scheduled=stats.retries_scheduled,
            terminal_failures=stats.terminal_failures,
            stale_claims=stats.stale_claims,
        )
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
