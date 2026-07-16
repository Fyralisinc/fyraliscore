"""Launcher for durable InterventionEpisode manifest projection work."""
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
from services.workers.intervention_episode_coordinator.worker import (
    InterventionEpisodeCoordinatorWorker,
)


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
        name = f"intervention_episode_coordinator_{metric}_total"
        lines.extend(
            [
                f"# HELP {name} Intervention episode coordinator "
                f"{metric.replace('_', ' ')}.",
                f"# TYPE {name} counter",
                f"{name} {value}",
            ]
        )
    return "\n".join(lines) + "\n" + render_default()


async def _main() -> None:
    log = structlog.get_logger("fyralis.intervention_episode_coordinator")
    dsn = os.environ["DATABASE_URL"]
    poll_s = float(
        os.environ.get(
            "INTERVENTION_EPISODE_COORDINATOR_POLL_INTERVAL_S",
            "5",
        )
    )
    batch_size = int(
        os.environ.get("INTERVENTION_EPISODE_COORDINATOR_BATCH_SIZE", "100")
    )
    lease_s = float(
        os.environ.get("INTERVENTION_EPISODE_COORDINATOR_LEASE_S", "120")
    )
    retry_s = float(
        os.environ.get("INTERVENTION_EPISODE_COORDINATOR_RETRY_S", "30")
    )
    max_attempts = int(
        os.environ.get("INTERVENTION_EPISODE_COORDINATOR_MAX_ATTEMPTS", "5")
    )
    worker_id = os.environ.get(
        "INTERVENTION_EPISODE_COORDINATOR_WORKER_ID",
        f"intervention-episode-coordinator:{socket.gethostname()}:{os.getpid()}",
    )
    once = _env_bool("INTERVENTION_EPISODE_COORDINATOR_ONCE", False)

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=4,
        init=_register_codecs,
    )
    register_pool("intervention_episode_coordinator", pool)
    shutdown = asyncio.Event()
    install_signal_handlers(shutdown)
    stats = _LauncherStats()
    health_shutdown = start_worker_health(
        "intervention_episode_coordinator",
        shutdown,
        render_metrics=lambda: _render_stats(stats),
    )
    worker = InterventionEpisodeCoordinatorWorker(
        pool=pool,
        worker_id=worker_id,
        lease_duration=timedelta(seconds=lease_s),
        retry_delay=timedelta(seconds=retry_s),
        max_attempts=max_attempts,
    )

    log.info(
        "intervention_episode_coordinator.starting",
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
                    "intervention_episode_coordinator.poll_done",
                    processed=processed,
                )
            except Exception as exc:  # noqa: BLE001
                stats.loop_errors += 1
                log.exception(
                    "intervention_episode_coordinator.loop_error",
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
            "intervention_episode_coordinator.stopping",
            batches=stats.batches,
            processed=stats.processed,
            loop_errors=stats.loop_errors,
        )
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
