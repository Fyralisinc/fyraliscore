"""Production launcher for the durable perception and episode pipeline roles."""

from __future__ import annotations

import argparse
import asyncio
import os
import socket
from collections import Counter
from datetime import timedelta
from typing import Literal

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
from services.domain.episodes.handoff_worker import EpisodeReasoningHandoffWorker
from services.domain.episodes.worker import EpisodeConstructorWorker, EpisodeSettlementWorker
from services.domain.identity.worker import IdentityResolutionWorker
from services.domain.perception.knowledge import PerceptionKnowledgeWorker


Role = Literal["identity", "knowledge", "episode", "settlement", "handoff"]
_PROCESS_NAMES: dict[Role, str] = {
    "identity": "identity_resolution_worker",
    "knowledge": "perception_knowledge_worker",
    "episode": "episode_constructor_worker",
    "settlement": "episode_settlement_worker",
    "handoff": "episode_handoff_worker",
}
_QUEUE_TABLES: dict[Role, str] = {
    "identity": "identity_resolution_outbox",
    "knowledge": "perception_knowledge_outbox",
    "episode": "perception_outbox",
    "handoff": "episode_snapshot_outbox",
}


def _positive_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    value = float(raw)
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _metrics(process_name: str, stats: Counter[str]) -> str:
    prefix = process_name.removesuffix("_worker")
    return "\n".join(
        [
            f"# HELP {prefix}_cycles_total Worker poll cycles.",
            f"# TYPE {prefix}_cycles_total counter",
            f"{prefix}_cycles_total {stats['cycles']}",
            f"# HELP {prefix}_items_processed_total Claimed or settled items processed.",
            f"# TYPE {prefix}_items_processed_total counter",
            f"{prefix}_items_processed_total {stats['items']}",
            f"# HELP {prefix}_cycle_errors_total Worker poll cycle failures.",
            f"# TYPE {prefix}_cycle_errors_total counter",
            f"{prefix}_cycle_errors_total {stats['errors']}",
            f"# HELP {prefix}_queue_pending Current pending durable work.",
            f"# TYPE {prefix}_queue_pending gauge",
            f"{prefix}_queue_pending {stats['queue_pending']}",
            f"# HELP {prefix}_queue_leased Current leased durable work.",
            f"# TYPE {prefix}_queue_leased gauge",
            f"{prefix}_queue_leased {stats['queue_leased']}",
            f"# HELP {prefix}_queue_dead_letter Current dead-letter durable work.",
            f"# TYPE {prefix}_queue_dead_letter gauge",
            f"{prefix}_queue_dead_letter {stats['queue_dead_letter']}",
            render_default(),
        ]
    )


async def _refresh_queue_stats(
    pool: asyncpg.Pool, role: Role, stats: Counter[str]
) -> None:
    if role == "settlement":
        async with pool.acquire() as conn:
            stats["queue_pending"] = await conn.fetchval(
                "SELECT count(*) FROM episodes "
                "WHERE lifecycle_state IN ('open','reopened','dormant')"
            )
        stats["queue_leased"] = 0
        stats["queue_dead_letter"] = 0
        return
    table = _QUEUE_TABLES[role]
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT status,count(*) AS count FROM {table} "
            "WHERE status IN ('pending','leased','dead_letter') GROUP BY status"
        )
    counts = {str(row["status"]): int(row["count"]) for row in rows}
    for status in ("pending", "leased", "dead_letter"):
        stats[f"queue_{status}"] = counts.get(status, 0)


async def _main(role: Role) -> None:
    process_name = _PROCESS_NAMES[role]
    log = structlog.get_logger(f"perception.{role}")
    dsn = os.environ["DATABASE_URL"]
    poll_s = _positive_float("PERCEPTION_PIPELINE_POLL_INTERVAL_S", 2.0)
    batch_size = positive_int_env("PERCEPTION_PIPELINE_BATCH_SIZE", default=50)
    lease_seconds = positive_int_env("PERCEPTION_PIPELINE_LEASE_SECONDS", default=60)
    retry_delay = positive_int_env(
        "PERCEPTION_PIPELINE_RETRY_DELAY_SECONDS", default=5
    )
    max_attempts = positive_int_env("PERCEPTION_PIPELINE_MAX_ATTEMPTS", default=5)
    pool_max = positive_int_env("PERCEPTION_PIPELINE_POSTGRES_POOL_SIZE", default=4)
    quiet_seconds = positive_int_env("EPISODE_QUIET_PERIOD_SECONDS", default=300)
    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=pool_max,
        init=_register_codecs,
        **asyncpg_pool_runtime_kwargs(
            dsn=dsn,
            process_env_var="PERCEPTION_PIPELINE_POSTGRES_PGBOUNCER_COMPATIBLE",
        ),
    )
    register_pool(process_name, pool)
    shutdown = asyncio.Event()
    install_signal_handlers(shutdown)
    stats: Counter[str] = Counter()
    health_shutdown = start_worker_health(
        process_name, shutdown,
        render_metrics=lambda: _metrics(process_name, stats),
    )
    worker_id = f"{process_name}:{socket.gethostname()}:{os.getpid()}"
    workers = {
        "identity": IdentityResolutionWorker(pool),
        "knowledge": PerceptionKnowledgeWorker(pool),
        "episode": EpisodeConstructorWorker(pool),
        "settlement": EpisodeSettlementWorker(
            pool, quiet_period=timedelta(seconds=quiet_seconds)
        ),
        "handoff": EpisodeReasoningHandoffWorker(pool),
    }
    worker = workers[role]
    log.info(
        "worker.starting", process=process_name, batch_size=batch_size,
        poll_interval_s=poll_s,
    )
    try:
        while not shutdown.is_set():
            stats["cycles"] += 1
            try:
                if role == "settlement":
                    processed = await worker.run_once(batch_size=batch_size)
                else:
                    processed = await worker.run_once(
                        worker_id=worker_id,
                        batch_size=batch_size,
                        lease_seconds=lease_seconds,
                        retry_delay_seconds=retry_delay,
                        max_attempts=max_attempts,
                    )
                stats["items"] += processed
                await _refresh_queue_stats(pool, role, stats)
            except Exception as exc:  # noqa: BLE001 - outer loop must survive
                stats["errors"] += 1
                log.exception("worker.cycle_failed", error=str(exc))
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=poll_s)
            except asyncio.TimeoutError:
                pass
    finally:
        log.info("worker.stopping", process=process_name, stats=dict(stats))
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, choices=tuple(_PROCESS_NAMES))
    args = parser.parse_args()
    asyncio.run(_main(args.role))
