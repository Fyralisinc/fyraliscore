"""Launcher for services.reasoning.think.post_commit.process_batch — polling loop.

Runs `process_batch` every POST_COMMIT_WORKER_POLL_INTERVAL_S seconds
and exits cleanly on SIGTERM/SIGINT.

P2-13: exposes /healthz + /metrics (opt-in via INGESTION_HEALTH_PORT, which
the compose x-app-env anchor already sets) so a hung poll loop goes 503 and
the WorkerStats counters are scrapeable instead of log-only.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import signal
import sys

import asyncpg
import structlog

# In-container the repo lives at /app but `python scripts/x.py` puts
# /app/scripts (not /app) on sys.path — same bootstrap as the other
# script launchers (run_discord_gateway_worker.py).
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lib.observability.health import (  # noqa: E402
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)
from lib.observability.metrics import render_default  # noqa: E402
from lib.observability.pools import register_pool  # noqa: E402
from lib.shared.db import asyncpg_pool_runtime_kwargs, positive_int_env  # noqa: E402
from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.reasoning.think.post_commit import (  # noqa: E402
    WorkerStats,
    process_batch,
)


def _render_stats(stats: WorkerStats) -> str:
    lines = [
        "# HELP post_commit_processed_total Post-commit actions processed.",
        "# TYPE post_commit_processed_total counter",
        f"post_commit_processed_total {stats.processed}",
        "# HELP post_commit_failed_total Post-commit actions that failed.",
        "# TYPE post_commit_failed_total counter",
        f"post_commit_failed_total {stats.failed}",
        "# HELP post_commit_dead_lettered_total Post-commit actions dead-lettered.",
        "# TYPE post_commit_dead_lettered_total counter",
        f"post_commit_dead_lettered_total {stats.dead_lettered}",
        "# HELP post_commit_iterations_total Poll-loop iterations.",
        "# TYPE post_commit_iterations_total counter",
        f"post_commit_iterations_total {stats.iterations}",
    ]
    return "\n".join(lines) + "\n" + render_default()


async def _main() -> None:
    log = structlog.get_logger("dogfood.post_commit_worker")
    dsn = os.environ["DATABASE_URL"]
    poll_s = float(os.environ.get("POST_COMMIT_WORKER_POLL_INTERVAL_S", "5"))
    pool_max = positive_int_env("POST_COMMIT_POSTGRES_POOL_SIZE", default=4)
    runtime_kwargs = asyncpg_pool_runtime_kwargs(
        dsn=dsn,
        process_env_var="POST_COMMIT_POSTGRES_PGBOUNCER_COMPATIBLE",
    )
    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,
        max_size=pool_max,
        init=_register_codecs,
        **runtime_kwargs,
    )
    register_pool("post_commit_worker", pool)

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            pass

    stats = WorkerStats()
    heartbeat = Heartbeat()
    health = start_health_server(
        worker_name="post_commit_worker",
        render_metrics=lambda: _render_stats(stats),
        heartbeat=heartbeat,
    )
    ticker = asyncio.create_task(run_heartbeat_ticker(heartbeat, shutdown))
    log.info("post_commit_worker.starting", poll_s=poll_s)
    try:
        while not shutdown.is_set():
            heartbeat.touch()
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
        ticker.cancel()
        await asyncio.gather(ticker, return_exceptions=True)
        if health is not None:
            health.shutdown()
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
