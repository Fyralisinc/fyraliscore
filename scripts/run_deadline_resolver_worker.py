"""Launcher for the deadline-resolver worker — one worker process.

Bridges `services.workers.deadline_resolver.worker.DeadlineResolver` to an
asyncio-driven CLI, mirroring the other DB-poll worker launchers
(run_housekeeper_worker.py / run_sage_structural_features_worker.py). The
resolver polls for prediction Models whose ``evaluate_at`` has passed and
enqueues T2 ``prediction_overdue`` triggers for Think — this is what makes a
document-memory commitment fire *proactively* when overdue
(docs/plans/document-memory-substrate.md §4.6 / §7 step 11).

Env:
  * DATABASE_URL                — required.
  * DEADLINE_POLL_INTERVAL_S    — poll cadence (default 60; read by the worker).
  * DEADLINE_RESOLVER_ONCE      — run a single cycle and exit (default off).
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import sys

import asyncpg
import structlog

# `python scripts/x.py` puts scripts/ (not the repo root) on sys.path — bootstrap
# the root first, same as the other script launchers (run_think_worker.py).
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from worker_observability import (  # noqa: E402
    install_signal_handlers,
    register_pool,
    start_worker_health,
)
from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.workers.deadline_resolver.worker import DeadlineResolver  # noqa: E402


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


async def _main() -> None:
    log = structlog.get_logger("dogfood.deadline_resolver")
    dsn = os.environ["DATABASE_URL"]
    once = _env_bool("DEADLINE_RESOLVER_ONCE", False)

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=4,
        init=_register_codecs,
    )
    register_pool("deadline_resolver_worker", pool)
    shutdown = asyncio.Event()
    install_signal_handlers(shutdown)
    health_shutdown = start_worker_health("deadline_resolver_worker", shutdown)

    resolver = DeadlineResolver(pool)
    log.info("deadline_resolver.starting", once=once)
    try:
        if once:
            result = await resolver.run_once()
            log.info(
                "deadline_resolver.once_done",
                enqueued=result.enqueued,
                skipped_idempotent=result.skipped_idempotent,
                errored=result.errored,
                tenants_scanned=result.tenants_scanned,
                by_outcome=result.by_outcome,
            )
        else:
            await resolver.run(shutdown)
    finally:
        log.info("deadline_resolver.stopping")
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
