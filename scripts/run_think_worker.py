"""Launcher for services.reasoning.think.worker.ThinkWorker — one worker process.

Bridges `ThinkWorker(pool).run()` to an asyncio-driven CLI. Kept minimal
on purpose: the worker owns its own poll/dispatch loop and graceful
shutdown via SIGTERM/SIGINT.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import sys

import asyncpg
import structlog

# In-container the repo lives at /app but `python scripts/x.py` puts
# /app/scripts (not /app) on sys.path — same bootstrap as the other
# script launchers (run_discord_gateway_worker.py).
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lib.llm.provider import build_provider  # noqa: E402
from lib.observability.pools import register_pool  # noqa: E402
from lib.shared.db import asyncpg_pool_runtime_kwargs, positive_int_env  # noqa: E402
from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.reasoning.think.worker import ThinkWorker  # noqa: E402


async def _main() -> None:
    log = structlog.get_logger("dogfood.think_worker")
    dsn = os.environ["DATABASE_URL"]
    pool_max = positive_int_env("THINK_POSTGRES_POOL_SIZE", default=8)
    runtime_kwargs = asyncpg_pool_runtime_kwargs(
        dsn=dsn,
        process_env_var="THINK_POSTGRES_PGBOUNCER_COMPATIBLE",
    )
    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,
        max_size=pool_max,
        init=_register_codecs,
        **runtime_kwargs,
    )
    register_pool("think_worker", pool)
    llm = build_provider()
    try:
        worker = ThinkWorker(pool, llm_provider=llm)
        worker.install_signal_handlers()
        log.info(
            "think_worker.starting",
            llm_provider=llm.config.provider,
            llm_model=llm.config.model,
        )
        await worker.run()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
