"""Shared health, metrics, pool, and signal wiring for script workers."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import pathlib
import signal
import sys
from typing import Any


def bootstrap_repo_root() -> None:
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


bootstrap_repo_root()

from lib.observability.health import (  # noqa: E402
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)
from lib.observability.metrics import render_default  # noqa: E402
from lib.observability.pools import register_pool as _register_pool  # noqa: E402


def install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass


def register_pool(pool_name: str, pool: Any) -> None:
    _register_pool(pool_name, pool)


def start_worker_health(
    worker_name: str,
    stop_event: asyncio.Event,
    *,
    render_metrics: Callable[[], str] | None = None,
) -> Callable[[], Awaitable[None]]:
    heartbeat = Heartbeat()
    health = start_health_server(
        worker_name=worker_name,
        render_metrics=render_metrics or render_default,
        heartbeat=heartbeat,
    )
    ticker = asyncio.create_task(run_heartbeat_ticker(heartbeat, stop_event))

    async def shutdown() -> None:
        ticker.cancel()
        await asyncio.gather(ticker, return_exceptions=True)
        if health is not None:
            health.shutdown()

    return shutdown


__all__ = [
    "bootstrap_repo_root",
    "install_signal_handlers",
    "register_pool",
    "start_worker_health",
]
