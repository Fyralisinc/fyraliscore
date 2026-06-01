"""Graceful shutdown for the long-running Kafka consumers (ticket #45).

The consumer run loops used `async for msg in consumer` and only checked
a stop flag at the top of the loop body — so a SIGTERM that arrived while
the loop was blocked waiting for the next message did not interrupt the
await. The process then died by SIGTERM/SIGKILL (rc=-15/-9), which is
indistinguishable from a real crash and trips the supervisor's
"worker died" alarm on every clean shutdown.

This module provides:
  * install_shutdown_event() — register SIGTERM/SIGINT handlers (via the
    asyncio loop where possible, falling back to `signal.signal` for
    off-main-thread/Windows) that set an `asyncio.Event`.
  * next_or_stop(consumer, stop_event) — await the next message, but
    return None promptly (cancelling the in-flight fetch cleanly) if
    shutdown is requested while waiting.

In production (Linux, main thread) `loop.add_signal_handler` is used,
so a SIGTERM resolves the racing wait immediately and the loop exits
cleanly with rc=0.
"""
from __future__ import annotations

import asyncio
import signal
from typing import Any


def install_shutdown_event(
    loop: asyncio.AbstractEventLoop | None = None,
) -> asyncio.Event:
    """Return an asyncio.Event set on the first SIGTERM/SIGINT."""
    ev = asyncio.Event()
    loop = loop or asyncio.get_running_loop()

    def _request_stop(*_args: Any) -> None:
        ev.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError, ValueError):
            # Not the main thread, or a platform without
            # add_signal_handler. Best-effort fallback for tests.
            try:
                signal.signal(sig, _request_stop)
            except (ValueError, OSError):
                pass
    return ev


async def next_or_stop(consumer: Any, stop_event: asyncio.Event) -> Any | None:
    """Return the next Kafka message, or None if shutdown was requested.

    Races ``consumer.getone()`` against ``stop_event``. On stop, cancels
    the in-flight fetch cleanly so the consumer can be stopped without a
    pending-task warning. Returning None is the loop's signal to break
    and run its normal teardown.
    """
    if stop_event.is_set():
        return None
    get_task: asyncio.Task = asyncio.ensure_future(consumer.getone())
    stop_task: asyncio.Task = asyncio.ensure_future(stop_event.wait())
    try:
        done, _pending = await asyncio.wait(
            {get_task, stop_task}, return_when=asyncio.FIRST_COMPLETED,
        )
        if get_task in done:
            return get_task.result()
        # Stop fired first — cancel the fetch and drain it.
        get_task.cancel()
        try:
            await get_task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        return None
    finally:
        if not stop_task.done():
            stop_task.cancel()
            try:
                await stop_task
            except asyncio.CancelledError:
                pass


__all__ = ["install_shutdown_event", "next_or_stop"]
