"""Unit tests for the graceful-shutdown helper (ticket #45).

No Kafka broker: a fake consumer whose getone() blocks until fed lets us
prove next_or_stop returns the message when one arrives and returns None
(cancelling the in-flight fetch) when the stop event fires first.
"""
from __future__ import annotations

import asyncio


from services.ingest.ingestion.kafka.shutdown import (
    install_shutdown_event,
    next_or_stop,
)


class _FakeConsumer:
    """getone() resolves from an internal queue; tracks cancellation."""

    def __init__(self) -> None:
        self._q: asyncio.Queue = asyncio.Queue()
        self.getone_cancelled = False

    async def getone(self):
        try:
            return await self._q.get()
        except asyncio.CancelledError:
            self.getone_cancelled = True
            raise

    def feed(self, msg) -> None:
        self._q.put_nowait(msg)


async def test_returns_message_when_available() -> None:
    consumer = _FakeConsumer()
    stop = asyncio.Event()
    consumer.feed("m1")
    assert await next_or_stop(consumer, stop) == "m1"


async def test_returns_none_when_stop_set_first() -> None:
    consumer = _FakeConsumer()  # never fed → getone blocks
    stop = asyncio.Event()

    async def _trip() -> None:
        await asyncio.sleep(0.01)
        stop.set()

    asyncio.create_task(_trip())
    result = await next_or_stop(consumer, stop)
    assert result is None
    # The in-flight fetch was cancelled cleanly.
    assert consumer.getone_cancelled is True


async def test_returns_none_immediately_if_already_stopped() -> None:
    consumer = _FakeConsumer()
    stop = asyncio.Event()
    stop.set()
    assert await next_or_stop(consumer, stop) is None


async def test_install_shutdown_event_sets_on_sigterm() -> None:
    import os
    import signal

    ev = install_shutdown_event()
    assert not ev.is_set()
    os.kill(os.getpid(), signal.SIGTERM)
    # The asyncio signal handler runs on the loop; give it a tick.
    await asyncio.wait_for(ev.wait(), timeout=2.0)
    assert ev.is_set()
