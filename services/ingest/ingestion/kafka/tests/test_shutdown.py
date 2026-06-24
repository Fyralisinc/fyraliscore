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


# ---------------------------------------------------------------------
# BYOC §12 G6 — producer stop with undelivered messages must increment the
# fleet-scraped shutdown-undelivered counter (silent restart-time data loss
# is now alertable instead of log-only). No broker: a fake confluent Producer
# whose flush() reports `remaining` undelivered drives the stop() path.
# ---------------------------------------------------------------------


class _FakeConfluentProducer:
    """Stand-in for confluent_kafka.Producer used by IdempotentProducer.

    flush(timeout) returns `remaining` — the count of messages still in the
    local queue when the timeout elapsed (0 = all delivered).
    """

    def __init__(self, remaining: int) -> None:
        self._remaining = remaining
        self.flush_calls = 0

    def flush(self, timeout_seconds: float) -> int:
        self.flush_calls += 1
        return self._remaining


async def test_stop_with_undelivered_increments_shutdown_counter() -> None:
    from lib.observability.metrics import (
        KAFKA_PRODUCER_SHUTDOWN_UNDELIVERED,
        reset_default_for_tests,
    )
    from services.ingest.ingestion.kafka.producer import IdempotentProducer

    reset_default_for_tests()
    prod = IdempotentProducer()
    # Inject the fake underlying producer so stop() → flush() → 3 undelivered.
    prod._producer = _FakeConfluentProducer(remaining=3)

    await prod.stop(timeout_seconds=0.01)

    # The counter records the LOSS MAGNITUDE (number of messages), not just
    # that a stop timed out.
    assert KAFKA_PRODUCER_SHUTDOWN_UNDELIVERED.get() == 3.0
    # stop() tore the producer down even though the flush was incomplete.
    assert prod._producer is None
    reset_default_for_tests()


async def test_stop_with_clean_flush_does_not_increment_shutdown_counter() -> None:
    from lib.observability.metrics import (
        KAFKA_PRODUCER_SHUTDOWN_UNDELIVERED,
        reset_default_for_tests,
    )
    from services.ingest.ingestion.kafka.producer import IdempotentProducer

    reset_default_for_tests()
    prod = IdempotentProducer()
    prod._producer = _FakeConfluentProducer(remaining=0)  # all delivered

    await prod.stop(timeout_seconds=0.01)

    # A clean shutdown is the common case — no data-loss counter movement.
    assert KAFKA_PRODUCER_SHUTDOWN_UNDELIVERED.get() == 0.0
    reset_default_for_tests()
