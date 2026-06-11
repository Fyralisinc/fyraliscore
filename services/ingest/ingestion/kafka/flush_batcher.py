"""Micro-batched Kafka flush barrier.

Request/response ingress paths need broker-ack semantics before they ack an
external provider or persist a cursor. Calling ``Producer.flush()`` once per
frame/request is durable but expensive. This helper coalesces flush waiters for
the same producer over a short delay so concurrent callers share one broker
round trip while each caller still waits for a definitive flush result.
"""
from __future__ import annotations

import asyncio
import os
import weakref
from typing import Any


_DEFAULT_DELAY_MS = int(os.environ.get("KAFKA_FLUSH_BATCH_MAX_DELAY_MS", "0"))
_BATCHERS: weakref.WeakKeyDictionary[Any, "KafkaFlushBatcher"] = (
    weakref.WeakKeyDictionary()
)


class KafkaFlushBatcher:
    """Coalesce multiple flush waiters for one producer instance."""

    def __init__(self, producer: Any, *, max_delay_ms: int) -> None:
        self._producer_ref = weakref.ref(producer)
        self._max_delay_s = max(0.0, max_delay_ms / 1000.0)
        self._lock = asyncio.Lock()
        self._waiters: list[asyncio.Future[int]] = []
        self._timeouts: list[float] = []
        self._task: asyncio.Task[None] | None = None

    async def flush(self, timeout_seconds: float) -> int:
        if self._max_delay_s <= 0:
            producer = self._producer_ref()
            if producer is None:
                return 0
            return await producer.flush(timeout_seconds)

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[int] = loop.create_future()
        async with self._lock:
            self._waiters.append(fut)
            self._timeouts.append(timeout_seconds)
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._drain_once())
        return await fut

    async def _drain_once(self) -> None:
        await asyncio.sleep(self._max_delay_s)
        async with self._lock:
            waiters = self._waiters
            timeouts = self._timeouts
            self._waiters = []
            self._timeouts = []
            self._task = None

        timeout = min(timeouts) if timeouts else 0.0
        try:
            producer = self._producer_ref()
            remaining = 0 if producer is None else await producer.flush(timeout)
        except Exception as exc:  # noqa: BLE001
            for waiter in waiters:
                if not waiter.done():
                    waiter.set_exception(exc)
            return

        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(remaining)


async def coalesced_flush(
    producer: Any,
    *,
    timeout_seconds: float,
    max_delay_ms: int | None = None,
) -> int:
    """Flush ``producer``, sharing a short-delay flush batch when enabled."""
    delay_ms = _DEFAULT_DELAY_MS if max_delay_ms is None else max_delay_ms
    if delay_ms <= 0:
        return await producer.flush(timeout_seconds)

    try:
        batcher = _BATCHERS.get(producer)
    except TypeError:
        # Non-weakrefable test doubles keep the old behavior.
        return await producer.flush(timeout_seconds)
    if batcher is None:
        batcher = KafkaFlushBatcher(producer, max_delay_ms=delay_ms)
        _BATCHERS[producer] = batcher
    return await batcher.flush(timeout_seconds)


__all__ = ["KafkaFlushBatcher", "coalesced_flush"]
