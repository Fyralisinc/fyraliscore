"""Ingestion strategies.

Phase 1 shipped :class:`SequentialIngester`; Phase 2.2 adds a
:class:`ParallelIngester` that preserves per-actor causal order while
dispatching work across actors concurrently. We expose the ``Ingester``
Protocol so the harness can be re-parameterised without surgery.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Iterable, Protocol, runtime_checkable

from lsob_contracts import Signal, SystemUnderTest


@runtime_checkable
class Ingester(Protocol):
    async def ingest(
        self, sut: SystemUnderTest, signals: Iterable[Signal]
    ) -> None: ...


@dataclass
class SignalTiming:
    """Per-signal ingest latency record."""

    signal_id: str
    latency_ms: float


@dataclass
class IngestStats:
    """Aggregated ingestion metrics for one or more ``ingest()`` calls."""

    total_signals: int = 0
    total_wall_clock_ms: float = 0.0
    timings: list[SignalTiming] = field(default_factory=list)

    def throughput_signals_per_sec(self) -> float:
        if self.total_wall_clock_ms <= 0:
            return 0.0
        return self.total_signals / (self.total_wall_clock_ms / 1000.0)


class _BaseIngester:
    """Shared bookkeeping for sequential/parallel ingesters."""

    def __init__(
        self,
        *,
        checkpoint_every_n: int | None = None,
        checkpoint_cb: Callable[["_BaseIngester", Signal, int], None] | None = None,
    ) -> None:
        self.checkpoint_every_n = checkpoint_every_n
        self._checkpoint_cb = checkpoint_cb
        self.stats = IngestStats()
        self._last_signal_id: str | None = None
        self._ingested_count: int = 0

    @property
    def last_signal_id(self) -> str | None:
        return self._last_signal_id

    @property
    def ingested_count(self) -> int:
        return self._ingested_count

    def throughput_signals_per_sec(self) -> float:
        return self.stats.throughput_signals_per_sec()

    def _record(self, signal: Signal, latency_ms: float) -> None:
        self.stats.total_signals += 1
        self.stats.timings.append(
            SignalTiming(signal_id=signal.signal_id, latency_ms=latency_ms)
        )
        self._last_signal_id = signal.signal_id
        self._ingested_count += 1

    def _maybe_checkpoint(self, signal: Signal) -> None:
        if self.checkpoint_every_n and self._checkpoint_cb:
            if self._ingested_count % self.checkpoint_every_n == 0:
                self._checkpoint_cb(self, signal, self._ingested_count)


class SequentialIngester(_BaseIngester):
    """One-signal-at-a-time ingestion with an optional rate limit (signals/sec)."""

    def __init__(
        self,
        rate_limit: float | None = None,
        *,
        checkpoint_every_n: int | None = None,
        checkpoint_cb: Callable[["_BaseIngester", Signal, int], None] | None = None,
    ) -> None:
        super().__init__(
            checkpoint_every_n=checkpoint_every_n, checkpoint_cb=checkpoint_cb
        )
        self.rate_limit = rate_limit

    async def ingest(
        self, sut: SystemUnderTest, signals: Iterable[Signal]
    ) -> None:
        delay = 0.0 if not self.rate_limit else 1.0 / self.rate_limit
        wall_start = time.monotonic()
        for sig in signals:
            t0 = time.monotonic()
            await sut.ingest_signal(sig)
            latency_ms = (time.monotonic() - t0) * 1000.0
            self._record(sig, latency_ms)
            self._maybe_checkpoint(sig)
            if delay:
                await asyncio.sleep(delay)
        self.stats.total_wall_clock_ms += (time.monotonic() - wall_start) * 1000.0


class ParallelIngester(_BaseIngester):
    """Cross-actor parallel ingestion with per-actor causal ordering.

    Signals are bucketed by ``author_id``; each actor's signals are then
    replayed sequentially in timestamp order, but distinct actors run
    concurrently under an ``asyncio.Semaphore`` whose size is taken from
    ``sut.max_concurrent_ingestion``.
    """

    def __init__(
        self,
        *,
        max_concurrency: int | None = None,
        checkpoint_every_n: int | None = None,
        checkpoint_cb: Callable[["_BaseIngester", Signal, int], None] | None = None,
    ) -> None:
        super().__init__(
            checkpoint_every_n=checkpoint_every_n, checkpoint_cb=checkpoint_cb
        )
        self._max_concurrency_override = max_concurrency
        self._record_lock: asyncio.Lock | None = None

    async def ingest(
        self, sut: SystemUnderTest, signals: Iterable[Signal]
    ) -> None:
        buckets: dict[str, list[Signal]] = defaultdict(list)
        for sig in signals:
            buckets[sig.author_id].append(sig)
        for key in buckets:
            buckets[key].sort(key=lambda s: s.timestamp)

        max_concurrent = (
            self._max_concurrency_override
            if self._max_concurrency_override is not None
            else getattr(sut, "max_concurrent_ingestion", 1)
        )
        if max_concurrent < 1:
            max_concurrent = 1
        semaphore = asyncio.Semaphore(max_concurrent)
        self._record_lock = asyncio.Lock()

        async def _run_actor(actor_signals: list[Signal]) -> None:
            for sig in actor_signals:
                async with semaphore:
                    t0 = time.monotonic()
                    await sut.ingest_signal(sig)
                    latency_ms = (time.monotonic() - t0) * 1000.0
                assert self._record_lock is not None
                async with self._record_lock:
                    self._record(sig, latency_ms)
                    self._maybe_checkpoint(sig)

        wall_start = time.monotonic()
        tasks = [asyncio.create_task(_run_actor(sigs)) for sigs in buckets.values()]
        if tasks:
            await asyncio.gather(*tasks)
        self.stats.total_wall_clock_ms += (time.monotonic() - wall_start) * 1000.0
