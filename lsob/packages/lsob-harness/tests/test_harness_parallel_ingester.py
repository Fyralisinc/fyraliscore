"""Tests for :class:`ParallelIngester`.

Covers:
- per-actor causal order is preserved (intra-actor signals stay ordered)
- max-concurrent bound is honored
- per-signal latency is recorded
- throughput_signals_per_sec() returns a positive value after a run
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from lsob_contracts import Signal, SourceChannel

from lsob_harness.ingester import ParallelIngester, SequentialIngester
from lsob_harness.mocks import MockSUT


def _sig(signal_id: str, author: str, ts_seconds: int) -> Signal:
    return Signal(
        signal_id=signal_id,
        source_channel=SourceChannel.slack,
        author_id=author,
        content_text=f"text-{signal_id}",
        timestamp=datetime(2026, 1, 1, 0, 0, ts_seconds, tzinfo=timezone.utc),
    )


class _RecordingSUT(MockSUT):
    """Records (author, signal_id) in ingest order + tracks concurrency peak."""

    max_concurrent_ingestion: int = 4

    def __init__(self, name: str = "rec", per_signal_ms: float = 20.0) -> None:
        super().__init__(name=name)
        self._per_signal_s = per_signal_ms / 1000.0
        self.order: list[tuple[str, str]] = []
        self.in_flight = 0
        self.peak_in_flight = 0
        self._lock = asyncio.Lock()

    async def ingest_signal(self, signal: Signal) -> None:  # type: ignore[override]
        async with self._lock:
            self.in_flight += 1
            if self.in_flight > self.peak_in_flight:
                self.peak_in_flight = self.in_flight
        await asyncio.sleep(self._per_signal_s)
        async with self._lock:
            self.order.append((signal.author_id, signal.signal_id))
            self.in_flight -= 1
        await super().ingest_signal(signal)


@pytest.mark.asyncio
async def test_parallel_preserves_per_actor_order() -> None:
    sut = _RecordingSUT(per_signal_ms=5.0)
    await sut.startup(None)  # type: ignore[arg-type]

    signals = [
        _sig("a1", "alice", 1),
        _sig("b1", "bob", 1),
        _sig("a2", "alice", 2),
        _sig("c1", "carol", 1),
        _sig("a3", "alice", 3),
        _sig("b2", "bob", 2),
    ]

    ing = ParallelIngester(max_concurrency=3)
    await ing.ingest(sut, signals)

    def _ordered(actor: str) -> list[str]:
        return [sid for a, sid in sut.order if a == actor]

    assert _ordered("alice") == ["a1", "a2", "a3"]
    assert _ordered("bob") == ["b1", "b2"]
    assert _ordered("carol") == ["c1"]


@pytest.mark.asyncio
async def test_parallel_respects_max_concurrency_bound() -> None:
    sut = _RecordingSUT(per_signal_ms=15.0)
    sut.max_concurrent_ingestion = 2
    await sut.startup(None)  # type: ignore[arg-type]

    # 4 distinct authors => up to 4-way parallelism without the bound.
    signals = [_sig(f"s{i}", f"actor{i}", 1) for i in range(4)]
    ing = ParallelIngester()  # derives max_concurrency from SUT
    await ing.ingest(sut, signals)

    assert sut.peak_in_flight <= 2
    assert sut.ingested_count == 4


@pytest.mark.asyncio
async def test_parallel_records_per_signal_latency_and_throughput() -> None:
    sut = _RecordingSUT(per_signal_ms=5.0)
    await sut.startup(None)  # type: ignore[arg-type]
    signals = [_sig(f"s{i}", f"actor{i%2}", i) for i in range(6)]

    ing = ParallelIngester(max_concurrency=2)
    await ing.ingest(sut, signals)

    assert len(ing.stats.timings) == 6
    # Every recorded signal_id appears exactly once
    ids = {t.signal_id for t in ing.stats.timings}
    assert ids == {s.signal_id for s in signals}
    # Latencies are positive real numbers
    assert all(t.latency_ms > 0 for t in ing.stats.timings)
    assert ing.throughput_signals_per_sec() > 0
    assert ing.last_signal_id is not None


@pytest.mark.asyncio
async def test_parallel_checkpoint_callback_fires() -> None:
    sut = _RecordingSUT(per_signal_ms=1.0)
    await sut.startup(None)  # type: ignore[arg-type]
    signals = [_sig(f"s{i}", "alice", i) for i in range(5)]

    fired: list[tuple[str, int]] = []

    def _cb(_ing, sig, count):
        fired.append((sig.signal_id, count))

    ing = ParallelIngester(
        max_concurrency=2, checkpoint_every_n=2, checkpoint_cb=_cb
    )
    await ing.ingest(sut, signals)

    # Should fire at ingested_count == 2 and 4.
    assert [c for _, c in fired] == [2, 4]


@pytest.mark.asyncio
async def test_parallel_faster_than_sequential_on_slow_sut() -> None:
    per_signal_ms = 12.0

    sut_seq = _RecordingSUT(per_signal_ms=per_signal_ms)
    await sut_seq.startup(None)  # type: ignore[arg-type]
    sut_par = _RecordingSUT(per_signal_ms=per_signal_ms)
    sut_par.max_concurrent_ingestion = 8
    await sut_par.startup(None)  # type: ignore[arg-type]

    signals = [_sig(f"s{i}", f"a{i%8}", i) for i in range(32)]

    seq = SequentialIngester()
    await seq.ingest(sut_seq, signals)

    par = ParallelIngester(max_concurrency=8)
    await par.ingest(sut_par, signals)

    assert par.stats.total_wall_clock_ms * 1.5 < seq.stats.total_wall_clock_ms, (
        f"expected >=1.5x speedup, got seq={seq.stats.total_wall_clock_ms:.2f}ms "
        f"par={par.stats.total_wall_clock_ms:.2f}ms"
    )
