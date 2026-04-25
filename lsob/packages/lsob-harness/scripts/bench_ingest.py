"""Benchmark: SequentialIngester vs ParallelIngester against MockSUT.

Run:
    /opt/homebrew/bin/uv run python packages/lsob-harness/scripts/bench_ingest.py

The mini corpus is intentionally tiny (10 signals) so we pad the workload
with per-signal latency inside the MockSUT to demonstrate the wiring. The
benchmark exits non-zero if parallel fails to beat sequential by at least
1.5x at ``max_concurrent_ingestion=8``.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Ensure package src is importable when invoked via `uv run python <script>`.
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "packages" / "lsob-harness" / "src"))
sys.path.insert(0, str(REPO_ROOT / "packages" / "lsob-contracts" / "src"))

from lsob_harness.corpus_io import load_corpus  # noqa: E402
from lsob_harness.ingester import ParallelIngester, SequentialIngester  # noqa: E402
from lsob_harness.mocks import MockSUT  # noqa: E402


FIXTURE = REPO_ROOT / "fixtures" / "mini_corpus_a.json"


class SlowMockSUT(MockSUT):
    """MockSUT with a synthetic 10ms per-signal delay, so concurrency shows up."""

    max_concurrent_ingestion: int = 8

    def __init__(self, name: str = "slow-mock", per_signal_ms: float = 10.0) -> None:
        super().__init__(name=name)
        self._per_signal_s = per_signal_ms / 1000.0

    async def ingest_signal(self, signal) -> None:  # type: ignore[override]
        await asyncio.sleep(self._per_signal_s)
        await super().ingest_signal(signal)


async def _bench_one(label: str, ingester, sut: SlowMockSUT, signals) -> dict:
    await ingester.ingest(sut, signals)
    wall_ms = ingester.stats.total_wall_clock_ms
    throughput = ingester.throughput_signals_per_sec()
    return {
        "impl": label,
        "wall_clock_ms": wall_ms,
        "signals_per_sec": throughput,
        "total_signals": ingester.stats.total_signals,
    }


async def _main() -> int:
    corpus = load_corpus(FIXTURE)
    # Pad for a reasonable measurement: replay signals across synthetic actors
    # so the parallel ingester can spread work beyond the (small) real author
    # pool in the mini corpus. Mini A has 3 authors with uneven distribution,
    # so we fan out via a round-robin ``author_id`` rewrite.
    from dataclasses import replace

    signals = []
    repeats = 6
    for r in range(repeats):
        for i, sig in enumerate(corpus.signals):
            # Assign signals to 8 synthetic actors so 8-way concurrency
            # actually parallelises. Each (actor) chain stays ordered by
            # (repeat, original-index) via the timestamp we clone from the
            # underlying signal.
            actor = f"actor{(i + r) % 8}"
            signals.append(sig.model_copy(update={"author_id": actor, "signal_id": f"{sig.signal_id}-r{r}"}))

    per_signal_ms = 20.0
    seq_sut = SlowMockSUT(per_signal_ms=per_signal_ms)
    await seq_sut.startup(None)  # type: ignore[arg-type]
    seq = await _bench_one("SequentialIngester", SequentialIngester(), seq_sut, signals)

    par_sut = SlowMockSUT(per_signal_ms=per_signal_ms)
    par_sut.max_concurrent_ingestion = 8
    await par_sut.startup(None)  # type: ignore[arg-type]
    par = await _bench_one(
        "ParallelIngester(8)",
        ParallelIngester(max_concurrency=8),
        par_sut,
        signals,
    )

    speedup = (seq["wall_clock_ms"] / par["wall_clock_ms"]) if par["wall_clock_ms"] > 0 else 0.0

    hdr = f"{'impl':<24} {'wall_ms':>12} {'sig/sec':>12} {'speedup':>10}"
    print(hdr)
    print("-" * len(hdr))
    for row in (seq, par):
        sp = speedup if row is par else 1.0
        print(
            f"{row['impl']:<24} {row['wall_clock_ms']:>12.2f} "
            f"{row['signals_per_sec']:>12.2f} {sp:>10.2f}"
        )

    if speedup < 1.5:
        print(f"FAIL: parallel speedup {speedup:.2f}x < 1.5x", file=sys.stderr)
        return 2
    print(f"OK: parallel speedup {speedup:.2f}x >= 1.5x")
    return 0


def main() -> int:
    return asyncio.run(_main())


if __name__ == "__main__":
    raise SystemExit(main())
