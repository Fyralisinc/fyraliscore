"""The token-bucket bounds throughput; `acquire()` blocks when tokens deplete."""

from __future__ import annotations

import asyncio

import pytest

from lsob_evaluator_l6.llm_judge.rate_limit import TokenBucket


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.slept = 0.0

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.slept += seconds
        self.now += seconds


@pytest.mark.asyncio
async def test_token_bucket_blocks_when_empty():
    clock = _FakeClock()
    # 60 req/min -> 1 req/sec; capacity 2 so first two go through instantly.
    tb = TokenBucket(
        rate_per_minute=60.0,
        capacity=2,
        time_fn=clock.time,
        sleep_fn=clock.sleep,
    )
    await tb.acquire()
    await tb.acquire()
    assert clock.slept == 0.0

    # Third request must wait for roughly 1 second of refill time.
    await tb.acquire()
    assert clock.slept == pytest.approx(1.0, abs=1e-6)


@pytest.mark.asyncio
async def test_token_bucket_refills_over_time():
    clock = _FakeClock()
    tb = TokenBucket(
        rate_per_minute=120.0,
        capacity=1,
        time_fn=clock.time,
        sleep_fn=clock.sleep,
    )
    await tb.acquire()
    # Advance the clock manually (no sleep call) and confirm tokens refill.
    clock.now += 0.5  # half a second -> 1 token at 2 req/s
    await tb.acquire()
    assert clock.slept == 0.0


def test_invalid_rate_rejected():
    with pytest.raises(ValueError):
        TokenBucket(rate_per_minute=0.0)


@pytest.mark.asyncio
async def test_token_bucket_bounds_throughput_over_window():
    """Across a simulated minute, we never exceed the configured rate."""
    clock = _FakeClock()
    tb = TokenBucket(
        rate_per_minute=30.0,  # 0.5 req/sec
        capacity=5,
        time_fn=clock.time,
        sleep_fn=clock.sleep,
    )
    for _ in range(20):
        await tb.acquire()
    # Elapsed virtual time must be at least (20 - capacity) / (rate/sec).
    # With capacity=5, we need 15 refills at 0.5/sec = 30s minimum.
    assert clock.now >= 30.0 - 1e-6
