"""Tests for the Lua token-bucket rate limiter (LLD §13).

Uses fakeredis with Lua support; no live Redis broker is needed. The
Lua scripts are loaded into fakeredis exactly as the production
client loads them, so test coverage is meaningful for the script
logic itself (including the lockout vs. token-math interaction at
LLD §13 lines 2911-2914).

The atomicity test (`test_rate_limiter_concurrent_acquires_serialize`)
is the load-bearing one: if Lua's EVAL/EVALSHA serialisation does
not hold, the bucket admits more grants than its capacity allows.
"""
from __future__ import annotations

import asyncio

import pytest
from fakeredis import aioredis as fake_aioredis

from services.ingestion.rate_limit import RateLimiter


@pytest.fixture
async def limiter():
    """Fresh RateLimiter + fakeredis instance per test.

    fakeredis Lua support requires the `[lua]` extra (lupa). Verified
    in M1.3 installation. If the dep is missing, evalsha raises
    NotImplementedError and the test fails loudly.
    """
    redis = fake_aioredis.FakeRedis()
    rl = RateLimiter(redis)
    try:
        yield rl
    finally:
        await redis.aclose()


# ---------------------------------------------------------------------
# Lua acquire — happy path.
# ---------------------------------------------------------------------

async def test_lua_acquire_grants_when_tokens_available(limiter: RateLimiter):
    result = await limiter.acquire(
        "rate:t1:slack:conversations.history",
        capacity=40,
        refill_per_sec=0.67,
    )
    assert result.granted is True
    # Initial state: bucket starts full at capacity; one cost=1 acquire
    # leaves 39 tokens.
    assert result.tokens_remaining == pytest.approx(39.0, abs=0.01)
    assert result.retry_after_ms == 0


# ---------------------------------------------------------------------
# Lua acquire — exhaustion path.
# ---------------------------------------------------------------------

async def test_lua_acquire_denies_when_exhausted(limiter: RateLimiter):
    # Drain a small bucket. cap=2 / refill=0.01 (1 token per 100s)
    # leaves effectively no refill within the test window.
    for _ in range(2):
        ok = await limiter.acquire(
            "rate:t1:s:m",
            capacity=2,
            refill_per_sec=0.01,
        )
        assert ok.granted is True

    # Third acquire must deny. retry_after_ms = ceil(deficit/refill *
    # 1000); with refill=0.01 the math yields a large finite ms value.
    denied = await limiter.acquire(
        "rate:t1:s:m",
        capacity=2,
        refill_per_sec=0.01,
    )
    assert denied.granted is False
    assert denied.retry_after_ms > 0


# ---------------------------------------------------------------------
# Lua refill math — wall-clock based. Sleep + reacquire must see the
# bucket grow. This test uses a high refill rate so the assertion
# tolerates Python's scheduler jitter.
# ---------------------------------------------------------------------

async def test_lua_token_refill_math(limiter: RateLimiter):
    # cap=10, refill=1000/sec (effectively instant). One acquire then a
    # 50ms sleep should refill far more than we consumed.
    key = "rate:t1:fast:m"
    first = await limiter.acquire(key, capacity=10, refill_per_sec=1000.0)
    assert first.granted is True
    assert first.tokens_remaining == pytest.approx(9.0, abs=0.01)

    await asyncio.sleep(0.05)  # 50 ms ≥ 1 token refill worth at 1000/sec

    second = await limiter.acquire(key, capacity=10, refill_per_sec=1000.0)
    assert second.granted is True
    # After 50ms at 1000 tokens/sec, refill would be 50 tokens but cap=10.
    # So the bucket should be full (capacity - cost = 9).
    assert second.tokens_remaining == pytest.approx(9.0, abs=0.01)


# ---------------------------------------------------------------------
# Lua lockout — overrides token math.
# ---------------------------------------------------------------------

async def test_lua_lockout_overrides_token_math(limiter: RateLimiter):
    """Even with a fully-stocked bucket, a 5000ms lockout must deny
    every acquire until the lockout expires. This is the path the
    fetcher takes when the source returns 429.
    """
    key = "rate:t1:locked:m"
    # Set a 5-second lockout via report_retry_after.
    await limiter.report_retry_after(key, retry_after_ms=5000)

    # Bucket starts full at capacity; without the lockout, this would
    # grant immediately. With the lockout, must deny.
    result = await limiter.acquire(key, capacity=100, refill_per_sec=10.0)
    assert result.granted is False
    assert result.retry_after_ms >= 4900, (
        "lockout retry_after must reflect remaining lockout window, "
        f"got {result.retry_after_ms}ms"
    )


# ---------------------------------------------------------------------
# Lua lockout — expires.
# ---------------------------------------------------------------------

async def test_lua_lockout_expires(limiter: RateLimiter):
    """A 100ms lockout must clear after the wall-clock window passes."""
    key = "rate:t1:expire:m"
    await limiter.report_retry_after(key, retry_after_ms=100)
    # Within the window — denied.
    denied = await limiter.acquire(key, capacity=10, refill_per_sec=10.0)
    assert denied.granted is False

    # Past the window — granted.
    await asyncio.sleep(0.15)
    granted = await limiter.acquire(key, capacity=10, refill_per_sec=10.0)
    assert granted.granted is True


# ---------------------------------------------------------------------
# Atomicity — the load-bearing test. If Lua serialisation breaks, this
# bucket admits more than its capacity.
# ---------------------------------------------------------------------

async def test_rate_limiter_concurrent_acquires_serialize(limiter: RateLimiter):
    """10 concurrent acquires against a bucket sized 5; exactly 5
    must grant and 5 must deny. refill_per_sec=0.01 (1 token per
    100s) means no measurable refill within the test window, so any
    extra grant is necessarily a Lua-atomicity break, not a refill
    artefact.
    """
    key = "rate:t1:concurrent:m"
    capacity = 5

    async def one_try() -> bool:
        r = await limiter.acquire(
            key,
            capacity=capacity,
            refill_per_sec=0.01,
        )
        return r.granted

    results = await asyncio.gather(*(one_try() for _ in range(10)))
    granted_count = sum(1 for g in results if g)
    denied_count = sum(1 for g in results if not g)
    assert granted_count == capacity, (
        f"expected exactly {capacity} grants, got {granted_count}. "
        f"If this fails, Lua atomicity is broken or refill_per_sec=0 "
        f"is being interpreted as something else."
    )
    assert denied_count == 10 - capacity


# ---------------------------------------------------------------------
# Cross-bucket independence — sanity check that buckets keyed on
# different tenant/source pairs don't share tokens.
# ---------------------------------------------------------------------

# ---------------------------------------------------------------------
# refill_per_sec=0 sentinel — `acquire.lua` returns retry_after_ms=-1
# when the bucket has insufficient tokens and refill is zero. Tests
# both the sentinel emission and the report_retry_after clearance
# path that callers must use to recover.
# ---------------------------------------------------------------------

async def test_lua_acquire_zero_refill_denies_with_sentinel(limiter: RateLimiter):
    """Drain a bucket configured with refill=0, then assert the next
    acquire returns the indefinite-lockout sentinel (retry_after_ms=-1)
    rather than a positive ms count or, worse, math.huge.
    """
    key = "rate:t1:zero:m"
    # Drain capacity.
    for _ in range(2):
        ok = await limiter.acquire(key, capacity=2, refill_per_sec=0.0)
        assert ok.granted is True

    denied = await limiter.acquire(key, capacity=2, refill_per_sec=0.0)
    assert denied.granted is False
    assert denied.retry_after_ms == -1, (
        "Zero-refill empty bucket must return the indefinite-lockout "
        f"sentinel -1, not {denied.retry_after_ms}. See acquire.lua "
        "header for sentinel semantics."
    )


async def test_lua_zero_refill_cleared_by_report_retry_after(
    limiter: RateLimiter,
):
    """The sentinel says 'never recovers on its own'. Callers can
    still bound the lockout by calling report_retry_after with a
    finite duration. After that call, acquire must reflect the
    finite window (a positive retry_after_ms), NOT the -1 sentinel.
    """
    key = "rate:t1:zero_recover:m"
    # Drain to the sentinel state.
    for _ in range(2):
        await limiter.acquire(key, capacity=2, refill_per_sec=0.0)
    denied = await limiter.acquire(key, capacity=2, refill_per_sec=0.0)
    assert denied.retry_after_ms == -1

    # Caller imposes a finite lockout. report_retry_after.lua's HMSET
    # of lockout_until_ms takes precedence over the token-math branch
    # (acquire.lua step 1: lockout check runs before the refill==0
    # branch).
    await limiter.report_retry_after(key, retry_after_ms=500)

    bounded = await limiter.acquire(key, capacity=2, refill_per_sec=0.0)
    assert bounded.granted is False
    assert bounded.retry_after_ms > 0, (
        "After report_retry_after with a finite duration, the lockout "
        "must dominate the zero-refill sentinel. Got "
        f"retry_after_ms={bounded.retry_after_ms}."
    )
    assert bounded.retry_after_ms <= 500, (
        "Bounded by the supplied retry_after_ms (500). Got "
        f"{bounded.retry_after_ms}."
    )


async def test_buckets_isolated_by_key(limiter: RateLimiter):
    # Drain tenant A's bucket.
    for _ in range(2):
        ok = await limiter.acquire("rate:A:s:m", capacity=2, refill_per_sec=0.01)
        assert ok.granted is True
    denied = await limiter.acquire("rate:A:s:m", capacity=2, refill_per_sec=0.01)
    assert denied.granted is False

    # Tenant B's bucket is untouched.
    ok_b = await limiter.acquire("rate:B:s:m", capacity=2, refill_per_sec=0.01)
    assert ok_b.granted is True
