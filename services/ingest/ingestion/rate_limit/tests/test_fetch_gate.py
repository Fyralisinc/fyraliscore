"""Tests for the FetchPage rate-limit gate (LLD §13).

The gate is the non-test importer of BUCKET_DEFAULTS that wires the Lua
token bucket into ShardFetch's fetch loop: one token per page fetch from
the (source, method) bucket, before the upstream call.

Uses fakeredis with Lua support (same harness as test_rate_limiter.py) —
no live Redis broker.
"""
from __future__ import annotations

import asyncio

import pytest
from fakeredis import aioredis as fake_aioredis

from services.ingest.ingestion.rate_limit import (
    BUCKET_DEFAULTS,
    FetchRateLimiter,
    PRIMARY_FETCH_METHOD,
    RateLimitWaitExceeded,
    RateLimiter,
    fetch_bucket_key,
)


@pytest.fixture
async def redis():
    r = fake_aioredis.FakeRedis()
    try:
        yield r
    finally:
        await r.aclose()


# ---------------------------------------------------------------------
# Happy path — a configured source acquires one token per call.
# ---------------------------------------------------------------------

async def test_acquire_grants_for_configured_source(redis):
    gate = FetchRateLimiter(RateLimiter(redis))
    granted = await gate.acquire(source="slack", tenant_id="t1")
    assert granted is True

    # The token came out of the (slack, conversations.history) bucket at
    # its BUCKET_DEFAULTS key — the gate is a real BUCKET_DEFAULTS importer.
    key = fetch_bucket_key("t1", "slack", "conversations.history")
    spec = BUCKET_DEFAULTS[("slack", "conversations.history")]
    follow = await RateLimiter(redis).acquire(
        key, capacity=spec.capacity, refill_per_sec=spec.refill_per_sec,
    )
    # One token already consumed by the gate; this second acquire sees
    # capacity-2 remaining.
    assert follow.granted is True
    assert follow.tokens_remaining == pytest.approx(spec.capacity - 2, abs=0.5)


# ---------------------------------------------------------------------
# Pass-through — a source with no published budget is never throttled.
# ---------------------------------------------------------------------

async def test_acquire_passthrough_for_unconfigured_source(redis):
    gate = FetchRateLimiter(RateLimiter(redis))
    # notion / jira / mercury / … have no PRIMARY_FETCH_METHOD entry.
    assert "notion" not in PRIMARY_FETCH_METHOD
    granted = await gate.acquire(source="notion", tenant_id="t1")
    assert granted is True
    assert FetchRateLimiter.method_for("notion") is None


# ---------------------------------------------------------------------
# Bucket key + method resolution.
# ---------------------------------------------------------------------

async def test_method_for_resolves_known_sources():
    assert FetchRateLimiter.method_for("slack") == "conversations.history"
    assert FetchRateLimiter.method_for("github") == "rest_authenticated"
    assert FetchRateLimiter.method_for("gmail") == "per-user"
    assert FetchRateLimiter.method_for("discord") == "channels_messages"
    # Every PRIMARY_FETCH_METHOD entry must have a real bucket.
    for source, method in PRIMARY_FETCH_METHOD.items():
        assert (source, method) in BUCKET_DEFAULTS, (
            f"{source}->{method} not in BUCKET_DEFAULTS"
        )


def test_fetch_bucket_key_scheme():
    assert (
        fetch_bucket_key("tenant-x", "github", "rest_authenticated")
        == "rate:tenant-x:github:rest_authenticated"
    )


# ---------------------------------------------------------------------
# Per-tenant isolation — one tenant draining its bucket doesn't starve
# another's.
# ---------------------------------------------------------------------

async def test_buckets_isolated_per_tenant(redis):
    # discord cap=30; drain tenant A's bucket fully.
    gate = FetchRateLimiter(RateLimiter(redis), max_wait_seconds=0.0)
    cap = BUCKET_DEFAULTS[("discord", "channels_messages")].capacity
    for _ in range(cap):
        assert await gate.acquire(source="discord", tenant_id="A") is True

    # A is now empty → next acquire exceeds the (zero) wait bound.
    with pytest.raises(RateLimitWaitExceeded):
        await gate.acquire(source="discord", tenant_id="A")

    # Tenant B's bucket is untouched.
    assert await gate.acquire(source="discord", tenant_id="B") is True


# ---------------------------------------------------------------------
# Exhaustion — an empty bucket past the wait bound raises (transient).
# ---------------------------------------------------------------------

async def test_acquire_raises_when_bucket_exhausted_past_bound(redis):
    gate = FetchRateLimiter(RateLimiter(redis), max_wait_seconds=0.0)
    cap = BUCKET_DEFAULTS[("slack", "conversations.history")].capacity
    for _ in range(cap):
        assert await gate.acquire(source="slack", tenant_id="t1") is True

    with pytest.raises(RateLimitWaitExceeded) as ei:
        await gate.acquire(source="slack", tenant_id="t1")
    err = ei.value
    assert err.source == "slack"
    assert err.method == "conversations.history"
    assert err.bucket_key == fetch_bucket_key(
        "t1", "slack", "conversations.history",
    )


# ---------------------------------------------------------------------
# Wait-then-grant — a denied acquire waits for refill, then grants
# within the bound (no raise).
# ---------------------------------------------------------------------

async def test_acquire_waits_for_refill_then_grants(redis):
    # discord refills at 5/sec; drain the bucket (cap 30) then a single
    # acquire should wait ~0.2s for one token and grant within the bound.
    gate = FetchRateLimiter(RateLimiter(redis), max_wait_seconds=5.0)
    cap = BUCKET_DEFAULTS[("discord", "channels_messages")].capacity
    for _ in range(cap):
        assert await gate.acquire(source="discord", tenant_id="t1") is True

    # This one must wait for refill, then grant — not raise.
    granted = await asyncio.wait_for(
        gate.acquire(source="discord", tenant_id="t1"), timeout=4.0,
    )
    assert granted is True
