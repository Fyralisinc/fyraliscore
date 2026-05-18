"""M4.1 — LeaderLock tests.

Most tests use fakeredis-lupa (M1.3 rate-limiter pattern). The TTL-
expiry test uses a real Redis via testcontainers because fakeredis's
TTL semantics around sub-second timing aren't reliable enough for a
"wait TTL+1s, assert next acquirer succeeds" assertion.

testcontainers.redis triggers a DeprecationWarning at import; pytest
is configured to treat warnings as errors. Suppressed via the same
catch_warnings pattern used in M3.3.
"""
from __future__ import annotations

import asyncio

import pytest

try:
    from fakeredis import aioredis as fake_aioredis  # type: ignore[import-not-found]
    _HAS_FAKEREDIS = True
except ImportError:
    _HAS_FAKEREDIS = False

try:
    import docker as _docker_module  # type: ignore[import-not-found]
    _HAS_DOCKER_SDK = True
except ImportError:
    _HAS_DOCKER_SDK = False

try:
    import warnings as _warnings
    with _warnings.catch_warnings():
        _warnings.simplefilter("ignore", DeprecationWarning)
        from testcontainers.redis import RedisContainer  # type: ignore[import-not-found]
    _HAS_TESTCONTAINERS = True
except ImportError:
    _HAS_TESTCONTAINERS = False


from services.integrations.discord.gateway.leader_lock import (
    DEFAULT_REFRESH_INTERVAL_SECONDS,
    DEFAULT_TTL_SECONDS,
    LEASE_KEY,
    LeaderLock,
)


pytestmark = [pytest.mark.timeout(60)]


def _docker_available() -> bool:
    if not _HAS_DOCKER_SDK:
        return False
    try:
        _docker_module.from_env().ping()
        return True
    except Exception:
        return False


# =====================================================================
# Constants — pin once-and-for-all.
# =====================================================================

def test_lock_constants_pinned():
    """The TTL + refresh-interval constants live in one place. Tests
    against the M4.1 work-order spec (30s TTL, 10s refresh)."""
    assert DEFAULT_TTL_SECONDS == 30
    assert DEFAULT_REFRESH_INTERVAL_SECONDS == 10
    assert LEASE_KEY == "gateway:discord:leader_lock"


# =====================================================================
# fakeredis-backed unit tests.
# =====================================================================

@pytest.mark.skipif(not _HAS_FAKEREDIS, reason="fakeredis with [lua] not available")
async def test_lock_acquire_when_free():
    """Empty key — acquire returns True and is_held() flips to True."""
    redis = fake_aioredis.FakeRedis()
    try:
        lock = LeaderLock(redis, ttl_seconds=5)
        assert lock.is_held() is False
        assert await lock.acquire() is True
        assert lock.is_held() is True
    finally:
        await redis.aclose()


@pytest.mark.skipif(not _HAS_FAKEREDIS, reason="fakeredis with [lua] not available")
async def test_lock_acquire_when_held_fails():
    """Two LeaderLock instances (different UUIDs); first acquires,
    second's acquire returns False."""
    redis = fake_aioredis.FakeRedis()
    try:
        lock_a = LeaderLock(redis, ttl_seconds=10, lease_value="pod-a")
        lock_b = LeaderLock(redis, ttl_seconds=10, lease_value="pod-b")
        assert await lock_a.acquire() is True
        assert await lock_b.acquire() is False
        assert lock_a.is_held() is True
        assert lock_b.is_held() is False
    finally:
        await redis.aclose()


@pytest.mark.skipif(not _HAS_FAKEREDIS, reason="fakeredis with [lua] not available")
async def test_lock_refresh_extends_ttl():
    """Acquire, observe TTL, refresh, observe extended TTL."""
    redis = fake_aioredis.FakeRedis()
    try:
        lock = LeaderLock(redis, ttl_seconds=30)
        assert await lock.acquire() is True

        # PTTL returns ms-precise remaining TTL.
        ttl_before = await redis.pttl(LEASE_KEY)
        assert ttl_before > 0

        # Sleep briefly so the TTL has actually advanced.
        await asyncio.sleep(0.05)
        assert await lock.refresh() is True

        ttl_after = await redis.pttl(LEASE_KEY)
        # After refresh, ttl should be back near the full 30000ms.
        assert ttl_after >= ttl_before, (
            f"Refresh did not extend TTL: before={ttl_before}ms "
            f"after={ttl_after}ms"
        )
    finally:
        await redis.aclose()


@pytest.mark.skipif(not _HAS_FAKEREDIS, reason="fakeredis with [lua] not available")
async def test_lock_refresh_fails_when_lost():
    """Acquire as A; manually delete the key (simulating expiry);
    A's refresh returns False and is_held flips to False."""
    redis = fake_aioredis.FakeRedis()
    try:
        lock = LeaderLock(redis, ttl_seconds=30, lease_value="pod-a")
        assert await lock.acquire() is True

        # Simulate the lease expiring (key deletion in Redis).
        await redis.delete(LEASE_KEY)

        assert await lock.refresh() is False
        assert lock.is_held() is False
    finally:
        await redis.aclose()


@pytest.mark.skipif(not _HAS_FAKEREDIS, reason="fakeredis with [lua] not available")
async def test_lock_refresh_fails_when_other_holder():
    """LOAD-BEARING SAFETY TEST.

    Acquire as A, manually overwrite the key with B's UUID, A's
    refresh returns False. This proves a holder CANNOT accidentally
    refresh someone else's lease — the Lua check-then-EXPIRE is
    atomic, and the GET sees B's value, not A's.

    Without this property: a long-paused worker (e.g. GC pause >TTL)
    whose lease expired and was re-acquired by another pod could
    extend the new holder's lease on its next refresh tick. That
    would silently break the single-holder invariant for the rest
    of the new holder's deployment.
    """
    redis = fake_aioredis.FakeRedis()
    try:
        lock_a = LeaderLock(redis, ttl_seconds=30, lease_value="pod-a-uuid")
        assert await lock_a.acquire() is True

        # B forcibly takes the key (simulating expiry + re-acquire by
        # another pod). Using `set` directly bypasses the acquire.lua
        # SET NX path — exactly the scenario refresh.lua guards.
        await redis.set(LEASE_KEY, "pod-b-uuid", ex=30)

        # A still thinks it holds the lease locally (no refresh tick
        # has fired yet), but Redis's authoritative value is B's.
        # A's refresh attempt MUST return False without extending
        # B's TTL.
        assert await lock_a.refresh() is False, (
            "LOAD-BEARING: A refreshed despite Redis holding B's "
            "lease value. The Lua check-then-EXPIRE atomicity is "
            "broken or refresh.lua is reading the wrong key."
        )
        assert lock_a.is_held() is False

        # Confirm B's lease was NOT extended by A's refresh attempt
        # (Redis-side value is still B's).
        assert await redis.get(LEASE_KEY) == b"pod-b-uuid"
    finally:
        await redis.aclose()


@pytest.mark.skipif(not _HAS_FAKEREDIS, reason="fakeredis with [lua] not available")
async def test_lock_release_only_releases_own():
    """Acquire as A. B's release returns False (B never owned it).
    Then A's release returns True."""
    redis = fake_aioredis.FakeRedis()
    try:
        lock_a = LeaderLock(redis, ttl_seconds=30, lease_value="pod-a")
        lock_b = LeaderLock(redis, ttl_seconds=30, lease_value="pod-b")

        assert await lock_a.acquire() is True
        # B's release is a no-op — the lease value matches A, not B.
        assert await lock_b.release() is False
        # A's release succeeds.
        assert await lock_a.release() is True

        # Key is now gone.
        assert await redis.get(LEASE_KEY) is None
    finally:
        await redis.aclose()


@pytest.mark.skipif(not _HAS_FAKEREDIS, reason="fakeredis with [lua] not available")
async def test_lock_release_idempotent():
    """First release returns True (we owned it). Second release
    returns False (key already gone)."""
    redis = fake_aioredis.FakeRedis()
    try:
        lock = LeaderLock(redis, ttl_seconds=30)
        assert await lock.acquire() is True
        assert await lock.release() is True
        assert await lock.release() is False
    finally:
        await redis.aclose()


# =====================================================================
# Real Redis TTL test (testcontainers).
# =====================================================================

@pytest.mark.skipif(not _HAS_TESTCONTAINERS, reason="testcontainers unavailable")
@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_lock_ttl_expires_on_holder_death():
    """Acquire, do not refresh, wait TTL+1s, second instance can
    now acquire. Real Redis because fakeredis's TTL precision is
    not reliable at the second-level resolution this test needs."""
    from redis.asyncio import Redis

    with RedisContainer("redis:7-alpine") as redis_box:
        host = redis_box.get_container_host_ip()
        port = redis_box.get_exposed_port(6379)
        redis = Redis(host=host, port=int(port), decode_responses=False)
        try:
            lock_a = LeaderLock(
                redis, ttl_seconds=2, lease_value="dead-pod-a",
            )
            lock_b = LeaderLock(
                redis, ttl_seconds=2, lease_value="taking-over-pod-b",
            )
            assert await lock_a.acquire() is True
            # B cannot acquire while A's lease is alive.
            assert await lock_b.acquire() is False

            # Holder "dies" — no refresh ticks. Sleep past the TTL.
            await asyncio.sleep(2.5)

            # B can now acquire (lease expired in Redis).
            assert await lock_b.acquire() is True
            # A's view is stale; if it tried to refresh now it would
            # see B's value and fail.
            assert await lock_a.refresh() is False
        finally:
            await redis.aclose()
