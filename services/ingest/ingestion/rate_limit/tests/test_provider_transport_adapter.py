from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from fakeredis import aioredis as fake_aioredis

from lib.shared.provider_transport import (
    ProviderRateLimited,
    ProviderTransientError,
    ProviderTransport,
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
    RetryLater,
    RetryReason,
)
from services.ingest.ingestion.rate_limit.client import RateLimiter
from services.ingest.ingestion.rate_limit.provider_transport import (
    DistributedCircuitConfig,
    RedisQuotaCoordinator,
)


_NOW = datetime(2026, 7, 25, 12, 0, 0, tzinfo=timezone.utc)


class _Clock:
    def __init__(self, value: float) -> None:
        self.value = value

    def now(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


async def test_multi_scope_weighted_acquisition_is_atomic() -> None:
    redis = fake_aioredis.FakeRedis()
    coordinator = RedisQuotaCoordinator(RateLimiter(redis))
    requirements = (
        QuotaRequirement(
            scope="app",
            bucket_key="rate:provider:app",
            capacity=2,
            refill_per_second=0,
            cost=2,
        ),
        QuotaRequirement(
            scope="installation",
            bucket_key="rate:provider:install:1",
            capacity=1,
            refill_per_second=0,
        ),
    )
    try:
        assert (await coordinator.acquire_many(requirements)).granted is True

        denied = await coordinator.acquire_many(requirements)

        assert denied.granted is False
        assert denied.blocked_scope == "app"
        assert denied.retry_after_seconds is None
    finally:
        await redis.aclose()


async def test_later_scope_denial_does_not_consume_earlier_scope() -> None:
    redis = fake_aioredis.FakeRedis()
    limiter = RateLimiter(redis)
    coordinator = RedisQuotaCoordinator(limiter)
    requirements = (
        QuotaRequirement(
            scope="app",
            bucket_key="rate:provider:atomic:app",
            capacity=2,
            refill_per_second=0,
        ),
        QuotaRequirement(
            scope="installation",
            bucket_key="rate:provider:atomic:install",
            capacity=1,
            refill_per_second=0,
        ),
    )
    try:
        assert (await coordinator.acquire_many(requirements)).granted is True
        denied = await coordinator.acquire_many(requirements)
        assert denied.granted is False
        assert denied.blocked_scope == "installation"

        app_only = await limiter.acquire(
            "rate:provider:atomic:app",
            capacity=2,
            refill_per_sec=0,
        )
        assert app_only.granted is True
    finally:
        await redis.aclose()


async def test_two_contexts_share_provider_cooldown() -> None:
    redis = fake_aioredis.FakeRedis()
    requirement = QuotaRequirement(
        scope="installation",
        bucket_key="rate:github:installation:42",
        capacity=100,
        refill_per_second=100,
    )
    first_transport = ProviderTransport(
        quota_coordinator=RedisQuotaCoordinator(RateLimiter(redis)),
        now=lambda: _NOW,
    )
    second_transport = ProviderTransport(
        quota_coordinator=RedisQuotaCoordinator(RateLimiter(redis)),
        now=lambda: _NOW,
    )
    first_context = RequestContext(
        source="github",
        operation="repos.list",
        request_id="first-context",
        quota_requirements=(requirement,),
    )
    second_context = RequestContext(
        source="github",
        operation="repos.list",
        request_id="second-context",
        quota_requirements=(requirement,),
    )

    async def throttled_call() -> None:
        raise ProviderRateLimited(retry_after_seconds=60)

    second_called = False

    async def second_call() -> None:
        nonlocal second_called
        second_called = True

    try:
        with pytest.raises(RetryLater) as first_retry:
            await first_transport.execute(
                first_context,
                RequestPolicy(max_attempts=1),
                throttled_call,
            )
        assert first_retry.value.reason is RetryReason.RATE_LIMIT

        with pytest.raises(RetryLater) as second_retry:
            await second_transport.execute(
                second_context,
                RequestPolicy(max_quota_wait_seconds=0),
                second_call,
            )

        assert second_called is False
        assert second_retry.value.reason is RetryReason.QUOTA
        assert second_retry.value.blocked_scope == "installation"
        assert second_retry.value.retry_after_seconds > 59
    finally:
        await redis.aclose()


async def test_two_workers_share_exact_bucket_circuit_and_isolate_tenants() -> None:
    redis = fake_aioredis.FakeRedis()
    clock = _Clock(_NOW.timestamp())
    circuit = DistributedCircuitConfig(
        consecutive_failure_threshold=2,
        open_duration_seconds=30,
        half_open_probe_lease_seconds=5,
        state_retention_seconds=300,
    )
    first_transport = ProviderTransport(
        quota_coordinator=RedisQuotaCoordinator(
            RateLimiter(redis, now=clock.now),
            circuit=circuit,
        ),
        now=lambda: datetime.fromtimestamp(clock.now(), timezone.utc),
    )
    second_transport = ProviderTransport(
        quota_coordinator=RedisQuotaCoordinator(
            RateLimiter(redis, now=clock.now),
            circuit=circuit,
        ),
        now=lambda: datetime.fromtimestamp(clock.now(), timezone.utc),
    )
    tenant_a = QuotaRequirement(
        scope="workspace",
        bucket_key="rate:slack:workspace:A",
        capacity=100,
        refill_per_second=100,
    )
    tenant_b = QuotaRequirement(
        scope="workspace",
        bucket_key="rate:slack:workspace:B",
        capacity=100,
        refill_per_second=100,
    )
    policy = RequestPolicy(
        max_attempts=1,
        max_quota_wait_seconds=0,
    )

    async def unavailable() -> None:
        raise ProviderTransientError("workspace A unavailable")

    upstream_calls = 0

    async def healthy() -> str:
        nonlocal upstream_calls
        upstream_calls += 1
        return "ok"

    try:
        for transport in (first_transport, second_transport):
            with pytest.raises(RetryLater) as failure:
                await transport.execute(
                    RequestContext(
                        source="slack",
                        operation="users.info",
                        quota_requirements=(tenant_a,),
                    ),
                    policy,
                    unavailable,
                )
            assert failure.value.reason is RetryReason.TRANSIENT

        with pytest.raises(RetryLater) as open_circuit:
            await second_transport.execute(
                RequestContext(
                    source="slack",
                    operation="users.info",
                    quota_requirements=(tenant_a,),
                ),
                policy,
                healthy,
            )

        assert upstream_calls == 0
        assert open_circuit.value.reason is RetryReason.CIRCUIT_OPEN
        assert open_circuit.value.blocked_scope == "workspace"
        assert open_circuit.value.blocked_bucket_key == tenant_a.bucket_key

        # A different concrete workspace bucket remains independent.
        assert await second_transport.execute(
            RequestContext(
                source="slack",
                operation="users.info",
                quota_requirements=(tenant_b,),
            ),
            policy,
            healthy,
        ) == "ok"
        assert upstream_calls == 1
    finally:
        await redis.aclose()


async def test_half_open_probe_is_single_replica_and_success_recovers_scope() -> None:
    redis = fake_aioredis.FakeRedis()
    clock = _Clock(_NOW.timestamp())
    circuit = DistributedCircuitConfig(
        consecutive_failure_threshold=1,
        open_duration_seconds=30,
        half_open_probe_lease_seconds=5,
        state_retention_seconds=300,
    )

    def transport() -> ProviderTransport:
        return ProviderTransport(
            quota_coordinator=RedisQuotaCoordinator(
                RateLimiter(redis, now=clock.now),
                circuit=circuit,
            ),
            now=lambda: datetime.fromtimestamp(clock.now(), timezone.utc),
        )

    first_transport = transport()
    second_transport = transport()
    requirement = QuotaRequirement(
        scope="route",
        bucket_key="rate:discord:route:messages",
        capacity=100,
        refill_per_second=100,
    )
    context = RequestContext(
        source="discord",
        operation="/channels/{channel_id}/messages",
        quota_requirements=(requirement,),
    )
    policy = RequestPolicy(max_attempts=1, max_quota_wait_seconds=0)

    async def unavailable() -> None:
        raise ProviderTransientError("route unavailable")

    probe_started = asyncio.Event()
    release_probe = asyncio.Event()
    probe_calls = 0

    async def probe() -> str:
        nonlocal probe_calls
        probe_calls += 1
        probe_started.set()
        await release_probe.wait()
        return "recovered"

    try:
        with pytest.raises(RetryLater):
            await first_transport.execute(
                context,
                policy,
                unavailable,
            )

        clock.advance(31)
        first_probe = asyncio.create_task(
            first_transport.execute(context, policy, probe)
        )
        await probe_started.wait()

        second_called = False

        async def competing_probe() -> str:
            nonlocal second_called
            second_called = True
            return "unexpected"

        with pytest.raises(RetryLater) as rejected:
            await second_transport.execute(
                context,
                policy,
                competing_probe,
            )

        assert rejected.value.reason is RetryReason.CIRCUIT_OPEN
        assert second_called is False
        assert probe_calls == 1

        release_probe.set()
        assert await first_probe == "recovered"

        # The successful half-open probe closes only this concrete route.
        assert await second_transport.execute(
            context,
            policy,
            competing_probe,
        ) == "unexpected"
        assert second_called is True
    finally:
        await redis.aclose()


async def test_failed_half_open_probe_reopens_exact_scope() -> None:
    redis = fake_aioredis.FakeRedis()
    clock = _Clock(_NOW.timestamp())
    circuit = DistributedCircuitConfig(
        consecutive_failure_threshold=1,
        open_duration_seconds=30,
        half_open_probe_lease_seconds=5,
        state_retention_seconds=300,
    )

    def transport() -> ProviderTransport:
        return ProviderTransport(
            quota_coordinator=RedisQuotaCoordinator(
                RateLimiter(redis, now=clock.now),
                circuit=circuit,
            ),
            now=lambda: datetime.fromtimestamp(clock.now(), timezone.utc),
        )

    requirement = QuotaRequirement(
        scope="tenant",
        bucket_key="rate:gmail:tenant:A",
        capacity=100,
        refill_per_second=100,
    )
    context = RequestContext(
        source="gmail",
        operation="messages.get",
        quota_requirements=(requirement,),
    )
    policy = RequestPolicy(max_attempts=1, max_quota_wait_seconds=0)
    attempts = 0

    async def unavailable() -> None:
        nonlocal attempts
        attempts += 1
        raise ProviderTransientError("still unavailable")

    try:
        with pytest.raises(RetryLater):
            await transport().execute(context, policy, unavailable)

        clock.advance(31)
        with pytest.raises(RetryLater) as failed_probe:
            await transport().execute(context, policy, unavailable)
        assert failed_probe.value.reason is RetryReason.TRANSIENT
        assert attempts == 2

        with pytest.raises(RetryLater) as reopened:
            await transport().execute(context, policy, unavailable)
        assert reopened.value.reason is RetryReason.CIRCUIT_OPEN
        assert attempts == 2
    finally:
        await redis.aclose()
