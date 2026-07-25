from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

import pytest

from lib.shared.provider_transport import (
    ProviderRateLimited,
    ProviderTransientError,
    ProviderTransport,
    QuotaDecision,
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
    RetryLater,
    RetryReason,
    full_jitter_delay,
)


_NOW = datetime(2026, 7, 25, 12, 0, 0, tzinfo=timezone.utc)


class RecordingQuotaCoordinator:
    def __init__(self, *decisions: QuotaDecision) -> None:
        self.decisions = list(decisions)
        self.acquire_calls: list[tuple[QuotaRequirement, ...]] = []
        self.cooldowns: list[tuple[tuple[QuotaRequirement, ...], float]] = []
        self.successes: list[tuple[QuotaRequirement, ...]] = []
        self.failures: list[tuple[QuotaRequirement, ...]] = []

    async def acquire_many(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> QuotaDecision:
        self.acquire_calls.append(tuple(requirements))
        if self.decisions:
            return self.decisions.pop(0)
        return QuotaDecision.allow()

    async def report_cooldown(
        self,
        requirements: Sequence[QuotaRequirement],
        *,
        retry_after_seconds: float,
    ) -> None:
        self.cooldowns.append((tuple(requirements), retry_after_seconds))

    async def report_success(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> None:
        self.successes.append(tuple(requirements))

    async def report_failure(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> None:
        self.failures.append(tuple(requirements))


def _requirement(scope: str, key: str, *, cost: int = 1) -> QuotaRequirement:
    return QuotaRequirement(
        scope=scope,
        bucket_key=key,
        capacity=100,
        refill_per_second=10.0,
        cost=cost,
    )


async def test_transient_retry_uses_bounded_full_jitter_and_reacquires_quota() -> None:
    quota = RecordingQuotaCoordinator()
    sleeps: list[float] = []
    attempts = 0
    requirements = (
        _requirement("app", "rate:github:app", cost=2),
        _requirement("installation", "rate:github:install:1"),
    )
    transport = ProviderTransport(
        quota_coordinator=quota,
        sleep=lambda delay: _record_sleep(sleeps, delay),
        random_fn=lambda: 0.5,
        now=lambda: _NOW,
    )

    async def call() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ProviderTransientError("temporary upstream failure")
        return "ok"

    result = await transport.execute(
        RequestContext(
            source="github",
            operation="repos.list",
            quota_requirements=requirements,
        ),
        RequestPolicy(
            max_attempts=2,
            base_backoff_seconds=2.0,
            max_backoff_seconds=10.0,
        ),
        call,
    )

    assert result == "ok"
    assert sleeps == [1.0]
    assert quota.acquire_calls == [requirements, requirements]


async def _record_sleep(sleeps: list[float], delay: float) -> None:
    sleeps.append(delay)


async def test_rate_limit_reports_only_affected_scope_then_raises_retry_later() -> None:
    quota = RecordingQuotaCoordinator()
    app = _requirement("app", "rate:source:app")
    installation = _requirement("installation", "rate:source:install:1")
    transport = ProviderTransport(
        quota_coordinator=quota,
        now=lambda: _NOW,
    )

    async def call() -> None:
        raise ProviderRateLimited(
            retry_after_seconds=90,
            affected_scopes=("installation",),
        )

    with pytest.raises(RetryLater) as caught:
        await transport.execute(
            RequestContext(
                source="source",
                operation="objects.list",
                quota_requirements=(app, installation),
            ),
            RequestPolicy(
                max_attempts=3,
                max_inline_retry_after_seconds=30,
            ),
            call,
        )

    assert quota.cooldowns == [((installation,), 90)]
    assert caught.value.reason is RetryReason.RATE_LIMIT
    assert caught.value.retry_after_seconds == 90
    assert caught.value.not_before.timestamp() == pytest.approx(
        _NOW.timestamp() + 90,
    )


async def test_quota_denial_never_calls_provider() -> None:
    quota = RecordingQuotaCoordinator(
        QuotaDecision.deny(
            retry_after_seconds=12,
            blocked_scope="user",
        ),
    )
    transport = ProviderTransport(
        quota_coordinator=quota,
        now=lambda: _NOW,
    )
    called = False

    async def call() -> None:
        nonlocal called
        called = True

    with pytest.raises(RetryLater) as caught:
        await transport.execute(
            RequestContext(
                source="gmail",
                operation="messages.get",
                quota_requirements=(_requirement("user", "rate:gmail:user:a"),),
            ),
            RequestPolicy(max_quota_wait_seconds=0),
            call,
        )

    assert called is False
    assert caught.value.reason is RetryReason.QUOTA
    assert caught.value.blocked_scope == "user"
    assert caught.value.retry_after_seconds == 12


async def test_operation_concurrency_is_bounded() -> None:
    transport = ProviderTransport(now=lambda: _NOW)
    release = asyncio.Event()
    first_started = asyncio.Event()
    active = 0
    maximum_active = 0

    async def call() -> str:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        first_started.set()
        try:
            await release.wait()
            return "done"
        finally:
            active -= 1

    policy = RequestPolicy(
        max_concurrency=1,
        max_elapsed_seconds=5,
        timeout_seconds=5,
    )
    first = asyncio.create_task(
        transport.execute(
            RequestContext(source="notion", operation="search"),
            policy,
            call,
        ),
    )
    await first_started.wait()
    second = asyncio.create_task(
        transport.execute(
            RequestContext(source="notion", operation="search"),
            policy,
            call,
        ),
    )
    await asyncio.sleep(0)

    assert active == 1
    assert maximum_active == 1

    release.set()
    assert await asyncio.gather(first, second) == ["done", "done"]
    assert maximum_active == 1


async def test_timeout_exhaustion_becomes_retry_later() -> None:
    transport = ProviderTransport(
        random_fn=lambda: 1.0,
        now=lambda: _NOW,
    )

    async def call() -> None:
        await asyncio.sleep(1)

    with pytest.raises(RetryLater) as caught:
        await transport.execute(
            RequestContext(source="jira", operation="issues.search"),
            RequestPolicy(
                max_attempts=1,
                timeout_seconds=0.01,
                max_elapsed_seconds=1,
                base_backoff_seconds=2,
                max_backoff_seconds=2,
            ),
            call,
        )

    assert caught.value.reason is RetryReason.TIMEOUT
    assert caught.value.cause_code == "provider_timeout"


def test_full_jitter_is_bounded_and_deterministic() -> None:
    assert (
        full_jitter_delay(
            attempt_number=3,
            base_seconds=2,
            max_seconds=5,
            random_fn=lambda: 0.0,
        )
        == 0
    )
    assert (
        full_jitter_delay(
            attempt_number=3,
            base_seconds=2,
            max_seconds=5,
            random_fn=lambda: 1.0,
        )
        == 5
    )


async def test_untyped_exception_is_not_retried() -> None:
    transport = ProviderTransport(now=lambda: _NOW)
    attempts = 0

    async def call() -> Any:
        nonlocal attempts
        attempts += 1
        raise ValueError("source adapter bug")

    with pytest.raises(ValueError, match="source adapter bug"):
        await transport.execute(
            RequestContext(source="source", operation="objects.list"),
            RequestPolicy(max_attempts=5),
            call,
        )

    assert attempts == 1


class _BrokenCoordinator(RecordingQuotaCoordinator):
    def __init__(self, *, fail_action: str) -> None:
        super().__init__()
        self.fail_action = fail_action

    async def acquire_many(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> QuotaDecision:
        if self.fail_action == "acquire":
            raise ConnectionError("Redis unavailable")
        return await super().acquire_many(requirements)

    async def report_success(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> None:
        if self.fail_action == "success":
            raise ConnectionError("Redis unavailable")
        await super().report_success(requirements)

    async def report_failure(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> None:
        if self.fail_action == "failure":
            raise ConnectionError("Redis unavailable")
        await super().report_failure(requirements)


async def test_circuit_gate_coordinator_error_fails_closed_before_upstream() -> None:
    requirement = _requirement(
        "installation",
        "rate:provider:installation:one",
    )
    transport = ProviderTransport(
        quota_coordinator=_BrokenCoordinator(fail_action="acquire"),
        now=lambda: _NOW,
    )
    called = False

    async def call() -> None:
        nonlocal called
        called = True

    with pytest.raises(RetryLater) as caught:
        await transport.execute(
            RequestContext(
                source="provider",
                operation="objects.list",
                quota_requirements=(requirement,),
            ),
            RequestPolicy(),
            call,
        )

    assert called is False
    assert caught.value.reason is RetryReason.QUOTA_BACKEND


async def test_circuit_outcome_coordinator_error_prevents_inline_retry() -> None:
    requirement = _requirement(
        "installation",
        "rate:provider:installation:one",
    )
    transport = ProviderTransport(
        quota_coordinator=_BrokenCoordinator(fail_action="failure"),
        now=lambda: _NOW,
    )
    attempts = 0

    async def call() -> None:
        nonlocal attempts
        attempts += 1
        raise ProviderTransientError("provider unavailable")

    with pytest.raises(RetryLater) as caught:
        await transport.execute(
            RequestContext(
                source="provider",
                operation="objects.list",
                quota_requirements=(requirement,),
            ),
            RequestPolicy(max_attempts=3),
            call,
        )

    assert attempts == 1
    assert caught.value.reason is RetryReason.QUOTA_BACKEND
