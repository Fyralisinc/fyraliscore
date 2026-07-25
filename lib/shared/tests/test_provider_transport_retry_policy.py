from __future__ import annotations

from collections.abc import Sequence

import pytest

from lib.shared.provider_transport import (
    ProviderPermanentError,
    ProviderRateLimited,
    ProviderRetryForbiddenError,
    ProviderTransientError,
    ProviderTransport,
    QuotaDecision,
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
    RetrySafety,
)


class _RecordingQuota:
    def __init__(self) -> None:
        self.acquisitions = 0
        self.cooldowns: list[float] = []

    async def acquire_many(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> QuotaDecision:
        assert requirements
        self.acquisitions += 1
        return QuotaDecision.allow()

    async def report_cooldown(
        self,
        requirements: Sequence[QuotaRequirement],
        *,
        retry_after_seconds: float,
    ) -> None:
        assert requirements
        self.cooldowns.append(retry_after_seconds)

    async def report_success(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> None:
        assert requirements

    async def report_failure(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> None:
        assert requirements


async def _record_sleep(sleeps: list[float], delay: float) -> None:
    sleeps.append(delay)


async def test_unsafe_non_idempotent_operation_never_repeats_provider_call() -> None:
    attempts = 0
    sleeps: list[float] = []
    transport = ProviderTransport(
        sleep=lambda delay: _record_sleep(sleeps, delay),
    )

    async def call() -> None:
        nonlocal attempts
        attempts += 1
        raise ProviderTransientError(
            "ambiguous write failure",
            status_code=503,
        )

    with pytest.raises(ProviderRetryForbiddenError) as caught:
        await transport.execute(
            RequestContext(source="provider", operation="objects.create"),
            RequestPolicy(
                max_attempts=5,
                retry_safety=RetrySafety.UNSAFE,
            ),
            call,
        )

    assert attempts == 1
    assert sleeps == []
    assert caught.value.recoverable is False
    assert caught.value.context["cause_code"] == "provider_transient_error"
    assert caught.value.context["status_code"] == 503


async def test_unsafe_rate_limit_publishes_cooldown_without_retrying() -> None:
    quota = _RecordingQuota()
    attempts = 0
    requirement = QuotaRequirement(
        scope="installation",
        bucket_key="provider:installation:one",
        capacity=10,
        refill_per_second=1,
    )
    transport = ProviderTransport(quota_coordinator=quota)

    async def call() -> None:
        nonlocal attempts
        attempts += 1
        raise ProviderRateLimited(retry_after_seconds=15)

    with pytest.raises(ProviderRetryForbiddenError) as caught:
        await transport.execute(
            RequestContext(
                source="provider",
                operation="payments.create",
                quota_requirements=(requirement,),
            ),
            RequestPolicy(
                max_attempts=5,
                retry_safety=RetrySafety.UNSAFE,
            ),
            call,
        )

    assert attempts == 1
    assert quota.acquisitions == 1
    assert quota.cooldowns == [15]
    assert caught.value.context["cause_code"] == "provider_rate_limited"


async def test_idempotency_key_policy_fails_before_quota_or_provider_call() -> None:
    quota = _RecordingQuota()
    called = False
    requirement = QuotaRequirement(
        scope="installation",
        bucket_key="provider:installation:one",
        capacity=10,
        refill_per_second=1,
    )

    async def call() -> None:
        nonlocal called
        called = True

    with pytest.raises(ProviderPermanentError, match="idempotency key"):
        await ProviderTransport(quota_coordinator=quota).execute(
            RequestContext(
                source="provider",
                operation="payments.create",
                quota_requirements=(requirement,),
            ),
            RequestPolicy(retry_safety=RetrySafety.IDEMPOTENCY_KEY),
            call,
        )

    assert called is False
    assert quota.acquisitions == 0


async def test_idempotency_key_allows_declared_safe_retry() -> None:
    attempts = 0

    async def call() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ProviderTransientError("temporary")
        return "ok"

    result = await ProviderTransport(random_fn=lambda: 0).execute(
        RequestContext(
            source="provider",
            operation="payments.create",
            idempotency_key="payment-attempt-1",
        ),
        RequestPolicy(
            max_attempts=2,
            retry_safety=RetrySafety.IDEMPOTENCY_KEY,
        ),
        call,
    )

    assert result == "ok"
    assert attempts == 2


@pytest.mark.parametrize(
    ("status_code", "expected_attempts"),
    [(502, 2), (409, 1)],
)
async def test_retryable_status_allowlist_is_enforced(
    status_code: int,
    expected_attempts: int,
) -> None:
    attempts = 0

    async def call() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ProviderTransientError(
                "HTTP failure",
                status_code=status_code,
            )
        return "ok"

    policy = RequestPolicy(
        max_attempts=2,
        retryable_status_codes=(502,),
    )
    if expected_attempts == 1:
        with pytest.raises(ProviderRetryForbiddenError) as caught:
            await ProviderTransport().execute(
                RequestContext(source="provider", operation="objects.list"),
                policy,
                call,
            )
        assert caught.value.context["status_code"] == status_code
    else:
        assert (
            await ProviderTransport(random_fn=lambda: 0).execute(
                RequestContext(
                    source="provider",
                    operation="objects.list",
                ),
                policy,
                call,
            )
            == "ok"
        )

    assert attempts == expected_attempts


async def test_retryable_error_code_allowlist_is_enforced() -> None:
    attempts = 0

    async def call() -> None:
        nonlocal attempts
        attempts += 1
        raise ProviderTransientError(
            "provider-specific failure",
            code="provider_busy",
        )

    with pytest.raises(ProviderRetryForbiddenError) as caught:
        await ProviderTransport().execute(
            RequestContext(source="provider", operation="objects.list"),
            RequestPolicy(
                max_attempts=3,
                retryable_error_codes=("provider_timeout",),
            ),
            call,
        )

    assert caught.value.code == "provider_retry_forbidden"
    assert caught.value.context["cause_code"] == "provider_busy"
    assert attempts == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"retry_safety": "unknown"},
        {"retryable_status_codes": (429, 429)},
        {"retryable_status_codes": (99,)},
        {"retryable_error_codes": ("Provider Busy",)},
        {"rate_limit_header_parser_id": "Retry After"},
    ],
)
def test_retry_policy_rejects_ambiguous_declarations(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        RequestPolicy(**kwargs)  # type: ignore[arg-type]


def test_default_request_policy_remains_legacy_compatible() -> None:
    policy = RequestPolicy()

    assert policy.retry_safety is RetrySafety.IDEMPOTENT
    assert policy.retryable_status_codes is None
    assert policy.retryable_error_codes is None
    assert policy.rate_limit_header_parser_id is None


def test_rate_limit_header_parser_identity_is_retained() -> None:
    policy = RequestPolicy(
        rate_limit_header_parser_id="github.primary_secondary",
    )

    assert policy.rate_limit_header_parser_id == "github.primary_secondary"


async def test_declared_header_parser_identity_is_enforced() -> None:
    attempts = 0

    async def call() -> None:
        nonlocal attempts
        attempts += 1
        raise ProviderRateLimited(
            retry_after_seconds=0,
            header_parser_id="discord.dynamic_bucket",
        )

    with pytest.raises(ProviderRetryForbiddenError) as caught:
        await ProviderTransport().execute(
            RequestContext(source="provider", operation="objects.list"),
            RequestPolicy(
                max_attempts=2,
                rate_limit_header_parser_id="http.retry_after",
            ),
            call,
        )

    assert attempts == 1
    assert caught.value.context["required_header_parser_id"] == ("http.retry_after")
    assert caught.value.context["observed_header_parser_id"] == (
        "discord.dynamic_bucket"
    )
