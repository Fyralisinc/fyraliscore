from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lib.shared.provider_transport import (
    ProviderRateLimited,
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
    RetryLater,
    RetryReason,
    parse_retry_after,
    rate_limited_from_headers,
)


_NOW = datetime(2026, 7, 25, 12, 0, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("15", 15.0),
        ("0.25", 0.25),
        (5, 5.0),
        (-1, None),
        ("nan", None),
        ("not-a-delay", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_retry_after_delta_seconds(
    raw: str | int | None,
    expected: float | None,
) -> None:
    assert parse_retry_after(raw, now=_NOW) == expected


def test_parse_retry_after_http_date() -> None:
    assert (
        parse_retry_after(
            "Sat, 25 Jul 2026 12:01:30 GMT",
            now=_NOW,
        )
        == 90.0
    )
    assert (
        parse_retry_after(
            "Sat, 25 Jul 2026 11:59:00 GMT",
            now=_NOW,
        )
        == 0.0
    )


def test_rate_limited_from_headers_is_case_insensitive() -> None:
    error = rate_limited_from_headers(
        {"retry-after": "Sat, 25 Jul 2026 12:00:45 GMT"},
        now=_NOW,
        affected_scopes=("installation",),
    )

    assert isinstance(error, ProviderRateLimited)
    assert error.retry_after_seconds == 45.0
    assert error.affected_scopes == ("installation",)
    assert error.header_parser_id == "http.retry_after"
    assert error.recoverable is True


def test_request_context_rejects_duplicate_bucket_charge() -> None:
    requirement = QuotaRequirement(
        scope="installation",
        bucket_key="rate:github:install:1",
        capacity=10,
        refill_per_second=1.0,
    )

    with pytest.raises(ValueError, match="same bucket_key twice"):
        RequestContext(
            source="github",
            operation="repos.list",
            quota_requirements=(requirement, requirement),
        )


@pytest.mark.parametrize(
    "value",
    [float("nan"), float("inf"), float("-inf")],
)
def test_runtime_policy_rejects_non_finite_durations(value: float) -> None:
    with pytest.raises(ValueError, match="finite"):
        RequestPolicy(timeout_seconds=value)


def test_quota_requirement_rejects_boolean_integer_fields() -> None:
    with pytest.raises(TypeError, match="capacity must be an integer"):
        QuotaRequirement(
            scope="installation",
            bucket_key="rate:github:install:1",
            capacity=True,
            refill_per_second=1.0,
        )


def test_retry_later_exposes_durable_schedule_fields() -> None:
    context = RequestContext(
        source="slack",
        operation="conversations.history",
        request_id="request-1",
    )

    retry = RetryLater.after(
        request_context=context,
        delay_seconds=75,
        reason=RetryReason.RATE_LIMIT,
        now=_NOW,
        blocked_scope="workspace",
        cause_code="provider_rate_limited",
    )

    assert retry.not_before == datetime(
        2026,
        7,
        25,
        12,
        1,
        15,
        tzinfo=timezone.utc,
    )
    assert retry.retry_after_seconds == 75
    assert retry.reason is RetryReason.RATE_LIMIT
    assert retry.blocked_scope == "workspace"
    assert retry.context["reason"] == "rate_limit"
    assert retry.context["request_id"] == "request-1"
    assert retry.recoverable is True


def test_retry_later_rejects_naive_clock() -> None:
    context = RequestContext(source="slack", operation="history")

    with pytest.raises(ValueError, match="timezone-aware"):
        RetryLater.after(
            request_context=context,
            delay_seconds=1,
            reason=RetryReason.TRANSIENT,
            now=datetime(2026, 7, 25, 12, 0, 0),
        )
