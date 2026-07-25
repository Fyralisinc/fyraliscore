"""Contracts and typed outcomes for outbound provider requests.

The provider transport is deliberately independent of HTTP and Redis. Source
clients translate their provider's response into the typed errors below, while
the ingestion layer supplies a ``QuotaCoordinator`` implementation.

Keeping these types in ``lib`` gives planners, backfills, live hydration, watch
renewal, and reconcilers one stable contract without making shared code import
an application service.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Protocol, Sequence, runtime_checkable
from uuid import uuid4

from lib.shared.errors import CompanyOSError


_POLICY_ID_RE = re.compile(r"^[a-z][a-z0-9_.:-]*$")


def _require_non_empty(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


def _require_non_negative(value: float, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a number")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"{field_name} must be finite")
    if normalized < 0:
        raise ValueError(f"{field_name} must be >= 0")
    return normalized


def _require_positive_int(value: int, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 1:
        raise ValueError(f"{field_name} must be >= 1")
    return value


@dataclass(frozen=True, slots=True)
class QuotaRequirement:
    """One weighted quota scope consumed by an outbound operation.

    A request can carry several requirements at once, for example a Discord
    global bucket plus a route bucket, or a Gmail project bucket plus a user
    bucket. ``bucket_key`` is opaque to the transport and must encode the
    provider-native quota identity selected by the source adapter.
    """

    scope: str
    bucket_key: str
    capacity: int
    refill_per_second: float
    cost: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "scope",
            _require_non_empty(self.scope, field_name="scope"),
        )
        object.__setattr__(
            self,
            "bucket_key",
            _require_non_empty(self.bucket_key, field_name="bucket_key"),
        )
        _require_positive_int(self.capacity, field_name="capacity")
        object.__setattr__(
            self,
            "refill_per_second",
            _require_non_negative(
                self.refill_per_second,
                field_name="refill_per_second",
            ),
        )
        _require_positive_int(self.cost, field_name="cost")
        if self.cost > self.capacity:
            raise ValueError("cost cannot exceed capacity")


class QuotaDenialReason(str, Enum):
    """Why the shared coordinator rejected an upstream attempt."""

    QUOTA = "quota"
    CIRCUIT_OPEN = "circuit_open"


@dataclass(frozen=True, slots=True)
class QuotaDecision:
    """Result of atomically acquiring circuit admission and quota."""

    granted: bool
    retry_after_seconds: float | None = None
    blocked_scope: str | None = None
    blocked_bucket_key: str | None = None
    denial_reason: QuotaDenialReason | None = None

    def __post_init__(self) -> None:
        if self.granted:
            if any(
                value is not None
                for value in (
                    self.retry_after_seconds,
                    self.blocked_scope,
                    self.blocked_bucket_key,
                    self.denial_reason,
                )
            ):
                raise ValueError("a granted decision cannot carry denial details")
            return
        if self.denial_reason is None:
            object.__setattr__(
                self,
                "denial_reason",
                QuotaDenialReason.QUOTA,
            )
        else:
            try:
                normalized_reason = QuotaDenialReason(self.denial_reason)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"unknown quota denial reason {self.denial_reason!r}"
                ) from exc
            object.__setattr__(self, "denial_reason", normalized_reason)
        if self.retry_after_seconds is not None:
            object.__setattr__(
                self,
                "retry_after_seconds",
                _require_non_negative(
                    self.retry_after_seconds,
                    field_name="retry_after_seconds",
                ),
            )

    @classmethod
    def allow(cls) -> "QuotaDecision":
        return cls(granted=True)

    @classmethod
    def deny(
        cls,
        *,
        retry_after_seconds: float | None,
        blocked_scope: str | None,
        blocked_bucket_key: str | None = None,
        denial_reason: QuotaDenialReason = QuotaDenialReason.QUOTA,
    ) -> "QuotaDecision":
        return cls(
            granted=False,
            retry_after_seconds=retry_after_seconds,
            blocked_scope=blocked_scope,
            blocked_bucket_key=blocked_bucket_key,
            denial_reason=denial_reason,
        )


@runtime_checkable
class QuotaCoordinator(Protocol):
    """Distributed quota seam used by :class:`ProviderTransport`."""

    async def acquire_many(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> QuotaDecision:
        """Try to consume all weighted requirements for one provider call."""

    async def report_cooldown(
        self,
        requirements: Sequence[QuotaRequirement],
        *,
        retry_after_seconds: float,
    ) -> None:
        """Share a provider-supplied cooldown with every worker replica."""

    async def report_success(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> None:
        """Record a reachable provider outcome for each concrete bucket."""

    async def report_failure(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> None:
        """Record one retryable provider failure for each concrete bucket."""


class NoopQuotaCoordinator:
    """Pass-through coordinator for providers without a certified policy."""

    async def acquire_many(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> QuotaDecision:
        del requirements
        return QuotaDecision.allow()

    async def report_cooldown(
        self,
        requirements: Sequence[QuotaRequirement],
        *,
        retry_after_seconds: float,
    ) -> None:
        del requirements, retry_after_seconds

    async def report_success(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> None:
        del requirements

    async def report_failure(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> None:
        del requirements


@dataclass(frozen=True, slots=True)
class RequestContext:
    """Stable identity and quota scope for one outbound provider request."""

    source: str
    operation: str
    tenant_id: str | None = None
    installation_id: str | None = None
    request_id: str = field(default_factory=lambda: str(uuid4()))
    idempotency_key: str | None = None
    quota_requirements: tuple[QuotaRequirement, ...] = ()
    concurrency_key: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "quota_requirements",
            tuple(self.quota_requirements),
        )
        object.__setattr__(
            self,
            "source",
            _require_non_empty(self.source, field_name="source"),
        )
        object.__setattr__(
            self,
            "operation",
            _require_non_empty(self.operation, field_name="operation"),
        )
        object.__setattr__(
            self,
            "request_id",
            _require_non_empty(self.request_id, field_name="request_id"),
        )
        if self.idempotency_key is not None:
            object.__setattr__(
                self,
                "idempotency_key",
                _require_non_empty(
                    self.idempotency_key,
                    field_name="idempotency_key",
                ),
            )
        if self.concurrency_key is not None:
            object.__setattr__(
                self,
                "concurrency_key",
                _require_non_empty(
                    self.concurrency_key,
                    field_name="concurrency_key",
                ),
            )
        bucket_keys = [item.bucket_key for item in self.quota_requirements]
        if len(bucket_keys) != len(set(bucket_keys)):
            raise ValueError(
                "quota_requirements cannot charge the same bucket_key twice",
            )

    @property
    def operation_key(self) -> str:
        """Process-local semaphore key for this operation."""
        return self.concurrency_key or f"{self.source}:{self.operation}"

    @property
    def quota_scopes(self) -> tuple[str, ...]:
        return tuple(item.scope for item in self.quota_requirements)


class RetrySafety(str, Enum):
    """Whether repeating an operation can duplicate provider-side effects."""

    IDEMPOTENT = "idempotent"
    IDEMPOTENCY_KEY = "idempotency_key"
    UNSAFE = "unsafe"


@dataclass(frozen=True, slots=True)
class RequestPolicy:
    """Bounded execution policy for one provider operation.

    ``None`` retry allowlists preserve the legacy typed-error classification.
    Supplying either tuple makes that discriminator fail closed: an observed
    status/error code must be present in its declared allowlist before the
    transport repeats the provider call.
    """

    max_attempts: int = 3
    timeout_seconds: float = 30.0
    max_elapsed_seconds: float = 60.0
    base_backoff_seconds: float = 0.5
    max_backoff_seconds: float = 30.0
    max_inline_retry_after_seconds: float = 30.0
    max_quota_wait_seconds: float = 0.0
    default_retry_later_seconds: float = 60.0
    max_concurrency: int | None = None
    retry_safety: RetrySafety = RetrySafety.IDEMPOTENT
    retryable_status_codes: tuple[int, ...] | None = None
    retryable_error_codes: tuple[str, ...] | None = None
    rate_limit_header_parser_id: str | None = None

    def __post_init__(self) -> None:
        _require_positive_int(self.max_attempts, field_name="max_attempts")
        for name in (
            "timeout_seconds",
            "max_elapsed_seconds",
            "default_retry_later_seconds",
        ):
            normalized = _require_non_negative(
                getattr(self, name),
                field_name=name,
            )
            object.__setattr__(self, name, normalized)
            if normalized <= 0:
                raise ValueError(f"{name} must be > 0")
        for name in (
            "base_backoff_seconds",
            "max_backoff_seconds",
            "max_inline_retry_after_seconds",
            "max_quota_wait_seconds",
        ):
            object.__setattr__(
                self,
                name,
                _require_non_negative(getattr(self, name), field_name=name),
            )
        if self.max_backoff_seconds < self.base_backoff_seconds:
            raise ValueError(
                "max_backoff_seconds must be >= base_backoff_seconds",
            )
        if self.max_concurrency is not None:
            _require_positive_int(
                self.max_concurrency,
                field_name="max_concurrency",
            )
        try:
            retry_safety = RetrySafety(self.retry_safety)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unknown retry_safety {self.retry_safety!r}") from exc
        object.__setattr__(self, "retry_safety", retry_safety)
        if self.retryable_status_codes is not None:
            statuses = tuple(self.retryable_status_codes)
            if len(statuses) != len(set(statuses)):
                raise ValueError("retryable_status_codes contains duplicates")
            for status in statuses:
                if (
                    isinstance(status, bool)
                    or not isinstance(status, int)
                    or not 100 <= status <= 599
                ):
                    raise ValueError(
                        "retryable_status_codes entries must be HTTP status "
                        "integers between 100 and 599"
                    )
            object.__setattr__(self, "retryable_status_codes", statuses)
        if self.retryable_error_codes is not None:
            error_codes = tuple(
                _require_non_empty(code, field_name="retryable_error_code")
                for code in self.retryable_error_codes
            )
            if len(error_codes) != len(set(error_codes)):
                raise ValueError("retryable_error_codes contains duplicates")
            for code in error_codes:
                if not _POLICY_ID_RE.fullmatch(code):
                    raise ValueError(
                        "retryable_error_codes entries must be stable "
                        "lowercase identifiers"
                    )
            object.__setattr__(self, "retryable_error_codes", error_codes)
        if self.rate_limit_header_parser_id is not None:
            parser_id = _require_non_empty(
                self.rate_limit_header_parser_id,
                field_name="rate_limit_header_parser_id",
            )
            if not _POLICY_ID_RE.fullmatch(parser_id):
                raise ValueError(
                    "rate_limit_header_parser_id must be a stable lowercase "
                    "identifier"
                )
            object.__setattr__(
                self,
                "rate_limit_header_parser_id",
                parser_id,
            )

    def allows_automatic_retry(
        self,
        *,
        error_code: str,
        status_code: int | None,
    ) -> bool:
        """Return whether this classified failure may repeat the provider call."""

        if self.retry_safety is RetrySafety.UNSAFE:
            return False
        if (
            self.retryable_error_codes is not None
            and error_code not in self.retryable_error_codes
        ):
            return False
        if (
            status_code is not None
            and self.retryable_status_codes is not None
            and status_code not in self.retryable_status_codes
        ):
            return False
        return True


class ProviderTransportError(CompanyOSError):
    """Base class for failures normalized at the provider boundary."""

    default_code = "provider_transport_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        recoverable: bool,
        **context: object,
    ) -> None:
        super().__init__(message, **context)
        if code is not None:
            self._code = code
        self._recoverable = recoverable


class ProviderTransientError(ProviderTransportError):
    """Transport or upstream failure that can succeed on a later attempt."""

    default_code = "provider_transient_error"

    def __init__(self, message: str, **context: object) -> None:
        super().__init__(message, recoverable=True, **context)


class ProviderTimeoutError(ProviderTransientError):
    """One provider attempt exceeded its operation timeout."""

    default_code = "provider_timeout"


class ProviderPermanentError(ProviderTransportError):
    """Auth, permission, validation, or not-found failure that must not retry."""

    default_code = "provider_permanent_error"

    def __init__(self, message: str, **context: object) -> None:
        super().__init__(message, recoverable=False, **context)


class ProviderRetryForbiddenError(ProviderPermanentError):
    """A classified provider failure that policy forbids repeating.

    This is deliberately non-recoverable so outer workflow machinery cannot
    silently convert an unsafe or non-allowlisted operation into another
    automated attempt. Its context retains the original failure classifier
    for explicit operator reconciliation.
    """

    default_code = "provider_retry_forbidden"


class ProviderRateLimited(ProviderTransportError):
    """Provider throttle response normalized by a source client."""

    default_code = "provider_rate_limited"

    def __init__(
        self,
        message: str = "provider rate limit",
        *,
        retry_after_seconds: float | None = None,
        status_code: int | None = 429,
        affected_scopes: Sequence[str] | None = None,
        header_parser_id: str | None = None,
        **context: object,
    ) -> None:
        if retry_after_seconds is not None:
            retry_after_seconds = _require_non_negative(
                retry_after_seconds,
                field_name="retry_after_seconds",
            )
        scopes = (
            tuple(
                _require_non_empty(item, field_name="affected_scope")
                for item in affected_scopes
            )
            if affected_scopes is not None
            else None
        )
        merged = dict(context)
        if status_code is not None:
            merged["status_code"] = status_code
        if retry_after_seconds is not None:
            merged["retry_after_seconds"] = retry_after_seconds
        if scopes is not None:
            merged["affected_scopes"] = scopes
        normalized_parser_id: str | None = None
        if header_parser_id is not None:
            normalized_parser_id = _require_non_empty(
                header_parser_id,
                field_name="header_parser_id",
            )
            if not _POLICY_ID_RE.fullmatch(normalized_parser_id):
                raise ValueError(
                    "header_parser_id must be a stable lowercase identifier"
                )
            merged["header_parser_id"] = normalized_parser_id
        super().__init__(message, recoverable=True, **merged)
        self.retry_after_seconds = retry_after_seconds
        self.status_code = status_code
        self.affected_scopes = scopes
        self.header_parser_id = normalized_parser_id


class QuotaCoordinatorError(ProviderTransientError):
    """The distributed quota backend could not make a safe decision."""

    default_code = "provider_quota_coordinator_error"


class RetryReason(str, Enum):
    """Durable reason written alongside a scheduled retry."""

    QUOTA = "quota"
    QUOTA_BACKEND = "quota_backend"
    CIRCUIT_OPEN = "circuit_open"
    RATE_LIMIT = "rate_limit"
    TRANSIENT = "transient"
    TIMEOUT = "timeout"
    CONCURRENCY = "concurrency"


class RetryLater(ProviderTransportError):
    """Durable scheduling signal; callers must not represent it as an empty page.

    Shard/workflow code catches this exception, stores ``not_before`` and
    ``reason``, and relinquishes ownership. No process should sleep for a long
    provider cooldown while holding a shard lease.
    """

    default_code = "provider_retry_later"

    def __init__(
        self,
        *,
        request_context: RequestContext,
        not_before: datetime,
        reason: RetryReason,
        retry_after_seconds: float,
        blocked_scope: str | None = None,
        blocked_bucket_key: str | None = None,
        cause_code: str | None = None,
        message: str | None = None,
    ) -> None:
        if not_before.tzinfo is None or not_before.utcoffset() is None:
            raise ValueError("not_before must be timezone-aware")
        normalized_not_before = not_before.astimezone(timezone.utc)
        normalized_delay = _require_non_negative(
            retry_after_seconds,
            field_name="retry_after_seconds",
        )
        details: dict[str, object] = {
            "source": request_context.source,
            "operation": request_context.operation,
            "request_id": request_context.request_id,
            "reason": reason.value,
            "not_before": normalized_not_before.isoformat(),
            "retry_after_seconds": normalized_delay,
            "quota_scopes": request_context.quota_scopes,
        }
        if blocked_scope is not None:
            details["blocked_scope"] = blocked_scope
        if blocked_bucket_key is not None:
            details["blocked_bucket_key"] = blocked_bucket_key
        if cause_code is not None:
            details["cause_code"] = cause_code
        super().__init__(
            message
            or (
                f"{request_context.source}.{request_context.operation} "
                f"must retry after {normalized_not_before.isoformat()} "
                f"({reason.value})"
            ),
            recoverable=True,
            **details,
        )
        self.request_context = request_context
        self.not_before = normalized_not_before
        self.reason = reason
        self.retry_after_seconds = normalized_delay
        self.blocked_scope = blocked_scope
        self.blocked_bucket_key = blocked_bucket_key
        self.cause_code = cause_code

    @classmethod
    def after(
        cls,
        *,
        request_context: RequestContext,
        delay_seconds: float,
        reason: RetryReason,
        now: datetime | None = None,
        blocked_scope: str | None = None,
        blocked_bucket_key: str | None = None,
        cause_code: str | None = None,
        message: str | None = None,
    ) -> "RetryLater":
        normalized_delay = _require_non_negative(
            delay_seconds,
            field_name="delay_seconds",
        )
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None or current.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        return cls(
            request_context=request_context,
            not_before=current.astimezone(timezone.utc)
            + timedelta(seconds=normalized_delay),
            reason=reason,
            retry_after_seconds=normalized_delay,
            blocked_scope=blocked_scope,
            blocked_bucket_key=blocked_bucket_key,
            cause_code=cause_code,
            message=message,
        )


__all__ = [
    "NoopQuotaCoordinator",
    "ProviderPermanentError",
    "ProviderRateLimited",
    "ProviderRetryForbiddenError",
    "ProviderTimeoutError",
    "ProviderTransientError",
    "ProviderTransportError",
    "QuotaCoordinator",
    "QuotaCoordinatorError",
    "QuotaDenialReason",
    "QuotaDecision",
    "QuotaRequirement",
    "RequestContext",
    "RequestPolicy",
    "RetrySafety",
    "RetryLater",
    "RetryReason",
]
