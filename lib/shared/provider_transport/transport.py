"""Bounded, quota-aware execution for outbound provider operations."""

from __future__ import annotations

import asyncio
import math
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timezone
from typing import TypeVar

from lib.shared.provider_transport.contracts import (
    NoopQuotaCoordinator,
    ProviderPermanentError,
    ProviderRateLimited,
    ProviderRetryForbiddenError,
    ProviderTimeoutError,
    ProviderTransientError,
    ProviderTransportError,
    QuotaCoordinator,
    QuotaCoordinatorError,
    QuotaDenialReason,
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
    RetryLater,
    RetryReason,
    RetrySafety,
)


T = TypeVar("T")
SleepFn = Callable[[float], Awaitable[None]]
MonotonicFn = Callable[[], float]
NowFn = Callable[[], datetime]
RandomFn = Callable[[], float]


def full_jitter_delay(
    *,
    attempt_number: int,
    base_seconds: float,
    max_seconds: float,
    random_fn: RandomFn = random.random,
) -> float:
    """AWS-style full jitter in ``[0, min(cap, base * 2**(attempt-1))]``."""
    if attempt_number < 1:
        raise ValueError("attempt_number must be >= 1")
    if (
        not math.isfinite(base_seconds)
        or not math.isfinite(max_seconds)
        or base_seconds < 0
        or max_seconds < 0
    ):
        raise ValueError("backoff bounds must be >= 0")
    cap = min(max_seconds, base_seconds * (2 ** (attempt_number - 1)))
    sample = float(random_fn())
    if not 0.0 <= sample <= 1.0:
        raise ValueError("random_fn must return a value in [0, 1]")
    return cap * sample


class ProviderTransport:
    """Execute provider calls under one universal failure policy.

    Every actual upstream attempt reacquires every weighted quota scope.
    Backoff and quota waits happen outside the per-operation semaphore, so a
    throttled request does not occupy scarce in-flight capacity.
    """

    def __init__(
        self,
        *,
        quota_coordinator: QuotaCoordinator | None = None,
        sleep: SleepFn = asyncio.sleep,
        monotonic: MonotonicFn = time.monotonic,
        now: NowFn | None = None,
        random_fn: RandomFn = random.random,
    ) -> None:
        self._quota = quota_coordinator or NoopQuotaCoordinator()
        self._sleep = sleep
        self._monotonic = monotonic
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._random = random_fn
        self._operation_semaphores: dict[str, tuple[int, asyncio.Semaphore]] = {}

    async def execute(
        self,
        request_context: RequestContext,
        policy: RequestPolicy,
        call: Callable[[], Awaitable[T]],
    ) -> T:
        """Run ``call`` or raise a typed terminal/retry-later outcome."""
        self._validate_retry_safety(request_context, policy)
        started = self._monotonic()
        attempt = 1
        while True:
            await self._acquire_quota(
                request_context=request_context,
                policy=policy,
                started=started,
            )
            try:
                result = await self._call_once(
                    request_context=request_context,
                    policy=policy,
                    call=call,
                    started=started,
                    attempt=attempt,
                )
            except ProviderRateLimited as exc:
                await self._report_circuit_success(
                    request_context=request_context,
                    policy=policy,
                    cause=exc,
                )
                automatic_retry_allowed = self._automatic_retry_allowed(
                    policy,
                    exc,
                )
                delay = (
                    exc.retry_after_seconds
                    if exc.retry_after_seconds is not None
                    else self._backoff(policy, attempt)
                )
                requirements = self._affected_requirements(
                    request_context.quota_requirements,
                    exc.affected_scopes,
                )
                try:
                    if requirements:
                        await self._quota.report_cooldown(
                            requirements,
                            retry_after_seconds=max(0.001, delay),
                        )
                except Exception as coordination_exc:  # noqa: BLE001
                    if not automatic_retry_allowed:
                        raise self._retry_forbidden(
                            request_context,
                            policy,
                            exc,
                            cooldown_publish_error_type=type(coordination_exc).__name__,
                        ) from exc
                    quota_error = QuotaCoordinatorError(
                        "failed to publish provider cooldown",
                        source=request_context.source,
                        operation=request_context.operation,
                        error_type=type(coordination_exc).__name__,
                    )
                    raise self._retry_later(
                        request_context,
                        policy.default_retry_later_seconds,
                        RetryReason.QUOTA_BACKEND,
                        cause=quota_error,
                    ) from coordination_exc

                if not automatic_retry_allowed:
                    raise self._retry_forbidden(
                        request_context,
                        policy,
                        exc,
                    ) from exc
                if (
                    attempt >= policy.max_attempts
                    or delay > policy.max_inline_retry_after_seconds
                    or not self._delay_fits(started, policy, delay)
                ):
                    raise self._retry_later(
                        request_context,
                        delay or policy.default_retry_later_seconds,
                        RetryReason.RATE_LIMIT,
                        cause=exc,
                    ) from exc
                await self._sleep(delay)
                attempt += 1
            except ProviderTransientError as exc:
                await self._report_circuit_failure(
                    request_context=request_context,
                    policy=policy,
                    cause=exc,
                )
                if not self._automatic_retry_allowed(policy, exc):
                    raise self._retry_forbidden(
                        request_context,
                        policy,
                        exc,
                    ) from exc
                delay = self._backoff(policy, attempt)
                reason = (
                    RetryReason.TIMEOUT
                    if isinstance(exc, ProviderTimeoutError)
                    else RetryReason.TRANSIENT
                )
                if attempt >= policy.max_attempts or not self._delay_fits(
                    started, policy, delay
                ):
                    raise self._retry_later(
                        request_context,
                        delay or policy.default_retry_later_seconds,
                        reason,
                        cause=exc,
                    ) from exc
                await self._sleep(delay)
                attempt += 1
            else:
                await self._report_circuit_success(
                    request_context=request_context,
                    policy=policy,
                )
                return result

    @staticmethod
    def _validate_retry_safety(
        request_context: RequestContext,
        policy: RequestPolicy,
    ) -> None:
        if (
            policy.retry_safety is RetrySafety.IDEMPOTENCY_KEY
            and request_context.idempotency_key is None
        ):
            raise ProviderPermanentError(
                "retry-safe provider operation requires an idempotency key",
                source=request_context.source,
                operation=request_context.operation,
                request_id=request_context.request_id,
                retry_safety=policy.retry_safety.value,
            )

    @classmethod
    def _automatic_retry_allowed(
        cls,
        policy: RequestPolicy,
        error: ProviderTransportError,
    ) -> bool:
        if (
            isinstance(error, ProviderRateLimited)
            and policy.rate_limit_header_parser_id is not None
            and error.header_parser_id != policy.rate_limit_header_parser_id
        ):
            return False
        return policy.allows_automatic_retry(
            error_code=error.code,
            status_code=cls._status_code(error),
        )

    @staticmethod
    def _status_code(error: ProviderTransportError) -> int | None:
        status = getattr(error, "status_code", None)
        if status is None:
            for key in ("status_code", "http_status", "status"):
                if key in error.context:
                    status = error.context[key]
                    break
        if isinstance(status, bool):
            return None
        try:
            normalized = int(status)
        except (TypeError, ValueError):
            return None
        return normalized if 100 <= normalized <= 599 else None

    @classmethod
    def _retry_forbidden(
        cls,
        request_context: RequestContext,
        policy: RequestPolicy,
        error: ProviderTransportError,
        *,
        cooldown_publish_error_type: str | None = None,
        coordination_error_type: str | None = None,
    ) -> ProviderRetryForbiddenError:
        status_code = cls._status_code(error)
        context: dict[str, object] = {
            "source": request_context.source,
            "operation": request_context.operation,
            "request_id": request_context.request_id,
            "retry_safety": policy.retry_safety.value,
            "cause_code": error.code,
            "policy_reason": (
                "unsafe_operation"
                if policy.retry_safety is RetrySafety.UNSAFE
                else "failure_not_allowlisted"
            ),
        }
        if status_code is not None:
            context["status_code"] = status_code
        if cooldown_publish_error_type is not None:
            context["cooldown_publish_error_type"] = cooldown_publish_error_type
        if coordination_error_type is not None:
            context["coordination_error_type"] = coordination_error_type
        if isinstance(error, ProviderRateLimited):
            context["required_header_parser_id"] = policy.rate_limit_header_parser_id
            context["observed_header_parser_id"] = error.header_parser_id
        return ProviderRetryForbiddenError(
            "provider failure cannot be retried automatically",
            **context,
        )

    async def _call_once(
        self,
        *,
        request_context: RequestContext,
        policy: RequestPolicy,
        call: Callable[[], Awaitable[T]],
        started: float,
        attempt: int,
    ) -> T:
        remaining = self._remaining(started, policy)
        if remaining <= 0:
            raise self._retry_later(
                request_context,
                policy.default_retry_later_seconds,
                RetryReason.TIMEOUT,
            )

        semaphore = self._semaphore_for(request_context, policy)
        acquired = False
        if semaphore is not None:
            try:
                await asyncio.wait_for(semaphore.acquire(), timeout=remaining)
                acquired = True
            except asyncio.TimeoutError as exc:
                raise self._retry_later(
                    request_context,
                    policy.default_retry_later_seconds,
                    RetryReason.CONCURRENCY,
                ) from exc

        try:
            remaining = self._remaining(started, policy)
            timeout = min(policy.timeout_seconds, remaining)
            if timeout <= 0:
                raise ProviderTimeoutError(
                    "provider request elapsed budget exhausted",
                    source=request_context.source,
                    operation=request_context.operation,
                    attempt=attempt,
                )
            try:
                return await asyncio.wait_for(call(), timeout=timeout)
            except asyncio.TimeoutError as exc:
                raise ProviderTimeoutError(
                    "provider request timed out",
                    source=request_context.source,
                    operation=request_context.operation,
                    attempt=attempt,
                    timeout_seconds=timeout,
                ) from exc
        finally:
            if acquired and semaphore is not None:
                semaphore.release()

    async def _acquire_quota(
        self,
        *,
        request_context: RequestContext,
        policy: RequestPolicy,
        started: float,
    ) -> None:
        requirements = request_context.quota_requirements
        if not requirements:
            return
        wait_started = self._monotonic()
        while True:
            try:
                decision = await self._quota.acquire_many(requirements)
            except Exception as exc:  # noqa: BLE001
                quota_error = QuotaCoordinatorError(
                    "quota coordinator acquire failed",
                    source=request_context.source,
                    operation=request_context.operation,
                    error_type=type(exc).__name__,
                )
                raise self._retry_later(
                    request_context,
                    policy.default_retry_later_seconds,
                    RetryReason.QUOTA_BACKEND,
                    cause=quota_error,
                ) from exc
            if decision.granted:
                return

            retry_after = decision.retry_after_seconds
            if retry_after is None:
                retry_after = policy.default_retry_later_seconds
            if decision.denial_reason is QuotaDenialReason.CIRCUIT_OPEN:
                raise self._retry_later(
                    request_context,
                    retry_after,
                    RetryReason.CIRCUIT_OPEN,
                    blocked_scope=decision.blocked_scope,
                    blocked_bucket_key=decision.blocked_bucket_key,
                )
            wait = max(0.001, retry_after)
            quota_waited = self._monotonic() - wait_started
            quota_budget_left = policy.max_quota_wait_seconds - quota_waited
            if (
                policy.max_quota_wait_seconds <= 0
                or wait > quota_budget_left
                or not self._delay_fits(started, policy, wait)
            ):
                raise self._retry_later(
                    request_context,
                    retry_after,
                    RetryReason.QUOTA,
                    blocked_scope=decision.blocked_scope,
                    blocked_bucket_key=decision.blocked_bucket_key,
                )
            await self._sleep(wait)

    async def _report_circuit_success(
        self,
        *,
        request_context: RequestContext,
        policy: RequestPolicy,
        cause: ProviderRateLimited | None = None,
    ) -> None:
        requirements = request_context.quota_requirements
        if not requirements:
            return
        try:
            await self._quota.report_success(requirements)
        except Exception as exc:  # noqa: BLE001
            self._raise_circuit_coordination_failure(
                request_context=request_context,
                policy=policy,
                cause=cause,
                action="record_success",
                coordination_exc=exc,
            )

    async def _report_circuit_failure(
        self,
        *,
        request_context: RequestContext,
        policy: RequestPolicy,
        cause: ProviderTransientError,
    ) -> None:
        requirements = request_context.quota_requirements
        if not requirements:
            return
        try:
            await self._quota.report_failure(requirements)
        except Exception as exc:  # noqa: BLE001
            self._raise_circuit_coordination_failure(
                request_context=request_context,
                policy=policy,
                cause=cause,
                action="record_failure",
                coordination_exc=exc,
            )

    def _raise_circuit_coordination_failure(
        self,
        *,
        request_context: RequestContext,
        policy: RequestPolicy,
        cause: ProviderTransientError | ProviderRateLimited | None,
        action: str,
        coordination_exc: Exception,
    ) -> None:
        if policy.retry_safety is RetrySafety.UNSAFE:
            if cause is not None:
                raise self._retry_forbidden(
                    request_context,
                    policy,
                    cause,
                    coordination_error_type=type(
                        coordination_exc
                    ).__name__,
                ) from coordination_exc
            raise ProviderPermanentError(
                "provider call completed but circuit outcome was not recorded",
                source=request_context.source,
                operation=request_context.operation,
                request_id=request_context.request_id,
                coordination_action=action,
                coordination_error_type=type(coordination_exc).__name__,
                upstream_outcome="success",
                manual_reconciliation_required=True,
            ) from coordination_exc

        quota_error = QuotaCoordinatorError(
            "failed to persist provider circuit outcome",
            source=request_context.source,
            operation=request_context.operation,
            coordination_action=action,
            error_type=type(coordination_exc).__name__,
        )
        raise self._retry_later(
            request_context,
            policy.default_retry_later_seconds,
            RetryReason.QUOTA_BACKEND,
            cause=quota_error,
        ) from coordination_exc

    def _semaphore_for(
        self,
        request_context: RequestContext,
        policy: RequestPolicy,
    ) -> asyncio.Semaphore | None:
        limit = policy.max_concurrency
        if limit is None:
            return None
        key = request_context.operation_key
        current = self._operation_semaphores.get(key)
        if current is None:
            semaphore = asyncio.Semaphore(limit)
            self._operation_semaphores[key] = (limit, semaphore)
            return semaphore
        configured_limit, semaphore = current
        if configured_limit != limit:
            raise ValueError(
                f"operation {key!r} configured with conflicting concurrency "
                f"limits {configured_limit} and {limit}",
            )
        return semaphore

    def _backoff(self, policy: RequestPolicy, attempt: int) -> float:
        return full_jitter_delay(
            attempt_number=attempt,
            base_seconds=policy.base_backoff_seconds,
            max_seconds=policy.max_backoff_seconds,
            random_fn=self._random,
        )

    def _remaining(self, started: float, policy: RequestPolicy) -> float:
        return policy.max_elapsed_seconds - (self._monotonic() - started)

    def _delay_fits(
        self,
        started: float,
        policy: RequestPolicy,
        delay: float,
    ) -> bool:
        return delay <= self._remaining(started, policy)

    def _retry_later(
        self,
        request_context: RequestContext,
        delay_seconds: float,
        reason: RetryReason,
        *,
        blocked_scope: str | None = None,
        blocked_bucket_key: str | None = None,
        cause: ProviderTransientError | ProviderRateLimited | None = None,
    ) -> RetryLater:
        delay = max(0.0, float(delay_seconds))
        return RetryLater.after(
            request_context=request_context,
            delay_seconds=delay,
            reason=reason,
            now=self._now(),
            blocked_scope=blocked_scope,
            blocked_bucket_key=blocked_bucket_key,
            cause_code=cause.code if cause is not None else None,
        )

    @staticmethod
    def _affected_requirements(
        requirements: Sequence[QuotaRequirement],
        affected_scopes: Sequence[str] | None,
    ) -> tuple[QuotaRequirement, ...]:
        if affected_scopes is None:
            return tuple(requirements)
        affected = set(affected_scopes)
        return tuple(item for item in requirements if item.scope in affected)


__all__ = ["ProviderTransport", "full_jitter_delay"]
