"""Fail-closed shared-Redis diagnostics for :class:`ProviderTransport`.

This module deliberately exercises two independent transport runtimes backed
by two independent Redis client pools.  It is a bounded diagnostic, not a
throughput or release-certification claim:

* a 429 observed by replica A must publish lockouts for every affected scope;
* replica B must observe those lockouts before invoking its provider callback;
* both replicas must recover only after the shared deadline;
* weighted multi-scope acquisition must be atomic; and
* an exhausted tenant bucket must not consume shared capacity or starve a
  different tenant that still has quota.

The diagnostic uses a unique key namespace and deletes only those keys.  It
never flushes a Redis database.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from redis.asyncio import Redis

from lib.shared.provider_transport import (
    ProviderTransport,
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
    RetryLater,
    RetryReason,
    rate_limited_from_headers,
)
from services.ingest.ingestion.rate_limit.client import RateLimiter
from services.ingest.ingestion.rate_limit.provider_transport import (
    RedisQuotaCoordinator,
)


DISTRIBUTED_TRANSPORT_REDIS_ENV = "FYRALIS_CERTIFICATION_REDIS_URL"
DISTRIBUTED_TRANSPORT_DIAGNOSTIC_SCHEMA_VERSION = (
    "fyralis.provider-transport-distributed-diagnostic.v1"
)
_DEFAULT_COOLDOWN_SECONDS = 0.3


def _blocked(reason: str) -> dict[str, object]:
    return {
        "schema_version": DISTRIBUTED_TRANSPORT_DIAGNOSTIC_SCHEMA_VERSION,
        "state": "blocked",
        "reason": reason,
        "exact_assertions_passed": False,
        "assertions": {},
        "failed_assertions": ["diagnostic_not_executed"],
        "synthetic_promotion_allowed": False,
        "claim_boundary": (
            "No distributed ProviderTransport claim is made without two "
            "independent Redis-backed runtime replicas and every exact "
            "assertion passing."
        ),
    }


async def run_distributed_transport_diagnostic_from_env(
    source_id: str,
    *,
    ambient_env: Mapping[str, str],
    cooldown_seconds: float = _DEFAULT_COOLDOWN_SECONDS,
) -> dict[str, object]:
    """Run the bounded diagnostic when an isolated Redis URL is supplied."""

    redis_url = ambient_env.get(DISTRIBUTED_TRANSPORT_REDIS_ENV, "").strip()
    if not redis_url:
        return _blocked(
            f"{DISTRIBUTED_TRANSPORT_REDIS_ENV} is not configured; "
            "the shared-Redis diagnostic was not executed",
        )
    if cooldown_seconds <= 0:
        return {
            **_blocked("cooldown_seconds must be positive"),
            "state": "failed",
            "failed_assertions": ["valid_cooldown_configuration"],
        }

    replica_a = Redis.from_url(redis_url, decode_responses=False)
    replica_b = Redis.from_url(redis_url, decode_responses=False)
    namespace = (
        f"fyralis:certification:provider-transport:{source_id}:"
        f"{uuid4().hex}"
    )
    started = time.perf_counter()
    result: dict[str, object]
    try:
        ping_a, ping_b = await asyncio.gather(
            replica_a.ping(),
            replica_b.ping(),
        )
        connection_a, connection_b = await asyncio.gather(
            replica_a.client_id(),
            replica_b.client_id(),
        )
        result = await _run_distributed_transport_diagnostic(
            source_id=source_id,
            replica_a=replica_a,
            replica_b=replica_b,
            namespace=namespace,
            cooldown_seconds=cooldown_seconds,
            connection_ids=(int(connection_a), int(connection_b)),
            ping_results=(bool(ping_a), bool(ping_b)),
        )
        result["elapsed_seconds"] = time.perf_counter() - started
    except Exception as exc:  # noqa: BLE001 - retain a fail-closed artifact
        result = {
            "schema_version": (
                DISTRIBUTED_TRANSPORT_DIAGNOSTIC_SCHEMA_VERSION
            ),
            "state": "failed",
            "reason": "the shared-Redis diagnostic raised an exception",
            "error_type": type(exc).__name__,
            "exact_assertions_passed": False,
            "assertions": {},
            "failed_assertions": ["diagnostic_execution"],
            "synthetic_promotion_allowed": False,
            "elapsed_seconds": time.perf_counter() - started,
            "claim_boundary": (
                "A failed diagnostic cannot supply certification evidence."
            ),
        }
    finally:
        cleanup_succeeded = await _delete_diagnostic_keys(
            replica_a,
            namespace=namespace,
        )
        await asyncio.gather(
            replica_a.aclose(),
            replica_b.aclose(),
            return_exceptions=True,
        )
    result["scoped_key_cleanup_succeeded"] = cleanup_succeeded
    if result["state"] == "passed" and not cleanup_succeeded:
        assertions = result.get("assertions")
        if isinstance(assertions, dict):
            assertions["scoped_diagnostic_keys_deleted"] = False
        failures = result.get("failed_assertions")
        if isinstance(failures, list):
            failures.append("scoped_diagnostic_keys_deleted")
            failures.sort()
        result["state"] = "failed"
        result["exact_assertions_passed"] = False
    elif result["state"] == "passed":
        assertions = result.get("assertions")
        if isinstance(assertions, dict):
            assertions["scoped_diagnostic_keys_deleted"] = True
    return result


async def _run_distributed_transport_diagnostic(
    *,
    source_id: str,
    replica_a: Any,
    replica_b: Any,
    namespace: str,
    cooldown_seconds: float,
    connection_ids: tuple[int, int],
    ping_results: tuple[bool, bool],
) -> dict[str, object]:
    """Exercise two independently coordinated runtime replicas."""

    transport_a = ProviderTransport(
        quota_coordinator=RedisQuotaCoordinator(RateLimiter(replica_a)),
    )
    transport_b = ProviderTransport(
        quota_coordinator=RedisQuotaCoordinator(RateLimiter(replica_b)),
    )
    policy = RequestPolicy(
        max_attempts=1,
        timeout_seconds=2,
        max_elapsed_seconds=5,
        max_quota_wait_seconds=0,
        default_retry_later_seconds=1,
        retryable_status_codes=(429,),
        rate_limit_header_parser_id="http.retry_after",
    )

    cooldown_requirements = (
        QuotaRequirement(
            scope="app",
            bucket_key=f"{namespace}:cooldown:app",
            capacity=100,
            refill_per_second=100,
            cost=2,
        ),
        QuotaRequirement(
            scope="installation",
            bucket_key=f"{namespace}:cooldown:installation",
            capacity=100,
            refill_per_second=100,
            cost=1,
        ),
    )
    reporter_context = RequestContext(
        source=source_id,
        operation="certification.distributed_cooldown",
        tenant_id="certification-tenant-a",
        installation_id="certification-installation-a",
        request_id=f"{namespace}:reporter",
        quota_requirements=cooldown_requirements,
    )
    observer_context = RequestContext(
        source=source_id,
        operation="certification.distributed_cooldown",
        tenant_id="certification-tenant-a",
        installation_id="certification-installation-a",
        request_id=f"{namespace}:observer",
        quota_requirements=cooldown_requirements,
    )

    reporter_retry: RetryLater | None = None

    parsed_throttle = rate_limited_from_headers(
        {"Retry-After": str(cooldown_seconds)},
        message="diagnostic 429",
        status_code=429,
        affected_scopes=("app", "installation"),
        header_parser_id="http.retry_after",
    )

    async def _throttled_provider_call() -> None:
        raise parsed_throttle

    try:
        await transport_a.execute(
            reporter_context,
            policy,
            _throttled_provider_call,
        )
    except RetryLater as exc:
        reporter_retry = exc

    observer_callback_count = 0

    async def _observer_provider_call() -> str:
        nonlocal observer_callback_count
        observer_callback_count += 1
        return "observer-called"

    observer_retry: RetryLater | None = None
    try:
        await transport_b.execute(
            observer_context,
            policy,
            _observer_provider_call,
        )
    except RetryLater as exc:
        observer_retry = exc

    lockout_deadlines = tuple(
        await asyncio.gather(
            *(
                replica_b.hget(requirement.bucket_key, "lockout_until_ms")
                for requirement in cooldown_requirements
            )
        )
    )
    normalized_deadlines = tuple(
        int(value) if value is not None else 0 for value in lockout_deadlines
    )
    shared_deadline_ms = max(normalized_deadlines, default=0)
    before_recovery_callback_count = observer_callback_count
    sleep_seconds = max(
        0.0,
        shared_deadline_ms / 1000.0 - time.time(),
    ) + 0.025
    await asyncio.sleep(sleep_seconds)

    recovery_callback_at_ms = 0

    async def _recovered_provider_call() -> str:
        nonlocal observer_callback_count, recovery_callback_at_ms
        observer_callback_count += 1
        recovery_callback_at_ms = int(time.time() * 1000)
        return "recovered"

    recovery_result: str | None = None
    recovery_error: str | None = None
    try:
        recovery_result = await transport_b.execute(
            observer_context,
            policy,
            _recovered_provider_call,
        )
    except Exception as exc:  # noqa: BLE001 - exact assertion records failure
        recovery_error = type(exc).__name__

    lockouts_after_recovery = tuple(
        await asyncio.gather(
            *(
                replica_a.hget(requirement.bucket_key, "lockout_until_ms")
                for requirement in cooldown_requirements
            )
        )
    )

    weighted_global = QuotaRequirement(
        scope="app",
        bucket_key=f"{namespace}:weighted:app",
        capacity=6,
        refill_per_second=0,
        cost=2,
    )
    weighted_tenant_a = QuotaRequirement(
        scope="tenant",
        bucket_key=f"{namespace}:weighted:tenant:a",
        capacity=1,
        refill_per_second=0,
        cost=1,
    )
    weighted_tenant_b = QuotaRequirement(
        scope="tenant",
        bucket_key=f"{namespace}:weighted:tenant:b",
        capacity=2,
        refill_per_second=0,
        cost=1,
    )

    def _weighted_context(
        tenant: str,
        tenant_requirement: QuotaRequirement,
        request_id: str,
    ) -> RequestContext:
        return RequestContext(
            source=source_id,
            operation="certification.weighted_multi_scope",
            tenant_id=tenant,
            installation_id=f"{tenant}-installation",
            request_id=f"{namespace}:{request_id}",
            quota_requirements=(weighted_global, tenant_requirement),
        )

    weighted_callbacks: list[str] = []

    async def _weighted_call(label: str) -> str:
        weighted_callbacks.append(label)
        return label

    initial_results = await asyncio.gather(
        transport_a.execute(
            _weighted_context(
                "tenant-a",
                weighted_tenant_a,
                "weighted-a-1",
            ),
            policy,
            lambda: _weighted_call("tenant-a-1"),
        ),
        transport_b.execute(
            _weighted_context(
                "tenant-b",
                weighted_tenant_b,
                "weighted-b-1",
            ),
            policy,
            lambda: _weighted_call("tenant-b-1"),
        ),
        return_exceptions=True,
    )
    global_tokens_after_initial_raw = await replica_a.hget(
        weighted_global.bucket_key,
        "tokens",
    )
    global_tokens_after_initial = (
        float(global_tokens_after_initial_raw)
        if global_tokens_after_initial_raw is not None
        else None
    )

    tenant_a_extra_callback_count = 0

    async def _tenant_a_extra() -> str:
        nonlocal tenant_a_extra_callback_count
        tenant_a_extra_callback_count += 1
        return "tenant-a-2"

    tenant_a_denial: RetryLater | None = None
    try:
        await transport_a.execute(
            _weighted_context(
                "tenant-a",
                weighted_tenant_a,
                "weighted-a-2",
            ),
            policy,
            _tenant_a_extra,
        )
    except RetryLater as exc:
        tenant_a_denial = exc
    global_tokens_after_tenant_a_denial_raw = await replica_b.hget(
        weighted_global.bucket_key,
        "tokens",
    )
    global_tokens_after_tenant_a_denial = (
        float(global_tokens_after_tenant_a_denial_raw)
        if global_tokens_after_tenant_a_denial_raw is not None
        else None
    )

    tenant_b_second_result: str | None = None
    tenant_b_second_error: str | None = None
    try:
        tenant_b_second_result = await transport_b.execute(
            _weighted_context(
                "tenant-b",
                weighted_tenant_b,
                "weighted-b-2",
            ),
            policy,
            lambda: _weighted_call("tenant-b-2"),
        )
    except Exception as exc:  # noqa: BLE001 - exact assertion records failure
        tenant_b_second_error = type(exc).__name__
    global_tokens_after_tenant_b_progress_raw = await replica_a.hget(
        weighted_global.bucket_key,
        "tokens",
    )
    global_tokens_after_tenant_b_progress = (
        float(global_tokens_after_tenant_b_progress_raw)
        if global_tokens_after_tenant_b_progress_raw is not None
        else None
    )

    assertions = {
        "both_redis_replicas_reachable": ping_results == (True, True),
        "independent_redis_connections": (
            connection_ids[0] > 0
            and connection_ids[1] > 0
            and connection_ids[0] != connection_ids[1]
        ),
        "simultaneous_quota_scopes_declared": (
            tuple(item.scope for item in cooldown_requirements)
            == ("app", "installation")
        ),
        "reporter_received_rate_limit_retry": (
            reporter_retry is not None
            and reporter_retry.reason is RetryReason.RATE_LIMIT
        ),
        "retry_after_header_parsed_exactly": (
            parsed_throttle.retry_after_seconds == cooldown_seconds
            and parsed_throttle.header_parser_id == "http.retry_after"
        ),
        "cooldown_published_to_every_scope": (
            len(normalized_deadlines) == len(cooldown_requirements)
            and all(deadline > 0 for deadline in normalized_deadlines)
        ),
        "observer_received_shared_quota_retry": (
            observer_retry is not None
            and observer_retry.reason is RetryReason.QUOTA
        ),
        "observer_callback_not_invoked_before_cooldown": (
            before_recovery_callback_count == 0
        ),
        "observer_recovered_after_shared_deadline": (
            recovery_result == "recovered"
            and recovery_error is None
            and recovery_callback_at_ms >= shared_deadline_ms
        ),
        "cooldown_cleared_after_recovery": all(
            value is None for value in lockouts_after_recovery
        ),
        "weighted_initial_requests_granted_across_replicas": (
            initial_results == ["tenant-a-1", "tenant-b-1"]
        ),
        "weighted_global_cost_charged_exactly": (
            global_tokens_after_initial == 2.0
        ),
        "exhausted_tenant_denied_at_exact_scope": (
            tenant_a_denial is not None
            and tenant_a_denial.reason is RetryReason.QUOTA
            and tenant_a_denial.blocked_scope == "tenant"
            and tenant_a_denial.blocked_bucket_key
            == weighted_tenant_a.bucket_key
            and tenant_a_extra_callback_count == 0
        ),
        "tenant_denial_does_not_consume_shared_weighted_capacity": (
            global_tokens_after_tenant_a_denial == 2.0
            and global_tokens_after_tenant_b_progress == 0.0
            and tenant_b_second_result == "tenant-b-2"
            and tenant_b_second_error is None
        ),
    }
    failed_assertions = sorted(
        name for name, passed in assertions.items() if not passed
    )
    exact_assertions_passed = not failed_assertions
    return {
        "schema_version": DISTRIBUTED_TRANSPORT_DIAGNOSTIC_SCHEMA_VERSION,
        "state": "passed" if exact_assertions_passed else "failed",
        "exact_assertions_passed": exact_assertions_passed,
        "assertions": assertions,
        "failed_assertions": failed_assertions,
        "replica_count": 2,
        "redis_connection_ids": list(connection_ids),
        "quota_scope_count": len(cooldown_requirements),
        "quota_scopes": [
            {
                "scope": requirement.scope,
                "cost": requirement.cost,
                "capacity": requirement.capacity,
            }
            for requirement in cooldown_requirements
        ],
        "cooldown": {
            "requested_seconds": cooldown_seconds,
            "shared_deadline_ms": shared_deadline_ms,
            "scope_deadlines_ms": list(normalized_deadlines),
            "observer_callback_count_before_deadline": (
                before_recovery_callback_count
            ),
            "observer_retry_after_seconds": (
                observer_retry.retry_after_seconds
                if observer_retry is not None
                else None
            ),
            "recovery_callback_at_ms": recovery_callback_at_ms,
        },
        "weighted_tenant_isolation": {
            "global_capacity": weighted_global.capacity,
            "global_cost_per_request": weighted_global.cost,
            "global_tokens_after_initial": global_tokens_after_initial,
            "global_tokens_after_tenant_a_denial": (
                global_tokens_after_tenant_a_denial
            ),
            "global_tokens_after_tenant_b_progress": (
                global_tokens_after_tenant_b_progress
            ),
            "initial_results": [
                (
                    item
                    if isinstance(item, str)
                    else type(item).__name__
                )
                for item in initial_results
            ],
            "tenant_a_extra_callback_count": tenant_a_extra_callback_count,
            "tenant_a_blocked_scope": (
                tenant_a_denial.blocked_scope
                if tenant_a_denial is not None
                else None
            ),
            "tenant_b_second_result": tenant_b_second_result,
            "callbacks": weighted_callbacks,
        },
        "fairness_boundary": {
            "tenant_quota_isolation_proved": (
                assertions[
                    "tenant_denial_does_not_consume_shared_weighted_capacity"
                ]
            ),
            "queue_scheduler_fairness_proved": False,
            "reason": (
                "ProviderTransport owns atomic quota admission, not worker "
                "queue ordering. Fair backfill/live scheduling requires its "
                "separate durable-scheduler diagnostic."
            ),
        },
        "synthetic_promotion_allowed": False,
        "claim_boundary": (
            "This proves only shared Redis quota/cooldown behavior across two "
            "independent ProviderTransport runtime replicas. It does not "
            "prove provider-safe throughput, pipeline backlog recovery, or "
            "a real-provider canary and cannot promote certification."
        ),
    }


async def _delete_diagnostic_keys(
    redis: Any,
    *,
    namespace: str,
) -> bool:
    try:
        keys = [
            key
            async for key in redis.scan_iter(
                match=f"{namespace}:*",
                count=100,
            )
        ]
        if keys:
            await redis.delete(*keys)
        remaining = [
            key
            async for key in redis.scan_iter(
                match=f"{namespace}:*",
                count=100,
            )
        ]
        return not remaining
    except Exception:  # noqa: BLE001 - cleanup must not mask diagnostic result
        return False


__all__ = [
    "DISTRIBUTED_TRANSPORT_DIAGNOSTIC_SCHEMA_VERSION",
    "DISTRIBUTED_TRANSPORT_REDIS_ENV",
    "run_distributed_transport_diagnostic_from_env",
]
