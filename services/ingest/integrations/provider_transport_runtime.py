"""Production wiring for the universal provider transport.

Quota numbers are never embedded here.  Deployments supply the verified
operation matrix through ``FYRALIS_PROVIDER_QUOTAS_JSON`` and Redis supplies
shared state across replicas.  Missing source/operation entries fail before an
HTTP request rather than silently becoming an unmetered provider call.

Configuration shape::

    {
      "slack": {
        "users.info": [
          {
            "scope": "workspace",
            "identity": "workspace",
            "capacity": "<verified integer>",
            "refill_per_second": "<verified number>",
            "cost": "<verified integer>",
            "evidence_ref": "<versioned URL or evidence-pack reference>",
            "verified_on": "<YYYY-MM-DD>"
          }
        ]
      }
    }

``identity`` is one of ``global``, ``tenant``, ``installation``,
``operation``, or a provider-native dimension supplied by the client (for
example ``workspace``, ``user``, ``route``, or ``provider_installation``).
The placeholders deliberately prevent this example from becoming an
accidental quota policy; this module does not ship or select provider limits.
Production (and callers using ``required=True``) rejects declarations without
both evidence fields. A verification date may not be in the future.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

from lib.shared.env import is_prod
from lib.shared.provider_transport import (
    ProviderPermanentError,
    ProviderTransport,
    QuotaRequirement,
)
from redis.asyncio import Redis
from services.ingest.ingestion.rate_limit.client import RateLimiter
from services.ingest.ingestion.rate_limit.provider_transport import (
    RedisQuotaCoordinator,
)
from services.ingest.source_contract.catalog import (
    PROVIDER_TRANSPORT_OPERATION_CATALOG,
)


_QUOTA_ENV = "FYRALIS_PROVIDER_QUOTAS_JSON"


@dataclass(frozen=True, slots=True)
class _QuotaRule:
    scope: str
    identity: str
    capacity: int
    refill_per_second: float
    cost: int
    evidence_ref: str | None
    verified_on: date | None


@dataclass(slots=True)
class ProviderTransportRuntime:
    transport: ProviderTransport
    quota_resolver: Any
    redis: Redis
    closed: bool = False

    async def aclose(self) -> None:
        if self.closed:
            return
        await self.redis.aclose()
        self.closed = True


class _DeclaredQuotaResolver:
    def __init__(
        self,
        declarations: Mapping[str, Mapping[str, tuple[_QuotaRule, ...]]],
    ) -> None:
        self._declarations = declarations

    def __call__(
        self,
        source: str,
        operation: str,
        tenant_id: str | None,
        installation_id: str | None,
        dimensions: Mapping[str, str],
    ) -> Sequence[QuotaRequirement]:
        rules = self._declarations.get(source, {}).get(operation)
        if rules is None:
            raise ProviderPermanentError(
                "provider operation is missing a verified quota declaration",
                source=source,
                operation=operation,
            )
        requirements: list[QuotaRequirement] = []
        for rule in rules:
            identity = _resolve_identity(
                rule.identity,
                source=source,
                operation=operation,
                tenant_id=tenant_id,
                installation_id=installation_id,
                dimensions=dimensions,
            )
            requirements.append(
                QuotaRequirement(
                    scope=rule.scope,
                    bucket_key=f"provider-quota:{source}:{rule.scope}:{identity}",
                    capacity=rule.capacity,
                    refill_per_second=rule.refill_per_second,
                    cost=rule.cost,
                )
            )
        if not requirements:
            raise ProviderPermanentError(
                "provider operation has an empty quota declaration",
                source=source,
                operation=operation,
            )
        return tuple(requirements)


def _resolve_identity(
    identity: str,
    *,
    source: str,
    operation: str,
    tenant_id: str | None,
    installation_id: str | None,
    dimensions: Mapping[str, str],
) -> str:
    if identity == "global":
        return "global"
    if identity == "tenant":
        value = tenant_id
    elif identity == "installation":
        value = installation_id
    elif identity == "operation":
        value = operation
    else:
        value = dimensions.get(identity)
    if not value:
        raise ProviderPermanentError(
            "provider quota identity is unavailable",
            source=source,
            operation=operation,
            quota_identity=identity,
        )
    return value


def _parse_declarations(
    raw: str,
    *,
    require_evidence: bool = False,
    today: date | None = None,
) -> dict[str, dict[str, tuple[_QuotaRule, ...]]]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{_QUOTA_ENV} is not valid JSON") from exc
    if not isinstance(payload, dict) or not payload:
        raise RuntimeError(f"{_QUOTA_ENV} must be a non-empty object")
    result: dict[str, dict[str, tuple[_QuotaRule, ...]]] = {}
    for source, operations in payload.items():
        if not isinstance(source, str) or not source.strip():
            raise RuntimeError(f"{_QUOTA_ENV} contains an invalid source")
        if not isinstance(operations, dict) or not operations:
            raise RuntimeError(
                f"{_QUOTA_ENV}.{source} must be a non-empty operation object",
            )
        parsed_operations: dict[str, tuple[_QuotaRule, ...]] = {}
        for operation, entries in operations.items():
            if not isinstance(operation, str) or not operation.strip():
                raise RuntimeError(
                    f"{_QUOTA_ENV}.{source} contains an invalid operation",
                )
            if not isinstance(entries, list) or not entries:
                raise RuntimeError(
                    f"{_QUOTA_ENV}.{source}.{operation} must be a non-empty list",
                )
            parsed_operations[operation] = tuple(
                _parse_rule(
                    source,
                    operation,
                    index,
                    entry,
                    require_evidence=require_evidence,
                    today=today,
                )
                for index, entry in enumerate(entries)
            )
        result[source] = parsed_operations
    _validate_operation_membership(result)
    return result


def _validate_operation_membership(
    declarations: Mapping[str, Mapping[str, tuple[_QuotaRule, ...]]],
) -> None:
    required_catalog = PROVIDER_TRANSPORT_OPERATION_CATALOG
    unknown_sources = set(declarations) - set(required_catalog)
    if unknown_sources:
        raise RuntimeError(
            f"{_QUOTA_ENV} has undeclared sources: {sorted(unknown_sources)}",
        )
    missing_sources = set(required_catalog) - set(declarations)
    if missing_sources:
        raise RuntimeError(
            f"{_QUOTA_ENV} is missing transport-enforced sources: "
            f"{sorted(missing_sources)}",
        )
    for source, required in required_catalog.items():
        configured = set(declarations[source])
        unknown = configured - required
        missing = required - configured
        if unknown:
            raise RuntimeError(
                f"{_QUOTA_ENV}.{source} has undeclared operations: "
                f"{sorted(unknown)}",
            )
        if missing:
            raise RuntimeError(
                f"{_QUOTA_ENV}.{source} is missing required operations: "
                f"{sorted(missing)}",
            )


def _parse_rule(
    source: str,
    operation: str,
    index: int,
    entry: object,
    *,
    require_evidence: bool,
    today: date | None,
) -> _QuotaRule:
    path = f"{_QUOTA_ENV}.{source}.{operation}[{index}]"
    if not isinstance(entry, dict):
        raise RuntimeError(f"{path} must be an object")
    allowed = {
        "scope",
        "identity",
        "capacity",
        "refill_per_second",
        "cost",
        "evidence_ref",
        "verified_on",
    }
    unknown = set(entry) - allowed
    if unknown:
        raise RuntimeError(f"{path} has unknown fields: {sorted(unknown)}")
    scope = entry.get("scope")
    identity = entry.get("identity")
    capacity = entry.get("capacity")
    refill = entry.get("refill_per_second")
    cost = entry.get("cost", 1)
    evidence_ref = entry.get("evidence_ref")
    verified_on = entry.get("verified_on")
    if not isinstance(scope, str) or not scope.strip():
        raise RuntimeError(f"{path}.scope must be non-empty")
    if not isinstance(identity, str) or not identity.strip():
        raise RuntimeError(f"{path}.identity must be non-empty")
    if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
        raise RuntimeError(f"{path}.capacity must be a positive integer")
    if (
        isinstance(refill, bool)
        or not isinstance(refill, (int, float))
        or not math.isfinite(float(refill))
        or float(refill) < 0
    ):
        raise RuntimeError(f"{path}.refill_per_second must be non-negative")
    if isinstance(cost, bool) or not isinstance(cost, int) or cost < 1:
        raise RuntimeError(f"{path}.cost must be a positive integer")
    if cost > capacity:
        raise RuntimeError(f"{path}.cost cannot exceed capacity")
    if (evidence_ref is None) != (verified_on is None):
        raise RuntimeError(
            f"{path} must declare evidence_ref and verified_on together",
        )
    if require_evidence and evidence_ref is None:
        raise RuntimeError(
            f"{path} is missing verified quota evidence "
            "(evidence_ref and verified_on)",
        )
    normalized_evidence_ref: str | None = None
    normalized_verified_on: date | None = None
    if evidence_ref is not None:
        if not isinstance(evidence_ref, str) or not evidence_ref.strip():
            raise RuntimeError(f"{path}.evidence_ref must be non-empty")
        normalized_evidence_ref = evidence_ref.strip()
        if not isinstance(verified_on, str):
            raise RuntimeError(f"{path}.verified_on must be YYYY-MM-DD")
        try:
            normalized_verified_on = date.fromisoformat(verified_on)
        except ValueError as exc:
            raise RuntimeError(
                f"{path}.verified_on must be YYYY-MM-DD",
            ) from exc
        verification_day = today or date.today()
        if normalized_verified_on > verification_day:
            raise RuntimeError(
                f"{path}.verified_on cannot be in the future",
            )
    return _QuotaRule(
        scope=scope,
        identity=identity,
        capacity=capacity,
        refill_per_second=float(refill),
        cost=cost,
        evidence_ref=normalized_evidence_ref,
        verified_on=normalized_verified_on,
    )


_RUNTIME: ProviderTransportRuntime | None = None


def get_provider_transport_runtime(
    *,
    required: bool | None = None,
) -> ProviderTransportRuntime | None:
    """Return the process runtime, requiring verified config in production."""

    global _RUNTIME
    if _RUNTIME is not None:
        return _RUNTIME
    effective_required = is_prod() if required is None else required
    raw = os.environ.get(_QUOTA_ENV, "").strip()
    redis_url = os.environ.get("REDIS_URL", "").strip()
    if not raw or not redis_url:
        if effective_required:
            missing = [
                name
                for name, value in (
                    (_QUOTA_ENV, raw),
                    ("REDIS_URL", redis_url),
                )
                if not value
            ]
            raise RuntimeError(
                "provider transport runtime is incomplete; missing "
                + ", ".join(missing),
            )
        return None
    declarations = _parse_declarations(
        raw,
        require_evidence=effective_required,
    )
    redis = Redis.from_url(redis_url, decode_responses=False)
    coordinator = RedisQuotaCoordinator(RateLimiter(redis))
    _RUNTIME = ProviderTransportRuntime(
        transport=ProviderTransport(quota_coordinator=coordinator),
        quota_resolver=_DeclaredQuotaResolver(declarations),
        redis=redis,
    )
    return _RUNTIME


def reset_provider_transport_runtime_for_tests() -> None:
    """Forget the singleton; tests must close a returned runtime first."""

    global _RUNTIME
    _RUNTIME = None


async def close_provider_transport_runtime(
    runtime: ProviderTransportRuntime | None,
) -> None:
    """Idempotently close an owned runtime and release the singleton.

    Long-running gateway/worker lifecycles use this instead of reaching into
    the Redis client directly. An externally supplied runtime remains the
    caller's responsibility unless that lifecycle explicitly marked it owned.
    """

    global _RUNTIME
    if runtime is None:
        return
    await runtime.aclose()
    if _RUNTIME is runtime:
        _RUNTIME = None


__all__ = [
    "ProviderTransportRuntime",
    "close_provider_transport_runtime",
    "get_provider_transport_runtime",
    "reset_provider_transport_runtime_for_tests",
]
