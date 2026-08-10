"""Tenant routing for non-source webhook providers.

Source Connector webhooks are routed by ``source_connector_callbacks`` before
the connector verifies them. This resolver intentionally serves only product
webhooks that are not ingestion sources (currently Linear and Stripe).
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Annotated, Any, Literal, NamedTuple
from uuid import UUID

import asyncpg
import structlog
from pydantic import BaseModel, ConfigDict, Field

from services.app.webhooks import metrics as resolver_metrics


log = structlog.get_logger("webhooks.tenant_resolver")
ResolverProvider = Literal["linear", "stripe"]


class Resolved(BaseModel):
    model_config = ConfigDict(extra="forbid")

    outcome: Literal["resolved"] = "resolved"
    tenant_id: UUID
    installation_row_id: UUID
    secret_ref: str | None


class UnknownInstallation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    outcome: Literal["unknown_installation"] = "unknown_installation"
    provider: ResolverProvider


class PayloadMissing(BaseModel):
    model_config = ConfigDict(extra="forbid")

    outcome: Literal["payload_missing"] = "payload_missing"
    provider: ResolverProvider


ResolverOutcome = Annotated[
    Resolved | UnknownInstallation | PayloadMissing,
    Field(discriminator="outcome"),
]


@dataclass(frozen=True, slots=True)
class CacheHit:
    tenant_id: UUID
    installation_row_id: UUID
    secret_ref: str | None


@dataclass(frozen=True, slots=True)
class CacheNegative:
    pass


CacheValue = CacheHit | CacheNegative


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    value: CacheValue
    expires_at: float


class InstallationCache:
    """Small bounded TTL/LRU cache for non-source installation mappings."""

    def __init__(
        self,
        *,
        max_entries: int = 4096,
        ttl_seconds: float = 300.0,
    ) -> None:
        if max_entries <= 0 or ttl_seconds <= 0:
            raise ValueError("cache size and TTL must be positive")
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._entries: OrderedDict[tuple[str, str], _CacheEntry] = OrderedDict()

    def get(self, key: tuple[str, str], now: float) -> CacheValue | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.expires_at <= now:
            self._entries.pop(key, None)
            return None
        self._entries.move_to_end(key)
        return entry.value

    def put(self, key: tuple[str, str], value: CacheValue, now: float) -> None:
        self._entries[key] = _CacheEntry(
            value=value,
            expires_at=now + self._ttl_seconds,
        )
        self._entries.move_to_end(key)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)

    def invalidate(self, key: tuple[str, str]) -> None:
        self._entries.pop(key, None)

    def size(self) -> int:
        return len(self._entries)


def _str_or_none(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, str)):
        result = str(value).strip()
        return result or None
    return None


def _extract_linear(
    payload: Mapping[str, Any], headers: Mapping[str, str]
) -> str | None:
    del headers
    return _str_or_none(payload.get("organizationId"))


def _extract_stripe(
    payload: Mapping[str, Any], headers: Mapping[str, str]
) -> str | None:
    del payload
    return _str_or_none(
        headers.get("Stripe-Account") or headers.get("stripe-account")
    )


PROVIDER_EXTRACTORS: dict[
    ResolverProvider,
    Callable[[Mapping[str, Any], Mapping[str, str]], str | None],
] = {
    "linear": _extract_linear,
    "stripe": _extract_stripe,
}


class ResolverMetrics(NamedTuple):
    record_outcome: Callable[[str, str], None]
    record_cache: Callable[[str, str], None]
    observe_duration: Callable[[str, float], None]


def default_metrics() -> ResolverMetrics:
    return ResolverMetrics(
        record_outcome=resolver_metrics.record_resolver_outcome,
        record_cache=resolver_metrics.record_resolver_cache,
        observe_duration=resolver_metrics.observe_resolver_duration,
    )


def noop_metrics() -> ResolverMetrics:
    return ResolverMetrics(
        record_outcome=lambda *_args, **_kwargs: None,
        record_cache=lambda *_args, **_kwargs: None,
        observe_duration=lambda *_args, **_kwargs: None,
    )


class TenantResolverDeps(NamedTuple):
    pool: asyncpg.Pool
    cache: InstallationCache
    clock: Callable[[], float]
    metrics: ResolverMetrics


class TenantResolver:
    def __init__(self, deps: TenantResolverDeps) -> None:
        self._pool = deps.pool
        self._cache = deps.cache
        self._clock = deps.clock
        self._metrics = deps.metrics

    async def resolve(
        self,
        provider: ResolverProvider,
        payload: Mapping[str, Any],
        headers: Mapping[str, str],
        *,
        subpath: str | None = None,
    ) -> ResolverOutcome:
        del subpath
        start = self._clock()
        try:
            extractor = PROVIDER_EXTRACTORS.get(provider)
            if extractor is None:
                self._metrics.record_outcome(provider, "payload_missing")
                return PayloadMissing(provider=provider)
            installation_id = extractor(payload, headers)
            if installation_id is None:
                self._metrics.record_outcome(provider, "payload_missing")
                return PayloadMissing(provider=provider)
            key = (provider, installation_id)
            cached = self._cache_get(key, provider)
            if isinstance(cached, CacheHit):
                self._metrics.record_outcome(provider, "resolved")
                return Resolved(
                    tenant_id=cached.tenant_id,
                    installation_row_id=cached.installation_row_id,
                    secret_ref=cached.secret_ref,
                )
            if isinstance(cached, CacheNegative):
                self._metrics.record_outcome(provider, "unknown_installation")
                return UnknownInstallation(provider=provider)
            row = await self._pool.fetchrow(
                """
                SELECT id, tenant_id, secret_ref
                  FROM provider_installations
                 WHERE provider = $1
                   AND installation_id = $2
                   AND enabled = TRUE
                 LIMIT 1
                """,
                provider,
                installation_id,
            )
            if row is None:
                self._cache_put(key, CacheNegative(), provider)
                self._metrics.record_outcome(provider, "unknown_installation")
                return UnknownInstallation(provider=provider)
            hit = CacheHit(
                tenant_id=row["tenant_id"],
                installation_row_id=row["id"],
                secret_ref=row["secret_ref"],
            )
            self._cache_put(key, hit, provider)
            self._metrics.record_outcome(provider, "resolved")
            return Resolved(
                tenant_id=hit.tenant_id,
                installation_row_id=hit.installation_row_id,
                secret_ref=hit.secret_ref,
            )
        finally:
            self._metrics.observe_duration(provider, self._clock() - start)

    def _cache_get(
        self, key: tuple[str, str], provider: str
    ) -> CacheValue | None:
        try:
            value = self._cache.get(key, self._clock())
        except Exception:
            log.warning("webhook_resolver_cache_get_failed", provider=provider)
            self._metrics.record_cache(provider, "bypass")
            return None
        self._metrics.record_cache(provider, "hit" if value is not None else "miss")
        return value

    def _cache_put(
        self,
        key: tuple[str, str],
        value: CacheValue,
        provider: str,
    ) -> None:
        try:
            self._cache.put(key, value, self._clock())
        except Exception:
            log.warning("webhook_resolver_cache_put_failed", provider=provider)
            self._metrics.record_cache(provider, "bypass")


def build_tenant_resolver(deps: TenantResolverDeps) -> TenantResolver:
    return TenantResolver(deps)


__all__ = [
    "CacheHit",
    "CacheNegative",
    "CacheValue",
    "InstallationCache",
    "PROVIDER_EXTRACTORS",
    "PayloadMissing",
    "Resolved",
    "ResolverMetrics",
    "ResolverOutcome",
    "ResolverProvider",
    "TenantResolver",
    "TenantResolverDeps",
    "UnknownInstallation",
    "build_tenant_resolver",
    "default_metrics",
    "noop_metrics",
]
