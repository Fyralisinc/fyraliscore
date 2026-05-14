"""services/webhooks/tenant_resolver.py — DB-backed tenant resolution
for the webhook ingress.

A Slack webhook payload doesn't say "this is tenant A." It says
`team_id=T_ACME_123`. This module reads the `provider_installations`
table to translate that into a Company OS tenant_id.

Public surface
--------------
* `ResolverOutcome` — discriminated union of `Resolved`,
  `UnknownInstallation`, `PayloadMissing`.
* `TenantResolver` — class with `resolve()` and four admin actions
  (register / disable / enable / update-secret-ref).
* `build_tenant_resolver(deps)` — factory returning a configured
  `TenantResolver`. Tests pass throwaway deps; production wires the
  pool + cache + clock + metrics at gateway startup.
* `PROVIDER_EXTRACTORS` — per-provider id extractors (Slack `team_id`,
  GitHub `installation.id`, Linear `organizationId`, Stripe
  `Stripe-Account` header, Discord `guild_id` or `application_id`).
* `InstallationCache` — TTL LRU keyed by `(provider, installation_id)`.
  Negative entries are cached too, so an attacker probing random ids
  cannot drive unbounded DB load.

Substrate alignment
-------------------
This feature creates NO Observation / Model / Act / Resource. The
`provider_installations` table is a per-feature side table for a
cross-cutting concern (tenant routing) — explicitly permitted by
Constitution §I ("Per-feature side tables for cross-cutting concerns
... are allowed and encouraged — they are not new foundations").
The table IS tenant-scoped, so §III applies in full: FK + RLS +
tenant-prefixed index, all in migration 0039.

Security
--------
* Unknown installation and disabled installation produce the
  **same** outcome (`UnknownInstallation`) so existence cannot be
  enumerated externally (FR-005, SC-003).
* The resolver never logs the installation_id verbatim (FR-015,
  SC-008). The `UnknownInstallation` outcome carries only the
  provider; the `installation_id` is in scope inside this module
  but never escapes via logs or HTTP error bodies.
* Cache backend failures are swallowed-and-logged (FR-011); the
  request continues with a direct DB lookup. Counter
  `webhook_resolver_cache_total{result='bypass'}` records the
  fallback.
"""
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Literal, NamedTuple
from uuid import UUID

import asyncpg
import structlog
from pydantic import BaseModel, ConfigDict, Field

from lib.shared.errors import (
    InstallationConflictError,
    InstallationNotFoundError,
)
from lib.shared.ids import uuid7
from services.webhooks import metrics as resolver_metrics


log = structlog.get_logger("webhooks.tenant_resolver")


# =====================================================================
# Types
# =====================================================================

ResolverProvider = Literal["slack", "github", "linear", "stripe", "discord"]


class Installation(BaseModel):
    """A persisted (provider, installation_id) → tenant_id mapping."""
    model_config = ConfigDict(extra="forbid")

    id: UUID
    tenant_id: UUID
    provider: ResolverProvider
    installation_id: str
    secret_ref: str | None
    enabled: bool
    installed_at: datetime


class Resolved(BaseModel):
    """Outcome: the resolver found an enabled installation."""
    model_config = ConfigDict(extra="forbid")

    outcome: Literal["resolved"] = "resolved"
    tenant_id: UUID
    installation_row_id: UUID
    secret_ref: str | None


class UnknownInstallation(BaseModel):
    """Outcome: never registered OR registered-but-disabled. The two
    cases are deliberately collapsed (FR-005, SC-003) so the router
    cannot leak existence.

    `installation_id` is NEVER carried on this outcome (FR-014).
    """
    model_config = ConfigDict(extra="forbid")

    outcome: Literal["unknown_installation"] = "unknown_installation"
    provider: ResolverProvider


class PayloadMissing(BaseModel):
    """Outcome: the request didn't contain a parseable installation
    identifier for the named provider (e.g. Slack payload missing
    `team_id`). Distinct from `UnknownInstallation` so the router can
    return 400 (bad request) vs 401 (auth failure).
    """
    model_config = ConfigDict(extra="forbid")

    outcome: Literal["payload_missing"] = "payload_missing"
    provider: ResolverProvider


ResolverOutcome = Annotated[
    Resolved | UnknownInstallation | PayloadMissing,
    Field(discriminator="outcome"),
]


class RegisterInstallationRequest(BaseModel):
    """Input to the register-installation admin action."""
    model_config = ConfigDict(extra="forbid")

    provider: ResolverProvider
    tenant_id: UUID
    installation_id: str
    secret_ref: str | None = None


# =====================================================================
# Cache
# =====================================================================

@dataclass(frozen=True, slots=True)
class CacheHit:
    """Cached positive lookup."""
    tenant_id: UUID
    installation_row_id: UUID
    secret_ref: str | None


@dataclass(frozen=True, slots=True)
class CacheNegative:
    """Cached negative lookup (unknown or disabled — indistinguishable)."""
    # No fields. Identity-only sentinel.


CacheValue = CacheHit | CacheNegative


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    value: CacheValue
    expires_at: float  # monotonic-clock seconds


class InstallationCache:
    """TTL LRU keyed by (provider, installation_id) → CacheValue.

    Implementation: an `OrderedDict` with move-to-end on access, plus
    a per-entry expiry timestamp. Eviction is LRU on insert when
    `max_entries` is exceeded.

    Negative caching is mandatory — without it an attacker probing
    random installation_ids forces one DB query per request (FR-009
    rationale, FR-011 fallback hardening).

    Thread safety: asyncio is single-threaded; no lock needed.
    """

    def __init__(
        self,
        *,
        max_entries: int = 4096,
        ttl_seconds: float = 300.0,
    ) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be > 0")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._entries: OrderedDict[tuple[str, str], _CacheEntry] = OrderedDict()

    def get(
        self, key: tuple[str, str], now: float,
    ) -> CacheValue | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.expires_at <= now:
            # Expired — drop and report miss.
            self._entries.pop(key, None)
            return None
        # Mark as recently used.
        self._entries.move_to_end(key)
        return entry.value

    def put(
        self,
        key: tuple[str, str],
        value: CacheValue,
        now: float,
    ) -> None:
        entry = _CacheEntry(value=value, expires_at=now + self._ttl_seconds)
        if key in self._entries:
            self._entries.move_to_end(key)
        self._entries[key] = entry
        # Evict LRU until we fit.
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)

    def invalidate(self, key: tuple[str, str]) -> None:
        self._entries.pop(key, None)

    def size(self) -> int:
        return len(self._entries)


# =====================================================================
# Per-provider id extraction
# =====================================================================

def _str_or_none(value: Any) -> str | None:
    """Stringify a payload field, returning None for absent / empty /
    non-stringifiable values.

    PayloadMissing fires when this returns None (FR-006).
    """
    if value is None:
        return None
    if isinstance(value, bool):
        # bools are ints in Python; reject explicitly because no
        # provider uses a bool as an installation identifier.
        return None
    if isinstance(value, (int, str)):
        s = str(value).strip()
        return s if s else None
    return None


def _extract_slack(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    return _str_or_none(payload.get("team_id"))


def _extract_github(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    inst = payload.get("installation")
    if not isinstance(inst, Mapping):
        return None
    return _str_or_none(inst.get("id"))


def _extract_linear(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    return _str_or_none(payload.get("organizationId"))


def _extract_stripe(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # Stripe Connect carries the account id in a request header. Header
    # name lookup is case-insensitive; check both common spellings.
    account = headers.get("Stripe-Account") or headers.get("stripe-account")
    return _str_or_none(account)


def _extract_discord(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # Guild-scoped interactions carry guild_id; DM / global commands
    # fall back to application_id. Order matters and is documented.
    guild = _str_or_none(payload.get("guild_id"))
    if guild is not None:
        return guild
    return _str_or_none(payload.get("application_id"))


PROVIDER_EXTRACTORS: dict[
    ResolverProvider,
    Callable[[Mapping[str, Any], Mapping[str, str]], str | None],
] = {
    "slack": _extract_slack,
    "github": _extract_github,
    "linear": _extract_linear,
    "stripe": _extract_stripe,
    "discord": _extract_discord,
}


# =====================================================================
# Resolver
# =====================================================================

class ResolverMetrics(NamedTuple):
    """Metric emitter functions. Tests pass throwaway no-ops to isolate.

    Each callable signature mirrors the helper in
    services.webhooks.metrics so the production wiring is a one-line
    pass-through.
    """
    record_outcome: Callable[[str, str], None]
    record_cache: Callable[[str, str], None]
    observe_duration: Callable[[str, float], None]


def default_metrics() -> ResolverMetrics:
    """Production wiring — points at the singletons in
    `services.webhooks.metrics`.
    """
    return ResolverMetrics(
        record_outcome=resolver_metrics.record_resolver_outcome,
        record_cache=resolver_metrics.record_resolver_cache,
        observe_duration=resolver_metrics.observe_resolver_duration,
    )


def noop_metrics() -> ResolverMetrics:
    """Test wiring — drops every call on the floor."""
    return ResolverMetrics(
        record_outcome=lambda *_a, **_k: None,
        record_cache=lambda *_a, **_k: None,
        observe_duration=lambda *_a, **_k: None,
    )


class TenantResolverDeps(NamedTuple):
    pool: asyncpg.Pool
    cache: InstallationCache
    clock: Callable[[], float]
    metrics: ResolverMetrics


class TenantResolver:
    """DB-backed tenant resolver. Pure function of `(provider, payload,
    headers, time)` given fixed DB state.

    No module-level globals: deps are injected through the factory
    (FR-016, Constitution stack constraints).
    """

    def __init__(self, deps: TenantResolverDeps) -> None:
        self._pool = deps.pool
        self._cache = deps.cache
        self._clock = deps.clock
        self._metrics = deps.metrics

    # -----------------------------------------------------------------
    # Resolver
    # -----------------------------------------------------------------

    async def resolve(
        self,
        provider: ResolverProvider,
        payload: Mapping[str, Any],
        headers: Mapping[str, str],
    ) -> ResolverOutcome:
        start = self._clock()
        try:
            return await self._resolve(provider, payload, headers)
        finally:
            self._metrics.observe_duration(provider, self._clock() - start)

    async def _resolve(
        self,
        provider: ResolverProvider,
        payload: Mapping[str, Any],
        headers: Mapping[str, str],
    ) -> ResolverOutcome:
        # Step 1: extract the provider-native installation identifier.
        extractor = PROVIDER_EXTRACTORS.get(provider)
        if extractor is None:
            # Unknown provider is structurally identical to a malformed
            # payload — collapse into PayloadMissing rather than
            # introducing a separate outcome (Constitution §X).
            self._metrics.record_outcome(provider, "payload_missing")
            return PayloadMissing(provider=provider)
        installation_id = extractor(payload, headers)
        if installation_id is None:
            self._metrics.record_outcome(provider, "payload_missing")
            return PayloadMissing(provider=provider)

        key = (provider, installation_id)

        # Step 2: cache read. Swallow cache exceptions — never fail
        # the request because the cache backend is unhealthy (FR-011).
        cached: CacheValue | None
        cache_bypassed = False
        try:
            cached = self._cache.get(key, self._clock())
        except Exception:  # noqa: BLE001 — cache must never raise
            log.warning(
                "webhook_resolver_cache_get_failed", provider=provider,
            )
            cached = None
            cache_bypassed = True
            self._metrics.record_cache(provider, "bypass")

        if cached is not None:
            if isinstance(cached, CacheHit):
                self._metrics.record_cache(provider, "hit")
                self._metrics.record_outcome(provider, "resolved")
                return Resolved(
                    tenant_id=cached.tenant_id,
                    installation_row_id=cached.installation_row_id,
                    secret_ref=cached.secret_ref,
                )
            # CacheNegative
            self._metrics.record_cache(provider, "hit")
            self._metrics.record_outcome(provider, "unknown_installation")
            return UnknownInstallation(provider=provider)

        if not cache_bypassed:
            self._metrics.record_cache(provider, "miss")

        # Step 3: DB lookup. Filter on enabled = true so a disabled
        # row is invisible to the resolver (FR-005 — collapses with
        # never-registered into a single outcome).
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
            # Step 4: cache negative result. Swallow put errors.
            self._safe_cache_put(key, CacheNegative(), provider)
            self._metrics.record_outcome(provider, "unknown_installation")
            return UnknownInstallation(provider=provider)

        hit = CacheHit(
            tenant_id=row["tenant_id"],
            installation_row_id=row["id"],
            secret_ref=row["secret_ref"],
        )
        self._safe_cache_put(key, hit, provider)
        self._metrics.record_outcome(provider, "resolved")
        return Resolved(
            tenant_id=hit.tenant_id,
            installation_row_id=hit.installation_row_id,
            secret_ref=hit.secret_ref,
        )

    def _safe_cache_put(
        self,
        key: tuple[str, str],
        value: CacheValue,
        provider: str,
    ) -> None:
        """Cache.put with swallow-and-log semantics. A failed write
        must not corrupt the resolver result.
        """
        try:
            self._cache.put(key, value, self._clock())
        except Exception:  # noqa: BLE001 — cache must never raise
            log.warning(
                "webhook_resolver_cache_put_failed", provider=provider,
            )
            self._metrics.record_cache(provider, "bypass")

    # -----------------------------------------------------------------
    # Admin actions
    # -----------------------------------------------------------------

    async def register_installation(
        self, req: RegisterInstallationRequest,
    ) -> Installation:
        row_id = uuid7()
        try:
            row = await self._pool.fetchrow(
                """
                INSERT INTO provider_installations
                    (id, tenant_id, provider, installation_id, secret_ref)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id, tenant_id, provider, installation_id,
                          secret_ref, enabled, installed_at
                """,
                row_id,
                req.tenant_id,
                req.provider,
                req.installation_id,
                req.secret_ref,
            )
        except asyncpg.UniqueViolationError as e:
            raise InstallationConflictError(
                f"installation already exists for ({req.provider},"
                f" {req.installation_id!r})",
                provider=req.provider,
                installation_id=req.installation_id,
            ) from e

        # Clear any prior negative-cache entry so the next resolve
        # sees the new row immediately.
        self._cache.invalidate((req.provider, req.installation_id))

        return Installation.model_validate(dict(row))

    async def disable_installation(self, installation_row_id: UUID) -> None:
        await self._set_enabled(installation_row_id, False)

    async def enable_installation(self, installation_row_id: UUID) -> None:
        await self._set_enabled(installation_row_id, True)

    async def _set_enabled(
        self,
        installation_row_id: UUID,
        enabled: bool,
    ) -> None:
        row = await self._pool.fetchrow(
            """
            UPDATE provider_installations
               SET enabled = $2
             WHERE id = $1
            RETURNING provider, installation_id
            """,
            installation_row_id,
            enabled,
        )
        if row is None:
            raise InstallationNotFoundError(
                f"installation {installation_row_id} not found",
                installation_row_id=str(installation_row_id),
            )
        self._cache.invalidate((row["provider"], row["installation_id"]))

    async def update_secret_ref(
        self,
        installation_row_id: UUID,
        new_secret_ref: str | None,
    ) -> None:
        row = await self._pool.fetchrow(
            """
            UPDATE provider_installations
               SET secret_ref = $2
             WHERE id = $1
            RETURNING provider, installation_id
            """,
            installation_row_id,
            new_secret_ref,
        )
        if row is None:
            raise InstallationNotFoundError(
                f"installation {installation_row_id} not found",
                installation_row_id=str(installation_row_id),
            )
        self._cache.invalidate((row["provider"], row["installation_id"]))


# =====================================================================
# Factory
# =====================================================================

def build_tenant_resolver(deps: TenantResolverDeps) -> TenantResolver:
    """Construct a resolver from injected deps.

    Production:
        cache = InstallationCache()
        deps = TenantResolverDeps(
            pool=app.state.pool,
            cache=cache,
            clock=time.monotonic,
            metrics=default_metrics(),
        )
        resolver = build_tenant_resolver(deps)

    Tests:
        cache = InstallationCache(max_entries=8, ttl_seconds=1.0)
        deps = TenantResolverDeps(
            pool=db_pool,
            cache=cache,
            clock=fake_clock,
            metrics=noop_metrics(),
        )
        resolver = build_tenant_resolver(deps)
    """
    return TenantResolver(deps)


__all__ = [
    # Types
    "ResolverProvider",
    "Installation",
    "Resolved",
    "UnknownInstallation",
    "PayloadMissing",
    "ResolverOutcome",
    "RegisterInstallationRequest",
    # Cache
    "InstallationCache",
    "CacheHit",
    "CacheNegative",
    "CacheValue",
    # Extractors
    "PROVIDER_EXTRACTORS",
    # Resolver
    "ResolverMetrics",
    "TenantResolverDeps",
    "TenantResolver",
    "build_tenant_resolver",
    "default_metrics",
    "noop_metrics",
]
