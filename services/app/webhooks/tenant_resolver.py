"""services/app/webhooks/tenant_resolver.py — DB-backed tenant resolution
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
from services.app.webhooks import metrics as resolver_metrics


log = structlog.get_logger("webhooks.tenant_resolver")


# =====================================================================
# Types
# =====================================================================

ResolverProvider = Literal[
    "slack", "github", "linear", "stripe", "discord", "notion", "jira",
    "mercury", "quickbooks", "grafana", "brex", "ramp", "gusto", "deel",
    "fireflies", "miro", "figma", "hibob", "ashby",
]


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


def _extract_notion(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # IN-14: Notion event deliveries carry the originating workspace id
    # at the top level as `workspace_id`. Installations are registered
    # keyed by workspace_id (the per-workspace `provider_installations`
    # row holds the bot token in `secret_ref`, used to fetch the changed
    # object). The app-level signing token is resolved separately in
    # services/app/webhooks/secrets.py.
    return _str_or_none(payload.get("workspace_id"))


def _extract_jira(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # IN-17: a Jira webhook body carries no cloudId, but the affected
    # entity's `self` URL embeds the site host (e.g.
    # "https://acme.atlassian.net/rest/api/2/issue/10001"). The site host is
    # the installation_id the seed/onboarding step registers in
    # provider_installations (provider='jira'). Check issue, then comment,
    # then any top-level object with a `self`.
    for key in ("issue", "comment"):
        obj = payload.get(key)
        if isinstance(obj, Mapping):
            host = _host_from_self(obj.get("self"))
            if host is not None:
                return host
    # Fallback: a top-level `matchedWebhookIds` / `self` some events carry.
    return _host_from_self(payload.get("self"))


def _extract_mercury(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # Finance: a Mercury webhook body carries the affected resource. We register
    # the installation keyed by the Mercury account/organization id. The seed/
    # onboarding step writes a provider_installations row (provider='mercury',
    # installation_id=<organization_id>). The body's top-level `organizationId`
    # (or the legacy `accountId`) identifies the tenant's install; the synthetic
    # finance harness sends `organizationId` explicitly. The signing secret is
    # resolved separately in services/app/webhooks/secrets.py.
    org = _str_or_none(payload.get("organizationId"))
    if org is not None:
        return org
    return _str_or_none(payload.get("accountId"))


def _extract_quickbooks(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # Finance: QuickBooks Online webhooks deliver an `eventNotifications` array,
    # each carrying the `realmId` (company id). Installations are registered
    # keyed by realmId (provider_installations row provider='quickbooks',
    # installation_id=<realmId>). We read the first notification's realmId; the
    # synthetic finance harness sends a top-level `realmId` too, so check both.
    notifications = payload.get("eventNotifications")
    if isinstance(notifications, list) and notifications:
        first = notifications[0]
        if isinstance(first, Mapping):
            realm = _str_or_none(first.get("realmId"))
            if realm is not None:
                return realm
    return _str_or_none(payload.get("realmId"))


def _extract_grafana(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # IN-GRAFANA: a Grafana Alerting webhook body carries the instance root URL
    # at top level as `externalURL` (populated from the instance's
    # [server] root_url). The instance HOST is the installation_id the
    # seed/onboarding step registers in provider_installations (provider='grafana',
    # installation_id=<instance host>). This mirrors the Jira host-from-`self`
    # approach. A single service-account token is org-scoped, so one instance host
    # == one install in v1; multi-org would combine host + `orgId`.
    return _host_from_self(payload.get("externalURL"))


def _extract_brex(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # IN-FIN2 (Bearer / Mercury archetype). A Brex webhook body carries the
    # affected resource; the install is registered keyed by the Brex
    # organization id (provider_installations provider='brex',
    # installation_id=<organizationId>). The body's top-level `organizationId`
    # (or the legacy/account-scoped `accountId`) identifies the tenant's
    # install; the synthetic finance harness sends `organizationId` explicitly.
    # The signing secret is resolved separately in
    # services/app/webhooks/secrets.py.
    # TODO(human): confirm brex webhook tenant-id field (organizationId vs
    #   accountId vs an event-envelope path) against Brex webhook docs.
    org = _str_or_none(payload.get("organizationId"))
    if org is not None:
        return org
    return _str_or_none(payload.get("accountId"))


def _extract_ramp(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # IN-FIN2 (OAuth / QuickBooks archetype). Ramp scopes installs by
    # `business_id`; the install is registered keyed by business_id
    # (provider_installations provider='ramp', installation_id=<business_id>).
    # Ramp event deliveries may wrap the body in an `eventNotifications`-style
    # array (QBO shape) or carry a top-level `business_id`; check both, mirroring
    # _extract_quickbooks. The signing secret is resolved separately in
    # services/app/webhooks/secrets.py.
    #
    # VERIFIED against docs.ramp.com webhooks: a real Ramp delivery is a flat
    # event object with a ROOT-level `business_id` (snake_case) — "The webhook
    # payload always includes a business_id". There is NO `eventNotifications`
    # wrapper (that was a QuickBooks-clone artifact); the prior
    # `eventNotifications[0].business_id` read always missed, so live Ramp
    # tenant resolution failed. Read the root field first.
    biz = _str_or_none(payload.get("business_id"))
    if biz is not None:
        return biz
    # Legacy/synthetic fallback (pre-real-shape harness payloads).
    notifications = payload.get("eventNotifications")
    if isinstance(notifications, list) and notifications:
        first = notifications[0]
        if isinstance(first, Mapping):
            return _str_or_none(first.get("business_id"))
    return None


def _extract_gusto(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # The install is keyed by the Gusto company UUID (provider_installations
    # provider='gusto', installation_id=<company_uuid>). VERIFIED against
    # docs.gusto.com webhooks: real Gusto deliveries are flat snake_case and the
    # company UUID is ALWAYS carried in `resource_uuid` (resource_type is always
    # "Company"; entity_type/entity_uuid name the changed resource). There is no
    # `company_uuid`/`companyId` body key and no `eventNotifications` wrapper —
    # those were a QuickBooks-clone artifact. Subscriptions are partner-level
    # with a shared endpoint, so the company is derived from the body, not a
    # per-install URL. The signing secret is resolved separately in
    # services/app/webhooks/secrets.py.
    resource_uuid = _str_or_none(payload.get("resource_uuid"))
    if resource_uuid is not None:
        return resource_uuid
    # Legacy/synthetic fallbacks (pre-real-shape harness payloads).
    notifications = payload.get("eventNotifications")
    if isinstance(notifications, list) and notifications:
        first = notifications[0]
        if isinstance(first, Mapping):
            company = _str_or_none(first.get("company_uuid"))
            if company is not None:
                return company
    return _str_or_none(payload.get("company_uuid"))


def _extract_deel(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # IN-FIN2 (Bearer / Mercury archetype). A Deel webhook body carries the
    # affected resource; the install is registered keyed by the Deel
    # organization id (provider_installations provider='deel',
    # installation_id=<organizationId>). The body's top-level `organizationId`
    # (or the legacy/account-scoped `accountId`) identifies the tenant's
    # install; the synthetic finance harness sends `organizationId` explicitly.
    # The signing secret is resolved separately in
    # services/app/webhooks/secrets.py.
    # TODO(human): confirm deel webhook tenant-id field (organizationId vs
    #   accountId vs an event-envelope path) against Deel webhook docs.
    org = _str_or_none(payload.get("organizationId"))
    if org is not None:
        return org
    return _str_or_none(payload.get("accountId"))


def _extract_fireflies(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # IN-FF (Brex/HMAC archetype). Fireflies scopes installs by workspace; the
    # install is registered keyed by the workspace id (provider_installations
    # provider='fireflies', installation_id=<workspace_id>). The webhook body
    # carries the workspace id at top level as `workspaceId` (camel); the
    # onboarding.register_webhook_installation seeds installation_id=workspace_id.
    # The synthetic harness sends `workspaceId` explicitly. The signing secret is
    # resolved separately in services/app/webhooks/secrets.py.
    # TODO(human): confirm fireflies webhook tenant-id field against Fireflies
    #   webhook docs.
    ws = _str_or_none(payload.get("workspaceId"))
    if ws is not None:
        return ws
    return _str_or_none(payload.get("workspace_id"))


def _extract_miro(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # IN-MIRO (Brex/HMAC archetype). Miro scopes installs by organization; the
    # install is registered keyed by the org id (provider_installations
    # provider='miro', installation_id=<org_id>). The webhook body carries the
    # org id at top level as `organizationId` (camel); the synthetic harness
    # sends it explicitly. The signing secret is resolved separately in
    # services/app/webhooks/secrets.py.
    # TODO(human): confirm miro webhook tenant-id field (organizationId vs orgId)
    #   against Miro webhook docs.
    org = _str_or_none(payload.get("organizationId"))
    if org is not None:
        return org
    return _str_or_none(payload.get("orgId"))


def _extract_figma(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # IN-FIGMA. VERIFIED against Figma Webhooks V2 docs (R2): a real Figma
    # delivery carries a Figma-assigned `webhook_id` and NO `team_id` in the
    # body (the webhook is created scoped to a team/project/file, but the
    # delivered event identifies itself only by webhook_id). So the install is
    # keyed by webhook_id — provider_installations(provider='figma',
    # installation_id=<webhook_id>), captured at registration from the
    # POST /v2/webhooks response (see integrations/figma/onboarding.py). Read
    # the real `webhook_id` first; fall back to the legacy synthetic `team_id`
    # so pre-R2 harness payloads still resolve during cutover. Figma carries no
    # installation_id in the URL path. The passcode-in-body secret is resolved
    # separately in services/app/webhooks/secrets.py (no HMAC header).
    webhook_id = _str_or_none(payload.get("webhook_id")) or _str_or_none(
        payload.get("webhookId")
    )
    if webhook_id is not None:
        return webhook_id
    # Legacy/synthetic fallback (pre-real-shape harness payloads).
    team = _str_or_none(payload.get("team_id"))
    if team is not None:
        return team
    return _str_or_none(payload.get("teamId"))


def _extract_hibob(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # IN-PEOPLE (gusto-structure / Basic-service-user archetype). HiBob scopes
    # installs by company; the install is registered keyed by the HiBob company
    # id (provider_installations provider='hibob', installation_id=<company_id>).
    # The synthetic harness sends the company id at top level as `companyId`
    # (camel). The signing secret is resolved separately in
    # services/app/webhooks/secrets.py.
    # TODO(human): in production HiBob does NOT carry the company id in the
    #   webhook body — the tenant is resolved by the per-install endpoint/secret
    #   (each install registers a distinct webhook URL + secret). The `companyId`
    #   body field here is the synthetic-gate stand-in; confirm the real
    #   per-endpoint resolution against HiBob webhook docs before production.
    return _str_or_none(payload.get("companyId"))


def _extract_ashby(payload: Mapping[str, Any], headers: Mapping[str, str]) -> str | None:
    # IN-RECRUITING. VERIFIED (R3): real Ashby webhook deliveries carry NO org id
    # in the body (`{webhookActionId, action, data}`) — the tenant is named by
    # the PER-INSTALL ENDPOINT URL (`/webhooks/ashby/{installId}`), each install
    # configured with a distinct URL + Ashby-Signature secret. So the resolver
    # resolves Ashby from the URL path FIRST (see `_PATH_RESOLVED_PROVIDERS` +
    # `TenantResolver._resolve`); this body extractor is now only the FALLBACK
    # for legacy/synthetic (org-in-body) payloads posted to the bare endpoint.
    # The install is registered keyed by the path segment / org id
    # (provider_installations provider='ashby', installation_id=<installId>); the
    # signing secret is resolved separately in services/app/webhooks/secrets.py.
    return _str_or_none(payload.get("organizationId"))


def _host_from_self(self_url: Any) -> str | None:
    if not isinstance(self_url, str) or "://" not in self_url:
        return None
    host = self_url.split("://", 1)[1].split("/", 1)[0].strip()
    return host or None


# R3 — providers that register a PER-INSTALL ENDPOINT and resolve the tenant from
# the URL path (`/webhooks/{provider}/{installId}`) rather than a body field.
# Ashby real deliveries carry NO org id in the body (`{webhookActionId, action,
# data}`); the tenant is named by the receiving endpoint URL (each install is
# configured with a distinct webhook URL + signing secret in Ashby's admin). The
# body extractor stays as a legacy/synthetic fallback.
_PATH_RESOLVED_PROVIDERS: frozenset[str] = frozenset({"ashby"})


def _first_path_segment(subpath: str | None) -> str | None:
    """The first segment of a webhook subpath (`installId` from
    `/webhooks/{provider}/{installId}[/...]`). None when empty."""
    if not subpath:
        return None
    seg = subpath.strip("/").split("/", 1)[0].strip()
    return seg or None


PROVIDER_EXTRACTORS: dict[
    ResolverProvider,
    Callable[[Mapping[str, Any], Mapping[str, str]], str | None],
] = {
    "slack": _extract_slack,
    "github": _extract_github,
    "linear": _extract_linear,
    "stripe": _extract_stripe,
    "discord": _extract_discord,
    "notion": _extract_notion,
    "jira": _extract_jira,
    "mercury": _extract_mercury,
    "quickbooks": _extract_quickbooks,
    "grafana": _extract_grafana,
    "brex": _extract_brex,
    "ramp": _extract_ramp,
    "gusto": _extract_gusto,
    "deel": _extract_deel,
    "fireflies": _extract_fireflies,
    "miro": _extract_miro,
    "figma": _extract_figma,
    "hibob": _extract_hibob,
    "ashby": _extract_ashby,
}


# =====================================================================
# Resolver
# =====================================================================

class ResolverMetrics(NamedTuple):
    """Metric emitter functions. Tests pass throwaway no-ops to isolate.

    Each callable signature mirrors the helper in
    services.app.webhooks.metrics so the production wiring is a one-line
    pass-through.
    """
    record_outcome: Callable[[str, str], None]
    record_cache: Callable[[str, str], None]
    observe_duration: Callable[[str, float], None]


def default_metrics() -> ResolverMetrics:
    """Production wiring — points at the singletons in
    `services.app.webhooks.metrics`.
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
        *,
        subpath: str | None = None,
    ) -> ResolverOutcome:
        start = self._clock()
        try:
            return await self._resolve(provider, payload, headers, subpath)
        finally:
            self._metrics.observe_duration(provider, self._clock() - start)

    async def _resolve(
        self,
        provider: ResolverProvider,
        payload: Mapping[str, Any],
        headers: Mapping[str, str],
        subpath: str | None = None,
    ) -> ResolverOutcome:
        # Step 1: extract the provider-native installation identifier.
        extractor = PROVIDER_EXTRACTORS.get(provider)
        if extractor is None:
            # Unknown provider is structurally identical to a malformed
            # payload — collapse into PayloadMissing rather than
            # introducing a separate outcome (Constitution §X).
            self._metrics.record_outcome(provider, "payload_missing")
            return PayloadMissing(provider=provider)
        # R3: per-install-endpoint providers (Ashby) resolve the tenant from the
        # URL path segment (`/webhooks/ashby/{installId}`) — real deliveries
        # carry no org id in the body. Prefer the path; fall back to the body
        # extractor so legacy/synthetic (org-in-body) payloads still resolve.
        installation_id: str | None = None
        if provider in _PATH_RESOLVED_PROVIDERS:
            installation_id = _first_path_segment(subpath)
        if installation_id is None:
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
