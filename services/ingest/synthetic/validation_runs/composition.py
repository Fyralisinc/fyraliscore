"""Live-phase orchestration (A30.1).

The spine validated the backfill path (Run 1). This module composes the
four in-process live generators into the runner's *live phase* so each
tenant — after its backfill drains — also ingests live events against the
SAME install backfill used.

Composition shape (per the Phase-1 substrate audit):
  - slack + github  → one shared FastAPI app (`build_app`, the canonical
    `services.app.gateway.main` builder). Tenant resolution is real
    (`provider_installations` by `installation_id`); the X3 harness wrote
    `installation_id = f"x3-{slug}-{source}"`, so the live drivers address
    the same rows.
  - gmail (Pub/Sub) → its OWN minimal app with just the gmail_pubsub
    router (the router reads `app.state.deps.pool`; it is NOT mounted by
    `build_app`). The generator's `_seed_db` reuses the existing
    `gmail_mailbox_watches` row backfill created (A30.1), so live shares
    backfill's install — required for the gmail cross-path twin since
    gmail's `external_id` embeds the install id.
  - discord → no HTTP; direct dispatch via `DispatchDeps` +
    `build_tenant_resolver` (resolution by guild_id == installation_id).

Live ingestion is INLINE (the webhook/dispatch handlers write the
observation synchronously); no Kafka consumer is needed for the live
phase — unlike backfill, which the spine's `BackfillHarness` drives
through the normalizer + observation_writer subprocesses.

The cross-path dedup twin (A30.2/A30.3): for gmail/github/slack the runner
captures one backfilled observation's identity and replays it live via the
Phase-0 injection kwargs; the `(source_channel, external_id, occurred_at)`
UNIQUE index must collapse the pair to one row. Discord is excluded — its
live ids (`msg-y2-*`) and backfill ids (fixture-derived) are disjoint
namespaces (A30.3).
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import FastAPI

from lib.shared.tenant_context import tenant_transaction
from services.ingest.ingestion.feature_flags.client import KAFKA_PATH_ENABLED
from services.ingest.source_contract.catalog import (
    SOURCE_DEFINITIONS,
    source_definition,
)
from services.ingest.source_contract.runtime import resolve_callable_reference


log = logging.getLogger("validation_runs.composition")

def _validation_runtime(source: str) -> Any:
    runtime = source_definition(source).certification.validation_runtime
    if runtime is None:
        raise RuntimeError(
            f"source {source!r} has no certification validation runtime"
        )
    return runtime


TWIN_SOURCES = tuple(
    definition.source_id
    for definition in SOURCE_DEFINITIONS
    if _validation_runtime(definition.source_id).twin_probe_binding is not None
)
# Historical public name retained for report compatibility. Membership is
# contract-derived and includes every source with an authentication-tamper probe.
HMAC_SOURCES = tuple(
    definition.source_id
    for definition in SOURCE_DEFINITIONS
    if _validation_runtime(definition.source_id).signature_probe_binding is not None
)
REPLAY_SOURCES = tuple(
    definition.source_id
    for definition in SOURCE_DEFINITIONS
    if _validation_runtime(definition.source_id).replay_probe_binding is not None
)


# =====================================================================
# Signing-secret env (threaded into os.environ before app build).
# =====================================================================
@dataclass(frozen=True)
class SigningSecrets:
    slack: str = "v-slack-signing-secret"
    github: str = "v-github-signing-secret"
    # The four HMAC providers added after the original 4-source harness; each
    # resolved via the `WEBHOOK_SECRET_<PROVIDER>` env fallback (gated by
    # WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1) the same way slack/github are.
    jira: str = "v-jira-signing-secret"
    mercury: str = "v-mercury-signing-secret"
    quickbooks: str = "v-quickbooks-verifier-token"
    grafana: str = "v-grafana-signing-secret"
    # IN-FIN2 finance sources (brex/deel = Bearer/Mercury archetype; ramp/gusto
    # = OAuth/QuickBooks archetype); each resolved via WEBHOOK_SECRET_<PROVIDER>.
    brex: str = "v-brex-signing-secret"
    ramp: str = "v-ramp-signing-secret"
    gusto: str = "v-gusto-signing-secret"
    deel: str = "v-deel-signing-secret"
    # Vertical-2 HMAC webhook sources (Fireflies/Figma = Brex archetype).
    # Signal/AWS/Miro/Carta use poll/gateway dispatch → NO webhook secret.
    fireflies: str = "v-fireflies-signing-secret"
    figma: str = "v-figma-signing-secret"
    # People/recruiting HMAC webhook sources (hibob = HMAC-SHA512/base64,
    # Bob-Signature; ashby = HMAC-SHA256/hex, Ashby-Signature). linkedin is
    # poll-only (no webhook edge) → NO signing secret.
    hibob: str = "v-hibob-signing-secret"
    ashby: str = "v-ashby-signing-secret"
    # Meta dedicated-ingress sources share X-Hub-Signature-256.
    whatsapp: str = "v-whatsapp-app-secret"
    facebook_pages: str = "v-facebook-pages-app-secret"
    # Notion's app-level verification token (NOT a per-tenant secret_ref); the
    # signatures/notion.py verifier keys HMAC on it.
    notion: str = "v-notion-verification-token"
    # A symmetric AES key the secret-store factory needs in `test` env.
    master_kek: str = "KuT6Cixjs4991zhixcpj1QAFbiQj3b9N8meZV2AJJyw="

    def apply_to_env(self) -> None:
        import os
        os.environ["WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW"] = "1"
        os.environ["WEBHOOK_SECRET_SLACK"] = self.slack
        os.environ["WEBHOOK_SECRET_GITHUB"] = self.github
        os.environ["WEBHOOK_SECRET_JIRA"] = self.jira
        os.environ["WEBHOOK_SECRET_MERCURY"] = self.mercury
        os.environ["WEBHOOK_SECRET_QUICKBOOKS"] = self.quickbooks
        os.environ["WEBHOOK_SECRET_GRAFANA"] = self.grafana
        os.environ["WEBHOOK_SECRET_BREX"] = self.brex
        os.environ["WEBHOOK_SECRET_RAMP"] = self.ramp
        os.environ["WEBHOOK_SECRET_GUSTO"] = self.gusto
        os.environ["WEBHOOK_SECRET_DEEL"] = self.deel
        os.environ["WEBHOOK_SECRET_FIREFLIES"] = self.fireflies
        os.environ["WEBHOOK_SECRET_FIGMA"] = self.figma
        os.environ["WEBHOOK_SECRET_HIBOB"] = self.hibob
        os.environ["WEBHOOK_SECRET_ASHBY"] = self.ashby
        os.environ["WHATSAPP_APP_SECRET"] = self.whatsapp
        os.environ["FACEBOOK_APP_SECRET"] = self.facebook_pages
        os.environ["NOTION_WEBHOOK_VERIFICATION_TOKEN"] = self.notion
        os.environ["MASTER_KEK"] = self.master_kek
        # Gmail Pub/Sub router import-time reads (verification is no-op'd
        # by the generator, but the values must be present).
        os.environ.setdefault(
            "GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE",
            "https://v-test.example.com/webhooks/gmail/pubsub",
        )
        os.environ.setdefault(
            "GMAIL_PUBSUB_PUSH_OIDC_SA",
            "pubsub-pusher@v-test.iam.gserviceaccount.com",
        )


# =====================================================================
# Per-tenant live addressing (derived from the backfill scenario).
# =====================================================================
@dataclass(frozen=True)
class LiveTarget:
    """How to address one tenant's live events. Derived from the X3
    harness's seeding convention (`installation_id = x3-{slug}-{source}`;
    gmail keyed by mailbox email)."""
    tenant_id: UUID
    source: str
    slug: str
    # source-specific addressing
    email: str | None = None            # gmail
    team_id: str | None = None          # slack
    guild_id: str | None = None         # discord
    installation_id: str | None = None  # github
    channel_id: str | None = None       # slack/discord
    repo_full_name: str | None = None   # github
    # HMAC-webhook providers (tenant resolved via provider_installations).
    provider_installation_id: str | None = None
    jira_site: str | None = None            # jira: issue.self host == installation_id
    mercury_org: str | None = None          # mercury: organizationId == installation_id
    mercury_account: str | None = None      # mercury: transaction.accountId
    qbo_realm: str | None = None            # quickbooks: realmId == installation_id
    qbo_entity: str | None = None           # quickbooks: entity name (e.g. Invoice)
    grafana_instance: str | None = None     # grafana: externalURL host == installation_id
    # IN-FIN2 finance HMAC providers (tenant resolved via provider_installations).
    brex_org: str | None = None             # brex: organizationId == installation_id
    brex_account: str | None = None         # brex: transaction.accountId
    ramp_business: str | None = None        # ramp: business_id == installation_id
    gusto_company: str | None = None        # gusto: company_uuid == installation_id
    deel_org: str | None = None             # deel: organizationId == installation_id
    # Google push (watch-row resolved).
    gcal_calendar_id: str | None = None
    gcal_channel_id: str | None = None
    gcal_watch_token: str | None = None
    gdrive_drive_id: str | None = None
    gdrive_kind: str | None = None
    gdrive_channel_id: str | None = None
    gdrive_watch_token: str | None = None
    # Notion (workspace_id == the seeded provider_installations.installation_id).
    notion_workspace_id: str | None = None
    # Telegram (gateway-style live; installation_id resolved from the pool by the
    # generator, so the live external_id matches backfill). The live message is
    # dispatched "into" the tenant's first backfill dialog.
    telegram_dialog_id: int | None = None
    telegram_dialog_kind: str | None = None
    telegram_dialog_title: str | None = None
    # Vertical-2 HMAC webhook providers (tenant resolved via provider_installations;
    # installation_id == workspace/org/team id seeded for backfill).
    fireflies_workspace: str | None = None  # fireflies: workspaceId == installation_id
    miro_org: str | None = None             # miro: organizationId == installation_id
    miro_board: str | None = None           # miro: boardId (event payload context)
    # figma: REAL Figma Webhooks V2 (R2) key the install by the Figma-assigned
    # `webhook_id` (the body carries no team_id and no event id). webhook_id ==
    # the seeded provider_installations.installation_id; team_id is backfill
    # context only; file_key + timestamp discriminate the event.
    figma_webhook_id: str | None = None     # figma: webhook_id == installation_id
    figma_team: str | None = None           # figma: team_id (backfill context)
    figma_file: str | None = None           # figma: file_key (event payload context)
    # Vertical-2 direct-dispatch sources (install resolved from own table by the
    # generator; no provider_installations row).
    signal_thread_id: int | None = None     # signal: gateway thread (== backfill)
    signal_thread_kind: str | None = None
    signal_thread_title: str | None = None
    aws_account_id: str | None = None       # aws: poll event namespace (== install)
    aws_region: str | None = None
    carta_firm: str | None = None           # carta: firm_id (== install scope)
    carta_entity_type: str | None = None    # carta: poll change entity kind
    # People/recruiting sources (IN-PEOPLE).
    # hibob/ashby: HMAC webhook (tenant resolved via provider_installations;
    # installation_id == company_id/org_id seeded for backfill).
    hibob_company: str | None = None         # hibob: companyId == installation_id
    ashby_org: str | None = None             # ashby: organizationId == installation_id
    # linkedin: poll live edge (install resolved from linkedin_installations by
    # tenant_id; NO provider_installations row).
    linkedin_org: str | None = None          # linkedin: organization_urn (== install scope)
    linkedin_entity_type: str | None = None  # linkedin: poll change entity kind
    # Meta dedicated webhook routers resolve their own exact install tables.
    whatsapp_phone_number_id: str | None = None
    facebook_page_id: str | None = None


def live_target_for(tenant_id: UUID, source: str, slug: str,
                    fixture_params: dict[str, Any]) -> LiveTarget:
    binding = _validation_runtime(source).live_target_binding
    return resolve_callable_reference(binding)(
        tenant_id,
        source,
        slug,
        fixture_params,
    )


async def seed_contract_live_only_targets(
    pool: asyncpg.Pool,
    *,
    tenants_per_source: int,
) -> list[LiveTarget]:
    """Seed exact local bindings for contract sources with no history.

    Historical targets come from ``BackfillHarness`` outcomes. A live-only
    source has no planner/fetcher outcome by design, so the composed runner
    needs a separate tenant and installation bootstrap. Membership is derived
    from the source contract and each provider-specific bootstrap is explicit
    and fail-closed. Today that set is exactly WhatsApp.
    """

    if (
        not isinstance(tenants_per_source, int)
        or isinstance(tenants_per_source, bool)
        or tenants_per_source < 0
    ):
        raise ValueError("tenants_per_source must be a non-negative integer")

    targets: list[LiveTarget] = []
    for definition in SOURCE_DEFINITIONS:
        if definition.history is not None:
            continue
        runtime = _validation_runtime(definition.source_id)
        binding = runtime.live_only_bootstrap_binding
        if binding is None:
            raise RuntimeError(
                "live-only certification bootstrap is not implemented for "
                f"contract source {definition.source_id!r}",
            )
        targets.extend(
            await resolve_callable_reference(binding)(
                pool,
                tenants_per_source=tenants_per_source,
                source=definition.source_id,
            )
        )
    return targets


# =====================================================================
# LiveDrivers bundle.
# =====================================================================
@dataclass
class LiveDrivers:
    generators: dict[str, Any]
    fastapi_app: FastAPI
    gmail_app: FastAPI
    google_app: FastAPI
    _exit_stack: Any = None

    def generator_for(self, source: str) -> Any:
        generator = self.generators.get(source)
        if generator is None:
            raise RuntimeError(
                f"live generator for configured source {source!r} was not built"
            )
        return generator

    @property
    def gmail_pubsub(self) -> Any:
        return self.generators.get("gmail")

    @property
    def discord_gateway(self) -> Any:
        return self.generators.get("discord")

    @property
    def slack_webhook(self) -> Any:
        return self.generators.get("slack")

    @property
    def github_webhook(self) -> Any:
        return self.generators.get("github")


@dataclass(frozen=True)
class _LiveCutoverDeps:
    kafka_producer: Any = None
    s3_raw_client: Any = None
    tenant_flags: Any = None


def _attach_cutover_state(app: FastAPI, cutover: _LiveCutoverDeps) -> None:
    app.state.kafka_producer = cutover.kafka_producer
    app.state.s3_raw_client = cutover.s3_raw_client
    app.state.tenant_flags = cutover.tenant_flags


def _build_shared_live_app(pool: asyncpg.Pool, cutover: _LiveCutoverDeps) -> FastAPI:
    from services.app.gateway.main import build_app
    from services.app.gateway.rate_limit import RateLimiter
    from services.domain.actors.repo import ActorRepo
    from services.domain.entity_aliases.repo import EntityAliasRepo

    app = build_app(
        pool=pool,
        actor_repo=ActorRepo(pool),
        alias_repo=EntityAliasRepo(pool),
        embedder=None,
        rate_limiter=RateLimiter(),
        configure_logging=False,
    )
    _attach_cutover_state(app, cutover)
    return app


def _build_gmail_live_app(pool: asyncpg.Pool, cutover: _LiveCutoverDeps) -> FastAPI:
    from services.app.webhooks.gmail_pubsub import router as gmail_router

    class _GmailDeps:
        pass

    app = FastAPI()
    app.include_router(gmail_router)
    deps = _GmailDeps()
    deps.pool = pool  # type: ignore[attr-defined]
    app.state.deps = deps
    _attach_cutover_state(app, cutover)
    return app


def _build_google_live_app(pool: asyncpg.Pool) -> FastAPI:
    from services.app.webhooks.google_push import router as google_push_router

    app = FastAPI()
    app.include_router(google_push_router)
    app.state.pool = pool
    return app


def _build_discord_dispatch_deps(
    pool: asyncpg.Pool,
    cutover: _LiveCutoverDeps,
) -> Any:
    from services.app.webhooks.tenant_resolver import (
        InstallationCache,
        TenantResolverDeps,
        build_tenant_resolver,
        noop_metrics,
    )
    from services.domain.actors.repo import ActorRepo
    from services.domain.entity_aliases.repo import EntityAliasRepo
    from services.ingest.integrations.discord.gateway.dispatch import DispatchDeps

    resolver = build_tenant_resolver(
        TenantResolverDeps(
            pool=pool,
            cache=InstallationCache(),
            clock=time.monotonic,
            metrics=noop_metrics(),
        ),
    )
    return DispatchDeps(
        pool=pool,
        tenant_resolver=resolver,
        actor_repo=ActorRepo(pool),
        alias_repo=EntityAliasRepo(pool),
        embedder=None,
        application_id="v-discord-app",
        s3_raw_client=cutover.s3_raw_client,
        kafka_producer=cutover.kafka_producer,
        tenant_flags=cutover.tenant_flags,
    )


async def build_live_drivers(
    pool: asyncpg.Pool,
    targets: list[LiveTarget],
    secrets: SigningSecrets,
    *,
    kafka_producer: Any = None,
    s3_raw_client: Any = None,
    tenant_flags: Any = None,
) -> LiveDrivers:
    """Build every selected source through its catalog-owned factory."""
    from contextlib import AsyncExitStack
    from services.ingest.synthetic.validation_runs.source_bindings import (
        LiveGeneratorBuildContext,
    )

    secrets.apply_to_env()
    cutover = _LiveCutoverDeps(
        kafka_producer=kafka_producer,
        s3_raw_client=s3_raw_client,
        tenant_flags=tenant_flags,
    )
    shared_app = _build_shared_live_app(pool, cutover)
    gmail_app = _build_gmail_live_app(pool, cutover)
    google_app = _build_google_live_app(pool)
    stack = AsyncExitStack()
    target_sources = {target.source for target in targets}
    targets_by_group: dict[str, list[LiveTarget]] = {}
    for target in targets:
        group = _validation_runtime(target.source).generator_group
        targets_by_group.setdefault(group, []).append(target)
    group_generators: dict[str, Any] = {}
    generators: dict[str, Any] = {}
    discord_deps = _build_discord_dispatch_deps(pool, cutover)
    for definition in SOURCE_DEFINITIONS:
        source = definition.source_id
        if source not in target_sources:
            continue
        runtime = _validation_runtime(source)
        generator = group_generators.get(runtime.generator_group)
        if generator is None:
            context = LiveGeneratorBuildContext(
                source=source,
                targets=tuple(targets_by_group[runtime.generator_group]),
                stack=stack,
                pool=pool,
                shared_app=shared_app,
                gmail_app=gmail_app,
                google_app=google_app,
                secrets=secrets,
                cutover=cutover,
                discord_dispatch_deps=discord_deps,
            )
            generator = await resolve_callable_reference(
                runtime.live_generator_binding
            )(context)
            group_generators[runtime.generator_group] = generator
        generators[source] = generator
    return LiveDrivers(
        generators=generators,
        fastapi_app=shared_app,
        gmail_app=gmail_app,
        google_app=google_app,
        _exit_stack=stack,
    )

async def teardown_live_drivers(drivers: LiveDrivers) -> None:
    if drivers._exit_stack is not None:
        await drivers._exit_stack.aclose()


# =====================================================================
# Live-only install/watch seeding (for the sources added after the 4).
# =====================================================================
async def seed_live_installs(
    pool: asyncpg.Pool,
    targets: list[LiveTarget],
    secrets: SigningSecrets | None = None,
    *,
    secret_store: Any = None,
) -> None:
    """Run each target's exact catalog-owned live-install seeder."""
    from lib.shared.secrets import build_secret_store
    from services.ingest.synthetic.validation_runs.source_bindings import (
        LiveInstallContext,
    )

    signing_secrets = secrets or SigningSecrets()
    needs_secret_store = any(
        _validation_runtime(target.source).live_install_binding.endswith(
            ("seed_whatsapp_live_install", "seed_facebook_pages_live_install")
        )
        for target in targets
    )
    if needs_secret_store:
        signing_secrets.apply_to_env()
        secret_store = secret_store or build_secret_store(pool)
    async with pool.acquire() as conn:
        context = LiveInstallContext(
            conn=conn,
            secrets=signing_secrets,
            secret_store=secret_store,
        )
        for target in targets:
            binding = _validation_runtime(target.source).live_install_binding
            await resolve_callable_reference(binding)(context, target)


async def prepare_live_drivers(
    pool: asyncpg.Pool,
    targets: list[LiveTarget],
    secrets: SigningSecrets,
    *,
    kafka_producer: Any = None,
    s3_raw_client: Any = None,
    tenant_flags: Any = None,
) -> LiveDrivers:
    """Seed exact live bindings, then construct every required generator.

    Keeping these operations behind one entry point prevents a runner from
    constructing a functional-looking driver bundle before the webhook tenant
    bindings or Google watch rows exist. ``seed_live_installs`` is idempotent,
    so retries and repeated preparation preserve the same exact installation.
    """
    await seed_live_installs(pool, targets, secrets=secrets)
    return await build_live_drivers(
        pool,
        targets,
        secrets,
        kafka_producer=kafka_producer,
        s3_raw_client=s3_raw_client,
        tenant_flags=tenant_flags,
    )


# =====================================================================
# Twin-pair identity capture (A30.2 / A30.3).
# =====================================================================
@dataclass(frozen=True)
class TwinIdentity:
    source: str
    tenant_id: UUID
    external_id: str
    occurred_at: dt.datetime


async def capture_twin_identities(
    pool: asyncpg.Pool, targets: list[LiveTarget],
) -> dict[str, TwinIdentity]:
    """For each cross-path source, pick its first tenant and read back
    ONE backfilled observation's (external_id, occurred_at). The live
    phase replays that identity so the dedup index must collapse the
    pair (A30.3)."""
    out: dict[str, TwinIdentity] = {}
    by_source: dict[str, list[LiveTarget]] = {}
    for t in targets:
        by_source.setdefault(t.source, []).append(t)
    for source in TWIN_SOURCES:
        cand = by_source.get(source, [])
        if not cand:
            continue
        twin_tenant = cand[0]
        async with tenant_transaction(twin_tenant.tenant_id, pool=pool) as tctx:
            row = await tctx.fetchrow(
                """
                SELECT external_id, occurred_at FROM observations
                 WHERE tenant_id = $1 AND external_id IS NOT NULL
                 ORDER BY occurred_at ASC LIMIT 1
                """,
                twin_tenant.tenant_id,
            )
        if row is None:
            log.warning("twin.no_backfill_obs", extra={"source": source})
            continue
        out[source] = TwinIdentity(
            source=source, tenant_id=twin_tenant.tenant_id,
            external_id=row["external_id"], occurred_at=row["occurred_at"],
        )
    return out


# =====================================================================
# Live phase.
# =====================================================================
@dataclass
class LivePhaseResult:
    expected_live_by_tenant: dict[UUID, int] = field(default_factory=dict)
    actual_live_by_tenant: dict[UUID, int] = field(default_factory=dict)
    per_source_counts: dict[str, int] = field(default_factory=dict)
    twin_external_ids: dict[str, str] = field(default_factory=dict)
    tamper_results: list[dict[str, Any]] = field(default_factory=list)
    replay_dispatched_unique: int = 0
    replay_probability: float = 0.0
    wall_seconds: float = 0.0


async def _count_obs(pool: asyncpg.Pool, tenant_id: UUID) -> int:
    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        return int(await tctx.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id = $1", tenant_id,
        ))


async def _dispatch_regular(
    drivers: LiveDrivers, t: LiveTarget, n: int,
) -> None:
    """Dispatch `n` fresh (auto-mint) live events for one tenant."""
    _ensure_live_source_supported(drivers, t.source)
    for event_index in range(n):
        await _dispatch_live_event(
            drivers,
            t,
            event_index=event_index,
        )


def _ensure_live_source_supported(
    drivers: LiveDrivers,
    source: str,
) -> None:
    """Fail closed for unsupported targets and incomplete driver bundles."""
    try:
        runtime = _validation_runtime(source)
    except KeyError as exc:
        raise ValueError(f"unsupported live source {source!r}") from exc
    drivers.generator_for(source)
    resolve_callable_reference(runtime.live_event_binding)


async def _dispatch_live_event(
    drivers: LiveDrivers,
    target: LiveTarget,
    *,
    event_index: int,
) -> Any:
    """Dispatch one event through the generator owned by ``target.source``."""
    _ensure_live_source_supported(drivers, target.source)
    binding = _validation_runtime(target.source).live_event_binding
    return await resolve_callable_reference(binding)(
        drivers,
        target,
        event_index,
    )


async def _dispatch_twin(
    drivers: LiveDrivers, t: LiveTarget, twin: TwinIdentity,
) -> str:
    """Replay a captured identity via its catalog-owned twin probe."""
    binding = _validation_runtime(t.source).twin_probe_binding
    if binding is None:
        raise ValueError(f"source {t.source!r} has no twin probe")
    return await resolve_callable_reference(binding)(drivers, t, twin)


async def _dispatch_signature_tamper(
    drivers: LiveDrivers,
    target: LiveTarget,
) -> int:
    """Send one invalidly authenticated request through the real live edge."""

    _ensure_live_source_supported(drivers, target.source)
    binding = _validation_runtime(target.source).signature_probe_binding
    if binding is None:
        raise ValueError(
            f"source {target.source!r} has no declared signature tamper probe"
        )
    return int(await resolve_callable_reference(binding)(drivers, target))


async def run_live_phase(
    pool: asyncpg.Pool,
    drivers: LiveDrivers,
    targets: list[LiveTarget],
    twins: dict[str, TwinIdentity],
    *,
    events_per_tenant: int = 5,
) -> LivePhaseResult:
    """Dispatch each tenant's regular live burst (concurrently across
    tenants), then the cross-path twin events, plus one tampered-signature
    probe per HMAC source. Returns counts + twin external_ids for the
    assertion layer."""
    t0 = time.monotonic()
    result = LivePhaseResult()
    result.expected_live_by_tenant = {
        t.tenant_id: events_per_tenant for t in targets
    }

    # snapshot pre-live counts so live delta is attributable.
    pre = {t.tenant_id: await _count_obs(pool, t.tenant_id) for t in targets}

    # ---- Regular bursts: parallel across tenants ----
    await asyncio.gather(*(
        _dispatch_regular(drivers, t, events_per_tenant) for t in targets
    ))

    # ---- Twin replays (cross-path dedup) ----
    # Dispatch each twin to the SAME tenant its identity was captured from
    # (by tenant_id, NOT by source) — gmail's external_id embeds the
    # install, so replaying val-gmail-0's identity through val-gmail-3's
    # mailbox would NOT collide. (Found at 16 tenants; masked at 1/source
    # where the only tenant is both first and last.)
    targets_by_tid = {t.tenant_id: t for t in targets}
    for source, twin in twins.items():
        t = targets_by_tid.get(twin.tenant_id)
        if t is None:
            continue
        ext = await _dispatch_twin(drivers, t, twin)
        result.twin_external_ids[source] = ext

    # ---- Tampered-authentication probes (one per declared source) ----
    probed_sources: set[str] = set()
    for t in targets:
        signature_binding = _validation_runtime(
            t.source
        ).signature_probe_binding
        if signature_binding is None or t.source in probed_sources:
            continue
        result.tamper_results.append({
            "source": t.source,
            "http_status": await _dispatch_signature_tamper(drivers, t),
        })
        probed_sources.add(t.source)

    # ---- Collect live deltas ----
    for t in targets:
        post = await _count_obs(pool, t.tenant_id)
        delta = post - pre[t.tenant_id]
        result.actual_live_by_tenant[t.tenant_id] = delta
        result.per_source_counts[t.source] = (
            result.per_source_counts.get(t.source, 0) + delta
        )

    result.wall_seconds = time.monotonic() - t0
    return result


@dataclass
class LiveDispatchResult:
    """Result of the concurrent live dispatch (Run 4). Unlike
    `LivePhaseResult`, this does NOT count observations synchronously —
    under live-via-Kafka the writer produces them asynchronously, so the
    caller counts after the shared consumer drain."""
    dispatched_by_tenant: dict[UUID, int] = field(default_factory=dict)
    dispatched_by_source: dict[str, int] = field(default_factory=dict)
    # Per-source set of HTTP statuses seen (slack/github/gmail are HTTP;
    # discord is direct-dispatch and reports no status). 202 == the
    # webhook router took the Kafka cutover (live-via-Kafka proof).
    http_status_by_source: dict[str, set[int]] = field(default_factory=dict)
    wall_seconds: float = 0.0


async def dispatch_live_concurrent(
    drivers: LiveDrivers,
    targets: list[LiveTarget],
    *,
    events_per_tenant: int = 5,
) -> LiveDispatchResult:
    """Fire `events_per_tenant` distinct live events for every tenant,
    concurrently across tenants. Returns dispatch counts + per-source
    HTTP statuses. Does NOT count observations (the consumer chain is
    async under live-via-Kafka — the runner counts after drain).

    Live ids are minted distinct from backfill (channel `C_LIVE_*`,
    content `live-*`), so there is no accidental cross-path dedup: the
    post-drain total is exactly backfill_expected + live_dispatched."""
    t0 = time.monotonic()
    result = LiveDispatchResult()
    lock = asyncio.Lock()

    async def _record_status(source: str, status: int | None) -> None:
        if status is None:
            return
        async with lock:
            result.http_status_by_source.setdefault(source, set()).add(status)

    async def _one(t: LiveTarget) -> None:
        _ensure_live_source_supported(drivers, t.source)
        for i in range(events_per_tenant):
            dispatch_result = await _dispatch_live_event(
                drivers,
                t,
                event_index=i,
            )
            status = getattr(dispatch_result, "http_status", None)
            await _record_status(t.source, status)
        async with lock:
            result.dispatched_by_tenant[t.tenant_id] = events_per_tenant
            result.dispatched_by_source[t.source] = (
                result.dispatched_by_source.get(t.source, 0) + events_per_tenant
            )

    await asyncio.gather(*(_one(t) for t in targets))
    result.wall_seconds = time.monotonic() - t0
    return result


async def run_replay_probe(
    pool: asyncpg.Pool,
    drivers: LiveDrivers,
    targets: list[LiveTarget],
) -> dict[str, dict[str, int]]:
    """For one tenant per replay source (Gmail/Slack/GitHub), dispatch a
    unique event then an at-least-once redelivery of it; measure that the
    observation delta is 1 (not 2). Returns
    `{source: {'dispatched_unique': 1, 'observed': delta}}`.

    Discord is excluded — no replay surface (A24/A30.4). Each probe adds
    exactly one net observation; the runner accounts for it in the
    per-source expected count."""
    out: dict[str, dict[str, int]] = {}
    by_source: dict[str, list[LiveTarget]] = {}
    for t in targets:
        by_source.setdefault(t.source, []).append(t)

    for source in REPLAY_SOURCES:
        cand = by_source.get(source, [])
        if not cand:
            continue
        t = cand[-1]  # last tenant — keep clear of the twin tenant (cand[0])
        before = await _count_obs(pool, t.tenant_id)
        binding = _validation_runtime(source).replay_probe_binding
        if binding is None:
            raise RuntimeError(
                f"contract replay source {source!r} has no replay probe"
            )
        await resolve_callable_reference(binding)(drivers, t)
        after = await _count_obs(pool, t.tenant_id)
        out[source] = {"dispatched_unique": 1, "observed": after - before}
    return out


class _ProbeEmbedder:
    """Deterministic embedder for the partition-boundary probe (mirrors the writer test's
    embedder). The partition CheckViolationError fires at INSERT, AFTER
    embedding, so a valid-dim vector is needed to reach it."""

    class _C:
        model = "validation-probe"

    def __init__(self) -> None:
        from lib.embeddings.ollama import EMBEDDING_DIM
        self.config = self._C()
        self.config.expected_dim = EMBEDDING_DIM
        self._dim = EMBEDDING_DIM

    async def embed(self, text: str) -> list[float]:
        import hashlib
        import struct
        h = hashlib.sha512((text or "").encode("utf-8")).digest()
        buf = b""
        while len(buf) < self._dim * 4:
            buf += hashlib.sha512(buf + h).digest()
        vec: list[float] = []
        for i in range(self._dim):
            raw = struct.unpack("<f", buf[i * 4:(i + 1) * 4])[0]
            if not (-1e6 < raw < 1e6):
                raw = 0.0
            vec.append(max(-1.0, min(1.0, raw / 1e3)))
        return vec


def _partition_probe_contract(
    source: str,
) -> tuple[str, str, str, str]:
    """Return ingress, channel, trust, and kind from one source contract."""

    definition = source_definition(source)
    route = next(
        (
            candidate
            for candidate in definition.ingress_routes
            if candidate.ingress_kind != "backfill"
        ),
        definition.ingress_routes[0],
    )
    return (
        route.ingress_kind,
        route.channel,
        definition.default_trust_tier,
        definition.allowed_observation_kinds[0],
    )


@dataclass(frozen=True)
class PartitionProbeObservation:
    """One expected persistence outcome from the partition-boundary probe."""

    tenant_id: UUID
    external_id: str
    occurred_at: dt.datetime


@dataclass(frozen=True)
class PartitionBoundaryProbeResult:
    """Run-2's positive writer partition-boundary evidence."""

    recovered: dict[str, PartitionProbeObservation]
    rejected_out_of_bounds: dict[str, PartitionProbeObservation]
    recovered_partitions: dict[str, str]

    @property
    def recovery_count(self) -> int:
        return len(self.recovered)

    @property
    def out_of_bounds_count(self) -> int:
        return len(self.rejected_out_of_bounds)


def _shift_month(value: dt.datetime, delta_months: int) -> dt.datetime:
    """Return the first UTC instant of ``value``'s month plus ``delta_months``."""

    absolute_month = value.year * 12 + value.month - 1 + delta_months
    year, zero_based_month = divmod(absolute_month, 12)
    return dt.datetime(
        year,
        zero_based_month + 1,
        1,
        tzinfo=dt.timezone.utc,
    )


def _partition_recovery_candidates(
    *,
    as_of: dt.datetime,
) -> tuple[dt.datetime, ...]:
    """Deterministic, safely in-guardrail historical months for Run 2.

    The writer accepts roughly ten years of history. Starting eight years
    back leaves ample room for calendar/leap-year differences while providing
    77 unique candidate months. The probe selects only empty months and uses
    a distinct month for every canonical source.
    """

    month = as_of.astimezone(dt.timezone.utc).replace(
        day=1,
        hour=12,
        minute=0,
        second=0,
        microsecond=0,
    )
    return tuple(_shift_month(month, -months_ago) for months_ago in range(96, 19, -1))


async def _select_missing_recovery_month(
    pool: asyncpg.Pool,
    *,
    candidates: tuple[dt.datetime, ...],
    reserved: set[str],
) -> tuple[dt.datetime, str]:
    """Select a deterministic empty month and leave its partition absent.

    A populated partition is never dropped. An empty pre-existing partition
    (for example, from a prior validation run) is safe to drop. This makes
    every source exercise the actual writer self-heal instead of sharing the
    first source's newly created month.
    """

    for occurred_at in candidates:
        partition_name = f"observations_{occurred_at.strftime('%Y_%m')}"
        if partition_name in reserved:
            continue
        exists = await pool.fetchval("SELECT to_regclass($1)", partition_name)
        if exists is not None:
            row_count = int(
                await pool.fetchval(f'SELECT count(*) FROM "{partition_name}"'),
            )
            if row_count:
                continue
            await pool.execute(f'DROP TABLE "{partition_name}"')
        if await pool.fetchval("SELECT to_regclass($1)", partition_name) is not None:
            raise RuntimeError(
                f"failed to prepare missing recovery partition {partition_name}",
            )
        reserved.add(partition_name)
        return occurred_at.replace(day=15), partition_name
    raise RuntimeError(
        "Run 2 could not find a distinct empty in-guardrail month for every "
        "source without dropping populated observations partitions",
    )


async def partition_boundary_probe(
    pool: asyncpg.Pool,
    targets: list[LiveTarget],
    *,
    bootstrap_servers: str,
) -> PartitionBoundaryProbeResult:
    """Certify both sides of the production writer's partition contract.

    For exactly one tenant per canonical source this drives two normalized
    envelopes through the real ``observation_writer._handle_message``:

    * a distinct, in-guardrail historical month whose empty partition was
      made absent; the writer must create that month and persist the row;
    * a distinct far-future timestamp; the writer must publish an
      ``out_of_bounds_occurred_at`` DLQ envelope and persist no row.

    Every source gets a different recovery month, so later sources cannot
    accidentally pass because the first source already healed a shared
    partition. The real producer is used for DLQ and downstream publishes.
    """
    import orjson

    from services.domain.actors.repo import ActorRepo
    from services.domain.entity_aliases.repo import EntityAliasRepo
    from services.ingest.ingestion.feature_flags.client import (
        TenantFlags,
    )
    from services.ingest.ingestion.kafka.producer import (
        IdempotentProducer,
        ProducerConfig,
    )
    from services.ingest.ingestion.normalizer.models import NormalizedEnvelope
    from services.ingest.ingestion.writers import observation_writer as W

    target_by_source: dict[str, LiveTarget] = {}
    for target in targets:
        target_by_source.setdefault(target.source, target)
    canonical_sources = tuple(
        definition.source_id for definition in SOURCE_DEFINITIONS
    )
    missing_sources = [
        source for source in canonical_sources if source not in target_by_source
    ]
    if missing_sources:
        raise RuntimeError(
            "Run 2 partition-boundary probe is missing live targets for "
            f"{missing_sources!r}",
        )

    now = dt.datetime.now(tz=dt.timezone.utc)
    recovery_candidates = _partition_recovery_candidates(as_of=now)
    reserved_partitions: set[str] = set()
    flags = TenantFlags(pool)
    config = W.WriterConfig(
        pool=pool, tenant_flags=flags,
        actor_repo=ActorRepo(pool), alias_repo=EntityAliasRepo(pool),
        embedder=_ProbeEmbedder(),
    )
    producer = IdempotentProducer(
        ProducerConfig(bootstrap_servers=bootstrap_servers),
    )
    await producer.start()
    recovered: dict[str, PartitionProbeObservation] = {}
    rejected: dict[str, PartitionProbeObservation] = {}
    recovered_partitions: dict[str, str] = {}
    try:
        for source_index, source in enumerate(canonical_sources):
            t = target_by_source[source]
            ingress_kind, source_channel, trust_tier, observation_kind = (
                _partition_probe_contract(source)
            )
            await flags.set_bool(
                t.tenant_id, KAFKA_PATH_ENABLED, True,
                set_by="validation:run2",
                note="partition recovery and bounds probe",
            )

            recovery_at, partition_name = await _select_missing_recovery_month(
                pool,
                candidates=recovery_candidates,
                reserved=reserved_partitions,
            )
            recovery_external_id = (
                f"partition-recovery:{source}:{t.tenant_id.hex[:8]}"
            )
            recovery_env = NormalizedEnvelope(
                envelope_version=1,
                source=source,
                ingress_kind=ingress_kind,
                tenant_id=t.tenant_id,
                raw_s3_key=(
                    f"v/{source}/{t.tenant_id}/"
                    f"{recovery_at:%Y-%m}/partition-recovery.json"
                ),
                content_hash=f"partition-recovery-{source}-{t.tenant_id.hex[:8]}",
                raw_ingested_at=now,
                source_channel=source_channel,
                content_text="partition recovery probe",
                content={"probe": "partition_recovery"},
                occurred_at=recovery_at,
                trust_tier=trust_tier,
                kind=observation_kind,
                source_actor_ref=None,
                external_id=recovery_external_id,
                entities_hint=[],
                normalized_at=now,
                ingress_metadata={}, idem_hints={},
            )
            await W._handle_message(
                orjson.dumps(recovery_env.model_dump(mode="json")),
                config=config, dlq_producer=producer,
                embedding_producer=producer,
            )
            if await pool.fetchval("SELECT to_regclass($1)", partition_name) is None:
                raise RuntimeError(
                    f"{source} writer did not recover partition {partition_name}",
                )
            recovered[source] = PartitionProbeObservation(
                tenant_id=t.tenant_id,
                external_id=recovery_external_id,
                occurred_at=recovery_at,
            )
            recovered_partitions[source] = partition_name

            # Each source gets a distinct far-future month. Do not drop a
            # pre-existing partition here: fail closed rather than risk
            # deleting anything outside the validation-owned recovery range.
            out_of_bounds_at = _shift_month(
                dt.datetime(2100, 1, 1, tzinfo=dt.timezone.utc),
                source_index,
            ).replace(day=15, hour=12)
            out_of_bounds_partition = (
                f"observations_{out_of_bounds_at.strftime('%Y_%m')}"
            )
            if (
                await pool.fetchval(
                    "SELECT to_regclass($1)",
                    out_of_bounds_partition,
                )
                is not None
            ):
                raise RuntimeError(
                    "Run 2 refuses to alter unexpected far-future partition "
                    f"{out_of_bounds_partition}",
                )
            out_of_bounds_external_id = (
                f"partition-out-of-bounds:{source}:{t.tenant_id.hex[:8]}"
            )
            out_of_bounds_env = recovery_env.model_copy(
                update={
                    "raw_s3_key": (
                        f"v/{source}/{t.tenant_id}/2100/"
                        "partition-out-of-bounds.json"
                    ),
                    "content_hash": (
                        f"partition-out-of-bounds-{source}-"
                        f"{t.tenant_id.hex[:8]}"
                    ),
                    "content_text": "partition out-of-bounds probe",
                    "content": {"probe": "partition_out_of_bounds"},
                    "occurred_at": out_of_bounds_at,
                    "external_id": out_of_bounds_external_id,
                },
            )
            await W._handle_message(
                orjson.dumps(out_of_bounds_env.model_dump(mode="json")),
                config=config,
                dlq_producer=producer,
                embedding_producer=producer,
            )
            if (
                await pool.fetchval(
                    "SELECT to_regclass($1)",
                    out_of_bounds_partition,
                )
                is not None
            ):
                raise RuntimeError(
                    f"{source} spawned out-of-bounds partition "
                    f"{out_of_bounds_partition}",
                )
            rejected[source] = PartitionProbeObservation(
                tenant_id=t.tenant_id,
                external_id=out_of_bounds_external_id,
                occurred_at=out_of_bounds_at,
            )
    finally:
        await producer.stop()
    return PartitionBoundaryProbeResult(
        recovered=recovered,
        rejected_out_of_bounds=rejected,
        recovered_partitions=recovered_partitions,
    )


async def wait_for_live_consumer_drain(
    pool: asyncpg.Pool, tenant_ids: set[UUID], *,
    stable_for_s: float = 2.0, poll_interval_s: float = 0.5,
    timeout_s: float = 20.0,
) -> bool:
    """Live writes are inline, so this is a stability poll: return once
    the total observation count for the tenants holds steady for
    `stable_for_s`. Mirrors the backfill drain's shape (D4)."""
    deadline = time.monotonic() + timeout_s
    last = -1
    stable_since = None
    ids = list(tenant_ids)
    while time.monotonic() < deadline:
        cur = sum([await _count_obs(pool, tenant_id) for tenant_id in ids])
        now = time.monotonic()
        if cur == last:
            if stable_since is None:
                stable_since = now
            elif now - stable_since >= stable_for_s:
                return True
        else:
            stable_since = None
            last = cur
        await asyncio.sleep(poll_interval_s)
    return False
