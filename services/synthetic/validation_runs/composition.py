"""Live-phase orchestration (A30.1).

The spine validated the backfill path (Run 1). This module composes the
four in-process live generators into the runner's *live phase* so each
tenant — after its backfill drains — also ingests live events against the
SAME install backfill used.

Composition shape (per the Phase-1 substrate audit):
  - slack + github  → one shared FastAPI app (`build_app`, the canonical
    `services.gateway.main` builder). Tenant resolution is real
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

from services.synthetic.fixtures import (
    make_discord_guild,
    make_gmail_mailbox,
    make_github_repos,
    make_slack_workspace,
)
from services.synthetic.live_generators import (
    DiscordGatewayGenerator,
    GithubWebhookGenerator,
    GmailPubSubGenerator,
    GuildBinding,
    SlackWebhookGenerator,
)
from services.synthetic.mock_clients import (
    MockDiscordClient,
    MockGithubClient,
    MockGmailClient,
    MockSlackClient,
)


log = logging.getLogger("validation_runs.composition")

# Cross-path twin sources (Discord excluded by namespace topology, A30.3).
TWIN_SOURCES = ("gmail", "github", "slack")
HMAC_SOURCES = ("slack", "github")  # signature-gate sources (A30.4)
REPLAY_SOURCES = ("gmail", "slack", "github")  # Discord has no replay (A24)


# =====================================================================
# Signing-secret env (threaded into os.environ before app build).
# =====================================================================
@dataclass(frozen=True)
class SigningSecrets:
    slack: str = "v-slack-signing-secret"
    github: str = "v-github-signing-secret"
    # A symmetric AES key the secret-store factory needs in `test` env.
    master_kek: str = "KuT6Cixjs4991zhixcpj1QAFbiQj3b9N8meZV2AJJyw="

    def apply_to_env(self) -> None:
        import os
        os.environ["WEBHOOK_SECRET_SLACK"] = self.slack
        os.environ["WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW"] = "1"
        os.environ["WEBHOOK_SECRET_GITHUB"] = self.github
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


def live_target_for(tenant_id: UUID, source: str, slug: str,
                    fixture_params: dict[str, Any]) -> LiveTarget:
    if source == "gmail":
        return LiveTarget(tenant_id=tenant_id, source=source, slug=slug,
                          email=fixture_params["email"])
    if source == "slack":
        return LiveTarget(tenant_id=tenant_id, source=source, slug=slug,
                          team_id=f"x3-{slug}-slack",
                          channel_id=f"C_LIVE_{slug}")
    if source == "discord":
        return LiveTarget(tenant_id=tenant_id, source=source, slug=slug,
                          guild_id=f"x3-{slug}-discord",
                          channel_id=f"chan_live_{slug}")
    if source == "github":
        return LiveTarget(tenant_id=tenant_id, source=source, slug=slug,
                          installation_id=f"x3-{slug}-github",
                          repo_full_name=f"{fixture_params.get('org_or_user', slug)}/live-{slug}")
    raise ValueError(f"unknown source {source!r}")


# =====================================================================
# LiveDrivers bundle.
# =====================================================================
@dataclass
class LiveDrivers:
    gmail_pubsub: GmailPubSubGenerator
    discord_gateway: DiscordGatewayGenerator
    slack_webhook: SlackWebhookGenerator
    github_webhook: GithubWebhookGenerator
    fastapi_app: FastAPI            # shared by slack + github
    gmail_app: FastAPI              # gmail's own minimal app
    _exit_stack: Any = None


async def build_live_drivers(
    pool: asyncpg.Pool,
    targets: list[LiveTarget],
    secrets: SigningSecrets,
) -> LiveDrivers:
    """Construct + enter all four generators against `targets`.

    Returns a `LiveDrivers` bundle; the caller MUST `await
    teardown_live_drivers(drivers)` to restore monkeypatches + close
    httpx clients. (Kept explicit rather than a context manager so the
    runner can interleave the live phase between backfill drain and
    assertion collection.)"""
    from contextlib import AsyncExitStack

    from services.actors.repo import ActorRepo
    from services.entity_aliases.repo import EntityAliasRepo
    from services.gateway.main import build_app
    from services.gateway.rate_limit import RateLimiter
    from services.integrations.discord.gateway.dispatch import DispatchDeps
    from services.webhooks.tenant_resolver import (
        InstallationCache,
        TenantResolverDeps,
        build_tenant_resolver,
        noop_metrics,
    )

    secrets.apply_to_env()

    # ---- Shared FastAPI app for slack + github webhooks ----
    shared_app = build_app(
        pool=pool,
        actor_repo=ActorRepo(pool),
        alias_repo=EntityAliasRepo(pool),
        embedder=None,
        rate_limiter=RateLimiter(),
        slack_signing_secret=secrets.slack,
        configure_logging=False,
    )

    # ---- Gmail's own minimal app (router not mounted by build_app) ----
    from services.webhooks.gmail_pubsub import router as gmail_router
    gmail_app = FastAPI()
    gmail_app.include_router(gmail_router)

    class _GmailDeps:
        pass
    _deps = _GmailDeps()
    _deps.pool = pool  # type: ignore[attr-defined]
    gmail_app.state.deps = _deps

    # ---- Per-source mock clients ----
    gmail_targets = [t for t in targets if t.source == "gmail"]
    slack_targets = [t for t in targets if t.source == "slack"]
    github_targets = [t for t in targets if t.source == "github"]
    discord_targets = [t for t in targets if t.source == "discord"]

    mailboxes = {
        t.email: MockGmailClient(
            fixture=make_gmail_mailbox(
                email=t.email, messages=0, starting_history_id=1000,
            ),
        )
        for t in gmail_targets
    }

    # slack/github: a single mock per source for state fidelity (tenant
    # resolution is by team_id/installation_id in the DB, not the mock).
    slack_mock = MockSlackClient(
        fixture=make_slack_workspace(
            team_id="LIVE_SHARED", channels=1, messages_per_channel=0,
        ),
    )
    github_mock = MockGithubClient(
        fixture=make_github_repos(
            org_or_user="live", repos=1, events_per_repo=0,
            installation_id="live-shared",
        ),
    )

    # discord: a guild binding per discord tenant. The mock fixture must
    # declare the channel the dispatcher appends to.
    guild_bindings: dict[str, GuildBinding] = {}
    for t in discord_targets:
        fixture = make_discord_guild(
            guild_id=t.guild_id, channels=1, messages_per_channel=0,
        )
        fixture["channels"][0]["id"] = t.channel_id
        guild_bindings[t.guild_id] = GuildBinding(
            guild_id=t.guild_id,
            mock_client=MockDiscordClient(fixture=fixture),
        )

    discord_resolver = build_tenant_resolver(
        TenantResolverDeps(
            pool=pool, cache=InstallationCache(),
            clock=time.monotonic, metrics=noop_metrics(),
        ),
    )
    discord_deps = DispatchDeps(
        pool=pool, tenant_resolver=discord_resolver,
        actor_repo=ActorRepo(pool), alias_repo=EntityAliasRepo(pool),
        embedder=None, application_id="v-discord-app",
    )

    # ---- Instantiate + enter the generators ----
    stack = AsyncExitStack()
    gmail_gen = await stack.enter_async_context(
        GmailPubSubGenerator(app=gmail_app, pool=pool, mailboxes=mailboxes),
    )
    discord_gen = await stack.enter_async_context(
        DiscordGatewayGenerator(
            dispatch_deps=discord_deps, guild_bindings=guild_bindings,
        ),
    )
    slack_gen = await stack.enter_async_context(
        SlackWebhookGenerator(
            app=shared_app, mock_client=slack_mock,
            signing_secret=secrets.slack,
        ),
    )
    github_gen = await stack.enter_async_context(
        GithubWebhookGenerator(
            app=shared_app, mock_client=github_mock,
            signing_secret=secrets.github,
        ),
    )

    return LiveDrivers(
        gmail_pubsub=gmail_gen, discord_gateway=discord_gen,
        slack_webhook=slack_gen, github_webhook=github_gen,
        fastapi_app=shared_app, gmail_app=gmail_app, _exit_stack=stack,
    )


async def teardown_live_drivers(drivers: LiveDrivers) -> None:
    if drivers._exit_stack is not None:
        await drivers._exit_stack.aclose()


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
        row = await pool.fetchrow(
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
    return int(await pool.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1", tenant_id,
    ))


async def _dispatch_regular(
    drivers: LiveDrivers, t: LiveTarget, n: int,
) -> None:
    """Dispatch `n` fresh (auto-mint) live events for one tenant."""
    if t.source == "gmail":
        for _ in range(n):
            await drivers.gmail_pubsub.simulate_push(
                mailbox_email=t.email, new_messages=1,
            )
    elif t.source == "slack":
        for i in range(n):
            await drivers.slack_webhook.simulate_message(
                team_id=t.team_id, channel_id=t.channel_id,
                content=f"live-{t.slug}-{i}",
            )
    elif t.source == "github":
        for i in range(n):
            await drivers.github_webhook.simulate_issue_event(
                installation_id=t.installation_id,
                repo_full_name=t.repo_full_name,
                issue_title=f"live-{t.slug}-{i}",
            )
    elif t.source == "discord":
        for i in range(n):
            await drivers.discord_gateway.simulate_message_create(
                guild_id=t.guild_id, channel_id=t.channel_id,
                content=f"live-{t.slug}-{i}",
            )


async def _dispatch_twin(
    drivers: LiveDrivers, t: LiveTarget, twin: TwinIdentity,
) -> str:
    """Replay the captured backfill identity live. Returns the
    external_id that must dedup."""
    if t.source == "slack":
        # external_id = "{channel}:{ts}"; occurred_at derives from ts.
        channel, _, ts = twin.external_id.partition(":")
        await drivers.slack_webhook.simulate_message(
            team_id=t.team_id, channel_id=channel, content="twin", ts=ts,
        )
    elif t.source == "github":
        # external_id IS node_id; occurred_at must match too.
        await drivers.github_webhook.simulate_issue_event(
            installation_id=t.installation_id,
            repo_full_name=t.repo_full_name,
            node_id=twin.external_id,
            occurred_at_iso=twin.occurred_at.isoformat(),
        )
    elif t.source == "gmail":
        # external_id = "gmail:{install}:{message_id}"; install is shared
        # because the generator reused backfill's watch (A30.1).
        parts = twin.external_id.split(":", 2)
        message_id = parts[2] if len(parts) == 3 else parts[-1]
        internal_date = str(int(twin.occurred_at.timestamp() * 1000))
        await drivers.gmail_pubsub.simulate_push(
            mailbox_email=t.email, new_messages=1,
            message_id=message_id, internal_date=internal_date,
        )
    return twin.external_id


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

    # ---- Tampered-signature probes (HMAC sources only) ----
    for t in targets:
        if t.source == "slack" and "slack" not in [
            r["source"] for r in result.tamper_results
        ]:
            r = await drivers.slack_webhook.simulate_message(
                team_id=t.team_id, channel_id=t.channel_id,
                content="tampered", tamper_signature=True,
            )
            result.tamper_results.append(
                {"source": "slack", "http_status": r.http_status})
        if t.source == "github" and "github" not in [
            r["source"] for r in result.tamper_results
        ]:
            r = await drivers.github_webhook.simulate_issue_event(
                installation_id=t.installation_id,
                repo_full_name=t.repo_full_name,
                issue_title="tampered", tamper_signature=True,
            )
            result.tamper_results.append(
                {"source": "github", "http_status": r.http_status})

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
        if source == "slack":
            ch = f"{t.channel_id}_rp"
            await drivers.slack_webhook.simulate_message(
                team_id=t.team_id, channel_id=ch, content="replay-unique",
            )
            await drivers.slack_webhook.simulate_message(
                team_id=t.team_id, channel_id=ch, content="replay-unique",
                replay=True,
            )
        elif source == "github":
            await drivers.github_webhook.simulate_issue_event(
                installation_id=t.installation_id,
                repo_full_name=f"{t.repo_full_name}-rp",
                issue_title="replay-unique",
            )
            await drivers.github_webhook.simulate_issue_event(
                installation_id=t.installation_id,
                repo_full_name=f"{t.repo_full_name}-rp", replay=True,
            )
        elif source == "gmail":
            await drivers.gmail_pubsub.simulate_push(
                mailbox_email=t.email, new_messages=1,
            )
            await drivers.gmail_pubsub.simulate_push(
                mailbox_email=t.email, new_messages=0, replay=True,
            )
        after = await _count_obs(pool, t.tenant_id)
        out[source] = {"dispatched_unique": 1, "observed": after - before}
    return out


_SOURCE_CHANNELS = {
    "gmail": ("gmail:", "backfill"),
    "slack": ("slack:message", "webhook"),
    "github": ("github:webhook", "webhook"),
    "discord": ("discord:message", "gateway"),
}


class _ProbeEmbedder:
    """Deterministic embedder for the A28 probe (mirrors the writer test's
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


async def partition_missing_probe(
    pool: asyncpg.Pool,
    targets: list[LiveTarget],
    *,
    bootstrap_servers: str,
) -> int:
    """A28 positive assertion under composition (Run 2): for one tenant
    per source, drive a NormalizedEnvelope whose `occurred_at` is OUTSIDE
    the observations partition coverage (2023-01-01) through the REAL
    `observation_writer._handle_message`, with a real `IdempotentProducer`
    publishing to the live `ingestion.dlq`. The writer must NOT raise
    (no crash-loop) and must route each to the DLQ as `partition_missing`.
    Returns the injection count (== expected DLQ entries).

    This is faithful to A28's production code path: real writer logic,
    real partitioned table (real CheckViolationError), real Kafka DLQ.
    The inline live path can't be used — it does not classify
    CheckViolationError to the DLQ (that branch is writer-only)."""
    import orjson

    from services.actors.repo import ActorRepo
    from services.entity_aliases.repo import EntityAliasRepo
    from services.ingestion.feature_flags.client import (
        KAFKA_PATH_ENABLED,
        TenantFlags,
    )
    from services.ingestion.kafka.producer import (
        IdempotentProducer,
        ProducerConfig,
    )
    from services.ingestion.normalizer.models import NormalizedEnvelope
    from services.ingestion.writers import observation_writer as W

    out_of_range = dt.datetime(2023, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
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
    n = 0
    seen: set[str] = set()
    try:
        for t in targets:
            if t.source in seen:
                continue
            seen.add(t.source)
            channel, ingress = _SOURCE_CHANNELS[t.source]
            await flags.set_bool(
                t.tenant_id, KAFKA_PATH_ENABLED, True,
                set_by="validation:run2", note="A28 partition-missing probe",
            )
            env = NormalizedEnvelope(
                envelope_version=1, source=t.source, ingress_kind=ingress,
                tenant_id=t.tenant_id,
                raw_s3_key=f"v/{t.source}/{t.tenant_id}/2023-01/oor.json",
                content_hash=f"oor-{t.source}-{t.tenant_id.hex[:8]}",
                raw_ingested_at=out_of_range, source_channel=channel,
                content_text="out-of-range partition probe",
                content={"probe": "partition_missing"},
                occurred_at=out_of_range, trust_tier="attested_agent",
                kind="signal", source_actor_ref=None,
                external_id=f"oor:{t.source}:{t.tenant_id.hex[:8]}",
                entities_hint=[], normalized_at=out_of_range,
                ingress_metadata={}, idem_hints={},
            )
            # MUST NOT raise (no crash-loop) — A28's contract.
            await W._handle_message(
                orjson.dumps(env.model_dump(mode="json")),
                config=config, dlq_producer=producer,
                embedding_producer=producer,
            )
            n += 1
    finally:
        await producer.stop()
    return n


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
        cur = int(await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id = ANY($1)",
            ids,
        ))
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
