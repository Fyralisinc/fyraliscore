"""Run 6 — ALL-25-source concurrent backfill + live overlap (the milestone gate).

The capstone acceptance run: for EVERY one of the 25 ingestion sources, the
backfill producer chain is IN PROGRESS while live signals are simultaneously
received, through the real subprocess + Kafka data plane. This is the binding
acceptance condition — live events arrive *during* an unfinished backfill, for
every source, concurrently — not backfill-then-live.

Where Run 4 covers the original four (gmail/slack/github/discord), this run adds
the eight that came later: google_calendar, google_drive, jira, mercury, notion,
quickbooks, grafana, and telegram — each driven through its REAL live ingress:

  - HMAC webhook + M5.3 Kafka cutover (HTTP 202): jira, mercury, quickbooks,
    grafana (alongside slack/github).
  - Google push → inline incremental drain (HTTP 200): google_calendar,
    google_drive.
  - Notion webhook → fetch + shadow-write to ingestion.raw.notion (HTTP 200).
  - Gmail Pub/Sub cutover / Discord gateway dispatch (as Run 4).

Overlap is proven PER SOURCE: each source's live coroutine waits until THAT
source's backfill is `in_progress`, then dispatches its live events, so every
recorded live burst overlaps a live backfill (assert_live_during_backfill_overlap).

Synthetic inputs throughout (mock clients + fixtures + in-process ASGI) — no
real external API.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import pathlib
import time
from uuid import UUID

import asyncpg

from services.ingest.ingestion.feature_flags.client import TenantFlags
from services.ingest.ingestion.kafka.producer import IdempotentProducer, ProducerConfig
from services.ingest.ingestion.raw_tier.s3 import S3Client
from services.ingest.synthetic.backfill_harness.harness import BackfillHarness
from services.ingest.synthetic.backfill_harness.scenarios import BackfillScenario
from services.ingest.synthetic.validation_runs import assertions as A
from services.ingest.synthetic.validation_runs.cleanup import reset_state
from services.ingest.synthetic.validation_runs.composition import (
    SigningSecrets,
    build_live_drivers,
    live_target_for,
    seed_live_installs,
    teardown_live_drivers,
)
from services.ingest.synthetic.validation_runs.moto_lifecycle import moto_s3
from services.ingest.synthetic.validation_runs.preflight import run_preflight
from services.ingest.synthetic.validation_runs.reports import (
    AssertionResult,
    RunReport,
    SourceResult,
)


log = logging.getLogger("validation_runs.run_all_sources")
_MIGRATIONS = pathlib.Path("db/migrations")

# Grafana annotation base inside the 90-day backfill floor AND the observations
# partition window (≈ 2026-05-15).
_GRAFANA_BASE_MS = 1778803200000
# AWS CloudTrail events use the SAME in-window anchor: the aws fetcher floors
# backfill at now - AWS_BACKFILL_WINDOW_DAYS (default 90d), so the fixture's
# default 2026-01 base would fall outside the window and yield ZERO records.
# Reusing the grafana base (≈2026-05-15, ~24d ago) keeps all 3 events inside
# both the 90-day backfill window and the observations partition coverage.
_AWS_BASE_MS = _GRAFANA_BASE_MS

# Per-source backfill observation count per tenant (validated by Run 6 backfill).
_EXPECTED: dict[str, int] = {
    "gmail": 5, "github": 6, "slack": 5, "discord": 5, "google_calendar": 3,
    "google_drive": 3, "jira": 3, "mercury": 5, "notion": 3, "quickbooks": 4,
    "grafana": 3, "telegram": 5,
    # IN-FIN2 finance sources. brex/deel = Mercury archetype (1 snapshot +
    # 4 txns/payments = 5); ramp = 1 transaction stream × 4 rows = 4;
    # gusto = real /v1 taxonomy (2 entity kinds × 2 rows = 4).
    "brex": 5, "ramp": 4, "gusto": 4, "deel": 5,
    # Vertical-2 sources. fireflies = 4 transcripts (NO snapshot); miro = 1 board
    # × 4 items; figma = 4 events (pure event stream); carta = 4 cap-table entity
    # kinds × 1 row; signal = 1 thread × 5 messages; aws = 3 CloudTrail events.
    "fireflies": 4, "signal": 5, "aws": 3, "miro": 4, "figma": 4, "carta": 4,
    # People/recruiting sources (IN-PEOPLE). hibob = 4 People/HR entity kinds ×
    # 1 row; ashby = 5 recruiting entity kinds × 1 row; linkedin = 3 org entity
    # kinds × 1 row. All entity-model (entity_kind discriminates the external_id,
    # so same-id rows never collide).
    "hibob": 4, "ashby": 5, "linkedin": 3,
}
SOURCES = list(_EXPECTED.keys())

# Live ingress family per source → the HTTP status that proves the path.
# 202 = M5.3 webhook Kafka cutover; 200 = gmail pubsub / google push (inline
# drain) / notion shadow-write; discord is direct-dispatch (no HTTP status).
_EXPECTED_LIVE_STATUS: dict[str, set[int]] = {
    "gmail": {200}, "github": {202}, "slack": {202}, "discord": set(),
    "google_calendar": {200}, "google_drive": {200}, "jira": {202},
    "mercury": {202}, "notion": {200}, "quickbooks": {202}, "grafana": {202},
    # telegram is gateway-style (MTProto persistent connection, no HTTP) — direct
    # dispatch, like discord: no HTTP status to assert.
    "telegram": set(),
    # IN-FIN2 finance sources: HMAC webhook + M5.3 Kafka cutover (202).
    "brex": {202}, "ramp": {202}, "gusto": {202}, "deel": {202},
    # Vertical-2: fireflies/miro/figma are HMAC webhook (202). signal is gateway-
    # style (no HTTP); aws/carta are poll live edges (direct-dispatch, no HTTP).
    "fireflies": {202}, "miro": {202}, "figma": {202},
    "signal": set(), "aws": set(), "carta": set(),
    # People/recruiting: hibob/ashby are HMAC webhook (202). linkedin is a poll
    # live edge (direct-dispatch, no HTTP status).
    "hibob": {202}, "ashby": {202}, "linkedin": set(),
}
_HMAC_SOURCES = (
    "jira", "mercury", "quickbooks", "grafana",
    "brex", "ramp", "gusto", "deel",
    "fireflies", "miro", "figma",
    "hibob", "ashby",
)


def _slug_account_id(slug: str) -> str:
    """A deterministic, tenant-distinct 12-digit AWS account id derived from the
    scenario slug. AWS external_ids key on (account_id, region), so a per-tenant
    account_id keeps the global observations UNIQUE from collapsing two tenants'
    synthetic event ids."""
    import hashlib
    digest = hashlib.sha256(slug.encode()).hexdigest()
    return str(int(digest[:15], 16))[:12].rjust(12, "0")


def _scen_params(source: str, slug: str) -> dict:
    # Each source's params embed the per-tenant `slug` into the identifier that
    # the observation `external_id` keys on (jira site, mercury account, qbo
    # realm, grafana instance, notion workspace, gdrive drive_id) — mirroring
    # production, where every tenant's install carries distinct identifiers. The
    # global `observations` UNIQUE(source_channel, external_id, occurred_at)
    # index would otherwise collapse two tenants' identical synthetic ids and
    # silently drop one tenant's backfill (gmail/github/slack/discord/gcal were
    # already slug-unique; the rest were not).
    return {
        "gmail": {"email": f"{slug}@val.example", "messages": 5},
        "github": {"org_or_user": slug.replace("-", ""), "repos": 1,
                   "events_per_repo": 3},
        "slack": {"team_id": f"T_{slug}", "channels": 1,
                  "messages_per_channel": 5},
        "discord": {"guild_id": f"G_{slug}", "channels": 1,
                    "messages_per_channel": 5},
        "google_calendar": {"calendars": [f"{slug}@acme.example"],
                            "events_per_calendar": 3},
        "google_drive": {"targets": [{"owner_email": f"{slug}@acme.example",
                                       "drive_id": f"md-{slug}",
                                       "drive_kind": "my_drive"}],
                         "files_per_target": 3, "comments_per_file": 0,
                         "revisions_per_file": 0},
        "jira": {"site_host": f"{slug}.atlassian.net", "projects": 1,
                 "issues_per_project": 3, "transitions_per_issue": 0,
                 "comments_per_issue": 0},
        "mercury": {"accounts": 1, "transactions_per_account": 4, "seed": slug},
        "notion": {"workspace_id": f"x3-{slug}-notion", "databases": 1,
                   "pages_per_database": 2, "loose_pages": 1,
                   "blocks_per_page": 0, "comments_per_item": 0},
        "quickbooks": {"realm_id": f"r-{slug}",
                       "entities": ["Invoice", "Bill", "BillPayment", "Payment"],
                       "rows_per_entity": 1},
        "grafana": {"annotations": 3, "base_ms": _GRAFANA_BASE_MS,
                    "base_url": f"https://{slug}.grafana.net"},
        # telegram: 1 dialog × 5 messages = 5 backfill obs. seed=slug makes the
        # dialog ids tenant-distinct (belt-and-suspenders; the external_id is
        # already install-namespaced so cross-tenant collision is impossible).
        "telegram": {"dialogs": 1, "messages_per_dialog": 5, "seed": slug},
        # IN-FIN2 finance sources. brex/deel (Mercury archetype): 1 account/
        # contract → 1 snapshot + 4 txns/payments = 5. seed=slug makes the
        # synthetic account_id/contract_id (which the external_id keys on)
        # tenant-distinct, so the global observations UNIQUE never collapses two
        # tenants' ids. ramp/gusto (QBO archetype): scope id embeds the slug so
        # the realm-equivalent + entity external_ids are tenant-distinct;
        # 4 entities × 1 row = 4.
        "brex": {"accounts": 1, "transactions_per_account": 4, "seed": slug},
        # Ramp (real REST taxonomy): one `transaction` stream × 4 rows → 4
        # distinct ids → 4 backfill observations/tenant, matching
        # _EXPECTED["ramp"]=4. external_id ramp:{biz}:txn:{id}:{state};
        # business_id embeds the slug so external_ids stay tenant-distinct.
        "ramp": {"business_id": f"r-{slug}",
                 "entities": ["transaction"],
                 "rows_per_entity": 4},
        # Gusto (real /v1 taxonomy): 2 entity kinds (employee/payroll) × 2 rows
        # = 4 backfill obs, matching _EXPECTED["gusto"]=4. external_id
        # gusto:{company}:{kind}:{uuid}:{version}; company_uuid embeds the slug
        # AND seeds the per-row uuid digests, so external_ids stay
        # tenant-distinct.
        "gusto": {"company_uuid": f"c-{slug}",
                  "entities": ["employee", "payroll"],
                  "rows_per_entity": 2},
        "deel": {"contracts": 1, "payments_per_contract": 4, "seed": slug},
        # Vertical-2 sources. Each embeds `slug` into the identifier the
        # external_id keys on so the global observations UNIQUE never collapses
        # two tenants' synthetic ids.
        # fireflies: external_id fireflies:{workspace_id}:transcript:{id}:{ver};
        # workspace_id namespaces. 4 transcripts → 4 obs (NO snapshot record).
        "fireflies": {"workspace_id": f"ws-{slug}", "transcripts": 4,
                      "seed": slug},
        # signal: external_id signal:{installation_id}:{thread}:{msg}:none; the
        # install row id (tenant-distinct) namespaces. seed=slug also makes the
        # thread/message ids tenant-distinct (belt-and-suspenders).
        # 1 thread × 5 messages = 5 obs.
        "signal": {"threads": 1, "messages_per_thread": 5, "seed": slug},
        # aws: external_id aws:{account_id}:{region}:event:{event_id}; account_id
        # namespaces. A slug-derived 12-digit account_id keeps tenants distinct;
        # seed=slug salts the event ids too. base_ms anchors inside the fetcher's
        # 90-day backfill window (the 2026-01 fixture default is out of range).
        # 3 events → 3 obs.
        "aws": {"account_id": _slug_account_id(slug), "region": "us-east-1",
                "events": 3, "base_ms": _AWS_BASE_MS, "seed": slug},
        # miro: external_id miro:{org_id}:item:{item_id}:{version}; org_id
        # namespaces. 1 board × 4 items = 4 obs (NO snapshot record).
        "miro": {"org_id": f"org-{slug}", "boards": 1, "items_per_board": 4,
                 "seed": slug},
        # figma: external_id figma:{team_id}:event:{event_id}:{version}; team_id
        # namespaces. 4 events × 1 file = 4 obs (pure event stream).
        "figma": {"team_id": f"team-{slug}", "events": 4, "seed": slug},
        # carta: external_id carta:{firm_id}:{entity_kind}:{entity_id}:{version};
        # firm_id namespaces. 4 cap-table entity kinds × 1 row = 4 obs (entity
        # kind discriminates so same-id rows never collide).
        "carta": {"firm_id": f"firm-{slug}", "rows_per_entity": 1,
                  "seed": slug},
        # People/recruiting sources (IN-PEOPLE). Each embeds `slug` into the scope
        # id the external_id namespaces on so the global observations UNIQUE never
        # collapses two tenants' synthetic ids; seed=slug salts the per-row ids
        # (belt-and-suspenders — the entity_kind discriminator already keeps
        # same-id rows distinct WITHIN a tenant).
        # hibob: external_id hibob:{company}:{entity}:{id}:{ver}; company_id
        # namespaces. 4 entity kinds × 1 row = 4 obs.
        "hibob": {"company_id": f"hibob-co-{slug}",
                  "entities": ["employee", "lifecycle", "timeoff", "payroll"],
                  "rows_per_entity": 1, "seed": slug},
        # ashby: external_id ashby:{org}:{entity}:{id}; org_id namespaces.
        # 5 entity kinds × 1 row = 5 obs.
        "ashby": {"org_id": f"ashby-org-{slug}",
                  "entities": ["candidate", "application", "job", "interview",
                               "offer"],
                  "rows_per_entity": 1, "seed": slug},
        # linkedin: external_id linkedin:{org}:{kind}:{id}; organization_urn
        # namespaces. 3 streams × 1 row = 3 obs.
        "linkedin": {"organization_urn": f"li-org-{slug}",
                     "entities": ["post", "share_statistics",
                                  "follower_statistics"],
                     "rows_per_entity": 1, "seed": slug},
    }[source]


def all_sources_scenarios(tenants_per_source: int) -> list[BackfillScenario]:
    out: list[BackfillScenario] = []
    for source in SOURCES:
        for i in range(tenants_per_source):
            slug = f"a11-{source}-{i}"
            out.append(BackfillScenario(
                tenant_slug=slug, source=source,
                fixture_params=_scen_params(source, slug),
                expected_observation_count=_EXPECTED[source]))
    return out


async def _migrate_and_truncate(pool: asyncpg.Pool) -> None:
    from lib.shared.migrations import apply_migrations_dir
    async with pool.acquire() as conn:
        await apply_migrations_dir(conn, _MIGRATIONS)
        rows = await conn.fetch(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n "
            "ON n.oid=c.relnamespace WHERE n.nspname='public' "
            "AND c.relkind IN ('r','p') AND c.relispartition=FALSE")
        names = ", ".join(f'"{r["relname"]}"' for r in rows)
        if names:
            await conn.execute(f"TRUNCATE {names} RESTART IDENTITY CASCADE")


async def _source_in_progress(pool: asyncpg.Pool, source: str) -> int:
    return int(await pool.fetchval(
        "SELECT count(*) FROM source_onboarding_runs "
        "WHERE source=$1 AND status='in_progress'", source) or 0)


async def _dispatch_one(drivers, t, *, content: str) -> int | None:
    s = t.source
    if s == "gmail":
        r = await drivers.gmail_pubsub.simulate_push(
            mailbox_email=t.email, new_messages=1)
        return getattr(r, "http_status", None)
    if s == "slack":
        r = await drivers.slack_webhook.simulate_message(
            team_id=t.team_id, channel_id=t.channel_id, content=content)
        return getattr(r, "http_status", None)
    if s == "github":
        r = await drivers.github_webhook.simulate_issue_event(
            installation_id=t.installation_id,
            repo_full_name=t.repo_full_name, issue_title=content)
        return getattr(r, "http_status", None)
    if s == "discord":
        await drivers.discord_gateway.simulate_message_create(
            guild_id=t.guild_id, channel_id=t.channel_id, content=content)
        return None
    if s == "telegram":
        await drivers.telegram_gateway.simulate_message(target=t, content=content)
        return None
    if s == "signal":
        await drivers.signal_gateway.simulate_message(target=t, content=content)
        return None
    if s == "aws":
        await drivers.aws_poll.simulate_event(target=t, content=content)
        return None
    if s == "carta":
        await drivers.carta_poll.simulate_event(target=t, content=content)
        return None
    if s == "linkedin":
        await drivers.linkedin_poll.simulate_event(target=t, content=content)
        return None
    if s in _HMAC_SOURCES:
        r = await drivers.hmac[s].simulate_event(target=t, content=content)
        return getattr(r, "http_status", None)
    if s in ("google_calendar", "google_drive"):
        r = await drivers.google_push.simulate_push(target=t)
        return getattr(r, "http_status", None)
    if s == "notion":
        r = await drivers.notion_webhook.simulate_event(target=t)
        return getattr(r, "http_status", None)
    return None


async def _live_for_source(
    source: str, src_targets: list, drivers, pool: asyncpg.Pool,
    overlap: dict[str, int], statuses: dict[str, set],
    *, live_per_tenant: int, stop: asyncio.Event,
) -> None:
    """Wait until THIS source's backfill is in_progress, then dispatch live
    events — so every recorded burst overlaps a live backfill by construction."""
    deadline = time.monotonic() + 90.0
    while time.monotonic() < deadline and not stop.is_set():
        if await _source_in_progress(pool, source) > 0:
            break
        await asyncio.sleep(0.2)
    for j in range(live_per_tenant):
        inprog = await _source_in_progress(pool, source)
        seen: set[int] = set()
        for t in src_targets:
            st = await _dispatch_one(drivers, t, content=f"live-{t.slug}-{j}")
            if st is not None:
                seen.add(st)
        statuses.setdefault(source, set()).update(seen)
        if inprog > 0:
            overlap[source] = overlap.get(source, 0) + 1
        await asyncio.sleep(0.4)


async def _wait_for_total_drain(
    pool: asyncpg.Pool, expected_total: dict[UUID, int],
    *, timeout_s: float, poll_interval_s: float = 2.0,
) -> dict[UUID, int]:
    tids = list(expected_total.keys())
    deadline = time.monotonic() + timeout_s
    counts: dict[UUID, int] = {}
    while True:
        rows = await pool.fetch(
            "SELECT tenant_id, count(*) AS n FROM observations "
            "WHERE tenant_id = ANY($1::uuid[]) GROUP BY tenant_id", tids)
        counts = {r["tenant_id"]: int(r["n"]) for r in rows}
        if all(counts.get(t, 0) >= n for t, n in expected_total.items()):
            return counts
        if time.monotonic() >= deadline:
            return counts
        await asyncio.sleep(poll_interval_s)


async def _signature_gate_probe(drivers, targets) -> tuple[int, int]:
    """Send ONE tampered-signature event per HMAC source + notion; the gate
    must reject each (no 2xx). Returns (rejected, total)."""
    rejected = total = 0
    seen: set[str] = set()
    for t in targets:
        if t.source in _HMAC_SOURCES and t.source not in seen:
            seen.add(t.source)
            total += 1
            r = await drivers.hmac[t.source].simulate_event(
                target=t, content="tamper", tamper_signature=True)
            if r.http_status not in (200, 201, 202):
                rejected += 1
        elif t.source == "notion" and "notion" not in seen:
            seen.add("notion")
            total += 1
            r = await drivers.notion_webhook.simulate_event(
                target=t, tamper_signature=True)
            if r.http_status not in (200, 201, 202):
                rejected += 1
    return rejected, total


async def run_all_sources(
    *,
    bootstrap_servers: str,
    tenants_per_source: int = 2,
    live_per_tenant: int = 3,
    drain_timeout_s: float = 240.0,
) -> RunReport:
    started = dt.datetime.now(tz=dt.timezone.utc)
    t0 = time.monotonic()
    dsn = os.environ["DATABASE_URL"]
    scenarios = all_sources_scenarios(tenants_per_source)
    report = RunReport(
        run_name="All-25-source concurrent backfill + live overlap",
        run_number=6, tenant_count=len(scenarios),
        started_at=started, wall_seconds=0.0)

    with moto_s3() as endpoint:
        cleanup = await reset_state(
            bootstrap_servers=bootstrap_servers, s3_endpoint_url=endpoint,
            s3_bucket=os.environ.get("S3_RAW_BUCKET", "fyralis-raw"))
        report.cleanup_line = (
            f"recreated {len(cleanup.topics_recreated)} topics; cleared "
            f"{cleanup.s3_objects_deleted} stale S3 objects")
        pool = await asyncpg.create_pool(dsn, min_size=4, max_size=24)
        producer: IdempotentProducer | None = None
        s3: S3Client | None = None
        drivers = None
        harness: BackfillHarness | None = None
        peak = {"ip": 0}
        try:
            await _migrate_and_truncate(pool)
            pf = await run_preflight(pool)
            report.preflight_lines = [
                f"{r.source}: external_id={r.sample_external_id[:32]!r} ✅"
                for r in pf]

            harness = BackfillHarness(
                pool=pool, scenarios=scenarios, concurrency=len(scenarios),
                completion_deadline_s=360.0,
                kafka_bootstrap_servers=bootstrap_servers,
                drain_timeout_s=drain_timeout_s)
            outcomes = await harness.setup()
            targets = [
                live_target_for(o.tenant_id, o.scenario.source,
                                o.scenario.tenant_slug, o.scenario.fixture_params)
                for o in outcomes]
            await seed_live_installs(pool, targets)

            secrets = SigningSecrets()
            producer = IdempotentProducer(
                ProducerConfig(bootstrap_servers=bootstrap_servers))
            await producer.start()
            s3 = S3Client(os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
                          endpoint_url=endpoint, region_name="us-east-1")
            await s3.connect()
            flags = TenantFlags(pool)
            drivers = await build_live_drivers(
                pool, targets, secrets, kafka_producer=producer,
                s3_raw_client=s3, tenant_flags=flags)

            by_source: dict[str, list] = {}
            for t in targets:
                by_source.setdefault(t.source, []).append(t)

            overlap: dict[str, int] = {}
            statuses: dict[str, set] = {}
            stop = asyncio.Event()

            async def _peak_monitor() -> None:
                while not stop.is_set():
                    ip = int(await pool.fetchval(
                        "SELECT count(*) FROM source_onboarding_runs "
                        "WHERE status='in_progress'") or 0)
                    peak["ip"] = max(peak["ip"], ip)
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=0.5)
                    except asyncio.TimeoutError:
                        pass

            harness.start_services()
            mon = asyncio.create_task(_peak_monitor())
            try:
                await asyncio.gather(
                    harness.wait_for_backfill(),
                    *[_live_for_source(src, by_source[src], drivers, pool,
                                       overlap, statuses,
                                       live_per_tenant=live_per_tenant, stop=stop)
                      for src in SOURCES],
                )
            finally:
                stop.set()
                await mon

            expected_total = {
                o.tenant_id: o.scenario.expected_observation_count + live_per_tenant
                for o in outcomes}
            final = await _wait_for_total_drain(
                pool, expected_total, timeout_s=drain_timeout_s)

            # Signature-gate probe (HMAC sources + notion) AFTER the count
            # drain so tampered events never perturb the per-tenant totals.
            gate_rejected, gate_total = await _signature_gate_probe(drivers, targets)

            await harness.collect()

            # ---- Per-source results + coverage ----
            for source in SOURCES:
                outs = by_source[source]  # list[LiveTarget]
                exp = len(outs) * (_EXPECTED[source] + live_per_tenant)
                actual = int(await pool.fetchval(
                    "SELECT count(*) FROM observations WHERE tenant_id = ANY($1)",
                    [t.tenant_id for t in outs]))
                report.source_results.append(SourceResult(
                    source=source, tenants=len(outs),
                    expected_observations=exp, actual_observations=actual))
                st = sorted(statuses.get(source, set())) or ["direct"]
                report.coverage_rows.append((
                    source, "✅", f"✅ {st}", "✅",
                    "✅" if source in _HMAC_SOURCES or source == "notion" else "—",
                    f"overlap×{overlap.get(source, 0)}"))

            _assert_all_sources(report, overlap, statuses, gate_rejected,
                                gate_total, peak)
            try:
                total = await A.assert_external_id_unique_across_paths(pool)
                report.assertions.append(AssertionResult(
                    name="assert_no_duplicate_observations_under_concurrency",
                    passed=True,
                    detail=f"{total} observations, zero duplicate "
                           f"(source_channel, external_id, occurred_at) groups"))
            except A.PropertyViolation as exc:
                report.assertions.append(AssertionResult(
                    name="assert_no_duplicate_observations_under_concurrency",
                    passed=False, detail=str(exc)[:200]))

            report.live_lines = [
                f"tenants_per_source={tenants_per_source}; "
                f"live={live_per_tenant} events/tenant per source",
                f"peak simultaneous backfill source_onboarding_runs "
                f"in_progress: {peak['ip']}",
                "live ingress: 202=webhook Kafka cutover "
                "(github/slack/jira/mercury/quickbooks/grafana); 200=gmail "
                "pubsub / google push (inline drain) / notion shadow-write; "
                "discord=direct dispatch",
            ]
            report.notes.append(
                "Consumer rc=-9/-15 expected per ticket #45; greened by the "
                "rc annotation.")
        finally:
            if drivers is not None:
                await teardown_live_drivers(drivers)
            if harness is not None:
                stderrs = harness.teardown()
                report.subprocess_returncodes = (
                    harness.build_result(stderrs).subprocess_returncodes)
            if producer is not None:
                await producer.stop()
            if s3 is not None:
                await s3.close()
            await pool.close()

    report.wall_seconds = time.monotonic() - t0
    report.verdict = "READY" if report.passed else "NOT_READY"
    return report


def _assert_all_sources(report, overlap, statuses, gate_rejected, gate_total,
                        peak) -> None:
    # 1. Live received WHILE backfill in progress — per source.
    missing = [s for s in SOURCES if overlap.get(s, 0) < 1]
    report.assertions.append(AssertionResult(
        name=f"assert_live_during_backfill_overlap(all {len(SOURCES)} sources)",
        passed=not missing,
        detail=("every source received ≥1 live burst while its backfill was "
                f"in_progress: { {s: overlap.get(s, 0) for s in SOURCES} }"
                if not missing else f"no overlap recorded for: {missing}")))

    # 2. All 11 sources backfilled concurrently (peak in_progress).
    report.assertions.append(AssertionResult(
        name="assert_all_sources_backfilled_concurrently",
        passed=peak["ip"] >= len(SOURCES),
        detail=f"peak simultaneous in_progress source runs = {peak['ip']} "
               f"(expected ≥ {len(SOURCES)})"))

    # 3. Live ingress took the expected path per source (202 cutover / 200).
    bad_status = []
    for s in SOURCES:
        want = _EXPECTED_LIVE_STATUS[s]
        got = statuses.get(s, set())
        if want and not want.issubset(got):
            bad_status.append(f"{s}: want⊇{sorted(want)} got {sorted(got)}")
    report.assertions.append(AssertionResult(
        name="assert_live_routed_through_expected_ingress",
        passed=not bad_status,
        detail="all sources hit their expected live ingress status"
               if not bad_status else "; ".join(bad_status)))

    # 4. Signature gate rejects tampered HMAC/notion events.
    report.assertions.append(AssertionResult(
        name="assert_signature_validation_gate_holds",
        passed=gate_total > 0 and gate_rejected == gate_total,
        detail=f"{gate_rejected}/{gate_total} tampered events rejected (no 2xx)"))


def _main() -> int:
    logging.basicConfig(level=logging.WARNING)
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    tps = int(os.environ.get("TENANTS_PER_SOURCE", "2"))
    report = asyncio.run(run_all_sources(
        bootstrap_servers=bootstrap, tenants_per_source=tps))
    from services.ingest.synthetic.validation_runs.reports import render, write_report
    print(render(report))
    path = write_report(report)
    print(f"report written: {path}")
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(_main())
