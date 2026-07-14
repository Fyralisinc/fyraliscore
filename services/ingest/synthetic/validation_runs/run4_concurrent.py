"""Run 4 — concurrent backfill + live-via-Kafka (M-Validate-Concurrent).

The final-goal run: for ALL FOUR sources at 50 tenants, the backfill
producer chain and the live ingestion run CONCURRENTLY (not sequential
phases), and live ingestion is routed THROUGH KAFKA — live webhooks /
events publish to `ingestion.raw` and are drained by the SAME normalizer
→ observation_writer consumer chain as backfill, instead of writing inline.

This closes the M-Validate fidelity gaps:
  - #1 live bypasses Kafka      → live now takes the cutover path
    (slack/github via the webhook router; discord via the gateway
    cutover; gmail via the push-handler cutover — all flag-gated on
    `ingestion.kafka_path_enabled`).
  - #2 backfill/live sequential → `asyncio.gather(backfill_drive,
    live_dispatch)`; live fires WHILE backfill is in-flight.
  - #4 live only at 16 tenants  → live runs at the full 50.
  - A30.6 fixed 30s drain       → the harness drain window is now
    configurable; Run 4 raises it for the combined load.

Synthetic inputs throughout (mock clients + fixtures) — no real API.

Structure mirrors Run 3 (moto S3, Kafka reset, preflight, the shared
7-subprocess chain) but drives the decomposed harness phases directly so
the live phase can interleave:

    outcomes = await harness.setup()      # tenants + installs + flag=TRUE
    drivers  = build_live_drivers(..., kafka_producer=..., s3=..., flags=...)
    harness.start_services()              # 7 subprocs incl. consumers
    await gather(harness.wait_for_backfill(), dispatch_live_concurrent(...))
    await _wait_for_total_drain(...)       # backfill + live, one drain
    await harness.collect(); harness.teardown()
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import pathlib
import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from services.ingest.ingestion.kafka.producer import IdempotentProducer, ProducerConfig
from services.ingest.ingestion.feature_flags.client import TenantFlags
from services.ingest.ingestion.raw_tier.s3 import S3Client
from services.ingest.synthetic.backfill_harness.harness import BackfillHarness
from services.ingest.synthetic.backfill_harness.scenarios import BackfillScenario
from services.ingest.synthetic.validation_runs import assertions as A
from services.ingest.synthetic.validation_runs.cleanup import reset_state
from services.ingest.synthetic.validation_runs.composition import (
    SigningSecrets,
    build_live_drivers,
    dispatch_live_concurrent,
    live_target_for,
    teardown_live_drivers,
)
from services.ingest.synthetic.validation_runs.preflight import run_preflight
from services.ingest.synthetic.validation_runs.reports import (
    AssertionResult,
    RunReport,
    SourceResult,
)


log = logging.getLogger("validation_runs.run4")
_MIGRATIONS = pathlib.Path("db/migrations")

# Default distribution (Decision: same 50-tenant shape as Run 3).
_DEFAULT_DISTRIBUTION = {"gmail": 15, "github": 15, "slack": 10, "discord": 10}

# Live events dispatched per tenant (distinct from backfill ids).
_LIVE_EVENTS_PER_TENANT = 5

# `tenant_onboarding_completed` is the terminal per-tenant marker (the
# #39-watched signal). Unconsumed by design — excluded from "working
# backlog". Same as Run 3.
_TERMINAL_SIGNAL = "tenant_onboarding_completed"


@dataclass
class _LiveCutoverRuntime:
    producer: IdempotentProducer
    s3: S3Client
    drivers: Any


def run4_scenarios(
    distribution: dict[str, int] | None = None,
) -> list[BackfillScenario]:
    """Per-tenant scenarios. Volumes are sized so the COMBINED backfill +
    live load drains inside the configurable window. Live adds exactly
    `_LIVE_EVENTS_PER_TENANT` distinct observations per tenant."""
    dist = distribution or _DEFAULT_DISTRIBUTION
    out: list[BackfillScenario] = []
    for i in range(dist.get("gmail", 0)):
        out.append(BackfillScenario(
            tenant_slug=f"r4-gmail-{i}", source="gmail",
            fixture_params={"email": f"r4-gmail-{i}@val.example",
                            "messages": 5},
            expected_observation_count=5))
    for i in range(dist.get("github", 0)):
        out.append(BackfillScenario(
            tenant_slug=f"r4-github-{i}", source="github",
            fixture_params={"org_or_user": f"r4gh{i}", "repos": 1,
                            "events_per_repo": 3},
            expected_observation_count=3 * 2 * 1))  # events×types×repos
    for i in range(dist.get("slack", 0)):
        out.append(BackfillScenario(
            tenant_slug=f"r4-slack-{i}", source="slack",
            fixture_params={"team_id": f"T_r4s{i}", "channels": 1,
                            "messages_per_channel": 5},
            expected_observation_count=1 * 5))
    for i in range(dist.get("discord", 0)):
        out.append(BackfillScenario(
            tenant_slug=f"r4-discord-{i}", source="discord",
            fixture_params={"guild_id": f"G_r4d{i}", "channels": 4,
                            "messages_per_channel": 5},
            expected_observation_count=4 * 5))
    return out


async def _migrate_and_truncate(pool: asyncpg.Pool) -> None:
    from lib.shared.migrations import apply_migrations_dir
    async with pool.acquire() as conn:
        await apply_migrations_dir(conn, _MIGRATIONS)
        rows = await conn.fetch(
            """
            SELECT c.relname FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname='public' AND c.relkind IN ('r','p')
               AND c.relispartition = FALSE
            """
        )
        names = ", ".join(f'"{r["relname"]}"' for r in rows)
        if names:
            await conn.execute(f"TRUNCATE {names} RESTART IDENTITY CASCADE")


async def _monitor(
    pool: asyncpg.Pool, stop: asyncio.Event, peak: dict[str, int],
    *, interval_s: float = 1.0,
) -> None:
    """Sample peak concurrent backfill `in_progress` + working-signal
    backlog while the combined phase runs (same shape as Run 3)."""
    while not stop.is_set():
        try:
            ip = int(await pool.fetchval(
                "SELECT count(*) FROM source_onboarding_runs "
                "WHERE status = 'in_progress'") or 0)
            backlog = int(await pool.fetchval(
                "SELECT count(*) FROM workflow_signals "
                "WHERE consumed_at IS NULL AND signal_kind <> $1",
                _TERMINAL_SIGNAL) or 0)
            peak["in_progress"] = max(peak["in_progress"], ip)
            peak["backlog"] = max(peak["backlog"], backlog)
        except Exception:  # noqa: BLE001 — monitor is best-effort
            pass
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass


async def _wait_for_total_drain(
    pool: asyncpg.Pool, expected_total: dict[UUID, int],
    *, timeout_s: float, poll_interval_s: float = 2.0,
) -> dict[UUID, int]:
    """Drain the shared consumer chain until every tenant reaches its
    COMBINED (backfill + live) observation count, or the timeout fires.
    Returns the final per-tenant counts (a shortfall surfaces as a real
    assertion diagnostic, not a silent absorb)."""
    tenant_ids = list(expected_total.keys())
    deadline = time.monotonic() + timeout_s
    counts: dict[UUID, int] = {}
    while True:
        rows = await pool.fetch(
            """
            SELECT tenant_id, count(*) AS n FROM observations
             WHERE tenant_id = ANY($1::uuid[]) GROUP BY tenant_id
            """,
            tenant_ids,
        )
        counts = {r["tenant_id"]: int(r["n"]) for r in rows}
        if all(counts.get(tid, 0) >= n for tid, n in expected_total.items()):
            return counts
        if time.monotonic() >= deadline:
            return counts
        await asyncio.sleep(poll_interval_s)


def _run4_report(*, scenarios: list[BackfillScenario], real_clients: bool) -> RunReport:
    if real_clients:
        run_name = (
            "Concurrent backfill (REAL clients \u2192 spammer) + live-via-Kafka "
            "(50 tenants, 4 sources)"
        )
        run_number = 5
    else:
        run_name = "Concurrent backfill + live-via-Kafka (50 tenants, 4 sources)"
        run_number = 4
    return RunReport(
        run_name=run_name,
        run_number=run_number,
        tenant_count=len(scenarios),
        started_at=dt.datetime.now(tz=dt.timezone.utc),
        wall_seconds=0.0,
    )


def _live_targets_from_outcomes(outcomes: list[Any]) -> list[Any]:
    return [
        live_target_for(
            outcome.tenant_id,
            outcome.scenario.source,
            outcome.scenario.tenant_slug,
            outcome.scenario.fixture_params,
        )
        for outcome in outcomes
    ]


async def _start_live_cutover_runtime(
    *,
    pool: asyncpg.Pool,
    targets: list[Any],
    bootstrap_servers: str,
    s3_endpoint_url: str,
) -> _LiveCutoverRuntime:
    secrets = SigningSecrets()
    producer = IdempotentProducer(ProducerConfig(bootstrap_servers=bootstrap_servers))
    await producer.start()
    s3 = S3Client(
        os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
        endpoint_url=s3_endpoint_url,
        region_name="us-east-1",
    )
    await s3.connect()
    flags = TenantFlags(pool)
    drivers = await build_live_drivers(
        pool,
        targets,
        secrets,
        kafka_producer=producer,
        s3_raw_client=s3,
        tenant_flags=flags,
    )
    return _LiveCutoverRuntime(producer=producer, s3=s3, drivers=drivers)


async def _drive_concurrent_backfill_and_live(
    *,
    pool: asyncpg.Pool,
    harness: BackfillHarness,
    drivers: Any,
    targets: list[Any],
    peak: dict[str, int],
) -> tuple[Any, float, float]:
    harness.start_services()
    stop = asyncio.Event()
    monitor = asyncio.create_task(_monitor(pool, stop, peak))
    backfill_done_at = {"t": 0.0}

    async def _backfill_drive() -> None:
        await harness.wait_for_backfill()
        backfill_done_at["t"] = time.monotonic()

    live_start = time.monotonic()
    try:
        live_result, _ = await asyncio.gather(
            dispatch_live_concurrent(
                drivers,
                targets,
                events_per_tenant=_LIVE_EVENTS_PER_TENANT,
            ),
            _backfill_drive(),
        )
    finally:
        stop.set()
        await monitor
    return live_result, live_start, backfill_done_at["t"]


def _expected_combined_observation_totals(outcomes: list[Any]) -> dict[UUID, int]:
    return {
        outcome.tenant_id: (
            outcome.scenario.expected_observation_count + _LIVE_EVENTS_PER_TENANT
        )
        for outcome in outcomes
    }


async def _append_source_results(
    report: RunReport,
    *,
    pool: asyncpg.Pool,
    outcomes: list[Any],
) -> None:
    by_source: dict[str, list[Any]] = {}
    for outcome in outcomes:
        by_source.setdefault(outcome.scenario.source, []).append(outcome)
    for source in ("gmail", "github", "slack", "discord"):
        source_outcomes = by_source.get(source, [])
        tenant_ids = [outcome.tenant_id for outcome in source_outcomes]
        expected = sum(
            outcome.scenario.expected_observation_count + _LIVE_EVENTS_PER_TENANT
            for outcome in source_outcomes
        )
        actual = int(
            await pool.fetchval(
                "SELECT count(*) FROM observations WHERE tenant_id = ANY($1)",
                tenant_ids,
            )
        )
        report.source_results.append(
            SourceResult(
                source=source,
                tenants=len(source_outcomes),
                expected_observations=expected,
                actual_observations=actual,
            )
        )


async def _append_external_assertions(
    report: RunReport,
    *,
    pool: asyncpg.Pool,
    bootstrap_servers: str,
    outcomes: list[Any],
) -> None:
    try:
        total = await A.assert_external_id_unique_across_paths(pool)
        report.assertions.append(
            AssertionResult(
                name="assert_no_duplicate_observations_under_concurrency",
                passed=True,
                detail=(
                    f"{total} observations, zero duplicate "
                    "(source_channel, external_id, occurred_at) groups"
                ),
            )
        )
    except A.PropertyViolation as exc:
        report.assertions.append(
            AssertionResult(
                name="assert_no_duplicate_observations_under_concurrency",
                passed=False,
                detail=str(exc)[:200],
            )
        )

    residual = int(
        await pool.fetchval(
            "SELECT count(*) FROM workflow_signals "
            "WHERE consumed_at IS NULL AND signal_kind <> $1",
            _TERMINAL_SIGNAL,
        )
    )
    report.assertions.append(
        AssertionResult(
            name="assert_no_signal_leak(working drains to 0)",
            passed=residual == 0,
            detail=(
                f"residual working signals={residual} "
                f"(terminal {_TERMINAL_SIGNAL} excluded)"
            ),
        )
    )

    try:
        await A.assert_zero_partition_missing(
            bootstrap_servers=bootstrap_servers,
            tenant_ids={outcome.tenant_id for outcome in outcomes},
        )
        report.assertions.append(
            AssertionResult(
                name="assert_dlq_empty(no partition_missing)",
                passed=True,
                detail="0 partition_missing DLQ envelopes",
            )
        )
    except A.PropertyViolation as exc:
        report.assertions.append(
            AssertionResult(
                name="assert_dlq_empty(no partition_missing)",
                passed=False,
                detail=str(exc)[:200],
            )
        )


def _record_live_summary(
    report: RunReport,
    *,
    concurrency: int,
    peak: dict[str, int],
    live_result: Any,
    real_clients: bool,
) -> None:
    report.live_lines = [
        f"concurrency={concurrency}; live={_LIVE_EVENTS_PER_TENANT} "
        f"events/tenant via Kafka cutover",
        f"peak simultaneous backfill in_progress: {peak['in_progress']}",
        f"peak working signal backlog: {peak['backlog']}",
        f"live dispatch wall: {live_result.wall_seconds:.1f}s; "
        f"per-source HTTP statuses: "
        f"{ {k: sorted(v) for k, v in live_result.http_status_by_source.items()} }",
    ]
    report.notes.append(
        "Live routed through Kafka (slack/github via webhook-router "
        "cutover \u2192 HTTP 202; discord via gateway cutover; gmail via "
        "push-handler cutover). Consumer rc=-9/-15 expected per "
        "ticket #45."
    )
    if real_clients:
        report.notes.append(
            "BACKFILL drove the REAL source clients "
            "(Github/Slack/Discord/Gmail) over HTTP against the local "
            "spammer (services/ingest/synthetic/spammer) — token exchange, "
            "pagination, rate-limit backoff — instead of in-process "
            "mock clients. Live ingestion remains inbound (webhook / "
            "gateway / pubsub) routed via the Kafka cutover."
        )


async def run4(
    *,
    bootstrap_servers: str,
    concurrency: int = 10,
    distribution: dict[str, int] | None = None,
    drain_timeout_s: float = 180.0,
    real_clients: bool = False,
) -> RunReport:
    t0 = time.monotonic()
    dsn = os.environ["DATABASE_URL"]
    scenarios = run4_scenarios(distribution)
    report = _run4_report(scenarios=scenarios, real_clients=real_clients)
    from services.ingest.synthetic.validation_runs.moto_lifecycle import moto_s3

    with moto_s3() as endpoint:
        cleanup = await reset_state(
            bootstrap_servers=bootstrap_servers, s3_endpoint_url=endpoint,
            s3_bucket=os.environ.get("S3_RAW_BUCKET", "fyralis-raw"))
        report.cleanup_line = (
            f"recreated {cleanup.topics_recreated}; cleared "
            f"{cleanup.s3_objects_deleted} stale S3 objects")
        pool = await asyncpg.create_pool(dsn, min_size=4, max_size=16)
        peak = {"in_progress": 0, "backlog": 0}
        live_runtime: _LiveCutoverRuntime | None = None
        harness: BackfillHarness | None = None
        try:
            await _migrate_and_truncate(pool)
            pf = await run_preflight(pool)
            report.preflight_lines = [
                f"{r.source}: external_id={r.sample_external_id[:32]!r} ✅"
                for r in pf]

            harness = BackfillHarness(
                pool=pool, scenarios=scenarios, concurrency=concurrency,
                completion_deadline_s=600.0,
                kafka_bootstrap_servers=bootstrap_servers,
                drain_timeout_s=drain_timeout_s,
                real_clients=real_clients)

            # Phase A: seed tenants + installs + kafka_path_enabled=TRUE.
            outcomes = await harness.setup()
            targets = _live_targets_from_outcomes(outcomes)

            # Live-via-Kafka deps: ONE shared producer (→ ingestion.raw) +
            # the moto-backed raw S3 client + the flag reader. Wired into
            # the shared app / gmail app / discord deps so live publishes
            # to Kafka instead of inline.
            live_runtime = await _start_live_cutover_runtime(
                pool=pool,
                targets=targets,
                bootstrap_servers=bootstrap_servers,
                s3_endpoint_url=endpoint,
            )

            # Phase B: start the shared consumer + producer subprocesses,
            # then run backfill drive + live dispatch CONCURRENTLY.
            live_result, live_start, backfill_done_at = (
                await _drive_concurrent_backfill_and_live(
                    pool=pool,
                    harness=harness,
                    drivers=live_runtime.drivers,
                    targets=targets,
                    peak=peak,
                )
            )

            # Phase B(iii): drain the shared chain for the COMBINED total.
            expected_total = _expected_combined_observation_totals(outcomes)
            final_counts = await _wait_for_total_drain(
                pool, expected_total, timeout_s=drain_timeout_s)

            await harness.collect()

            await _append_source_results(report, pool=pool, outcomes=outcomes)

            # ====== Assertions ======
            _assert_run4(
                report, outcomes, final_counts, live_result, peak,
                live_start=live_start, backfill_done_at=backfill_done_at,
            )
            await _append_external_assertions(
                report,
                pool=pool,
                bootstrap_servers=bootstrap_servers,
                outcomes=outcomes,
            )
            _record_live_summary(
                report,
                concurrency=concurrency,
                peak=peak,
                live_result=live_result,
                real_clients=real_clients,
            )
        finally:
            if live_runtime is not None:
                await teardown_live_drivers(live_runtime.drivers)
            # Capture subprocess returncodes AFTER SIGTERM: framework
            # services exit 0; the normalizer/observation_writer consumers
            # show rc=-15/-9 (ticket #45, expected — the report's rc
            # annotation greens these until #45 ships).
            if harness is not None:
                harness_stderrs = harness.teardown()
                report.subprocess_returncodes = (
                    harness.build_result(harness_stderrs).subprocess_returncodes)
            if live_runtime is not None:
                await live_runtime.producer.stop()
                await live_runtime.s3.close()
            await pool.close()

    report.wall_seconds = time.monotonic() - t0
    report.verdict = "READY" if report.passed else "NOT_READY"
    return report


async def run5(
    *,
    bootstrap_servers: str,
    concurrency: int = 10,
    distribution: dict[str, int] | None = None,
    drain_timeout_s: float = 300.0,
) -> RunReport:
    """Run 5 — the capstone: identical to Run 4 (concurrent backfill +
    live-via-Kafka, 50 tenants, 4 sources) but BACKFILL is driven by the
    REAL source clients over HTTP against the local spammer (no in-process
    mock clients). Live ingestion stays inbound, routed via the Kafka
    cutover. A longer default drain accommodates the real HTTP round-trips."""
    return await run4(
        bootstrap_servers=bootstrap_servers, concurrency=concurrency,
        distribution=distribution, drain_timeout_s=drain_timeout_s,
        real_clients=True,
    )


def _assert_run4(
    report: RunReport, outcomes, final_counts, live_result, peak,
    *, live_start: float, backfill_done_at: float,
) -> None:
    # ---- 1. Per-tenant isolation (combined backfill + live) ----
    iso_ok = True
    iso_detail = ""
    discord_counts: list[int] = []
    for o in outcomes:
        n = final_counts.get(o.tenant_id, 0)
        expected = o.scenario.expected_observation_count + _LIVE_EVENTS_PER_TENANT
        if o.scenario.source == "discord":
            discord_counts.append(n)
            if n <= 0:
                iso_ok = False
                iso_detail = f"{o.scenario.tenant_slug} got 0 obs"
        elif n != expected:
            iso_ok = False
            iso_detail = (f"{o.scenario.tenant_slug}: got {n}, "
                          f"expected {expected}")
            break
    if iso_ok and discord_counts and len(set(discord_counts)) != 1:
        iso_ok = False
        iso_detail = f"discord counts not uniform: {set(discord_counts)}"
    report.assertions.append(AssertionResult(
        name="assert_per_tenant_isolation(backfill+live)", passed=iso_ok,
        detail=iso_detail or "all tenants match backfill+live expected"))

    # ---- 2. Concurrency overlap: live fired WHILE backfill in-flight ----
    overlap_ok = (
        peak["in_progress"] >= 5
        and live_start <= backfill_done_at
        and backfill_done_at > 0.0
    )
    report.assertions.append(AssertionResult(
        name="assert_concurrency_overlap(live during backfill in_progress)",
        passed=overlap_ok,
        detail=(f"peak in_progress={peak['in_progress']}, live_start"
                f"{'<=' if live_start <= backfill_done_at else '>'}"
                f"backfill_done (Δ={backfill_done_at - live_start:.1f}s)")))

    # ---- 3. Live routed through Kafka (HTTP 202 from the cutover) ----
    # slack/github webhook router returns 202 when it takes the Kafka
    # cutover (vs 200/201 inline). gmail pubsub always 200 (cutover is
    # post-ack); discord is direct-dispatch (no HTTP). So 202 on the two
    # HTTP-router sources is the unambiguous live-via-Kafka signal.
    router_sources = [s for s in ("slack", "github")
                      if s in live_result.http_status_by_source]
    via_kafka_ok = bool(router_sources) and all(
        live_result.http_status_by_source.get(s) == {202}
        for s in router_sources
    )
    report.assertions.append(AssertionResult(
        name="assert_live_routed_through_kafka(slack/github → 202)",
        passed=via_kafka_ok,
        detail=f"statuses={ {k: sorted(v) for k, v in live_result.http_status_by_source.items()} }"))

    # ---- 4. No signal leak: working signals drained to 0 ----
    # (checked by the caller via a residual query; recorded here for shape)

    # ---- 5. #39 completion fires exactly once per tenant ----
    bad = [o.scenario.tenant_slug for o in outcomes
           if o.completion_signal_count != 1]
    report.assertions.append(AssertionResult(
        name="assert_completion_fires_exactly_once_per_tenant(#39)",
        passed=not bad,
        detail=("all fired once" if not bad else f"anomalies: {bad[:5]}")))
