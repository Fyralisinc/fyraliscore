"""Run 4 — contract-wide concurrent backfill + live-via-Kafka.

Two tenants for every history-capable canonical source run backfill and live
ingress concurrently.  The contract's live-only source (WhatsApp) gets two
live targets but no fabricated planner/fetcher outcome.  This currently means
52 historical tenants across 26 sources plus two WhatsApp tenants: all 27
canonical sources and 54 tenants in one shared consumer run.

Live webhooks/events publish to ``ingestion.raw`` where the declared Kafka
cutover applies and are drained by the same normalizer → observation-writer
consumer chain as backfill.

This closes the M-Validate fidelity gaps:
  - #1 live bypasses Kafka      → live now takes the cutover path
    wherever the provider-ingress contract enables it.
  - #2 backfill/live sequential → `asyncio.gather(backfill_drive,
    live_dispatch)`; live fires WHILE backfill is in-flight.
  - #4 partial live matrix      → live covers every canonical source.
  - A30.6 fixed 30s drain       → the harness drain window is now
    configurable; Run 4 raises it for the combined load.

Synthetic provider data throughout — history uses unmodified production
clients against Provider Lab, while live events remain generated locally.
No real provider API is called.

Structure mirrors Run 3 (moto S3, Kafka reset, preflight, the shared
7-subprocess chain) but drives the decomposed harness phases directly so
the live phase can interleave:

    outcomes = await harness.setup()        # 26 history-capable sources
    targets += seed_contract_live_only_targets(...)  # WhatsApp only
    drivers = prepare_live_drivers(..., kafka_producer=..., s3=..., flags=...)
    harness.start_services()                # 7 subprocs incl. consumers
    await gather(harness.wait_for_backfill(), dispatch_live_concurrent(...))
    await _wait_for_total_drain(...)         # backfill + all live, one drain
    await harness.collect(); harness.teardown()
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import pathlib
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from services.ingest.ingestion.feature_flags.client import TenantFlags
from services.ingest.ingestion.kafka.producer import (
    IdempotentProducer,
    ProducerConfig,
)
from services.ingest.ingestion.raw_tier.s3 import S3Client
from services.ingest.source_contract.catalog import (
    DEDICATED_INGRESS_CATALOG,
    SOURCE_DEFINITIONS,
    WEBHOOK_INGRESS_CATALOG,
)
from services.ingest.synthetic.backfill_harness.harness import (
    BackfillHarness,
    TenantOutcome,
)
from services.ingest.synthetic.backfill_harness.scenarios import BackfillScenario
from services.ingest.synthetic.validation_runs import assertions as A
from services.ingest.synthetic.validation_runs.cleanup import reset_state
from services.ingest.synthetic.validation_runs.composition import (
    LiveDispatchResult,
    LiveTarget,
    SigningSecrets,
    dispatch_live_concurrent,
    live_target_for,
    prepare_live_drivers,
    seed_contract_live_only_targets,
    teardown_live_drivers,
)
from services.ingest.synthetic.validation_runs.preflight import run_preflight
from services.ingest.synthetic.validation_runs.reports import (
    AssertionResult,
    RunReport,
    SourceResult,
)
from services.ingest.synthetic.validation_runs.runs import (
    certification_history_scenarios,
)


log = logging.getLogger("validation_runs.run4")
_MIGRATIONS = pathlib.Path("db/migrations")

# Preserve the original roughly-50-tenant stress shape without owning another
# source list: two tenants for each contract-declared historical source.
_TENANTS_PER_SOURCE = 2

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


def _contract_source_membership() -> tuple[tuple[str, ...], tuple[str, ...]]:
    historical = tuple(
        definition.source_id
        for definition in SOURCE_DEFINITIONS
        if definition.history is not None
    )
    live_only = tuple(
        definition.source_id
        for definition in SOURCE_DEFINITIONS
        if definition.history is None
    )
    return historical, live_only


def run4_scenarios() -> list[BackfillScenario]:
    """Build two exact, source-owned scenarios for every history source."""

    scenarios = certification_history_scenarios(
        tenants_per_source=_TENANTS_PER_SOURCE,
    )
    historical_source_ids, _ = _contract_source_membership()
    expected_membership = Counter(
        {source_id: _TENANTS_PER_SOURCE for source_id in historical_source_ids},
    )
    actual_membership = Counter(scenario.source for scenario in scenarios)
    if actual_membership != expected_membership:
        raise RuntimeError(
            "Run 4 historical scenarios drifted from the source contract: "
            f"expected {dict(expected_membership)!r}, "
            f"got {dict(actual_membership)!r}",
        )
    for scenario in scenarios:
        expected = scenario.expected_observation_count
        if isinstance(expected, bool) or not isinstance(expected, int) or expected <= 0:
            raise ValueError(
                f"{scenario.source} Run 4 expected_observation_count must be "
                f"a positive exact integer, got {expected!r}",
            )
    return scenarios


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
        names = ", ".join(
            f'"{r["relname"]}"'
            for r in rows
            if r["relname"] != "ingestion_source_catalog"
        )
        if names:
            await conn.execute(f"TRUNCATE {names} RESTART IDENTITY CASCADE")


async def _monitor(
    pool: asyncpg.Pool,
    stop: asyncio.Event,
    peak: dict[str, int],
    *,
    interval_s: float = 1.0,
) -> None:
    """Sample peak concurrent backfill `in_progress` + working-signal
    backlog while the combined phase runs (same shape as Run 3)."""
    while not stop.is_set():
        try:
            ip = int(
                await pool.fetchval(
                    "SELECT count(*) FROM source_onboarding_runs "
                    "WHERE status = 'in_progress'"
                )
                or 0
            )
            backlog = int(
                await pool.fetchval(
                    "SELECT count(*) FROM workflow_signals "
                    "WHERE consumed_at IS NULL AND signal_kind <> $1",
                    _TERMINAL_SIGNAL,
                )
                or 0
            )
            peak["in_progress"] = max(peak["in_progress"], ip)
            peak["backlog"] = max(peak["backlog"], backlog)
        except Exception:  # noqa: BLE001 — monitor is best-effort
            pass
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass


async def _wait_for_total_drain(
    pool: asyncpg.Pool,
    expected_total: dict[UUID, int],
    *,
    timeout_s: float,
    poll_interval_s: float = 2.0,
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


def _run4_report(
    *,
    scenarios: Sequence[BackfillScenario],
    live_only_tenant_count: int,
) -> RunReport:
    source_count = len(SOURCE_DEFINITIONS)
    tenant_count = len(scenarios) + live_only_tenant_count
    return RunReport(
        run_name=(
            "Concurrent backfill (production clients \u2192 Provider Lab) + "
            f"live-via-Kafka ({tenant_count} tenants, "
            f"{source_count} canonical sources)"
        ),
        run_number=4,
        tenant_count=tenant_count,
        started_at=dt.datetime.now(tz=dt.timezone.utc),
        wall_seconds=0.0,
    )


def _live_targets_from_outcomes(
    outcomes: Sequence[TenantOutcome],
) -> list[LiveTarget]:
    targets: list[LiveTarget] = []
    for outcome in outcomes:
        fixture = outcome.fixture
        if not isinstance(fixture, Mapping):
            raise RuntimeError(
                "Run 4 cannot derive a historical live target without the "
                "resolved certification fixture for "
                f"{outcome.scenario.source}/{outcome.scenario.tenant_slug}",
            )
        targets.append(
            live_target_for(
                outcome.tenant_id,
                outcome.scenario.source,
                outcome.scenario.tenant_slug,
                dict(fixture),
            )
        )
    return targets


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
    drivers = await prepare_live_drivers(
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


def _expected_combined_observation_totals(
    outcomes: Sequence[TenantOutcome],
    targets: Sequence[LiveTarget],
    live_result: LiveDispatchResult,
) -> dict[UUID, int]:
    """Combine exact historical oracles with exact dispatched live counts.

    Live-only targets contribute only their live dispatch count.  Historical
    outcomes contribute their source-owned backfill count plus live.  The
    dispatch maps must cover every target exactly; otherwise Run 4 fails
    before a drain can accidentally hide a missing source.
    """

    outcomes_by_tenant: dict[UUID, TenantOutcome] = {}
    for outcome in outcomes:
        if outcome.tenant_id in outcomes_by_tenant:
            raise RuntimeError(
                f"Run 4 received duplicate outcome tenant {outcome.tenant_id}",
            )
        outcomes_by_tenant[outcome.tenant_id] = outcome

    targets_by_tenant: dict[UUID, LiveTarget] = {}
    for target in targets:
        if target.tenant_id in targets_by_tenant:
            raise RuntimeError(
                f"Run 4 received duplicate live target tenant {target.tenant_id}",
            )
        targets_by_tenant[target.tenant_id] = target

    missing_historical_targets = set(outcomes_by_tenant) - set(targets_by_tenant)
    if missing_historical_targets:
        raise RuntimeError(
            "Run 4 has historical outcomes without live targets: "
            f"{sorted(map(str, missing_historical_targets))[:5]!r}",
        )

    expected_dispatch_by_source = Counter(target.source for target in targets)
    expected_dispatch_by_source = Counter(
        {
            source: tenant_count * _LIVE_EVENTS_PER_TENANT
            for source, tenant_count in expected_dispatch_by_source.items()
        }
    )
    if Counter(live_result.dispatched_by_source) != expected_dispatch_by_source:
        raise RuntimeError(
            "Run 4 per-source live dispatch accounting drifted: "
            f"expected {dict(expected_dispatch_by_source)!r}, "
            f"got {live_result.dispatched_by_source!r}",
        )

    expected_tenant_ids = set(targets_by_tenant)
    dispatched_tenant_ids = set(live_result.dispatched_by_tenant)
    if dispatched_tenant_ids != expected_tenant_ids:
        raise RuntimeError(
            "Run 4 live dispatch did not cover the target tenant set: "
            f"missing={sorted(map(str, expected_tenant_ids - dispatched_tenant_ids))[:5]!r}, "
            f"unexpected={sorted(map(str, dispatched_tenant_ids - expected_tenant_ids))[:5]!r}",
        )

    totals: dict[UUID, int] = {}
    for tenant_id, target in targets_by_tenant.items():
        dispatched = live_result.dispatched_by_tenant[tenant_id]
        if dispatched != _LIVE_EVENTS_PER_TENANT:
            raise RuntimeError(
                f"Run 4 live dispatch count for {target.source}/{target.slug} "
                f"must be exactly {_LIVE_EVENTS_PER_TENANT}, got {dispatched!r}",
            )
        backfill_expected = 0
        outcome = outcomes_by_tenant.get(tenant_id)
        if outcome is not None:
            if outcome.scenario.source != target.source:
                raise RuntimeError(
                    "Run 4 target source does not match its historical "
                    f"outcome for tenant {tenant_id}",
                )
            backfill_expected = outcome.scenario.expected_observation_count
            if (
                isinstance(backfill_expected, bool)
                or not isinstance(backfill_expected, int)
                or backfill_expected <= 0
            ):
                raise RuntimeError(
                    f"Run 4 cannot account for {target.source}/{target.slug} "
                    f"with historical count {backfill_expected!r}",
                )
        totals[tenant_id] = backfill_expected + dispatched
    return totals


async def _append_source_results(
    report: RunReport,
    *,
    pool: asyncpg.Pool,
    targets: Sequence[LiveTarget],
    expected_total: Mapping[UUID, int],
) -> None:
    targets_by_source: dict[str, list[LiveTarget]] = {
        definition.source_id: [] for definition in SOURCE_DEFINITIONS
    }
    for target in targets:
        try:
            targets_by_source[target.source].append(target)
        except KeyError as exc:
            raise RuntimeError(
                "Run 4 received a live target outside the source contract: "
                f"{target.source!r}",
            ) from exc

    for definition in SOURCE_DEFINITIONS:
        source = definition.source_id
        source_targets = targets_by_source[source]
        tenant_ids = [target.tenant_id for target in source_targets]
        expected = sum(expected_total[tenant_id] for tenant_id in tenant_ids)
        actual = 0
        if tenant_ids:
            actual = int(
                await pool.fetchval(
                    "SELECT count(*) FROM observations "
                    "WHERE tenant_id = ANY($1::uuid[])",
                    tenant_ids,
                )
            )
        report.source_results.append(
            SourceResult(
                source=source,
                tenants=len(source_targets),
                expected_observations=expected,
                actual_observations=actual,
            )
        )


async def _append_external_assertions(
    report: RunReport,
    *,
    pool: asyncpg.Pool,
    bootstrap_servers: str,
    tenant_ids: set[UUID],
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

    try:
        triggered = await A.assert_observations_have_exactly_one_t1_trigger(
            pool,
            tenant_ids,
        )
        report.assertions.append(
            AssertionResult(
                name="assert_observation_persistence_and_t1_trigger",
                passed=True,
                detail=(
                    f"{triggered} observations each own exactly one "
                    "same-tenant T1/event_arrival trigger"
                ),
            )
        )
    except A.PropertyViolation as exc:
        report.assertions.append(
            AssertionResult(
                name="assert_observation_persistence_and_t1_trigger",
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
            tenant_ids=tenant_ids,
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


def _contract_http_ack_status(source: str) -> set[int] | None:
    """Derive the expected success ACK from provider-ingress contracts."""

    kafka_modes = [
        ingress.kafka_mode
        for ingress in WEBHOOK_INGRESS_CATALOG.values()
        if ingress.source_id == source
    ]
    kafka_modes.extend(
        ingress.kafka_mode
        for ingress in DEDICATED_INGRESS_CATALOG.values()
        if ingress.source_id == source
    )
    if not kafka_modes:
        return None
    if len(kafka_modes) != 1:
        raise RuntimeError(
            f"Run 4 cannot choose between multiple HTTP ingress contracts "
            f"for {source!r}: {kafka_modes!r}",
        )
    if kafka_modes[0] == "flagged_kafka_first_with_inline_fallback":
        return {202}
    return {200}


def _required_http_status_sources(
    target_sources: set[str],
) -> set[str]:
    """Sources whose preferred contract transport produces an HTTP ACK.

    Sources such as Discord may also declare a webhook for synchronous
    interactions while Run 4 deliberately exercises their preferred gateway
    transport.  Requiring a status only when the first declared live transport
    is HTTP keeps the assertion contract-derived instead of maintaining an
    exception list.  Any additional reported HTTP source is still validated.
    """

    return {
        definition.source_id
        for definition in SOURCE_DEFINITIONS
        if definition.source_id in target_sources
        and definition.live_transports
        and definition.live_transports[0] in {"webhook", "pubsub"}
    }


def _append_live_dispatch_assertions(
    report: RunReport,
    *,
    targets: Sequence[LiveTarget],
    live_result: LiveDispatchResult,
) -> None:
    canonical_source_ids = tuple(
        definition.source_id for definition in SOURCE_DEFINITIONS
    )
    target_source_ids = tuple(dict.fromkeys(target.source for target in targets))
    dispatched_source_ids = set(live_result.dispatched_by_source)
    canonical_source_set = set(canonical_source_ids)
    report.assertions.append(
        AssertionResult(
            name="assert_all_contract_sources_dispatched_live",
            passed=(
                set(target_source_ids) == canonical_source_set
                and dispatched_source_ids == canonical_source_set
            ),
            detail=(
                f"targets={len(target_source_ids)}/{len(canonical_source_ids)}, "
                f"dispatched={len(dispatched_source_ids)}/"
                f"{len(canonical_source_ids)}"
            ),
        )
    )

    actual_statuses = live_result.http_status_by_source
    required_status_sources = _required_http_status_sources(
        set(target_source_ids),
    )
    missing_status_sources = required_status_sources - set(actual_statuses)
    mismatches: dict[str, dict[str, list[int]]] = {}
    for source, statuses in actual_statuses.items():
        expected = _contract_http_ack_status(source)
        if expected is None or statuses != expected:
            mismatches[source] = {
                "expected": sorted(expected or set()),
                "actual": sorted(statuses),
            }
    report.assertions.append(
        AssertionResult(
            name="assert_http_ack_statuses_follow_ingress_contract",
            passed=not missing_status_sources and not mismatches,
            detail=(
                f"missing={sorted(missing_status_sources)!r}; "
                f"mismatches={mismatches!r}; "
                "Kafka-cutover routes require HTTP 202"
            ),
        )
    )


def _record_live_summary(
    report: RunReport,
    *,
    concurrency: int,
    peak: dict[str, int],
    live_result: LiveDispatchResult,
    historical_source_ids: Sequence[str],
    live_only_source_ids: Sequence[str],
) -> None:
    report.live_lines = [
        f"concurrency={concurrency}; live={_LIVE_EVENTS_PER_TENANT} "
        f"events/tenant via Kafka cutover",
        f"peak simultaneous backfill in_progress: {peak['in_progress']}",
        f"peak working signal backlog: {peak['backlog']}",
        f"per-source dispatched live events: " f"{live_result.dispatched_by_source}",
        f"live dispatch wall: {live_result.wall_seconds:.1f}s; "
        f"per-source HTTP statuses: "
        f"{ {k: sorted(v) for k, v in live_result.http_status_by_source.items()} }",
    ]
    report.notes.append(
        "Live dispatch covered the complete contract catalog. Routes whose "
        "provider-ingress contract declares the flagged Kafka cutover must "
        "acknowledge with HTTP 202; provider-managed push acknowledgements "
        "and direct gateway/poll transports retain their declared boundary. "
        "Consumer rc=-9/-15 remains accepted per ticket #45."
    )
    report.notes.append(
        "Backfill drove production clients for "
        f"{len(historical_source_ids)} history-capable contract sources "
        "against Provider Lab. Live-only sources "
        f"{list(live_only_source_ids)!r} contributed live observations "
        "without fabricated onboarding-completion or historical assertions."
    )


async def run4(
    *,
    bootstrap_servers: str,
    concurrency: int = 10,
    drain_timeout_s: float = 180.0,
) -> RunReport:
    t0 = time.monotonic()
    dsn = os.environ["DATABASE_URL"]
    scenarios = run4_scenarios()
    historical_source_ids, live_only_source_ids = _contract_source_membership()
    planned_live_only_tenants = len(live_only_source_ids) * _TENANTS_PER_SOURCE
    report = _run4_report(
        scenarios=scenarios,
        live_only_tenant_count=planned_live_only_tenants,
    )
    from services.ingest.synthetic.validation_runs.moto_lifecycle import moto_s3

    with moto_s3() as endpoint:
        cleanup = await reset_state(
            bootstrap_servers=bootstrap_servers,
            s3_endpoint_url=endpoint,
            s3_bucket=os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
        )
        report.cleanup_line = (
            f"recreated {cleanup.topics_recreated}; cleared "
            f"{cleanup.s3_objects_deleted} stale S3 objects"
        )
        pool = await asyncpg.create_pool(dsn, min_size=4, max_size=16)
        peak = {"in_progress": 0, "backlog": 0}
        live_runtime: _LiveCutoverRuntime | None = None
        harness: BackfillHarness | None = None
        try:
            await _migrate_and_truncate(pool)
            pf = await run_preflight(pool)
            report.preflight_lines = [
                f"{r.source}: external_id={r.sample_external_id[:32]!r} ✅" for r in pf
            ]

            harness = BackfillHarness(
                pool=pool,
                scenarios=scenarios,
                concurrency=concurrency,
                completion_deadline_s=600.0,
                kafka_bootstrap_servers=bootstrap_servers,
                drain_timeout_s=drain_timeout_s,
            )

            # Phase A: seed tenants + installs + kafka_path_enabled=TRUE.
            outcomes = await harness.setup()
            historical_targets = _live_targets_from_outcomes(outcomes)
            live_only_targets = await seed_contract_live_only_targets(
                pool,
                tenants_per_source=_TENANTS_PER_SOURCE,
            )
            targets = [*historical_targets, *live_only_targets]
            if len(live_only_targets) != planned_live_only_tenants:
                raise RuntimeError(
                    "Run 4 live-only target count drifted from the contract: "
                    f"expected {planned_live_only_tenants}, "
                    f"got {len(live_only_targets)}",
                )
            report.tenant_count = len(targets)

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
            (
                live_result,
                live_start,
                backfill_done_at,
            ) = await _drive_concurrent_backfill_and_live(
                pool=pool,
                harness=harness,
                drivers=live_runtime.drivers,
                targets=targets,
                peak=peak,
            )

            # Phase B(iii): drain the shared chain for the COMBINED total.
            expected_total = _expected_combined_observation_totals(
                outcomes,
                targets,
                live_result,
            )
            final_counts = await _wait_for_total_drain(
                pool, expected_total, timeout_s=drain_timeout_s
            )

            await harness.collect()

            await _append_source_results(
                report,
                pool=pool,
                targets=targets,
                expected_total=expected_total,
            )

            # ====== Assertions ======
            _assert_run4(
                report,
                scenarios,
                outcomes,
                targets,
                expected_total,
                final_counts,
                peak,
                live_start=live_start,
                backfill_done_at=backfill_done_at,
            )
            _append_live_dispatch_assertions(
                report,
                targets=targets,
                live_result=live_result,
            )
            await _append_external_assertions(
                report,
                pool=pool,
                bootstrap_servers=bootstrap_servers,
                tenant_ids=set(expected_total),
            )
            _record_live_summary(
                report,
                concurrency=concurrency,
                peak=peak,
                live_result=live_result,
                historical_source_ids=historical_source_ids,
                live_only_source_ids=live_only_source_ids,
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
                report.subprocess_returncodes = harness.build_result(
                    harness_stderrs
                ).subprocess_returncodes
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
    drain_timeout_s: float = 300.0,
) -> RunReport:
    """Run 5 — the longer capstone form of the Provider Lab-backed Run 4."""
    report = await run4(
        bootstrap_servers=bootstrap_servers,
        concurrency=concurrency,
        drain_timeout_s=drain_timeout_s,
    )
    report.run_number = 5
    report.run_name = report.run_name.replace(
        "Concurrent",
        "Capstone concurrent",
        1,
    )
    return report


def _assert_run4(
    report: RunReport,
    scenarios: Sequence[BackfillScenario],
    outcomes: Sequence[TenantOutcome],
    targets: Sequence[LiveTarget],
    expected_total: Mapping[UUID, int],
    final_counts: Mapping[UUID, int],
    peak: Mapping[str, int],
    *,
    live_start: float,
    backfill_done_at: float,
) -> None:
    # ---- 1. Planned historical scenarios produced exactly one outcome ----
    planned_keys = [(scenario.source, scenario.tenant_slug) for scenario in scenarios]
    outcome_keys = [
        (outcome.scenario.source, outcome.scenario.tenant_slug) for outcome in outcomes
    ]
    missing_outcomes = sorted(set(planned_keys) - set(outcome_keys))
    unexpected_outcomes = sorted(set(outcome_keys) - set(planned_keys))
    report.assertions.append(
        AssertionResult(
            name="assert_contract_scenario_outcome_coverage",
            passed=(
                len(outcome_keys) == len(planned_keys)
                and len(set(outcome_keys)) == len(outcome_keys)
                and not missing_outcomes
                and not unexpected_outcomes
            ),
            detail=(
                f"planned={len(planned_keys)}, outcomes={len(outcome_keys)}, "
                f"missing={missing_outcomes[:3]}, "
                f"unexpected={unexpected_outcomes[:3]}"
            ),
        )
    )

    # ---- 2. Per-tenant isolation (historical + live-only combined) ----
    targets_by_tenant = {target.tenant_id: target for target in targets}
    mismatches = []
    for tenant_id, expected in expected_total.items():
        actual = final_counts.get(tenant_id, 0)
        if actual != expected:
            target = targets_by_tenant[tenant_id]
            mismatches.append(
                f"{target.source}/{target.slug}: got {actual}, " f"expected {expected}",
            )
    report.assertions.append(
        AssertionResult(
            name="assert_per_tenant_isolation(backfill+live)",
            passed=not mismatches,
            detail=(
                "all historical and live-only tenants match exact totals"
                if not mismatches
                else f"mismatches={mismatches[:5]!r}"
            ),
        )
    )

    # ---- 3. Concurrency overlap: live fired WHILE backfill in-flight ----
    overlap_ok = (
        peak["in_progress"] >= 5
        and live_start <= backfill_done_at
        and backfill_done_at > 0.0
    )
    report.assertions.append(
        AssertionResult(
            name="assert_concurrency_overlap(live during backfill in_progress)",
            passed=overlap_ok,
            detail=(
                f"peak in_progress={peak['in_progress']}, live_start"
                f"{'<=' if live_start <= backfill_done_at else '>'}"
                f"backfill_done (Δ={backfill_done_at - live_start:.1f}s)"
            ),
        )
    )

    # ---- 4. No signal leak: working signals drained to 0 ----
    # (checked by the caller via a residual query; recorded here for shape)

    # ---- 5. #39 completion fires once for historical outcomes only ----
    bad = [
        outcome.scenario.tenant_slug
        for outcome in outcomes
        if outcome.completion_signal_count != 1
    ]
    report.assertions.append(
        AssertionResult(
            name=("assert_completion_fires_exactly_once_per_historical_tenant" "(#39)"),
            passed=not bad,
            detail=(
                "all historical tenants fired once; live-only targets "
                "correctly excluded"
                if not bad
                else f"historical anomalies: {bad[:5]}"
            ),
        )
    )
