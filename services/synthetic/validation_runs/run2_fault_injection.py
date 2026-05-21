"""Run 2 — fault injection (A30.3).

Same 16-tenant shape as Run 1, but every backfill mock runs the `FLAKY`
fault profile (10% random 5xx) — propagated to the X3 backfill
subprocesses through the fixture registry's `fault_profile` field — plus
a deliberate **partition-missing injection**: one out-of-range
`occurred_at` event per source driven through the real
`observation_writer` to verify A28's permanent-error DLQ routing fires
under composition (NOT a crash-loop).

Validates the framework resilience contract:
  - A19 broad-exception handling — no orchestrator (non-consumer)
    subprocess crashes despite ~10% injected 5xx.
  - A28 permanent-error routing — out-of-range rows land on
    `ingestion.dlq` as `partition_missing`.

Under FLAKY, per-tenant backfill counts MAY fall short (dropped/again-
retried fetches); that is expected and yields a PARTIAL verdict, not
NOT_READY. NOT_READY is reserved for an orchestrator crash or a missed
A28 routing.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import logging
import os
import pathlib
import time

import asyncpg

from services.synthetic.backfill_harness.harness import BackfillHarness
from services.synthetic.fault_profiles import FLAKY
from services.synthetic.validation_runs import assertions as A
from services.synthetic.validation_runs import composition as C
from services.synthetic.validation_runs.cleanup import reset_state
from services.synthetic.validation_runs.composition import (
    SigningSecrets,
    build_live_drivers,
    capture_twin_identities,
    live_target_for,
    partition_missing_probe,
    run_live_phase,
    teardown_live_drivers,
    wait_for_live_consumer_drain,
)
from services.synthetic.validation_runs.moto_lifecycle import moto_s3
from services.synthetic.validation_runs.preflight import run_preflight
from services.synthetic.validation_runs.reports import (
    AssertionResult,
    RunReport,
    SourceResult,
)
from services.synthetic.validation_runs.runs import run1_scenarios


log = logging.getLogger("validation_runs.run2")
_MIGRATIONS = pathlib.Path("db/migrations")


def run2_scenarios(tenants_per_source: int = 4):
    """Run 1's scenarios with the FLAKY fault profile applied to every
    tenant (the profile is serialized into the fixture registry the X3
    subprocesses read)."""
    return [
        dataclasses.replace(s, fault_profile=FLAKY)
        for s in run1_scenarios(tenants_per_source)
    ]


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


async def _run_assertion(results, name, coro) -> bool:
    try:
        await coro
        results.append(AssertionResult(name=name, passed=True))
        return True
    except A.PropertyViolation as exc:
        results.append(
            AssertionResult(name=name, passed=False, detail=str(exc)[:300]))
        return False


async def run2(
    *, bootstrap_servers: str, tenants_per_source: int = 4,
    events_per_tenant: int = 5,
) -> RunReport:
    started = dt.datetime.now(tz=dt.timezone.utc)
    t0 = time.monotonic()
    dsn = os.environ["DATABASE_URL"]
    report = RunReport(
        run_name="Fault injection (FLAKY + partition-missing)",
        run_number=2, tenant_count=tenants_per_source * 4,
        started_at=started, wall_seconds=0.0,
    )

    with moto_s3() as endpoint:
        cleanup = await reset_state(
            bootstrap_servers=bootstrap_servers, s3_endpoint_url=endpoint,
            s3_bucket=os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
        )
        report.cleanup_line = (
            f"recreated {cleanup.topics_recreated}; cleared "
            f"{cleanup.s3_objects_deleted} stale S3 objects")
        pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
        a28_ok = False
        counts_short = False
        try:
            await _migrate_and_truncate(pool)
            pf = await run_preflight(pool)
            report.preflight_lines = [
                f"{r.source}: {r.records_checked} records, "
                f"external_id={r.sample_external_id[:32]!r} ✅" for r in pf]

            # ---- Backfill (FLAKY) ----
            harness = BackfillHarness(
                pool=pool, scenarios=run2_scenarios(tenants_per_source),
                concurrency=8, completion_deadline_s=180.0,
                kafka_bootstrap_servers=bootstrap_servers,
            )
            result = await harness.run()
            report.subprocess_returncodes = dict(result.subprocess_returncodes)

            # ---- Live phase ----
            targets = [
                live_target_for(o.tenant_id, o.scenario.source,
                                o.scenario.tenant_slug, o.scenario.fixture_params)
                for o in result.outcomes
            ]
            twins = await capture_twin_identities(pool, targets)
            drivers = await build_live_drivers(pool, targets, SigningSecrets())
            try:
                live = await run_live_phase(
                    pool, drivers, targets, twins,
                    events_per_tenant=events_per_tenant)
                await wait_for_live_consumer_drain(
                    pool, {t.tenant_id for t in targets})
            finally:
                await teardown_live_drivers(drivers)

            # ---- A28 partition-missing injection (one per source) ----
            expected_pm = await partition_missing_probe(
                pool, targets, bootstrap_servers=bootstrap_servers)
            report.live_lines = [
                f"FLAKY (10% 5xx) applied to all backfill mocks",
                f"partition-missing injections (one/source): {expected_pm}",
                f"live per-source deltas: {live.per_source_counts}",
            ]

            # ---- Per-source counts (informational under FLAKY) ----
            by_source: dict[str, list] = {}
            for o in result.outcomes:
                by_source.setdefault(o.scenario.source, []).append(o)
            counts_short = False
            for source in ("gmail", "github", "slack", "discord"):
                outs = by_source.get(source, [])
                src_tids = [o.tenant_id for o in outs]
                bf_expected = sum(
                    o.scenario.expected_observation_count for o in outs)
                live_expected = events_per_tenant * len(outs)
                actual = int(await pool.fetchval(
                    "SELECT count(*) FROM observations WHERE tenant_id = ANY($1)",
                    src_tids))
                # +1/source for partition-missing tenants are NOT written
                # (they DLQ), so expected excludes them.
                exp = bf_expected + live_expected
                if actual < exp:
                    counts_short = True
                report.source_results.append(SourceResult(
                    source=source, tenants=len(outs),
                    expected_observations=exp, actual_observations=actual))

            # ---- Assertions ----
            a28_ok = await _run_assertion(
                report.assertions, "assert_partition_missing_routes_to_dlq",
                A.assert_partition_missing_routes_to_dlq(
                    bootstrap_servers=bootstrap_servers,
                    expected_count=expected_pm,
                    tenant_ids={t.tenant_id for t in targets}),
            )
            await _run_assertion(
                report.assertions, "assert_cross_path_twins_dedup",
                A.assert_cross_path_twins_dedup(pool, live.twin_external_ids))
            await _run_assertion(
                report.assertions,
                "assert_signature_validation_gate_holds_for_hmac_sources",
                A.assert_signature_validation_gate_holds_for_hmac_sources(
                    live.tamper_results))
            await _run_assertion(
                report.assertions, "assert_no_duplicate_observations",
                _noraise(A.assert_no_duplicate_observations, result))

            report.notes.append(
                "FLAKY fault profile; partial backfill counts are expected "
                "(verdict PARTIAL). A19: orchestrator subprocesses must not "
                "crash; A28: partition-missing must route to DLQ. "
                "Consumer rc=-9/-15 expected per ticket #45.")
        finally:
            await pool.close()

    report.wall_seconds = time.monotonic() - t0

    # ---- Verdict (A19 + A28 gate; FLAKY count-shortfall → PARTIAL) ----
    orchestrator_crash = bool(report.rc_violations())
    if orchestrator_crash or not a28_ok:
        report.verdict = "NOT_READY"
    elif counts_short or not report.passed:
        report.verdict = "PARTIAL"
    else:
        report.verdict = "READY"
    return report


async def _noraise(fn, *args):
    fn(*args)
