"""Run 2 — contract-wide fault injection (A30.3).

Every history-capable source runs the `FLAKY` fault profile (a deterministic
one-in-ten Provider Lab 503 rule), every canonical source runs its live edge,
and the suite adds
a deliberate **partition-missing injection**: one out-of-range
`occurred_at` event per source driven through the real
`observation_writer` to verify A28's permanent-error DLQ routing fires
under composition (NOT a crash-loop).

Validates the framework resilience contract:
  - A19 broad-exception handling — no orchestrator (non-consumer)
    subprocess crashes despite ~10% injected 5xx.
  - A28 permanent-error routing — out-of-range rows land on
    `ingestion.dlq` as `partition_missing`.

The injected failures are transient. Recovery is part of the certification
contract, so missing data, incomplete onboarding, a failed assertion, or a
worker crash makes the run ``NOT_READY``; there is no partial-success waiver.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import logging
import os
import pathlib
import time
from collections.abc import Mapping

import asyncpg

from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.synthetic.backfill_harness.harness import BackfillHarness
from services.ingest.synthetic.fault_profiles import FLAKY
from services.ingest.synthetic.validation_runs import assertions as A
from services.ingest.synthetic.validation_runs.cleanup import reset_state
from services.ingest.synthetic.validation_runs.composition import (
    HMAC_SOURCES,
    SigningSecrets,
    capture_twin_identities,
    live_target_for,
    partition_missing_probe,
    prepare_live_drivers,
    run_live_phase,
    seed_contract_live_only_targets,
    teardown_live_drivers,
    wait_for_live_consumer_drain,
)
from services.ingest.synthetic.validation_runs.moto_lifecycle import moto_s3
from services.ingest.synthetic.validation_runs.preflight import run_preflight
from services.ingest.synthetic.validation_runs.reports import (
    AssertionResult,
    RunReport,
    SourceResult,
)
from services.ingest.synthetic.validation_runs.runs import (
    certification_history_scenarios,
)


log = logging.getLogger("validation_runs.run2")
_MIGRATIONS = pathlib.Path("db/migrations")


def run2_scenarios(tenants_per_source: int = 4):
    """Apply FLAKY to every contract-declared historical source."""

    scenarios = [
        dataclasses.replace(s, fault_profile=FLAKY)
        for s in certification_history_scenarios(tenants_per_source)
    ]
    unresolved = [
        f"{scenario.source}/{scenario.tenant_slug}"
        for scenario in scenarios
        if scenario.expected_observation_count <= 0
    ]
    if unresolved:
        raise RuntimeError(
            "Run 2 requires exact positive source-owned fixture counts; "
            f"unresolved={unresolved[:10]!r}",
        )
    return scenarios


def _historical_live_targets(outcomes) -> list:  # noqa: ANN001
    targets = []
    for outcome in outcomes:
        if not isinstance(outcome.fixture, Mapping):
            raise RuntimeError(
                "Run 2 cannot derive a live target without the resolved "
                f"fixture for {outcome.scenario.source}/"
                f"{outcome.scenario.tenant_slug}",
            )
        targets.append(
            live_target_for(
                outcome.tenant_id,
                outcome.scenario.source,
                outcome.scenario.tenant_slug,
                dict(outcome.fixture),
            )
        )
    return targets


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
    scenarios = run2_scenarios(tenants_per_source)
    live_only_source_ids = tuple(
        definition.source_id
        for definition in SOURCE_DEFINITIONS
        if definition.history is None
    )
    report = RunReport(
        run_name=(
            "Fault injection across all canonical sources "
            "(FLAKY + partition-missing)"
        ),
        run_number=2,
        tenant_count=(
            len(scenarios)
            + len(live_only_source_ids) * tenants_per_source
        ),
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
        try:
            await _migrate_and_truncate(pool)
            pf = await run_preflight(pool)
            report.preflight_lines = [
                f"{r.source}: {r.records_checked} records, "
                f"external_id={r.sample_external_id[:32]!r} ✅" for r in pf]

            # ---- Backfill (FLAKY) ----
            harness = BackfillHarness(
                pool=pool,
                scenarios=scenarios,
                concurrency=8, completion_deadline_s=180.0,
                kafka_bootstrap_servers=bootstrap_servers,
            )
            result = await harness.run()
            report.subprocess_returncodes = dict(result.subprocess_returncodes)

            # ---- Live phase ----
            targets = [
                *_historical_live_targets(result.outcomes),
                *await seed_contract_live_only_targets(
                    pool,
                    tenants_per_source=tenants_per_source,
                ),
            ]
            twins = await capture_twin_identities(pool, targets)
            drivers = await prepare_live_drivers(
                pool,
                targets,
                SigningSecrets(),
            )
            try:
                live = await run_live_phase(
                    pool, drivers, targets, twins,
                    events_per_tenant=events_per_tenant)
                drained = await wait_for_live_consumer_drain(
                    pool, {t.tenant_id for t in targets})
            finally:
                await teardown_live_drivers(drivers)

            # ---- A28 partition-missing injection (one per source) ----
            expected_pm = await partition_missing_probe(
                pool, targets, bootstrap_servers=bootstrap_servers)
            report.live_lines = [
                "FLAKY (one-in-ten 503) applied to all Provider Lab sources",
                f"partition-missing injections (one/source): {expected_pm}",
                f"live per-source deltas: {live.per_source_counts}",
                f"live drain stable: {drained}",
            ]

            # ---- Per-source exact recovery counts ----
            by_source: dict[str, list] = {}
            for o in result.outcomes:
                by_source.setdefault(o.scenario.source, []).append(o)
            targets_by_source: dict[str, list] = {
                definition.source_id: []
                for definition in SOURCE_DEFINITIONS
            }
            for target in targets:
                targets_by_source[target.source].append(target)
            for definition in SOURCE_DEFINITIONS:
                source = definition.source_id
                outs = by_source.get(source, [])
                src_targets = targets_by_source[source]
                src_tids = [target.tenant_id for target in src_targets]
                bf_expected = sum(
                    o.scenario.expected_observation_count for o in outs)
                live_expected = events_per_tenant * len(src_targets)
                actual = int(await pool.fetchval(
                    "SELECT count(*) FROM observations WHERE tenant_id = ANY($1)",
                    src_tids))
                # +1/source for partition-missing tenants are NOT written
                # (they DLQ), so expected excludes them.
                exp = bf_expected + live_expected
                report.source_results.append(SourceResult(
                    source=source, tenants=len(src_targets),
                    expected_observations=exp, actual_observations=actual))

            # ---- Assertions ----
            await _run_assertion(
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
                    live.tamper_results,
                    expected_sources=HMAC_SOURCES,
                ))
            await _run_assertion(
                report.assertions,
                "assert_live_observations_attributed_correctly",
                A.assert_live_observations_attributed_correctly(
                    live.actual_live_by_tenant,
                    live.expected_live_by_tenant,
                ),
            )
            await _run_assertion(
                report.assertions,
                "assert_all_backfills_complete_after_transient_faults",
                _noraise(A.assert_all_complete, result),
            )
            await _run_assertion(
                report.assertions,
                "assert_backfill_counts_recovered_after_transient_faults",
                _noraise(A.assert_observation_count_matches_fixture, result),
            )
            report.assertions.append(AssertionResult(
                name="assert_live_drain_stable",
                passed=drained,
                detail="" if drained else "live observation count did not stabilize",
            ))
            await _run_assertion(
                report.assertions, "assert_no_duplicate_observations",
                _noraise(A.assert_no_duplicate_observations, result))

            report.notes.append(
                "FLAKY faults are transient and must recover without missing "
                "records; partial results fail certification. A19: "
                "orchestrator subprocesses must not crash; A28: "
                "partition-missing must route to DLQ. "
                "Historical sources="
                f"{sum(d.history is not None for d in SOURCE_DEFINITIONS)}; "
                f"live-only sources={list(live_only_source_ids)}. "
                "Consumer rc=-9/-15 expected per ticket #45.")
        finally:
            await pool.close()

    report.wall_seconds = time.monotonic() - t0

    report.verdict = "READY" if report.passed else "NOT_READY"
    return report


async def _noraise(fn, *args):
    fn(*args)
