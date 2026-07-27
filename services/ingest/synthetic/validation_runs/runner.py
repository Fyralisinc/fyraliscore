"""Composed validation runner — spine (A29).

Standalone, operator-invokable (Decision 1). It brings up its own moto S3
(Decision 9), resets Kafka + bucket state (Decision 10), runs the
fixture-realism pre-flight (Decision 12), executes the selected contract-
derived validation run, checks run-level assertions (Decision 5), and writes
a markdown report (Decision 6) with the consumer-rc policy applied
(Decision 11).

    COMPANY_OS_ENV=test \
    DATABASE_URL=postgresql://... \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    python -m services.ingest.synthetic.validation_runs.runner --run=1

Runs 1–4 and the Provider Lab capstone are executable. ``--run=all`` executes
Runs 1–4 sequentially; ``--run=5`` executes the Provider Lab capstone shape.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
import os
import pathlib
import sys
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence

import asyncpg

from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.synthetic.backfill_harness.harness import (
    BackfillHarness,
    TenantOutcome,
)
from services.ingest.synthetic.backfill_harness.scenarios import BackfillScenario
from services.ingest.synthetic.validation_runs import assertions as A
from services.ingest.synthetic.validation_runs import composition as C
from services.ingest.synthetic.validation_runs.composition import (
    SigningSecrets,
    capture_twin_identities,
    live_target_for,
    prepare_live_drivers,
    run_live_phase,
    run_replay_probe,
    seed_contract_live_only_targets,
    teardown_live_drivers,
    wait_for_live_consumer_drain,
)
from services.ingest.synthetic.validation_runs.cleanup import reset_state
from services.ingest.synthetic.validation_runs.moto_lifecycle import moto_s3
from services.ingest.synthetic.validation_runs.preflight import (
    run_preflight,
)
from services.ingest.synthetic.validation_runs.reports import (
    AssertionResult,
    RunReport,
    SourceResult,
    write_report,
)
from services.ingest.synthetic.validation_runs.runs import (
    certification_history_scenarios,
)


log = logging.getLogger("validation_runs")

_MIGRATIONS = pathlib.Path("db/migrations")


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


def _validate_run1_scenario_membership(
    scenarios: Sequence[BackfillScenario],
    *,
    tenants_per_source: int,
) -> tuple[str, ...]:
    """Require the scenario matrix to match the source contract exactly."""

    historical_source_ids, _ = _contract_source_membership()
    counts = Counter(scenario.source for scenario in scenarios)
    expected_counts = {
        source_id: tenants_per_source
        for source_id in historical_source_ids
    }
    if counts != expected_counts:
        raise RuntimeError(
            "Run 1 historical scenario membership drifted from the source "
            f"contract: expected {expected_counts!r}, got {dict(counts)!r}",
        )
    return historical_source_ids


def _require_exact_fixture_counts(
    scenarios: Sequence[BackfillScenario],
) -> None:
    """Fail before E2E setup when a source-owned count oracle is unresolved."""

    unresolved = [
        f"{scenario.source}/{scenario.tenant_slug}"
        for scenario in scenarios
        if scenario.expected_observation_count <= 0
    ]
    if unresolved:
        raise RuntimeError(
            "Run 1 requires an exact source-owned fixture observation count "
            "for every historical scenario; unresolved: "
            f"{unresolved[:10]!r}"
            + (
                f" (+{len(unresolved) - 10} more)"
                if len(unresolved) > 10
                else ""
            ),
        )


def _live_targets_from_outcomes(
    outcomes: Sequence[TenantOutcome],
) -> list[C.LiveTarget]:
    """Build live addressing from the fixture the harness actually seeded."""

    targets = []
    for outcome in outcomes:
        fixture = outcome.fixture
        if not isinstance(fixture, Mapping):
            raise RuntimeError(
                "Run 1 cannot derive a live target without the resolved "
                "certification fixture for "
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


def _outcomes_by_source(
    outcomes: Sequence[TenantOutcome],
    source_ids: Sequence[str],
) -> dict[str, list[TenantOutcome]]:
    grouped = {source_id: [] for source_id in source_ids}
    for outcome in outcomes:
        try:
            grouped[outcome.scenario.source].append(outcome)
        except KeyError as exc:
            raise RuntimeError(
                "Run 1 produced an outcome outside its contract-derived "
                f"historical membership: {outcome.scenario.source!r}",
            ) from exc
    return grouped


def _expected_source_observations(
    source: str,
    outcomes: Sequence[TenantOutcome],
    *,
    events_per_tenant: int,
    replay: Mapping[str, Mapping[str, int]],
) -> int:
    """Combine source-owned backfill counts with declared live capabilities."""

    expected_backfill = sum(
        outcome.scenario.expected_observation_count
        for outcome in outcomes
    )
    if outcomes and any(
        outcome.scenario.expected_observation_count <= 0
        for outcome in outcomes
    ):
        raise RuntimeError(
            f"Run 1 cannot account for {source!r} with an unresolved "
            "fixture observation count",
        )

    expected_replay = 0
    if source in C.REPLAY_SOURCES and outcomes:
        probe = replay.get(source)
        if probe is None:
            raise RuntimeError(
                f"Run 1 replay accounting is missing declared source {source!r}",
            )
        expected_replay = int(probe["dispatched_unique"])
    elif source in replay:
        raise RuntimeError(
            f"Run 1 received replay accounting for undeclared source {source!r}",
        )

    return (
        expected_backfill
        + events_per_tenant * len(outcomes)
        + expected_replay
    )


def _capability_cell(
    source: str,
    *,
    live_sources: set[str],
    capability_sources: Sequence[str],
    covered_sources: set[str],
    capability_name: str,
) -> str:
    if source not in live_sources:
        return "— (not run)"
    if source not in capability_sources:
        return f"— (not in {capability_name})"
    if source in covered_sources:
        return "✅"
    return "❌ (declared, not observed)"


def _run1_coverage_rows(
    *,
    historical_sources: Sequence[str],
    live_sources: Sequence[str],
    live_only_sources: Sequence[str],
    twin_covered_sources: Iterable[str],
    signature_covered_sources: Iterable[str],
    replay_covered_sources: Iterable[str],
) -> list[tuple[str, str, str, str, str, str]]:
    """Render every canonical source without inventing universal probes."""

    historical = set(historical_sources)
    live = set(live_sources)
    live_only = set(live_only_sources)
    twin_covered = set(twin_covered_sources)
    signature_covered = set(signature_covered_sources)
    replay_covered = set(replay_covered_sources)
    rows: list[tuple[str, str, str, str, str, str]] = []
    for definition in SOURCE_DEFINITIONS:
        source = definition.source_id
        if source in live_only:
            rows.append(
                (
                    source,
                    "— (history=None)",
                    "✅" if source in live else "❌",
                    _capability_cell(
                        source,
                        live_sources=live,
                        capability_sources=C.TWIN_SOURCES,
                        covered_sources=twin_covered,
                        capability_name="TWIN_SOURCES",
                    ),
                    _capability_cell(
                        source,
                        live_sources=live,
                        capability_sources=C.HMAC_SOURCES,
                        covered_sources=signature_covered,
                        capability_name="HMAC_SOURCES",
                    ),
                    _capability_cell(
                        source,
                        live_sources=live,
                        capability_sources=C.REPLAY_SOURCES,
                        covered_sources=replay_covered,
                        capability_name="REPLAY_SOURCES",
                    ),
                )
            )
            continue
        if source not in historical:
            raise RuntimeError(
                f"Run 1 coverage has no classification for source {source!r}",
            )
        rows.append(
            (
                source,
                "✅",
                "✅" if source in live else "❌",
                _capability_cell(
                    source,
                    live_sources=live,
                    capability_sources=C.TWIN_SOURCES,
                    covered_sources=twin_covered,
                    capability_name="TWIN_SOURCES",
                ),
                _capability_cell(
                    source,
                    live_sources=live,
                    capability_sources=C.HMAC_SOURCES,
                    covered_sources=signature_covered,
                    capability_name="HMAC_SOURCES",
                ),
                _capability_cell(
                    source,
                    live_sources=live,
                    capability_sources=C.REPLAY_SOURCES,
                    covered_sources=replay_covered,
                    capability_name="REPLAY_SOURCES",
                ),
            )
        )
    return rows


def _declared_sources(
    ordered_sources: Sequence[str],
    capability_sources: Sequence[str],
) -> tuple[str, ...]:
    declared = set(capability_sources)
    return tuple(source for source in ordered_sources if source in declared)


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
            await conn.execute(
                f"TRUNCATE {names} RESTART IDENTITY CASCADE"
            )


async def _run_assertion(
    results: list[AssertionResult], name: str, coro,
) -> None:
    try:
        await coro
        results.append(AssertionResult(name=name, passed=True))
    except A.PropertyViolation as exc:
        results.append(
            AssertionResult(name=name, passed=False, detail=str(exc)[:300])
        )


async def run1(
    *, bootstrap_servers: str, tenants_per_source: int = 4,
    events_per_tenant: int = 5,
) -> RunReport:
    """Execute Run 1 for every historical source and expose live-only gaps."""
    started = dt.datetime.now(tz=dt.timezone.utc)
    t0 = time.monotonic()
    dsn = os.environ["DATABASE_URL"]
    scenarios = certification_history_scenarios(tenants_per_source)
    historical_source_ids = _validate_run1_scenario_membership(
        scenarios,
        tenants_per_source=tenants_per_source,
    )
    _require_exact_fixture_counts(scenarios)
    _, live_only_source_ids = _contract_source_membership()

    report = RunReport(
        run_name="E2E backfill + live (all canonical sources)",
        run_number=1,
        tenant_count=(
            len(scenarios)
            + len(live_only_source_ids) * tenants_per_source
        ),
        started_at=started,
        wall_seconds=0.0,
    )

    with moto_s3() as endpoint:
        cleanup = await reset_state(
            bootstrap_servers=bootstrap_servers,
            s3_endpoint_url=endpoint,
            s3_bucket=os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
        )
        report.cleanup_line = (
            f"recreated {cleanup.topics_recreated}; "
            f"cleared {cleanup.s3_objects_deleted} stale S3 objects"
        )

        pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
        try:
            await _migrate_and_truncate(pool)

            # ---- Pre-flight (fail-fast) ----
            pf = await run_preflight(pool)
            report.preflight_lines = [
                f"{r.source}: {r.records_checked} records, "
                f"external_id={r.sample_external_id[:32]!r}, "
                f"occurred_at={r.sample_occurred_at} ✅"
                for r in pf
            ]

            # ---- Backfill phase (drain built into harness — D4) ----
            harness = BackfillHarness(
                pool=pool,
                scenarios=scenarios,
                concurrency=8,
                completion_deadline_s=120.0,
                kafka_bootstrap_servers=bootstrap_servers,
                # The all-source matrix emits more than one thousand raw
                # records.  Producer completion can precede the
                # observation-writer by tens of seconds on a contended CI
                # host; the harness must keep the consumers alive until the
                # exact per-tenant count oracle is satisfied.  Run 4 already
                # uses a larger explicit drain budget for the same reason.
                drain_timeout_s=120.0,
            )
            result = await harness.run()
            report.subprocess_returncodes = dict(result.subprocess_returncodes)

            # ---- Backfill-snapshot assertions (on the harness result) ----
            tenant_ids = {o.tenant_id for o in result.outcomes}
            await _run_assertion(
                report.assertions, "assert_all_complete",
                _as_coro(A.assert_all_complete, result),
            )
            await _run_assertion(
                report.assertions, "assert_observation_count_matches_fixture",
                _as_coro(A.assert_observation_count_matches_fixture, result),
            )

            # ---- Live phase (A30) ----
            targets = [
                *_live_targets_from_outcomes(result.outcomes),
                *await seed_contract_live_only_targets(
                    pool,
                    tenants_per_source=tenants_per_source,
                ),
            ]
            tenant_ids = {target.tenant_id for target in targets}
            live_source_ids = tuple(
                dict.fromkeys(target.source for target in targets)
            )
            twins = await capture_twin_identities(pool, targets)
            drivers = await prepare_live_drivers(
                pool,
                targets,
                SigningSecrets(),
            )
            try:
                live = await run_live_phase(
                    pool, drivers, targets, twins,
                    events_per_tenant=events_per_tenant,
                )
                drained = await wait_for_live_consumer_drain(
                    pool, {t.tenant_id for t in targets},
                )
                replay = await run_replay_probe(pool, drivers, targets)
            finally:
                await teardown_live_drivers(drivers)

            twin_sources = _declared_sources(
                historical_source_ids,
                C.TWIN_SOURCES,
            )
            signature_sources = _declared_sources(
                historical_source_ids,
                C.HMAC_SOURCES,
            )
            replay_sources = _declared_sources(
                historical_source_ids,
                C.REPLAY_SOURCES,
            )
            report.live_lines = [
                f"live events/tenant: {events_per_tenant}; "
                f"per-source live deltas: {live.per_source_counts}",
                f"cross-path twins (declared={list(twin_sources)}; "
                "dispatched): "
                f"{sorted(live.twin_external_ids.keys())}",
                f"signature-gate probes (declared={list(signature_sources)}): "
                f"{[(r['source'], r['http_status']) for r in live.tamper_results]}",
                f"replay probe (declared={list(replay_sources)}; "
                "dispatched_unique→observed): "
                f"{ {s: v['observed'] for s, v in replay.items()} }",
                f"live drain stable: {drained}",
                f"live-only sources: {list(live_only_source_ids)}",
            ]

            # ---- Per-source observation counts (backfill + live) ----
            by_source = _outcomes_by_source(
                result.outcomes,
                historical_source_ids,
            )
            for source in historical_source_ids:
                outs = by_source[source]
                src_tids = [o.tenant_id for o in outs]
                actual = int(await pool.fetchval(
                    "SELECT count(*) FROM observations "
                    "WHERE tenant_id = ANY($1)", src_tids,
                ))
                report.source_results.append(SourceResult(
                    source=source,
                    tenants=len(outs),
                    expected_observations=_expected_source_observations(
                        source,
                        outs,
                        events_per_tenant=events_per_tenant,
                        replay=replay,
                    ),
                    actual_observations=actual,
                ))
            targets_by_source: dict[str, list[C.LiveTarget]] = {
                source: [] for source in live_only_source_ids
            }
            for target in targets:
                if target.source in targets_by_source:
                    targets_by_source[target.source].append(target)
            for source in live_only_source_ids:
                source_targets = targets_by_source[source]
                source_tenant_ids = [
                    target.tenant_id for target in source_targets
                ]
                actual = int(await pool.fetchval(
                    "SELECT count(*) FROM observations "
                    "WHERE tenant_id = ANY($1)",
                    source_tenant_ids,
                ))
                report.source_results.append(SourceResult(
                    source=source,
                    tenants=len(source_targets),
                    expected_observations=(
                        events_per_tenant * len(source_targets)
                    ),
                    actual_observations=actual,
                ))

            # ---- Run-level assertions (D5 + A30) ----
            await _run_assertion(
                report.assertions, "assert_no_duplicate_observations",
                _as_coro(A.assert_no_duplicate_observations, result),
            )
            await _run_assertion(
                report.assertions, "assert_external_id_unique_across_paths",
                A.assert_external_id_unique_across_paths(pool),
            )
            await _run_assertion(
                report.assertions,
                "assert_observations_have_exactly_one_t1_trigger",
                A.assert_observations_have_exactly_one_t1_trigger(
                    pool,
                    tenant_ids,
                ),
            )
            await _run_assertion(
                report.assertions,
                "assert_cross_path_twins_dedup",
                A.assert_cross_path_twins_dedup(pool, live.twin_external_ids),
            )
            await _run_assertion(
                report.assertions,
                "assert_live_observations_attributed_correctly",
                A.assert_live_observations_attributed_correctly(
                    live.actual_live_by_tenant, live.expected_live_by_tenant,
                ),
            )
            await _run_assertion(
                report.assertions,
                "assert_signature_validation_gate_holds_for_hmac_sources",
                A.assert_signature_validation_gate_holds_for_hmac_sources(
                    live.tamper_results,
                    expected_sources=C.HMAC_SOURCES,
                ),
            )
            await _run_assertion(
                report.assertions, "assert_live_replay_idempotency_holds",
                A.assert_live_replay_idempotency_holds(replay),
            )
            await _run_assertion(
                report.assertions, "assert_per_tenant_timeline_monotonic",
                A.assert_per_tenant_timeline_monotonic(pool, tenant_ids),
            )
            await _run_assertion(
                report.assertions, "assert_zero_partition_missing",
                A.assert_zero_partition_missing(
                    bootstrap_servers=bootstrap_servers,
                    tenant_ids=tenant_ids,
                ),
            )

            report.assertions.append(
                AssertionResult(
                    name="assert_all_contract_sources_have_live_targets",
                    passed=set(live_source_ids) == {
                        definition.source_id
                        for definition in SOURCE_DEFINITIONS
                    },
                    detail=(
                        f"resolved live targets for {len(live_source_ids)}/"
                        f"{len(SOURCE_DEFINITIONS)} canonical sources"
                    ),
                )
            )
            report.coverage_rows = _run1_coverage_rows(
                historical_sources=historical_source_ids,
                live_sources=live_source_ids,
                live_only_sources=live_only_source_ids,
                twin_covered_sources=live.twin_external_ids,
                signature_covered_sources=tuple(
                    result["source"]
                    for result in live.tamper_results
                ),
                replay_covered_sources=replay,
            )
            report.notes.append(
                "Live ingestion is inline (no Kafka consumer needed); "
                f"cross-path twins={list(twin_sources)}, "
                f"signature probes={list(signature_sources)}, "
                f"replay probes={list(replay_sources)}. "
                "Consumer rc=-9/-15 expected per ticket #45."
            )
            report.notes.append(
                "Contract live-only bootstrap covered "
                f"{list(live_only_source_ids)} without fabricating a "
                "historical planner/fetcher result."
            )
        finally:
            await pool.close()

    report.wall_seconds = time.monotonic() - t0
    return report


async def _as_coro(fn, *args):
    """Adapt a sync assertion (raises PropertyViolation) to an awaitable."""
    fn(*args)


def _execute_run(n: int, *, bootstrap: str, tenants_per_source: int):
    """Execute one run (1/2/3) and return its RunReport."""
    if n == 1:
        return asyncio.run(run1(
            bootstrap_servers=bootstrap,
            tenants_per_source=tenants_per_source,
        ))
    if n == 2:
        from services.ingest.synthetic.validation_runs.run2_fault_injection import (
            run2,
        )
        return asyncio.run(run2(
            bootstrap_servers=bootstrap,
            tenants_per_source=tenants_per_source,
        ))
    if n == 3:
        from services.ingest.synthetic.validation_runs.run3_concurrency_stress import (
            run3,
        )
        return asyncio.run(run3(bootstrap_servers=bootstrap))
    if n == 5:
        from services.ingest.synthetic.validation_runs.run4_concurrent import run5
        return asyncio.run(run5(bootstrap_servers=bootstrap))
    from services.ingest.synthetic.validation_runs.run4_concurrent import run4
    return asyncio.run(run4(bootstrap_servers=bootstrap))


def _run_ok(report) -> bool:
    if report.verdict is not None:
        return report.verdict == "READY"
    return report.passed


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("VALIDATION_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Composed validation runs")
    parser.add_argument(
        "--run", default="1", choices=("1", "2", "3", "4", "5", "all"),
        help="which run to execute; 'all' runs 1→2→3→4 sequentially. "
             "5 = Provider Lab capstone (Run 4 shape, real clients → lab).",
    )
    parser.add_argument("--tenants-per-source", type=int, default=4)
    args = parser.parse_args()

    if "DATABASE_URL" not in os.environ:
        print("DATABASE_URL is required.", file=sys.stderr)
        return 2
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    run_numbers = [1, 2, 3, 4] if args.run == "all" else [int(args.run)]
    all_ok = True
    verdicts: list[str] = []
    for n in run_numbers:
        report = _execute_run(
            n, bootstrap=bootstrap,
            tenants_per_source=args.tenants_per_source,
        )
        path = write_report(report)
        status = report.verdict or ("PASS" if report.passed else "FAIL")
        verdicts.append(f"Run {n}={status}")
        print(f"\nRun {n} {status} — report: {path}")
        rc_bad = report.rc_violations()
        if rc_bad:
            print(f"  rc violations: {rc_bad}", file=sys.stderr)
        all_ok = all_ok and _run_ok(report)

    if len(run_numbers) > 1:
        print(f"\nAll runs: {', '.join(verdicts)}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
