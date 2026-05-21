"""Composed validation runner — spine (A29).

Standalone, operator-invokable (Decision 1). Brings up its own moto S3
(Decision 9), resets Kafka + bucket state (Decision 10), runs the
fixture-realism pre-flight (Decision 12), executes Run 1's backfill
across all four sources via the proven `BackfillHarness` (which already
does the consumer-drain wait, Decision 4), checks run-level assertions
(Decision 5), and writes a markdown report (Decision 6) with the
consumer-rc policy applied (Decision 11).

    COMPANY_OS_ENV=test \
    DATABASE_URL=postgresql://... \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    python -m services.synthetic.validation_runs.runner --run=1

DEFERRED to M-Validate-Live (ticket #47): the live phase (4 in-process
generators) and Runs 2 (fault) + 3 (concurrency). `--run=2|3` exit with
a pointer to that work-unit.
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

import asyncpg

from services.synthetic.backfill_harness.harness import BackfillHarness
from services.synthetic.validation_runs import assertions as A
from services.synthetic.validation_runs import composition as C
from services.synthetic.validation_runs.composition import (
    SigningSecrets,
    build_live_drivers,
    capture_twin_identities,
    live_target_for,
    run_live_phase,
    run_replay_probe,
    teardown_live_drivers,
    wait_for_live_consumer_drain,
)
from services.synthetic.validation_runs.cleanup import reset_state
from services.synthetic.validation_runs.moto_lifecycle import moto_s3
from services.synthetic.validation_runs.preflight import (
    PreflightFailure,
    run_preflight,
)
from services.synthetic.validation_runs.reports import (
    AssertionResult,
    RunReport,
    SourceResult,
    write_report,
)
from services.synthetic.validation_runs.runs import run1_scenarios


log = logging.getLogger("validation_runs")

_MIGRATIONS = pathlib.Path("db/migrations")


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
    """Execute Run 1 (E2E backfill + live, all sources) and return its
    report."""
    started = dt.datetime.now(tz=dt.timezone.utc)
    t0 = time.monotonic()
    dsn = os.environ["DATABASE_URL"]

    report = RunReport(
        run_name="E2E backfill + live (all sources)",
        run_number=1,
        tenant_count=tenants_per_source * 4,
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
            scenarios = run1_scenarios(tenants_per_source)
            harness = BackfillHarness(
                pool=pool,
                scenarios=scenarios,
                concurrency=8,
                completion_deadline_s=120.0,
                kafka_bootstrap_servers=bootstrap_servers,
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
                live_target_for(
                    o.tenant_id, o.scenario.source,
                    o.scenario.tenant_slug, o.scenario.fixture_params,
                )
                for o in result.outcomes
            ]
            twins = await capture_twin_identities(pool, targets)
            drivers = await build_live_drivers(pool, targets, SigningSecrets())
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

            report.live_lines = [
                f"live events/tenant: {events_per_tenant}; "
                f"per-source live deltas: {live.per_source_counts}",
                f"cross-path twins dispatched (gmail/github/slack): "
                f"{sorted(live.twin_external_ids.keys())}",
                f"signature-gate probes (HMAC): "
                f"{[(r['source'], r['http_status']) for r in live.tamper_results]}",
                f"replay probe (dispatched_unique→observed): "
                f"{ {s: v['observed'] for s, v in replay.items()} }",
                f"live drain stable: {drained}",
            ]

            # ---- Per-source observation counts (backfill + live) ----
            by_source: dict[str, list] = {}
            for o in result.outcomes:
                by_source.setdefault(o.scenario.source, []).append(o)
            for source in ("gmail", "github", "slack", "discord"):
                outs = by_source.get(source, [])
                src_tids = [o.tenant_id for o in outs]
                bf_expected = sum(
                    o.scenario.expected_observation_count for o in outs
                )
                live_expected = events_per_tenant * len(outs)
                replay_extra = 1 if source in C.REPLAY_SOURCES and outs else 0
                actual = int(await pool.fetchval(
                    "SELECT count(*) FROM observations "
                    "WHERE tenant_id = ANY($1)", src_tids,
                ))
                report.source_results.append(SourceResult(
                    source=source,
                    tenants=len(outs),
                    expected_observations=(
                        bf_expected + live_expected + replay_extra
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

            report.coverage_rows = [
                ("gmail", "✅", "✅", "✅", "— (OIDC no-op)", "✅"),
                ("github", "✅", "✅", "✅", "✅", "✅"),
                ("slack", "✅", "✅", "✅", "✅", "✅"),
                ("discord", "✅", "✅", "— (namespace, A30.3)",
                 "— (direct dispatch)", "— (no replay, A24)"),
            ]
            report.notes.append(
                "Live ingestion is inline (no Kafka consumer needed); "
                "cross-path twins exercised for gmail/github/slack; "
                "Discord excluded by namespace topology (A30.3). "
                "Consumer rc=-9/-15 expected per ticket #45."
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
        from services.synthetic.validation_runs.run2_fault_injection import (
            run2,
        )
        return asyncio.run(run2(
            bootstrap_servers=bootstrap,
            tenants_per_source=tenants_per_source,
        ))
    from services.synthetic.validation_runs.run3_concurrency_stress import (
        run3,
    )
    return asyncio.run(run3(bootstrap_servers=bootstrap))


def _run_ok(report) -> bool:
    if report.verdict is not None:
        return report.verdict in ("READY", "PARTIAL")
    return report.passed


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("VALIDATION_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Composed validation runs")
    parser.add_argument(
        "--run", default="1", choices=("1", "2", "3", "all"),
        help="which run to execute; 'all' runs 1→2→3 sequentially",
    )
    parser.add_argument("--tenants-per-source", type=int, default=4)
    args = parser.parse_args()

    if "DATABASE_URL" not in os.environ:
        print("DATABASE_URL is required.", file=sys.stderr)
        return 2
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    run_numbers = [1, 2, 3] if args.run == "all" else [int(args.run)]
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
