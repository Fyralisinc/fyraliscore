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
) -> RunReport:
    """Execute Run 1 (E2E backfill, all sources) and return its report."""
    started = dt.datetime.now(tz=dt.timezone.utc)
    t0 = time.monotonic()
    dsn = os.environ["DATABASE_URL"]

    report = RunReport(
        run_name="E2E backfill (all sources)",
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

            # ---- Per-source observation counts ----
            by_source: dict[str, list] = {}
            for o in result.outcomes:
                by_source.setdefault(o.scenario.source, []).append(o)
            for source in ("gmail", "github", "slack", "discord"):
                outs = by_source.get(source, [])
                report.source_results.append(SourceResult(
                    source=source,
                    tenants=len(outs),
                    expected_observations=sum(
                        o.scenario.expected_observation_count for o in outs
                    ),
                    actual_observations=sum(len(o.observations) for o in outs),
                ))

            # ---- Run-level assertions (D5) ----
            tenant_ids = {o.tenant_id for o in result.outcomes}
            await _run_assertion(
                report.assertions, "assert_all_complete",
                _as_coro(A.assert_all_complete, result),
            )
            await _run_assertion(
                report.assertions, "assert_observation_count_matches_fixture",
                _as_coro(A.assert_observation_count_matches_fixture, result),
            )
            await _run_assertion(
                report.assertions, "assert_no_duplicate_observations",
                _as_coro(A.assert_no_duplicate_observations, result),
            )
            await _run_assertion(
                report.assertions, "assert_external_id_unique_across_paths",
                A.assert_external_id_unique_across_paths(pool),
            )
            await _run_assertion(
                report.assertions, "assert_zero_partition_missing",
                A.assert_zero_partition_missing(
                    bootstrap_servers=bootstrap_servers,
                    tenant_ids=tenant_ids,
                ),
            )

            report.notes.append(
                "Live phase + Runs 2/3 deferred to M-Validate-Live "
                "(ticket #47). Consumer rc=-9/-15 expected per ticket #45."
            )
        finally:
            await pool.close()

    report.wall_seconds = time.monotonic() - t0
    return report


async def _as_coro(fn, *args):
    """Adapt a sync assertion (raises PropertyViolation) to an awaitable."""
    fn(*args)


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("VALIDATION_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Composed validation runs")
    parser.add_argument("--run", type=int, default=1, choices=(1, 2, 3))
    parser.add_argument("--tenants-per-source", type=int, default=4)
    args = parser.parse_args()

    if args.run in (2, 3):
        print(
            f"Run {args.run} (fault injection / concurrency) is deferred to "
            f"the M-Validate-Live work-unit (ticket #47). This spine ships "
            f"Run 1 only.",
            file=sys.stderr,
        )
        return 2

    if "DATABASE_URL" not in os.environ:
        print("DATABASE_URL is required.", file=sys.stderr)
        return 2
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    report = asyncio.run(run1(
        bootstrap_servers=bootstrap,
        tenants_per_source=args.tenants_per_source,
    ))
    path = write_report(report)
    status = "PASS" if report.passed else "FAIL"
    print(f"\nRun 1 {status} — report: {path}")
    rc_bad = report.rc_violations()
    if rc_bad:
        print(f"  rc violations: {rc_bad}", file=sys.stderr)
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
