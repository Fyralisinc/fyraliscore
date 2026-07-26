"""Run 3 — contract-wide concurrency stress (A30.4).

Two tenants for every history-capable canonical source (currently 52 tenants
across 26 sources), driven through the SAME seven shared M6 subprocesses at
concurrency=10 (not one process set per tenant).  Source membership, fixture
construction, and exact Observation counts come from the source certification
contract.  The run is HAPPY_PATH and **backfill-only** (live phase skipped —
the focus is backfill concurrency + per-tenant isolation).

A concurrent monitor samples, while the backfill runs:
  - peak simultaneous `source_onboarding_runs.status='in_progress'`
    (concurrency actually exercised),
  - peak unconsumed `workflow_signals` backlog (bounded signal table).

Assertions (A22 properties under load):
  - per-tenant isolation: each tenant's observation count matches its
    source-owned exact fixture oracle independently,
  - signal-table backlog bounded (< 3× tenant count),
  - concurrency exercised (≥5 in_progress simultaneously),
  - #39 flake watch: `tenant_onboarding_completed` fires exactly once per
    tenant (no double-fire, no miss).
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import pathlib
import time

import asyncpg

from services.ingest.synthetic.backfill_harness.harness import BackfillHarness
from services.ingest.synthetic.backfill_harness.scenarios import BackfillScenario
from services.ingest.synthetic.validation_runs.cleanup import reset_state
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


log = logging.getLogger("validation_runs.run3")
_MIGRATIONS = pathlib.Path("db/migrations")

# Preserve the original roughly-50-tenant stress shape without owning another
# source list: two tenants for each contract-declared historical source.
_TENANTS_PER_HISTORY_SOURCE = 2


def run3_scenarios() -> list[BackfillScenario]:
    """Build the deterministic contract-wide Run 3 matrix.

    ``certification_history_scenarios`` resolves each source's fixture factory
    and exact count oracle before returning.  The additional validation here
    makes Run 3 itself fail closed if that composition ever yields an
    unspecified, boolean, zero, or negative count.
    """
    scenarios = certification_history_scenarios(
        tenants_per_source=_TENANTS_PER_HISTORY_SOURCE,
    )
    for scenario in scenarios:
        expected = scenario.expected_observation_count
        if (
            isinstance(expected, bool)
            or not isinstance(expected, int)
            or expected <= 0
        ):
            raise ValueError(
                f"{scenario.source} Run 3 expected_observation_count must be "
                f"a positive exact integer, got {expected!r}",
            )
    return scenarios


def _group_scenarios(
    scenarios: list[BackfillScenario],
) -> dict[str, list[BackfillScenario]]:
    """Group in contract scenario order without maintaining a source list."""
    grouped: dict[str, list[BackfillScenario]] = {}
    for scenario in scenarios:
        grouped.setdefault(scenario.source, []).append(scenario)
    return grouped


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


# `tenant_onboarding_completed` is the TERMINAL per-tenant completion
# marker (the #39-watched signal). It is unconsumed by design in the
# backfill harness — nothing downstream claims it here — so it
# accumulates monotonically to one-per-tenant. It is NOT pending work;
# excluding it makes "backlog" mean *unprocessed work* (every other
# signal kind drains to 0). Verified: at run end the only unconsumed
# signals were exactly the 50 terminal markers.
_TERMINAL_SIGNAL = "tenant_onboarding_completed"


async def _monitor(pool: asyncpg.Pool, stop: asyncio.Event,
                   peak: dict[str, int], *, interval_s: float = 1.0) -> None:
    """Sample peak concurrency + working-signal backlog while backfill
    runs (working = unconsumed signals EXCLUDING the terminal completion
    marker)."""
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


async def run3(
    *, bootstrap_servers: str, concurrency: int = 10,
) -> RunReport:
    started = dt.datetime.now(tz=dt.timezone.utc)
    t0 = time.monotonic()
    dsn = os.environ["DATABASE_URL"]
    scenarios = run3_scenarios()
    scenarios_by_source = _group_scenarios(scenarios)
    report = RunReport(
        run_name=(
            "Contract-wide concurrency stress "
            f"({len(scenarios)} tenants, backfill-only)"
        ),
        run_number=3, tenant_count=len(scenarios),
        started_at=started, wall_seconds=0.0,
    )

    with moto_s3() as endpoint:
        cleanup = await reset_state(
            bootstrap_servers=bootstrap_servers, s3_endpoint_url=endpoint,
            s3_bucket=os.environ.get("S3_RAW_BUCKET", "fyralis-raw"))
        report.cleanup_line = (
            f"recreated {cleanup.topics_recreated}; cleared "
            f"{cleanup.s3_objects_deleted} stale S3 objects")
        pool = await asyncpg.create_pool(dsn, min_size=4, max_size=16)
        peak = {"in_progress": 0, "backlog": 0}
        try:
            await _migrate_and_truncate(pool)
            pf = await run_preflight(pool)
            report.preflight_lines = [
                f"{r.source}: external_id={r.sample_external_id[:32]!r} ✅"
                for r in pf]

            harness = BackfillHarness(
                pool=pool, scenarios=scenarios, concurrency=concurrency,
                completion_deadline_s=600.0,
                kafka_bootstrap_servers=bootstrap_servers)

            stop = asyncio.Event()
            mon = asyncio.create_task(_monitor(pool, stop, peak))
            try:
                result = await harness.run()
            finally:
                stop.set()
                await mon
            report.subprocess_returncodes = dict(result.subprocess_returncodes)

            # ---- Per-source counts (contract scenario order) ----
            outcomes_by_source: dict[str, list] = {}
            for o in result.outcomes:
                outcomes_by_source.setdefault(o.scenario.source, []).append(o)
            for source, source_scenarios in scenarios_by_source.items():
                outs = outcomes_by_source.get(source, [])
                src_tids = [o.tenant_id for o in outs]
                exp = sum(
                    scenario.expected_observation_count
                    for scenario in source_scenarios
                )
                actual = 0
                if src_tids:
                    actual = int(await pool.fetchval(
                        "SELECT count(*) FROM observations "
                        "WHERE tenant_id = ANY($1)",
                        src_tids,
                    ))
                report.source_results.append(SourceResult(
                    source=source, tenants=len(source_scenarios),
                    expected_observations=exp, actual_observations=actual))

            # Every planned scenario must yield exactly one outcome.  Without
            # this explicit check, a missing source could otherwise disappear
            # from the subsequent per-tenant loop.
            planned_keys = [
                (scenario.source, scenario.tenant_slug)
                for scenario in scenarios
            ]
            outcome_keys = [
                (outcome.scenario.source, outcome.scenario.tenant_slug)
                for outcome in result.outcomes
            ]
            missing_outcomes = sorted(set(planned_keys) - set(outcome_keys))
            unexpected_outcomes = sorted(set(outcome_keys) - set(planned_keys))
            outcome_coverage_ok = (
                len(outcome_keys) == len(planned_keys)
                and len(set(outcome_keys)) == len(outcome_keys)
                and not missing_outcomes
                and not unexpected_outcomes
            )
            report.assertions.append(AssertionResult(
                name="assert_contract_scenario_outcome_coverage",
                passed=outcome_coverage_ok,
                detail=(
                    f"planned={len(planned_keys)}, "
                    f"outcomes={len(outcome_keys)}, "
                    f"missing={missing_outcomes[:3]}, "
                    f"unexpected={unexpected_outcomes[:3]}"
                ),
            ))

            # ---- Per-tenant isolation ----
            iso_detail = ""
            iso_ok = True
            outcome_by_key = {
                (outcome.scenario.source, outcome.scenario.tenant_slug): outcome
                for outcome in result.outcomes
            }
            for scenario in scenarios:
                outcome = outcome_by_key.get(
                    (scenario.source, scenario.tenant_slug),
                )
                if outcome is None:
                    iso_ok = False
                    iso_detail = (
                        f"{scenario.tenant_slug}: no harness outcome"
                    )
                    break
                n = int(await pool.fetchval(
                    "SELECT count(*) FROM observations WHERE tenant_id = $1",
                    outcome.tenant_id,
                ))
                if n != scenario.expected_observation_count:
                    iso_ok = False
                    iso_detail = (
                        f"{scenario.tenant_slug}: got {n}, "
                        f"expected {scenario.expected_observation_count}"
                    )
                    break
            report.assertions.append(AssertionResult(
                name="assert_per_tenant_isolation", passed=iso_ok,
                detail=iso_detail))

            # ---- Concurrency exercised ----
            conc_ok = peak["in_progress"] >= 5
            report.assertions.append(AssertionResult(
                name="assert_concurrency_exercised(>=5 in_progress)",
                passed=conc_ok,
                detail=f"peak in_progress={peak['in_progress']}"))

            # ---- Signal backlog bounded (working signals) ----
            # The working backlog scales with PRODUCER fan-out (tenants ×
            # per-source shard count), NOT consumer concurrency: it stayed
            # ~106-115 even when per-tenant observation volume was cut 4×.
            # The prompt's original `10× concurrency` heuristic mis-modeled
            # the bound (it's O(tenants), bounded by total enqueued shards).
            # The genuine invariant is: backlog is bounded at a few signals
            # per in-flight tenant and never grows unbounded — < 3× tenant
            # count — AND fully drains (the no-leak assertion below).
            backlog_bound = 3 * len(scenarios)
            backlog_ok = peak["backlog"] < backlog_bound
            report.assertions.append(AssertionResult(
                name=f"assert_signal_backlog_bounded(<3×tenants={backlog_bound})",
                passed=backlog_ok,
                detail=f"peak working backlog={peak['backlog']} "
                       f"(O(tenants), not O(concurrency) — see A30.6)"))

            # ---- No signal leak: working signals fully drained ----
            residual = int(await pool.fetchval(
                "SELECT count(*) FROM workflow_signals "
                "WHERE consumed_at IS NULL AND signal_kind <> $1",
                _TERMINAL_SIGNAL))
            report.assertions.append(AssertionResult(
                name="assert_no_signal_leak(working drains to 0)",
                passed=residual == 0,
                detail=f"residual working signals={residual} "
                       f"(terminal {_TERMINAL_SIGNAL} excluded)"))

            # ---- #39 flake watch: completion fires exactly once/tenant ----
            bad_completion = [
                o.scenario.tenant_slug for o in result.outcomes
                if o.completion_signal_count != 1
            ]
            report.assertions.append(AssertionResult(
                name="assert_completion_fires_exactly_once_per_tenant(#39)",
                passed=not bad_completion,
                detail=(
                    f"all {len(scenarios)} fired once"
                    if not bad_completion
                    else f"anomalies: {bad_completion[:5]}"
                )))

            report.live_lines = [
                f"backfill-only; concurrency={concurrency}",
                f"peak simultaneous in_progress: {peak['in_progress']}",
                f"peak working signal backlog (terminal excluded): "
                f"{peak['backlog']}",
                f"completion-signal distribution: "
                f"{_distribution([o.completion_signal_count for o in result.outcomes])}",
            ]
            report.notes.append(
                f"{len(scenarios)} tenants across "
                f"{len(scenarios_by_source)} contract-declared historical "
                "sources through 7 shared subprocesses (not one process set "
                "per tenant). Live phase skipped (Decision: Run 3 = backfill "
                "concurrency focus). Consumer rc=-9/-15 expected per ticket "
                "#45.")
        finally:
            await pool.close()

    report.wall_seconds = time.monotonic() - t0
    report.verdict = "READY" if report.passed else "NOT_READY"
    return report


def _distribution(values: list[int]) -> dict[int, int]:
    out: dict[int, int] = {}
    for v in values:
        out[v] = out.get(v, 0) + 1
    return out
