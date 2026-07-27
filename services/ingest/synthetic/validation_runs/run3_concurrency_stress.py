"""Run 3 — contract-wide multi-install, multi-replica stress (A30.4).

Two tenants with two sibling installations each for every history-capable
canonical source (52 tenants / 104 installations across 26 sources), driven
through two complete Fyralis service replicas at concurrency=10. Source
membership, fixture construction, and exact Observation counts come from the
source certification contract. The run is HAPPY_PATH and **backfill-only**
(live phase skipped — the focus is backfill concurrency, exact installation
binding, replica sharing, and per-tenant isolation).

A concurrent monitor samples, while the backfill runs:
  - peak simultaneous `source_onboarding_runs.status='in_progress'`
    (concurrency actually exercised),
  - peak unconsumed `workflow_signals` backlog (bounded signal table).

Assertions (A22 properties under load):
  - per-tenant isolation: each tenant's observation count matches the sum of
    both source-owned fixture oracles,
  - sibling installs retain distinct trigger/install/onboarding identities,
  - both OAuth poller replicas durably claim work,
  - signal-table backlog remains within the exact planned-shard working set,
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
from services.ingest.synthetic.backfill_harness.assertions import (
    PropertyViolation,
    assert_sibling_installation_identity,
)
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
_INSTALLATIONS_PER_TENANT = 2
_REPLICAS = 2


def _working_signal_bound(*, planned_shards: int, installations: int) -> int:
    """Maximum valid working set for the happy-path onboarding pipeline.

    Each durable shard has at most one outstanding request/completion signal:
    claiming a request consumes it before its completion successor becomes
    visible.  Source onboarding may additionally leave one control signal
    outstanding per installation while its shard fan-out drains.  Basing the
    bound on the durable plan matters because catalog sources fan out very
    differently (one AWS shard versus 25 GitHub shards in the default kit);
    a fixed multiplier of tenant count is therefore not a valid invariant.
    """

    if planned_shards < 0 or installations < 0:
        raise ValueError("signal-bound inputs must be non-negative")
    return planned_shards + installations


def run3_scenarios() -> list[BackfillScenario]:
    """Build the deterministic contract-wide Run 3 matrix.

    ``certification_history_scenarios`` resolves each source's fixture factory
    and exact count oracle before returning.  The additional validation here
    makes Run 3 itself fail closed if that composition ever yields an
    unspecified, boolean, zero, or negative count.
    """
    scenarios = certification_history_scenarios(
        tenants_per_source=_TENANTS_PER_HISTORY_SOURCE,
        installations_per_tenant=_INSTALLATIONS_PER_TENANT,
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
            "Contract-wide multi-install/multi-replica stress "
            f"({_TENANTS_PER_HISTORY_SOURCE * len(scenarios_by_source)} "
            f"tenants, {len(scenarios)} installations, backfill-only)"
        ),
        run_number=3,
        tenant_count=_TENANTS_PER_HISTORY_SOURCE * len(scenarios_by_source),
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
                kafka_bootstrap_servers=bootstrap_servers,
                # Run 3 materializes the largest exact-count payload in the
                # validation matrix through two competing consumer replicas.
                # Keep the drain bounded, but give the asynchronous
                # raw->normalized->Observation chain enough time to reach the
                # exact oracle on slower shared brokers.
                drain_timeout_s=90.0,
                replicas=_REPLICAS,
            )

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
                src_tids = list({o.tenant_id for o in outs})
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
                    source=source, tenants=len(src_tids),
                    expected_observations=exp, actual_observations=actual))

            # Every planned scenario must yield exactly one outcome.  Without
            # this explicit check, a missing source could otherwise disappear
            # from the subsequent per-tenant loop.
            planned_keys = [
                scenario.identity
                for scenario in scenarios
            ]
            outcome_keys = [
                outcome.scenario.identity
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
            outcomes_by_tenant: dict[object, list] = {}
            for outcome in result.outcomes:
                outcomes_by_tenant.setdefault(
                    outcome.tenant_id,
                    [],
                ).append(outcome)
            for tenant_outcomes in outcomes_by_tenant.values():
                expected = sum(
                    outcome.scenario.expected_observation_count
                    for outcome in tenant_outcomes
                )
                n = int(await pool.fetchval(
                    "SELECT count(*) FROM observations WHERE tenant_id = $1",
                    tenant_outcomes[0].tenant_id,
                ))
                if n != expected:
                    iso_ok = False
                    iso_detail = (
                        f"{tenant_outcomes[0].scenario.tenant_slug}: got {n}, "
                        f"expected {expected} across "
                        f"{len(tenant_outcomes)} installations"
                    )
                    break
            report.assertions.append(AssertionResult(
                name="assert_per_tenant_isolation", passed=iso_ok,
                detail=iso_detail))

            # ---- Exact same-tenant sibling installation identity ----
            try:
                assert_sibling_installation_identity(
                    result,
                    installations_per_tenant=_INSTALLATIONS_PER_TENANT,
                )
            except PropertyViolation as exc:
                sibling_ok = False
                sibling_detail = str(exc)
            else:
                sibling_ok = True
                sibling_detail = (
                    f"{len(outcomes_by_tenant)} tenants each retained "
                    f"{_INSTALLATIONS_PER_TENANT} exact install identities"
                )
            report.assertions.append(AssertionResult(
                name="assert_same_tenant_sibling_installation_identity",
                passed=sibling_ok,
                detail=sibling_detail,
            ))

            # ---- Two durable replicas both observed and participating ----
            replica_activity = result.replica_workflow_activity.get(
                "oauth_poller",
                {},
            )
            expected_replica_ids = set(
                harness.replica_workflow_ids("oauth_poller"),
            )
            replica_ok = (
                result.configured_replicas == _REPLICAS
                and result.observed_replica_count == _REPLICAS
                and result.participating_replica_count == _REPLICAS
                and set(replica_activity) == expected_replica_ids
            )
            report.assertions.append(AssertionResult(
                name="assert_two_replicas_share_onboarding_claims",
                passed=replica_ok,
                detail=(
                    f"configured={result.configured_replicas}, "
                    f"observed={result.observed_replica_count}, "
                    f"participating={result.participating_replica_count}, "
                    f"oauth_claims={replica_activity!r}"
                ),
            ))

            # ---- Concurrency exercised ----
            conc_ok = peak["in_progress"] >= 5
            report.assertions.append(AssertionResult(
                name="assert_concurrency_exercised(>=5 in_progress)",
                passed=conc_ok,
                detail=f"peak in_progress={peak['in_progress']}"))

            # ---- Signal backlog bounded (working signals) ----
            # The durable shard plan is the correct denominator: sources range
            # from one shard/installation to 25 in the default certification
            # kit.  At most one request/completion signal is outstanding per
            # shard plus one source-control signal per installation.
            planned_shards = int(
                await pool.fetchval("SELECT count(*) FROM onboarding_shards")
                or 0
            )
            backlog_bound = _working_signal_bound(
                planned_shards=planned_shards,
                installations=len(scenarios),
            )
            backlog_ok = peak["backlog"] <= backlog_bound
            report.assertions.append(AssertionResult(
                name=(
                    "assert_signal_backlog_bounded("
                    f"<=planned_shards+installations={backlog_bound})"
                ),
                passed=backlog_ok,
                detail=(
                    f"peak working backlog={peak['backlog']}; "
                    f"planned_shards={planned_shards}, "
                    f"installations={len(scenarios)}"
                ),
            ))

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

            # ---- #39: completion fires exactly once/installation run ----
            bad_completion = [
                o.scenario.tenant_slug for o in result.outcomes
                if o.completion_signal_count != 1
            ]
            report.assertions.append(AssertionResult(
                name="assert_completion_fires_exactly_once_per_installation(#39)",
                passed=not bad_completion,
                detail=(
                    f"all {len(scenarios)} fired once"
                    if not bad_completion
                    else f"anomalies: {bad_completion[:5]}"
                )))

            report.live_lines = [
                f"backfill-only; concurrency={concurrency}; replicas={_REPLICAS}",
                f"replica OAuth claims: {replica_activity!r}",
                f"peak simultaneous in_progress: {peak['in_progress']}",
                f"peak working signal backlog (terminal excluded): "
                f"{peak['backlog']}",
                f"completion-signal distribution: "
                f"{_distribution([o.completion_signal_count for o in result.outcomes])}",
            ]
            report.notes.append(
                f"{len(outcomes_by_tenant)} tenants / {len(scenarios)} "
                "installations across "
                f"{len(scenarios_by_source)} contract-declared historical "
                f"sources through {_REPLICAS} shared seven-service replicas "
                "(not one process set per tenant). Live phase skipped "
                "(Decision: Run 3 = backfill concurrency focus). Consumer "
                "rc=-9/-15 expected per ticket #45.")
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
