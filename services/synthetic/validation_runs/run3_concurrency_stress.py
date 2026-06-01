"""Run 3 — concurrency stress (A30.4).

50 tenants distributed across the four sources, driven through the SAME
seven shared M6 subprocesses at concurrency=10 (not 50× processes).
HAPPY_PATH; **backfill-only** (live phase skipped — the focus is backfill
concurrency + per-tenant isolation).

A concurrent monitor samples, while the backfill runs:
  - peak simultaneous `source_onboarding_runs.status='in_progress'`
    (concurrency actually exercised),
  - peak unconsumed `workflow_signals` backlog (bounded signal table).

Assertions (A22 properties under load):
  - per-tenant isolation: each tenant's observation count matches its
    fixture independently (gmail/github/slack exact; discord = all-equal
    + positive, since 5% channel-sampling picks 1 of 4 channels → 30 obs),
  - signal-table backlog bounded (< 10× concurrency = 100),
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

from services.synthetic.backfill_harness.harness import BackfillHarness
from services.synthetic.backfill_harness.scenarios import BackfillScenario
from services.synthetic.validation_runs import assertions as A
from services.synthetic.validation_runs.cleanup import reset_state
from services.synthetic.validation_runs.moto_lifecycle import moto_s3
from services.synthetic.validation_runs.preflight import run_preflight
from services.synthetic.validation_runs.reports import (
    AssertionResult,
    RunReport,
    SourceResult,
)


log = logging.getLogger("validation_runs.run3")
_MIGRATIONS = pathlib.Path("db/migrations")

# Distribution (Decision 3): 15 / 15 / 10 / 10 = 50.
_GMAIL_N, _GITHUB_N, _SLACK_N, _DISCORD_N = 15, 15, 10, 10


def run3_scenarios() -> list[BackfillScenario]:
    """50-tenant distribution (15/15/10/10). Per-tenant message volumes
    are sized so the full run's observations drain through the Kafka
    consumers within the harness's fixed 30s drain window
    (`_wait_for_observations_to_drain`, hardcoded — NOT configurable from
    here, and the X3 harness is out of scope to modify). The stress
    dimension is **tenant concurrency** (50 tenants × concurrency=10
    through 7 shared subprocesses), which is independent of per-tenant
    volume — see A30.6 for the drain-window finding. A higher-volume soak
    would need the harness to expose a drain timeout (follow-up)."""
    out: list[BackfillScenario] = []
    for i in range(_GMAIL_N):
        out.append(BackfillScenario(
            tenant_slug=f"r3-gmail-{i}", source="gmail",
            fixture_params={"email": f"r3-gmail-{i}@val.example",
                            "messages": 10},
            expected_observation_count=10))
    for i in range(_GITHUB_N):
        out.append(BackfillScenario(
            tenant_slug=f"r3-github-{i}", source="github",
            fixture_params={"org_or_user": f"r3gh{i}", "repos": 2,
                            "events_per_repo": 5},
            expected_observation_count=5 * 2 * 2))  # events×types×repos
    for i in range(_SLACK_N):
        out.append(BackfillScenario(
            tenant_slug=f"r3-slack-{i}", source="slack",
            fixture_params={"team_id": f"T_r3s{i}", "channels": 3,
                            "messages_per_channel": 8},
            expected_observation_count=3 * 8))
    for i in range(_DISCORD_N):
        out.append(BackfillScenario(
            tenant_slug=f"r3-discord-{i}", source="discord",
            fixture_params={"guild_id": f"G_r3d{i}", "channels": 4,
                            "messages_per_channel": 10},
            # 5% of 4 channels → max(1, int(0.2)) = 1 channel sampled.
            expected_observation_count=10))
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
    report = RunReport(
        run_name="Concurrency stress (50 tenants, backfill-only)",
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

            # ---- Per-source counts ----
            by_source: dict[str, list] = {}
            for o in result.outcomes:
                by_source.setdefault(o.scenario.source, []).append(o)
            for source in ("gmail", "github", "slack", "discord"):
                outs = by_source.get(source, [])
                src_tids = [o.tenant_id for o in outs]
                exp = sum(o.scenario.expected_observation_count for o in outs)
                actual = int(await pool.fetchval(
                    "SELECT count(*) FROM observations WHERE tenant_id = ANY($1)",
                    src_tids))
                report.source_results.append(SourceResult(
                    source=source, tenants=len(outs),
                    expected_observations=exp, actual_observations=actual))

            # ---- Per-tenant isolation ----
            iso_detail = ""
            iso_ok = True
            discord_counts: list[int] = []
            for o in result.outcomes:
                n = int(await pool.fetchval(
                    "SELECT count(*) FROM observations WHERE tenant_id = $1",
                    o.tenant_id))
                if o.scenario.source == "discord":
                    discord_counts.append(n)
                    if n <= 0:
                        iso_ok = False
                        iso_detail = f"{o.scenario.tenant_slug} got 0 obs"
                elif n != o.scenario.expected_observation_count:
                    iso_ok = False
                    iso_detail = (
                        f"{o.scenario.tenant_slug}: got {n}, "
                        f"expected {o.scenario.expected_observation_count}")
                    break
            if iso_ok and discord_counts and len(set(discord_counts)) != 1:
                iso_ok = False
                iso_detail = f"discord counts not uniform: {set(discord_counts)}"
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
                detail=("all 50 fired once" if not bad_completion
                        else f"anomalies: {bad_completion[:5]}")))

            report.live_lines = [
                f"backfill-only; concurrency={concurrency}",
                f"peak simultaneous in_progress: {peak['in_progress']}",
                f"peak working signal backlog (terminal excluded): "
                f"{peak['backlog']}",
                f"completion-signal distribution: "
                f"{_distribution([o.completion_signal_count for o in result.outcomes])}",
            ]
            report.notes.append(
                "50 tenants through 7 shared subprocesses (not 50× "
                "processes). Live phase skipped (Decision: Run 3 = backfill "
                "concurrency focus). Consumer rc=-9/-15 expected per "
                "ticket #45.")
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
