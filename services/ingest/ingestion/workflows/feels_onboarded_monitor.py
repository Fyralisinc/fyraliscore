"""services/ingest/ingestion/workflows/feels_onboarded_monitor.py
   — The substrate's first real consumer. Per LLD §2.6.

Polls `onboarding_runs` for active runs and, per (run, source), checks
whether the source has accumulated enough recent observations to fire
`source.onboarding.feels_onboarded` on the `onboarding.progress` topic.

============================================================
WHY THIS IS THE SUBSTRATE'S FIRST CONSUMER (M6.0 Phase 2)
============================================================
Per [04-implementation-plan.md §M6.0]: the monitor proves the
substrate is usable. The substrate components exercised here:

  - `runtime.LongRunningService`     — the loop owner.
  - `state.load_state` / `persist_state` — diagnostic
                                      "last_scan_at" tracking.
  - `progress.publish_progress_event` — Kafka publish wrapper.
  - `retry.retry_with_jitter_on_5xx` (NOT exercised here directly —
    the monitor doesn't make external API calls; it only reads
    `onboarding_runs` and publishes to Kafka. Tests for retry land
    in M6.1 which makes API calls).

If the substrate's surface is wrong (e.g. `LongRunningService.tick()`
is the wrong granularity, `WorkflowState.state_data` fights the use
case), this is where we'd discover it.

============================================================
N1 vs. CLAIM-VIA-UPDATE
============================================================
Two different invariants govern Kafka publish + DB update:

  - N1 (cursor-data ordering, LLD §3.1): publish-then-flush-then-
    advance, used for cursor-style services where re-publishing on
    retry is safe (idempotent producer + downstream UNIQUE dedup).
    The substrate primitive is
    `state.advance_cursor_atomic_with_kafka_publish`.

  - Claim-via-UPDATE (LLD §2.6): UPDATE-with-WHERE-guard-then-publish,
    used for single-fire events where the UPDATE acts as a
    distributed lock claim. Concurrent monitor instances racing on
    the same (run, source) BOTH attempt the UPDATE; only one's
    `WHERE feels_onboarded_at IS NULL` succeeds; only that one
    publishes. The cost: if the publish fails after the UPDATE
    commits, the run is marked feels_onboarded but Bridge never
    sees the event. The benefit: no duplicate publishes across
    concurrent monitors.

This module uses CLAIM-VIA-UPDATE because feels_onboarded is a
single-fire-per-run event. The N1 invariant doesn't apply.

============================================================
PATTERN-ALIGNMENT MAPPING
============================================================
  Rule 1 (orchestration separated from side effects):
    `tick()` is the orchestrator. The side effects — the SELECT on
    `onboarding_runs`, the recency-gap query, the UPDATE+publish —
    are named module functions below.

  Rule 2 (state in Postgres, not memory):
    `state.persist_state` after every tick records the scan
    diagnostics. No per-process state survives SIGTERM.

  Rule 3 (retry in named functions):
    None required at this granularity. The only fallible operation
    is the publish, and that's already in the N1-vs-claim-via-UPDATE
    contract above; no retry helper changes the semantics.

  Rule 4 (signals via Postgres polling):
    The monitor is a producer-of-truth (it queries observation
    counts). It doesn't poll signals from upstream services. M6.1
    (TenantOnboarding) will use `signals.poll_signals` for the
    cross-service "source started" handoff.

  Rule 5 (no cross-workflow shared state):
    No module-level mutable state. The `_metrics` dict in M3.3 was a
    deliberate exception per amendment A4; the monitor follows
    `state_data` instead.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any, get_args

import asyncpg

from services.ingest.ingestion.progress.events import (
    Source,
    SourceOnboardingFeelsOnboarded,
    TenantOnboardingBehindSchedule,
)
from services.ingest.ingestion.progress.publisher import publish_progress_event
from services.ingest.ingestion.workflows.runtime import LongRunningService
from services.ingest.ingestion.workflows.state import (
    WorkflowState,
    load_state,
    persist_state,
)


log = logging.getLogger(__name__)


WORKFLOW_KIND = "feels_onboarded_monitor"
WORKFLOW_ID_GLOBAL = "default"  # one global instance scans all tenants

# Single source of truth for the source allowlist: the `Source` Literal
# from the progress-event contract (= every production source). Deriving
# it here rather than re-typing a tuple kills the drift class that left
# `google_drive` out of an earlier hand-maintained list while the event
# model already knew about it (same fix shape as the `google_drive`
# embedding-allowlist resolution — derive, don't duplicate).
VALID_SOURCES: frozenset[str] = frozenset(get_args(Source))

# Recency window: count observations whose occurred_at is within this
# many days of now(). Matches LLD §2.6 "last 7 days are queryable."
DEFAULT_RECENCY_WINDOW_DAYS = 7

# Minimum observations in the recency window to declare feels_onboarded.
# 1 = "any data lands"; production tuning lives in env / config.
# M6.2's reconciliation framework will replace this with the
# source-side-vs-observation-side gap measurement.
DEFAULT_MIN_OBSERVATIONS = 1

# How long after a run starts, with NO source having reached
# feels_onboarded, to emit the ops-only `tenant.onboarding.behind_schedule`
# signal (LLD §6). 15 min matches the event model's docstring.
DEFAULT_BEHIND_SCHEDULE_AFTER_SECONDS = 15 * 60


# ---------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------
# Active runs that have NOT yet fired feels_onboarded. The monitor
# scans these every tick. `started_at` + `behind_schedule_emitted_at`
# feed the ops-only behind_schedule check (migration 0080); a run with
# `started_at` NULL falls back to `created_at` for the age computation.
_SELECT_ACTIVE_RUNS_SQL = """
SELECT id, tenant_id, sources_enabled,
       COALESCE(started_at, created_at) AS started_at,
       behind_schedule_emitted_at
  FROM onboarding_runs
 WHERE status IN ('pending', 'running')
   AND feels_onboarded_at IS NULL
 ORDER BY created_at ASC
"""

# Recency gap measurement. The monitor uses a SELECT-only count;
# M6.2's reconciler will subtract source-side claimed counts.
# `source_channel LIKE $2 || ':%'` matches e.g. 'slack:T123' for
# source='slack'. Operates under the permissive RLS default from
# migration 0036: when `app.current_tenant` is unset, all rows are
# visible. The monitor is global by design (LLD §2.6).
_COUNT_RECENT_OBSERVATIONS_SQL = """
SELECT count(*) FROM observations
 WHERE tenant_id = $1
   AND source_channel LIKE $2 || ':%'
   AND occurred_at >= $3
"""

# Claim-via-UPDATE for the single feels_onboarded slot per run.
# RETURNING id distinguishes "we won the race" (returns the id) from
# "another scan already won" (returns nothing).
_CLAIM_FEELS_ONBOARDED_SQL = """
UPDATE onboarding_runs
   SET feels_onboarded_at = now()
 WHERE id = $1
   AND feels_onboarded_at IS NULL
RETURNING id
"""

# Claim-via-UPDATE for the single behind_schedule slot per run (migration
# 0080). The `feels_onboarded_at IS NULL` guard makes the claim lose the
# race against a feels_onboarded that lands first — a run that just
# became queryable is NOT behind schedule.
_CLAIM_BEHIND_SCHEDULE_SQL = """
UPDATE onboarding_runs
   SET behind_schedule_emitted_at = now()
 WHERE id = $1
   AND behind_schedule_emitted_at IS NULL
   AND feels_onboarded_at IS NULL
RETURNING id
"""

# Per-source shard progress for the run, for the behind_schedule event's
# `shard_progress` payload ({source: {done, total, in_progress}}).
_SHARD_PROGRESS_SQL = """
SELECT source, state, count(*) AS n
  FROM onboarding_shards
 WHERE onboarding_run_id = $1
 GROUP BY source, state
"""

# Sources for the run that have NOT completed — the behind_schedule
# event's `sources_pending`. Falls back to onboarding_runs.sources_enabled
# when no per-source rows exist yet (run picked up but not yet fanned out).
_PENDING_SOURCES_SQL = """
SELECT source
  FROM source_onboarding_runs
 WHERE onboarding_run_id = $1
   AND status NOT IN ('completed', 'failed')
 ORDER BY source
"""

_ALL_SOURCE_ROWS_EXIST_SQL = """
SELECT count(*) FROM source_onboarding_runs WHERE onboarding_run_id = $1
"""


# ---------------------------------------------------------------------
# Config.
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class FeelsMonitorConfig:
    """Configuration knobs. Test injection + env-driven production."""

    tick_interval_seconds: float = 30.0
    recency_window_days: int = DEFAULT_RECENCY_WINDOW_DAYS
    min_observations_for_feels_onboarded: int = DEFAULT_MIN_OBSERVATIONS
    behind_schedule_after_seconds: float = DEFAULT_BEHIND_SCHEDULE_AFTER_SECONDS


# ---------------------------------------------------------------------
# Named side-effect functions (Rule 1).
# ---------------------------------------------------------------------
async def _load_active_runs(
    pool: asyncpg.Pool,
) -> list[asyncpg.Record]:
    """Read every active run not yet feels_onboarded."""
    return await pool.fetch(_SELECT_ACTIVE_RUNS_SQL)


async def _count_recent_observations(
    pool: asyncpg.Pool,
    *,
    tenant_id: Any, source: str, window_days: int,
) -> int:
    """Count observations for (tenant, source) within `window_days`.

    TODO(M6.2): replace this placeholder with the real
    `measure_recency_gap` from LLD §2.6. M6.2's reconciliation
    framework brings per-source count APIs that subtract the
    observation-side count from the source-side claimed count
    (gap = claimed_count − observation_count). For M6.0 substrate
    proof, the bare `count >= MIN_OBSERVATIONS` heuristic exercises
    every code path; the cleanup is purely the SQL + the threshold
    name change. The `source_channel LIKE $2 || ':%'` matcher is
    the part most likely to need per-source adjustment when the
    LLD §3 channel-naming conventions land per source.
    """
    cutoff = (
        dt.datetime.now(tz=dt.timezone.utc)
        - dt.timedelta(days=window_days)
    )
    val = await pool.fetchval(
        _COUNT_RECENT_OBSERVATIONS_SQL,
        tenant_id, source, cutoff,
    )
    return int(val or 0)


async def _claim_and_publish_feels_onboarded(
    pool: asyncpg.Pool,
    kafka_producer: Any,
    *,
    run_id: Any, tenant_id: Any, source: Source,
    observations_count: int, recency_window_days: int,
) -> bool:
    """Claim-via-UPDATE then publish. Returns True iff this caller won
    the race (and therefore published the event). Concurrent monitor
    instances racing on the same `run_id` all attempt the UPDATE;
    only one's `WHERE feels_onboarded_at IS NULL` matches.

    Per LLD §2.6: "Only if the UPDATE affected 1 row, publish the
    `source.onboarding.feels_onboarded` event to Kafka."

    Race-trade-off: if the UPDATE commits but the publish raises
    (Kafka outage), the run is stamped feels_onboarded but Bridge
    never sees the event. Operator reconciliation can re-emit by
    inspecting `onboarding_runs.feels_onboarded_at IS NOT NULL` and
    a missing Bridge-side record; that's an M6.2 concern.
    """
    claimed_id = await pool.fetchval(_CLAIM_FEELS_ONBOARDED_SQL, run_id)
    if claimed_id is None:
        return False
    event = SourceOnboardingFeelsOnboarded(
        tenant_id=tenant_id,
        source=source,
        observations_count=observations_count,
        recency_window_days=recency_window_days,
    )
    await publish_progress_event(kafka_producer, event)
    return True


async def _build_shard_progress(
    pool: asyncpg.Pool, run_id: Any,
) -> dict[str, dict[str, int]]:
    """{source: {done, total, in_progress}} from `onboarding_shards`.

    `total` is the count across ALL states for the source; `done` and
    `in_progress` are the two states operators care about when a run is
    flagged behind schedule. Missing keys default to 0 in the consumer."""
    rows = await pool.fetch(_SHARD_PROGRESS_SQL, run_id)
    progress: dict[str, dict[str, int]] = {}
    for row in rows:
        source = row["source"]
        n = int(row["n"])
        bucket = progress.setdefault(
            source, {"done": 0, "total": 0, "in_progress": 0},
        )
        bucket["total"] += n
        if row["state"] in ("done", "in_progress"):
            bucket[row["state"]] += n
    return progress


async def _pending_sources(
    pool: asyncpg.Pool, run_id: Any, sources_enabled: list[str],
) -> list[str]:
    """Sources not yet completed for the run. Falls back to
    `sources_enabled` when fan-out hasn't created per-source rows yet."""
    have_rows = int(await pool.fetchval(_ALL_SOURCE_ROWS_EXIST_SQL, run_id) or 0)
    if have_rows == 0:
        return list(sources_enabled)
    rows = await pool.fetch(_PENDING_SOURCES_SQL, run_id)
    return [r["source"] for r in rows]


async def _claim_and_publish_behind_schedule(
    pool: asyncpg.Pool,
    kafka_producer: Any,
    *,
    run_id: Any, tenant_id: Any, sources_enabled: list[str],
) -> bool:
    """Claim-via-UPDATE the per-run behind_schedule slot then publish.

    Returns True iff this caller won the claim (and published). The
    `_CLAIM_BEHIND_SCHEDULE_SQL` guard (`behind_schedule_emitted_at IS
    NULL AND feels_onboarded_at IS NULL`) makes this single-fire per run
    AND race-safe against a feels_onboarded that lands first. Same
    claim-via-UPDATE trade-off as feels_onboarded: a publish that fails
    after the UPDATE commits drops an ops signal but never double-fires.
    """
    claimed_id = await pool.fetchval(_CLAIM_BEHIND_SCHEDULE_SQL, run_id)
    if claimed_id is None:
        return False
    event = TenantOnboardingBehindSchedule(
        tenant_id=tenant_id,
        sources_pending=await _pending_sources(pool, run_id, sources_enabled),
        shard_progress=await _build_shard_progress(pool, run_id),
    )
    await publish_progress_event(kafka_producer, event)
    return True


# ---------------------------------------------------------------------
# Service.
# ---------------------------------------------------------------------
class FeelsOnboardedMonitor(LongRunningService):
    """LongRunningService scanning `onboarding_runs` for the
    feels_onboarded threshold.

    Constructor takes the pool + producer rather than DSNs/configs so
    the test surface stays small (pass a fake producer + fresh_db
    fixture). The `__main__.py` CLI entrypoint owns DSN-to-pool
    bootstrapping.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        kafka_producer: Any,
        *,
        config: FeelsMonitorConfig | None = None,
        workflow_id: str = WORKFLOW_ID_GLOBAL,
    ) -> None:
        self._pool = pool
        self._kafka_producer = kafka_producer
        self._config = config or FeelsMonitorConfig()
        self._workflow_id = workflow_id

    @property
    def tick_interval_seconds(self) -> float:
        return self._config.tick_interval_seconds

    async def tick(self) -> None:
        """One scan pass. Idempotent under SIGTERM-restart.

        Algorithm:
          1. Load every active run not yet feels_onboarded.
          2. For each (run, source) in run.sources_enabled:
             a. Measure recency-window observation count.
             b. If count >= threshold, claim-via-UPDATE then publish.
             c. If claim won: stop iterating sources for THIS run
                (feels_onboarded is per-run; the LLD §2.6 schema has
                one `feels_onboarded_at` column).
          3. If feels_onboarded did NOT fire for a run AND it has been
             `behind_schedule_after_seconds` since it started, claim +
             publish the ops-only `tenant.onboarding.behind_schedule`.
          4. Persist scan diagnostics.
        """
        runs_scanned = 0
        events_emitted = 0
        now = dt.datetime.now(tz=dt.timezone.utc)
        runs = await _load_active_runs(self._pool)
        for run in runs:
            runs_scanned += 1
            run_id = run["id"]
            tenant_id = run["tenant_id"]
            sources_enabled: list[str] = list(run["sources_enabled"])

            feels_fired = False
            for source in sources_enabled:
                if source not in VALID_SOURCES:
                    # Defensive: the migration's CHECK constraint
                    # should prevent unknown sources, but skip
                    # rather than crash if one leaks in.
                    continue
                count = await _count_recent_observations(
                    self._pool,
                    tenant_id=tenant_id,
                    source=source,
                    window_days=self._config.recency_window_days,
                )
                if count < self._config.min_observations_for_feels_onboarded:
                    continue
                won = await _claim_and_publish_feels_onboarded(
                    self._pool, self._kafka_producer,
                    run_id=run_id, tenant_id=tenant_id,
                    source=source,  # type: ignore[arg-type]
                    observations_count=count,
                    recency_window_days=self._config.recency_window_days,
                )
                if won:
                    events_emitted += 1
                    feels_fired = True
                    # feels_onboarded is per-run; once stamped, the
                    # remaining sources for THIS run no longer qualify.
                    break

            # behind_schedule: ops-only, fires once per run when it's been
            # too long with no feels_onboarded. Skip runs that just fired
            # feels_onboarded (this tick) or already emitted behind_schedule.
            if (
                not feels_fired
                and run["behind_schedule_emitted_at"] is None
                and self._is_behind_schedule(run["started_at"], now)
            ):
                won = await _claim_and_publish_behind_schedule(
                    self._pool, self._kafka_producer,
                    run_id=run_id, tenant_id=tenant_id,
                    sources_enabled=sources_enabled,
                )
                if won:
                    events_emitted += 1

        await self._persist_scan_state(
            runs_scanned=runs_scanned, events_emitted=events_emitted,
        )

    def _is_behind_schedule(
        self, started_at: dt.datetime | None, now: dt.datetime,
    ) -> bool:
        """True if `started_at` is older than the behind_schedule
        threshold. `started_at` is COALESCE(started_at, created_at) from
        the scan query, so it's never NULL for a real row; guard anyway."""
        if started_at is None:
            return False
        age = (now - started_at).total_seconds()
        return age >= self._config.behind_schedule_after_seconds

    async def _persist_scan_state(
        self, *, runs_scanned: int, events_emitted: int,
    ) -> None:
        """Record diagnostic state. Not load-bearing for correctness;
        useful for operator queries against `workflow_states`."""
        existing = await load_state(
            self._pool, WORKFLOW_KIND, self._workflow_id,
        )
        state = WorkflowState(
            workflow_kind=WORKFLOW_KIND,
            workflow_id=self._workflow_id,
            tenant_id=None,  # global service; not tenant-scoped
            state_data={
                "last_scan_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
                "last_runs_scanned": runs_scanned,
                "last_events_emitted": events_emitted,
                "lifetime_events_emitted": (
                    (existing.state_data.get("lifetime_events_emitted", 0)
                     if existing else 0)
                    + events_emitted
                ),
            },
            last_advanced_at=dt.datetime.now(tz=dt.timezone.utc),
        )
        await persist_state(self._pool, state)


# ---------------------------------------------------------------------
# CLI entrypoint — `python -m services.ingest.ingestion.workflows.feels_onboarded_monitor`.
# ---------------------------------------------------------------------
# Per-module entrypoint, sibling to oauth_poller / tenant_onboarding /
# source_onboarding / shard_fetch / reconciler. This is the form every
# other M6 workflow service is launched by in docker-compose
# (`python -m …workflows.<module>`); the shared `workflows/__main__.py`
# `WORKFLOW_SERVICE` selector predates that convention and still works,
# but compose targets THIS module directly. ENV:
#   DATABASE_URL                    — Postgres DSN (required).
#   KAFKA_BOOTSTRAP_SERVERS         — Kafka bootstrap (default localhost:9092).
#   FEELS_MONITOR_TICK_SEC          — tick interval (default 30.0).
#   FEELS_MONITOR_RECENCY_DAYS      — recency window (default 7).
#   FEELS_MONITOR_MIN_OBS           — observations threshold (default 1).
#   FEELS_MONITOR_BEHIND_SCHEDULE_SEC — behind_schedule delay (default 900).
#   WORKFLOWS_LOG_LEVEL             — log level (default INFO).
def build_config_from_env() -> FeelsMonitorConfig:
    """Build a `FeelsMonitorConfig` from the FEELS_MONITOR_* env vars.

    Shared by this module's `_run_service` and the legacy
    `workflows/__main__.py` selector so the two entrypoints can't drift
    on which knobs they honour."""
    import os
    return FeelsMonitorConfig(
        tick_interval_seconds=float(
            os.environ.get("FEELS_MONITOR_TICK_SEC", "30.0"),
        ),
        recency_window_days=int(
            os.environ.get("FEELS_MONITOR_RECENCY_DAYS", "7"),
        ),
        min_observations_for_feels_onboarded=int(
            os.environ.get("FEELS_MONITOR_MIN_OBS", "1"),
        ),
        behind_schedule_after_seconds=float(
            os.environ.get(
                "FEELS_MONITOR_BEHIND_SCHEDULE_SEC",
                str(DEFAULT_BEHIND_SCHEDULE_AFTER_SECONDS),
            ),
        ),
    )


async def _run_service() -> None:
    import asyncio
    import os
    import signal as sig_module

    from services.ingest.ingestion.kafka.producer import (
        IdempotentProducer,
        ProducerConfig,
    )
    from services.ingest.ingestion.workflows.runtime import (
        make_workflow_pool,
        start_workflow_health,
    )

    pool = await make_workflow_pool(os.environ["DATABASE_URL"])
    producer = IdempotentProducer(ProducerConfig(
        bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
        ),
        client_id="workflow-feels_onboarded_monitor",
    ))
    await producer.start()

    service = FeelsOnboardedMonitor(
        pool, producer, config=build_config_from_env(),
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for s in (sig_module.SIGTERM, sig_module.SIGINT):
        loop.add_signal_handler(s, stop_event.set)

    log.info("workflow.feels_onboarded_monitor.started")
    # Liveness + metrics surface (opt-in via INGESTION_HEALTH_PORT).
    health_shutdown = start_workflow_health(stop_event)
    try:
        await service.run(stop_event=stop_event)
    finally:
        log.info("workflow.feels_onboarded_monitor.shutting_down")
        await health_shutdown()
        await producer.stop()
        await pool.close()
    log.info("workflow.feels_onboarded_monitor.exited")


def main() -> None:
    import asyncio
    import os
    logging.basicConfig(
        level=os.environ.get("WORKFLOWS_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_run_service())


if __name__ == "__main__":
    main()


__all__ = [
    "DEFAULT_BEHIND_SCHEDULE_AFTER_SECONDS",
    "DEFAULT_MIN_OBSERVATIONS",
    "DEFAULT_RECENCY_WINDOW_DAYS",
    "FeelsMonitorConfig",
    "FeelsOnboardedMonitor",
    "VALID_SOURCES",
    "WORKFLOW_ID_GLOBAL",
    "WORKFLOW_KIND",
    "build_config_from_env",
    "main",
]
