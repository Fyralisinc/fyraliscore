"""services/ingestion/workflows/feels_onboarded_monitor.py
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
from typing import Any

import asyncpg

from services.ingestion.progress.events import (
    Source,
    SourceOnboardingFeelsOnboarded,
)
from services.ingestion.progress.publisher import publish_progress_event
from services.ingestion.workflows.runtime import LongRunningService
from services.ingestion.workflows.state import (
    WorkflowState,
    load_state,
    persist_state,
)


log = logging.getLogger(__name__)


WORKFLOW_KIND = "feels_onboarded_monitor"
WORKFLOW_ID_GLOBAL = "default"  # one global instance scans all tenants

# Recency window: count observations whose occurred_at is within this
# many days of now(). Matches LLD §2.6 "last 7 days are queryable."
DEFAULT_RECENCY_WINDOW_DAYS = 7

# Minimum observations in the recency window to declare feels_onboarded.
# 1 = "any data lands"; production tuning lives in env / config.
# M6.2's reconciliation framework will replace this with the
# source-side-vs-observation-side gap measurement.
DEFAULT_MIN_OBSERVATIONS = 1


# ---------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------
# Active runs that have NOT yet fired feels_onboarded. The monitor
# scans these every tick.
_SELECT_ACTIVE_RUNS_SQL = """
SELECT id, tenant_id, sources_enabled
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


# ---------------------------------------------------------------------
# Config.
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class FeelsMonitorConfig:
    """Configuration knobs. Test injection + env-driven production."""

    tick_interval_seconds: float = 30.0
    recency_window_days: int = DEFAULT_RECENCY_WINDOW_DAYS
    min_observations_for_feels_onboarded: int = DEFAULT_MIN_OBSERVATIONS


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

    LLD §2.6's `measure_recency_gap` will replace this with a
    source-side-vs-observation-side delta when M6.2 lands. For now
    the bare count is enough to validate the substrate plumbing.
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
          3. Persist scan diagnostics.
        """
        runs_scanned = 0
        events_emitted = 0
        runs = await _load_active_runs(self._pool)
        for run in runs:
            runs_scanned += 1
            run_id = run["id"]
            tenant_id = run["tenant_id"]
            sources_enabled: list[str] = list(run["sources_enabled"])

            for source in sources_enabled:
                if source not in ("slack", "github", "discord", "gmail"):
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
                    # feels_onboarded is per-run; once stamped, the
                    # remaining sources for THIS run no longer qualify.
                    break

        await self._persist_scan_state(
            runs_scanned=runs_scanned, events_emitted=events_emitted,
        )

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


__all__ = [
    "DEFAULT_MIN_OBSERVATIONS",
    "DEFAULT_RECENCY_WINDOW_DAYS",
    "FeelsMonitorConfig",
    "FeelsOnboardedMonitor",
    "WORKFLOW_ID_GLOBAL",
    "WORKFLOW_KIND",
]
