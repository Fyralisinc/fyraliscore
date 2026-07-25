"""services/ingest/ingestion/workflows/periodic_reconciler.py
   — Periodic re-reconciliation for live tenants.

============================================================
WHY THIS EXISTS
============================================================
The M6.2b `Reconciler` (reconciler.py) runs each source's
gap-detection algorithm exactly ONCE — at end-of-backfill, driven by
the `source_shards_completed` signal. After that one pass stamps
`reconciled_at`, the (run, source) is never re-checked.

For steady-state completeness that one-shot is not enough. github /
slack / discord have no durable live watermark (unlike Gmail's
`history_id` + poller): if a provider webhook or the gateway misses
an event AFTER onboarding completes, nothing re-fetches it. The
operator runbook flagged this as the headline steady-state gap.

This service closes it. On a schedule it re-runs the SAME per-source
reconciler resolved from SourceDefinition for already-reconciled
runs and, when it finds new activity past the last-seen cursor,
re-shares exactly as the at-completion path does — re-using
`reconciler.apply_reshare` so the two services share one re-share
implementation and diverge only on *when* a check fires.

============================================================
WHY IT ALSO HARDENS THE TRANSIENT-ERROR GAP
============================================================
Every per-source reconciler treats a transient gap-check error
(rate limit, blip, expired-then-refreshed token) as "no gap"
(returns None) — best-effort by design. With only the one-shot pass
that meant a transient error at completion = a permanently missed
gap. With this periodic loop, an indeterminate check simply advances
`last_reconcile_check_at` and is retried on the next cycle. The
best-effort behaviour becomes self-healing instead of lossy.

============================================================
ELIGIBILITY + RATE CONTROL
============================================================
A run is eligible when `status='completed' AND reconciled_at IS NOT
NULL` (settled, not mid-reshare) and its `last_reconcile_check_at`
is older than `min_age` (default 6h) — NULL sorts first so freshly
reconciled runs enter the rotation immediately. Each tick claims up
to `batch_size` eligible runs `FOR UPDATE SKIP LOCKED` (one
transaction per run, so a slow source API can't hold locks on the
rest, and multiple instances cooperate), stamps the watermark, runs
the bounded gap check, and re-shares on a positive decision. A
re-share flips `status` back to 'in_progress', which removes the run
from the eligibility set until its reshare cycle completes — so a run
cannot be re-opened twice concurrently.

============================================================
PATTERN ALIGNMENT
============================================================
Mirrors reconciler.py: orchestration in the class, all DB I/O in
module-level `_*` functions (Rule 1), state.py import (Rule 2), no
inline retry loops (Rule 3), no in-process queues (Rule 4),
`_metrics` is the only module-level mutable (Rule 5 allowlist).
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
from dataclasses import dataclass

import asyncpg

from lib.shared.provider_transport import RetryLater
from services.ingest.ingestion.observability import (
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)
from services.ingest.ingestion.workflows.reconciler import (
    RECONCILER_DISPATCH_TIMEOUT_S,
    _load_shards,
    apply_reshare,
    reconciliation_timeout_retry,
    schedule_reconciliation_retry,
)
from services.ingest.source_contract.runtime import resolve_reconciler
from services.ingest.ingestion.workflows.runtime import LongRunningService
from services.ingest.ingestion.workflows.state import (
    WorkflowState,
    load_state,
    persist_state,
)


log = logging.getLogger(__name__)


WORKFLOW_KIND = "periodic_reconciler"
WORKFLOW_ID_DEFAULT = "default"

DEFAULT_TICK_INTERVAL_SECONDS = 300.0   # wake every 5 min
DEFAULT_MIN_AGE_SECONDS = 6 * 3600.0    # re-check a given run at most ~every 6h
DEFAULT_BATCH_SIZE = 20                  # runs checked per tick


# In-process observability (Rule 5 allowlist: name ends with `_metrics`).
_metrics: dict[str, float] = {
    "periodic_reconciler.runs_checked":     0.0,
    "periodic_reconciler.gaps_found":       0.0,
    "periodic_reconciler.shards_resharded": 0.0,
    "periodic_reconciler.check_errors":     0.0,
    "periodic_reconciler.retries_scheduled": 0.0,
    "periodic_reconciler.last_tick_checked": 0.0,
}


def get_metrics() -> dict[str, float]:
    return dict(_metrics)


def reset_metrics() -> None:
    for k in _metrics:
        _metrics[k] = 0.0


def _bump(key: str, by: float = 1.0) -> None:
    _metrics[key] = _metrics.get(key, 0.0) + by


# ---------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------
# Snapshot eligible generations once per tick.  A retry scheduled during this
# tick cannot immediately re-enter the same batch, and attempt_count fences two
# workers that snapshotted the same due retry generation.
_LIST_ELIGIBLE_RUN_CANDIDATES_SQL = """
WITH eligible AS MATERIALIZED (
    SELECT run.onboarding_run_id,
           run.source,
           run.tenant_id,
           run.installation_row_id,
           run.reconcile_attempt_count,
           run.reconcile_next_attempt_at,
           run.last_reconcile_check_at,
           CASE
               WHEN run.reconcile_next_attempt_at IS NOT NULL THEN 0
               ELSE 1
           END AS scheduling_lane,
           COALESCE(
               run.installation_row_id::text,
               run.onboarding_run_id::text
           ) AS installation_key,
           tenant_history.last_served_at AS tenant_last_served_at,
           installation_history.last_served_at
               AS installation_last_served_at,
           row_number() OVER (
               PARTITION BY
                   CASE
                       WHEN run.reconcile_next_attempt_at IS NOT NULL THEN 0
                       ELSE 1
                   END,
                   run.tenant_id,
                   COALESCE(
                       run.installation_row_id::text,
                       run.onboarding_run_id::text
                   )
               ORDER BY run.reconcile_next_attempt_at NULLS LAST,
                        run.last_reconcile_check_at NULLS FIRST,
                        run.onboarding_run_id,
                        run.source
           ) AS installation_turn
      FROM source_onboarding_runs run
      LEFT JOIN LATERAL (
          SELECT max(prior.reconcile_last_claimed_at) AS last_served_at
            FROM source_onboarding_runs prior
           WHERE prior.tenant_id = run.tenant_id
      ) tenant_history ON TRUE
      LEFT JOIN LATERAL (
          SELECT max(prior.reconcile_last_claimed_at) AS last_served_at
            FROM source_onboarding_runs prior
           WHERE prior.tenant_id = run.tenant_id
             AND (
                  (
                    run.installation_row_id IS NOT NULL
                    AND prior.installation_row_id = run.installation_row_id
                  )
                  OR (
                    run.installation_row_id IS NULL
                    AND prior.installation_row_id IS NULL
                    AND prior.onboarding_run_id = run.onboarding_run_id
                    AND prior.source = run.source
                  )
             )
      ) installation_history ON TRUE
     WHERE run.status = 'completed'
       AND run.reconciled_at IS NOT NULL
       AND (
            (
              run.reconcile_next_attempt_at IS NOT NULL
              AND run.reconcile_next_attempt_at <= now()
            )
            OR (
              run.reconcile_next_attempt_at IS NULL
              AND (
                run.last_reconcile_check_at IS NULL
                OR run.last_reconcile_check_at < $1
              )
            )
       )
),
tenant_ranked AS MATERIALIZED (
    SELECT eligible.*,
           row_number() OVER (
               PARTITION BY scheduling_lane, tenant_id
               ORDER BY installation_turn,
                        installation_last_served_at NULLS FIRST,
                        installation_key,
                        reconcile_next_attempt_at NULLS LAST,
                        last_reconcile_check_at NULLS FIRST,
                        onboarding_run_id,
                        source
           ) AS tenant_turn
      FROM eligible
),
lane_ranked AS MATERIALIZED (
    SELECT tenant_ranked.*,
           row_number() OVER (
               PARTITION BY scheduling_lane
               ORDER BY tenant_turn,
                        tenant_last_served_at NULLS FIRST,
                        tenant_id,
                        installation_turn,
                        installation_last_served_at NULLS FIRST,
                        installation_key,
                        reconcile_next_attempt_at NULLS LAST,
                        last_reconcile_check_at NULLS FIRST,
                        onboarding_run_id,
                        source
           ) AS lane_turn
      FROM tenant_ranked
)
SELECT onboarding_run_id, source, reconcile_attempt_count
  FROM lane_ranked
 ORDER BY lane_turn,
          scheduling_lane,
          tenant_turn,
          tenant_last_served_at NULLS FIRST,
          tenant_id,
          installation_turn,
          installation_last_served_at NULLS FIRST,
          installation_key,
          reconcile_next_attempt_at NULLS LAST,
          last_reconcile_check_at NULLS FIRST,
          onboarding_run_id,
          source
 LIMIT $2
"""

# Claim one still-eligible generation. FOR UPDATE SKIP LOCKED makes multiple
# instances cooperate without waiting or double-dispatching a provider call.
_CLAIM_ELIGIBLE_RUN_SQL = """
WITH claimed AS MATERIALIZED (
    SELECT onboarding_run_id, source
      FROM source_onboarding_runs
     WHERE onboarding_run_id = $1
       AND source = $2
       AND reconcile_attempt_count = $3
       AND status = 'completed'
       AND reconciled_at IS NOT NULL
       AND (
            (
              reconcile_next_attempt_at IS NOT NULL
              AND reconcile_next_attempt_at <= now()
            )
            OR (
              reconcile_next_attempt_at IS NULL
              AND (
                last_reconcile_check_at IS NULL
                OR last_reconcile_check_at < $4
              )
            )
       )
     FOR UPDATE SKIP LOCKED
)
UPDATE source_onboarding_runs run
   SET reconcile_last_claimed_at = now()
  FROM claimed
 WHERE run.onboarding_run_id = claimed.onboarding_run_id
   AND run.source = claimed.source
RETURNING run.onboarding_run_id, run.source, run.tenant_id, run.status,
          run.installation_row_id, run.reconciled_at,
          run.reconciliation_pass_count, run.started_at,
          run.reconcile_next_attempt_at, run.reconcile_attempt_count,
          run.reconcile_retry_reason, run.reconcile_retry_operation,
          run.reconcile_last_claimed_at
"""

# Advance the normal periodic watermark and clear a completed retry schedule.
# Stamped on clean, gap, or ordinary indeterminate checks. Timeout and explicit
# RetryLater take the separate durable not-before path instead.
_STAMP_CHECK_SQL = """
UPDATE source_onboarding_runs
   SET last_reconcile_check_at = now(),
       reconcile_next_attempt_at = NULL,
       reconcile_retry_reason = NULL,
       reconcile_retry_operation = NULL
 WHERE onboarding_run_id = $1 AND source = $2
"""


# ---------------------------------------------------------------------
# Named side-effect functions (Rule 1).
# ---------------------------------------------------------------------
async def _list_eligible_run_candidates(
    pool: asyncpg.Pool,
    *,
    cutoff: dt.datetime,
    limit: int,
) -> list[asyncpg.Record]:
    if limit <= 0:
        return []
    return list(
        await pool.fetch(
            _LIST_ELIGIBLE_RUN_CANDIDATES_SQL,
            cutoff,
            limit,
        )
    )


async def _claim_eligible_run(
    conn: asyncpg.Connection,
    *,
    run_id,
    source: str,
    expected_attempt_count: int,
    cutoff: dt.datetime,
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        _CLAIM_ELIGIBLE_RUN_SQL,
        run_id,
        source,
        expected_attempt_count,
        cutoff,
    )


async def _stamp_check(
    conn: asyncpg.Connection, *, run_id, source: str,
) -> None:
    await conn.execute(_STAMP_CHECK_SQL, run_id, source)


# ---------------------------------------------------------------------
# Config.
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class PeriodicReconcilerConfig:
    """Knobs. Test injection + env-driven production."""

    tick_interval_seconds: float = DEFAULT_TICK_INTERVAL_SECONDS
    min_age_seconds: float = DEFAULT_MIN_AGE_SECONDS
    batch_size: int = DEFAULT_BATCH_SIZE
    dispatch_timeout_seconds: float = RECONCILER_DISPATCH_TIMEOUT_S
    instance_name: str = WORKFLOW_ID_DEFAULT


# ---------------------------------------------------------------------
# Service.
# ---------------------------------------------------------------------
class PeriodicReconciler(LongRunningService):
    """Re-runs per-source gap detection for settled runs on a schedule.

    One transaction per claimed run: claim (SKIP LOCKED) → stamp
    watermark → bounded gap check → re-share on a positive decision.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        config: PeriodicReconcilerConfig | None = None,
    ) -> None:
        self._pool = pool
        self._config = config or PeriodicReconcilerConfig()

    @property
    def tick_interval_seconds(self) -> float:
        return self._config.tick_interval_seconds

    async def tick(self) -> None:
        """Check one snapshotted, bounded generation of each due run."""
        cutoff = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(
            seconds=self._config.min_age_seconds,
        )
        candidates = await _list_eligible_run_candidates(
            self._pool,
            cutoff=cutoff,
            limit=self._config.batch_size,
        )
        checked = 0
        for candidate in candidates:
            processed = await self._check_one_run(
                candidate,
                cutoff=cutoff,
            )
            if processed:
                checked += 1
        _metrics["periodic_reconciler.last_tick_checked"] = float(checked)
        await self._persist_scan_state(checked=checked)

    async def _check_one_run(
        self,
        candidate: asyncpg.Record,
        *,
        cutoff: dt.datetime,
    ) -> bool:
        """Claim + re-reconcile one eligible run. Returns True iff a
        run was claimed (False = nothing due → stop this tick)."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                run = await _claim_eligible_run(
                    conn,
                    run_id=candidate["onboarding_run_id"],
                    source=candidate["source"],
                    expected_attempt_count=int(
                        candidate["reconcile_attempt_count"],
                    ),
                    cutoff=cutoff,
                )
                if run is None:
                    return False
                run_id = run["onboarding_run_id"]
                source = run["source"]
                _bump("periodic_reconciler.runs_checked")

                shards = await _load_shards(
                    conn, run_id=run_id, source=source,
                )
                try:
                    decision = await self._gap_check(source, shards, run)
                except RetryLater as exc:
                    attempt_count = await schedule_reconciliation_retry(
                        conn,
                        run=run,
                        retry=exc,
                    )
                    _bump("periodic_reconciler.retries_scheduled")
                    log.info(
                        "periodic_reconciler.retry_scheduled",
                        extra={
                            "source": source,
                            "run_id": str(run_id),
                            "tenant_id": str(run["tenant_id"]),
                            "installation_row_id": str(
                                run["installation_row_id"],
                            ),
                            "next_attempt_at": exc.not_before.isoformat(),
                            "retry_reason": exc.reason.value,
                            "retry_operation": (
                                exc.request_context.operation
                            ),
                            "attempt_count": attempt_count,
                            "blocked_scope": exc.blocked_scope,
                        },
                    )
                    return True

                # An ordinary result (including a swallowed permanent error)
                # completes this check generation and returns to min-age
                # scheduling. RetryLater deliberately bypasses this watermark.
                await _stamp_check(conn, run_id=run_id, source=source)
                if decision is not None and decision.has_gaps:
                    await apply_reshare(
                        conn, run_id=run_id, source=source,
                        tenant_id=run["tenant_id"],
                        installation_row_id=run["installation_row_id"],
                        decision=decision,
                    )
                    _bump("periodic_reconciler.gaps_found")
                    _bump(
                        "periodic_reconciler.shards_resharded",
                        float(len(decision.new_shards)),
                    )
                    log.info(
                        "periodic_reconciler.reshared",
                        extra={
                            "run_id": str(run_id), "source": source,
                            "new_shards": len(decision.new_shards),
                        },
                    )
        return True

    async def _gap_check(self, source, shards, run):
        """Run the bounded per-source gap check. A timeout or any
        permanent dispatch error is logged and swallowed (decision=None) — the
        caller advances the watermark, so it re-checks next cycle. Timeout and
        RetryLater are control flow and propagate to the durable scheduler."""
        try:
            reconciler = resolve_reconciler(source)
            return await asyncio.wait_for(
                reconciler(shards, run),
                timeout=self._config.dispatch_timeout_seconds,
            )
        except asyncio.TimeoutError:
            _bump("periodic_reconciler.check_errors")
            log.warning(
                "periodic_reconciler.dispatch_timeout_retry_scheduled",
                extra={"source": source,
                       "timeout_s": self._config.dispatch_timeout_seconds},
            )
            raise reconciliation_timeout_retry(
                run,
                source=source,
                timeout_seconds=self._config.dispatch_timeout_seconds,
            )
        except RetryLater:
            raise
        except Exception as exc:  # noqa: BLE001 — best-effort gap check
            _bump("periodic_reconciler.check_errors")
            log.warning(
                "periodic_reconciler.dispatch_failed",
                extra={"source": source, "error": str(exc)[:200]},
            )
            return None

    async def _persist_scan_state(self, *, checked: int) -> None:
        """Diagnostic state row. Not load-bearing for correctness."""
        existing = await load_state(
            self._pool, WORKFLOW_KIND, self._config.instance_name,
        )
        now = dt.datetime.now(tz=dt.timezone.utc)
        state = WorkflowState(
            workflow_kind=WORKFLOW_KIND,
            workflow_id=self._config.instance_name,
            tenant_id=None,
            state_data={
                "last_tick_at": now.isoformat(),
                "last_runs_checked": checked,
                "lifetime_runs_checked": (
                    (existing.state_data.get("lifetime_runs_checked", 0)
                     if existing else 0)
                    + checked
                ),
            },
            last_advanced_at=now,
        )
        await persist_state(self._pool, state)


# ---------------------------------------------------------------------
# CLI entrypoint — `python -m services.ingest.ingestion.workflows.periodic_reconciler`.
# ---------------------------------------------------------------------
# ENV:
#   DATABASE_URL                        — Postgres DSN (required).
#   PERIODIC_RECONCILE_TICK_SEC         — loop interval (default 300).
#   PERIODIC_RECONCILE_MIN_AGE_SEC      — min seconds between re-checks
#                                         of one run (default 21600 = 6h).
#   PERIODIC_RECONCILE_BATCH            — runs per tick (default 20).
#   PERIODIC_RECONCILE_DISPATCH_TIMEOUT_SEC — per-source check timeout
#                                         (default = reconciler's 30s).
#   PERIODIC_RECONCILE_INSTANCE         — instance name for diagnostics.
#   INGESTION_HEALTH_PORT               — /healthz + /metrics (opt-in).
#   WORKFLOWS_LOG_LEVEL                 — log level (default INFO).
async def _run_service() -> None:
    import signal as sig_module

    from services.ingest.ingestion.workflows.runtime import make_workflow_pool

    pool = await make_workflow_pool(os.environ["DATABASE_URL"])
    # Per-source reconcilers need pool access for auxiliary reads (shard
    # cursors, installation rows) and raise if their pool isn't registered.
    # Register ALL historical sources (derived from SourceDefinition) — the same
    # registration the at-completion Reconciler does. This block previously
    # listed only 7 of 25 sources by hand, so every steady-state gap re-check
    # of the other 18 raised RuntimeError (silently swallowed as a dispatch
    # exception) — permanently disabling periodic gap detection for them. The
    # shared helper keeps the two services in lockstep so the drift can't recur.
    from services.ingest.ingestion.reconcilers import register_pool_provider

    registered = register_pool_provider(pool)
    log.info(
        "periodic_reconciler.pool_providers_registered",
        extra={"source_count": len(registered)},
    )

    config = PeriodicReconcilerConfig(
        tick_interval_seconds=float(
            os.environ.get("PERIODIC_RECONCILE_TICK_SEC", "300"),
        ),
        min_age_seconds=float(
            os.environ.get("PERIODIC_RECONCILE_MIN_AGE_SEC", "21600"),
        ),
        batch_size=int(
            os.environ.get("PERIODIC_RECONCILE_BATCH", "20"),
        ),
        dispatch_timeout_seconds=float(
            os.environ.get(
                "PERIODIC_RECONCILE_DISPATCH_TIMEOUT_SEC",
                str(RECONCILER_DISPATCH_TIMEOUT_S),
            ),
        ),
        instance_name=os.environ.get(
            "PERIODIC_RECONCILE_INSTANCE", WORKFLOW_ID_DEFAULT,
        ),
    )
    service = PeriodicReconciler(pool, config=config)

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for s in (sig_module.SIGTERM, sig_module.SIGINT):
        loop.add_signal_handler(s, stop_event.set)

    heartbeat = Heartbeat()
    health = start_health_server(get_metrics=get_metrics, heartbeat=heartbeat)
    ticker = asyncio.ensure_future(run_heartbeat_ticker(heartbeat, stop_event))

    log.info("workflow.periodic_reconciler.started", extra={
        "instance": config.instance_name,
        "tick_s": config.tick_interval_seconds,
        "min_age_s": config.min_age_seconds,
        "batch": config.batch_size,
    })
    try:
        await service.run(stop_event=stop_event)
    finally:
        ticker.cancel()
        if health is not None:
            health.shutdown()
        log.info("workflow.periodic_reconciler.shutting_down")
        await pool.close()
    log.info("workflow.periodic_reconciler.exited")


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("WORKFLOWS_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_run_service())


if __name__ == "__main__":
    main()


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_MIN_AGE_SECONDS",
    "DEFAULT_TICK_INTERVAL_SECONDS",
    "PeriodicReconciler",
    "PeriodicReconcilerConfig",
    "get_metrics",
    "main",
    "reset_metrics",
]
