"""services/ingestion/workflows/periodic_reconciler.py
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
reconciler (`RECONCILER_DISPATCH[source]`) for already-reconciled
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

from services.ingestion.observability import (
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)
from services.ingestion.reconcilers import RECONCILER_DISPATCH
from services.ingestion.workflows.reconciler import (
    RECONCILER_DISPATCH_TIMEOUT_S,
    _load_shards,
    apply_reshare,
)
from services.ingestion.workflows.runtime import LongRunningService
from services.ingestion.workflows.state import (
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
# Claim one settled, due-for-recheck run. FOR UPDATE SKIP LOCKED so
# multiple instances cooperate and a row mid-check is skipped, never
# double-processed. NULLS FIRST so never-checked runs go first.
_CLAIM_ELIGIBLE_RUN_SQL = """
SELECT onboarding_run_id, source, tenant_id, status,
       reconciled_at, reconciliation_pass_count
  FROM source_onboarding_runs
 WHERE status = 'completed'
   AND reconciled_at IS NOT NULL
   AND (last_reconcile_check_at IS NULL OR last_reconcile_check_at < $1)
 ORDER BY last_reconcile_check_at NULLS FIRST
 LIMIT 1
 FOR UPDATE SKIP LOCKED
"""

# Advance the watermark. Stamped on EVERY pass (clean, gap, or
# indeterminate) so a failing run throttles to one attempt per
# min_age window instead of being hammered every tick.
_STAMP_CHECK_SQL = """
UPDATE source_onboarding_runs
   SET last_reconcile_check_at = now()
 WHERE onboarding_run_id = $1 AND source = $2
"""


# ---------------------------------------------------------------------
# Named side-effect functions (Rule 1).
# ---------------------------------------------------------------------
async def _claim_eligible_run(
    conn: asyncpg.Connection, *, cutoff: dt.datetime,
) -> asyncpg.Record | None:
    return await conn.fetchrow(_CLAIM_ELIGIBLE_RUN_SQL, cutoff)


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
        """Check up to `batch_size` due runs this tick."""
        checked = 0
        for _ in range(self._config.batch_size):
            processed = await self._check_one_run()
            if not processed:
                break
            checked += 1
        _metrics["periodic_reconciler.last_tick_checked"] = float(checked)
        await self._persist_scan_state(checked=checked)

    async def _check_one_run(self) -> bool:
        """Claim + re-reconcile one eligible run. Returns True iff a
        run was claimed (False = nothing due → stop this tick)."""
        cutoff = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(
            seconds=self._config.min_age_seconds,
        )
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                run = await _claim_eligible_run(conn, cutoff=cutoff)
                if run is None:
                    return False
                run_id = run["onboarding_run_id"]
                source = run["source"]
                # Advance the watermark first — committed even if the
                # check below errors, so a failing run is retried only
                # after min_age, not every tick.
                await _stamp_check(conn, run_id=run_id, source=source)
                _bump("periodic_reconciler.runs_checked")

                shards = await _load_shards(
                    conn, run_id=run_id, source=source,
                )
                decision = await self._gap_check(source, shards, run)
                if decision is not None and decision.has_gaps:
                    await apply_reshare(
                        conn, run_id=run_id, source=source,
                        tenant_id=run["tenant_id"], decision=decision,
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
        dispatch error is logged and swallowed (decision=None) — the
        watermark already advanced, so it re-checks next cycle. This
        is what makes the best-effort per-source checks self-healing
        rather than lossy."""
        try:
            return await asyncio.wait_for(
                RECONCILER_DISPATCH[source](shards, run),
                timeout=self._config.dispatch_timeout_seconds,
            )
        except asyncio.TimeoutError:
            _bump("periodic_reconciler.check_errors")
            log.warning(
                "periodic_reconciler.dispatch_timeout",
                extra={"source": source,
                       "timeout_s": self._config.dispatch_timeout_seconds},
            )
            return None
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
# CLI entrypoint — `python -m services.ingestion.workflows.periodic_reconciler`.
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

    from services.ingestion.workflows.runtime import make_workflow_pool

    pool = await make_workflow_pool(os.environ["DATABASE_URL"])
    # Per-source reconcilers need pool access for auxiliary reads
    # (shard cursors, installation rows). Same registration the
    # at-completion Reconciler does at startup.
    from services.ingestion.reconcilers import gmail as gmail_mod
    from services.ingestion.reconcilers import github as github_mod
    from services.ingestion.reconcilers import slack as slack_mod
    from services.ingestion.reconcilers import discord as discord_mod
    gmail_mod.set_pool_provider(pool)
    github_mod.set_pool_provider(pool)
    slack_mod.set_pool_provider(pool)
    discord_mod.set_pool_provider(pool)

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
