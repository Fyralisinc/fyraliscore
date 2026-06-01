"""services/ingestion/workflows/oauth_poller.py
   — M6.1 OAuth outbox poller. Step 1 of the M6.1 handoff chain.

Per ingestion LLD §1.4 (OAuth callback outbox) and §2.2
(OnboardingTriggerPollerWorkflow). Implemented as an asyncio service
per [04-implementation-plan.md §M6 pattern-alignment requirements]
and [05-lld-amendments.md A11] (Temporal deferred indefinitely).

============================================================
RESPONSIBILITY
============================================================
Per tick:
  1. Claim one unconsumed `onboarding_triggers` row under
     `SELECT ... FOR UPDATE SKIP LOCKED` (the row-level lock holds for
     the duration of the transaction; concurrent pollers skip
     locked rows rather than block).
  2. INSERT one `onboarding_runs` row (status='pending') describing
     the per-tenant onboarding that this trigger initiates.
  3. UPDATE the trigger row to stamp `consumed_at = now()` and
     `consumed_by_workflow_id = <the new run's workflow_id>`.
  4. Emit `onboarding_run_created` signal via
     `signals.emit_signal(conn, ...)` so the M6.1
     `tenant_onboarding_orchestrator` picks up the run on its next
     tick.

All four steps run inside ONE `async with conn.transaction()` block.
The transactional invariant (load-bearing for M6.1):

  trigger.consumed_at SET   ⟺   onboarding_runs row EXISTS
                            ⟺   workflow_signals row EXISTS

If any step fails, the whole txn rolls back: trigger stays
unconsumed, no run row, no signal. The next tick re-claims the
trigger and retries cleanly. Idempotency is via the signal's
`idempotency_key=onboarding_run_id`: even if a retry creates a
different `onboarding_run_id` (because uuid7() is fresh each
attempt), the signal carries the run_id of THIS attempt and Bridge-
side dedup keys remain unique per attempt. The trigger row's
consumed_by_workflow_id records which attempt won.

============================================================
A12 SUBSTRATE AMENDMENT (commit 81481a5)
============================================================
This service is the substrate amendment's first real consumer.
`signals.emit_signal(conn, ...)` is called with a Connection (the
caller-supplied txn variant from A12), so the signal emit
participates in the same transaction as the trigger claim + run
insert. Without A12, the emit would auto-commit independently and
the atomicity invariant above would be unenforceable.

The load-bearing precedent test is
`test_emit_signal_with_connection_participates_in_caller_txn` in
[services/ingestion/workflows/tests/test_executor_surface.py].
M6.1's `test_oauth_poller_atomic_rollback_on_signal_failure`
exercises the same property at the service-integration level.

============================================================
PATTERN-ALIGNMENT MAPPING
============================================================
  Rule 1 (orchestration separated from side effects):
    `tick()` is the orchestrator; module-level functions
    (`_claim_pending_trigger`, `_create_onboarding_run`,
    `_mark_trigger_consumed`) own the DB I/O. The method passes
    the connection through; no direct `self._pool.X(...)` calls.

  Rule 2 (state in Postgres, not memory):
    `state.persist_state` after every tick records scan
    diagnostics (`last_tick_at`, `last_triggers_claimed`,
    `lifetime_triggers_claimed`). No per-process counters that
    diverge from the durable record.

  Rule 3 (retry in named functions):
    None required at this granularity. Failed transactions roll
    back; the next tick re-claims the same trigger. There is no
    inline `try/except` retry loop because the txn rollback +
    next-tick-reclaim shape replaces it.

  Rule 4 (signals via Postgres polling):
    Cross-service handoff via `emit_signal` (this service is the
    producer side of the M6.1 chain; the orchestrator is the
    consumer). No `asyncio.Queue` or in-process channels.

  Rule 5 (no cross-workflow shared state):
    No module-level mutable state. State diagnostics go through
    `workflow_states`.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.ingestion.workflows.runtime import LongRunningService
from services.ingestion.workflows.signals import emit_signal
from services.ingestion.workflows.state import (
    WorkflowState,
    load_state,
    persist_state,
)


log = logging.getLogger(__name__)


WORKFLOW_KIND = "oauth_poller"
WORKFLOW_ID_DEFAULT = "default"

SIGNAL_KIND_RUN_CREATED = "onboarding_run_created"

# Tick interval default. The M6.1 prompt specifies 5s for operator UX
# (install-to-onboarding handoff under 10s total — 5s poll + ~5s
# orchestrator pickup).
DEFAULT_TICK_INTERVAL_SECONDS = 5.0
# Max triggers processed per tick. Each trigger gets its own
# transaction; the batch is a wall-clock-soft cap rather than a
# strict batch insert. Tests can set this to 1 for determinism.
DEFAULT_MAX_TRIGGERS_PER_TICK = 50


# ---------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------
# Claim one unconsumed trigger under SKIP LOCKED. The SELECT holds a
# row lock for the duration of the caller's transaction; the caller's
# subsequent UPDATE + commit converts the in-flight lock to a
# durable consumed_at stamp. If the caller rolls back, the row
# becomes claimable again to another poller.
_CLAIM_ONE_TRIGGER_SQL = """
SELECT id, tenant_id, source, trigger_kind,
       installation_row_id, gmail_installation_id, payload,
       consume_attempts
  FROM onboarding_triggers
 WHERE consumed_at IS NULL
 ORDER BY created_at ASC
 LIMIT 1
 FOR UPDATE SKIP LOCKED
"""

# UPSERT the onboarding_runs row. uuid7 for the id (time-sortable);
# workflow_id is deterministic from the trigger_id (tracing).
_CREATE_RUN_SQL = """
INSERT INTO onboarding_runs
    (id, tenant_id, trigger_kind, workflow_id, status,
     sources_enabled, started_at)
VALUES ($1, $2, $3, $4, 'pending', $5::text[], now())
"""

# Mark trigger consumed. consumed_by_workflow_id records the
# onboarding_runs.workflow_id we just created — useful for ops tracing
# (find the run that consumed this trigger).
_MARK_TRIGGER_CONSUMED_SQL = """
UPDATE onboarding_triggers
   SET consumed_at = now(),
       consumed_by_workflow_id = $2,
       consume_attempts = consume_attempts + 1,
       last_attempt_at = now()
 WHERE id = $1
"""


# ---------------------------------------------------------------------
# Config.
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class OAuthPollerConfig:
    """Configuration knobs. Test injection + env-driven production."""

    tick_interval_seconds: float = DEFAULT_TICK_INTERVAL_SECONDS
    max_triggers_per_tick: int = DEFAULT_MAX_TRIGGERS_PER_TICK
    instance_name: str = WORKFLOW_ID_DEFAULT


# ---------------------------------------------------------------------
# Named side-effect functions (Rule 1).
# ---------------------------------------------------------------------
async def _claim_pending_trigger(
    conn: asyncpg.Connection,
) -> asyncpg.Record | None:
    """SELECT-FOR-UPDATE-SKIP-LOCKED one unconsumed trigger.

    Returns None if there are no unclaimed triggers OR all unclaimed
    triggers are locked by another poller. The caller MUST be inside
    a transaction; the row lock is released on commit/rollback.
    """
    return await conn.fetchrow(_CLAIM_ONE_TRIGGER_SQL)


async def _create_onboarding_run(
    conn: asyncpg.Connection,
    *,
    trigger: asyncpg.Record,
) -> tuple[UUID, str]:
    """INSERT one onboarding_runs row for this trigger.

    Returns `(run_id, workflow_id)`. The workflow_id encodes the
    trigger_id so an operator querying "which run consumed which
    trigger" can read it off either side of the join.

    `sources_enabled` is populated with the trigger's single source.
    The M6.1 TenantOnboarding orchestrator (Phase 2) uses
    `provider_installations` as the source-applicability source of
    truth and may fan out beyond this list per the design decision
    documented in tenant_onboarding.py.
    """
    run_id = uuid7()
    workflow_id = f"onboarding:{trigger['id']}"
    await conn.execute(
        _CREATE_RUN_SQL,
        run_id,
        trigger["tenant_id"],
        trigger["trigger_kind"],
        workflow_id,
        [trigger["source"]],
    )
    return run_id, workflow_id


async def _mark_trigger_consumed(
    conn: asyncpg.Connection,
    *,
    trigger_id: UUID,
    workflow_id: str,
) -> None:
    """Stamp the trigger row: consumed_at=now(), consumed_by_workflow_id.
    Must run inside the same transaction as the run-create + signal-
    emit so all four touch points commit together."""
    await conn.execute(
        _MARK_TRIGGER_CONSUMED_SQL, trigger_id, workflow_id,
    )


# ---------------------------------------------------------------------
# Service.
# ---------------------------------------------------------------------
class OAuthPoller(LongRunningService):
    """LongRunningService that drains `onboarding_triggers` into
    `onboarding_runs` + `onboarding_run_created` signals.

    Constructor takes a pool (and config); the per-trigger
    transactions acquire connections from it. The `__main__poller.py`
    CLI entrypoint owns DSN-to-pool bootstrapping.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        config: OAuthPollerConfig | None = None,
    ) -> None:
        self._pool = pool
        self._config = config or OAuthPollerConfig()

    @property
    def tick_interval_seconds(self) -> float:
        return self._config.tick_interval_seconds

    async def tick(self) -> None:
        """One tick: process up to `max_triggers_per_tick` triggers.

        Each trigger runs in its own transaction so one bad trigger
        (e.g., a unique-constraint collision on workflow_id) does
        not block the others. The "up to N" cap is wall-clock
        soft — a tick that processes N triggers and then finds no
        more returns immediately; a tick that fills the cap stops
        and waits for the next interval to continue.
        """
        triggers_claimed = 0
        for _ in range(self._config.max_triggers_per_tick):
            processed = await self._process_one_trigger()
            if not processed:
                break
            triggers_claimed += 1

        await self._persist_scan_state(triggers_claimed=triggers_claimed)

    async def _process_one_trigger(self) -> bool:
        """One trigger, one transaction. Returns True if a trigger
        was processed; False if no unclaimed triggers were available.

        ============================================================
        LOAD-BEARING TRANSACTIONAL INVARIANT (M6.1)
        ============================================================
        The body of `async with conn.transaction()` runs the entire
        (claim trigger + create run + emit signal + mark consumed)
        sequence. Either all four commit together or none does.
        `signals.emit_signal(conn, ...)` (the A12 connection-typed
        variant) is what makes this possible — the emit
        participates in the txn rather than auto-committing.

        If any step raises, the with-block exits via exception and
        the transaction rolls back: trigger.consumed_at stays NULL,
        no onboarding_runs row, no workflow_signals row. The next
        tick re-claims the trigger and retries.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                trigger = await _claim_pending_trigger(conn)
                if trigger is None:
                    return False

                run_id, workflow_id = await _create_onboarding_run(
                    conn, trigger=trigger,
                )

                # Signal addressing: workflow_id is the routing-
                # partition key (the consumer's inbox), NOT a per-run
                # instance identifier. Per A13: the asyncio
                # TenantOnboarding orchestrator is a single global
                # service consuming from `(kind="tenant_onboarding",
                # id="tenant_onboarding")` — there is exactly one
                # inbox for the orchestrator-family of consumers.
                # Per-run identity lives in idempotency_key (which is
                # str(run_id)) and signal_data.
                await emit_signal(
                    conn,
                    workflow_kind="tenant_onboarding",
                    workflow_id="tenant_onboarding",
                    signal_kind=SIGNAL_KIND_RUN_CREATED,
                    idempotency_key=str(run_id),
                    signal_data={
                        "onboarding_run_id": str(run_id),
                        "tenant_id": str(trigger["tenant_id"]),
                        "trigger_id": str(trigger["id"]),
                        "source": trigger["source"],
                        "trigger_kind": trigger["trigger_kind"],
                    },
                )

                await _mark_trigger_consumed(
                    conn,
                    trigger_id=trigger["id"],
                    workflow_id=workflow_id,
                )
        return True

    async def _persist_scan_state(
        self, *, triggers_claimed: int,
    ) -> None:
        """Record diagnostic state. Not load-bearing for correctness;
        useful for operator queries against `workflow_states`.

        Runs OUTSIDE the per-trigger transaction so even ticks that
        claim zero triggers still write a heartbeat row."""
        existing = await load_state(
            self._pool, WORKFLOW_KIND, self._config.instance_name,
        )
        state = WorkflowState(
            workflow_kind=WORKFLOW_KIND,
            workflow_id=self._config.instance_name,
            tenant_id=None,  # global service
            state_data={
                "last_tick_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
                "last_triggers_claimed": triggers_claimed,
                "lifetime_triggers_claimed": (
                    (existing.state_data.get("lifetime_triggers_claimed", 0)
                     if existing else 0)
                    + triggers_claimed
                ),
            },
            last_advanced_at=dt.datetime.now(tz=dt.timezone.utc),
        )
        await persist_state(self._pool, state)


# ---------------------------------------------------------------------
# CLI entrypoint — `python -m services.ingestion.workflows.oauth_poller`.
# ---------------------------------------------------------------------
# Per the M6.1 prompt: each of the two services (poller + orchestrator)
# has its own entrypoint module so they can be deployed as two
# independent processes. SIGTERM/SIGINT handler shape matches the M6.0
# __main__.py precedent.
#
# ENV:
#   DATABASE_URL                   — Postgres DSN (required).
#   OAUTH_POLLER_TICK_SEC          — tick interval (default 5.0).
#   OAUTH_POLLER_BATCH             — max triggers per tick (default 50).
#   OAUTH_POLLER_INSTANCE          — instance name for workflow_states
#                                    diagnostics (default "default").
#   WORKFLOWS_LOG_LEVEL            — log level (default INFO).
async def _run_poller() -> None:
    import asyncio
    import os
    import signal

    from services.ingestion.workflows.runtime import make_workflow_pool

    pool = await make_workflow_pool(os.environ["DATABASE_URL"])
    config = OAuthPollerConfig(
        tick_interval_seconds=float(
            os.environ.get("OAUTH_POLLER_TICK_SEC", "5.0"),
        ),
        max_triggers_per_tick=int(
            os.environ.get("OAUTH_POLLER_BATCH", "50"),
        ),
        instance_name=os.environ.get(
            "OAUTH_POLLER_INSTANCE", WORKFLOW_ID_DEFAULT,
        ),
    )
    service = OAuthPoller(pool, config=config)

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    log.info("workflow.oauth_poller.started", extra={
        "instance": config.instance_name,
    })
    try:
        await service.run(stop_event=stop_event)
    finally:
        log.info("workflow.oauth_poller.shutting_down")
        await pool.close()
    log.info("workflow.oauth_poller.exited")


def main() -> None:
    import asyncio
    import os
    logging.basicConfig(
        level=os.environ.get("WORKFLOWS_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_run_poller())


if __name__ == "__main__":
    main()


__all__ = [
    "DEFAULT_MAX_TRIGGERS_PER_TICK",
    "DEFAULT_TICK_INTERVAL_SECONDS",
    "OAuthPoller",
    "OAuthPollerConfig",
    "SIGNAL_KIND_RUN_CREATED",
    "WORKFLOW_ID_DEFAULT",
    "WORKFLOW_KIND",
    "main",
]
