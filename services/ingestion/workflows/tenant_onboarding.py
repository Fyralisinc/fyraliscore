"""services/ingestion/workflows/tenant_onboarding.py
   — M6.1 TenantOnboarding orchestrator. Step 2 of the M6.1 chain.

Per ingestion LLD §2 (TenantOnboardingWorkflow shape, ported to
asyncio per [05-lld-amendments.md A11]). Consumes
`onboarding_run_created` signals from M6.1's OAuth poller, fans out
to per-source `source_onboarding_runs` rows (one per applicable
source), polls `source_onboarding_completed` signals from M6.2's
SourceOnboarding (deferred), and marks the parent run complete when
all sources finish.

============================================================
RESPONSIBILITY (the two per-tick phases)
============================================================
(a) **New-runs phase.** Consume `onboarding_run_created` signals
    from the inbox `(tenant_onboarding, tenant_onboarding)`. Per
    signal: load the run row, query active installs to determine
    applicable sources, INSERT one `source_onboarding_runs` row per
    source, emit one `source_onboarding_requested` signal per
    source to M6.2's inbox `(source_onboarding, source_onboarding)`.
    Mark the parent run status='running'.

(b) **Completion phase.** Consume `source_onboarding_completed`
    signals from the same inbox `(tenant_onboarding,
    tenant_onboarding)`. Per signal: mark the source row
    'completed' (or 'failed' if `failure_reason` is present in
    signal_data). If all sources for the parent run are now done,
    mark the parent run 'complete' (or 'failed' if any source
    failed) and emit `tenant_onboarding_completed` to Bridge's
    inbox.

Each signal consumption runs in its own transaction — same shape
as M6.1's OAuth poller. Failure rolls back the claim AND any
adjacent writes; the next tick re-claims and retries.

============================================================
SIGNAL ADDRESSING (per A13)
============================================================
The orchestrator's inbox is `(kind="tenant_onboarding",
id="tenant_onboarding")` — same as what the poller emits to. Both
`onboarding_run_created` and `source_onboarding_completed` signals
land here; the orchestrator dispatches on `signal_kind` in Python
after claim. Single inbox simplifies operations (one set of
metrics, one consumed-by audit string).

Emits from the orchestrator:
  - `source_onboarding_requested` → `(source_onboarding,
    source_onboarding)` — M6.2's SourceOnboarding inbox.
  - `tenant_onboarding_completed` → `(bridge, bridge)` — Bridge's
    consumption inbox (Bridge implementation is out of M6.1 scope).

============================================================
SOURCE APPLICABILITY (the Phase 2 design moment)
============================================================
Per the M6.1 Phase 2 design decision:

  `provider_installations` (for slack/github/discord) +
  `gmail_installations` (for gmail), filtered to ACTIVE rows at
  orchestrator-tick-time, IS the source of truth for which sources
  apply to a tenant's onboarding.

  `onboarding_runs.sources_enabled[]` is a SNAPSHOT artifact for
  audit, NOT a controlling input.

Rationale: provider_installations reflects current reality. If a
tenant installs slack at t=0 (trigger fires), then installs gmail
at t=5s (another trigger fires), then the orchestrator picks up the
slack-trigger run at t=10s, it sees BOTH active installs. The slack
trigger's run fans out to both sources. The gmail trigger's run
(picked up next) ALSO sees both active installs and ALSO fans out
to both — duplicate work for slack across two runs.

This duplicate-work cost is accepted at M6.1 scope. M6.2's
SourceOnboarding service is responsible for deciding whether to
re-backfill an already-backfilled (tenant, source) pair —
idempotent backfill is an M6.2 design concern, not M6.1's. This
trade-off is documented in [05-lld-amendments.md A13] cross-
reference and in the M6.1 final gate output.

============================================================
PARTIAL-FAILURE HANDLING (M6.1 default)
============================================================
Per the M6.1 prompt: "if Slack fails but Gmail succeeds, the
tenant onboarding is failed not partial."

  - One source's `source_onboarding_completed` signal with
    `failure_reason` populated → mark that source 'failed' AND
    mark the parent run 'failed' (with `error_summary` rolling up
    the source-side reason).
  - The parent run's failure does NOT cancel in-flight sibling
    sources — M6.2's SourceOnboarding may still be running them.
    Their later `source_onboarding_completed` signals are
    consumed (idempotent transitions); the parent run stays
    'failed'.

M6.2+ may refine this with retry-vs-permanent-failure distinction
(e.g., 'partial' status for some-sources-completed, others-failed).
For M6.1, 'failed' is terminal.

============================================================
PATTERN-ALIGNMENT MAPPING
============================================================
  Rule 1 (orchestration separated from side effects):
    `tick()` is the orchestrator; module-level functions own DB I/O.

  Rule 2 (state in Postgres, not memory):
    `state.persist_state` after every tick. The per-tick claim +
    state mutations are themselves Postgres-state changes.

  Rule 3 (retry in named functions):
    None needed at this granularity. Failure → txn rollback → next
    tick re-claims. No inline `try/except` retry loops.

  Rule 4 (signals via Postgres polling):
    The orchestrator is the consumer-of-truth for two signal kinds
    AND the producer of two more. All via the substrate.

  Rule 5 (no cross-workflow shared state):
    No module-level mutable state.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from services.ingestion.workflows.runtime import LongRunningService
from services.ingestion.workflows.signals import (
    WorkflowSignal,
    claim_signals,
    emit_signal,
)
from services.ingestion.workflows.state import (
    WorkflowState,
    load_state,
    persist_state,
)


log = logging.getLogger(__name__)


WORKFLOW_KIND = "tenant_onboarding"
WORKFLOW_ID_INBOX = "tenant_onboarding"  # per A13: workflow_id = inbox
WORKFLOW_ID_DEFAULT = "default"  # for workflow_states diagnostics

# Signal kinds.
SIGNAL_KIND_RUN_CREATED = "onboarding_run_created"
SIGNAL_KIND_SOURCE_REQUESTED = "source_onboarding_requested"
SIGNAL_KIND_SOURCE_COMPLETED = "source_onboarding_completed"
SIGNAL_KIND_TENANT_COMPLETED = "tenant_onboarding_completed"

# Downstream inbox addresses.
SOURCE_ONBOARDING_INBOX_KIND = "source_onboarding"
SOURCE_ONBOARDING_INBOX_ID = "source_onboarding"
BRIDGE_INBOX_KIND = "bridge"
BRIDGE_INBOX_ID = "bridge"

DEFAULT_TICK_INTERVAL_SECONDS = 10.0
DEFAULT_MAX_SIGNALS_PER_TICK = 50

VALID_SOURCES = ("slack", "github", "discord", "gmail")


# ---------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------
_LOAD_RUN_SQL = """
SELECT id, tenant_id, status, sources_enabled
  FROM onboarding_runs
 WHERE id = $1
"""

# Source applicability: active installs at tick-time per A13's
# "provider_installations is the source of truth" decision.
_LOAD_ACTIVE_SOURCES_SQL = """
SELECT provider AS source
  FROM provider_installations
 WHERE tenant_id = $1
   AND enabled = TRUE
   AND provider IN ('slack', 'github', 'discord')
UNION
SELECT 'gmail' AS source
  FROM gmail_installations
 WHERE tenant_id = $1
   AND disabled_at IS NULL
"""

# ON CONFLICT DO NOTHING: defensive against concurrent claims racing
# on the same signal (which SKIP LOCKED prevents but cost-free to
# guard) or duplicate emits at the producer side.
_INSERT_SOURCE_ROW_SQL = """
INSERT INTO source_onboarding_runs
    (onboarding_run_id, source, tenant_id, status, started_at)
VALUES ($1, $2, $3, 'pending', now())
ON CONFLICT (onboarding_run_id, source) DO NOTHING
"""

_MARK_RUN_RUNNING_SQL = """
UPDATE onboarding_runs
   SET status = 'running'
 WHERE id = $1 AND status = 'pending'
"""

_MARK_SOURCE_COMPLETED_SQL = """
UPDATE source_onboarding_runs
   SET status = 'completed', completed_at = now()
 WHERE onboarding_run_id = $1 AND source = $2
   AND status IN ('pending', 'in_progress')
"""

_MARK_SOURCE_FAILED_SQL = """
UPDATE source_onboarding_runs
   SET status = 'failed', completed_at = now(), failure_reason = $3
 WHERE onboarding_run_id = $1 AND source = $2
   AND status IN ('pending', 'in_progress')
"""

# Count source rows still un-terminal for the parent run.
_COUNT_UNFINISHED_SOURCES_SQL = """
SELECT count(*) FROM source_onboarding_runs
 WHERE onboarding_run_id = $1
   AND status NOT IN ('completed', 'failed')
"""

# Did ANY source fail for this run?
_ANY_SOURCE_FAILED_SQL = """
SELECT count(*) FROM source_onboarding_runs
 WHERE onboarding_run_id = $1 AND status = 'failed'
"""

_MARK_RUN_COMPLETE_SQL = """
UPDATE onboarding_runs
   SET status = 'complete', completed_at = now()
 WHERE id = $1 AND status IN ('pending', 'running')
"""

_MARK_RUN_FAILED_SQL = """
UPDATE onboarding_runs
   SET status = 'failed', completed_at = now(), error_summary = $2
 WHERE id = $1 AND status IN ('pending', 'running')
"""


# ---------------------------------------------------------------------
# Config.
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class TenantOnboardingConfig:
    """Configuration knobs. Test injection + env-driven production."""

    tick_interval_seconds: float = DEFAULT_TICK_INTERVAL_SECONDS
    max_signals_per_tick: int = DEFAULT_MAX_SIGNALS_PER_TICK
    instance_name: str = WORKFLOW_ID_DEFAULT


# ---------------------------------------------------------------------
# Named side-effect functions (Rule 1).
# ---------------------------------------------------------------------
async def _load_run_row(
    conn: asyncpg.Connection, run_id: UUID,
) -> asyncpg.Record | None:
    return await conn.fetchrow(_LOAD_RUN_SQL, run_id)


async def _determine_applicable_sources(
    conn: asyncpg.Connection, tenant_id: UUID,
) -> list[str]:
    """Query provider_installations + gmail_installations at
    tick-time. Returns the list of sources that are CURRENTLY
    active for this tenant.

    Per A13: this is the source of truth for source applicability.
    The trigger's source (recorded in onboarding_runs.sources_enabled
    as a snapshot) is informational only. Cited file:line for the
    decision rationale: this function, plus the module docstring's
    "SOURCE APPLICABILITY" section above.
    """
    rows = await conn.fetch(_LOAD_ACTIVE_SOURCES_SQL, tenant_id)
    return [r["source"] for r in rows if r["source"] in VALID_SOURCES]


async def _insert_source_row(
    conn: asyncpg.Connection,
    *,
    run_id: UUID, source: str, tenant_id: UUID,
) -> None:
    await conn.execute(
        _INSERT_SOURCE_ROW_SQL, run_id, source, tenant_id,
    )


async def _mark_source_completed(
    conn: asyncpg.Connection,
    *,
    run_id: UUID, source: str,
) -> None:
    await conn.execute(_MARK_SOURCE_COMPLETED_SQL, run_id, source)


async def _mark_source_failed(
    conn: asyncpg.Connection,
    *,
    run_id: UUID, source: str, failure_reason: str,
) -> None:
    await conn.execute(
        _MARK_SOURCE_FAILED_SQL, run_id, source, failure_reason,
    )


async def _count_unfinished_sources(
    conn: asyncpg.Connection, run_id: UUID,
) -> int:
    return int(await conn.fetchval(_COUNT_UNFINISHED_SOURCES_SQL, run_id))


async def _any_source_failed(
    conn: asyncpg.Connection, run_id: UUID,
) -> bool:
    return int(await conn.fetchval(_ANY_SOURCE_FAILED_SQL, run_id)) > 0


# ---------------------------------------------------------------------
# Service.
# ---------------------------------------------------------------------
class TenantOnboardingOrchestrator(LongRunningService):
    """LongRunningService that drains the tenant_onboarding inbox.

    Constructor takes a pool + config; the per-signal transactions
    acquire connections from it. The `__main__orchestrator.py` CLI
    owns DSN-to-pool bootstrapping.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        config: TenantOnboardingConfig | None = None,
    ) -> None:
        self._pool = pool
        self._config = config or TenantOnboardingConfig()

    @property
    def tick_interval_seconds(self) -> float:
        return self._config.tick_interval_seconds

    async def tick(self) -> None:
        """One tick: drain up to `max_signals_per_tick` inbox signals.

        Each signal runs in its own transaction. New-run signals
        and source-completion signals share the inbox; the
        orchestrator dispatches on signal_kind in Python after
        claiming each signal.
        """
        signals_processed = 0
        for _ in range(self._config.max_signals_per_tick):
            processed = await self._process_one_signal()
            if not processed:
                break
            signals_processed += 1

        await self._persist_scan_state(signals_processed=signals_processed)

    async def _process_one_signal(self) -> bool:
        """Claim ONE signal under the load-bearing A12 + A13 +
        SKIP LOCKED contract, dispatch by kind, commit on success.

        Returns True if a signal was processed; False if the inbox
        is empty.

        Failure modes:
          - Signal claim succeeds but downstream write fails →
            transaction rolls back → signal becomes claimable again
            on next tick (A12 property: claim_signals participates
            in the caller's transaction).
          - Unknown signal_kind → log + treat as consumed (the
            substrate moves on; a wrong-kind signal in this inbox
            is a programming error elsewhere, not the orchestrator's
            recovery point).
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                signals = await claim_signals(
                    conn,
                    workflow_kind=WORKFLOW_KIND,
                    workflow_id=WORKFLOW_ID_INBOX,
                    consumed_by=self._config.instance_name,
                    batch_size=1,
                )
                if not signals:
                    return False
                sig = signals[0]
                if sig.signal_kind == SIGNAL_KIND_RUN_CREATED:
                    await self._handle_run_created(conn, sig)
                elif sig.signal_kind == SIGNAL_KIND_SOURCE_COMPLETED:
                    await self._handle_source_completed(conn, sig)
                else:
                    log.warning(
                        "orchestrator.unknown_signal_kind",
                        extra={
                            "signal_id": str(sig.id),
                            "signal_kind": sig.signal_kind,
                            "workflow_kind": sig.workflow_kind,
                        },
                    )
        return True

    async def _handle_run_created(
        self, conn: asyncpg.Connection, sig: WorkflowSignal,
    ) -> None:
        """New-runs phase. Source-applicability determined by
        provider_installations + gmail_installations at tick-time
        (A13 / Phase 2 decision)."""
        run_id = UUID(sig.signal_data["onboarding_run_id"])
        tenant_id = UUID(sig.signal_data["tenant_id"])

        run = await _load_run_row(conn, run_id)
        if run is None:
            log.warning(
                "orchestrator.run_missing",
                extra={"run_id": str(run_id), "signal_id": str(sig.id)},
            )
            return
        if run["status"] != "pending":
            # Idempotency: a re-claimed signal whose run is already
            # advanced is a no-op success.
            return

        sources = await _determine_applicable_sources(conn, tenant_id)
        if not sources:
            # No active installs at tick-time. Fail the run loudly
            # rather than create a zero-source onboarding that
            # never completes — same shape as M3.3's "DLQ the row
            # and advance" rather than loop forever.
            await conn.execute(
                _MARK_RUN_FAILED_SQL,
                run_id,
                "No active installs for tenant at orchestrator tick-time.",
            )
            return

        for source in sources:
            await _insert_source_row(
                conn, run_id=run_id, source=source, tenant_id=tenant_id,
            )
            await emit_signal(
                conn,
                workflow_kind=SOURCE_ONBOARDING_INBOX_KIND,
                workflow_id=SOURCE_ONBOARDING_INBOX_ID,
                signal_kind=SIGNAL_KIND_SOURCE_REQUESTED,
                idempotency_key=f"{run_id}:{source}",
                signal_data={
                    "onboarding_run_id": str(run_id),
                    "tenant_id": str(tenant_id),
                    "source": source,
                },
            )

        await conn.execute(_MARK_RUN_RUNNING_SQL, run_id)

    async def _handle_source_completed(
        self, conn: asyncpg.Connection, sig: WorkflowSignal,
    ) -> None:
        """Completion phase. If failure_reason is present in
        signal_data, the source failed and the parent run fails too
        (M6.1 default; no 'partial' status until M6.2+)."""
        run_id = UUID(sig.signal_data["onboarding_run_id"])
        source = sig.signal_data["source"]
        failure_reason = sig.signal_data.get("failure_reason")

        if failure_reason:
            await _mark_source_failed(
                conn, run_id=run_id, source=source,
                failure_reason=str(failure_reason),
            )
            await conn.execute(
                _MARK_RUN_FAILED_SQL,
                run_id,
                f"Source {source!r} failed: {failure_reason}",
            )
            return

        await _mark_source_completed(conn, run_id=run_id, source=source)

        unfinished = await _count_unfinished_sources(conn, run_id)
        if unfinished > 0:
            return

        # All sources in terminal state — check if any failed.
        if await _any_source_failed(conn, run_id):
            # A sibling source had already failed; parent already
            # marked 'failed'. Nothing more to do.
            return

        # All sources completed successfully. Mark run complete +
        # emit tenant_onboarding_completed.
        await conn.execute(_MARK_RUN_COMPLETE_SQL, run_id)

        tenant_id = await conn.fetchval(
            "SELECT tenant_id FROM onboarding_runs WHERE id = $1", run_id,
        )
        await emit_signal(
            conn,
            workflow_kind=BRIDGE_INBOX_KIND,
            workflow_id=BRIDGE_INBOX_ID,
            signal_kind=SIGNAL_KIND_TENANT_COMPLETED,
            idempotency_key=str(run_id),
            signal_data={
                "onboarding_run_id": str(run_id),
                "tenant_id": str(tenant_id),
            },
        )

    async def _persist_scan_state(
        self, *, signals_processed: int,
    ) -> None:
        """Diagnostic state row. Not load-bearing for correctness;
        operator queries against workflow_states grep this for
        progress signals."""
        existing = await load_state(
            self._pool, WORKFLOW_KIND, self._config.instance_name,
        )
        state = WorkflowState(
            workflow_kind=WORKFLOW_KIND,
            workflow_id=self._config.instance_name,
            tenant_id=None,
            state_data={
                "last_tick_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
                "last_signals_processed": signals_processed,
                "lifetime_signals_processed": (
                    (existing.state_data.get("lifetime_signals_processed", 0)
                     if existing else 0)
                    + signals_processed
                ),
            },
            last_advanced_at=dt.datetime.now(tz=dt.timezone.utc),
        )
        await persist_state(self._pool, state)


# ---------------------------------------------------------------------
# CLI entrypoint — python -m services.ingestion.workflows.tenant_onboarding.
# ---------------------------------------------------------------------
# Per the M6.1 architectural decision: two processes per logical
# workflow. The orchestrator has its own entrypoint module, sibling
# to oauth_poller's. ENV:
#   DATABASE_URL                — Postgres DSN (required).
#   ORCHESTRATOR_TICK_SEC       — tick interval (default 10.0).
#   ORCHESTRATOR_BATCH          — max signals per tick (default 50).
#   ORCHESTRATOR_INSTANCE       — instance name for diagnostics.
#   WORKFLOWS_LOG_LEVEL         — log level (default INFO).
async def _run_orchestrator() -> None:
    import asyncio
    import os
    import signal

    from services.ingestion.workflows.runtime import make_workflow_pool

    pool = await make_workflow_pool(os.environ["DATABASE_URL"])
    config = TenantOnboardingConfig(
        tick_interval_seconds=float(
            os.environ.get("ORCHESTRATOR_TICK_SEC", "10.0"),
        ),
        max_signals_per_tick=int(
            os.environ.get("ORCHESTRATOR_BATCH", "50"),
        ),
        instance_name=os.environ.get(
            "ORCHESTRATOR_INSTANCE", WORKFLOW_ID_DEFAULT,
        ),
    )
    service = TenantOnboardingOrchestrator(pool, config=config)

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    log.info("workflow.tenant_onboarding.started", extra={
        "instance": config.instance_name,
    })
    try:
        await service.run(stop_event=stop_event)
    finally:
        log.info("workflow.tenant_onboarding.shutting_down")
        await pool.close()
    log.info("workflow.tenant_onboarding.exited")


def main() -> None:
    import asyncio
    import os
    logging.basicConfig(
        level=os.environ.get("WORKFLOWS_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_run_orchestrator())


if __name__ == "__main__":
    main()


__all__ = [
    "BRIDGE_INBOX_ID",
    "BRIDGE_INBOX_KIND",
    "DEFAULT_MAX_SIGNALS_PER_TICK",
    "DEFAULT_TICK_INTERVAL_SECONDS",
    "SIGNAL_KIND_RUN_CREATED",
    "SIGNAL_KIND_SOURCE_COMPLETED",
    "SIGNAL_KIND_SOURCE_REQUESTED",
    "SIGNAL_KIND_TENANT_COMPLETED",
    "SOURCE_ONBOARDING_INBOX_ID",
    "SOURCE_ONBOARDING_INBOX_KIND",
    "TenantOnboardingConfig",
    "TenantOnboardingOrchestrator",
    "WORKFLOW_ID_DEFAULT",
    "WORKFLOW_ID_INBOX",
    "WORKFLOW_KIND",
    "main",
]
