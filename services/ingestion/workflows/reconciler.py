"""services/ingestion/workflows/reconciler.py
   — M6.2b Reconciler service. Intercepts the M6.2a chain between
     SourceOnboarding's "all shards complete" emission and
     TenantOnboarding's source-completion handling.

Per ingestion LLD §2 (Reconciler workflow shape, ported to asyncio
per [05-lld-amendments.md A11]) + §3 (per-source gap-detection
algorithms — M6.3-M6.6 territory). Uses the existing M1-shipped
`onboarding_shards` schema's `parent_shard_id` + `'reconciliation_resharded'`
state anchors per [A15](../../../docs/ingestion/05-lld-amendments.md#a15--m62a-uses-m1-shipped-onboarding_shards-schema-no-new-migration);
adds two columns to `source_onboarding_runs` per migration 0056
(`reconciled_at` and `reconciliation_pass_count`).

============================================================
WHERE IT SITS IN THE CHAIN (M6.2b chain change)
============================================================
M6.2a flow (pre-M6.2b): SourceOnboarding rolls up shard completions
and emits `source_onboarding_completed` directly to TenantOnboarding.

M6.2b flow (this commit): SourceOnboarding emits
`source_shards_completed` to THIS Reconciler service's inbox.
Reconciler calls `RECONCILER_DISPATCH[source]` to decide clean vs.
re-share. CLEAN → emit `source_onboarding_completed` to
TenantOnboarding (preserving the M6.1 consumer contract). RE-SHARE
→ create new shards with `parent_shard_id` linkage and emit
`shard_fetch_requested` per new shard, restarting the cycle.

The **failure path is unchanged** by M6.2b — SourceOnboarding's
`_handle_shard_completed` for a "any shard failed" case still emits
`source_onboarding_completed` directly to TenantOnboarding (failed
runs have nothing to reconcile; the Reconciler would no-op them).

============================================================
RE-SHARE CYCLE SEMANTICS (per Phase 1 Decision 3)
============================================================
The `source_onboarding_runs.status` may transition through:

    'pending' → 'in_progress' → 'completed'                  ← M6.2a roll-up
        ↑                          ↓
        '──── 'in_progress' ←──────'                         ← M6.2b re-share
                  ↓
              'completed' (reconciled_at stamped this time)  ← Reconciler clean

Each `completed → in_progress` transition represents one
re-share cycle. `reconciliation_pass_count` increments on each
Reconciler re-share decision. A run can cycle multiple times if
multiple gap passes are needed; the count caps via per-source
algorithm design (M6.3-M6.6's algorithms decide when "enough is
enough"). At-completion-only per M6.2 decomposition: there is no
periodic re-reconciliation for live tenants in this work-unit.

The TRANSIENT state of interest to operators:
`status='completed' AND reconciled_at IS NULL` — the run has rolled
up its shards but the Reconciler hasn't yet processed it. This is
the normal hand-off window between SourceOnboarding's emit and
Reconciler's pickup. If a row sits in this state for >1 minute,
investigate per the runbook §6.C.

============================================================
SIGNAL ADDRESSING (per A13)
============================================================
Inbox: `(kind="reconciler", id="reconciler")`. Consumes only
`source_shards_completed` (no shared-inbox dispatch needed in
M6.2b — only one signal kind lands here). Emits:

  - CLEAN path: `source_onboarding_completed` → `(tenant_onboarding,
    tenant_onboarding)`. Idempotency key: `f"{run_id}:{source}"`
    (same shape as M6.1's TenantOnboarding consumer expectation).
  - RE-SHARE path: `shard_fetch_requested` → `(shard_fetch,
    shard_fetch)`. Idempotency key: `str(new_shard_id)` (matches
    M6.2a's ShardFetch consumer expectation).

============================================================
IDEMPOTENCY (load-bearing for the re-share cycle)
============================================================
The Reconciler is idempotent in two distinct ways:

  (a) **Signal-replay idempotent** via the substrate's
      `emit_signal` UNIQUE constraint on
      `(workflow_kind, workflow_id, signal_kind, idempotency_key)`.
      A second emit of the same key returns `was_new=False`; the
      consumer-side claim is at-most-once across pollers.

  (b) **Reconciled-state idempotent** via the `reconciled_at`
      column: if a `source_shards_completed` signal is somehow
      replayed AFTER a clean Reconciler pass, the handler short-
      circuits on `reconciled_at IS NOT NULL` and emits
      `source_onboarding_completed` again (idempotent re-emit
      handles the consumer-side gap).

============================================================
PATTERN-ALIGNMENT MAPPING
============================================================
  Rule 1 (orchestration separated from side effects):
    `tick()` and `_handle_source_shards_completed` are
    orchestration; module-level `_load_*` / `_mark_*` /
    `_insert_*` functions own DB I/O.

  Rule 2 (state in Postgres, not memory):
    `state.persist_state` every tick. The Reconciler's per-tick
    state surface is `workflow_states` keyed by the instance name;
    the per-run reconciliation state lives in
    `source_onboarding_runs.reconciled_at` +
    `reconciliation_pass_count` (migration 0056).

  Rule 3 (retry in named functions):
    None at this granularity. Failure rolls the transaction back;
    the next tick re-claims via SKIP LOCKED.

  Rule 4 (signals via Postgres polling):
    Consumes `source_shards_completed`. Produces
    `source_onboarding_completed` (clean) or `shard_fetch_requested`
    (re-share). All via the substrate.

  Rule 5 (no cross-workflow shared state):
    No module-level mutable state in this file. `RECONCILER_DISPATCH`
    in `services/ingestion/reconcilers/__init__.py` is ALL_CAPS
    (constant-style) and outside the analyzer's
    `services/ingestion/workflows/*.py` scope.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import orjson

from lib.shared.ids import uuid7
from services.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
    ResharedShard,
)
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


WORKFLOW_KIND = "reconciler"
WORKFLOW_ID_INBOX = "reconciler"  # per A13: workflow_id = inbox
WORKFLOW_ID_DEFAULT = "default"

# Signal kinds.
SIGNAL_KIND_SHARDS_COMPLETED = "source_shards_completed"   # consumed (from M6.2a)
SIGNAL_KIND_SOURCE_COMPLETED = "source_onboarding_completed"  # emitted (to M6.1)
SIGNAL_KIND_SHARD_REQUESTED = "shard_fetch_requested"   # emitted (to M6.2a ShardFetch)

# Downstream inbox addresses.
TENANT_ONBOARDING_INBOX_KIND = "tenant_onboarding"
TENANT_ONBOARDING_INBOX_ID = "tenant_onboarding"
SHARD_FETCH_INBOX_KIND = "shard_fetch"
SHARD_FETCH_INBOX_ID = "shard_fetch"

DEFAULT_TICK_INTERVAL_SECONDS = 5.0
DEFAULT_MAX_SIGNALS_PER_TICK = 50


# ---------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------
_LOAD_RUN_SQL = """
SELECT onboarding_run_id, source, tenant_id, status,
       reconciled_at, reconciliation_pass_count
  FROM source_onboarding_runs
 WHERE onboarding_run_id = $1 AND source = $2
"""

# All shards for this (run, source) — Reconciler needs the full
# state to make a gap-detection decision.
_LOAD_SHARDS_SQL = """
SELECT id, onboarding_run_id, tenant_id, source, shard_kind,
       shard_identifier, state, parent_shard_id, last_error,
       observations_seen, pages_fetched, started_at, completed_at
  FROM onboarding_shards
 WHERE onboarding_run_id = $1 AND source = $2
 ORDER BY created_at, id
"""

# Clean-path: stamp reconciled_at on the source_onboarding_runs row.
# WHERE-guard ensures idempotency: the second clean pass for the same
# row is a no-op.
_STAMP_RECONCILED_SQL = """
UPDATE source_onboarding_runs
   SET reconciled_at = now()
 WHERE onboarding_run_id = $1 AND source = $2
   AND reconciled_at IS NULL
"""

# Re-share path:
#   1. Increment pass_count (load-bearing for idempotency_key uniqueness
#      on the next source_shards_completed emit — see module docstring).
#   2. Flip status back to 'in_progress'.
#   3. Mark the original shards 'reconciliation_resharded'.
#   4. INSERT new shards with parent_shard_id linkage.
_RESHARE_RUN_SQL = """
UPDATE source_onboarding_runs
   SET status = 'in_progress',
       reconciliation_pass_count = reconciliation_pass_count + 1
 WHERE onboarding_run_id = $1 AND source = $2
RETURNING reconciliation_pass_count
"""

_MARK_SHARD_RESHARDED_SQL = """
UPDATE onboarding_shards
   SET state = 'reconciliation_resharded'
 WHERE id = $1 AND state = 'done'
"""

_INSERT_RESHARED_SHARD_SQL = """
INSERT INTO onboarding_shards
    (id, onboarding_run_id, tenant_id, source, shard_kind,
     shard_identifier, window_start, window_end, recency_score,
     state, parent_shard_id, created_at)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, 'pending',
        $10, now())
"""


# ---------------------------------------------------------------------
# Config.
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class ReconcilerConfig:
    """Configuration knobs. Test injection + env-driven production."""

    tick_interval_seconds: float = DEFAULT_TICK_INTERVAL_SECONDS
    max_signals_per_tick: int = DEFAULT_MAX_SIGNALS_PER_TICK
    instance_name: str = WORKFLOW_ID_DEFAULT


# ---------------------------------------------------------------------
# Named side-effect functions (Rule 1).
# ---------------------------------------------------------------------
async def _load_run(
    conn: asyncpg.Connection, *, run_id: UUID, source: str,
) -> asyncpg.Record | None:
    return await conn.fetchrow(_LOAD_RUN_SQL, run_id, source)


async def _load_shards(
    conn: asyncpg.Connection, *, run_id: UUID, source: str,
) -> list[asyncpg.Record]:
    return await conn.fetch(_LOAD_SHARDS_SQL, run_id, source)


async def _stamp_reconciled(
    conn: asyncpg.Connection, *, run_id: UUID, source: str,
) -> None:
    await conn.execute(_STAMP_RECONCILED_SQL, run_id, source)


async def _start_reshare(
    conn: asyncpg.Connection, *, run_id: UUID, source: str,
) -> int:
    """Flip status back + increment pass_count. Returns the new
    pass_count for use in subsequent idempotency keys."""
    return int(await conn.fetchval(_RESHARE_RUN_SQL, run_id, source))


async def _mark_original_resharded(
    conn: asyncpg.Connection, *, shard_id: UUID,
) -> None:
    await conn.execute(_MARK_SHARD_RESHARDED_SQL, shard_id)


async def _insert_reshared_shard(
    conn: asyncpg.Connection, *,
    shard_id: UUID, run_id: UUID, tenant_id: UUID, source: str,
    reshared: ResharedShard,
) -> None:
    """INSERT one onboarding_shards row from a ResharedShard."""
    await conn.execute(
        _INSERT_RESHARED_SHARD_SQL,
        shard_id, run_id, tenant_id, source,
        reshared.shard.shard_kind,
        orjson.dumps(reshared.shard.shard_identifier).decode("utf-8"),
        reshared.shard.window_start, reshared.shard.window_end,
        reshared.shard.recency_score,
        reshared.parent_shard_id,
    )


# ---------------------------------------------------------------------
# Service.
# ---------------------------------------------------------------------
class Reconciler(LongRunningService):
    """LongRunningService draining the reconciler inbox.

    Single signal kind: `source_shards_completed`. Per-signal
    transaction commits all state changes atomically.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        config: ReconcilerConfig | None = None,
    ) -> None:
        self._pool = pool
        self._config = config or ReconcilerConfig()

    @property
    def tick_interval_seconds(self) -> float:
        return self._config.tick_interval_seconds

    async def tick(self) -> None:
        """One tick: drain up to `max_signals_per_tick` inbox signals."""
        signals_processed = 0
        for _ in range(self._config.max_signals_per_tick):
            processed = await self._process_one_signal()
            if not processed:
                break
            signals_processed += 1

        await self._persist_scan_state(signals_processed=signals_processed)

    async def _process_one_signal(self) -> bool:
        """Claim ONE signal + dispatch to the handler.

        Returns True iff a signal was processed. The claim, dispatch,
        and downstream emits are one transaction; failure rolls back
        and the next tick re-claims (A12 + A13 contract).
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
                if sig.signal_kind == SIGNAL_KIND_SHARDS_COMPLETED:
                    await self._handle_source_shards_completed(conn, sig)
                else:
                    log.warning(
                        "reconciler.unknown_signal_kind",
                        extra={
                            "signal_id": str(sig.id),
                            "signal_kind": sig.signal_kind,
                        },
                    )
        return True

    async def _handle_source_shards_completed(
        self, conn: asyncpg.Connection, sig: WorkflowSignal,
    ) -> None:
        """Process one `source_shards_completed` signal.

        Atomic transaction body:
          - Load source_onboarding_runs row.
          - Idempotency check on reconciled_at.
          - Load all shards for (run, source).
          - Call RECONCILER_DISPATCH[source](shards, run).
          - Clean path: stamp reconciled_at + emit source_onboarding_completed.
          - Re-share path: increment pass_count, transition status,
            mark originals resharded, INSERT new shards, emit
            shard_fetch_requested per new shard.
        """
        run_id = UUID(sig.signal_data["onboarding_run_id"])
        source = sig.signal_data["source"]

        run = await _load_run(conn, run_id=run_id, source=source)
        if run is None:
            log.warning(
                "reconciler.run_missing",
                extra={
                    "run_id": str(run_id), "source": source,
                    "signal_id": str(sig.id),
                },
            )
            return

        if run["reconciled_at"] is not None:
            # Already reconciled clean. Re-emit source_onboarding_completed
            # idempotently to cover the consumer-side gap (the second
            # emit's was_new=False is fine — the first emit already
            # landed and is at-most-once-across-pollers).
            await self._emit_source_completed(
                conn, run_id=run_id, source=source, failure_reason=None,
            )
            return

        shards = await _load_shards(conn, run_id=run_id, source=source)

        decision = await RECONCILER_DISPATCH[source](shards, run)

        if not decision.has_gaps:
            await self._handle_clean_path(
                conn, run_id=run_id, source=source,
                tenant_id=run["tenant_id"],
            )
        else:
            await self._handle_reshare_path(
                conn, run_id=run_id, source=source,
                tenant_id=run["tenant_id"], decision=decision,
            )

    async def _handle_clean_path(
        self, conn: asyncpg.Connection, *,
        run_id: UUID, source: str, tenant_id: UUID,
    ) -> None:
        """Reconciler decided no gaps. Stamp reconciled_at and emit
        source_onboarding_completed to TenantOnboarding."""
        await _stamp_reconciled(conn, run_id=run_id, source=source)
        await self._emit_source_completed(
            conn, run_id=run_id, source=source, failure_reason=None,
        )

    async def _handle_reshare_path(
        self, conn: asyncpg.Connection, *,
        run_id: UUID, source: str, tenant_id: UUID,
        decision: ReconciliationDecision,
    ) -> None:
        """Reconciler decided gaps exist. Increment pass_count,
        transition status back to in_progress, mark originals
        resharded, INSERT new shards, emit shard_fetch_requested
        per new shard. All in this transaction."""
        # _start_reshare returns the new pass_count, but we don't use
        # it directly here — SourceOnboarding reads it from the row
        # when constructing the next rollup's idempotency_key.
        await _start_reshare(conn, run_id=run_id, source=source)

        # Mark the parent shards 'reconciliation_resharded'. Per the
        # ResharedShard contract, each new shard references one
        # original; multiple new shards may reference the SAME
        # original (one original split into N new ones). De-duplicate
        # via a set so we don't run the UPDATE more than once per
        # original.
        original_ids = {rs.parent_shard_id for rs in decision.new_shards}
        for orig_id in original_ids:
            await _mark_original_resharded(conn, shard_id=orig_id)

        # INSERT new shards + emit shard_fetch_requested per shard.
        for reshared in decision.new_shards:
            new_shard_id = uuid7()
            await _insert_reshared_shard(
                conn, shard_id=new_shard_id,
                run_id=run_id, tenant_id=tenant_id, source=source,
                reshared=reshared,
            )
            await emit_signal(
                conn,
                workflow_kind=SHARD_FETCH_INBOX_KIND,
                workflow_id=SHARD_FETCH_INBOX_ID,
                signal_kind=SIGNAL_KIND_SHARD_REQUESTED,
                idempotency_key=str(new_shard_id),
                signal_data={
                    "shard_id": str(new_shard_id),
                    "onboarding_run_id": str(run_id),
                    "tenant_id": str(tenant_id),
                    "source": source,
                },
            )

    async def _emit_source_completed(
        self, conn: asyncpg.Connection, *,
        run_id: UUID, source: str, failure_reason: str | None,
    ) -> None:
        """Emit `source_onboarding_completed` to TenantOnboarding's
        inbox. Idempotency key matches M6.1's consumer expectation
        (preserved across the M6.2b chain change so M6.1 needs no
        modification)."""
        data: dict[str, Any] = {
            "onboarding_run_id": str(run_id),
            "source": source,
        }
        if failure_reason is not None:
            data["failure_reason"] = failure_reason
        await emit_signal(
            conn,
            workflow_kind=TENANT_ONBOARDING_INBOX_KIND,
            workflow_id=TENANT_ONBOARDING_INBOX_ID,
            signal_kind=SIGNAL_KIND_SOURCE_COMPLETED,
            idempotency_key=f"{run_id}:{source}",
            signal_data=data,
        )

    async def _persist_scan_state(
        self, *, signals_processed: int,
    ) -> None:
        """Diagnostic state row. Not load-bearing for correctness."""
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
# CLI entrypoint — `python -m services.ingestion.workflows.reconciler`.
# ---------------------------------------------------------------------
# ENV:
#   DATABASE_URL          — Postgres DSN (required).
#   RECONCILER_TICK_SEC   — tick interval (default 5.0).
#   RECONCILER_BATCH      — max signals per tick (default 50).
#   RECONCILER_INSTANCE   — instance name for diagnostics.
#   WORKFLOWS_LOG_LEVEL   — log level (default INFO).
async def _run_service() -> None:
    import asyncio
    import os
    import signal as sig_module

    from services.ingestion.workflows.runtime import make_workflow_pool

    pool = await make_workflow_pool(os.environ["DATABASE_URL"])
    # M6.3: per-source reconcilers may need pool access for auxiliary
    # reads (e.g., Gmail reads workflow_states for each shard's
    # final_history_id). Register the pool with each per-source module
    # that needs it; the per-source module raises an explicit error
    # if its pool isn't registered when called.
    from services.ingestion.reconcilers import gmail as gmail_reconciler_mod
    from services.ingestion.reconcilers import github as github_reconciler_mod
    gmail_reconciler_mod.set_pool_provider(pool)
    github_reconciler_mod.set_pool_provider(pool)

    config = ReconcilerConfig(
        tick_interval_seconds=float(
            os.environ.get("RECONCILER_TICK_SEC", "5.0"),
        ),
        max_signals_per_tick=int(
            os.environ.get("RECONCILER_BATCH", "50"),
        ),
        instance_name=os.environ.get(
            "RECONCILER_INSTANCE", WORKFLOW_ID_DEFAULT,
        ),
    )
    service = Reconciler(pool, config=config)

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for s in (sig_module.SIGTERM, sig_module.SIGINT):
        loop.add_signal_handler(s, stop_event.set)

    log.info("workflow.reconciler.started", extra={
        "instance": config.instance_name,
    })
    try:
        await service.run(stop_event=stop_event)
    finally:
        log.info("workflow.reconciler.shutting_down")
        await pool.close()
    log.info("workflow.reconciler.exited")


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
    "DEFAULT_MAX_SIGNALS_PER_TICK",
    "DEFAULT_TICK_INTERVAL_SECONDS",
    "Reconciler",
    "ReconcilerConfig",
    "SHARD_FETCH_INBOX_ID",
    "SHARD_FETCH_INBOX_KIND",
    "SIGNAL_KIND_SHARDS_COMPLETED",
    "SIGNAL_KIND_SHARD_REQUESTED",
    "SIGNAL_KIND_SOURCE_COMPLETED",
    "TENANT_ONBOARDING_INBOX_ID",
    "TENANT_ONBOARDING_INBOX_KIND",
    "WORKFLOW_ID_DEFAULT",
    "WORKFLOW_ID_INBOX",
    "WORKFLOW_KIND",
    "main",
]
