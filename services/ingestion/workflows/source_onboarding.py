"""services/ingestion/workflows/source_onboarding.py
   — M6.2a SourceOnboarding service. Per-source planner-driven shard
     fan-out.

Per ingestion LLD §2 (SourceOnboardingWorkflow shape, ported to
asyncio per [05-lld-amendments.md A11]) + §1.2 (onboarding_shards
schema, M1-shipped per A15) + §3 (per-source planners).

============================================================
RESPONSIBILITY (the two per-tick phases — same shape as M6.1)
============================================================
(a) **New-request phase.** Consume `source_onboarding_requested`
    signals from the inbox `(source_onboarding, source_onboarding)`.
    Per signal: load the `source_onboarding_runs` row, load the
    install row from `provider_installations` or
    `gmail_installations`, call `PLANNER_DISPATCH[source](tenant_id,
    install)` → `list[Shard]`. INSERT one `onboarding_shards` row per
    shard. Emit one `shard_fetch_requested` per shard to ShardFetch's
    inbox `(shard_fetch, shard_fetch)`. Mark the parent
    `source_onboarding_runs.status='in_progress'`.

    Empty planner result → mark run 'completed', emit
    `source_onboarding_completed` with success. `NotImplementedError`
    from a stubbed planner → mark run 'failed', emit
    `source_onboarding_completed` with failure (the pre-M6.3 expected
    pre-real-planner steady state).

(b) **Shard-completion phase.** Consume `shard_fetch_completed`
    signals from the same inbox `(source_onboarding,
    source_onboarding)`. Per signal: mark the `onboarding_shards.state`
    'done' (or 'failed' if `failure_reason` is present in
    signal_data). If all shards for the parent
    `source_onboarding_runs` are terminal, mark the parent
    'completed' (or 'failed' if any shard failed) and emit
    `source_onboarding_completed` to M6.1's TenantOnboarding inbox
    `(tenant_onboarding, tenant_onboarding)`.

Each signal consumption runs in its own transaction — same shape as
M6.1's TenantOnboarding orchestrator. Per-signal-per-transaction is
the established M6 default; failure rolls back the claim AND any
adjacent writes; the next tick re-claims and retries.

============================================================
SIGNAL ADDRESSING (per A13)
============================================================
The service's inbox is `(kind="source_onboarding",
id="source_onboarding")` — what M6.1 emits to. Both
`source_onboarding_requested` (from M6.1) and `shard_fetch_completed`
(from M6.2a's own ShardFetch) land here; the service dispatches on
`signal_kind` in Python after claim. Same shared-inbox pattern as
M6.1's TenantOnboarding which consumes both `onboarding_run_created`
and `source_onboarding_completed`.

Emits:
  - `shard_fetch_requested` → `(shard_fetch, shard_fetch)` —
    M6.2a's ShardFetch inbox (Phase 2).
  - `source_onboarding_completed` → `(tenant_onboarding,
    tenant_onboarding)` — M6.1's orchestrator inbox.

============================================================
SCHEMA — A15 COLUMN-NAMING MAP (LOAD-BEARING for M6.2a)
============================================================
M6.2a uses the M1-shipped `onboarding_shards` schema (LLD §1.2;
migration 0045). The M6.2a prompt described a different schema; per
[05-lld-amendments.md A15](../../../docs/ingestion/05-lld-amendments.md#a15--m62a-uses-m1-shipped-onboarding_shards-schema-no-new-migration),
the existing schema is authoritative and M6.2a uses it without
modification. The column-naming map:

  | M6.2a prompt term | Existing column (0045) |
  |---|---|
  | `shard_id` (PK) | `id UUID PRIMARY KEY` |
  | `shard_descriptor` | `shard_identifier JSONB` + `shard_kind TEXT` |
  | `cursor` | `cursor_token TEXT` (M6.2a leaves NULL) |
  | `status` | `state` |
  | `failure_reason` | `last_error` |

Status-value mapping: `pending → in_progress → done | failed`. M6.2a
does NOT write the `'reconciliation_resharded'` state (reserved for
M6.2b's Reconciler). The shard `cursor_token` column stays NULL —
the N1 primitive's cursor lives in `workflow_states.state_data`,
keyed by `(workflow_kind="shard_fetch", workflow_id=str(shard_id))`,
per the M6.0 substrate contract.

============================================================
PATTERN-ALIGNMENT MAPPING
============================================================
  Rule 1 (orchestration separated from side effects):
    `tick()` is the orchestrator; module-level `_load_*` / `_insert_*`
    / `_mark_*` functions own DB I/O. The class method passes the
    connection through; no `await self._pool.X(...)` calls in the
    class body.

  Rule 2 (state in Postgres, not memory):
    `state.persist_state` after every tick. The per-signal claim +
    state mutations are themselves Postgres-state changes.

  Rule 3 (retry in named functions):
    None needed at this granularity. Failure → txn rollback → next
    tick re-claims. No inline `try/except` retry loops.

  Rule 4 (signals via Postgres polling):
    The service consumes two signal kinds AND produces two more. All
    via the substrate.

  Rule 5 (no cross-workflow shared state):
    No module-level mutable state in this file. `PLANNER_DISPATCH`
    in `services/ingestion/planners/__init__.py` is ALL_CAPS
    (constant-style) and outside the analyzer's `services/ingestion/
    workflows/*.py` scope; it's the established dispatch-table
    pattern (same shape as the not-yet-shipped `FETCHER_DISPATCH`).
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.ingestion.planners import PLANNER_DISPATCH, Shard
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


WORKFLOW_KIND = "source_onboarding"
WORKFLOW_ID_INBOX = "source_onboarding"  # per A13: workflow_id = inbox
WORKFLOW_ID_DEFAULT = "default"  # for workflow_states diagnostics

# Signal kinds.
SIGNAL_KIND_REQUESTED = "source_onboarding_requested"   # consumed from M6.1
SIGNAL_KIND_SHARD_REQUESTED = "shard_fetch_requested"   # emitted to ShardFetch
SIGNAL_KIND_SHARD_COMPLETED = "shard_fetch_completed"   # consumed from ShardFetch
SIGNAL_KIND_COMPLETED = "source_onboarding_completed"   # emitted to M6.1

# Downstream inbox addresses.
SHARD_FETCH_INBOX_KIND = "shard_fetch"
SHARD_FETCH_INBOX_ID = "shard_fetch"
TENANT_ONBOARDING_INBOX_KIND = "tenant_onboarding"
TENANT_ONBOARDING_INBOX_ID = "tenant_onboarding"

DEFAULT_TICK_INTERVAL_SECONDS = 5.0
DEFAULT_MAX_SIGNALS_PER_TICK = 50

VALID_SOURCES = ("slack", "github", "discord", "gmail")


# ---------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------
_LOAD_SOURCE_RUN_SQL = """
SELECT onboarding_run_id, source, tenant_id, status
  FROM source_onboarding_runs
 WHERE onboarding_run_id = $1 AND source = $2
"""

_LOAD_PROVIDER_INSTALL_SQL = """
SELECT id, tenant_id, provider, installation_id, enabled
  FROM provider_installations
 WHERE tenant_id = $1 AND provider = $2 AND enabled = TRUE
 LIMIT 1
"""

_LOAD_GMAIL_INSTALL_SQL = """
SELECT id, tenant_id, workspace_domain, service_account_email,
       scope, disabled_at
  FROM gmail_installations
 WHERE tenant_id = $1 AND disabled_at IS NULL
 LIMIT 1
"""

_MARK_SOURCE_RUN_IN_PROGRESS_SQL = """
UPDATE source_onboarding_runs
   SET status = 'in_progress', started_at = COALESCE(started_at, now())
 WHERE onboarding_run_id = $1 AND source = $2 AND status = 'pending'
"""

_MARK_SOURCE_RUN_COMPLETED_SQL = """
UPDATE source_onboarding_runs
   SET status = 'completed', completed_at = now()
 WHERE onboarding_run_id = $1 AND source = $2
   AND status IN ('pending', 'in_progress')
"""

_MARK_SOURCE_RUN_FAILED_SQL = """
UPDATE source_onboarding_runs
   SET status = 'failed', completed_at = now(), failure_reason = $3
 WHERE onboarding_run_id = $1 AND source = $2
   AND status IN ('pending', 'in_progress')
"""

# Use the existing M1-shipped 0045 columns. `cursor_token` is omitted
# (stays NULL); the N1 primitive's cursor lives in workflow_states.
_INSERT_SHARD_SQL = """
INSERT INTO onboarding_shards
    (id, onboarding_run_id, tenant_id, source, shard_kind,
     shard_identifier, window_start, window_end, recency_score,
     state, created_at)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, 'pending', now())
"""

_LOAD_SHARD_SQL = """
SELECT id, onboarding_run_id, tenant_id, source, shard_kind, state
  FROM onboarding_shards
 WHERE id = $1
"""

_MARK_SHARD_DONE_SQL = """
UPDATE onboarding_shards
   SET state = 'done', completed_at = now()
 WHERE id = $1 AND state IN ('pending', 'in_progress')
"""

_MARK_SHARD_FAILED_SQL = """
UPDATE onboarding_shards
   SET state = 'failed', completed_at = now(), last_error = $2
 WHERE id = $1 AND state IN ('pending', 'in_progress')
"""

# Count non-terminal shards for the parent (run, source) pair.
# Excludes 'reconciliation_resharded' from terminal-set per A15:
# that state is M6.2b territory; in M6.2a it shouldn't appear, but
# treating it as non-terminal here is the conservative default if
# M6.2b ever overlaps with M6.2a code.
_COUNT_UNFINISHED_SHARDS_SQL = """
SELECT count(*) FROM onboarding_shards
 WHERE onboarding_run_id = $1 AND source = $2
   AND state NOT IN ('done', 'failed')
"""

_ANY_SHARD_FAILED_SQL = """
SELECT count(*) FROM onboarding_shards
 WHERE onboarding_run_id = $1 AND source = $2 AND state = 'failed'
"""

# Collect failure reasons across failed shards, for rollup into the
# parent source_onboarding_runs.failure_reason.
_COLLECT_SHARD_FAILURES_SQL = """
SELECT id, last_error FROM onboarding_shards
 WHERE onboarding_run_id = $1 AND source = $2 AND state = 'failed'
 ORDER BY completed_at ASC
"""


# ---------------------------------------------------------------------
# Config.
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class SourceOnboardingConfig:
    """Configuration knobs. Test injection + env-driven production."""

    tick_interval_seconds: float = DEFAULT_TICK_INTERVAL_SECONDS
    max_signals_per_tick: int = DEFAULT_MAX_SIGNALS_PER_TICK
    instance_name: str = WORKFLOW_ID_DEFAULT


# ---------------------------------------------------------------------
# Named side-effect functions (Rule 1).
# ---------------------------------------------------------------------
async def _load_source_run(
    conn: asyncpg.Connection, *, run_id: UUID, source: str,
) -> asyncpg.Record | None:
    return await conn.fetchrow(_LOAD_SOURCE_RUN_SQL, run_id, source)


async def _load_install(
    conn: asyncpg.Connection, *, tenant_id: UUID, source: str,
) -> asyncpg.Record | None:
    """Load the active install row for this (tenant, source).

    Returns None if no active install exists (the source got disabled
    between trigger-fire and source-onboarding-pickup — an A14 race).
    """
    if source == "gmail":
        return await conn.fetchrow(_LOAD_GMAIL_INSTALL_SQL, tenant_id)
    return await conn.fetchrow(_LOAD_PROVIDER_INSTALL_SQL, tenant_id, source)


async def _insert_shard(
    conn: asyncpg.Connection, *,
    shard_id: UUID, run_id: UUID, tenant_id: UUID, source: str,
    shard: Shard,
) -> None:
    """INSERT one onboarding_shards row using the existing 0045 schema.

    Per A15: writes `shard_kind`, `shard_identifier`, leaves
    `cursor_token` NULL (cursor lives in workflow_states under the
    N1 primitive).
    """
    import orjson
    await conn.execute(
        _INSERT_SHARD_SQL,
        shard_id, run_id, tenant_id, source,
        shard.shard_kind,
        orjson.dumps(shard.shard_identifier).decode("utf-8"),
        shard.window_start, shard.window_end,
        shard.recency_score,
    )


async def _load_shard(
    conn: asyncpg.Connection, shard_id: UUID,
) -> asyncpg.Record | None:
    return await conn.fetchrow(_LOAD_SHARD_SQL, shard_id)


async def _count_unfinished_shards(
    conn: asyncpg.Connection, *, run_id: UUID, source: str,
) -> int:
    return int(await conn.fetchval(
        _COUNT_UNFINISHED_SHARDS_SQL, run_id, source,
    ))


async def _any_shard_failed(
    conn: asyncpg.Connection, *, run_id: UUID, source: str,
) -> bool:
    return int(await conn.fetchval(
        _ANY_SHARD_FAILED_SQL, run_id, source,
    )) > 0


async def _collect_shard_failure_summary(
    conn: asyncpg.Connection, *, run_id: UUID, source: str,
) -> str:
    """Roll up failed-shard `last_error` strings into a single summary
    for the parent run's failure_reason column."""
    rows = await conn.fetch(_COLLECT_SHARD_FAILURES_SQL, run_id, source)
    parts = [
        f"shard {row['id']}: {row['last_error'] or '<no reason>'}"
        for row in rows
    ]
    return "; ".join(parts) if parts else "<no failed shards found>"


# ---------------------------------------------------------------------
# Service.
# ---------------------------------------------------------------------
class SourceOnboarding(LongRunningService):
    """LongRunningService draining the source_onboarding inbox.

    Two signal kinds expected: `source_onboarding_requested` (from M6.1)
    and `shard_fetch_completed` (from M6.2a's own ShardFetch, Phase 2).
    Python-dispatch on `signal_kind` after claiming each signal.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        config: SourceOnboardingConfig | None = None,
    ) -> None:
        self._pool = pool
        self._config = config or SourceOnboardingConfig()

    @property
    def tick_interval_seconds(self) -> float:
        return self._config.tick_interval_seconds

    async def tick(self) -> None:
        """One tick: drain up to `max_signals_per_tick` inbox signals.

        Each signal runs in its own transaction. The two signal kinds
        share the inbox; dispatch on `signal_kind` in Python.
        """
        signals_processed = 0
        for _ in range(self._config.max_signals_per_tick):
            processed = await self._process_one_signal()
            if not processed:
                break
            signals_processed += 1

        await self._persist_scan_state(signals_processed=signals_processed)

    async def _process_one_signal(self) -> bool:
        """Claim ONE signal under SKIP LOCKED + dispatch by kind.

        Returns True iff a signal was processed. False signals an
        empty inbox.

        Failure mode: signal claim succeeds but downstream write
        raises → transaction rolls back → signal claimable again on
        next tick (the A12 + A13 property + the M6.1 precedent).
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
                if sig.signal_kind == SIGNAL_KIND_REQUESTED:
                    await self._handle_source_requested(conn, sig)
                elif sig.signal_kind == SIGNAL_KIND_SHARD_COMPLETED:
                    await self._handle_shard_completed(conn, sig)
                else:
                    log.warning(
                        "source_onboarding.unknown_signal_kind",
                        extra={
                            "signal_id": str(sig.id),
                            "signal_kind": sig.signal_kind,
                            "workflow_kind": sig.workflow_kind,
                        },
                    )
        return True

    async def _handle_source_requested(
        self, conn: asyncpg.Connection, sig: WorkflowSignal,
    ) -> None:
        """Handle one `source_onboarding_requested` signal.

        Atomic transaction body — all of:
          - Load source_onboarding_runs row.
          - Idempotency check on status.
          - Mark in_progress.
          - Load install row.
          - Call planner via dispatch.
          - INSERT shard rows + emit shard_fetch_requested per shard.
        commit together or roll back together.
        """
        run_id = UUID(sig.signal_data["onboarding_run_id"])
        tenant_id = UUID(sig.signal_data["tenant_id"])
        source = sig.signal_data["source"]

        if source not in VALID_SOURCES:
            log.warning(
                "source_onboarding.invalid_source",
                extra={"source": source, "signal_id": str(sig.id)},
            )
            return

        run = await _load_source_run(conn, run_id=run_id, source=source)
        if run is None:
            log.warning(
                "source_onboarding.run_missing",
                extra={
                    "run_id": str(run_id), "source": source,
                    "signal_id": str(sig.id),
                },
            )
            return
        if run["status"] != "pending":
            # Idempotency: a re-claimed signal whose run already
            # advanced is a no-op success.
            return

        install = await _load_install(
            conn, tenant_id=tenant_id, source=source,
        )
        if install is None:
            failure_reason = (
                f"No active install for tenant {tenant_id} source "
                f"{source!r} at source-onboarding tick-time. The "
                f"install was likely disabled between trigger fire "
                f"and source-onboarding pickup (A14 race)."
            )
            await conn.execute(
                _MARK_SOURCE_RUN_FAILED_SQL,
                run_id, source, failure_reason,
            )
            await self._emit_source_completed(
                conn, run_id=run_id, source=source,
                failure_reason=failure_reason,
            )
            return

        # Mark in-progress BEFORE planner call so planner failures
        # can transition to 'failed' cleanly (the WHERE clause on
        # _MARK_SOURCE_RUN_FAILED_SQL accepts both 'pending' and
        # 'in_progress').
        await conn.execute(_MARK_SOURCE_RUN_IN_PROGRESS_SQL, run_id, source)

        try:
            shards = await PLANNER_DISPATCH[source](tenant_id, install)
        except NotImplementedError as exc:
            failure_reason = str(exc)
            await conn.execute(
                _MARK_SOURCE_RUN_FAILED_SQL,
                run_id, source, failure_reason,
            )
            await self._emit_source_completed(
                conn, run_id=run_id, source=source,
                failure_reason=failure_reason,
            )
            return

        if not shards:
            # Empty planner result: source has nothing to fetch.
            # Mark complete immediately + emit success.
            await conn.execute(
                _MARK_SOURCE_RUN_COMPLETED_SQL, run_id, source,
            )
            await self._emit_source_completed(
                conn, run_id=run_id, source=source, failure_reason=None,
            )
            return

        # Fan out: INSERT one shard row per planner output, emit one
        # shard_fetch_requested per shard. All in this transaction.
        for shard in shards:
            shard_id = uuid7()
            await _insert_shard(
                conn,
                shard_id=shard_id, run_id=run_id,
                tenant_id=tenant_id, source=source, shard=shard,
            )
            await emit_signal(
                conn,
                workflow_kind=SHARD_FETCH_INBOX_KIND,
                workflow_id=SHARD_FETCH_INBOX_ID,
                signal_kind=SIGNAL_KIND_SHARD_REQUESTED,
                idempotency_key=str(shard_id),
                signal_data={
                    "shard_id": str(shard_id),
                    "onboarding_run_id": str(run_id),
                    "tenant_id": str(tenant_id),
                    "source": source,
                },
            )

    async def _handle_shard_completed(
        self, conn: asyncpg.Connection, sig: WorkflowSignal,
    ) -> None:
        """Handle one `shard_fetch_completed` signal.

        Atomic transaction body. If this completion is the last
        non-terminal shard for the parent (run, source) pair, also
        emit `source_onboarding_completed` to M6.1's inbox.

        Wire vocabulary: `signal_data["status"]` is `'done'` or
        `'failed'` (matches the onboarding_shards.state values per
        A15). `signal_data.get("failure_reason")` is set on failure.
        """
        shard_id = UUID(sig.signal_data["shard_id"])
        status = sig.signal_data.get("status", "done")
        failure_reason = sig.signal_data.get("failure_reason")

        shard = await _load_shard(conn, shard_id)
        if shard is None:
            log.warning(
                "source_onboarding.shard_missing",
                extra={"shard_id": str(shard_id), "signal_id": str(sig.id)},
            )
            return

        run_id = shard["onboarding_run_id"]
        source = shard["source"]

        if status == "failed":
            await conn.execute(
                _MARK_SHARD_FAILED_SQL,
                shard_id, failure_reason or "<unspecified failure>",
            )
        else:
            await conn.execute(_MARK_SHARD_DONE_SQL, shard_id)

        unfinished = await _count_unfinished_shards(
            conn, run_id=run_id, source=source,
        )
        if unfinished > 0:
            return

        # All shards terminal — roll up to parent.
        if await _any_shard_failed(conn, run_id=run_id, source=source):
            rollup = await _collect_shard_failure_summary(
                conn, run_id=run_id, source=source,
            )
            await conn.execute(
                _MARK_SOURCE_RUN_FAILED_SQL, run_id, source, rollup,
            )
            await self._emit_source_completed(
                conn, run_id=run_id, source=source, failure_reason=rollup,
            )
            return

        await conn.execute(_MARK_SOURCE_RUN_COMPLETED_SQL, run_id, source)
        await self._emit_source_completed(
            conn, run_id=run_id, source=source, failure_reason=None,
        )

    async def _emit_source_completed(
        self, conn: asyncpg.Connection, *,
        run_id: UUID, source: str, failure_reason: str | None,
    ) -> None:
        """Emit `source_onboarding_completed` to M6.1's inbox.

        Idempotency key matches M6.1's TenantOnboarding orchestrator
        expectation: `f"{run_id}:{source}"`. The signal_data payload
        shape matches what M6.1's `_handle_source_completed` reads
        (onboarding_run_id, source, optional failure_reason).
        """
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
            signal_kind=SIGNAL_KIND_COMPLETED,
            idempotency_key=f"{run_id}:{source}",
            signal_data=data,
        )

    async def _persist_scan_state(
        self, *, signals_processed: int,
    ) -> None:
        """Diagnostic state row. Not load-bearing; operator queries
        against workflow_states grep this for progress signals."""
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
# CLI entrypoint — `python -m services.ingestion.workflows.source_onboarding`.
# ---------------------------------------------------------------------
# ENV:
#   DATABASE_URL                — Postgres DSN (required).
#   SOURCE_ONBOARDING_TICK_SEC  — tick interval (default 5.0).
#   SOURCE_ONBOARDING_BATCH     — max signals per tick (default 50).
#   SOURCE_ONBOARDING_INSTANCE  — instance name for diagnostics.
#   WORKFLOWS_LOG_LEVEL         — log level (default INFO).
async def _run_service() -> None:
    import asyncio
    import os
    import signal as sig_module

    from services.ingestion.workflows.runtime import make_workflow_pool

    pool = await make_workflow_pool(os.environ["DATABASE_URL"])
    config = SourceOnboardingConfig(
        tick_interval_seconds=float(
            os.environ.get("SOURCE_ONBOARDING_TICK_SEC", "5.0"),
        ),
        max_signals_per_tick=int(
            os.environ.get("SOURCE_ONBOARDING_BATCH", "50"),
        ),
        instance_name=os.environ.get(
            "SOURCE_ONBOARDING_INSTANCE", WORKFLOW_ID_DEFAULT,
        ),
    )
    service = SourceOnboarding(pool, config=config)

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for s in (sig_module.SIGTERM, sig_module.SIGINT):
        loop.add_signal_handler(s, stop_event.set)

    log.info("workflow.source_onboarding.started", extra={
        "instance": config.instance_name,
    })
    try:
        await service.run(stop_event=stop_event)
    finally:
        log.info("workflow.source_onboarding.shutting_down")
        await pool.close()
    log.info("workflow.source_onboarding.exited")


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
    "SHARD_FETCH_INBOX_ID",
    "SHARD_FETCH_INBOX_KIND",
    "SIGNAL_KIND_COMPLETED",
    "SIGNAL_KIND_REQUESTED",
    "SIGNAL_KIND_SHARD_COMPLETED",
    "SIGNAL_KIND_SHARD_REQUESTED",
    "SourceOnboarding",
    "SourceOnboardingConfig",
    "TENANT_ONBOARDING_INBOX_ID",
    "TENANT_ONBOARDING_INBOX_KIND",
    "WORKFLOW_ID_DEFAULT",
    "WORKFLOW_ID_INBOX",
    "WORKFLOW_KIND",
    "main",
]
