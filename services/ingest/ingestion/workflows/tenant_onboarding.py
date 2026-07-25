"""services/ingest/ingestion/workflows/tenant_onboarding.py
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
    from the inbox `(tenant_onboarding, tenant_onboarding)`. Each
    signal identifies exactly one canonical historical source and one
    Fyralis installation-row UUID. Validate that exact enabled row
    belongs to the signal tenant/source, INSERT one
    `source_onboarding_runs` row, and emit one
    `source_onboarding_requested` signal. Mark the parent run
    status='running'.

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
EXACT-INSTALLATION SEMANTICS (contract cutover)
============================================================
The former tick-time "fan out every active source for this tenant"
behavior was ambiguous for tenants with two installations of the same
source and duplicated unrelated backfills whenever another source was
connected. Contract-only onboarding deliberately uses one
trigger → one run → one source → one exact installation UUID.

Gmail's legacy `gmail_installation_id` is accepted only at this ingress
boundary and normalized to `installation_row_id`. WhatsApp has
`history=None`, so it cannot enter this workflow. Missing, malformed,
disabled, cross-tenant, and wrong-source installation identities fail
closed before a source request is emitted.

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

from services.ingest.ingestion.feature_flags.client import KAFKA_PATH_ENABLED
from services.ingest.ingestion.progress.events import (
    ProgressEvent,
    TenantOnboardingComplete,
    TenantOnboardingStarted,
)
from services.ingest.ingestion.progress.publisher import publish_progress_events
from services.ingest.ingestion.workflows.runtime import LongRunningService
from services.ingest.ingestion.workflows.signals import (
    WorkflowSignal,
    claim_signals,
    emit_signal,
    process_signal_with_serialization_retry,
)
from services.ingest.ingestion.workflows.state import (
    WorkflowState,
    load_state,
    persist_state,
)
from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.source_contract.runtime import resolve_installation_loader


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

VALID_SOURCES = frozenset(
    definition.source_id
    for definition in SOURCE_DEFINITIONS
    if definition.history is not None
)

# Coarse, NON-BINDING per-source estimate for the `tenant.onboarding.started`
# event's `eta_minutes`. The event model documents this field as a
# "planner estimate; non-binding" — Bridge uses it only for an at-a-glance
# progress hint, never as a deadline. A real planner-derived ETA would
# replace this when per-source shard-count planning feeds back here.
ETA_MINUTES_PER_SOURCE = 5


# ---------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------
_LOAD_RUN_SQL = """
SELECT id, tenant_id, status, sources_enabled
  FROM onboarding_runs
 WHERE id = $1
"""

# ON CONFLICT DO NOTHING: defensive against concurrent claims racing
# on the same signal (which SKIP LOCKED prevents but cost-free to
# guard) or duplicate emits at the producer side.
_INSERT_SOURCE_ROW_SQL = """
INSERT INTO source_onboarding_runs
    (onboarding_run_id, source, tenant_id, installation_row_id,
     status, started_at)
VALUES ($1, $2, $3, $4, 'pending', now())
ON CONFLICT (onboarding_run_id, source) DO NOTHING
"""

_MARK_RUN_RUNNING_SQL = """
UPDATE onboarding_runs
   SET status = 'running'
 WHERE id = $1 AND status = 'pending'
"""

# Default a freshly-onboarded tenant onto the full Kafka pipeline so
# its data-plane envelopes are persisted (not dropped as shadow-only).
# ON CONFLICT DO NOTHING: only establishes the default on first
# onboarding — it never clobbers an explicit later decision, in
# particular the circuit breaker's `auto:circuit_breaker` trip-off
# (which sets the flag FALSE under sustained consumer lag) or an
# operator who deliberately seeded the tenant FALSE.
_ENABLE_KAFKA_PATH_SQL = """
INSERT INTO tenant_flags
    (tenant_id, flag_name, flag_value, set_by, note, set_at)
VALUES ($1, $2, TRUE, 'auto:tenant_onboarding',
        'enabled by default at tenant onboarding', now())
ON CONFLICT (tenant_id, flag_name) DO NOTHING
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

# For the `tenant.onboarding.complete` progress event.
_LOAD_RUN_SOURCES_SQL = """
SELECT source FROM source_onboarding_runs
 WHERE onboarding_run_id = $1
 ORDER BY source
"""

# Total observations for the tenant at completion — the user-facing
# "how much landed" count on `tenant.onboarding.complete`. Tenant-scoped
# (not run-scoped) because onboarding is the tenant's first data load;
# `onboarding_shards.observations_seen` is post-dedup and not maintained
# by the backfill path, so the observations table is the honest source.
_COUNT_TENANT_OBSERVATIONS_SQL = """
SELECT count(*) FROM observations WHERE tenant_id = $1
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


async def _insert_source_row(
    conn: asyncpg.Connection,
    *,
    run_id: UUID, source: str, tenant_id: UUID,
    installation_row_id: UUID,
) -> None:
    await conn.execute(
        _INSERT_SOURCE_ROW_SQL,
        run_id,
        source,
        tenant_id,
        installation_row_id,
    )


async def _enable_kafka_path(
    conn: asyncpg.Connection, tenant_id: UUID,
) -> None:
    """Set `ingestion.kafka_path_enabled=TRUE` for the tenant if unset.

    Idempotent (ON CONFLICT DO NOTHING) and runs inside the
    run-created transaction, so it commits atomically with the
    'running' transition — the flag is TRUE before any backfill
    envelope reaches the observation writer."""
    await conn.execute(_ENABLE_KAFKA_PATH_SQL, tenant_id, KAFKA_PATH_ENABLED)


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
        kafka_producer: Any | None = None,
        config: TenantOnboardingConfig | None = None,
    ) -> None:
        self._pool = pool
        # OPTIONAL: present in production (wired by `_run_orchestrator`),
        # absent in unit tests that only assert signal/DB behaviour. When
        # None, progress-event publishing is a no-op (see
        # `publish_progress_events`).
        self._kafka_producer = kafka_producer
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
        """Claim + dispatch ONE signal, retrying transient serialization
        conflicts on the shared `workflow_signals` table (see
        `process_signal_with_serialization_retry`). Previously an unhandled
        `DeadlockDetectedError` from the signal INSERT crashed the worker
        (rc=1), so NO `tenant_onboarding_completed` signal fired for ANY
        in-flight tenant under concurrent multi-source onboarding."""
        return await process_signal_with_serialization_retry(
            self._process_one_signal_once, label="tenant_onboarding",
        )

    async def _process_one_signal_once(self) -> bool:
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

        Progress events (`tenant.onboarding.started` / `…complete`) are
        returned by the handlers and published AFTER the transaction
        commits — the lifecycle transitions are claim-via-UPDATE guarded,
        so post-commit publish gives the at-least-once + Bridge-dedup
        contract without risking a publish for an uncommitted transition.
        """
        events: list[ProgressEvent] = []
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
                    events = await self._handle_run_created(conn, sig)
                elif sig.signal_kind == SIGNAL_KIND_SOURCE_COMPLETED:
                    events = await self._handle_source_completed(conn, sig)
                else:
                    log.warning(
                        "orchestrator.unknown_signal_kind",
                        extra={
                            "signal_id": str(sig.id),
                            "signal_kind": sig.signal_kind,
                            "workflow_kind": sig.workflow_kind,
                        },
                    )
        await publish_progress_events(self._kafka_producer, events)
        return True

    async def _handle_run_created(
        self, conn: asyncpg.Connection, sig: WorkflowSignal,
    ) -> list[ProgressEvent]:
        """Validate and start one exact source-installation run.

        Returns `[TenantOnboardingStarted]` on the pending→running
        transition (the moment tenant onboarding actually begins);
        `[]` on the idempotent no-op and any fail-closed identity failure
        (a run that never starts emits no `started`)."""
        run_id = UUID(sig.signal_data["onboarding_run_id"])
        tenant_id = UUID(sig.signal_data["tenant_id"])

        run = await _load_run_row(conn, run_id)
        if run is None:
            log.warning(
                "orchestrator.run_missing",
                extra={"run_id": str(run_id), "signal_id": str(sig.id)},
            )
            return []
        if run["status"] != "pending":
            # Idempotency: a re-claimed signal whose run is already
            # advanced is a no-op success.
            return []

        if run["tenant_id"] != tenant_id:
            await conn.execute(
                _MARK_RUN_FAILED_SQL,
                run_id,
                "Onboarding signal tenant does not own the onboarding run.",
            )
            return []

        source = sig.signal_data.get("source")
        if source not in VALID_SOURCES:
            await conn.execute(
                _MARK_RUN_FAILED_SQL,
                run_id,
                f"Source {source!r} is not a historical canonical source.",
            )
            return []

        # Gmail's pre-contract ingress field is accepted at this one boundary,
        # then immediately normalized. Downstream workflows receive only the
        # common installation_row_id field.
        raw_installation_id = (
            sig.signal_data.get("installation_row_id")
            or sig.signal_data.get("gmail_installation_id")
        )
        try:
            installation_row_id = UUID(str(raw_installation_id))
        except (TypeError, ValueError):
            await conn.execute(
                _MARK_RUN_FAILED_SQL,
                run_id,
                "Historical onboarding requires an exact installation UUID.",
            )
            return []

        loader = resolve_installation_loader(source)
        install = await loader(
            conn,
            tenant_id=tenant_id,
            installation_id=installation_row_id,
        )
        if install is None:
            await conn.execute(
                _MARK_RUN_FAILED_SQL,
                run_id,
                "The exact installation is missing, disabled, belongs to "
                "another tenant, or belongs to another source.",
            )
            return []

        await _insert_source_row(
            conn,
            run_id=run_id,
            source=source,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
        )
        await emit_signal(
            conn,
            workflow_kind=SOURCE_ONBOARDING_INBOX_KIND,
            workflow_id=SOURCE_ONBOARDING_INBOX_ID,
            signal_kind=SIGNAL_KIND_SOURCE_REQUESTED,
            idempotency_key=f"{run_id}:{source}:{installation_row_id}",
            signal_data={
                "onboarding_run_id": str(run_id),
                "tenant_id": str(tenant_id),
                "source": source,
                "installation_row_id": str(installation_row_id),
            },
        )

        # Default this tenant onto the full Kafka pipeline so its
        # observations persist (idempotent; never overrides a later
        # operator/circuit-breaker FALSE). Same transaction as the
        # 'running' transition below.
        await _enable_kafka_path(conn, tenant_id)

        await conn.execute(_MARK_RUN_RUNNING_SQL, run_id)

        return [TenantOnboardingStarted(
            tenant_id=tenant_id,
            started_at=dt.datetime.now(tz=dt.timezone.utc),
            sources=[source],  # type: ignore[list-item]
            eta_minutes=ETA_MINUTES_PER_SOURCE,
        )]

    async def _handle_source_completed(
        self, conn: asyncpg.Connection, sig: WorkflowSignal,
    ) -> list[ProgressEvent]:
        """Completion phase. If failure_reason is present in
        signal_data, the source failed and the parent run fails too
        (M6.1 default; no 'partial' status until M6.2+).

        Returns `[TenantOnboardingComplete]` on the all-sources-success
        roll-up (the same transition that emits the Bridge
        `tenant_onboarding_completed` signal); `[]` otherwise (failure,
        not-yet-all-done, sibling-already-failed). There is no
        `tenant.onboarding` failed event in the contract, so failures
        surface no progress event."""
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
            return []

        await _mark_source_completed(conn, run_id=run_id, source=source)

        unfinished = await _count_unfinished_sources(conn, run_id)
        if unfinished > 0:
            return []

        # All sources in terminal state — check if any failed.
        if await _any_source_failed(conn, run_id):
            # A sibling source had already failed; parent already
            # marked 'failed'. Nothing more to do.
            return []

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

        # User-facing `tenant.onboarding.complete` (LLD §6). Sources +
        # observation count are gathered in-transaction; the event is
        # published post-commit by `_process_one_signal`.
        source_rows = await conn.fetch(_LOAD_RUN_SOURCES_SQL, run_id)
        total_observations = int(
            await conn.fetchval(_COUNT_TENANT_OBSERVATIONS_SQL, tenant_id) or 0
        )
        return [TenantOnboardingComplete(
            tenant_id=tenant_id,
            total_observations=total_observations,
            completed_at=dt.datetime.now(tz=dt.timezone.utc),
            sources=[
                r["source"] for r in source_rows
                if r["source"] in VALID_SOURCES
            ],  # type: ignore[misc]
        )]

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
# CLI entrypoint — python -m services.ingest.ingestion.workflows.tenant_onboarding.
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

    from services.ingest.ingestion.kafka.producer import (
        IdempotentProducer,
        ProducerConfig,
    )
    from services.ingest.ingestion.workflows.runtime import (
        make_workflow_pool,
        start_workflow_health,
    )

    pool = await make_workflow_pool(os.environ["DATABASE_URL"])
    # Progress-event producer for `tenant.onboarding.started` / `…complete`
    # (LLD §6 Bridge contract). Same IdempotentProducer the shard_fetch /
    # feels_onboarded services use.
    producer = IdempotentProducer(ProducerConfig(
        bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
        ),
        client_id="workflow-tenant_onboarding",
    ))
    await producer.start()
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
    service = TenantOnboardingOrchestrator(
        pool, kafka_producer=producer, config=config,
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    log.info("workflow.tenant_onboarding.started", extra={
        "instance": config.instance_name,
    })
    # Liveness + metrics surface (opt-in via INGESTION_HEALTH_PORT).
    health_shutdown = start_workflow_health(stop_event)
    try:
        await service.run(stop_event=stop_event)
    finally:
        log.info("workflow.tenant_onboarding.shutting_down")
        await health_shutdown()
        await producer.stop()
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
