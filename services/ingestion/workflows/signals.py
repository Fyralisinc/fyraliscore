"""services/ingestion/workflows/signals.py
   — Postgres-table-based signal polling for cross-service communication.

Per [04-implementation-plan.md §M6 pattern-alignment requirement #4]
(signals-via-Postgres-polling) and migration 0054's `workflow_signals`
table.

============================================================
PATTERN-ALIGNMENT EXEMPTION
============================================================
This module is one of three substrate modules (state.py, signals.py,
runtime.py) that may import `asyncpg` directly. Every concrete
workflow service MUST go through this module — `emit_signal` /
`poll_signals` / `claim_signals` / `signal_count` — for cross-service
handoffs instead of using `asyncio.Queue`, shared module state, or
`multiprocessing` primitives. The pattern-alignment static analyzer
(M6.0 Phase 3) enforces this.

============================================================
EXECUTOR-TYPED SURFACE (M6.0 substrate amendment — A12)
============================================================
The DB-touching functions accept `asyncpg.Pool | asyncpg.Connection`
(spelled as a union at each parameter; no aliased name). Semantics:

  - **Pool** — the function opens-and-closes a connection per call.
    Each call commits independently. This is the simple-caller shape
    (`FeelsOnboardedMonitor` and any service that doesn't need to
    extend the substrate operation with adjacent writes).

  - **Connection** — the function uses the caller's connection and
    participates in whatever transaction the caller has open. The
    caller MUST be inside `async with conn.transaction(): ...` when
    the function does INSERT/UPDATE work; otherwise the substrate
    operation autocommits and the caller's "atomic" assumption is
    silently violated. M6.1's OAuth poller and TenantOnboarding
    orchestrator are the first consumers of this shape.

Two distinct claim entry points:

  - `poll_signals(pool, ...)` — substrate-managed atomicity. Opens
    its own connection and transaction, claims under SKIP LOCKED,
    commits, returns an async iterator. Use this when you don't need
    to extend the claim with additional writes.

  - `claim_signals(conn, ...)` — caller-managed atomicity. Returns a
    `list[WorkflowSignal]`. MUST be called inside an open transaction
    on `conn`; the caller's transaction's commit (or rollback) is
    what makes the claim durable (or undone). Use this when the claim
    must be atomic with subsequent state writes (the M6.1 case:
    consume `onboarding_run_created` signal + insert `source_onboarding_runs`
    rows + emit `source_onboarding_requested` signals as one txn).

`poll_signals` is now a thin wrapper that delegates to `claim_signals`
under a substrate-opened transaction. No external behaviour change.

============================================================
TEMPORAL MAPPING (A11 trigger conditions)
============================================================
This module's API maps 1:1 to Temporal's signal API when the
[A11 trigger conditions](../../../docs/ingestion/05-lld-amendments.md)
fire and the Temporal port is opened:

  - `emit_signal(...)` → `client.get_workflow_handle(workflow_id).signal(...)`.
  - `poll_signals(...)` / `claim_signals(...)` → `@workflow.signal`
    handler + an `asyncio.Queue` inside the workflow. Temporal's
    signal handling is inherently inside a workflow execution; the
    in-transaction-vs-substrate-managed distinction collapses there.
  - `signal_count(...)` → a query handler.

The idempotency contract is the load-bearing piece: every
`emit_signal` call MUST pass a non-empty `idempotency_key`. The
schema (migration 0054) enforces `NOT NULL`. A producer calling
`emit_signal(...)` twice with the same key is a no-op success on the
second call — exactly the shape Temporal's signal API has via
`SignalWithStartWorkflowOptions.idempotency_key`. Without (c) in the
contract below, callers would think the second emit failed and retry
endlessly.

============================================================
EMIT / CLAIM CONTRACT
============================================================
  - `emit_signal(...)` is idempotent on `(workflow_kind, workflow_id,
    signal_kind, idempotency_key)`. ON CONFLICT DO NOTHING means the
    second call with the same key SUCCEEDS WITHOUT EXCEPTION; the
    return value `was_new` distinguishes "this call inserted a row"
    from "the row already existed."
  - `poll_signals(...)` uses `SELECT ... FOR UPDATE SKIP LOCKED` so
    multiple polling services compete safely. Each polled signal is
    stamped `consumed_at = now()` + `consumed_by = <poller_name>`
    within the same transaction that holds the lock — once committed,
    no other poller can re-claim the same row.
  - Claim-and-mark is one transaction. If the polling service crashes
    BETWEEN `poll_signals` returning a signal and the service
    finishing the handler, the row is still marked consumed. This is
    deliberate: re-delivery semantics are the producer's
    responsibility (re-emit with a fresh idempotency key); the
    consumer is "at-most-once across pollers, at-least-once across
    process restarts via re-emit." Same shape as M1's outbox-poller
    and `services/think/post_commit.py` workers.
  - `claim_signals(...)` — same SKIP LOCKED semantics, but inside
    the caller's transaction. If the caller rolls back, the claim is
    undone (the signal becomes available again to another poller).
    This is the asymmetry with `poll_signals` — substrate-managed
    pollers can't roll back the claim; caller-managed pollers can.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, AsyncIterator
from uuid import UUID

import asyncpg
import orjson
from pydantic import BaseModel, ConfigDict, Field

from lib.shared.ids import uuid7


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Models.
# ---------------------------------------------------------------------
class WorkflowSignal(BaseModel):
    """One row in `workflow_signals`.

    `signal_kind` is the producer-defined event type (e.g.
    `"source_started"`, `"shard_complete"`). `signal_data` is the
    JSONB payload; the substrate treats it as opaque.

    `idempotency_key` is REQUIRED (NOT NULL in the schema). Callers
    that need dedup pass a stable key (e.g.
    `f"feels_onboarded:{tenant}:{source}"`); callers that don't want
    dedup pass `uuid7().hex`. There is no "key not provided" option —
    the schema and the Pydantic model both enforce this.
    """

    model_config = ConfigDict(extra="forbid")

    id: UUID
    workflow_kind: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    signal_kind: str = Field(min_length=1)
    signal_data: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str = Field(min_length=1)


@dataclass(frozen=True)
class EmitResult:
    """Return value from `emit_signal`.

    `signal_id`: the id of the row that holds this logical signal (the
    one already-there or the freshly-inserted one).
    `was_new`: True if this call inserted; False if the
    `idempotency_key` already had a row (the no-op-success case).
    """

    signal_id: UUID
    was_new: bool


# ---------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------
_EMIT_SIGNAL_SQL = """
INSERT INTO workflow_signals
    (id, workflow_kind, workflow_id, signal_kind, signal_data,
     idempotency_key)
VALUES ($1, $2, $3, $4, $5::jsonb, $6)
ON CONFLICT (workflow_kind, workflow_id, signal_kind, idempotency_key)
    DO NOTHING
RETURNING id
"""

# Look up the existing row's id when ON CONFLICT DO NOTHING swallowed
# the insert. We need it so the caller has a stable identifier
# regardless of which call won the race.
_FETCH_EXISTING_ID_SQL = """
SELECT id FROM workflow_signals
 WHERE workflow_kind = $1
   AND workflow_id = $2
   AND signal_kind = $3
   AND idempotency_key = $4
"""

# Claim-and-mark with SKIP LOCKED. Same shape as
# `services/think/post_commit.py::fetch_pending_actions` and the M1
# outbox-poller. Two concurrent pollers running this CTE-style
# UPDATE...FROM(SELECT...FOR UPDATE SKIP LOCKED) get disjoint rows;
# the locked-but-not-yet-committed rows are skipped, not waited on.
_CLAIM_SIGNALS_SQL = """
WITH claimed AS (
    SELECT id
      FROM workflow_signals
     WHERE workflow_kind = $1
       AND workflow_id = $2
       AND consumed_at IS NULL
     ORDER BY created_at ASC
     LIMIT $3
     FOR UPDATE SKIP LOCKED
)
UPDATE workflow_signals s
   SET consumed_at = now(),
       consumed_by = $4
  FROM claimed
 WHERE s.id = claimed.id
RETURNING s.id, s.workflow_kind, s.workflow_id,
          s.signal_kind, s.signal_data, s.idempotency_key
"""

# Unclaimed-only count for the polling diagnostic / `signal_count` API.
_COUNT_UNCONSUMED_SQL = """
SELECT count(*) FROM workflow_signals
 WHERE workflow_kind = $1
   AND workflow_id = $2
   AND consumed_at IS NULL
"""


# ---------------------------------------------------------------------
# Internal helpers.
# ---------------------------------------------------------------------
def _row_to_signal(row: asyncpg.Record) -> WorkflowSignal:
    raw = row["signal_data"]
    data = (
        orjson.loads(raw) if isinstance(raw, (str, bytes, bytearray))
        else dict(raw)
    )
    return WorkflowSignal(
        id=row["id"],
        workflow_kind=row["workflow_kind"],
        workflow_id=row["workflow_id"],
        signal_kind=row["signal_kind"],
        signal_data=data,
        idempotency_key=row["idempotency_key"],
    )


# ---------------------------------------------------------------------
# Public API.
# ---------------------------------------------------------------------
async def emit_signal(
    executor: asyncpg.Pool | asyncpg.Connection,
    *,
    workflow_kind: str,
    workflow_id: str,
    signal_kind: str,
    idempotency_key: str,
    signal_data: dict[str, Any] | None = None,
) -> EmitResult:
    """Insert a signal row, idempotent on `idempotency_key`.

    `executor` accepts either:
      - `asyncpg.Pool` — the function opens-and-closes a connection
        per call; the INSERT commits independently. Use this when the
        emit is not part of a larger atomic operation.
      - `asyncpg.Connection` — the INSERT runs on the caller's
        connection. If the caller is inside `async with
        conn.transaction(): ...`, the emit is part of that
        transaction (commits/rolls back atomically with the caller's
        other writes). M6.1's OAuth poller relies on this to keep
        trigger-consume + onboarding-run-insert + signal-emit atomic.

    Contract — three-part:
      (a) Same `(workflow_kind, workflow_id, signal_kind,
          idempotency_key)` across two calls collides on the schema
          UNIQUE constraint.
      (b) After both calls (committed), exactly ONE row exists.
      (c) The second call SUCCEEDS WITHOUT EXCEPTION; `was_new=False`
          identifies it as a no-op. Callers MUST NOT retry on the
          second-call result — it already landed.

    `signal_data` defaults to `{}` if omitted.
    """
    if not idempotency_key:
        # The schema enforces NOT NULL; this catches the empty-string
        # case which Postgres would store. Both are programming errors.
        raise ValueError(
            "idempotency_key is required and must be non-empty. "
            "Pass a stable key for dedup or `uuid7().hex` for a "
            "single-shot emit."
        )
    signal_data = signal_data or {}
    new_id = uuid7()
    inserted = await executor.fetchval(
        _EMIT_SIGNAL_SQL,
        new_id,
        workflow_kind,
        workflow_id,
        signal_kind,
        orjson.dumps(signal_data).decode("utf-8"),
        idempotency_key,
    )
    if inserted is not None:
        return EmitResult(signal_id=inserted, was_new=True)
    # ON CONFLICT DO NOTHING — fetch the existing row's id so the
    # caller has a stable identifier either way.
    existing = await executor.fetchval(
        _FETCH_EXISTING_ID_SQL,
        workflow_kind, workflow_id, signal_kind, idempotency_key,
    )
    if existing is None:
        # Should be unreachable: we just observed the conflict.
        raise RuntimeError(
            f"emit_signal: insert collided on idempotency_key "
            f"{idempotency_key!r} but the existing row could not be "
            f"located. workflow=({workflow_kind!r}, {workflow_id!r})."
        )
    return EmitResult(signal_id=existing, was_new=False)


async def claim_signals(
    conn: asyncpg.Connection,
    *,
    workflow_kind: str,
    workflow_id: str,
    consumed_by: str,
    batch_size: int = 32,
) -> list[WorkflowSignal]:
    """Claim up to `batch_size` unclaimed signals under SKIP LOCKED.

    Caller-managed atomicity: this function does NOT open its own
    transaction. The caller MUST be inside
    `async with conn.transaction(): ...` when calling this. The
    `FOR UPDATE` lock is released when the caller's transaction
    commits or rolls back; if the caller rolls back, the claim is
    undone and another poller may re-claim the signals.

    Returns a `list[WorkflowSignal]` (not an async iterator) — the
    caller is already inside their own transaction, so accumulating
    the batch in memory is cheap and the iterator-style return would
    complicate transaction scoping.

    Use `poll_signals(pool, ...)` if you want substrate-managed
    atomicity (the substrate opens its own connection + transaction,
    commits the claim, and returns an iterator). This function exists
    for callers (M6.1+) that need to extend the claim with adjacent
    writes in the same transaction.

    Concurrency contract:
      - Two concurrent calls with the same `(workflow_kind,
        workflow_id)` on DIFFERENT connections in DIFFERENT
        transactions claim DISJOINT subsets — `FOR UPDATE SKIP
        LOCKED` guarantees no overlap.
      - Each returned signal has `consumed_at = now()` and
        `consumed_by = <this caller's value>` staged in the caller's
        transaction. Durability depends on the caller's commit.
    """
    rows = await conn.fetch(
        _CLAIM_SIGNALS_SQL,
        workflow_kind, workflow_id, batch_size, consumed_by,
    )
    return [_row_to_signal(row) for row in rows]


async def poll_signals(
    pool: asyncpg.Pool,
    *,
    workflow_kind: str,
    workflow_id: str,
    consumed_by: str,
    batch_size: int = 32,
) -> AsyncIterator[WorkflowSignal]:
    """Claim and yield up to `batch_size` unclaimed signals for this
    workflow, oldest first. Substrate-managed atomicity.

    This is a thin wrapper that delegates to `claim_signals` under a
    substrate-opened connection + transaction. Use this when you do
    NOT need to extend the claim with adjacent writes; use
    `claim_signals(conn, ...)` if you do.

    Concurrency contract:
      - Two concurrent calls with the same `(workflow_kind,
        workflow_id)` claim DISJOINT subsets — `FOR UPDATE SKIP LOCKED`
        guarantees no overlap.
      - Each yielded signal has `consumed_at = now()` and
        `consumed_by = <this caller's value>` already committed before
        the iterator yields it.

    `consumed_by` is an audit string (the service name / instance id).
    It's stored alongside `consumed_at` for "who polled this and
    when" forensics.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            signals = await claim_signals(
                conn,
                workflow_kind=workflow_kind,
                workflow_id=workflow_id,
                consumed_by=consumed_by,
                batch_size=batch_size,
            )
    # The transaction has committed by this point — the rows are
    # marked consumed in the DB. If the caller's consumer raises
    # AFTER we yield, the row is still consumed; producers re-emit
    # with a fresh idempotency_key if re-delivery is needed. Same
    # at-most-once-across-pollers / at-least-once-across-restarts
    # contract as `services/think/post_commit.py`.
    for sig in signals:
        yield sig


async def signal_count(
    executor: asyncpg.Pool | asyncpg.Connection,
    *,
    workflow_kind: str,
    workflow_id: str,
) -> int:
    """Return the count of UNCONSUMED signals for this workflow.

    Useful for operator queries ("is the poller falling behind?")
    and for tests asserting backlog state.

    `executor` accepts a Pool (opens-and-closes a connection per
    call) or a Connection (reads on the caller's connection). The
    count reflects whatever is visible to that executor: a
    Connection inside an uncommitted transaction sees its own
    pending claims as consumed, the Pool sees the committed state.
    """
    val = await executor.fetchval(
        _COUNT_UNCONSUMED_SQL, workflow_kind, workflow_id,
    )
    return int(val or 0)


__all__ = [
    "EmitResult",
    "WorkflowSignal",
    "claim_signals",
    "emit_signal",
    "poll_signals",
    "signal_count",
]
