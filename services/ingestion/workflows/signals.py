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
`poll_signals` / `signal_count` — for cross-service handoffs instead
of using `asyncio.Queue`, shared module state, or `multiprocessing`
primitives. The pattern-alignment static analyzer (M6.0 Phase 3)
enforces this.

============================================================
TEMPORAL MAPPING (A11 trigger conditions)
============================================================
This module's API maps 1:1 to Temporal's signal API when the
[A11 trigger conditions](../../../docs/ingestion/05-lld-amendments.md)
fire and the Temporal port is opened:

  - `emit_signal(...)` → `client.get_workflow_handle(workflow_id).signal(...)`.
  - `poll_signals(...)` → `@workflow.signal` handler + an
    `asyncio.Queue` inside the workflow.
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
# Public API.
# ---------------------------------------------------------------------
async def emit_signal(
    pool: asyncpg.Pool,
    *,
    workflow_kind: str,
    workflow_id: str,
    signal_kind: str,
    idempotency_key: str,
    signal_data: dict[str, Any] | None = None,
) -> EmitResult:
    """Insert a signal row, idempotent on `idempotency_key`.

    Contract — three-part:
      (a) Same `(workflow_kind, workflow_id, signal_kind,
          idempotency_key)` across two calls collides on the schema
          UNIQUE constraint.
      (b) After both calls, exactly ONE row exists.
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
    inserted = await pool.fetchval(
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
    existing = await pool.fetchval(
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


async def poll_signals(
    pool: asyncpg.Pool,
    *,
    workflow_kind: str,
    workflow_id: str,
    consumed_by: str,
    batch_size: int = 32,
) -> AsyncIterator[WorkflowSignal]:
    """Claim and yield up to `batch_size` unclaimed signals for this
    workflow, oldest first.

    Concurrency contract:
      - Two concurrent calls with the same `(workflow_kind,
        workflow_id)` claim DISJOINT subsets — `FOR UPDATE SKIP LOCKED`
        guarantees no overlap.
      - Each yielded signal has `consumed_at = now()` and
        `consumed_by = <this caller's value>` already committed before
        the iterator yields it.

    Why an async iterator: callers typically process each signal in
    sequence; yielding lets them apply per-signal handling without
    accumulating the whole batch in memory. For the small batch
    sizes M6 uses (≤32), this is mostly a style choice; the contract
    is the claim semantics.

    `consumed_by` is an audit string (the service name / instance id).
    It's stored alongside `consumed_at` for "who polled this and
    when" forensics.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                _CLAIM_SIGNALS_SQL,
                workflow_kind, workflow_id, batch_size, consumed_by,
            )
    # The transaction has committed by this point — the rows are
    # marked consumed in the DB. If the caller's consumer raises
    # AFTER we yield, the row is still consumed; producers re-emit
    # with a fresh idempotency_key if re-delivery is needed. Same
    # at-most-once-across-pollers / at-least-once-across-restarts
    # contract as `services/think/post_commit.py`.
    for row in rows:
        raw = row["signal_data"]
        data = (
            orjson.loads(raw) if isinstance(raw, (str, bytes, bytearray))
            else dict(raw)
        )
        yield WorkflowSignal(
            id=row["id"],
            workflow_kind=row["workflow_kind"],
            workflow_id=row["workflow_id"],
            signal_kind=row["signal_kind"],
            signal_data=data,
            idempotency_key=row["idempotency_key"],
        )


async def signal_count(
    pool: asyncpg.Pool,
    *,
    workflow_kind: str,
    workflow_id: str,
) -> int:
    """Return the count of UNCONSUMED signals for this workflow.
    Useful for operator queries ("is the poller falling behind?")
    and for tests asserting backlog state.
    """
    val = await pool.fetchval(
        _COUNT_UNCONSUMED_SQL, workflow_kind, workflow_id,
    )
    return int(val or 0)


__all__ = [
    "EmitResult",
    "WorkflowSignal",
    "emit_signal",
    "poll_signals",
    "signal_count",
]
