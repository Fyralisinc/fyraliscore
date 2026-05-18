"""services/ingestion/workflows/state.py
   — Workflow state persistence + the N1 cursor-advance primitive.

Per ingestion LLD §3.1 (cursor-data ordering invariant) and
[04-implementation-plan.md §M6 pattern-alignment requirement #2]
(state-in-Postgres-not-memory).

============================================================
PATTERN-ALIGNMENT EXEMPTION
============================================================
This module is one of three substrate modules (state.py, signals.py,
runtime.py) that may import `asyncpg` directly. Every other module
under `services/ingestion/workflows/` MUST go through these substrate
modules for DB access — the pattern-alignment static analyzer (M6.0
Phase 3) enforces this.

============================================================
THE N1 INVARIANT
============================================================
`advance_cursor_atomic_with_kafka_publish` is the load-bearing
primitive that makes the LLD §3.1 cursor-data ordering invariant a
property of the running system rather than a written aspiration. The
contract — repeated in three places per the established documentation
pattern — is:

  1. Publish every Kafka message in the batch.
  2. Flush the producer; await broker-acks (A6 precedent — same shape
     as `services/integrations/discord/gateway/_durability.py::pre_save_flush`).
  3. ONLY IF flush returned 0 (all messages broker-acked): UPDATE the
     state row with the new `state_data` payload.

If step 2 fails (broker timeout, leader unavailable, network glitch),
this function raises `CursorAdvanceFlushFailure` and does NOT touch
the state row. The next service tick reads the unchanged state and
republishes — Kafka idempotent-producer dedups the broker side; the
observation UNIQUE constraint dedups the writer side; the N1
invariant ("publish-then-advance, never advance-then-publish") holds.

This is the same ordering A6 fixed for the Discord Gateway worker.
Same precedent test: `test_advance_cursor_atomic_publishes_before_persists`
(M6.0 Phase 1).
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import orjson
from pydantic import BaseModel, ConfigDict, Field


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Errors.
# ---------------------------------------------------------------------
class CursorAdvanceError(Exception):
    """Base class for cursor-advance failures."""


class CursorAdvanceFlushFailure(CursorAdvanceError):
    """Kafka flush did not broker-ack all messages within the timeout.
    The state row was NOT advanced; the caller's next tick will retry
    publish from the same state."""


class CursorAdvanceMissingState(CursorAdvanceError):
    """The state row `(workflow_kind, workflow_id)` does not exist.
    Callers MUST `persist_state` an initial row before calling
    `advance_cursor_atomic_with_kafka_publish`; the function refuses
    to silently create state because that would mask a programming
    error (workflow advancing without a started state)."""


# ---------------------------------------------------------------------
# Models.
# ---------------------------------------------------------------------
class WorkflowState(BaseModel):
    """One row in `workflow_states`.

    `workflow_kind` is the service-family ("feels_onboarded_monitor",
    "oauth_poller", "tenant_onboarding", ...). `workflow_id` is the
    per-instance identifier within the kind ("default" for global
    services; `f"{tenant_id}:{source}"` for per-tenant ones, etc.).

    `state_data` is the JSONB blob the service uses to store its
    cursor / progress / decision state. The schema is service-local
    (each kind defines its own internal shape); the substrate treats
    it as opaque.
    """

    model_config = ConfigDict(extra="forbid")

    workflow_kind: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    tenant_id: UUID | None = None
    state_data: dict[str, Any] = Field(default_factory=dict)
    last_advanced_at: dt.datetime
    paused_at: dt.datetime | None = None


@dataclass(frozen=True)
class KafkaMessage:
    """One Kafka message to publish under the N1 invariant.

    `key` is the partition key (typically `tenant_id` bytes for
    partition affinity per LLD §5.2). Use `bytes` (not str) so the
    producer adapter doesn't have to guess encoding.
    """

    topic: str
    value: bytes
    key: bytes | None = None


# ---------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------
_LOAD_STATE_SQL = """
SELECT workflow_kind, workflow_id, tenant_id, state_data,
       last_advanced_at, paused_at
  FROM workflow_states
 WHERE workflow_kind = $1 AND workflow_id = $2
"""

_UPSERT_STATE_SQL = """
INSERT INTO workflow_states
    (workflow_kind, workflow_id, tenant_id, state_data,
     last_advanced_at, paused_at)
VALUES ($1, $2, $3, $4::jsonb, $5, $6)
ON CONFLICT (workflow_kind, workflow_id) DO UPDATE SET
    tenant_id        = EXCLUDED.tenant_id,
    state_data       = EXCLUDED.state_data,
    last_advanced_at = EXCLUDED.last_advanced_at,
    paused_at        = EXCLUDED.paused_at
"""

# Cursor-advance UPDATE — used ONLY after a successful Kafka flush.
# RETURNING workflow_id lets us detect the missing-state case loudly.
_ADVANCE_STATE_SQL = """
UPDATE workflow_states
   SET state_data       = $1::jsonb,
       last_advanced_at = now()
 WHERE workflow_kind = $2 AND workflow_id = $3
RETURNING workflow_id
"""


# ---------------------------------------------------------------------
# Public API.
# ---------------------------------------------------------------------
async def load_state(
    pool: asyncpg.Pool, workflow_kind: str, workflow_id: str,
) -> WorkflowState | None:
    """Read the state row. Returns None if the workflow has never
    been started. Services typically call `persist_state` with a
    fresh row when this returns None."""
    row = await pool.fetchrow(_LOAD_STATE_SQL, workflow_kind, workflow_id)
    if row is None:
        return None
    raw_state = row["state_data"]
    state_data = (
        orjson.loads(raw_state) if isinstance(raw_state, (str, bytes, bytearray))
        else dict(raw_state)
    )
    return WorkflowState(
        workflow_kind=row["workflow_kind"],
        workflow_id=row["workflow_id"],
        tenant_id=row["tenant_id"],
        state_data=state_data,
        last_advanced_at=row["last_advanced_at"],
        paused_at=row["paused_at"],
    )


async def persist_state(pool: asyncpg.Pool, state: WorkflowState) -> None:
    """UPSERT the state row. Use this for non-cursor-advancing updates:
    initial workflow start, pause/resume toggling, metadata changes
    that don't involve a Kafka publish. For cursor advancement under
    the N1 invariant, use
    `advance_cursor_atomic_with_kafka_publish` instead."""
    await pool.execute(
        _UPSERT_STATE_SQL,
        state.workflow_kind,
        state.workflow_id,
        state.tenant_id,
        orjson.dumps(state.state_data).decode("utf-8"),
        state.last_advanced_at,
        state.paused_at,
    )


async def advance_cursor_atomic_with_kafka_publish(
    pool: asyncpg.Pool,
    kafka_producer: Any,  # services.ingestion.kafka.IdempotentProducer
    *,
    workflow_kind: str,
    workflow_id: str,
    new_state_data: dict[str, Any],
    kafka_messages: list[KafkaMessage],
    flush_timeout_seconds: float = 5.0,
) -> None:
    """The N1 cursor-data ordering invariant primitive (LLD §3.1).

    Contract — three-place documentation:
      [1] Module docstring (top of this file).
      [2] This docstring (the function contract).
      [3] The test `test_advance_cursor_atomic_publishes_before_persists`
          that proves the ordering by injecting a flush failure and
          asserting the state row was NOT advanced.

    Ordering:
      1. For each KafkaMessage in `kafka_messages`: enqueue via
         `kafka_producer.produce(topic, value, key)`.
      2. Await `kafka_producer.flush(timeout=flush_timeout_seconds)`.
         Returns the count of messages still queued. 0 means all
         broker-acked (success).
      3. ONLY IF step 2 returned 0: UPDATE `workflow_states.state_data`
         to `new_state_data` and stamp `last_advanced_at = now()`.

    Failure modes:
      - Flush returns >0 (broker did not ack within timeout) →
        raise `CursorAdvanceFlushFailure`. State row UNCHANGED. The
        caller's next tick reads the same state and republishes; the
        Kafka idempotent-producer dedups the broker side.
      - State row does not exist → raise `CursorAdvanceMissingState`.
        The caller MUST `persist_state` an initial row before
        advancing (refusing to silently create state prevents masking
        a programming error where a workflow advances without having
        started).

    Same ordering as A6's `pre_save_flush` → save in the Discord
    Gateway worker. Same load-bearing test pattern.
    """
    # ---- Step 1: enqueue every message. ----
    for msg in kafka_messages:
        await kafka_producer.produce(
            topic=msg.topic, value=msg.value, key=msg.key,
        )

    # ---- Step 2: flush — broker-ack barrier (A6 precedent). ----
    remaining = await kafka_producer.flush(flush_timeout_seconds)
    if remaining > 0:
        log.warning(
            "workflow.cursor_advance_flush_failed",
            extra={
                "workflow_kind": workflow_kind,
                "workflow_id": workflow_id,
                "remaining": remaining,
                "flush_timeout_seconds": flush_timeout_seconds,
            },
        )
        raise CursorAdvanceFlushFailure(
            f"Kafka flush failed for ({workflow_kind!r}, {workflow_id!r}): "
            f"{remaining} of {len(kafka_messages)} messages still queued "
            f"after {flush_timeout_seconds}s timeout. State NOT advanced; "
            f"caller's next tick will retry publish."
        )

    # ---- Step 3: safe to advance — broker has all messages. ----
    row = await pool.fetchrow(
        _ADVANCE_STATE_SQL,
        orjson.dumps(new_state_data).decode("utf-8"),
        workflow_kind,
        workflow_id,
    )
    if row is None:
        raise CursorAdvanceMissingState(
            f"No workflow_states row for ({workflow_kind!r}, "
            f"{workflow_id!r}). Call `persist_state` with an initial "
            f"row before advancing."
        )


__all__ = [
    "CursorAdvanceError",
    "CursorAdvanceFlushFailure",
    "CursorAdvanceMissingState",
    "KafkaMessage",
    "WorkflowState",
    "advance_cursor_atomic_with_kafka_publish",
    "load_state",
    "persist_state",
]
