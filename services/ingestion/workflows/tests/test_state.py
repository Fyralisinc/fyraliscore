"""M6.0 Phase 1 — workflow state tests.

Covers:
  - load/persist round-trip.
  - The N1 cursor-data ordering invariant primitive
    (advance_cursor_atomic_with_kafka_publish):
      * LOAD-BEARING: flush failure → state NOT advanced.
      * Happy path: flush success → state advanced; reload yields the
        new state.
      * Missing-state error path.
"""
from __future__ import annotations

import datetime as dt
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from services.ingestion.workflows.state import (
    CursorAdvanceFlushFailure,
    CursorAdvanceMissingState,
    KafkaMessage,
    WorkflowState,
    advance_cursor_atomic_with_kafka_publish,
    load_state,
    persist_state,
)


pytestmark = [pytest.mark.timeout(60)]


_NOW = dt.datetime(2026, 5, 18, 12, 0, 0, tzinfo=dt.timezone.utc)


# =====================================================================
# Fakes
# =====================================================================

class _CapturingProducer:
    """IdempotentProducer stand-in for unit-level tests.

    `produce` captures the call; `flush` returns whatever the test
    pre-configures via `pending_after_flush`. Set to 0 for "broker
    acked everything" (happy path) or >0 for "broker timeout"
    (the LOAD-BEARING failure-path test).
    """

    def __init__(self, *, pending_after_flush: int = 0) -> None:
        self.published: list[tuple[str, bytes, bytes | None]] = []
        self.flush_calls: list[float] = []
        self.pending_after_flush = pending_after_flush

    async def produce(
        self, topic: str, value: bytes, *,
        key: bytes | None = None, **_kw: Any,
    ) -> None:
        self.published.append((topic, value, key))

    async def flush(self, timeout_seconds: float = 10.0) -> int:
        self.flush_calls.append(timeout_seconds)
        return self.pending_after_flush


# =====================================================================
# Helpers
# =====================================================================

async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"workflow-state-test-{tid.hex[:8]}",
    )
    return tid


# =====================================================================
# 1. load/persist round-trip.
# =====================================================================

async def test_load_persist_state_round_trip(fresh_db: asyncpg.Pool) -> None:
    tid = await _seed_tenant(fresh_db)
    state = WorkflowState(
        workflow_kind="test_kind",
        workflow_id="instance-1",
        tenant_id=tid,
        state_data={"cursor": "page-3", "pages_seen": 3},
        last_advanced_at=_NOW,
        paused_at=None,
    )
    await persist_state(fresh_db, state)

    reloaded = await load_state(fresh_db, "test_kind", "instance-1")
    assert reloaded is not None
    assert reloaded.workflow_kind == "test_kind"
    assert reloaded.workflow_id == "instance-1"
    assert reloaded.tenant_id == tid
    assert reloaded.state_data == {"cursor": "page-3", "pages_seen": 3}
    assert reloaded.paused_at is None

    # Updating the same key UPSERTs (no duplicate row).
    updated = state.model_copy(update={
        "state_data": {"cursor": "page-4", "pages_seen": 4},
        "last_advanced_at": _NOW + dt.timedelta(seconds=30),
    })
    await persist_state(fresh_db, updated)

    reloaded2 = await load_state(fresh_db, "test_kind", "instance-1")
    assert reloaded2 is not None
    assert reloaded2.state_data == {"cursor": "page-4", "pages_seen": 4}

    # Confirm there's still only one row for this (kind, id).
    row_count = await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_states "
        "WHERE workflow_kind = $1 AND workflow_id = $2",
        "test_kind", "instance-1",
    )
    assert row_count == 1


async def test_load_state_returns_none_for_missing_row(
    fresh_db: asyncpg.Pool,
) -> None:
    assert await load_state(fresh_db, "absent_kind", "absent-id") is None


# =====================================================================
# 2. LOAD-BEARING — flush failure does NOT advance cursor.
#     The N1 cursor-data ordering invariant proof.
# =====================================================================

async def test_advance_cursor_atomic_publishes_before_persists(
    fresh_db: asyncpg.Pool,
) -> None:
    """LOAD-BEARING (M6.0): inject a Kafka flush failure. The state row
    MUST remain at its pre-attempt value; CursorAdvanceFlushFailure
    MUST propagate. This is the N1 ordering invariant — "publish-
    then-advance, never advance-then-publish" — verified by observing
    Postgres state, not by mocking internal call order.
    """
    tid = await _seed_tenant(fresh_db)
    initial = WorkflowState(
        workflow_kind="fetch_shard",
        workflow_id="shard-77",
        tenant_id=tid,
        state_data={"cursor": "page-0", "pages_seen": 0},
        last_advanced_at=_NOW,
        paused_at=None,
    )
    await persist_state(fresh_db, initial)

    # Producer where flush() reports 2 messages still queued — simulates
    # broker timeout / leader unavailable.
    producer = _CapturingProducer(pending_after_flush=2)

    msgs = [
        KafkaMessage(
            topic="ingestion.raw",
            value=b'{"page": "1"}',
            key=str(tid).encode("utf-8"),
        ),
        KafkaMessage(
            topic="ingestion.raw",
            value=b'{"page": "2"}',
            key=str(tid).encode("utf-8"),
        ),
    ]

    with pytest.raises(CursorAdvanceFlushFailure) as excinfo:
        await advance_cursor_atomic_with_kafka_publish(
            fresh_db, producer,
            workflow_kind="fetch_shard",
            workflow_id="shard-77",
            new_state_data={"cursor": "page-2", "pages_seen": 2},
            kafka_messages=msgs,
            flush_timeout_seconds=2.0,
        )
    assert "2 of 2" in str(excinfo.value)
    assert "shard-77" in str(excinfo.value)

    # ---- Observable state #1: state row UNCHANGED ----
    reloaded = await load_state(fresh_db, "fetch_shard", "shard-77")
    assert reloaded is not None
    assert reloaded.state_data == {"cursor": "page-0", "pages_seen": 0}, (
        f"N1 INVARIANT VIOLATED: state_data is {reloaded.state_data!r} "
        f"after a failed flush; expected the pre-attempt value "
        f"{{'cursor': 'page-0', 'pages_seen': 0}}. The cursor "
        f"advance happened BEFORE the publish was confirmed."
    )
    assert reloaded.last_advanced_at == _NOW, (
        "last_advanced_at was bumped despite the flush failure"
    )

    # ---- Observable state #2: both messages were enqueued ----
    # (publish-then-flush-then-advance: produce calls happen first;
    # the flush is what fails.)
    assert len(producer.published) == 2
    assert producer.flush_calls == [2.0]


# =====================================================================
# 3. Happy path — flush success → state advanced.
# =====================================================================

async def test_advance_cursor_atomic_happy_path(
    fresh_db: asyncpg.Pool,
) -> None:
    """When the producer's flush returns 0 (broker acked everything),
    the state row is advanced and reload returns the new state.
    Equivalent to the testcontainers real-Kafka happy path but
    deterministic — the real-broker behaviour is already tested by
    the A6 + M3.3 + test_e2e_shadow suites.
    """
    tid = await _seed_tenant(fresh_db)
    initial_advanced_at = dt.datetime(
        2020, 1, 1, tzinfo=dt.timezone.utc,
    )  # clearly in the past so the post-advance time is later.
    await persist_state(fresh_db, WorkflowState(
        workflow_kind="fetch_shard",
        workflow_id="shard-happy",
        tenant_id=tid,
        state_data={"cursor": "page-0"},
        last_advanced_at=initial_advanced_at,
    ))

    producer = _CapturingProducer(pending_after_flush=0)
    msgs = [KafkaMessage(
        topic="ingestion.raw",
        value=b'{"page": "1"}',
        key=str(tid).encode("utf-8"),
    )]

    await advance_cursor_atomic_with_kafka_publish(
        fresh_db, producer,
        workflow_kind="fetch_shard",
        workflow_id="shard-happy",
        new_state_data={"cursor": "page-1"},
        kafka_messages=msgs,
        flush_timeout_seconds=1.0,
    )

    reloaded = await load_state(fresh_db, "fetch_shard", "shard-happy")
    assert reloaded is not None
    assert reloaded.state_data == {"cursor": "page-1"}
    assert reloaded.last_advanced_at > initial_advanced_at, (
        f"last_advanced_at should be bumped to now() on advance; "
        f"got {reloaded.last_advanced_at!r}, was {initial_advanced_at!r}."
    )
    assert len(producer.published) == 1
    assert producer.flush_calls == [1.0]


# =====================================================================
# 4. Missing state row raises (no silent INSERT).
# =====================================================================

async def test_advance_cursor_atomic_missing_state_raises(
    fresh_db: asyncpg.Pool,
) -> None:
    """If no state row exists for (workflow_kind, workflow_id), the
    advance MUST raise — silent INSERT would mask a programming error
    where a workflow advances without having started.
    """
    producer = _CapturingProducer(pending_after_flush=0)
    with pytest.raises(CursorAdvanceMissingState) as excinfo:
        await advance_cursor_atomic_with_kafka_publish(
            fresh_db, producer,
            workflow_kind="never_started",
            workflow_id="phantom-shard",
            new_state_data={"cursor": "page-1"},
            kafka_messages=[KafkaMessage(
                topic="ingestion.raw",
                value=b"x",
            )],
        )
    assert "never_started" in str(excinfo.value)
    assert "phantom-shard" in str(excinfo.value)
    # Publish DID happen — that's fine; Kafka idempotent-producer
    # dedups on retry. The contract is "no SILENT state INSERT."
    assert len(producer.published) == 1
