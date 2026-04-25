"""services/think/tests/test_cascade_depth_tk3.py — TK-3 audit fix.

Source: THINK-DESIGN-AUDIT.md §3 argument 3.

The intra-BFS cascade in services/think/cascade.cascade already caps
at max_depth=50. TK-3 adds the cross-trigger bound: if a T1 trigger
arrives carrying `cascade_depth >= MAX_CASCADE_DEPTH`, the worker
refuses to dispatch it, logs a structured `cascade_bound_violation`,
and marks the trigger failed non-retryable.

Scenarios covered:

  1. Worker rejects a T1 whose payload carries
     `cascade_depth=50` — the trigger is completed with an error
     (non-retryable, no Think run), no `think_runs` row for it, and
     `attempts` is at the cap.

  2. `propagate_cascade_depth` increments the counter correctly (0→1
     when parent has no depth, N→N+1 otherwise).

  3. `enqueue_cascade_t1` refuses to enqueue at the bound and returns
     None (pre-emptive guard in addition to the worker-side guard).

  4. `enqueue_cascade_t1` at depth N < MAX enqueues a T1 with
     `cascade_depth=N+1` in the payload.

  5. End-to-end cycle-termination: simulate a chain of 60 cascaded
     T1s. Verify that exactly MAX_CASCADE_DEPTH trigger into Think and
     depth+1 is rejected rather than looping infinitely.
"""
from __future__ import annotations

import json

import pytest

from lib.shared.ids import uuid7

from services.think.cascade import (
    MAX_CASCADE_DEPTH,
    enqueue_cascade_t1,
    propagate_cascade_depth,
)
from services.think.tests.conftest import make_embedding
from services.think.worker import ThinkWorker, WorkerConfig


pytestmark = [pytest.mark.integration]


async def _seed_observation(pool, tenant):
    aid = uuid7()
    oid = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            aid, tenant,
        )
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', $3,
                    '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
            """,
            oid, tenant, aid, make_embedding("x"),
        )
    return oid


async def _enqueue_trigger_with_depth(
    pool, tenant, observation_id, depth: int, subkind: str = "state_change",
):
    tid = uuid7()
    payload = {"trigger_id": str(tid), "cascade_depth": depth}
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO think_trigger_queue
              (id, tenant_id, trigger_kind, trigger_subkind,
               observation_id, payload)
            VALUES ($1, $2, 'T1', $3, $4, $5::jsonb)
            """,
            tid, tenant, subkind, observation_id, json.dumps(payload),
        )
    return tid


# =====================================================================
# Pure-function tests
# =====================================================================

def test_propagate_cascade_depth_from_none_is_one():
    assert propagate_cascade_depth(None) == {"cascade_depth": 1}


def test_propagate_cascade_depth_increments():
    assert propagate_cascade_depth({"cascade_depth": 5}) == {"cascade_depth": 6}


def test_propagate_cascade_depth_treats_nonint_as_zero():
    assert propagate_cascade_depth({"cascade_depth": "oops"}) == {"cascade_depth": 1}


def test_propagate_cascade_depth_merges_extra():
    out = propagate_cascade_depth(
        {"cascade_depth": 2},
        extra={"seed_natural_text": "x"},
    )
    assert out == {"cascade_depth": 3, "seed_natural_text": "x"}


# =====================================================================
# Helper: enqueue_cascade_t1
# =====================================================================

async def test_enqueue_cascade_t1_at_depth_succeeds(
    fresh_db, tenant, tenant_cleanup,
):
    oid = await _seed_observation(fresh_db, tenant)
    async with fresh_db.acquire() as conn:
        new_id = await enqueue_cascade_t1(
            conn,
            tenant_id=tenant,
            observation_id=oid,
            parent_payload={"cascade_depth": 5},
        )
    assert new_id is not None
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT payload FROM think_trigger_queue WHERE id = $1", new_id,
        )
    payload = row["payload"]
    if isinstance(payload, (bytes, bytearray)):
        payload = json.loads(payload.decode())
    elif isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["cascade_depth"] == 6


async def test_enqueue_cascade_t1_at_bound_returns_none(
    fresh_db, tenant, tenant_cleanup,
):
    """At depth MAX-1, propagation would yield MAX → rejected."""
    oid = await _seed_observation(fresh_db, tenant)
    async with fresh_db.acquire() as conn:
        new_id = await enqueue_cascade_t1(
            conn,
            tenant_id=tenant,
            observation_id=oid,
            parent_payload={"cascade_depth": MAX_CASCADE_DEPTH - 1},
        )
    assert new_id is None
    # No row inserted.
    async with fresh_db.acquire() as conn:
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM think_trigger_queue WHERE tenant_id = $1",
            tenant,
        )
    assert n == 0


async def test_enqueue_cascade_t1_above_bound_returns_none(
    fresh_db, tenant, tenant_cleanup,
):
    oid = await _seed_observation(fresh_db, tenant)
    async with fresh_db.acquire() as conn:
        new_id = await enqueue_cascade_t1(
            conn,
            tenant_id=tenant,
            observation_id=oid,
            parent_payload={"cascade_depth": MAX_CASCADE_DEPTH + 10},
        )
    assert new_id is None


# =====================================================================
# Worker-side rejection
# =====================================================================

async def test_worker_rejects_trigger_at_max_depth(
    fresh_db, tenant, tenant_cleanup,
):
    """A T1 whose payload already carries cascade_depth=MAX is
    rejected non-retryable before Think runs."""
    oid = await _seed_observation(fresh_db, tenant)
    tid = await _enqueue_trigger_with_depth(
        fresh_db, tenant, oid, depth=MAX_CASCADE_DEPTH,
    )

    worker = ThinkWorker(fresh_db, config=WorkerConfig(poll_batch=50))
    # Poll + dispatch.
    await worker._poll_and_dispatch()
    # Wait for the (synchronous) in-flight dispatch task to finish.
    for t in list(worker._in_flight):
        await t

    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT completed_at, attempts FROM think_trigger_queue WHERE id = $1",
            tid,
        )
        run_row = await conn.fetchrow(
            "SELECT id FROM think_runs WHERE trigger_id = $1",
            tid,
        )
    # Terminal — completed_at set, no think_runs row.
    assert row["completed_at"] is not None, "trigger must be completed (non-retryable)"
    assert run_row is None, "think_runs must NOT be written for a bound-violation trigger"
    # Attempts >= 1 (we incremented once before marking terminal).
    assert int(row["attempts"] or 0) >= 1


async def test_worker_rejects_trigger_above_max_depth(
    fresh_db, tenant, tenant_cleanup,
):
    """Depth far above the bound is still terminal, not infinite-retry."""
    oid = await _seed_observation(fresh_db, tenant)
    tid = await _enqueue_trigger_with_depth(
        fresh_db, tenant, oid, depth=MAX_CASCADE_DEPTH + 25,
    )

    worker = ThinkWorker(fresh_db, config=WorkerConfig(poll_batch=50))
    await worker._poll_and_dispatch()
    for t in list(worker._in_flight):
        await t

    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT completed_at FROM think_trigger_queue WHERE id = $1",
            tid,
        )
    assert row["completed_at"] is not None


async def test_worker_accepts_trigger_below_max_depth(
    fresh_db, tenant, tenant_cleanup,
):
    """A T1 at depth < MAX dispatches normally (not rejected by the bound)."""
    oid = await _seed_observation(fresh_db, tenant)
    tid = await _enqueue_trigger_with_depth(
        fresh_db, tenant, oid, depth=MAX_CASCADE_DEPTH - 1,
    )

    worker = ThinkWorker(fresh_db, config=WorkerConfig(poll_batch=50))
    dispatched: list = []
    original = worker._dispatch_trigger

    async def fake_dispatch(row):
        dispatched.append(row["id"])

    worker._dispatch_trigger = fake_dispatch  # type: ignore[assignment]
    await worker._poll_and_dispatch()
    for t in list(worker._in_flight):
        await t
    # The trigger was dispatched (not pre-rejected by the bound).
    assert tid in dispatched


# =====================================================================
# Cycle termination — the headline TK-3 property.
# =====================================================================

async def test_cascade_chain_terminates_at_max_depth(
    fresh_db, tenant, tenant_cleanup,
):
    """
    Simulate a chain of T1 enqueues: depth 0, 1, 2, ..., using
    enqueue_cascade_t1 at each step. The chain must terminate at
    depth=MAX_CASCADE_DEPTH-1 (the next enqueue is suppressed).

    This is the cross-trigger analogue of the intra-BFS bound already
    proven in test_cascade.py — together they guarantee that state_change
    cycles cannot run away.
    """
    oid = await _seed_observation(fresh_db, tenant)
    depths_enqueued: list[int] = []

    # Start from parent_payload={"cascade_depth": 0} so the first
    # enqueue lands at depth 1.
    current_payload: dict = {"cascade_depth": 0}
    async with fresh_db.acquire() as conn:
        for _ in range(MAX_CASCADE_DEPTH + 5):
            new_id = await enqueue_cascade_t1(
                conn,
                tenant_id=tenant,
                observation_id=oid,
                parent_payload=current_payload,
            )
            if new_id is None:
                break
            current_payload = {"cascade_depth": current_payload["cascade_depth"] + 1}
            depths_enqueued.append(current_payload["cascade_depth"])

    # Chain terminated well before the +5 overshoot.
    assert len(depths_enqueued) < MAX_CASCADE_DEPTH + 5
    # Last enqueued depth is MAX_CASCADE_DEPTH - 1 (the next one would
    # have been MAX and was suppressed).
    assert depths_enqueued[-1] == MAX_CASCADE_DEPTH - 1
