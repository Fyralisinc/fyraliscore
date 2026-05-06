"""services/think/tests/test_post_commit_op1.py — OP-1 tests.

THINK-DESIGN-AUDIT §8.1, §10 arg 1. Verifies:
  * enqueue_post_commit_actions writes expected rows inside a tx
  * dedup collapses duplicate enqueues on the same (tenant, trigger, kind)
  * post_commit_worker processes pending rows
  * a failing handler increments attempts + reschedules with backoff
  * after MAX_ATTEMPTS the row is moved to dead-letter
  * mid-dispatch crash (simulated via a handler that raises) leaves the
    row pending for retry — it is NOT marked processed
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from services.think.diff_schema import ClaimOp, ValidatedDiff
from services.think.post_commit import (
    BACKOFF_BASE_SECONDS,
    MAX_ATTEMPTS,
    enqueue_post_commit_actions,
    fetch_pending_actions,
    process_batch,
    register_handler,
    reset_handlers,
    _compute_backoff,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _make_diff(
    *,
    tenant_id: UUID,
    trigger_ref: UUID,
    with_predictions: bool = True,
    with_entities: bool = True,
) -> ValidatedDiff:
    """Build a ValidatedDiff that produces enqueues for all four kinds
    (anomalies is passed separately). Predictions + entities are
    included by default so the non-empty gates don't filter us out."""
    predictions: list[ClaimOp] = []
    if with_predictions:
        predictions.append(
            ClaimOp(
                op="insert",
                entry={
                    "confidence": 0.5,
                    "evaluate_at": datetime.now(timezone.utc).isoformat(),
                    "scope_actors": [],
                    "scope_entities": [],
                    "falsifier": "deadline passes without completion",
                    "proposition": {"kind": "prediction"},
                },
            )
        )
    claim_ops: list[ClaimOp] = []
    if with_entities:
        claim_ops.append(
            ClaimOp(
                op="insert",
                entry={
                    "confidence": 0.5,
                    "scope_entities": [
                        {"type": "commitment", "id": str(uuid.uuid4())},
                    ],
                },
            )
        )
    return ValidatedDiff(
        trigger_ref=trigger_ref,
        tenant_id=tenant_id,
        claim_ops=claim_ops,
        act_ops=[],
        resource_ops=[],
        new_predictions=predictions,
    )


@pytest_asyncio.fixture
async def clean_queue(db_pool: asyncpg.Pool, tenant):
    """Ensure the queue starts empty for this tenant and clean up after."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM pending_post_commit_actions WHERE tenant_id = $1",
            tenant,
        )
    yield
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM pending_post_commit_actions WHERE tenant_id = $1",
            tenant,
        )


@pytest.fixture(autouse=True)
def _reset_handlers_each_test():
    reset_handlers()
    yield
    reset_handlers()


# ---------------------------------------------------------------------
# _compute_backoff — pure unit test
# ---------------------------------------------------------------------


def test_compute_backoff_exponential():
    assert _compute_backoff(0) == 0
    assert _compute_backoff(1) == BACKOFF_BASE_SECONDS          # 2s
    assert _compute_backoff(2) == BACKOFF_BASE_SECONDS * 2      # 4s
    assert _compute_backoff(3) == BACKOFF_BASE_SECONDS * 4      # 8s
    assert _compute_backoff(5) == BACKOFF_BASE_SECONDS * 16     # 32s
    # Cap at 300s regardless of exponent blowing up.
    assert _compute_backoff(20) == 300


# ---------------------------------------------------------------------
# Enqueue tests
# ---------------------------------------------------------------------


async def test_enqueue_creates_rows_per_action_kind(
    db_pool: asyncpg.Pool, tenant, clean_queue,
):
    """Enqueueing a diff with content in every action-kind creates four
    rows: publish_anomalies, schedule_predictions, broadcast_realtime,
    invalidate_metrics."""
    trigger_ref = uuid.uuid4()
    diff = _make_diff(tenant_id=tenant, trigger_ref=trigger_ref)
    anomalies = [{"kind": "confidence_drop", "region": {"model_id": "abc"}, "significance": 0.6}]

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            inserted = await enqueue_post_commit_actions(
                trigger=None,  # unused
                validated_diff=diff,
                conn=conn,
                anomalies=anomalies,
            )

    assert len(inserted) == 4, f"expected 4 rows, got {len(inserted)}"

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT action_kind FROM pending_post_commit_actions
            WHERE trigger_id = $1
            ORDER BY action_kind
            """,
            trigger_ref,
        )
    kinds = sorted(r["action_kind"] for r in rows)
    assert kinds == [
        "broadcast_realtime",
        "invalidate_metrics",
        "publish_anomalies",
        "schedule_predictions",
    ]


async def test_enqueue_skips_empty_payloads(
    db_pool: asyncpg.Pool, tenant, clean_queue,
):
    """A diff with no anomalies / predictions / entity mutations should
    only enqueue broadcast_realtime (always-on heartbeat)."""
    trigger_ref = uuid.uuid4()
    diff = ValidatedDiff(
        trigger_ref=trigger_ref,
        tenant_id=tenant,
        claim_ops=[],
        act_ops=[],
        resource_ops=[],
        new_predictions=[],
    )
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            inserted = await enqueue_post_commit_actions(
                trigger=None, validated_diff=diff, conn=conn, anomalies=[],
            )

    # Only broadcast_realtime is unconditional.
    assert len(inserted) == 1
    async with db_pool.acquire() as conn:
        kinds = [r["action_kind"] for r in await conn.fetch(
            "SELECT action_kind FROM pending_post_commit_actions "
            "WHERE trigger_id = $1", trigger_ref,
        )]
    assert kinds == ["broadcast_realtime"]


async def test_enqueue_dedup_collapses_duplicates(
    db_pool: asyncpg.Pool, tenant, clean_queue,
):
    """Calling enqueue_post_commit_actions twice with the same trigger
    produces one set of rows, not two."""
    trigger_ref = uuid.uuid4()
    diff = _make_diff(tenant_id=tenant, trigger_ref=trigger_ref)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            first = await enqueue_post_commit_actions(
                trigger=None, validated_diff=diff, conn=conn,
                anomalies=[{"kind": "confidence_drop", "region": {}, "significance": 0.5}],
            )
        async with conn.transaction():
            second = await enqueue_post_commit_actions(
                trigger=None, validated_diff=diff, conn=conn,
                anomalies=[{"kind": "confidence_drop", "region": {}, "significance": 0.5}],
            )

    # Second call returns empty (all dedupped by NULLS NOT DISTINCT unique).
    assert len(first) == 4
    assert len(second) == 0

    async with db_pool.acquire() as conn:
        total = await conn.fetchval(
            "SELECT count(*) FROM pending_post_commit_actions "
            "WHERE trigger_id = $1", trigger_ref,
        )
    assert total == 4


async def test_enqueue_rolls_back_with_outer_tx(
    db_pool: asyncpg.Pool, tenant, clean_queue,
):
    """If the outer transaction rolls back, the enqueued rows are rolled
    back with it — this is the entire point of enqueuing inside the
    apply tx."""
    trigger_ref = uuid.uuid4()
    diff = _make_diff(tenant_id=tenant, trigger_ref=trigger_ref)

    class _IntentionalRollback(Exception):
        pass

    with pytest.raises(_IntentionalRollback):
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await enqueue_post_commit_actions(
                    trigger=None, validated_diff=diff, conn=conn,
                    anomalies=[{"kind": "x", "region": {}, "significance": 0.5}],
                )
                raise _IntentionalRollback("simulated apply failure")

    async with db_pool.acquire() as conn:
        total = await conn.fetchval(
            "SELECT count(*) FROM pending_post_commit_actions "
            "WHERE trigger_id = $1", trigger_ref,
        )
    assert total == 0, "enqueued rows should roll back with the outer tx"


# ---------------------------------------------------------------------
# Worker dispatch tests
# ---------------------------------------------------------------------


async def test_worker_processes_pending_rows(
    db_pool: asyncpg.Pool, tenant, clean_queue,
):
    """Worker picks up a pending row, dispatches to the registered
    handler, and marks processed_at."""
    trigger_ref = uuid.uuid4()
    diff = _make_diff(tenant_id=tenant, trigger_ref=trigger_ref)

    dispatched: list[str] = []

    async def _capturing(payload, tid, trid):
        dispatched.append(f"{tid}:{trid}")

    register_handler("broadcast_realtime", _capturing)
    register_handler("publish_anomalies", _capturing)
    register_handler("schedule_predictions", _capturing)
    register_handler("invalidate_metrics", _capturing)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await enqueue_post_commit_actions(
                trigger=None, validated_diff=diff, conn=conn,
                anomalies=[{"kind": "confidence_drop", "region": {}, "significance": 0.5}],
            )

    stats = await process_batch(db_pool, limit=50, tenant_id=tenant)
    assert stats.processed == 4
    assert stats.failed == 0
    assert stats.dead_lettered == 0
    assert len(dispatched) == 4

    async with db_pool.acquire() as conn:
        pending = await conn.fetchval(
            """
            SELECT count(*) FROM pending_post_commit_actions
            WHERE trigger_id = $1 AND processed_at IS NULL
            """,
            trigger_ref,
        )
    assert pending == 0


async def test_worker_retries_on_handler_failure(
    db_pool: asyncpg.Pool, tenant, clean_queue,
):
    """A handler that raises causes attempts to increment and the row
    to be rescheduled (scheduled_at > now)."""
    trigger_ref = uuid.uuid4()
    diff = _make_diff(
        tenant_id=tenant, trigger_ref=trigger_ref,
        with_predictions=False, with_entities=False,
    )

    call_count = {"n": 0}

    async def _failing(payload, tid, trid):
        call_count["n"] += 1
        raise RuntimeError("boom")

    register_handler("broadcast_realtime", _failing)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await enqueue_post_commit_actions(
                trigger=None, validated_diff=diff, conn=conn, anomalies=[],
            )

    stats = await process_batch(db_pool, limit=10, tenant_id=tenant)
    assert stats.processed == 0
    assert stats.failed == 1
    assert stats.dead_lettered == 0
    assert call_count["n"] == 1

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT attempts, processed_at, dead_lettered_at, last_error,
                   scheduled_at
            FROM pending_post_commit_actions WHERE trigger_id = $1
            """,
            trigger_ref,
        )
    assert row["attempts"] == 1
    assert row["processed_at"] is None
    assert row["dead_lettered_at"] is None
    assert "boom" in (row["last_error"] or "")
    # Scheduled_at should be in the future (+2s backoff).
    now = await _db_now(db_pool)
    assert row["scheduled_at"] > now


async def test_worker_dead_letters_after_max_attempts(
    db_pool: asyncpg.Pool, tenant, clean_queue,
):
    """5 consecutive failures → row is moved to dead-letter (the partial
    index excludes it from the pending poll)."""
    trigger_ref = uuid.uuid4()
    diff = _make_diff(
        tenant_id=tenant, trigger_ref=trigger_ref,
        with_predictions=False, with_entities=False,
    )

    async def _always_failing(payload, tid, trid):
        raise RuntimeError("permanent failure")

    register_handler("broadcast_realtime", _always_failing)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await enqueue_post_commit_actions(
                trigger=None, validated_diff=diff, conn=conn, anomalies=[],
            )

    # Drive N failures. Because scheduled_at is advanced into the future
    # each time, we manually reset it back to now() so the next poll
    # picks up the same row.
    for i in range(MAX_ATTEMPTS):
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE pending_post_commit_actions
                SET scheduled_at = now() - interval '1 second'
                WHERE trigger_id = $1
                """,
                trigger_ref,
            )
        await process_batch(db_pool, limit=10, tenant_id=tenant)

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT attempts, processed_at, dead_lettered_at, last_error
            FROM pending_post_commit_actions WHERE trigger_id = $1
            """,
            trigger_ref,
        )
    assert row["attempts"] == MAX_ATTEMPTS
    assert row["dead_lettered_at"] is not None
    assert row["processed_at"] is None
    # Row is excluded from the pending poll now.
    async with db_pool.acquire() as conn:
        pending = await fetch_pending_actions(conn, limit=10, tenant_id=tenant)
    assert all(a.trigger_id != trigger_ref for a in pending)


async def test_worker_fetch_respects_scheduled_at(
    db_pool: asyncpg.Pool, tenant, clean_queue,
):
    """A row scheduled in the future is not returned by fetch_pending."""
    trigger_ref = uuid.uuid4()
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO pending_post_commit_actions
              (tenant_id, trigger_id, action_kind, action_payload,
               scheduled_at)
            VALUES ($1, $2, 'broadcast_realtime', '{}'::jsonb,
                    now() + interval '1 hour')
            """,
            tenant, trigger_ref,
        )
        pending = await fetch_pending_actions(conn, limit=10, tenant_id=tenant)
    assert all(a.trigger_id != trigger_ref for a in pending)


# ---------------------------------------------------------------------
# Worker loop integration — start + stop
# ---------------------------------------------------------------------


async def test_worker_loop_processes_and_stops(
    db_pool: asyncpg.Pool, tenant, clean_queue,
):
    """Run `post_commit_worker` with a stop_event; enqueue a row;
    verify it gets processed within one poll cycle."""
    from services.think.post_commit import post_commit_worker

    trigger_ref = uuid.uuid4()
    diff = _make_diff(tenant_id=tenant, trigger_ref=trigger_ref,
                      with_predictions=False, with_entities=False)

    seen = asyncio.Event()

    async def _handler(payload, tid, trid):
        seen.set()

    register_handler("broadcast_realtime", _handler)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await enqueue_post_commit_actions(
                trigger=None, validated_diff=diff, conn=conn, anomalies=[],
            )

    stop = asyncio.Event()
    task = asyncio.create_task(
        post_commit_worker(
            db_pool, poll_interval=0.1, stop_event=stop, tenant_id=tenant,
        )
    )
    try:
        await asyncio.wait_for(seen.wait(), timeout=5.0)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5.0)

    async with db_pool.acquire() as conn:
        processed = await conn.fetchval(
            "SELECT processed_at FROM pending_post_commit_actions "
            "WHERE trigger_id = $1",
            trigger_ref,
        )
    assert processed is not None


# ---------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------


async def _db_now(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT now()")
