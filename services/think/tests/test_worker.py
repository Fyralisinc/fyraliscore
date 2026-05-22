"""services/think/tests/test_worker.py — ThinkWorker poll + lock +
concurrency cap + backpressure + graceful shutdown.

Covers Wave 3-B Outstanding #2 + #11 (worker-level idempotency).

  * `FOR UPDATE SKIP LOCKED` dequeue: two workers on the same queue
    pick different rows (not the same row twice).
  * Per-tenant concurrency cap: spawning 8 dispatches at once with cap
    = 4 never lets more than 4 run concurrently.
  * Graceful shutdown: ThinkWorker.stop() wakes the poll loop and
    awaits in-flight tasks.
  * Poll backoff: empty queue → no crash, just waits.
  * Backpressure limit: queue depth > threshold triggers the warning
    log (observable via _queue_depth returning the expected value).
  * Worker re-enqueue-on-failure: _mark_trigger_failed bumps attempts
    and sets a future scheduled_for.
  * Worker-level idempotency: same trigger_id fired twice at worker →
    second run produces `status='skipped_idempotent'` in think_runs.
"""
from __future__ import annotations

import asyncio
import json
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.ids import uuid7

from services.think.observability import METRICS
from services.think.tests.conftest import ScriptedProvider, make_embedding
from services.think.worker import ThinkWorker, WorkerConfig


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# =====================================================================
# Helpers
# =====================================================================


async def _seed_signal_observation(pool, tenant: UUID) -> UUID:
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


async def _enqueue_trigger_row(
    pool, tenant: UUID, observation_id: UUID,
    *, subkind: str = "event_arrival",
) -> UUID:
    trigger_id = uuid7()
    payload = {"trigger_id": str(trigger_id), "seed_natural_text": "x"}
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO think_trigger_queue
              (id, tenant_id, trigger_kind, trigger_subkind,
               observation_id, payload)
            VALUES ($1, $2, 'T1', $3, $4, $5::jsonb)
            """,
            trigger_id, tenant, subkind, observation_id, json.dumps(payload),
        )
    return trigger_id


# =====================================================================
# Dequeue — FOR UPDATE SKIP LOCKED
# =====================================================================


async def test_poll_dequeues_pending_rows(fresh_db, tenant, tenant_cleanup):
    obs = await _seed_signal_observation(fresh_db, tenant)
    t_a = await _enqueue_trigger_row(fresh_db, tenant, obs)
    t_b = await _enqueue_trigger_row(fresh_db, tenant, obs)

    # Worker polls but we stub `_dispatch_trigger` so no actual Think runs.
    worker = ThinkWorker(fresh_db, config=WorkerConfig(poll_batch=50))
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row["id"])

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    # Both our rows were dispatched (other rows may also be in flight from
    # parallel test activity; we care only about OUR trigger ids here).
    got = set(dispatched)
    assert t_a in got and t_b in got

    async with fresh_db.acquire() as conn:
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM think_trigger_queue "
            "WHERE tenant_id = $1 AND locked_by IS NOT NULL",
            tenant,
        )
    assert n == 2


async def test_two_workers_pick_different_rows(fresh_db, tenant, tenant_cleanup):
    """FOR UPDATE SKIP LOCKED ensures the two pollers don't grab the
    same row."""
    obs = await _seed_signal_observation(fresh_db, tenant)
    ids = [await _enqueue_trigger_row(fresh_db, tenant, obs) for _ in range(4)]

    w1_got: list = []
    w2_got: list = []

    w1 = ThinkWorker(fresh_db, config=WorkerConfig(
        poll_batch=2, worker_id="w1",
    ))
    w2 = ThinkWorker(fresh_db, config=WorkerConfig(
        poll_batch=2, worker_id="w2",
    ))

    async def fake_dispatch_w1(row):
        w1_got.append(row["id"])

    async def fake_dispatch_w2(row):
        w2_got.append(row["id"])

    w1._dispatch_trigger = fake_dispatch_w1  # type: ignore[method-assign]
    w2._dispatch_trigger = fake_dispatch_w2  # type: ignore[method-assign]
    await asyncio.gather(
        w1._poll_and_dispatch(),
        w2._poll_and_dispatch(),
    )
    await asyncio.sleep(0.01)

    # Union contains our 4 ids; intersection on our 4 ids is empty.
    ours = set(ids)
    w1_ours = set(w1_got) & ours
    w2_ours = set(w2_got) & ours
    assert (w1_ours | w2_ours) == ours
    assert (w1_ours & w2_ours) == set()


async def test_poll_skips_already_locked_rows(fresh_db, tenant, tenant_cleanup):
    """A row locked_by some other worker is skipped by the ready-rows
    partial index query."""
    obs = await _seed_signal_observation(fresh_db, tenant)
    trig = await _enqueue_trigger_row(fresh_db, tenant, obs)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "UPDATE think_trigger_queue SET locked_by = 'other' WHERE id = $1",
            trig,
        )

    worker = ThinkWorker(fresh_db, config=WorkerConfig(poll_batch=50))
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row["id"])

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)
    # OUR trig is NOT in the dispatched set (other tenants' rows may be).
    assert trig not in set(dispatched)


async def test_poll_skips_completed_rows(fresh_db, tenant, tenant_cleanup):
    """completed_at set → not polled."""
    obs = await _seed_signal_observation(fresh_db, tenant)
    trig = await _enqueue_trigger_row(fresh_db, tenant, obs)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "UPDATE think_trigger_queue SET completed_at = now() WHERE id = $1",
            trig,
        )
    worker = ThinkWorker(fresh_db, config=WorkerConfig(poll_batch=50))
    dispatched: list = []

    async def fake_dispatch(r):
        dispatched.append(r["id"])

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)
    assert trig not in set(dispatched)


async def test_poll_respects_scheduled_for_future(fresh_db, tenant, tenant_cleanup):
    """scheduled_for in the future → not dequeued yet."""
    obs = await _seed_signal_observation(fresh_db, tenant)
    trig = await _enqueue_trigger_row(fresh_db, tenant, obs)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "UPDATE think_trigger_queue "
            "SET scheduled_for = now() + interval '10 minutes' WHERE id = $1",
            trig,
        )
    worker = ThinkWorker(fresh_db, config=WorkerConfig(poll_batch=50))
    dispatched: list = []

    async def fake_dispatch(r):
        dispatched.append(r["id"])

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)
    assert trig not in set(dispatched)


# =====================================================================
# Per-tenant concurrency cap
# =====================================================================


async def test_per_tenant_concurrency_cap(fresh_db, tenant, tenant_cleanup):
    """Spawn 8 dispatches with cap=4; verify max concurrent in-flight
    never exceeded 4."""
    obs = await _seed_signal_observation(fresh_db, tenant)
    # 8 trigger rows.
    trig_ids = [
        await _enqueue_trigger_row(fresh_db, tenant, obs) for _ in range(8)
    ]

    worker = ThinkWorker(
        fresh_db, config=WorkerConfig(poll_batch=10, max_concurrency_per_tenant=4),
    )

    active = {"count": 0, "max_seen": 0}
    lock = asyncio.Lock()

    async def fake_process(row):
        async with lock:
            active["count"] += 1
            active["max_seen"] = max(active["max_seen"], active["count"])
        await asyncio.sleep(0.05)
        async with lock:
            active["count"] -= 1

    # Replace _process_trigger (inside _dispatch_trigger the semaphore
    # is applied before _process_trigger).
    worker._process_trigger = fake_process  # type: ignore[method-assign]

    # Manually dispatch to trigger semaphore path.
    async with fresh_db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM think_trigger_queue WHERE tenant_id = $1",
            tenant,
        )
    await asyncio.gather(*(worker._dispatch_trigger(r) for r in rows))

    assert active["max_seen"] <= 4, (
        f"cap violated: saw {active['max_seen']} concurrent dispatches"
    )
    # All 8 ran eventually.
    assert active["count"] == 0


# =====================================================================
# Queue depth + backpressure
# =====================================================================


async def test_queue_depth_counts_pending_rows(fresh_db, tenant, tenant_cleanup):
    obs = await _seed_signal_observation(fresh_db, tenant)
    for _ in range(5):
        await _enqueue_trigger_row(fresh_db, tenant, obs)
    worker = ThinkWorker(fresh_db, config=WorkerConfig())
    depth = await worker._queue_depth()
    assert depth >= 5


async def test_backpressure_does_not_prevent_enqueue(
    fresh_db, tenant, tenant_cleanup,
):
    """Queue depth > backpressure_limit still allows new rows to land —
    the worker just logs a warning and keeps polling."""
    obs = await _seed_signal_observation(fresh_db, tenant)
    for _ in range(12):
        await _enqueue_trigger_row(fresh_db, tenant, obs)
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(backpressure_limit=5),
    )
    depth = await worker._queue_depth()
    assert depth >= 12
    # New enqueue still succeeds.
    extra = await _enqueue_trigger_row(fresh_db, tenant, obs)
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM think_trigger_queue WHERE id = $1", extra,
        )
    assert row is not None


# =====================================================================
# Graceful shutdown
# =====================================================================


async def test_stop_sets_shutdown_event(fresh_db, tenant, tenant_cleanup):
    worker = ThinkWorker(fresh_db, config=WorkerConfig(poll_interval_s=0.05))
    await worker.stop()
    assert worker._shutdown_event.is_set()


async def test_run_exits_on_shutdown_event(fresh_db, tenant, tenant_cleanup):
    """A fresh worker with an empty queue responds to stop() quickly."""
    worker = ThinkWorker(fresh_db, config=WorkerConfig(poll_interval_s=0.05))

    async def stopper():
        await asyncio.sleep(0.1)
        await worker.stop()

    t_stop = asyncio.create_task(stopper())
    await worker.run()
    await t_stop


async def test_run_waits_for_in_flight_tasks_on_shutdown(
    fresh_db, tenant, tenant_cleanup,
):
    """If the worker has in-flight tasks, run() awaits them on
    shutdown before returning."""
    worker = ThinkWorker(fresh_db, config=WorkerConfig(poll_interval_s=0.05))

    finished = []

    async def slow():
        await asyncio.sleep(0.2)
        finished.append(True)

    t = asyncio.create_task(slow())
    worker._in_flight.add(t)
    t.add_done_callback(worker._in_flight.discard)

    async def stopper():
        await asyncio.sleep(0.01)
        await worker.stop()

    t_stop = asyncio.create_task(stopper())
    await worker.run()
    await t_stop
    assert finished == [True]


# =====================================================================
# Re-enqueue on failure (attempts++)
# =====================================================================


async def test_mark_trigger_failed_bumps_attempts(
    fresh_db, tenant, tenant_cleanup,
):
    obs = await _seed_signal_observation(fresh_db, tenant)
    trig = await _enqueue_trigger_row(fresh_db, tenant, obs)
    worker = ThinkWorker(
        fresh_db, config=WorkerConfig(trigger_max_attempts=5),
    )
    await worker._mark_trigger_failed(trig, "boom1")
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT attempts, completed_at, scheduled_for FROM think_trigger_queue WHERE id = $1",
            trig,
        )
    assert row["attempts"] == 1
    assert row["completed_at"] is None


async def test_mark_trigger_failed_eventually_dead_letters(
    fresh_db, tenant, tenant_cleanup,
):
    obs = await _seed_signal_observation(fresh_db, tenant)
    trig = await _enqueue_trigger_row(fresh_db, tenant, obs)
    worker = ThinkWorker(
        fresh_db, config=WorkerConfig(trigger_max_attempts=3),
    )
    for i in range(3):
        await worker._mark_trigger_failed(trig, f"fail{i}")
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT attempts, completed_at FROM think_trigger_queue WHERE id = $1",
            trig,
        )
    # After trigger_max_attempts, the row is marked complete (dead-letter
    # semantics for trigger queue is "completed_at set + attempts=N").
    assert row["attempts"] == 3
    assert row["completed_at"] is not None


# =====================================================================
# Worker-level idempotency
# =====================================================================


async def test_worker_idempotency_second_run_skipped(
    fresh_db, tenant, tenant_cleanup,
):
    """
    Dispatch the same trigger row twice through _process_trigger. The
    second call sees applied_triggers has a prior row and records
    status='skipped_idempotent' in think_runs.
    """
    obs = await _seed_signal_observation(fresh_db, tenant)
    trig = await _enqueue_trigger_row(
        fresh_db, tenant, obs, subkind="event_arrival",
    )

    # Fetch the row once for the first dispatch + clone the record for the
    # second dispatch (second call uses a fresh ScriptedProvider).
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, tenant_id, trigger_kind, trigger_subkind, "
            "observation_id, model_id, payload, attempts "
            "FROM think_trigger_queue WHERE id = $1",
            trig,
        )

    # First pass — scripted provider returns an empty diff.
    worker = ThinkWorker(
        fresh_db,
        llm_provider=ScriptedProvider(responses=[json.dumps({
            "trigger_ref": str(trig),
            "tenant_id": str(tenant),
            "claim_ops": [],
            "act_ops": [],
            "resource_ops": [],
            "new_predictions": [],
            "reasoning_trace": "scripted empty",
        })]),
    )
    await worker._process_trigger(row)

    # Second pass with a fresh worker + provider.
    worker2 = ThinkWorker(
        fresh_db,
        llm_provider=ScriptedProvider(responses=[json.dumps({
            "trigger_ref": str(trig),
            "tenant_id": str(tenant),
            "claim_ops": [],
            "act_ops": [],
            "resource_ops": [],
            "new_predictions": [],
            "reasoning_trace": "scripted empty 2",
        })]),
    )
    await worker2._process_trigger(row)

    async with fresh_db.acquire() as conn:
        statuses = await conn.fetch(
            "SELECT status FROM think_runs WHERE trigger_id = $1 "
            "ORDER BY started_at",
            trig,
        )
    names = [r["status"] for r in statuses]
    assert "success" in names
    assert "skipped_idempotent" in names
    # Exactly one applied_triggers row.
    async with fresh_db.acquire() as conn:
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM applied_triggers WHERE trigger_id = $1", trig,
        )
    assert n == 1


# =====================================================================
# Regression: _populate_seed_fields rehydrates the full payload.
#
# Bug surfaced by the Wave 3-B test-completion follow-up agent:
# `_process_trigger` previously only copied `seed_natural_text` from
# the queue row's payload; `seed_entity_ids`, `seed_occurred_at`,
# `scope_actors`, and `region_spec` were dropped on the floor. The
# consequence was OutOfRegionError when a T3 enqueuer (Wave 4-B
# anomaly processor) included entity hints in the payload.
#
# These tests lock the rehydration contract in place so no future
# worker refactor quietly drops fields again.
# =====================================================================

# Pure-unit tests — declared async so the module-level asyncio mark
# is satisfied. No DB interaction.
async def test_populate_seed_fields_copies_natural_text():
    from services.think.worker import _populate_seed_fields
    from services.retrieval.primary import TriggerContext
    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    _populate_seed_fields(trigger, {"seed_natural_text": "hello world"})
    assert trigger.seed_natural_text == "hello world"


async def test_populate_seed_fields_copies_entity_ids():
    from services.think.worker import _populate_seed_fields
    from services.retrieval.primary import TriggerContext
    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    entities = [
        {"type": "commitment", "id": "c-187"},
        {"type": "goal", "id": "g-42"},
    ]
    _populate_seed_fields(trigger, {"seed_entity_ids": entities})
    assert trigger.seed_entity_ids == entities


async def test_populate_seed_fields_copies_occurred_at_iso():
    from services.think.worker import _populate_seed_fields
    from services.retrieval.primary import TriggerContext
    from datetime import datetime, timezone
    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    _populate_seed_fields(trigger, {"seed_occurred_at": "2026-04-20T12:34:56Z"})
    assert trigger.seed_occurred_at == datetime(2026, 4, 20, 12, 34, 56, tzinfo=timezone.utc)


async def test_populate_seed_fields_copies_scope_actors_as_uuids():
    from services.think.worker import _populate_seed_fields
    from services.retrieval.primary import TriggerContext
    a = uuid7()
    b = uuid7()
    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    _populate_seed_fields(trigger, {"scope_actors": [str(a), str(b)]})
    assert trigger.scope_actors == [a, b]


async def test_populate_seed_fields_copies_region_spec():
    from services.think.worker import _populate_seed_fields
    from services.retrieval.primary import TriggerContext
    trigger = TriggerContext(kind="T3", tenant_id=uuid7())
    region = {"entity_ids": [{"type": "commitment", "id": "c-1"}], "scope": "x"}
    _populate_seed_fields(trigger, {"region_spec": region})
    assert trigger.region_spec == region


async def test_populate_seed_fields_skips_missing_fields():
    from services.think.worker import _populate_seed_fields
    from services.retrieval.primary import TriggerContext
    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    _populate_seed_fields(trigger, {})
    assert trigger.seed_natural_text is None
    assert trigger.seed_entity_ids == []
    assert trigger.seed_occurred_at is None
    assert trigger.scope_actors == []
    assert trigger.region_spec is None


async def test_populate_seed_fields_ignores_malformed_entries():
    from services.think.worker import _populate_seed_fields
    from services.retrieval.primary import TriggerContext
    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    _populate_seed_fields(trigger, {
        "seed_entity_ids": ["not-a-dict", {"type": "commitment", "id": "c-1"}],
        "scope_actors": ["not-a-uuid", str(uuid7())],
        "seed_occurred_at": "garbage-timestamp",
    })
    # Only the valid dict entry survives.
    assert trigger.seed_entity_ids == [{"type": "commitment", "id": "c-1"}]
    # Only the valid UUID survives.
    assert len(trigger.scope_actors) == 1
    # Malformed timestamp leaves the field at its default.
    assert trigger.seed_occurred_at is None


async def test_process_trigger_rehydrates_full_payload_for_retrieval(
    fresh_db: asyncpg.Pool, tenant: UUID, tenant_cleanup,
):
    """End-to-end regression: an enqueuer supplying seed_entity_ids +
    scope_actors + seed_occurred_at must reach retrieval through the
    worker, not just seed_natural_text."""
    # Stub `think()` to capture the TriggerContext it sees.
    import services.think.worker as worker_mod

    captured: dict = {}

    async def _fake_think(trigger, pool, **kwargs):
        captured["trigger"] = trigger
        # Simulate a successful run so the worker marks the queue row complete.
        from services.think.reason import ThinkRunOutcome
        return ThinkRunOutcome(
            run_id=uuid7(),
            trigger_id=kwargs.get("trigger_id") or uuid7(),
            tenant_id=trigger.tenant_id,
            succeeded=True,
            skipped_idempotent=False,
            ops_applied={"claim": 0, "act": 0, "resource": 0},
            cascade_depth=0,
            error=None,
        )

    worker_mod.think = _fake_think
    try:
        oid = await _seed_signal_observation(fresh_db, tenant)
        trig = uuid7()
        actor = uuid7()
        payload = {
            "trigger_id": str(trig),
            "seed_natural_text": "Alice merged PR",
            "seed_entity_ids": [{"type": "commitment", "id": "c-187"}],
            "seed_occurred_at": "2026-04-21T08:00:00Z",
            "scope_actors": [str(actor)],
        }
        async with fresh_db.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO think_trigger_queue
                  (id, tenant_id, trigger_kind, trigger_subkind,
                   observation_id, payload)
                VALUES ($1, $2, 'T1', 'event_arrival', $3, $4::jsonb)
                """,
                trig, tenant, oid, json.dumps(payload),
            )
            row = await conn.fetchrow(
                "SELECT * FROM think_trigger_queue WHERE id = $1", trig,
            )
        w = ThinkWorker(fresh_db, config=WorkerConfig(), llm_provider=ScriptedProvider([]))
        await w._process_trigger(row)
    finally:
        # Restore the real `think` binding so subsequent tests aren't polluted.
        from services.think.reason import think as real_think
        worker_mod.think = real_think

    t = captured["trigger"]
    assert t.seed_natural_text == "Alice merged PR"
    assert t.seed_entity_ids == [{"type": "commitment", "id": "c-187"}]
    assert t.seed_occurred_at.year == 2026 and t.seed_occurred_at.month == 4
    assert t.scope_actors == [actor]
