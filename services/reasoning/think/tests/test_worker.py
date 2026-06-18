"""services/reasoning/think/tests/test_worker.py — ThinkWorker poll + lock +
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
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7

from services.reasoning.relationships import (
    JudgmentScores,
    RelationshipCandidatesRepo,
    make_edge_candidate,
    make_edge_type_candidate,
    make_situation_candidate,
)
from services.reasoning.think.tests.conftest import ScriptedProvider, make_embedding
from services.reasoning.think.worker import ThinkWorker, WorkerConfig


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# =====================================================================
# Helpers
# =====================================================================


async def _seed_signal_observation(
    pool,
    tenant: UUID,
    *,
    actor_id: UUID | None = None,
    entities_mentioned: list[dict] | None = None,
    text: str = "x",
) -> UUID:
    aid = actor_id or uuid7()
    oid = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active') "
            "ON CONFLICT (id) DO NOTHING",
            aid,
            tenant,
        )
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier,
               entities_mentioned)
            VALUES ($1, $2, now(), 'signal', 'test', $3,
                    '{}'::jsonb, $4, $5, FALSE, 'authoritative', $6::jsonb)
            """,
            oid,
            tenant,
            aid,
            text,
            make_embedding(text),
            json.dumps(entities_mentioned or []),
        )
    return oid


async def _enqueue_trigger_row(
    pool,
    tenant: UUID,
    observation_id: UUID,
    *,
    subkind: str = "event_arrival",
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
            trigger_id,
            tenant,
            subkind,
            observation_id,
            json.dumps(payload),
        )
    return trigger_id


async def _lock_trigger(pool, trigger_id: UUID, worker_id: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET locked_by = $2, locked_at = now()
            WHERE id = $1
            """,
            trigger_id,
            worker_id,
        )


async def test_worker_config_defaults_are_batch_first(monkeypatch):
    monkeypatch.delenv("THINK_T1_BATCH_WINDOW_S", raising=False)
    monkeypatch.delenv("THINK_T1_BATCH_MIN_SIZE", raising=False)
    monkeypatch.delenv("THINK_T1_BATCH_MAX_SIZE", raising=False)
    monkeypatch.delenv("THINK_DOWNSTREAM_BATCH_WINDOW_S", raising=False)
    monkeypatch.delenv("THINK_DOWNSTREAM_BATCH_MIN_SIZE", raising=False)
    monkeypatch.delenv("THINK_T2_BATCH_MAX_SIZE", raising=False)
    monkeypatch.delenv("THINK_T4_BATCH_MAX_SIZE", raising=False)

    cfg = WorkerConfig.from_env()

    assert cfg.t1_batch_window_s == 30.0
    assert cfg.t1_batch_min_size == 20
    assert cfg.t1_batch_max_size == 30
    assert cfg.downstream_batch_window_s == 60.0
    assert cfg.downstream_batch_min_size == 2
    assert cfg.t2_batch_max_size == 8
    assert cfg.t4_batch_max_size == 4


async def _seed_model(
    pool,
    tenant: UUID,
    *,
    born_event: UUID,
    natural: str,
    scope_entities: list[dict] | None = None,
    scope_actors: list[UUID] | None = None,
) -> UUID:
    mid = uuid7()
    customer_id = uuid7()
    proposition = {
        "kind": "belief",
        "claim_role": "fact",
        "subject": natural,
        "assertion": "true",
        "summary": natural,
    }
    if scope_entities is None:
        scope_entities = [{"type": "customer", "id": str(customer_id)}]
    scope_actors = scope_actors or []
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO models
              (id, tenant_id, born_from_event_id, proposition, "natural",
               embedding, scope_actors, scope_entities, scope_temporal,
               confidence, activation, status, confidence_at_assertion,
               activation_coefficient)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::uuid[], $8::jsonb,
                    '{}'::jsonb, 0.6, 1.0, 'active', 0.6, 1.0)
            """,
            mid,
            tenant,
            born_event,
            json.dumps(proposition),
            natural,
            make_embedding(natural),
            scope_actors,
            json.dumps(scope_entities),
        )
    return mid


async def _enqueue_t2_belief_updated(
    pool,
    tenant: UUID,
    *,
    model_id: UUID,
    observation_id: UUID | None = None,
) -> UUID:
    trigger_id = uuid7()
    payload = {
        "trigger_id": str(trigger_id),
        "source_model_id": str(model_id),
        "seed_natural_text": "updated belief",
    }
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO think_trigger_queue
              (id, tenant_id, trigger_kind, trigger_subkind,
               observation_id, model_id, payload)
            VALUES ($1, $2, 'T2', 'belief_updated', $3, $4, $5::jsonb)
            """,
            trigger_id,
            tenant,
            observation_id,
            model_id,
            json.dumps(payload),
        )
    return trigger_id


async def _enqueue_t4_latent_candidate(
    pool,
    tenant: UUID,
    *,
    candidate_id: UUID,
    member_model_ids: list[UUID],
) -> UUID:
    trigger_id = uuid7()
    payload = {
        "trigger_id": str(trigger_id),
        "relationship_candidate_id": str(candidate_id),
        "member_model_ids": [str(mid) for mid in member_model_ids],
        "seed_natural_text": "candidate explanation",
    }
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO think_trigger_queue
              (id, tenant_id, trigger_kind, trigger_subkind, payload)
            VALUES ($1, $2, 'T4', 'latent_relationship_candidate', $3::jsonb)
            """,
            trigger_id,
            tenant,
            json.dumps(payload),
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
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=50,
            tenant_filter=tenant,
            t1_batch_window_s=0.0,
        ),
    )
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


async def test_poll_reclaims_stale_locked_rows(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs = await _seed_signal_observation(fresh_db, tenant)
    trig = await _enqueue_trigger_row(fresh_db, tenant, obs)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET locked_by = 'dead-worker',
                locked_at = now() - interval '10 minutes'
            WHERE id = $1
            """,
            trig,
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=1,
            worker_id="reclaimer",
            trigger_lock_timeout_s=60,
            tenant_filter=tenant,
            t1_batch_window_s=0.0,
        ),
    )
    dispatched: list[UUID] = []

    async def fake_dispatch(row):
        dispatched.append(row["id"])

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()

    assert dispatched == [trig]
    async with fresh_db.acquire() as conn:
        owner = await conn.fetchval(
            "SELECT locked_by FROM think_trigger_queue WHERE id = $1",
            trig,
        )
    assert owner == "reclaimer"


async def test_poll_does_not_overlease_when_in_flight_is_full(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Repeated polls should not lock unbounded rows while tasks wait."""
    obs = await _seed_signal_observation(fresh_db, tenant)
    for _ in range(5):
        await _enqueue_trigger_row(fresh_db, tenant, obs)
    worker_id = "overlease-test-worker"
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=3,
            worker_id=worker_id,
            tenant_filter=tenant,
            t1_batch_window_s=0.0,
        ),
    )
    release = asyncio.Event()
    dispatched: list[UUID] = []

    async def blocked_dispatch(row):
        dispatched.append(row["id"])
        await release.wait()

    worker._dispatch_trigger = blocked_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    for _ in range(50):
        if len(dispatched) == 3:
            break
        await asyncio.sleep(0.01)

    await worker._poll_and_dispatch()

    async with fresh_db.acquire() as conn:
        locked = await conn.fetchval(
            """
            SELECT COUNT(*)::int
            FROM think_trigger_queue
            WHERE locked_by = $1
            """,
            worker_id,
        )

    release.set()
    if worker._in_flight:
        await asyncio.gather(*worker._in_flight, return_exceptions=True)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET locked_by = NULL, locked_at = NULL
            WHERE locked_by = $1
            """,
            worker_id,
        )

    assert len(dispatched) == 3
    assert locked == 3


async def test_t1_batch_window_holds_fresh_singletons(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs = await _seed_signal_observation(fresh_db, tenant)
    trig = await _enqueue_trigger_row(fresh_db, tenant, obs)
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            tenant_filter=tenant,
            t1_batch_window_s=60.0,
            t1_batch_max_size=4,
            t1_batch_min_size=2,
        ),
    )
    dispatched: list[UUID] = []

    async def fake_dispatch(row):
        dispatched.append(row["id"])

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    assert dispatched == []
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT locked_by, batch_parent_id, completed_at
            FROM think_trigger_queue
            WHERE id = $1
            """,
            trig,
        )
    assert row["locked_by"] is None
    assert row["batch_parent_id"] is None
    assert row["completed_at"] is None


async def test_default_worker_config_coalesces_ready_t1_batches(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    del tenant_cleanup
    obs_ids: list[UUID] = []
    trig_ids: list[UUID] = []
    for _ in range(20):
        obs = await _seed_signal_observation(fresh_db, tenant)
        trig = await _enqueue_trigger_row(fresh_db, tenant, obs)
        obs_ids.append(obs)
        trig_ids.append(trig)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET enqueued_at = now() - interval '31 seconds'
            WHERE id = ANY($1::uuid[])
            """,
            trig_ids,
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="default-batcher",
            tenant_filter=tenant,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    assert len(dispatched) == 1
    batch = dispatched[0]
    assert batch["trigger_kind"] == "T1"
    assert batch["trigger_subkind"] == "event_batch"
    assert set(batch["payload"]["batch_member_trigger_ids"]) == {
        str(trig_id) for trig_id in trig_ids
    }
    assert set(batch["payload"]["batch_observation_ids"]) == {
        str(obs_id) for obs_id in obs_ids
    }


async def test_t1_batch_window_holds_fresh_partial_batches(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs_a = await _seed_signal_observation(fresh_db, tenant)
    obs_b = await _seed_signal_observation(fresh_db, tenant)
    trig_a = await _enqueue_trigger_row(fresh_db, tenant, obs_a)
    trig_b = await _enqueue_trigger_row(fresh_db, tenant, obs_b)
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            tenant_filter=tenant,
            t1_batch_window_s=60.0,
            t1_batch_max_size=4,
            t1_batch_min_size=2,
        ),
    )
    dispatched: list[UUID] = []

    async def fake_dispatch(row):
        dispatched.append(row["id"])

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    assert dispatched == []
    async with fresh_db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, locked_by, batch_parent_id, completed_at
            FROM think_trigger_queue
            WHERE id = ANY($1::uuid[])
            ORDER BY id
            """,
            [trig_a, trig_b],
        )
    assert len(rows) == 2
    assert all(row["locked_by"] is None for row in rows)
    assert all(row["batch_parent_id"] is None for row in rows)
    assert all(row["completed_at"] is None for row in rows)


async def test_t1_batch_coalesces_ready_rows_and_attaches_members(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs_a = await _seed_signal_observation(fresh_db, tenant)
    obs_b = await _seed_signal_observation(fresh_db, tenant)
    trig_a = await _enqueue_trigger_row(fresh_db, tenant, obs_a)
    trig_b = await _enqueue_trigger_row(fresh_db, tenant, obs_b)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET enqueued_at = now() - interval '10 seconds'
            WHERE id = ANY($1::uuid[])
            """,
            [trig_a, trig_b],
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="batcher",
            tenant_filter=tenant,
            t1_batch_window_s=1.0,
            t1_batch_max_size=4,
            t1_batch_min_size=2,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    assert len(dispatched) == 1
    batch = dispatched[0]
    assert batch["trigger_kind"] == "T1"
    assert batch["trigger_subkind"] == "event_batch"
    payload = batch["payload"]
    assert payload["batch"] is True
    assert set(payload["batch_member_trigger_ids"]) == {str(trig_a), str(trig_b)}
    assert set(payload["batch_observation_ids"]) == {str(obs_a), str(obs_b)}
    assert {
        fragment["observation_id"] for fragment in payload["batch_signal_fragments"]
    } == {str(obs_a), str(obs_b)}
    assert all(fragment.get("text") for fragment in payload["batch_signal_fragments"])
    assert "Evidence window containing 2 source signals" in payload["seed_natural_text"]

    async with fresh_db.acquire() as conn:
        members = await conn.fetch(
            """
            SELECT id, batch_parent_id, locked_by, completed_at
            FROM think_trigger_queue
            WHERE id = ANY($1::uuid[])
            ORDER BY id
            """,
            [trig_a, trig_b],
        )
        batch_locked_by = await conn.fetchval(
            "SELECT locked_by FROM think_trigger_queue WHERE id = $1",
            batch["id"],
        )
    assert batch_locked_by == "batcher"
    assert {row["batch_parent_id"] for row in members} == {batch["id"]}
    assert all(row["locked_by"] is None for row in members)
    assert all(row["completed_at"] is None for row in members)


async def test_t1_batch_dispatch_cycle_does_not_lease_singleton_tail(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs_a = await _seed_signal_observation(fresh_db, tenant, text="batch signal 1")
    obs_b = await _seed_signal_observation(fresh_db, tenant, text="batch signal 2")
    obs_c = await _seed_signal_observation(fresh_db, tenant, text="tail signal")
    trig_a = await _enqueue_trigger_row(fresh_db, tenant, obs_a)
    trig_b = await _enqueue_trigger_row(fresh_db, tenant, obs_b)
    trig_c = await _enqueue_trigger_row(fresh_db, tenant, obs_c)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET enqueued_at = now() - interval '10 seconds'
            WHERE id = ANY($1::uuid[])
            """,
            [trig_a, trig_b, trig_c],
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=2,
            worker_id="batch-first",
            tenant_filter=tenant,
            t1_batch_window_s=1.0,
            t1_batch_max_size=2,
            t1_batch_min_size=2,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    assert len(dispatched) == 1
    assert dispatched[0]["trigger_subkind"] == "event_batch"
    async with fresh_db.acquire() as conn:
        tail = await conn.fetchrow(
            """
            SELECT locked_by, batch_parent_id, completed_at
            FROM think_trigger_queue
            WHERE id = $1
            """,
            trig_c,
        )
    assert tail["locked_by"] is None
    assert tail["batch_parent_id"] is None
    assert tail["completed_at"] is None


async def test_t1_batch_prefers_entity_lanes(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    del tenant_cleanup
    acme = {"type": "customer", "id": "acme"}
    nimbus = {"type": "customer", "id": "nimbus"}
    obs_a1 = await _seed_signal_observation(
        fresh_db, tenant, entities_mentioned=[acme], text="acme signal 1"
    )
    obs_a2 = await _seed_signal_observation(
        fresh_db, tenant, entities_mentioned=[acme], text="acme signal 2"
    )
    obs_b1 = await _seed_signal_observation(
        fresh_db, tenant, entities_mentioned=[nimbus], text="nimbus signal 1"
    )
    obs_b2 = await _seed_signal_observation(
        fresh_db, tenant, entities_mentioned=[nimbus], text="nimbus signal 2"
    )
    trig_a1 = await _enqueue_trigger_row(fresh_db, tenant, obs_a1)
    trig_b1 = await _enqueue_trigger_row(fresh_db, tenant, obs_b1)
    trig_a2 = await _enqueue_trigger_row(fresh_db, tenant, obs_a2)
    trig_b2 = await _enqueue_trigger_row(fresh_db, tenant, obs_b2)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET enqueued_at = now() - interval '2 seconds'
            WHERE id = ANY($1::uuid[])
            """,
            [trig_a1, trig_a2, trig_b1, trig_b2],
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="t1-entity-batcher",
            tenant_filter=tenant,
            t1_batch_window_s=1.0,
            t1_batch_max_size=4,
            t1_batch_min_size=2,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    assert len(dispatched) == 2
    member_sets = {
        frozenset(batch["payload"]["batch_member_trigger_ids"])
        for batch in dispatched
    }
    assert member_sets == {
        frozenset({str(trig_a1), str(trig_a2)}),
        frozenset({str(trig_b1), str(trig_b2)}),
    }


async def test_t1_batch_falls_back_to_arrival_when_lanes_stall(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    del tenant_cleanup
    obs_a = await _seed_signal_observation(
        fresh_db,
        tenant,
        entities_mentioned=[{"type": "customer", "id": "acme"}],
    )
    obs_b = await _seed_signal_observation(
        fresh_db,
        tenant,
        entities_mentioned=[{"type": "customer", "id": "nimbus"}],
    )
    trig_a = await _enqueue_trigger_row(fresh_db, tenant, obs_a)
    trig_b = await _enqueue_trigger_row(fresh_db, tenant, obs_b)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET enqueued_at = now() - interval '2 seconds'
            WHERE id = ANY($1::uuid[])
            """,
            [trig_a, trig_b],
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="t1-arrival-fallback",
            tenant_filter=tenant,
            t1_batch_window_s=1.0,
            t1_batch_max_size=4,
            t1_batch_min_size=2,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    assert len(dispatched) == 1
    assert set(dispatched[0]["payload"]["batch_member_trigger_ids"]) == {
        str(trig_a),
        str(trig_b),
    }


async def test_t1_batch_complete_marks_member_triggers_complete(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs_a = await _seed_signal_observation(fresh_db, tenant)
    obs_b = await _seed_signal_observation(fresh_db, tenant)
    trig_a = await _enqueue_trigger_row(fresh_db, tenant, obs_a)
    trig_b = await _enqueue_trigger_row(fresh_db, tenant, obs_b)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET enqueued_at = now() - interval '10 seconds'
            WHERE id = ANY($1::uuid[])
            """,
            [trig_a, trig_b],
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="batch-complete",
            tenant_filter=tenant,
            t1_batch_window_s=1.0,
            t1_batch_max_size=4,
            t1_batch_min_size=2,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)
    batch = dispatched[0]

    await worker._mark_trigger_complete(batch["id"], payload=batch["payload"])

    async with fresh_db.acquire() as conn:
        completed = await conn.fetch(
            """
            SELECT id, completed_at
            FROM think_trigger_queue
            WHERE id = ANY($1::uuid[])
            """,
            [batch["id"], trig_a, trig_b],
        )
    assert len(completed) == 3
    assert all(row["completed_at"] is not None for row in completed)


async def test_t1_batch_terminal_failure_releases_member_triggers(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs_a = await _seed_signal_observation(fresh_db, tenant)
    obs_b = await _seed_signal_observation(fresh_db, tenant)
    trig_a = await _enqueue_trigger_row(fresh_db, tenant, obs_a)
    trig_b = await _enqueue_trigger_row(fresh_db, tenant, obs_b)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET enqueued_at = now() - interval '10 seconds'
            WHERE id = ANY($1::uuid[])
            """,
            [trig_a, trig_b],
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="batch-fail",
            tenant_filter=tenant,
            t1_batch_window_s=1.0,
            t1_batch_max_size=4,
            t1_batch_min_size=2,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)
    batch = dispatched[0]

    await worker._mark_trigger_failed(batch["id"], "boom", force_terminal=True)

    async with fresh_db.acquire() as conn:
        members = await conn.fetch(
            """
            SELECT id, batch_parent_id, completed_at, payload
            FROM think_trigger_queue
            WHERE id = ANY($1::uuid[])
            """,
            [trig_a, trig_b],
        )
        batch_completed = await conn.fetchval(
            "SELECT completed_at FROM think_trigger_queue WHERE id = $1",
            batch["id"],
        )
    assert batch_completed is not None
    assert all(row["batch_parent_id"] is None for row in members)
    assert all(row["completed_at"] is None for row in members)
    # Cost-plan §2.3 C7: released members are stamped with the parent id so
    # they can never re-batch (the dead-letter unbundle amplifier).
    for row in members:
        payload = row["payload"]
        payload = json.loads(payload) if isinstance(payload, str) else payload
        assert payload.get("unbatched_from") == str(batch["id"])
    # The stamp must exclude them from a fresh batch-creation pass.
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            rebatched = await worker._create_t1_batch_rows(conn, available_slots=4)
    assert rebatched == []


async def test_downstream_batch_coalesces_t2_belief_updates(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs = await _seed_signal_observation(fresh_db, tenant)
    model_a = await _seed_model(
        fresh_db, tenant, born_event=obs, natural="customer risk increased"
    )
    model_b = await _seed_model(
        fresh_db, tenant, born_event=obs, natural="security review blocked"
    )
    trig_a = await _enqueue_t2_belief_updated(
        fresh_db, tenant, model_id=model_a, observation_id=obs
    )
    trig_b = await _enqueue_t2_belief_updated(
        fresh_db, tenant, model_id=model_b, observation_id=obs
    )
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET enqueued_at = now() - interval '2 seconds'
            WHERE id = ANY($1::uuid[])
            """,
            [trig_a, trig_b],
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="downstream-batcher",
            tenant_filter=tenant,
            downstream_batch_window_s=1.0,
            downstream_batch_min_size=2,
            t2_batch_max_size=4,
            prune_low_value_downstream_triggers=False,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)
    assert len(dispatched) == 1

    batch = dispatched[0]
    assert batch["trigger_kind"] == "T2"
    assert batch["trigger_subkind"] == "belief_updated"
    assert batch["model_id"] == model_a
    payload = batch["payload"]
    assert payload["batch"] is True
    assert payload["batch_kind"] == "downstream"
    assert set(payload["batch_member_trigger_ids"]) == {str(trig_a), str(trig_b)}
    assert payload["model_ids"] == [str(model_a), str(model_b)]
    assert "Batch of 2 updated beliefs" in payload["seed_natural_text"]

    async with fresh_db.acquire() as conn:
        members = await conn.fetch(
            """
            SELECT id, batch_parent_id, locked_by, completed_at
            FROM think_trigger_queue
            WHERE id = ANY($1::uuid[])
            ORDER BY id
            """,
            [trig_a, trig_b],
        )
    assert {row["batch_parent_id"] for row in members} == {batch["id"]}
    assert all(row["locked_by"] is None for row in members)
    assert all(row["completed_at"] is None for row in members)


async def test_downstream_batch_prefers_t2_entity_lanes(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs = await _seed_signal_observation(fresh_db, tenant)
    acme = [{"type": "customer", "id": "acme"}]
    nimbus = [{"type": "customer", "id": "nimbus"}]
    model_a1 = await _seed_model(
        fresh_db, tenant, born_event=obs, natural="acme risk 1",
        scope_entities=acme,
    )
    model_a2 = await _seed_model(
        fresh_db, tenant, born_event=obs, natural="acme risk 2",
        scope_entities=acme,
    )
    model_b1 = await _seed_model(
        fresh_db, tenant, born_event=obs, natural="nimbus risk 1",
        scope_entities=nimbus,
    )
    model_b2 = await _seed_model(
        fresh_db, tenant, born_event=obs, natural="nimbus risk 2",
        scope_entities=nimbus,
    )
    trig_a1 = await _enqueue_t2_belief_updated(
        fresh_db, tenant, model_id=model_a1, observation_id=obs
    )
    trig_b1 = await _enqueue_t2_belief_updated(
        fresh_db, tenant, model_id=model_b1, observation_id=obs
    )
    trig_a2 = await _enqueue_t2_belief_updated(
        fresh_db, tenant, model_id=model_a2, observation_id=obs
    )
    trig_b2 = await _enqueue_t2_belief_updated(
        fresh_db, tenant, model_id=model_b2, observation_id=obs
    )
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET enqueued_at = now() - interval '2 seconds'
            WHERE id = ANY($1::uuid[])
            """,
            [trig_a1, trig_a2, trig_b1, trig_b2],
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="t2-entity-batcher",
            tenant_filter=tenant,
            downstream_batch_window_s=1.0,
            downstream_batch_min_size=2,
            t2_batch_max_size=4,
            prune_low_value_downstream_triggers=False,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    assert len(dispatched) == 2
    member_sets = {
        frozenset(batch["payload"]["batch_member_trigger_ids"])
        for batch in dispatched
    }
    assert member_sets == {
        frozenset({str(trig_a1), str(trig_a2)}),
        frozenset({str(trig_b1), str(trig_b2)}),
    }


async def test_downstream_batch_complete_marks_t2_members_complete(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs = await _seed_signal_observation(fresh_db, tenant)
    model_a = await _seed_model(fresh_db, tenant, born_event=obs, natural="a")
    model_b = await _seed_model(fresh_db, tenant, born_event=obs, natural="b")
    trig_a = await _enqueue_t2_belief_updated(fresh_db, tenant, model_id=model_a)
    trig_b = await _enqueue_t2_belief_updated(fresh_db, tenant, model_id=model_b)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET enqueued_at = now() - interval '2 seconds'
            WHERE id = ANY($1::uuid[])
            """,
            [trig_a, trig_b],
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="downstream-complete",
            tenant_filter=tenant,
            downstream_batch_window_s=1.0,
            downstream_batch_min_size=2,
            t2_batch_max_size=4,
            prune_low_value_downstream_triggers=False,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)
    batch = dispatched[0]
    await worker._mark_trigger_complete(batch["id"], payload=batch["payload"])

    async with fresh_db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, completed_at
            FROM think_trigger_queue
            WHERE id = ANY($1::uuid[])
            """,
            [batch["id"], trig_a, trig_b],
        )
    assert all(row["completed_at"] is not None for row in rows)


async def test_worker_prunes_non_prediction_t2_belief_updated(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs = await _seed_signal_observation(fresh_db, tenant)
    model_id = await _seed_model(
        fresh_db, tenant, born_event=obs, natural="ordinary customer risk"
    )
    trigger_id = await _enqueue_t2_belief_updated(
        fresh_db,
        tenant,
        model_id=model_id,
        observation_id=obs,
    )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="t2-pruner",
            tenant_filter=tenant,
            downstream_batch_window_s=1.0,
            downstream_batch_min_size=2,
            t2_batch_max_size=4,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT completed_at, payload
            FROM think_trigger_queue
            WHERE id = $1
            """,
            trigger_id,
        )

    assert dispatched == []
    assert row is not None
    assert row["completed_at"] is not None
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["auto_completed_reason"] == ("non_prediction_belief_updated_noop")


async def test_downstream_batch_coalesces_t4_latent_candidates(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    candidate_a = uuid7()
    candidate_b = uuid7()
    member_a = uuid7()
    member_b = uuid7()
    trig_a = await _enqueue_t4_latent_candidate(
        fresh_db, tenant, candidate_id=candidate_a, member_model_ids=[member_a]
    )
    trig_b = await _enqueue_t4_latent_candidate(
        fresh_db, tenant, candidate_id=candidate_b, member_model_ids=[member_b]
    )
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET enqueued_at = now() - interval '2 seconds'
            WHERE id = ANY($1::uuid[])
            """,
            [trig_a, trig_b],
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="t4-batcher",
            tenant_filter=tenant,
            downstream_batch_window_s=1.0,
            downstream_batch_min_size=2,
            t4_batch_max_size=4,
            prune_low_value_downstream_triggers=False,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)
    assert len(dispatched) == 1

    batch = dispatched[0]
    assert batch["trigger_kind"] == "T4"
    assert batch["trigger_subkind"] == "latent_relationship_candidate"
    payload = batch["payload"]
    assert payload["batch"] is True
    assert payload["relationship_candidate_id"] == str(candidate_a)
    assert payload["relationship_candidate_ids"] == [
        str(candidate_a),
        str(candidate_b),
    ]
    assert set(payload["member_model_ids"]) == {str(member_a), str(member_b)}
    assert "Batch of 2 latent relationship candidates" in payload["seed_natural_text"]


async def test_downstream_batch_keeps_t4_edge_and_situation_lanes_separate(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    repo = RelationshipCandidatesRepo()
    edge_candidates = []
    situation_candidates = []
    async with fresh_db.acquire() as conn:
        for index in range(2):
            left = uuid7()
            right = uuid7()
            edge = make_edge_candidate(
                tenant_id=tenant,
                source_model_id=left,
                target_model_id=right,
                edge_kind="blocks",
                basis="topology_suggested",
                explanation=f"Edge candidate {index} has a blocker surface.",
                scores=JudgmentScores(
                    impact=0.9,
                    actionability=0.9,
                    urgency=0.8,
                    authority_required=0.8,
                    novelty=0.8,
                    confidence=0.8,
                ),
                source="latent_topology",
            )
            edge_candidates.append(await repo.insert(conn, edge))

            members = (uuid7(), uuid7(), uuid7())
            situation = make_situation_candidate(
                tenant_id=tenant,
                situation=f"mixed pressure {index}",
                summary="A composite situation may be forming.",
                relationship_summary="Members share an execution pressure.",
                member_model_ids=members,
                basis="topology_suggested",
                scores=JudgmentScores(
                    impact=0.9,
                    actionability=0.9,
                    urgency=0.8,
                    authority_required=0.8,
                    novelty=0.8,
                    confidence=0.8,
                ),
                source="latent_topology",
                metadata={"pressure_type": "execution"},
            )
            situation_candidates.append(await repo.insert(conn, situation))

    trigger_ids = []
    for record in [
        edge_candidates[0],
        situation_candidates[0],
        edge_candidates[1],
        situation_candidates[1],
    ]:
        members = [
            value
            for value in (
                record.get("member_model_ids")
                or [
                    record.get("source_model_id"),
                    record.get("target_model_id"),
                ]
            )
            if value is not None
        ]
        trigger_ids.append(
            await _enqueue_t4_latent_candidate(
                fresh_db,
                tenant,
                candidate_id=record["id"],
                member_model_ids=members,
            )
        )

    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET enqueued_at = now() - interval '2 seconds'
            WHERE id = ANY($1::uuid[])
            """,
            trigger_ids,
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="t4-kind-aware-batcher",
            tenant_filter=tenant,
            downstream_batch_window_s=1.0,
            downstream_batch_min_size=2,
            t4_batch_max_size=4,
            prune_low_value_downstream_triggers=False,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    assert len(dispatched) == 2
    payloads = [row["payload"] for row in dispatched]
    by_kind = {
        payload["seed_signature"]["candidate_kind"]: payload for payload in payloads
    }
    assert set(by_kind) == {"edge", "situation"}
    assert by_kind["edge"]["relationship_candidate_ids"] == [
        str(record["id"]) for record in edge_candidates
    ]
    assert by_kind["situation"]["relationship_candidate_ids"] == [
        str(record["id"]) for record in situation_candidates
    ]
    assert by_kind["edge"]["seed_signature"]["candidate_kind_counts"] == {"edge": 2}
    assert by_kind["situation"]["seed_signature"]["candidate_kind_counts"] == {
        "situation": 2
    }


async def test_downstream_batch_prefers_t4_entity_lanes(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs = await _seed_signal_observation(fresh_db, tenant)
    acme = [{"type": "customer", "id": "acme"}]
    nimbus = [{"type": "customer", "id": "nimbus"}]
    acme_models = [
        await _seed_model(
            fresh_db,
            tenant,
            born_event=obs,
            natural=f"acme relation endpoint {index}",
            scope_entities=acme,
        )
        for index in range(4)
    ]
    nimbus_models = [
        await _seed_model(
            fresh_db,
            tenant,
            born_event=obs,
            natural=f"nimbus relation endpoint {index}",
            scope_entities=nimbus,
        )
        for index in range(4)
    ]

    repo = RelationshipCandidatesRepo()
    candidates = []
    async with fresh_db.acquire() as conn:
        for index, model_ids in enumerate(
            [
                acme_models[:2],
                nimbus_models[:2],
                acme_models[2:],
                nimbus_models[2:],
            ]
        ):
            candidate = make_edge_candidate(
                tenant_id=tenant,
                source_model_id=model_ids[0],
                target_model_id=model_ids[1],
                edge_kind="blocks",
                basis="topology_suggested",
                explanation=f"Scoped edge candidate {index}.",
                scores=JudgmentScores(
                    impact=0.9,
                    actionability=0.9,
                    urgency=0.8,
                    authority_required=0.8,
                    novelty=0.8,
                    confidence=0.8,
                ),
                source="latent_topology",
            )
            candidates.append(await repo.insert(conn, candidate))

    trigger_ids = []
    for record in candidates:
        trigger_ids.append(
            await _enqueue_t4_latent_candidate(
                fresh_db,
                tenant,
                candidate_id=record["id"],
                member_model_ids=[
                    record["source_model_id"],
                    record["target_model_id"],
                ],
            )
        )
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET enqueued_at = now() - interval '2 seconds'
            WHERE id = ANY($1::uuid[])
            """,
            trigger_ids,
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="t4-entity-batcher",
            tenant_filter=tenant,
            downstream_batch_window_s=1.0,
            downstream_batch_min_size=2,
            t4_batch_max_size=4,
            prune_low_value_downstream_triggers=False,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    assert len(dispatched) == 2
    candidate_sets = {
        frozenset(batch["payload"]["relationship_candidate_ids"])
        for batch in dispatched
    }
    assert candidate_sets == {
        frozenset([str(candidates[0]["id"]), str(candidates[2]["id"])]),
        frozenset([str(candidates[1]["id"]), str(candidates[3]["id"])]),
    }


async def test_worker_prunes_edge_type_t4_candidate_trigger(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    left = uuid7()
    right = uuid7()
    candidate = make_edge_type_candidate(
        tenant_id=tenant,
        proposed_edge_kind="gated_by_decision",
        description="Progress depends on an explicit approval decision.",
        relationship_summary="The relationship belongs in ontology review.",
        nearest_existing_kind="blocks",
        parent_kind="blocks",
        example_source_model_id=left,
        example_target_model_id=right,
        scores=JudgmentScores(impact=0.8, actionability=0.7, confidence=0.6),
    )
    async with fresh_db.acquire() as conn:
        await RelationshipCandidatesRepo().insert(conn, candidate)
    trigger_id = await _enqueue_t4_latent_candidate(
        fresh_db,
        tenant,
        candidate_id=candidate.id,
        member_model_ids=[left, right],
    )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="t4-edge-type-pruner",
            tenant_filter=tenant,
            downstream_batch_window_s=1.0,
            downstream_batch_min_size=2,
            t4_batch_max_size=4,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT completed_at, payload
            FROM think_trigger_queue
            WHERE id = $1
            """,
            trigger_id,
        )
        candidate_row = await RelationshipCandidatesRepo().get(
            conn,
            candidate_id=candidate.id,
            tenant_id=tenant,
        )

    assert dispatched == []
    assert row is not None
    assert row["completed_at"] is not None
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["auto_completed_reason"] == ("edge_type_candidate_aggregation_path")
    assert candidate_row is not None
    assert candidate_row["review_status"] == "needs_review"


async def test_worker_prunes_low_value_topology_t4_candidate_trigger(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    left = uuid7()
    right = uuid7()
    candidate = make_edge_candidate(
        tenant_id=tenant,
        source_model_id=left,
        target_model_id=right,
        edge_kind="same_issue_as",
        basis="topology_suggested",
        explanation="Weak shared-surface topology hint.",
        scores=JudgmentScores(
            impact=0.35,
            actionability=0.25,
            urgency=0.20,
            uncertainty=0.45,
            confidence=0.90,
        ),
        source="latent_topology",
    )
    async with fresh_db.acquire() as conn:
        inserted = await RelationshipCandidatesRepo().insert(conn, candidate)
    trigger_id = await _enqueue_t4_latent_candidate(
        fresh_db,
        tenant,
        candidate_id=inserted["id"],
        member_model_ids=[left, right],
    )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="t4-low-value-pruner",
            tenant_filter=tenant,
            downstream_batch_window_s=0.0,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT completed_at, payload
            FROM think_trigger_queue
            WHERE id = $1
            """,
            trigger_id,
        )

    assert dispatched == []
    assert row is not None
    assert row["completed_at"] is not None
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["auto_completed_reason"] == "low_value_topology_candidate_noop"
    assert payload["candidate_judgment_leverage"] < 0.70


async def test_worker_keeps_high_value_topology_t4_candidate_trigger(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    left = uuid7()
    right = uuid7()
    candidate = make_edge_candidate(
        tenant_id=tenant,
        source_model_id=left,
        target_model_id=right,
        edge_kind="blocks",
        basis="topology_suggested",
        explanation="Material blocker topology hint with action surface.",
        scores=JudgmentScores(
            impact=0.95,
            actionability=0.90,
            urgency=0.85,
            authority_required=0.80,
            uncertainty=0.60,
            novelty=0.75,
            reversibility=0.70,
            confidence=0.80,
        ),
        source="latent_topology",
    )
    async with fresh_db.acquire() as conn:
        inserted = await RelationshipCandidatesRepo().insert(conn, candidate)
    trigger_id = await _enqueue_t4_latent_candidate(
        fresh_db,
        tenant,
        candidate_id=inserted["id"],
        member_model_ids=[left, right],
    )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="t4-high-value-keeper",
            tenant_filter=tenant,
            downstream_batch_window_s=0.0,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    assert len(dispatched) == 1
    assert dispatched[0]["id"] == trigger_id


async def test_worker_aggregates_and_retires_edge_type_t4_examples(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    triggers = []
    candidate_ids = []
    async with fresh_db.acquire() as conn:
        repo = RelationshipCandidatesRepo()
        for _ in range(3):
            left = uuid7()
            right = uuid7()
            candidate = make_edge_type_candidate(
                tenant_id=tenant,
                proposed_edge_kind="gated_by_decision",
                description="Progress depends on an explicit approval decision.",
                relationship_summary="Repeated examples belong in one proposal.",
                nearest_existing_kind="blocks",
                parent_kind="blocks",
                example_source_model_id=left,
                example_target_model_id=right,
                scores=JudgmentScores(
                    impact=0.8,
                    actionability=0.7,
                    confidence=0.6,
                ),
            )
            inserted = await repo.insert(conn, candidate)
            candidate_ids.append(inserted["id"])

    for candidate_id in candidate_ids:
        triggers.append(
            await _enqueue_t4_latent_candidate(
                fresh_db,
                tenant,
                candidate_id=candidate_id,
                member_model_ids=[uuid7(), uuid7()],
            )
        )

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            worker_id="t4-edge-type-aggregator",
            tenant_filter=tenant,
            downstream_batch_window_s=1.0,
            downstream_batch_min_size=2,
            t4_batch_max_size=4,
        ),
    )
    dispatched: list = []

    async def fake_dispatch(row):
        dispatched.append(row)

    worker._dispatch_trigger = fake_dispatch  # type: ignore[method-assign]
    await worker._poll_and_dispatch()
    await asyncio.sleep(0.01)

    async with fresh_db.acquire() as conn:
        trigger_rows = await conn.fetch(
            """
            SELECT completed_at, payload
            FROM think_trigger_queue
            WHERE id = ANY($1::uuid[])
            """,
            triggers,
        )
        candidate_rows = await conn.fetch(
            """
            SELECT review_status, decided_at, metadata
            FROM relationship_candidates
            WHERE id = ANY($1::uuid[])
            """,
            candidate_ids,
        )
        proposals = await conn.fetch(
            """
            SELECT status, example_count
            FROM relationship_ontology_proposals
            WHERE tenant_id = $1
              AND proposed_edge_kind = 'gated_by_decision'
            """,
            tenant,
        )

    assert dispatched == []
    assert all(row["completed_at"] is not None for row in trigger_rows)
    assert {row["review_status"] for row in candidate_rows} == {"retired"}
    assert all(row["decided_at"] is not None for row in candidate_rows)
    assert len(proposals) == 1
    assert proposals[0]["status"] == "review_ready"
    assert proposals[0]["example_count"] == 3
    for row in candidate_rows:
        metadata = row["metadata"]
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        assert metadata.get("ontology_proposal_id")


async def test_two_workers_pick_different_rows(fresh_db, tenant, tenant_cleanup):
    """FOR UPDATE SKIP LOCKED ensures the two pollers don't grab the
    same row."""
    obs = await _seed_signal_observation(fresh_db, tenant)
    ids = [await _enqueue_trigger_row(fresh_db, tenant, obs) for _ in range(4)]

    w1_got: list = []
    w2_got: list = []

    w1 = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=2,
            worker_id="w1",
            tenant_filter=tenant,
            t1_batch_window_s=0.0,
        ),
    )
    w2 = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=2,
            worker_id="w2",
            tenant_filter=tenant,
            t1_batch_window_s=0.0,
        ),
    )

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

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(poll_batch=50, tenant_filter=tenant),
    )
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
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(poll_batch=50, tenant_filter=tenant),
    )
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
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(poll_batch=50, tenant_filter=tenant),
    )
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
    for _ in range(8):
        await _enqueue_trigger_row(fresh_db, tenant, obs)

    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(
            poll_batch=10,
            max_concurrency_per_tenant=4,
            tenant_filter=tenant,
        ),
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

    assert (
        active["max_seen"] <= 4
    ), f"cap violated: saw {active['max_seen']} concurrent dispatches"
    # All 8 ran eventually.
    assert active["count"] == 0


# =====================================================================
# Queue depth + backpressure
# =====================================================================


async def test_queue_depth_counts_pending_rows(fresh_db, tenant, tenant_cleanup):
    obs = await _seed_signal_observation(fresh_db, tenant)
    for _ in range(5):
        await _enqueue_trigger_row(fresh_db, tenant, obs)
    worker = ThinkWorker(fresh_db, config=WorkerConfig(tenant_filter=tenant))
    depth = await worker._queue_depth()
    assert depth >= 5


async def test_backpressure_does_not_prevent_enqueue(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """Queue depth > backpressure_limit still allows new rows to land —
    the worker just logs a warning and keeps polling."""
    obs = await _seed_signal_observation(fresh_db, tenant)
    for _ in range(12):
        await _enqueue_trigger_row(fresh_db, tenant, obs)
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(backpressure_limit=5, tenant_filter=tenant),
    )
    depth = await worker._queue_depth()
    assert depth >= 12
    # New enqueue still succeeds.
    extra = await _enqueue_trigger_row(fresh_db, tenant, obs)
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM think_trigger_queue WHERE id = $1",
            extra,
        )
    assert row is not None


# =====================================================================
# Graceful shutdown
# =====================================================================


async def test_stop_sets_shutdown_event(fresh_db, tenant, tenant_cleanup):
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(poll_interval_s=0.05, tenant_filter=tenant),
    )
    await worker.stop()
    assert worker._shutdown_event.is_set()


async def test_run_exits_on_shutdown_event(fresh_db, tenant, tenant_cleanup):
    """A fresh worker with an empty queue responds to stop() quickly."""
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(poll_interval_s=0.05, tenant_filter=tenant),
    )

    async def stopper():
        await asyncio.sleep(0.1)
        await worker.stop()

    t_stop = asyncio.create_task(stopper())
    await worker.run()
    await t_stop


async def test_run_waits_for_in_flight_tasks_on_shutdown(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    """If the worker has in-flight tasks, run() awaits them on
    shutdown before returning."""
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(poll_interval_s=0.05, tenant_filter=tenant),
    )

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
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs = await _seed_signal_observation(fresh_db, tenant)
    trig = await _enqueue_trigger_row(fresh_db, tenant, obs)
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(trigger_max_attempts=5, tenant_filter=tenant),
    )
    await _lock_trigger(fresh_db, trig, worker.config.worker_id)
    await worker._mark_trigger_failed(trig, "boom1")
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT attempts, completed_at, scheduled_for FROM think_trigger_queue WHERE id = $1",
            trig,
        )
    assert row["attempts"] == 1
    assert row["completed_at"] is None


async def test_mark_trigger_failed_provider_quota_uses_long_backoff(
    fresh_db,
    tenant,
    tenant_cleanup,
    monkeypatch,
):
    monkeypatch.setenv("THINK_PROVIDER_QUOTA_BACKOFF_S", "7200")
    obs = await _seed_signal_observation(fresh_db, tenant)
    trig = await _enqueue_trigger_row(fresh_db, tenant, obs)
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(trigger_max_attempts=5, tenant_filter=tenant),
    )
    await _lock_trigger(fresh_db, trig, worker.config.worker_id)
    await worker._mark_trigger_failed(
        trig,
        "codex cli exited 1: You've hit your usage limit. try again at 8:03 AM.",
    )
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT attempts, completed_at, scheduled_for, now() AS db_now
            FROM think_trigger_queue
            WHERE id = $1
            """,
            trig,
        )
    assert row["attempts"] == 1
    assert row["completed_at"] is None
    assert (row["scheduled_for"] - row["db_now"]).total_seconds() >= 7100


async def test_mark_trigger_failed_eventually_dead_letters(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    obs = await _seed_signal_observation(fresh_db, tenant)
    trig = await _enqueue_trigger_row(fresh_db, tenant, obs)
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(trigger_max_attempts=3, tenant_filter=tenant),
    )
    for i in range(3):
        await _lock_trigger(fresh_db, trig, worker.config.worker_id)
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
    fresh_db,
    tenant,
    tenant_cleanup,
    monkeypatch,
):
    """
    Dispatch the same trigger row twice through _process_trigger. The
    second call sees applied_triggers has a prior row and records
    status='skipped_idempotent' in think_runs.
    """
    monkeypatch.setenv("INQUIRY_LLM_QUESTION_PLANNING_ENABLED", "0")

    obs = await _seed_signal_observation(fresh_db, tenant)
    trig = await _enqueue_trigger_row(
        fresh_db,
        tenant,
        obs,
        subkind="event_arrival",
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
        llm_provider=ScriptedProvider(
            responses=[
                json.dumps(
                    {
                        "trigger_ref": str(trig),
                        "tenant_id": str(tenant),
                        "claim_ops": [],
                        "act_ops": [],
                        "resource_ops": [],
                        "new_predictions": [],
                        "reasoning_trace": "scripted empty",
                    }
                )
            ]
        ),
    )
    await worker._process_trigger(row)

    # Second pass with a fresh worker + provider.
    worker2 = ThinkWorker(
        fresh_db,
        llm_provider=ScriptedProvider(
            responses=[
                json.dumps(
                    {
                        "trigger_ref": str(trig),
                        "tenant_id": str(tenant),
                        "claim_ops": [],
                        "act_ops": [],
                        "resource_ops": [],
                        "new_predictions": [],
                        "reasoning_trace": "scripted empty 2",
                    }
                )
            ]
        ),
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
            "SELECT COUNT(*) FROM applied_triggers WHERE trigger_id = $1",
            trig,
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
    from services.reasoning.think.worker import _populate_seed_fields
    from services.reasoning.retrieval.primary import TriggerContext

    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    _populate_seed_fields(trigger, {"seed_natural_text": "hello world"})
    assert trigger.seed_natural_text == "hello world"


async def test_populate_seed_fields_copies_entity_ids():
    from services.reasoning.think.worker import _populate_seed_fields
    from services.reasoning.retrieval.primary import TriggerContext

    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    entities = [
        {"type": "commitment", "id": "c-187"},
        {"type": "goal", "id": "g-42"},
    ]
    _populate_seed_fields(trigger, {"seed_entity_ids": entities})
    assert trigger.seed_entity_ids == entities


async def test_populate_seed_fields_copies_occurred_at_iso():
    from services.reasoning.think.worker import _populate_seed_fields
    from services.reasoning.retrieval.primary import TriggerContext
    from datetime import datetime, timezone

    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    _populate_seed_fields(trigger, {"seed_occurred_at": "2026-04-20T12:34:56Z"})
    assert trigger.seed_occurred_at == datetime(
        2026, 4, 20, 12, 34, 56, tzinfo=timezone.utc
    )


async def test_populate_seed_fields_copies_scope_actors_as_uuids():
    from services.reasoning.think.worker import _populate_seed_fields
    from services.reasoning.retrieval.primary import TriggerContext

    a = uuid7()
    b = uuid7()
    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    _populate_seed_fields(trigger, {"scope_actors": [str(a), str(b)]})
    assert trigger.scope_actors == [a, b]


async def test_populate_seed_fields_copies_region_spec():
    from services.reasoning.think.worker import _populate_seed_fields
    from services.reasoning.retrieval.primary import TriggerContext

    trigger = TriggerContext(kind="T3", tenant_id=uuid7())
    region = {"entity_ids": [{"type": "commitment", "id": "c-1"}], "scope": "x"}
    _populate_seed_fields(trigger, {"region_spec": region})
    assert trigger.region_spec == region


async def test_populate_seed_fields_skips_missing_fields():
    from services.reasoning.think.worker import _populate_seed_fields
    from services.reasoning.retrieval.primary import TriggerContext

    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    _populate_seed_fields(trigger, {})
    assert trigger.seed_natural_text is None
    assert trigger.seed_entity_ids == []
    assert trigger.seed_occurred_at is None
    assert trigger.scope_actors == []
    assert trigger.region_spec is None


async def test_populate_seed_fields_ignores_malformed_entries():
    from services.reasoning.think.worker import _populate_seed_fields
    from services.reasoning.retrieval.primary import TriggerContext

    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    _populate_seed_fields(
        trigger,
        {
            "seed_entity_ids": ["not-a-dict", {"type": "commitment", "id": "c-1"}],
            "scope_actors": ["not-a-uuid", str(uuid7())],
            "seed_occurred_at": "garbage-timestamp",
        },
    )
    # Only the valid dict entry survives.
    assert trigger.seed_entity_ids == [{"type": "commitment", "id": "c-1"}]
    # Only the valid UUID survives.
    assert len(trigger.scope_actors) == 1
    # Malformed timestamp leaves the field at its default.
    assert trigger.seed_occurred_at is None


async def test_process_trigger_rehydrates_full_payload_for_retrieval(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    tenant_cleanup,
):
    """End-to-end regression: an enqueuer supplying seed_entity_ids +
    scope_actors + seed_occurred_at must reach retrieval through the
    worker, not just seed_natural_text."""
    # Stub `think()` to capture the TriggerContext it sees.
    import services.reasoning.think.worker as worker_mod

    captured: dict = {}

    async def _fake_think(trigger, pool, **kwargs):
        captured["trigger"] = trigger
        # Simulate a successful run so the worker marks the queue row complete.
        from services.reasoning.think.reason import ThinkRunOutcome

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
                trig,
                tenant,
                oid,
                json.dumps(payload),
            )
            row = await conn.fetchrow(
                "SELECT * FROM think_trigger_queue WHERE id = $1",
                trig,
            )
        w = ThinkWorker(
            fresh_db,
            config=WorkerConfig(tenant_filter=tenant),
            llm_provider=ScriptedProvider([]),
        )
        await w._process_trigger(row)
    finally:
        # Restore the real `think` binding so subsequent tests aren't polluted.
        from services.reasoning.think.reason import think as real_think

        worker_mod.think = real_think

    t = captured["trigger"]
    assert t.seed_natural_text == "Alice merged PR"
    assert t.seed_entity_ids == [{"type": "commitment", "id": "c-187"}]
    assert t.seed_occurred_at.year == 2026 and t.seed_occurred_at.month == 4
    assert t.scope_actors == [actor]
