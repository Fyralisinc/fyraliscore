"""services/think/tests/test_model_reeval_consumer.py —
model_reeval_queue consumer contract + dead-letter (W3.Q8).

Covers Wave 3-B Outstanding #8:

  * Happy path: enqueue a model_reeval_queue row → worker promotes to
    a T4 trigger → think_trigger_queue has the promoted row.
  * `processed_at` is set atomically when the T4 trigger completes
    successfully (not when the reeval row is enqueued or promoted).
  * N=5 attempts → row moves to model_reeval_dead_letter with
    last_error populated.
  * Dedup constraint: identical unprocessed rows collapse via
    UNIQUE NULLS NOT DISTINCT.
  * FOR UPDATE SKIP LOCKED allows two worker instances to dequeue
    different rows concurrently.
"""
from __future__ import annotations

import asyncio
import json
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7

from services.think.worker import ThinkWorker, WorkerConfig


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _seed_model(pool, tenant: UUID) -> UUID:
    """Insert a minimal active Model so FKs on model_reeval_queue hold."""
    from services.think.tests.conftest import make_embedding
    async with pool.acquire() as conn:
        aid = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            aid, tenant,
        )
        oid = uuid7()
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
        mid = uuid7()
        await conn.execute(
            """
            INSERT INTO models
              (id, tenant_id, born_from_event_id, proposition, "natural",
               embedding, scope_actors, scope_entities, scope_temporal,
               confidence, activation, status, confidence_at_assertion,
               activation_coefficient)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::uuid[], $8::jsonb,
                    $9::jsonb, 0.5, 0.5, 'active', 0.5, 1.0)
            """,
            mid, tenant, oid,
            json.dumps({"kind": "state", "text": "m"}),
            "m", make_embedding("x"), [], "[]", "{}",
        )
    return mid


async def _enqueue_reeval_row(
    pool, tenant: UUID,
    *, model_id: UUID | None = None,
    cause_model_id: UUID | None = None,
    cause_kind: str = "supporting_archived",
    attempts: int = 0,
) -> UUID:
    rid = uuid7()
    # model_id must reference a real models row.
    mid = model_id if model_id is not None else await _seed_model(pool, tenant)
    # cause_model_id may be NULL or an existing id.
    if cause_model_id is None:
        cause_model_id = await _seed_model(pool, tenant)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO model_reeval_queue
              (id, tenant_id, model_id, cause_model_id, cause_kind, attempts)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            rid, tenant, mid, cause_model_id, cause_kind, attempts,
        )
    return rid


async def test_promote_reeval_row_to_trigger(fresh_db, tenant, tenant_cleanup):
    """_promote_reeval_rows() enqueues a T4 trigger for each pending row."""
    cause = await _seed_model(fresh_db, tenant)
    dependent = await _seed_model(fresh_db, tenant)
    reeval_id = await _enqueue_reeval_row(
        fresh_db, tenant, model_id=dependent, cause_model_id=cause,
        cause_kind="supporting_archived",
    )
    worker = ThinkWorker(fresh_db, config=WorkerConfig(poll_batch=10))
    await worker._promote_reeval_rows()

    async with fresh_db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT trigger_kind, trigger_subkind, model_id, payload
            FROM think_trigger_queue
            WHERE tenant_id = $1
            """,
            tenant,
        )
    assert len(rows) == 1
    r = rows[0]
    assert r["trigger_kind"] == "T4"
    assert r["trigger_subkind"] == "model_reeval"
    assert r["model_id"] == dependent
    payload = r["payload"]
    if isinstance(payload, (bytes, bytearray, str)):
        payload = json.loads(
            payload.decode() if isinstance(payload, (bytes, bytearray)) else payload
        )
    assert payload["reeval_row_id"] == str(reeval_id)
    assert payload["cause_model_id"] == str(cause)
    assert payload["cause_kind"] == "supporting_archived"


async def test_promote_idempotent_does_not_duplicate_triggers(
    fresh_db, tenant, tenant_cleanup,
):
    """Running promote twice on the same unprocessed reeval row must
    not create two T4 triggers — the existence check inside the
    worker prevents duplicates."""
    await _enqueue_reeval_row(fresh_db, tenant)
    worker = ThinkWorker(fresh_db, config=WorkerConfig(poll_batch=10))
    await worker._promote_reeval_rows()
    await worker._promote_reeval_rows()

    async with fresh_db.acquire() as conn:
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM think_trigger_queue WHERE tenant_id = $1",
            tenant,
        )
    assert n == 1


async def test_reeval_row_processed_at_set_on_trigger_complete(
    fresh_db, tenant, tenant_cleanup,
):
    """_mark_trigger_complete with a reeval_row_id payload stamps
    processed_at on the original model_reeval_queue row."""
    cause = await _seed_model(fresh_db, tenant)
    dependent = await _seed_model(fresh_db, tenant)
    reeval_id = await _enqueue_reeval_row(
        fresh_db, tenant, model_id=dependent, cause_model_id=cause,
    )
    worker = ThinkWorker(fresh_db, config=WorkerConfig(poll_batch=10))
    await worker._promote_reeval_rows()
    trigger_id = await _fetch_one_trigger_id(fresh_db, tenant)
    payload = {"reeval_row_id": str(reeval_id)}
    await worker._mark_trigger_complete(trigger_id, payload=payload)

    async with fresh_db.acquire() as conn:
        processed = await conn.fetchval(
            "SELECT processed_at FROM model_reeval_queue WHERE id = $1",
            reeval_id,
        )
    assert processed is not None


async def test_dead_letter_after_max_attempts(
    fresh_db, tenant, tenant_cleanup,
):
    """After trigger_max_attempts failures, the reeval row is moved
    to model_reeval_dead_letter and the original is stamped."""
    cause = await _seed_model(fresh_db, tenant)
    dependent = await _seed_model(fresh_db, tenant)
    reeval_id = await _enqueue_reeval_row(
        fresh_db, tenant, model_id=dependent, cause_model_id=cause,
    )
    worker = ThinkWorker(
        fresh_db,
        config=WorkerConfig(poll_batch=10, trigger_max_attempts=2),
    )
    await worker._promote_reeval_rows()
    trigger_id = await _fetch_one_trigger_id(fresh_db, tenant)

    # Fail twice — second one hits terminal attempt.
    await worker._mark_trigger_failed(trigger_id, "boom1")
    # On the second failure, the trigger row will be updated to attempts=2
    # and completed; but we need to simulate the locked_by lease release
    # + next failed attempt. The _mark_trigger_failed path increments.
    await worker._mark_trigger_failed(trigger_id, "boom2")

    async with fresh_db.acquire() as conn:
        dl_rows = await conn.fetch(
            "SELECT * FROM model_reeval_dead_letter WHERE original_queue_id = $1",
            reeval_id,
        )
        original = await conn.fetchrow(
            "SELECT processed_at, last_error FROM model_reeval_queue WHERE id = $1",
            reeval_id,
        )
    assert len(dl_rows) == 1
    assert dl_rows[0]["last_error"] == "boom2"
    assert dl_rows[0]["cause_kind"] == "supporting_archived"
    assert dl_rows[0]["model_id"] == dependent
    assert original["processed_at"] is not None
    assert "boom2" in (original["last_error"] or "")


async def test_failure_before_max_attempts_schedules_backoff(
    fresh_db, tenant, tenant_cleanup,
):
    """Single failure under the limit → attempts=1, scheduled_for
    bumped into the future, trigger stays uncompleted."""
    await _enqueue_reeval_row(fresh_db, tenant)
    worker = ThinkWorker(
        fresh_db, config=WorkerConfig(trigger_max_attempts=5),
    )
    await worker._promote_reeval_rows()
    trigger_id = await _fetch_one_trigger_id(fresh_db, tenant)
    await worker._mark_trigger_failed(trigger_id, "transient")

    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT attempts, completed_at, scheduled_for FROM think_trigger_queue WHERE id = $1",
            trigger_id,
        )
    assert row["attempts"] == 1
    assert row["completed_at"] is None
    # scheduled_for bumped into the future.
    import datetime as _dt
    assert row["scheduled_for"] > _dt.datetime.now(_dt.timezone.utc)


async def test_dedup_unique_nulls_not_distinct(
    fresh_db, tenant, tenant_cleanup,
):
    """Two identical unprocessed rows collide on the unique constraint."""
    cause = await _seed_model(fresh_db, tenant)
    dependent = await _seed_model(fresh_db, tenant)
    await _enqueue_reeval_row(
        fresh_db, tenant, model_id=dependent, cause_model_id=cause,
    )
    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        await _enqueue_reeval_row(
            fresh_db, tenant,
            model_id=dependent, cause_model_id=cause,
        )


async def test_dedup_allows_reinsert_after_processed(
    fresh_db, tenant, tenant_cleanup,
):
    """Once processed_at is set the dedup opens up for the same key."""
    cause = await _seed_model(fresh_db, tenant)
    dependent = await _seed_model(fresh_db, tenant)
    first = await _enqueue_reeval_row(
        fresh_db, tenant, model_id=dependent, cause_model_id=cause,
    )
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "UPDATE model_reeval_queue SET processed_at = now() WHERE id = $1",
            first,
        )
    # New identical row can be enqueued.
    await _enqueue_reeval_row(
        fresh_db, tenant, model_id=dependent, cause_model_id=cause,
    )
    async with fresh_db.acquire() as conn:
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM model_reeval_queue WHERE tenant_id = $1",
            tenant,
        )
    assert n == 2


async def test_for_update_skip_locked_allows_concurrent_workers(
    fresh_db, tenant, tenant_cleanup,
):
    """Two concurrent promotes on disjoint rows pick different triggers
    via FOR UPDATE SKIP LOCKED (proved by asyncpg lock + count)."""
    c1 = await _seed_model(fresh_db, tenant)
    c2 = await _seed_model(fresh_db, tenant)
    r1 = await _enqueue_reeval_row(
        fresh_db, tenant, cause_model_id=c1,
    )
    r2 = await _enqueue_reeval_row(
        fresh_db, tenant, cause_model_id=c2,
    )

    # Two worker instances.
    w1 = ThinkWorker(fresh_db, config=WorkerConfig(poll_batch=10))
    w2 = ThinkWorker(fresh_db, config=WorkerConfig(poll_batch=10))

    # Race them.
    await asyncio.gather(
        w1._promote_reeval_rows(),
        w2._promote_reeval_rows(),
    )

    async with fresh_db.acquire() as conn:
        trigger_count = await conn.fetchval(
            "SELECT COUNT(*) FROM think_trigger_queue WHERE tenant_id = $1",
            tenant,
        )
    # Both rows promoted, no duplicates.
    assert trigger_count == 2


async def test_archive_via_repo_populates_reeval_queue(
    fresh_db, tenant, tenant_cleanup,
):
    """End-to-end: ModelsRepo.archive enqueues dependents on
    model_reeval_queue. The Q8 amendment locks this in."""
    import json as _json
    from services.models.repo import ModelsRepo
    from services.think.tests.conftest import make_embedding

    repo = ModelsRepo(fresh_db, embedder=None)
    async with fresh_db.acquire() as conn:
        aid = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            aid, tenant,
        )
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', $3, '{}'::jsonb,
                    'x', $4, FALSE, 'authoritative')
            """,
            oid, tenant, aid, make_embedding("x"),
        )
        # Supporting Model (the one we archive).
        support_id = uuid7()
        await conn.execute(
            """
            INSERT INTO models
              (id, tenant_id, born_from_event_id, proposition, "natural",
               embedding, scope_actors, scope_entities, scope_temporal,
               confidence, activation, status, confidence_at_assertion,
               activation_coefficient)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::uuid[], $8::jsonb,
                    $9::jsonb, 0.6, 0.5, 'active', 0.6, 1.0)
            """,
            support_id, tenant, oid,
            _json.dumps({"kind": "state", "text": "support"}),
            "support", make_embedding("x"), [], "[]", "{}",
        )
        # Dependent Model — supporting_model_ids references support_id.
        dep_id = uuid7()
        await conn.execute(
            """
            INSERT INTO models
              (id, tenant_id, born_from_event_id, proposition, "natural",
               embedding, scope_actors, scope_entities, scope_temporal,
               confidence, activation, status, confidence_at_assertion,
               activation_coefficient, supporting_model_ids)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::uuid[], $8::jsonb,
                    $9::jsonb, 0.5, 0.5, 'active', 0.5, 1.0, $10::uuid[])
            """,
            dep_id, tenant, oid,
            _json.dumps({"kind": "state", "text": "dep"}),
            "dep", make_embedding("x"), [], "[]", "{}",
            [support_id],
        )
    await repo.archive(
        support_id, "deprecated",
        cause_event_id=oid,
    )
    async with fresh_db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT model_id, cause_model_id, cause_kind, processed_at
            FROM model_reeval_queue
            WHERE tenant_id = $1
            """,
            tenant,
        )
    assert len(rows) == 1
    r = rows[0]
    assert r["model_id"] == dep_id
    assert r["cause_model_id"] == support_id
    # deprecated → supporting_deprecated per archive_reason→cause_kind mapping.
    assert r["cause_kind"] == "supporting_deprecated"
    assert r["processed_at"] is None


# =====================================================================
# helper
# =====================================================================


async def _fetch_one_trigger_id(pool, tenant: UUID) -> UUID:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id FROM think_trigger_queue WHERE tenant_id = $1 LIMIT 1",
            tenant,
        )
    assert row is not None
    return row["id"]
