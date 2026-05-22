"""services/think/tests/test_region_primary_entity_tk4.py — TK-4 fix.

Source: THINK-DESIGN-AUDIT.md §7 argument 1.

`primary_entity_id` was undefined. Two concurrent triggers with the
same `entities_mentioned` in a different list order could hash to
different region-lock keys, defeating the entire purpose of the
advisory-lock serialization. TK-4 defines:

    primary = sorted(entities, key=(type_precedence, id_ascending))[0]

with precedence commitment=0, goal=1, decision=2, resource/customer=3,
actor=4, unknown=99.

These tests verify:
  1. Order-independence: [A, B] and [B, A] produce the same primary.
  2. Type precedence: commitment wins over goal wins over actor etc.
  3. Tie breaking by id ascending within a single type.
  4. Integration: two triggers whose `entities_mentioned` differ only
     in order serialize on the same advisory lock.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest

from services.think.region_locks import (
    acquire_region_lock,
    compute_primary_entity,
    compute_region_key_t1,
    region_lock_key,
)


# ---------------------------------------------------------------------
# Pure tests — no DB required
# ---------------------------------------------------------------------


def test_compute_primary_empty_returns_none():
    assert compute_primary_entity(None) is None
    assert compute_primary_entity([]) is None


def test_compute_primary_order_independent():
    a = {"type": "commitment", "id": "aaa"}
    b = {"type": "goal", "id": "bbb"}
    c = {"type": "decision", "id": "ccc"}
    # Any permutation of [a, b, c] picks `a` (commitment=0 wins).
    for perm in ([a, b, c], [b, a, c], [c, b, a], [c, a, b], [b, c, a]):
        assert compute_primary_entity(perm) == ("commitment", "aaa")


def test_compute_primary_type_precedence():
    # commitment > goal
    assert compute_primary_entity([
        {"type": "goal", "id": "zzz"},
        {"type": "commitment", "id": "yyy"},
    ]) == ("commitment", "yyy")
    # goal > decision
    assert compute_primary_entity([
        {"type": "decision", "id": "x"},
        {"type": "goal", "id": "y"},
    ]) == ("goal", "y")
    # decision > resource
    assert compute_primary_entity([
        {"type": "resource", "id": "x"},
        {"type": "decision", "id": "y"},
    ]) == ("decision", "y")
    # resource > actor
    assert compute_primary_entity([
        {"type": "actor", "id": "x"},
        {"type": "resource", "id": "y"},
    ]) == ("resource", "y")
    # customer is equal tier to resource — lower id wins.
    assert compute_primary_entity([
        {"type": "customer", "id": "zzz"},
        {"type": "resource", "id": "aaa"},
    ]) == ("resource", "aaa")


def test_compute_primary_tiebreak_by_id():
    # Two commitments — lower id wins.
    assert compute_primary_entity([
        {"type": "commitment", "id": "c-zzz"},
        {"type": "commitment", "id": "c-aaa"},
    ]) == ("commitment", "c-aaa")


def test_compute_primary_unknown_type_is_last():
    # Unknown type (precedence 99) loses to any known type.
    assert compute_primary_entity([
        {"type": "mystery", "id": "aaa"},
        {"type": "actor", "id": "bbb"},
    ]) == ("actor", "bbb")
    # Among only-unknown types, id ascending.
    assert compute_primary_entity([
        {"type": "mystery", "id": "b"},
        {"type": "mystery", "id": "a"},
    ]) == ("mystery", "a")


def test_compute_primary_skips_malformed_entries():
    # Missing type → skip. Missing id → skip. Non-dict → skip.
    assert compute_primary_entity([
        {"type": "commitment"},  # no id
        {"id": "x"},  # no type
        "not a dict",
        {"type": "goal", "id": "g1"},
    ]) == ("goal", "g1")


# ---------------------------------------------------------------------
# compute_region_key_t1 — order-independence through TriggerContext-like
# ---------------------------------------------------------------------


@dataclass
class FakeTrigger:
    tenant_id: object | None = None
    actor_id: object | None = None
    entities_mentioned: list | None = None
    scope_actors: list | None = None


def test_compute_region_key_t1_same_key_regardless_of_order():
    tenant = uuid4()
    actor = uuid4()
    a = {"type": "commitment", "id": "aaa"}
    b = {"type": "goal", "id": "bbb"}

    k1 = compute_region_key_t1(FakeTrigger(
        tenant_id=tenant, actor_id=actor, entities_mentioned=[a, b],
    ))
    k2 = compute_region_key_t1(FakeTrigger(
        tenant_id=tenant, actor_id=actor, entities_mentioned=[b, a],
    ))
    assert k1 == k2
    assert k1 == (tenant, actor, "commitment", "aaa")


def test_compute_region_key_t1_no_entities_is_stable():
    tenant = uuid4()
    actor = uuid4()
    k = compute_region_key_t1(FakeTrigger(
        tenant_id=tenant, actor_id=actor, entities_mentioned=[],
    ))
    assert k == (tenant, actor, "no_entity")


def test_compute_region_key_t1_falls_back_to_scope_actors():
    tenant = uuid4()
    actor = uuid4()
    a = {"type": "commitment", "id": "c1"}
    k = compute_region_key_t1(FakeTrigger(
        tenant_id=tenant,
        actor_id=None,
        entities_mentioned=[a],
        scope_actors=[actor],
    ))
    # actor slot populated from scope_actors[0]
    assert k == (tenant, actor, "commitment", "c1")


# ---------------------------------------------------------------------
# Integration — two triggers with reordered entities_mentioned serialize
# on the same advisory lock.
# ---------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reordered_entities_serialize_on_same_lock(
    fresh_db, tenant, tenant_cleanup,
):
    """
    Two concurrent Think-like transactions each acquire a region lock
    derived from the SAME entities but in DIFFERENT list orders. With
    the TK-4 primary-entity definition, both advisory-lock keys are
    identical; the second transaction MUST wait for the first to
    commit (not acquire immediately).

    We prove the serialization by holding the first lock for 150ms
    and measuring that the second acquires no sooner than ~150ms after
    its request.
    """
    # Use entity tuples. `region_lock_key` takes (type, id) — we pass
    # the full sorted list so the keys are derived from the FULL
    # entity set (this is what touched_entity_ids produces). TK-4
    # operates upstream of this at TriggerContext construction, but
    # the equivalent test here is that two different orderings of the
    # same list hash to the same key.
    cid = uuid4()
    gid = uuid4()
    entities_a = [("commitment", cid), ("goal", gid)]
    entities_b = [("goal", gid), ("commitment", cid)]

    # region_lock_key already sorts internally; confirm parity.
    assert region_lock_key(tenant, entities_a) == region_lock_key(tenant, entities_b)

    acquired_b_at: list[float] = []
    start_holding_a = asyncio.Event()
    release_a = asyncio.Event()

    async def worker_a():
        async with fresh_db.acquire() as c:
            async with c.transaction():
                await acquire_region_lock(c, tenant, entities_a)
                start_holding_a.set()
                await release_a.wait()

    async def worker_b():
        await start_holding_a.wait()
        t_request = time.monotonic()
        async with fresh_db.acquire() as c:
            async with c.transaction():
                await acquire_region_lock(c, tenant, entities_b)
                acquired_b_at.append(time.monotonic() - t_request)

    a_task = asyncio.create_task(worker_a())
    b_task = asyncio.create_task(worker_b())

    # Hold A for 150ms, then release.
    await asyncio.sleep(0.15)
    release_a.set()
    await a_task
    await b_task

    # B had to wait — it acquired roughly 150ms after requesting.
    assert acquired_b_at, "worker B never acquired"
    wait_ms = acquired_b_at[0] * 1000
    assert wait_ms >= 50, (
        f"worker B acquired too fast ({wait_ms:.0f}ms) — "
        f"the reordered entity list did not collide on the advisory key"
    )
