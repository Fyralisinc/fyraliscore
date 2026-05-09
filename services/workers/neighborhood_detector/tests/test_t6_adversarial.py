"""Adversarial tests for the neighborhood_detector worker's T6 enqueue
behavior. Targets:
  - per-kind cap (NEIGHBORHOOD_DETECTOR_T6_LIMIT_PER_KIND)
  - over-cap events still get processed_at marked
  - multiple tenants in one sweep
  - reentrancy (back-to-back run_once doesn't double-enqueue)"""
from __future__ import annotations

import json
import os

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from services.topology.events_repo import (
    PhaseEvent,
    TopologyEventsRepo,
)
from services.workers.neighborhood_detector.worker import (
    T6_ENQUEUE_PER_KIND_LIMIT,
    _enqueue_t6_for_events,
)


@pytest_asyncio.fixture
async def db_pool():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def tx_conn(db_pool):
    from pgvector.asyncpg import register_vector

    conn = await db_pool.acquire()
    try:
        await register_vector(conn)
    except Exception:
        pass
    tx = conn.transaction()
    await tx.start()
    try:
        yield conn
    finally:
        try:
            await tx.rollback()
        finally:
            await db_pool.release(conn)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_t6_per_kind_cap_enqueues_at_most_n_per_kind(tx_conn):
    """Inject 15 'emergence' events; the cap is 10 → 10 enqueued, 5
    over-cap events still get processed_at set so they don't re-emit."""
    tenant = uuid7()
    events_repo = TopologyEventsRepo()
    event_ids = []
    for _ in range(15):
        ev = PhaseEvent(
            kind="emergence",
            tenant_id=tenant,
            neighborhood_id=uuid7(),
            member_model_ids=[uuid7()],
            magnitude=1.0,
            named_signature="t6 cap test",
        )
        eid = await events_repo.record(tx_conn, event=ev)
        event_ids.append(eid)
    enqueued = await _enqueue_t6_for_events(
        tx_conn, events_repo, tenant, event_ids,
    )
    assert enqueued == T6_ENQUEUE_PER_KIND_LIMIT
    # All 15 events should be processed_at != NULL.
    n = await tx_conn.fetchval(
        "SELECT COUNT(*) FROM topology_events "
        "WHERE tenant_id = $1 AND processed_at IS NOT NULL",
        tenant,
    )
    assert n == 15


@pytest.mark.integration
@pytest.mark.asyncio
async def test_t6_caps_independently_per_kind(tx_conn):
    """5 emergence + 5 dissolution + 5 split should all enqueue (each
    well under the cap of 10)."""
    tenant = uuid7()
    events_repo = TopologyEventsRepo()
    event_ids = []
    for kind in ("emergence", "dissolution", "split"):
        for _ in range(5):
            ev = PhaseEvent(
                kind=kind,  # type: ignore[arg-type]
                tenant_id=tenant,
                neighborhood_id=uuid7(),
                member_model_ids=[uuid7()],
                magnitude=1.0,
            )
            eid = await events_repo.record(tx_conn, event=ev)
            event_ids.append(eid)
    enqueued = await _enqueue_t6_for_events(
        tx_conn, events_repo, tenant, event_ids,
    )
    assert enqueued == 15

    rows = await tx_conn.fetch(
        "SELECT trigger_subkind FROM think_trigger_queue "
        "WHERE tenant_id = $1",
        tenant,
    )
    by_kind: dict[str, int] = {}
    for r in rows:
        by_kind[r["trigger_subkind"]] = by_kind.get(r["trigger_subkind"], 0) + 1
    assert by_kind == {"emergence": 5, "dissolution": 5, "split": 5}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_t6_over_cap_events_skipped_in_second_sweep(tx_conn):
    """Run the enqueue twice with the same event ids: the second
    sweep is a no-op because the events are already processed."""
    tenant = uuid7()
    events_repo = TopologyEventsRepo()
    event_ids = [
        await events_repo.record(
            tx_conn,
            event=PhaseEvent(
                kind="emergence",
                tenant_id=tenant,
                neighborhood_id=uuid7(),
                member_model_ids=[uuid7()],
                magnitude=1.0,
            ),
        )
        for _ in range(3)
    ]
    enqueued1 = await _enqueue_t6_for_events(
        tx_conn, events_repo, tenant, event_ids,
    )
    enqueued2 = await _enqueue_t6_for_events(
        tx_conn, events_repo, tenant, event_ids,
    )
    assert enqueued1 == 3
    assert enqueued2 == 0
    # Trigger queue should still hold only the first 3.
    n = await tx_conn.fetchval(
        "SELECT COUNT(*) FROM think_trigger_queue WHERE tenant_id = $1",
        tenant,
    )
    assert n == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_t6_payload_carries_event_metadata(tx_conn):
    """T6 trigger payload must include topology_event_id, kind,
    neighborhood_id, members, magnitude, named_signature, and a
    seed_natural_text."""
    tenant = uuid7()
    nh_id = uuid7()
    member_ids = [uuid7(), uuid7()]
    pred_ids = [uuid7()]
    ev = PhaseEvent(
        kind="merge",
        tenant_id=tenant,
        neighborhood_id=nh_id,
        member_model_ids=member_ids,
        predecessor_neighborhood_ids=pred_ids,
        magnitude=2.0,
        named_signature="merged cluster",
    )
    events_repo = TopologyEventsRepo()
    eid = await events_repo.record(tx_conn, event=ev)
    await _enqueue_t6_for_events(tx_conn, events_repo, tenant, [eid])

    row = await tx_conn.fetchrow(
        "SELECT payload FROM think_trigger_queue WHERE tenant_id = $1",
        tenant,
    )
    raw = row["payload"]
    payload = json.loads(raw) if isinstance(raw, str) else raw
    assert payload["topology_event_id"] == str(eid)
    assert payload["topology_event_kind"] == "merge"
    assert payload["neighborhood_id"] == str(nh_id)
    assert payload["named_signature"] == "merged cluster"
    assert payload["magnitude"] == 2.0
    assert set(payload["member_model_ids"]) == {str(m) for m in member_ids}
    assert payload["predecessor_neighborhood_ids"] == [str(pred_ids[0])]
    assert "seed_natural_text" in payload
    assert "merged cluster" in payload["seed_natural_text"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_t6_seed_text_handles_drift_with_none_magnitude(tx_conn):
    """A drift event with magnitude=None shouldn't crash the seed-text
    formatter."""
    tenant = uuid7()
    ev = PhaseEvent(
        kind="drift",
        tenant_id=tenant,
        neighborhood_id=uuid7(),
        member_model_ids=[uuid7()],
        magnitude=None,
        named_signature=None,
    )
    events_repo = TopologyEventsRepo()
    eid = await events_repo.record(tx_conn, event=ev)
    enqueued = await _enqueue_t6_for_events(
        tx_conn, events_repo, tenant, [eid],
    )
    assert enqueued == 1
    payload_raw = await tx_conn.fetchval(
        "SELECT payload FROM think_trigger_queue WHERE tenant_id = $1",
        tenant,
    )
    payload = (
        json.loads(payload_raw) if isinstance(payload_raw, str)
        else payload_raw
    )
    assert "drifted" in payload["seed_natural_text"].lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_t6_processed_event_not_re_enqueued(tx_conn):
    """An event already marked processed (e.g., by a prior crash-loop
    cleanup) is filtered out by the SELECT WHERE processed_at IS NULL."""
    tenant = uuid7()
    events_repo = TopologyEventsRepo()
    eid = await events_repo.record(
        tx_conn,
        event=PhaseEvent(
            kind="emergence",
            tenant_id=tenant,
            neighborhood_id=uuid7(),
            member_model_ids=[uuid7()],
            magnitude=1.0,
        ),
    )
    await events_repo.mark_processed(tx_conn, event_id=eid)
    enqueued = await _enqueue_t6_for_events(
        tx_conn, events_repo, tenant, [eid],
    )
    assert enqueued == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_t6_handles_empty_event_id_list(tx_conn):
    tenant = uuid7()
    events_repo = TopologyEventsRepo()
    enqueued = await _enqueue_t6_for_events(
        tx_conn, events_repo, tenant, [],
    )
    assert enqueued == 0
