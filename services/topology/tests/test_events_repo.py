"""Integration tests for TopologyEventsRepo + the events emitted by
NeighborhoodsRepo.recompute_for_tenant. Uses the per-test transaction
fixture from conftest.py (services/topology/tests)."""
from __future__ import annotations

import pytest

from services.models.edges_repo import EdgesRepo
from services.topology.events_repo import TopologyEventsRepo
from services.topology.neighborhoods_repo import NeighborhoodsRepo
from services.topology.topo_repo import TopoRepo


async def _init_topo(conn, tenant, model_id):
    topo = TopoRepo()
    row = await conn.fetchrow(
        "SELECT embedding FROM models WHERE id = $1", model_id
    )
    await topo.set_initial_topo(
        conn, model_id=model_id,
        content_embedding=list(float(x) for x in row["embedding"]),
        tenant_id=tenant, enqueue_propagation=False,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_emergence_event_emitted_for_first_neighborhood(
    tx_conn, tenant, make_model,
):
    """The first connected pair produces a single emergence event."""
    a = await make_model("A")
    b = await make_model("B")
    await _init_topo(tx_conn, tenant, a)
    await _init_topo(tx_conn, tenant, b)
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    repo = NeighborhoodsRepo()
    report = await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    assert report.phase_events_emitted == 1

    events_repo = TopologyEventsRepo()
    events = await events_repo.list_recent(
        tx_conn, tenant_id=tenant, limit=10,
    )
    assert len(events) == 1
    assert events[0]["kind"] == "emergence"
    assert events[0]["named_signature"] is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dissolution_event_emitted_when_edge_removed(
    tx_conn, tenant, make_model,
):
    a = await make_model("A")
    b = await make_model("B")
    await _init_topo(tx_conn, tenant, a)
    await _init_topo(tx_conn, tenant, b)
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    repo = NeighborhoodsRepo()
    await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    # Now disconnect.
    await edges.unlink(
        tx_conn, source=a, target=b, kind="supports", tenant_id=tenant,
    )
    report = await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    assert report.phase_events_emitted == 1

    events_repo = TopologyEventsRepo()
    events = await events_repo.list_recent(
        tx_conn, tenant_id=tenant, limit=10,
    )
    # 1 emergence (first run) + 1 dissolution (second).
    assert {e["kind"] for e in events} == {"emergence", "dissolution"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_neighborhood_named_signature_populated(
    tx_conn, tenant, make_model,
):
    """Heuristic name lands on the model_neighborhoods row at INSERT
    time."""
    a = await make_model("A")
    b = await make_model("B")
    await _init_topo(tx_conn, tenant, a)
    await _init_topo(tx_conn, tenant, b)
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    repo = NeighborhoodsRepo()
    await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    rows = await repo.list_active(tx_conn, tenant)
    assert len(rows) == 1
    assert rows[0]["named_signature"] is not None
    # Both A and B were inserted with proposition_kind=NULL by the
    # fixture (proposition only). The namer falls back to "unnamed"
    # for kind-less Models.
    assert rows[0]["named_at"] is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pending_returns_unprocessed_only(
    tx_conn, tenant, make_model,
):
    a = await make_model("A")
    b = await make_model("B")
    await _init_topo(tx_conn, tenant, a)
    await _init_topo(tx_conn, tenant, b)
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    repo = NeighborhoodsRepo()
    await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)

    events_repo = TopologyEventsRepo()
    pending = await events_repo.pending(tx_conn, tenant_id=tenant)
    assert len(pending) == 1
    ev_id = pending[0]["id"]
    await events_repo.mark_processed(tx_conn, event_id=ev_id)
    pending2 = await events_repo.pending(tx_conn, tenant_id=tenant)
    assert pending2 == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_idempotent_recompute_emits_no_events(
    tx_conn, tenant, make_model,
):
    """Two recomputes back-to-back with no graph change → second emits
    no events."""
    a = await make_model("A")
    b = await make_model("B")
    await _init_topo(tx_conn, tenant, a)
    await _init_topo(tx_conn, tenant, b)
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    repo = NeighborhoodsRepo()
    r1 = await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    r2 = await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    assert r1.phase_events_emitted == 1
    assert r2.phase_events_emitted == 0
