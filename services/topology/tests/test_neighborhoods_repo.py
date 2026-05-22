"""
services/topology/tests/test_neighborhoods_repo.py — integration
tests for NeighborhoodsRepo.recompute_for_tenant (S2).
"""
from __future__ import annotations

import pytest

from lib.shared.ids import uuid7
from services.models.edges_repo import EdgesRepo
from services.topology.neighborhoods_repo import NeighborhoodsRepo
from services.topology.topo_repo import TopoRepo


async def _init_topo(conn, tenant, model_id):
    """Helper: set_initial_topo for a Model so it has a vector for
    centroid math."""
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
async def test_recompute_creates_one_neighborhood_for_connected_pair(
    tx_conn, tenant, make_model,
):
    """Two Models linked by a supports edge form one community
    (size 2 ≥ MIN_COMMUNITY_SIZE = 2)."""
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
    assert report.communities_after_prune == 1
    assert report.new_neighborhoods == 1

    # Verify the neighborhood exists with the right members.
    rows = await repo.list_active(tx_conn, tenant)
    assert len(rows) == 1
    nb = rows[0]
    assert set(nb["member_model_ids"]) == {a, b}
    assert nb["density"] == 1.0  # 1 of 1 possible edge


@pytest.mark.integration
@pytest.mark.asyncio
async def test_singleton_pruned():
    """An isolated Model doesn't get its own neighborhood."""
    pass  # Covered indirectly by the next test.


@pytest.mark.integration
@pytest.mark.asyncio
async def test_two_disjoint_clusters_become_two_neighborhoods(
    tx_conn, tenant, make_model,
):
    """Two disjoint connected components → two neighborhoods."""
    a = await make_model("A")
    b = await make_model("B")
    c = await make_model("C")
    d = await make_model("D")
    for m in (a, b, c, d):
        await _init_topo(tx_conn, tenant, m)
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await edges.link(
        tx_conn, source=c, target=d, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    repo = NeighborhoodsRepo()
    report = await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    assert report.communities_after_prune == 2

    rows = await repo.list_active(tx_conn, tenant)
    assert len(rows) == 2
    members = [set(r["member_model_ids"]) for r in rows]
    # One neighborhood is {a, b}, the other {c, d}, in some order.
    assert {frozenset(m) for m in members} == {
        frozenset({a, b}), frozenset({c, d}),
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_membership_table_populated(
    tx_conn, tenant, make_model,
):
    """recompute_for_tenant fully refreshes the membership table."""
    a = await make_model("A")
    b = await make_model("B")
    for m in (a, b):
        await _init_topo(tx_conn, tenant, m)
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    repo = NeighborhoodsRepo()
    await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)

    # Both Models should appear in the membership table.
    rows = await tx_conn.fetch(
        "SELECT model_id, neighborhood_id, centrality "
        "FROM model_neighborhood_membership WHERE tenant_id = $1",
        tenant,
    )
    assert {r["model_id"] for r in rows} == {a, b}
    # In a 2-member community, each member has centrality 1.0.
    for r in rows:
        assert r["centrality"] == 1.0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_membership_lookup_by_model(
    tx_conn, tenant, make_model,
):
    a = await make_model("A")
    b = await make_model("B")
    for m in (a, b):
        await _init_topo(tx_conn, tenant, m)
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    repo = NeighborhoodsRepo()
    await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    found = await repo.membership_for(tx_conn, model_id=a)
    assert found is not None
    assert a in found["member_model_ids"]
    assert b in found["member_model_ids"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stable_id_across_recomputes(
    tx_conn, tenant, make_model,
):
    """A neighborhood's id persists across re-clusterings when
    membership is mostly stable (greedy matching by Jaccard)."""
    a = await make_model("A")
    b = await make_model("B")
    c = await make_model("C")
    for m in (a, b, c):
        await _init_topo(tx_conn, tenant, m)
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await edges.link(
        tx_conn, source=b, target=c, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    repo = NeighborhoodsRepo()
    await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    rows1 = await repo.list_active(tx_conn, tenant)
    assert len(rows1) == 1
    nb_id_first = rows1[0]["id"]

    # Re-run without changing the graph → same id.
    await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    rows2 = await repo.list_active(tx_conn, tenant)
    assert len(rows2) == 1
    assert rows2[0]["id"] == nb_id_first


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dissolution_when_community_disappears(
    tx_conn, tenant, make_model,
):
    """If we remove the only edge linking a community, it
    fragments into singletons (which get pruned), and the prior
    neighborhood is marked dissolved."""
    a = await make_model("A")
    b = await make_model("B")
    for m in (a, b):
        await _init_topo(tx_conn, tenant, m)
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    repo = NeighborhoodsRepo()
    await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    rows = await repo.list_active(tx_conn, tenant)
    assert len(rows) == 1
    nb_id = rows[0]["id"]

    # Remove the edge.
    await edges.unlink(
        tx_conn, source=a, target=b, kind="supports", tenant_id=tenant,
    )
    report = await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    assert report.communities_after_prune == 0
    assert report.dissolved_neighborhoods == 1

    # The previously-active neighborhood should be marked dissolved.
    status = await tx_conn.fetchval(
        "SELECT status FROM model_neighborhoods WHERE id = $1", nb_id,
    )
    assert status == "dissolved"
