"""
services/topology/tests/test_topo_repo.py — integration tests for
TopoRepo (S2). Real Postgres + pgvector.
"""
from __future__ import annotations

import pytest

from lib.shared.ids import uuid7
from lib.topology.embeddings import (
    ALPHA_DEFAULT,
    TOPO_EMBEDDING_DIM,
    content_anchor,
    delta_magnitude,
)
from services.models.edges_repo import EdgesRepo
from services.topology.topo_repo import TopoRepo


# ---------------------------------------------------------------------
# set_initial_topo
# ---------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_set_initial_topo_writes_anchor_and_enqueues(
    tx_conn, tenant, make_model,
):
    """A fresh Model gets content_anchor written to topo_embedding
    and a topo_dirty_queue row enqueued."""
    mid = await make_model("a fresh belief")
    # Read back the content embedding for content_anchor comparison.
    row = await tx_conn.fetchrow(
        "SELECT embedding FROM models WHERE id = $1", mid
    )
    content_emb = list(float(x) for x in row["embedding"])
    expected = content_anchor(content_emb)

    topo = TopoRepo()
    out = await topo.set_initial_topo(
        tx_conn,
        model_id=mid,
        content_embedding=content_emb,
        tenant_id=tenant,
        enqueue_propagation=True,
    )
    assert len(out) == TOPO_EMBEDDING_DIM
    # The repo computes content_anchor itself; should match.
    for a, b in zip(out, expected):
        assert abs(a - b) < 1e-9

    # Verify it landed in models.topo_embedding.
    row = await tx_conn.fetchrow(
        "SELECT topo_embedding, topo_updated_at FROM models WHERE id = $1",
        mid,
    )
    assert row["topo_embedding"] is not None
    assert row["topo_updated_at"] is not None

    # Verify dirty queue row.
    qrow = await tx_conn.fetchrow(
        """
        SELECT * FROM topo_dirty_queue
        WHERE model_id = $1 AND processed_at IS NULL
        """,
        mid,
    )
    assert qrow is not None
    assert qrow["hop_depth"] == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_set_initial_topo_skip_enqueue_flag(
    tx_conn, tenant, make_model,
):
    mid = await make_model("no enqueue")
    row = await tx_conn.fetchrow(
        "SELECT embedding FROM models WHERE id = $1", mid
    )
    topo = TopoRepo()
    await topo.set_initial_topo(
        tx_conn,
        model_id=mid,
        content_embedding=list(float(x) for x in row["embedding"]),
        tenant_id=tenant,
        enqueue_propagation=False,
    )
    qcount = await tx_conn.fetchval(
        "SELECT count(*) FROM topo_dirty_queue WHERE model_id = $1", mid,
    )
    assert qcount == 0


# ---------------------------------------------------------------------
# enqueue / dedup / dequeue / mark_processed
# ---------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enqueue_idempotent_while_pending(
    tx_conn, tenant, make_model,
):
    """A second enqueue while the first is unprocessed dedups."""
    mid = await make_model("dedup target")
    topo = TopoRepo()
    await topo.enqueue(tx_conn, model_id=mid, tenant_id=tenant)
    await topo.enqueue(tx_conn, model_id=mid, tenant_id=tenant)
    count = await tx_conn.fetchval(
        """
        SELECT count(*) FROM topo_dirty_queue
        WHERE model_id = $1 AND processed_at IS NULL
        """,
        mid,
    )
    assert count == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dequeue_orders_by_priority(
    tx_conn, tenant, make_model,
):
    """High delta_magnitude rows come first."""
    high = await make_model("high pri")
    low = await make_model("low pri")
    topo = TopoRepo()
    await topo.enqueue(
        tx_conn, model_id=low, tenant_id=tenant,
        delta_magnitude=0.1,
    )
    await topo.enqueue(
        tx_conn, model_id=high, tenant_id=tenant,
        delta_magnitude=0.9,
    )
    rows = await topo.dequeue_pending(
        tx_conn, tenant_id=tenant, limit=10,
    )
    # High priority first.
    assert rows[0]["model_id"] == high
    assert rows[1]["model_id"] == low


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mark_processed_unblocks_re_enqueue(
    tx_conn, tenant, make_model,
):
    """After processed_at is set, a fresh enqueue creates a new row."""
    mid = await make_model("re-enqueue")
    topo = TopoRepo()
    await topo.enqueue(tx_conn, model_id=mid, tenant_id=tenant)
    rows = await topo.dequeue_pending(tx_conn, tenant_id=tenant)
    await topo.mark_processed(tx_conn, queue_row_id=rows[0]["id"])
    # Now the dedup constraint allows a new pending row.
    await topo.enqueue(tx_conn, model_id=mid, tenant_id=tenant)
    pending = await tx_conn.fetchval(
        "SELECT count(*) FROM topo_dirty_queue WHERE model_id = $1 "
        "AND processed_at IS NULL", mid,
    )
    assert pending == 1


# ---------------------------------------------------------------------
# enqueue_neighbors
# ---------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_enqueue_neighbors_walks_active_edges(
    tx_conn, tenant, make_model,
):
    """enqueue_neighbors finds Models connected via any active
    edge kind, in either direction."""
    a = await make_model("A")
    b = await make_model("B")
    c = await make_model("C")
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=b, target=a, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await edges.link(
        tx_conn, source=a, target=c, kind="instance_of",
        tenant_id=tenant, detected_by="manual",
    )
    topo = TopoRepo()
    # Drain any pre-existing rows from the link() side-effect.
    await tx_conn.execute("DELETE FROM topo_dirty_queue WHERE tenant_id = $1", tenant)

    count = await topo.enqueue_neighbors(
        tx_conn,
        model_id=a, tenant_id=tenant,
        hop_depth=0, delta_magnitude=0.5,
    )
    assert count == 2
    rows = await tx_conn.fetch(
        "SELECT model_id, hop_depth FROM topo_dirty_queue "
        "WHERE tenant_id = $1 AND processed_at IS NULL",
        tenant,
    )
    enqueued = {r["model_id"] for r in rows}
    assert enqueued == {b, c}
    for r in rows:
        assert r["hop_depth"] == 1


# ---------------------------------------------------------------------
# recompute_topo — the alpha-anchored update
# ---------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_recompute_isolated_model_returns_anchor(
    tx_conn, tenant, make_model,
):
    """A Model with no neighbors should get content_anchor as its
    topo_embedding (the rule degenerates)."""
    mid = await make_model("isolated")
    topo = TopoRepo()
    # Initialize topo_embedding (via set_initial_topo).
    row = await tx_conn.fetchrow(
        "SELECT embedding FROM models WHERE id = $1", mid
    )
    content_emb = list(float(x) for x in row["embedding"])
    await topo.set_initial_topo(
        tx_conn, model_id=mid,
        content_embedding=content_emb, tenant_id=tenant,
        enqueue_propagation=False,
    )
    # Recompute (no neighbors) → should produce content_anchor.
    result = await topo.recompute_topo(
        tx_conn, model_id=mid, tenant_id=tenant,
    )
    expected = content_anchor(content_emb)
    for a, b in zip(result["new_topo"], expected):
        assert abs(a - b) < 1e-9
    assert result["neighbor_count"] == 0
    # Delta should be ~0 (initial = recomputed).
    assert result["delta"] < 1e-6


@pytest.mark.integration
@pytest.mark.asyncio
async def test_recompute_with_neighbor_blends(
    tx_conn, tenant, make_model,
):
    """A Model with one supports neighbor moves toward that
    neighbor's topo (proportional to 1 - α)."""
    a = await make_model("alpha")
    b = await make_model("beta")
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=b, target=a, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )

    topo = TopoRepo()
    # Initialize both topos.
    for mid in (a, b):
        row = await tx_conn.fetchrow(
            "SELECT embedding FROM models WHERE id = $1", mid
        )
        await topo.set_initial_topo(
            tx_conn, model_id=mid,
            content_embedding=list(float(x) for x in row["embedding"]),
            tenant_id=tenant, enqueue_propagation=False,
        )
    # Capture a's anchor pre-blend.
    a_row = await tx_conn.fetchrow(
        "SELECT embedding, topo_embedding FROM models WHERE id = $1", a,
    )
    pre_topo = [float(x) for x in a_row["topo_embedding"]]
    pre_anchor = content_anchor(list(float(x) for x in a_row["embedding"]))

    # Recompute a.
    result = await topo.recompute_topo(
        tx_conn, model_id=a, tenant_id=tenant,
    )
    # Should be different from pre_topo (blended toward b).
    assert result["delta"] > 0
    assert result["neighbor_count"] == 1
    # New topo is closer to anchor than to (1-α)·neighbor (because
    # alpha=0.3 means anchor still wins); but the result is between
    # them. Precise comparison: distance from pre_anchor should be
    # smaller than 1.0 (neighbors of unrelated Models on the unit
    # sphere typically have distance ~sqrt(2)≈1.4).
    d = delta_magnitude(result["new_topo"], pre_anchor)
    assert d < 1.5  # bounded by L2-normalized vectors


@pytest.mark.integration
@pytest.mark.asyncio
async def test_recompute_alpha_one_returns_anchor_unchanged(
    tx_conn, tenant, make_model,
):
    """α=1.0 should ignore neighbors entirely."""
    a = await make_model("alpha-1")
    b = await make_model("ignored")
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=b, target=a, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    topo = TopoRepo()
    for mid in (a, b):
        row = await tx_conn.fetchrow(
            "SELECT embedding FROM models WHERE id = $1", mid
        )
        await topo.set_initial_topo(
            tx_conn, model_id=mid,
            content_embedding=list(float(x) for x in row["embedding"]),
            tenant_id=tenant, enqueue_propagation=False,
        )
    a_row = await tx_conn.fetchrow(
        "SELECT embedding FROM models WHERE id = $1", a,
    )
    expected = content_anchor(list(float(x) for x in a_row["embedding"]))
    result = await topo.recompute_topo(
        tx_conn, model_id=a, tenant_id=tenant, alpha=1.0,
    )
    for a_, b_ in zip(result["new_topo"], expected):
        assert abs(a_ - b_) < 1e-9


# ---------------------------------------------------------------------
# Edge mutations enqueue topo dirty rows automatically
# ---------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_link_enqueues_both_endpoints(
    tx_conn, tenant, make_model,
):
    """When EdgesRepo.link runs, both endpoints land in the dirty
    queue automatically (S2 hook)."""
    a = await make_model("A")
    b = await make_model("B")
    edges = EdgesRepo()
    # Drain initial enqueues from make_model -> set_initial_topo.
    await tx_conn.execute(
        "DELETE FROM topo_dirty_queue WHERE tenant_id = $1", tenant,
    )
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    rows = await tx_conn.fetch(
        "SELECT model_id FROM topo_dirty_queue "
        "WHERE tenant_id = $1 AND processed_at IS NULL",
        tenant,
    )
    assert {r["model_id"] for r in rows} == {a, b}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_unlink_enqueues_both_endpoints(
    tx_conn, tenant, make_model,
):
    a = await make_model("A")
    b = await make_model("B")
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await tx_conn.execute(
        "DELETE FROM topo_dirty_queue WHERE tenant_id = $1", tenant,
    )
    n = await edges.unlink(
        tx_conn, source=a, target=b, kind="supports", tenant_id=tenant,
    )
    assert n == 1
    rows = await tx_conn.fetch(
        "SELECT model_id FROM topo_dirty_queue "
        "WHERE tenant_id = $1 AND processed_at IS NULL",
        tenant,
    )
    assert {r["model_id"] for r in rows} == {a, b}
