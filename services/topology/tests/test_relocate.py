"""Integration tests for TopoRepo.relocate + bounded_cascade. Uses
the per-test transaction fixtures from conftest.py."""
from __future__ import annotations

import pytest

from lib.shared.errors import ValidationError
from lib.topology.relocate import RelocateTarget
from services.models.edges_repo import EdgesRepo
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
async def test_relocate_to_model_id_writes_new_topo(
    tx_conn, tenant, make_model,
):
    """Relocate Model A toward Model B; A's topo should move from its
    own anchor toward B's anchor (alpha=1.0 → snap to B)."""
    a = await make_model("A")
    b = await make_model("B")
    await _init_topo(tx_conn, tenant, a)
    await _init_topo(tx_conn, tenant, b)

    # Read B's topo as the expected result.
    b_topo_row = await tx_conn.fetchrow(
        "SELECT topo_embedding FROM models WHERE id = $1", b,
    )
    b_topo = [float(x) for x in b_topo_row["topo_embedding"]]

    repo = TopoRepo()
    result = await repo.relocate(
        tx_conn,
        model_id=a,
        tenant_id=tenant,
        target=RelocateTarget(kind="model_id", value=b, alpha=1.0),
        reason="test relocate",
    )
    assert result["target_kind"] == "model_id"
    assert result["delta"] > 0

    # Verify A's new topo equals B's (within rounding).
    a_topo_row = await tx_conn.fetchrow(
        "SELECT topo_embedding FROM models WHERE id = $1", a,
    )
    a_new_topo = [float(x) for x in a_topo_row["topo_embedding"]]
    assert all(abs(a_new_topo[i] - b_topo[i]) < 1e-4 for i in range(len(b_topo)))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relocate_records_topology_event(
    tx_conn, tenant, make_model,
):
    a = await make_model("A")
    b = await make_model("B")
    await _init_topo(tx_conn, tenant, a)
    await _init_topo(tx_conn, tenant, b)

    repo = TopoRepo()
    result = await repo.relocate(
        tx_conn,
        model_id=a,
        tenant_id=tenant,
        target=RelocateTarget(kind="model_id", value=b, alpha=0.5),
        reason="halfway test",
    )
    event_id = result["event_id"]

    row = await tx_conn.fetchrow(
        """
        SELECT kind, member_model_ids, magnitude, payload
        FROM topology_events WHERE id = $1
        """,
        event_id,
    )
    assert row is not None
    assert row["kind"] == "relocate"
    assert row["member_model_ids"] == [a]
    assert row["magnitude"] is not None and row["magnitude"] > 0
    import json
    payload = (
        json.loads(row["payload"])
        if isinstance(row["payload"], str) else row["payload"]
    )
    assert payload["target_kind"] == "model_id"
    assert payload["alpha"] == 0.5
    assert payload["reason"] == "halfway test"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relocate_to_explicit_vector(
    tx_conn, tenant, make_model,
):
    """Pass an explicit 128-d vector as target.value."""
    from lib.shared.types import TOPO_EMBEDDING_DIM
    import math

    a = await make_model("A")
    await _init_topo(tx_conn, tenant, a)

    target_vec = [0.0] * TOPO_EMBEDDING_DIM
    target_vec[0] = 1.0  # unit vector along axis 0

    repo = TopoRepo()
    result = await repo.relocate(
        tx_conn,
        model_id=a,
        tenant_id=tenant,
        target=RelocateTarget(kind="vector", value=target_vec, alpha=1.0),
        reason="snap to axis-0",
    )
    assert result["delta"] > 0
    a_topo_row = await tx_conn.fetchrow(
        "SELECT topo_embedding FROM models WHERE id = $1", a,
    )
    a_new = [float(x) for x in a_topo_row["topo_embedding"]]
    # Result was L2-normalized; first component should be 1.0.
    assert abs(a_new[0] - 1.0) < 1e-4
    # Norm sanity.
    norm = math.sqrt(sum(x * x for x in a_new))
    assert abs(norm - 1.0) < 1e-4


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relocate_to_neighborhood_centroid(
    tx_conn, tenant, make_model,
):
    """Relocate Model C toward a neighborhood (formed by A↔B)."""
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
    nh_repo = NeighborhoodsRepo()
    await nh_repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    rows = await nh_repo.list_active(tx_conn, tenant)
    assert rows, "expected at least one materialized neighborhood"
    nh_id = rows[0]["id"]

    repo = TopoRepo()
    result = await repo.relocate(
        tx_conn,
        model_id=c,
        tenant_id=tenant,
        target=RelocateTarget(
            kind="neighborhood_id", value=nh_id, alpha=0.7,
        ),
        reason="join the cluster",
    )
    assert result["delta"] > 0
    assert result["target_kind"] == "neighborhood_id"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relocate_with_significant_delta_enqueues_cascade(
    tx_conn, tenant, make_model,
):
    """Relocate that moves the topo a noticeable amount enqueues
    bounded cascade rows for neighbors."""
    a = await make_model("A")
    b = await make_model("B")
    c = await make_model("C")
    for m in (a, b, c):
        await _init_topo(tx_conn, tenant, m)
    edges = EdgesRepo()
    # Connect A to both B and C so cascade has someone to enqueue.
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await edges.link(
        tx_conn, source=a, target=c, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )

    # Drain anything the link calls enqueued so we can assert
    # exclusively on relocate's cascade.
    await tx_conn.execute(
        "UPDATE topo_dirty_queue SET processed_at = now() "
        "WHERE tenant_id = $1 AND processed_at IS NULL",
        tenant,
    )

    # Pick a vector orthogonal to A's content_anchor so the move
    # is large.
    from lib.shared.types import TOPO_EMBEDDING_DIM
    target_vec = [0.0] * TOPO_EMBEDDING_DIM
    target_vec[-1] = 1.0

    repo = TopoRepo()
    result = await repo.relocate(
        tx_conn,
        model_id=a,
        tenant_id=tenant,
        target=RelocateTarget(kind="vector", value=target_vec, alpha=1.0),
        reason="cascade test",
    )
    assert result["delta"] > 0
    assert result["cascade_enqueued"] >= 2

    # Verify B and C are pending in topo_dirty_queue.
    pending = await tx_conn.fetch(
        """
        SELECT model_id FROM topo_dirty_queue
        WHERE tenant_id = $1 AND processed_at IS NULL
        """,
        tenant,
    )
    pending_ids = {r["model_id"] for r in pending}
    assert b in pending_ids
    assert c in pending_ids


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relocate_rejects_missing_model(tx_conn, tenant):
    from uuid import uuid4
    repo = TopoRepo()
    with pytest.raises(ValidationError):
        await repo.relocate(
            tx_conn,
            model_id=uuid4(),
            tenant_id=tenant,
            target=RelocateTarget(kind="vector", value=[0.0] * 128, alpha=1.0),
            reason="should fail",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relocate_rejects_dissolved_neighborhood(
    tx_conn, tenant, make_model,
):
    """A relocate target that points to a dissolved neighborhood must
    be rejected."""
    a = await make_model("A")
    await _init_topo(tx_conn, tenant, a)

    # Create + dissolve a neighborhood manually.
    from uuid import uuid4
    nid = uuid4()
    centroid = [0.0] * 128
    centroid[0] = 1.0
    await tx_conn.execute(
        """
        INSERT INTO model_neighborhoods (
          id, tenant_id, centroid_topo_embedding, member_model_ids,
          status
        ) VALUES ($1, $2, $3::vector, $4, 'dissolved')
        """,
        nid, tenant, centroid, [a],
    )

    repo = TopoRepo()
    with pytest.raises(ValidationError, match="not active"):
        await repo.relocate(
            tx_conn,
            model_id=a,
            tenant_id=tenant,
            target=RelocateTarget(
                kind="neighborhood_id", value=nid, alpha=1.0,
            ),
            reason="should fail",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bounded_cascade_caps_fanout(
    tx_conn, tenant, make_model,
):
    """If a node has >max_fanout neighbors, only top-K (by centrality)
    are enqueued."""
    # Make a "hub" with lots of leaves.
    hub = await make_model("hub")
    leaves = []
    for i in range(8):
        leaf = await make_model(f"leaf-{i}")
        leaves.append(leaf)
    for m in [hub, *leaves]:
        await _init_topo(tx_conn, tenant, m)
    edges = EdgesRepo()
    for leaf in leaves:
        await edges.link(
            tx_conn, source=hub, target=leaf, kind="supports",
            tenant_id=tenant, detected_by="manual",
        )
    # Drain any enqueued rows.
    await tx_conn.execute(
        "UPDATE topo_dirty_queue SET processed_at = now() "
        "WHERE tenant_id = $1 AND processed_at IS NULL",
        tenant,
    )

    repo = TopoRepo()
    enqueued = await repo.bounded_cascade(
        tx_conn,
        origin_model_id=hub,
        tenant_id=tenant,
        base_delta=1.0,
        max_depth=1,
        max_fanout=3,
    )
    assert enqueued == 3

    rows = await tx_conn.fetch(
        "SELECT model_id FROM topo_dirty_queue "
        "WHERE tenant_id = $1 AND processed_at IS NULL",
        tenant,
    )
    assert len(rows) == 3
