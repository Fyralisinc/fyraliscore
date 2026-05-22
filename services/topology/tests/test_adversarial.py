"""Adversarial integration tests for services.topology.

Targets phase-event boundary conditions, T6 cap behavior, relocate
edge cases (cross-tenant, archived, self-relocate), bounded-cascade
cycles + huge fan-out. Uses the per-test transaction fixture from
conftest.py."""
from __future__ import annotations

import json
from uuid import UUID, uuid4

import pytest

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import TOPO_EMBEDDING_DIM
from lib.topology.relocate import RelocateTarget
from services.models.edges_repo import EdgesRepo
from services.topology.events_repo import (
    DRIFT_JACCARD_THRESHOLD,
    PrevSnapshot,
    TopologyEventsRepo,
    detect_phase_events,
)
from services.topology.neighborhoods_repo import NeighborhoodsRepo
from services.topology.topo_repo import TopoRepo


async def _init_topo(conn, tenant, model_id):
    topo = TopoRepo()
    row = await conn.fetchrow(
        "SELECT embedding FROM models WHERE id = $1", model_id,
    )
    await topo.set_initial_topo(
        conn, model_id=model_id,
        content_embedding=list(float(x) for x in row["embedding"]),
        tenant_id=tenant, enqueue_propagation=False,
    )


# =====================================================================
# Phase-event detector boundary conditions
# =====================================================================


def test_drift_at_exact_threshold_does_emit():
    """Jaccard distance == DRIFT_JACCARD_THRESHOLD → emit (>=)."""
    tenant = uuid4()
    prev_id = uuid4()
    # Build prev/new sets so Jaccard distance ≈ threshold exactly.
    # 5 prev, 3 shared, 5 new: Jaccard 3/7 ≈ 0.428 → distance 0.572.
    # Bump prev to 4 shared: 4/6 ≈ 0.667 → distance 0.333 (below).
    # We want distance >= 0.4. Use 5 prev, 2 shared, 5 new: 2/8 = 0.25 → 0.75.
    a, b, c, d, e = (uuid4() for _ in range(5))
    f, g, h = (uuid4() for _ in range(3))
    nid = uuid4()
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[
            PrevSnapshot(id=prev_id, members=frozenset({a, b, c, d, e}))
        ],
        new_communities={0: {a, b, f, g, h}},  # share 2, total 8 → drift 0.75
        label_to_neighborhood_id={0: nid},
        matched_prev_ids_by_label={0: prev_id},
    )
    drifts = [ev for ev in events if ev.kind == "drift"]
    assert len(drifts) == 1


def test_drift_just_below_threshold_does_not_emit():
    """Jaccard distance just below threshold → no drift."""
    tenant = uuid4()
    prev_id = uuid4()
    # 5 prev, 4 shared, 5 new: Jaccard 4/6=0.667 → distance 0.333 < 0.4.
    a, b, c, d, e = (uuid4() for _ in range(5))
    f = uuid4()
    nid = uuid4()
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[
            PrevSnapshot(id=prev_id, members=frozenset({a, b, c, d, e}))
        ],
        new_communities={0: {a, b, c, d, f}},
        label_to_neighborhood_id={0: nid},
        matched_prev_ids_by_label={0: prev_id},
    )
    assert events == []


def test_one_split_into_many_children():
    """A prior splits into 5 new communities."""
    tenant = uuid4()
    prev_id = uuid4()
    members = [uuid4() for _ in range(20)]
    new_communities = {
        i: set(members[i*4:(i+1)*4])
        for i in range(5)
    }
    label_to_id = {i: uuid4() for i in range(5)}
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[
            PrevSnapshot(id=prev_id, members=frozenset(members))
        ],
        new_communities=new_communities,
        label_to_neighborhood_id=label_to_id,
        matched_prev_ids_by_label={i: None for i in range(5)},
    )
    splits = [ev for ev in events if ev.kind == "split"]
    assert len(splits) == 1
    s = splits[0]
    # 4 siblings besides the largest-share child.
    assert len(s.sibling_neighborhood_ids) == 4


def test_many_priors_merge_into_one():
    """5 priors collapse into a single new community."""
    tenant = uuid4()
    members = [uuid4() for _ in range(20)]
    priors = [
        PrevSnapshot(id=uuid4(), members=frozenset(members[i*4:(i+1)*4]))
        for i in range(5)
    ]
    new_id = uuid4()
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=priors,
        new_communities={0: set(members)},
        label_to_neighborhood_id={0: new_id},
        matched_prev_ids_by_label={0: None},
    )
    merges = [ev for ev in events if ev.kind == "merge"]
    assert len(merges) == 1
    assert len(merges[0].predecessor_neighborhood_ids) == 5


def test_empty_prev_and_empty_new_emits_nothing():
    """Sanity: nothing in, nothing out."""
    tenant = uuid4()
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[],
        new_communities={},
        label_to_neighborhood_id={},
        matched_prev_ids_by_label={},
    )
    assert events == []


def test_split_event_has_singleton_member_list_for_largest_child():
    """Split's neighborhood_id should reference the largest-share
    child; member_model_ids holds that child's members specifically."""
    tenant = uuid4()
    prev_id = uuid4()
    a, b, c, d, e = (uuid4() for _ in range(5))
    new_communities = {
        0: {a, b, c},  # bigger
        1: {d, e},     # smaller
    }
    label_to_id = {0: uuid4(), 1: uuid4()}
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[
            PrevSnapshot(id=prev_id, members=frozenset({a, b, c, d, e}))
        ],
        new_communities=new_communities,
        label_to_neighborhood_id=label_to_id,
        matched_prev_ids_by_label={0: None, 1: None},
    )
    splits = [ev for ev in events if ev.kind == "split"]
    assert len(splits) == 1
    s = splits[0]
    # The bigger child wins (label=0).
    assert s.neighborhood_id == label_to_id[0]
    assert set(s.member_model_ids) == {a, b, c}


# =====================================================================
# TopologyEventsRepo edge cases
# =====================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mark_processed_twice_is_idempotent(tx_conn, tenant, make_model):
    """Mark same event processed twice — no error, no double-update."""
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
    rep = await repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    assert rep.phase_events_emitted == 1
    ev_id = rep.phase_event_ids[0]

    events_repo = TopologyEventsRepo()
    await events_repo.mark_processed(tx_conn, event_id=ev_id)
    await events_repo.mark_processed(tx_conn, event_id=ev_id)  # no-op
    # One row with one processed_at.
    rows = await tx_conn.fetch(
        "SELECT processed_at FROM topology_events WHERE id = $1", ev_id,
    )
    assert len(rows) == 1
    assert rows[0]["processed_at"] is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pending_query_without_tenant(tx_conn, tenant, make_model):
    """`pending(tenant_id=None)` should return events across all
    tenants. Not commonly used but documented."""
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
    pending = await events_repo.pending(tx_conn, tenant_id=None)
    assert any(p["tenant_id"] == tenant for p in pending)


# =====================================================================
# Relocate adversarial — cross-tenant, archived, self-relocate, etc
# =====================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relocate_self_referential_target_no_op_or_zero_delta(
    tx_conn, tenant, make_model,
):
    """Relocate Model A toward A itself with alpha=1.0 should produce
    delta=0 and skip the cascade."""
    a = await make_model("A")
    await _init_topo(tx_conn, tenant, a)
    repo = TopoRepo()
    result = await repo.relocate(
        tx_conn,
        model_id=a,
        tenant_id=tenant,
        target=RelocateTarget(kind="model_id", value=a, alpha=1.0),
        reason="self-target",
    )
    # delta should be ~0 (within float noise).
    assert result["delta"] < 1e-5
    assert result["cascade_enqueued"] == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relocate_target_in_different_tenant_rejects(
    tx_conn, tenant, make_model, born_from_event, actor_id,
):
    """Cross-tenant target → ValidationError. Critical security gate."""
    other_tenant = uuid7()
    other_actor = uuid7()
    other_obs = uuid7()
    await tx_conn.execute(
        "INSERT INTO actors (id, tenant_id, type, display_name, "
        "email, status, metadata, specification_id, created_at) "
        "VALUES ($1, $2, 'human_internal', 'other', null, 'active', "
        "'{}'::jsonb, NULL, now())",
        other_actor, other_tenant,
    )
    await tx_conn.execute(
        "INSERT INTO observations (id, tenant_id, occurred_at, kind, "
        "source_channel, actor_id, content, content_text, embedding, "
        "embedding_pending, trust_tier, external_id, "
        "entities_mentioned) VALUES ($1, $2, now(), 'signal', "
        "'test:other', $3, '{}'::jsonb, 'other obs', NULL, TRUE, "
        "'authoritative', $4, '[]'::jsonb)",
        other_obs, other_tenant, other_actor, f"o-{other_obs}",
    )
    other_model = uuid7()
    import hashlib
    import math
    import random as rng
    seed = int.from_bytes(hashlib.sha256(b"other").digest()[:8], "big")
    r = rng.Random(seed)
    v = [r.gauss(0, 1) for _ in range(768)]
    n = math.sqrt(sum(x * x for x in v))
    emb = [x / n for x in v]
    await tx_conn.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, falsifier, signal_readings,
            supporting_event_ids, supporting_model_ids,
            contributing_models, status, confidence_at_assertion
        ) VALUES (
            $1, $2, $3,
            '{"kind":"state","subject":"x","assertion":"y"}'::jsonb,
            'other', $4,
            '{}'::uuid[], '[]'::jsonb,
            '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
            0.6, NULL, '[]'::jsonb,
            '{}'::uuid[], '{}'::uuid[],
            '{}'::uuid[], 'active', 0.6
        )
        """,
        other_model, other_tenant, other_obs, emb,
    )
    topo = TopoRepo()
    await topo.set_initial_topo(
        tx_conn, model_id=other_model, content_embedding=emb,
        tenant_id=other_tenant, enqueue_propagation=False,
    )

    # Now try to relocate a Model in `tenant` toward other_model
    # (which lives in other_tenant).
    a = await make_model("A")
    await _init_topo(tx_conn, tenant, a)
    repo = TopoRepo()
    with pytest.raises(ValidationError, match="not found"):
        await repo.relocate(
            tx_conn,
            model_id=a,
            tenant_id=tenant,
            target=RelocateTarget(
                kind="model_id", value=other_model, alpha=1.0,
            ),
            reason="cross-tenant attempt",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relocate_archived_model_succeeds_currently(
    tx_conn, tenant, make_model,
):
    """Archived Models can still be relocated — there's no status
    check today. Document the behavior; if we want to reject, add an
    AND status='active' filter to the SELECT in TopoRepo.relocate."""
    a = await make_model("A")
    b = await make_model("B")
    await _init_topo(tx_conn, tenant, a)
    await _init_topo(tx_conn, tenant, b)
    # Archive A.
    await tx_conn.execute(
        "UPDATE models SET status='archived', archived_at=now(), "
        "archive_reason='deprecated' WHERE id = $1", a,
    )
    repo = TopoRepo()
    # Currently this succeeds. If we ever decide archived Models
    # should be immutable in topology, this test will need updating.
    result = await repo.relocate(
        tx_conn,
        model_id=a,
        tenant_id=tenant,
        target=RelocateTarget(kind="model_id", value=b, alpha=1.0),
        reason="archive permissive",
    )
    assert result["delta"] >= 0
    # No bounded cascade (archived A has no neighbors).
    assert result["cascade_enqueued"] == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relocate_alpha_tiny_skips_cascade(
    tx_conn, tenant, make_model,
):
    """Tiny move (alpha~0) → delta tiny → cascade not enqueued."""
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
        tx_conn, source=a, target=c, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await tx_conn.execute(
        "UPDATE topo_dirty_queue SET processed_at = now() "
        "WHERE tenant_id = $1 AND processed_at IS NULL",
        tenant,
    )

    # Tiny move: alpha=0.001 means delta ≈ 0.001 * angular distance.
    repo = TopoRepo()
    result = await repo.relocate(
        tx_conn,
        model_id=a,
        tenant_id=tenant,
        target=RelocateTarget(kind="model_id", value=b, alpha=0.001),
        reason="tiny move",
    )
    # Tiny delta — depending on the absolute size, may be below
    # DELTA_EPSILON (0.05) → no cascade.
    assert result["delta"] < 0.05
    assert result["cascade_enqueued"] == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relocate_records_event_kind_relocate(
    tx_conn, tenant, make_model,
):
    """Verify the topology_events row uses kind='relocate' (not the
    S3 phase kinds). Catches regressions where someone confuses the
    namespaces."""
    a = await make_model("A")
    b = await make_model("B")
    await _init_topo(tx_conn, tenant, a)
    await _init_topo(tx_conn, tenant, b)
    repo = TopoRepo()
    result = await repo.relocate(
        tx_conn,
        model_id=a,
        tenant_id=tenant,
        target=RelocateTarget(kind="model_id", value=b, alpha=1.0),
        reason="test event kind",
    )
    kind = await tx_conn.fetchval(
        "SELECT kind FROM topology_events WHERE id = $1",
        result["event_id"],
    )
    assert kind == "relocate"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relocate_payload_records_alpha_and_reason(
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
        target=RelocateTarget(kind="model_id", value=b, alpha=0.42),
        reason="audit trail check",
    )
    raw_payload = await tx_conn.fetchval(
        "SELECT payload FROM topology_events WHERE id = $1",
        result["event_id"],
    )
    payload = (
        json.loads(raw_payload) if isinstance(raw_payload, str)
        else raw_payload
    )
    assert payload["alpha"] == 0.42
    assert payload["reason"] == "audit trail check"
    assert payload["target_kind"] == "model_id"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bounded_cascade_handles_cycles_in_edge_graph(
    tx_conn, tenant, make_model,
):
    """An undirected cycle (triangle: A→B, A→C, B→C — DAG-legal but
    undirected-cyclic); cascade walks undirected, so the visited set
    must prevent infinite loops."""
    a = await make_model("M0")
    b = await make_model("M1")
    c = await make_model("M2")
    for m in (a, b, c):
        await _init_topo(tx_conn, tenant, m)
    edges = EdgesRepo()
    # Triangle that's still DAG-legal: A→B, A→C, B→C.
    pairs = [(a, b), (a, c), (b, c)]
    for src, tgt in pairs:
        await edges.link(
            tx_conn, source=src, target=tgt, kind="supports",
            tenant_id=tenant, detected_by="manual",
        )
    await tx_conn.execute(
        "UPDATE topo_dirty_queue SET processed_at = now() "
        "WHERE tenant_id = $1 AND processed_at IS NULL",
        tenant,
    )
    repo = TopoRepo()
    enqueued = await repo.bounded_cascade(
        tx_conn,
        origin_model_id=a,
        tenant_id=tenant,
        base_delta=1.0,
        max_depth=10,
        max_fanout=10,
    )
    # 2 unique reachable nodes (B, C), regardless of max_depth.
    # If the visited set didn't work, hop_depth=10 would infinite-loop.
    assert enqueued == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bounded_cascade_max_depth_zero_returns_zero(
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
    repo = TopoRepo()
    enqueued = await repo.bounded_cascade(
        tx_conn,
        origin_model_id=a,
        tenant_id=tenant,
        base_delta=1.0,
        max_depth=0,
    )
    assert enqueued == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bounded_cascade_zero_base_delta_returns_zero(
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
    repo = TopoRepo()
    enqueued = await repo.bounded_cascade(
        tx_conn,
        origin_model_id=a,
        tenant_id=tenant,
        base_delta=0.0,
    )
    assert enqueued == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bounded_cascade_origin_with_no_neighbors(
    tx_conn, tenant, make_model,
):
    """Isolated Model — cascade returns 0 cleanly."""
    a = await make_model("A")
    await _init_topo(tx_conn, tenant, a)
    repo = TopoRepo()
    enqueued = await repo.bounded_cascade(
        tx_conn,
        origin_model_id=a,
        tenant_id=tenant,
        base_delta=1.0,
    )
    assert enqueued == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bounded_cascade_respects_inert_edges(
    tx_conn, tenant, make_model,
):
    """Inert edges (post-archive) should not be walked by the
    bounded cascade. If they were, we'd cascade into archived
    Model neighborhoods unexpectedly."""
    a = await make_model("M0")
    b = await make_model("M1")
    c = await make_model("M2")
    for m in (a, b, c):
        await _init_topo(tx_conn, tenant, m)
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await edges.link(
        tx_conn, source=a, target=c, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    # Mark the A→B edge inert directly.
    await tx_conn.execute(
        "UPDATE model_edges SET status = 'inert', "
        "status_changed_at = now(), status_reason = 'test' "
        "WHERE source_model_id = $1 AND target_model_id = $2 "
        "AND tenant_id = $3",
        a, b, tenant,
    )
    await tx_conn.execute(
        "UPDATE topo_dirty_queue SET processed_at = now() "
        "WHERE tenant_id = $1 AND processed_at IS NULL",
        tenant,
    )
    repo = TopoRepo()
    enqueued = await repo.bounded_cascade(
        tx_conn,
        origin_model_id=a,
        tenant_id=tenant,
        base_delta=1.0,
        max_depth=1,
    )
    # Only C should be reached; B's edge is inert.
    rows = await tx_conn.fetch(
        "SELECT model_id FROM topo_dirty_queue "
        "WHERE tenant_id = $1 AND processed_at IS NULL",
        tenant,
    )
    ids = {r["model_id"] for r in rows}
    assert c in ids
    assert b not in ids
    assert enqueued == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relocate_cascade_dedups_via_queue_constraint(
    tx_conn, tenant, make_model,
):
    """Two relocates back-to-back of the same Model: second cascade
    enqueue should DEDUP the same neighbor that's still pending."""
    a = await make_model("M0")
    b = await make_model("M1")
    c = await make_model("M2")
    for m in (a, b, c):
        await _init_topo(tx_conn, tenant, m)
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await edges.link(
        tx_conn, source=a, target=c, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await tx_conn.execute(
        "UPDATE topo_dirty_queue SET processed_at = now() "
        "WHERE tenant_id = $1 AND processed_at IS NULL",
        tenant,
    )
    target_vec = [0.0] * TOPO_EMBEDDING_DIM
    target_vec[-1] = 1.0
    repo = TopoRepo()
    await repo.relocate(
        tx_conn,
        model_id=a,
        tenant_id=tenant,
        target=RelocateTarget(kind="vector", value=target_vec, alpha=1.0),
        reason="first",
    )
    # Second relocate (toward a different vector). Cascades again;
    # neighbors are already pending → UNIQUE NULLS NOT DISTINCT
    # absorbs duplicates, no error.
    target_vec2 = [0.0] * TOPO_EMBEDDING_DIM
    target_vec2[0] = 1.0
    await repo.relocate(
        tx_conn,
        model_id=a,
        tenant_id=tenant,
        target=RelocateTarget(kind="vector", value=target_vec2, alpha=1.0),
        reason="second",
    )
    # Pending count: B and C, each appears exactly once.
    rows = await tx_conn.fetch(
        "SELECT model_id, COUNT(*) AS n FROM topo_dirty_queue "
        "WHERE tenant_id = $1 AND processed_at IS NULL "
        "GROUP BY model_id",
        tenant,
    )
    assert all(r["n"] == 1 for r in rows)
