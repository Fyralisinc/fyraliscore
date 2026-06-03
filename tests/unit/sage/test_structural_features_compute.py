"""Unit tests for services.reasoning.sage.structural_features.compute.

Pure compute layer — no Postgres. Each test builds a tiny synthetic
Synthesis graph and verifies the topological invariants described in
fyralis-sage-synthesis-self-evolution.md §10 / Phase 5:

  * 5-node star — center should win hub_score; leaves are tied
  * 4-node "bridge" (two triangles sharing one edge) — the bridge
    endpoints should have high bridge_likelihood / low Jaccard
  * Empty graph — features collapse to defaults without raising
  * Isolated node — degree 0, clustering 0, hub_score 0
"""
from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from services.reasoning.sage.structural_features.compute import (
    build_adjacency,
    compute_edge_features,
    compute_model_features,
)
from services.reasoning.sage.structural_features.types import StructuralEdge


pytestmark = pytest.mark.asyncio


def _mk_edge(src: UUID, tgt: UUID, *, kind: str = "supports") -> StructuralEdge:
    return StructuralEdge(
        edge_id=uuid4(),
        source_model_id=src,
        target_model_id=tgt,
        edge_kind=kind,
        weight=None,
    )


# ----------------------------------------------------------------------
# 5-node star: hub_score concentrated on the center
# ----------------------------------------------------------------------


async def test_star_graph_hub_score_concentrates_on_center() -> None:
    tenant = uuid4()
    center = uuid4()
    leaves = [uuid4() for _ in range(4)]
    ids = [center, *leaves]
    edges = [_mk_edge(center, leaf) for leaf in leaves]

    rows = await compute_model_features(ids, edges, tenant_id=tenant)
    by_id = {r.model_id: r for r in rows}

    # Center has degree 4 (max), leaves have degree 1 each.
    assert by_id[center].degree_total == 4
    for leaf in leaves:
        assert by_id[leaf].degree_total == 1

    # hub_score is degree-normalized, so center == 1.0 and leaves == 0.25.
    assert by_id[center].hub_score == pytest.approx(1.0)
    for leaf in leaves:
        assert by_id[leaf].hub_score == pytest.approx(0.25)

    # Star center: no triangles ⇒ clustering = 0; leaves have degree 1
    # ⇒ clustering also 0. bridge_score for the center is non-zero
    # (degree >= 2 and clustering = 0).
    assert by_id[center].clustering_coefficient == pytest.approx(0.0)
    assert by_id[center].bridge_score == pytest.approx(1.0)
    for leaf in leaves:
        # Leaves are degree 1 ⇒ bridge_score short-circuits to 0.
        assert by_id[leaf].bridge_score == pytest.approx(0.0)

    # Directed degrees: in this snapshot every edge points OUT of the
    # center, so center.out = 4 / center.in = 0, leaves are mirror.
    assert by_id[center].degree_out == 4
    assert by_id[center].degree_in == 0
    for leaf in leaves:
        assert by_id[leaf].degree_in == 1
        assert by_id[leaf].degree_out == 0


# ----------------------------------------------------------------------
# 4-node bridge: two triangles sharing one edge
# ----------------------------------------------------------------------
#
#  a --- b
#  |  X  |
#  c --- d
#
# All four nodes pairwise connected EXCEPT (a, d). The (b, c) edge is
# the "bridge" of the diamond — but in this configuration every node
# has the same degree (3). We verify the per-edge stats: between two
# tightly connected nodes Jaccard is high; between the "shared"
# diagonal it is lower, exercising bridge_likelihood.


async def test_diamond_edge_features_distinguish_bridges_from_redundant_edges() -> None:
    tenant = uuid4()
    a, b, c, d = (uuid4() for _ in range(4))
    ids = [a, b, c, d]
    # Two triangles a-b-c and b-c-d sharing edge (b, c).
    edges = [
        _mk_edge(a, b),
        _mk_edge(a, c),
        _mk_edge(b, c),  # shared edge — endpoints share many neighbors
        _mk_edge(b, d),
        _mk_edge(c, d),
    ]
    undirected, _, _ = build_adjacency(ids, edges)
    edge_rows = await compute_edge_features(edges, undirected, tenant_id=tenant)
    by_endpoints = {(r.source_model_id, r.target_model_id): r for r in edge_rows}

    # (a, b): neighbors of a (minus b) = {c}; neighbors of b (minus a) = {c, d}.
    # common = {c} (1), union = {c, d} (2) ⇒ jaccard = 0.5.
    ab = by_endpoints[(a, b)]
    assert ab.common_neighbors == 1
    assert ab.jaccard_overlap == pytest.approx(0.5)
    assert ab.bridge_likelihood == pytest.approx(0.5)

    # (b, c): neighbors of b (minus c) = {a, d}; neighbors of c (minus b)
    # = {a, d}. common = {a, d} (2), union = {a, d} (2) ⇒ jaccard = 1.0.
    # This is the "redundant" edge in the diamond.
    bc = by_endpoints[(b, c)]
    assert bc.common_neighbors == 2
    assert bc.jaccard_overlap == pytest.approx(1.0)
    assert bc.bridge_likelihood == pytest.approx(0.0)
    # redundancy_score = common / max(deg) = 2 / 3.
    assert bc.redundancy_score == pytest.approx(2.0 / 3.0)


# ----------------------------------------------------------------------
# Pure-bridge edge between two otherwise disjoint cliques
# ----------------------------------------------------------------------
#
#   a - b           e - f
#    \ /     bc      \ /
#     c   ------>     d
#
# Triangle {a,b,c} + triangle {d,e,f} joined by single edge (c, d).
# That edge has zero common neighbors ⇒ jaccard = 0 ⇒
# bridge_likelihood = 1. This is the canonical bridge case.


async def test_bridge_between_two_cliques_has_max_bridge_likelihood() -> None:
    tenant = uuid4()
    a, b, c, d, e, f = (uuid4() for _ in range(6))
    ids = [a, b, c, d, e, f]
    edges = [
        # Triangle 1
        _mk_edge(a, b),
        _mk_edge(b, c),
        _mk_edge(a, c),
        # Bridge
        _mk_edge(c, d),
        # Triangle 2
        _mk_edge(d, e),
        _mk_edge(e, f),
        _mk_edge(d, f),
    ]
    undirected, _, _ = build_adjacency(ids, edges)
    edge_rows = await compute_edge_features(edges, undirected, tenant_id=tenant)
    bridge = next(r for r in edge_rows
                  if (r.source_model_id, r.target_model_id) == (c, d))

    assert bridge.common_neighbors == 0
    assert bridge.jaccard_overlap == pytest.approx(0.0)
    assert bridge.bridge_likelihood == pytest.approx(1.0)
    assert bridge.redundancy_score == pytest.approx(0.0)

    # Model-level bridge_score: c and d should both rank high because
    # they have non-trivial degree (3 each) AND their neighbors don't
    # share many triangles across the bridge.
    model_rows = await compute_model_features(ids, edges, tenant_id=tenant)
    by_id = {r.model_id: r for r in model_rows}
    assert by_id[c].bridge_score > 0.0
    assert by_id[d].bridge_score > 0.0


# ----------------------------------------------------------------------
# Empty + isolated cases
# ----------------------------------------------------------------------


async def test_empty_graph_returns_empty_lists() -> None:
    tenant = uuid4()
    assert await compute_model_features([], [], tenant_id=tenant) == []
    assert await compute_edge_features([], {}, tenant_id=tenant) == []


async def test_isolated_node_yields_zero_features() -> None:
    tenant = uuid4()
    lonely = uuid4()
    rows = await compute_model_features([lonely], [], tenant_id=tenant)
    assert len(rows) == 1
    r = rows[0]
    assert r.degree_total == 0
    assert r.degree_in == 0
    assert r.degree_out == 0
    assert r.clustering_coefficient == pytest.approx(0.0)
    assert r.avg_neighbor_degree == pytest.approx(0.0)
    assert r.hub_score == pytest.approx(0.0)
    assert r.bridge_score == pytest.approx(0.0)
    # k-core of an isolated node is 0.
    assert r.core_number == 0


# ----------------------------------------------------------------------
# Adjacency builder: self-loops + dangling edges are dropped
# ----------------------------------------------------------------------


async def test_build_adjacency_drops_self_loops_and_unknown_endpoints() -> None:
    a, b = uuid4(), uuid4()
    ghost = uuid4()  # not in model_ids
    edges = [
        _mk_edge(a, a),         # self-loop
        _mk_edge(a, b),         # normal
        _mk_edge(a, ghost),     # dangling
        _mk_edge(ghost, b),     # dangling
    ]
    undirected, out, in_ = build_adjacency([a, b], edges)
    assert undirected[a] == {b}
    assert undirected[b] == {a}
    assert out[a] == {b}
    assert in_[b] == {a}
    # Ghost is not a key.
    assert ghost not in undirected
