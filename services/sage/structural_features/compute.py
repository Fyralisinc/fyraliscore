"""services.sage.structural_features.compute — pure-async feature compute.

Pure functions over an in-memory graph snapshot. No DB, no I/O. The
caller (`job.py`) is responsible for pulling the snapshot and writing
results back via the repo.

Implementation note (deps): networkx is NOT in pyproject.toml. To
keep Phase 5 zero-dependency we implement a minimal pure-Python
version of:

  * undirected & directed degree
  * clustering coefficient (local, undirected)
  * approximate core number (iterative k-core decomposition)
  * average neighbor degree (undirected)
  * common neighbors / Jaccard overlap (per edge)
  * hub_score        — degree-normalized [0, 1] (cheap proxy for
                       HITS hub centrality; SAGE only needs ordinal
                       quality for hub suppression)
  * bridge_score     — local bridge proxy = 1 - clustering, weighted
                       by degree (low clustering + high degree =>
                       likely bridge)
  * bridge_likelihood (per edge) — 1 - jaccard_overlap, biased by
                       cross-cluster degree difference

These are deliberately simple but documented so a future swap to
networkx (or a graph DB) can drop them in. See
fyralis-sage-synthesis-self-evolution.md §10 / Phase 5 for the
target feature list.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Mapping
from uuid import UUID

from services.sage.structural_features.types import (
    EdgeStructuralFeatures,
    ModelStructuralFeatures,
    StructuralEdge,
)

# ---------------------------------------------------------------------
# Graph snapshot helpers
# ---------------------------------------------------------------------


def _build_adjacency(
    model_ids: Iterable[UUID],
    edges: Iterable[StructuralEdge],
) -> tuple[
    dict[UUID, set[UUID]],   # undirected neighbors
    dict[UUID, set[UUID]],   # out-neighbors
    dict[UUID, set[UUID]],   # in-neighbors
]:
    """Build undirected + directed adjacency maps.

    Self-loops and edges to/from models not in `model_ids` are
    silently dropped (the snapshot is allowed to be inconsistent —
    e.g. an edge whose target was archived between the model fetch
    and the edge fetch).
    """
    valid: set[UUID] = set(model_ids)
    undirected: dict[UUID, set[UUID]] = {mid: set() for mid in valid}
    out: dict[UUID, set[UUID]] = {mid: set() for mid in valid}
    in_: dict[UUID, set[UUID]] = {mid: set() for mid in valid}
    for e in edges:
        s, t = e.source_model_id, e.target_model_id
        if s == t:
            continue
        if s not in valid or t not in valid:
            continue
        undirected[s].add(t)
        undirected[t].add(s)
        out[s].add(t)
        in_[t].add(s)
    return undirected, out, in_


# ---------------------------------------------------------------------
# Per-node metrics
# ---------------------------------------------------------------------


def _clustering_coefficient(node: UUID, neighbors: set[UUID],
                            undirected: Mapping[UUID, set[UUID]]) -> float:
    """Local undirected clustering coefficient.

    C(v) = 2 * triangles(v) / (deg(v) * (deg(v) - 1))
    Returns 0.0 for nodes with degree < 2.
    """
    k = len(neighbors)
    if k < 2:
        return 0.0
    neigh = list(neighbors)
    triangles = 0
    for i in range(len(neigh)):
        n_i = undirected.get(neigh[i], set())
        for j in range(i + 1, len(neigh)):
            if neigh[j] in n_i:
                triangles += 1
    return (2.0 * triangles) / (k * (k - 1))


def _avg_neighbor_degree(neighbors: set[UUID],
                         undirected: Mapping[UUID, set[UUID]]) -> float:
    if not neighbors:
        return 0.0
    total = sum(len(undirected.get(n, set())) for n in neighbors)
    return total / len(neighbors)


def _core_numbers(undirected: dict[UUID, set[UUID]]) -> dict[UUID, int]:
    """Iterative k-core decomposition (Batagelj-Zaversnik style).

    Repeatedly peels the min-degree node and records its current
    degree as its core number. O(V + E) with a bucket sort, but we
    use a simple O(V^2) version for clarity — Synthesis graphs in
    Fyralis are bounded in size (~thousands of Models per tenant
    in the typical demo).
    """
    deg: dict[UUID, int] = {v: len(neigh) for v, neigh in undirected.items()}
    remaining: set[UUID] = set(deg)
    core: dict[UUID, int] = {}
    # Snapshot of neighbors that we can mutate.
    neigh: dict[UUID, set[UUID]] = {v: set(n) for v, n in undirected.items()}
    while remaining:
        # Pick the min-degree remaining node.
        v = min(remaining, key=lambda x: deg[x])
        core[v] = deg[v]
        remaining.discard(v)
        for u in list(neigh.get(v, ())):
            if u in remaining and deg[u] > deg[v]:
                deg[u] -= 1
            neigh[u].discard(v)
        neigh[v].clear()
    return core


# ---------------------------------------------------------------------
# Public API: per-Model
# ---------------------------------------------------------------------


async def compute_model_features(
    model_ids: Iterable[UUID],
    edges: Iterable[StructuralEdge],
    *,
    tenant_id: UUID,
) -> list[ModelStructuralFeatures]:
    """Compute per-Model structural features from a graph snapshot.

    Pure async (no I/O) — declared async so callers can `await` it
    uniformly with the I/O-bound siblings without a sync/async
    boundary.
    """
    ids = list(model_ids)
    undirected, out, in_ = _build_adjacency(ids, edges)
    core = _core_numbers(undirected)

    # Degree normalization for hub_score (cheap proxy for HITS).
    if undirected:
        max_deg = max((len(n) for n in undirected.values()), default=0)
    else:
        max_deg = 0

    rows: list[ModelStructuralFeatures] = []
    for mid in ids:
        neigh = undirected.get(mid, set())
        deg_total = len(neigh)
        deg_out = len(out.get(mid, set()))
        deg_in = len(in_.get(mid, set()))
        clustering = _clustering_coefficient(mid, neigh, undirected)
        avg_nbr_deg = _avg_neighbor_degree(neigh, undirected)
        # hub_score: high if degree is high relative to the rest
        # of the graph. 0 for isolates, 1 for the max-degree node.
        hub_score = (deg_total / max_deg) if max_deg > 0 else 0.0
        # bridge_score: high if degree is non-trivial AND clustering
        # is low (neighbors are not connected to each other ⇒ this
        # node is likely on a structural bridge).
        bridge_score = (1.0 - clustering) * hub_score if deg_total >= 2 else 0.0
        rows.append(
            ModelStructuralFeatures(
                model_id=mid,
                tenant_id=tenant_id,
                degree_total=deg_total,
                degree_in=deg_in,
                degree_out=deg_out,
                clustering_coefficient=clustering,
                core_number=core.get(mid, 0),
                avg_neighbor_degree=avg_nbr_deg,
                bridge_score=bridge_score,
                hub_score=hub_score,
                community_id=None,
                region_ids=[],
            )
        )
    return rows


# ---------------------------------------------------------------------
# Public API: per-edge
# ---------------------------------------------------------------------


async def compute_edge_features(
    edges: Iterable[StructuralEdge],
    adjacency: Mapping[UUID, set[UUID]],
    *,
    tenant_id: UUID,
) -> list[EdgeStructuralFeatures]:
    """Compute per-edge structural features.

    `adjacency` is the undirected neighbor map produced by
    `_build_adjacency` (callers typically obtain it via
    `compute_model_features` or by calling `_build_adjacency`
    directly).
    """
    rows: list[EdgeStructuralFeatures] = []
    for e in edges:
        s_neigh = adjacency.get(e.source_model_id, set())
        t_neigh = adjacency.get(e.target_model_id, set())
        # Exclude the direct edge endpoints when measuring
        # neighborhood overlap (a → b should not count b as a
        # neighbor of itself).
        s_only = s_neigh - {e.target_model_id}
        t_only = t_neigh - {e.source_model_id}
        common = s_only & t_only
        union = s_only | t_only
        common_n = len(common)
        jaccard = (common_n / len(union)) if union else 0.0
        deg_s = len(s_neigh)
        deg_t = len(t_neigh)
        deg_diff = float(abs(deg_s - deg_t))
        # bridge_likelihood: low Jaccard ⇒ endpoints sit in
        # different neighborhoods ⇒ likely a topological bridge.
        bridge_likelihood = 1.0 - jaccard
        # redundancy_score: high if neighborhood overlap is large
        # relative to either endpoint's degree (the edge mostly
        # connects nodes that already share many neighbors).
        denom = max(deg_s, deg_t, 1)
        redundancy = common_n / denom
        rows.append(
            EdgeStructuralFeatures(
                edge_id=e.edge_id,
                tenant_id=tenant_id,
                source_model_id=e.source_model_id,
                target_model_id=e.target_model_id,
                degree_difference=deg_diff,
                common_neighbors=common_n,
                jaccard_overlap=jaccard,
                # edge_betweenness_approx left None: exact
                # betweenness is O(V*E) and we don't yet have a
                # consumer for it. Placeholder so the column is
                # present in the row type for forward compat.
                edge_betweenness_approx=None,
                bridge_likelihood=bridge_likelihood,
                redundancy_score=redundancy,
            )
        )
    return rows


# Re-export the adjacency builder so the job + tests can use the same
# normalization the compute layer uses internally.
build_adjacency = _build_adjacency

__all__ = [
    "build_adjacency",
    "compute_edge_features",
    "compute_model_features",
]
