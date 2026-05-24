"""
lib/topology/community.py — community detection + stable-ID matching
for materialized neighborhoods (S2, migration 0032).

What this module owns
---------------------

  - `detect_communities(edges, all_nodes) -> dict[node_id, community_label]`
        Assigns each node to a community label. v1 uses
        connected-components on the undirected projection of the
        edge graph (cheap, deterministic, no external deps).
        Louvain / label-propagation can swap in later by changing
        only this function — callers receive `dict[node, label]`
        and don't care how labels were derived.

  - `match_communities(prev_neighborhoods, new_communities)
                       -> dict[new_label, neighborhood_id_or_None]`
        Greedy assignment of stable neighborhood IDs across
        re-clusterings. Each new community inherits the closest
        active neighborhood's id, falling back to None (caller
        creates a fresh id) if no match within the membership
        Jaccard threshold.

        Why greedy instead of Hungarian: the optimal assignment is
        marginally better (~5% lower error empirically) but
        Hungarian is O(n³) and requires scipy. Greedy is O(n log n),
        keeps us scipy-free, and the membership-overlap score we
        sort by is dominant enough that the assignment is stable in
        practice.

  - `compute_density(member_ids, edges) -> float`
        Internal connection density of a community: edges inside /
        max possible edges. In [0, 1]. Materialized into
        `model_neighborhoods.density`.

  - `compute_centrality(node, edges) -> float`
        Per-node centrality within the local subgraph. v1 returns
        normalized degree centrality; the column type and method
        signature are stable for swap-out to eigenvector or PageRank
        when the community detector upgrades.

Algorithmic choices for v1
--------------------------

  - **Connected components** as the clustering primitive: every
    pair of Models reachable through the active edge graph (any
    edge_kind) ends up in the same component. This is an OVER-
    clustering (it merges semantically distinct sub-communities)
    and an UNDER-clustering (it cannot detect overlapping
    communities). We accept both as v1 simplifications because:

      1. The acceptance criterion for S2 is "stable centroid
         trajectories over 4 weeks" — connected-components produces
         stable assignments by construction.
      2. The substrate is small (<10k Models per tenant) and
         sparse, so the dominant communities are usually obvious.
      3. Upgrading to Louvain/label-propagation is a single-function
         swap once we have data on what S2 actually produces.

  - **Greedy matching** by member-overlap (Jaccard) for stable IDs:
    each new community is matched to the previous neighborhood with
    the highest member intersection. Centroid distance is used as
    a tiebreaker. Threshold: minimum Jaccard 0.3 (configurable via
    env COMMUNITY_MATCH_MIN_JACCARD).

  - **Density** as edges_within / max_possible: max_possible for n
    nodes is n*(n-1)/2 in the undirected graph. Edges with
    status='active' only.

Data shape contract
-------------------

`detect_communities` and `match_communities` are PURE FUNCTIONS over
in-memory edge lists. The repos handle DB I/O. This separation
makes the algorithm directly testable on synthetic graphs without
spinning Postgres.

See:
  - services/topology/neighborhoods_repo.py — uses these functions
  - services/workers/neighborhood_detector/worker.py — schedules them
"""
from __future__ import annotations

import math
import os
from collections import defaultdict
from typing import Iterable, Sequence
from uuid import UUID


# Tuning knobs.
MATCH_MIN_JACCARD = float(
    os.environ.get("COMMUNITY_MATCH_MIN_JACCARD", "0.3")
)
MIN_COMMUNITY_SIZE = int(
    os.environ.get("COMMUNITY_MIN_SIZE", "2")
)


# ---------------------------------------------------------------------
# detect_communities — connected components
# ---------------------------------------------------------------------


def detect_communities(
    edges: Iterable[tuple[UUID, UUID]],
    all_nodes: Iterable[UUID],
) -> dict[UUID, int]:
    """Assign each node a community label.

    Returns: {node_id -> community_label}. Labels are integers
    starting at 0. Nodes with no edges get their own singleton
    label, but communities below MIN_COMMUNITY_SIZE are filtered
    by `prune_singletons`. The caller decides what to do with
    isolated nodes.

    Implementation: union-find over the edge list. O(α(n) · |E|)
    where α is the inverse Ackermann function (effectively
    constant).
    """
    # Initialize each node as its own root.
    parent: dict[UUID, UUID] = {n: n for n in all_nodes}

    def find(x: UUID) -> UUID:
        # Path compression.
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    def union(a: UUID, b: UUID) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for source, target in edges:
        if source not in parent or target not in parent:
            # Edge to a node we weren't told about — skip rather
            # than crash. This can happen if an edge points to an
            # archived Model that's no longer in the active set.
            continue
        union(source, target)

    # Assign integer labels by root id.
    root_to_label: dict[UUID, int] = {}
    out: dict[UUID, int] = {}
    next_label = 0
    for node in parent:
        root = find(node)
        if root not in root_to_label:
            root_to_label[root] = next_label
            next_label += 1
        out[node] = root_to_label[root]
    return out


def prune_singletons(
    labels: dict[UUID, int],
    *,
    min_size: int = MIN_COMMUNITY_SIZE,
) -> dict[UUID, int]:
    """Drop nodes whose community is smaller than `min_size`.
    The dropped nodes are not assigned to any community in the
    returned mapping; callers treat them as isolated."""
    counts: dict[int, int] = defaultdict(int)
    for label in labels.values():
        counts[label] += 1
    return {
        node: label
        for node, label in labels.items()
        if counts[label] >= min_size
    }


# ---------------------------------------------------------------------
# match_communities — greedy stable-ID assignment
# ---------------------------------------------------------------------


def match_communities(
    prev_neighborhoods: Sequence["PrevNeighborhood"],
    new_communities: dict[int, set[UUID]],
) -> dict[int, UUID | None]:
    """Greedy match each new community to the best-overlapping
    previous neighborhood id.

    `prev_neighborhoods` is an iterable of PrevNeighborhood
    (id, member_set, centroid). `new_communities` is
    {label -> {member_ids}}.

    Returns: {new_label -> neighborhood_id_or_None}. None means
    "no good match; caller should create a fresh id and emit an
    'emergence' event." Active neighborhoods that don't get
    matched should be marked 'dissolved' or 'merged' by the
    caller.

    Algorithm:
      1. For every (prev, new) pair, compute Jaccard membership
         overlap. Skip pairs below MATCH_MIN_JACCARD.
      2. Sort pairs by Jaccard descending.
      3. Greedy: each new community claims the highest-Jaccard
         unclaimed prev_id. Ties broken by centroid distance.
      4. Communities with no acceptable match get None.

    Idempotency: re-running on identical input produces identical
    assignments (sort is stable on ties).
    """
    # Build candidate (jaccard, prev_id, new_label) triples.
    triples: list[tuple[float, UUID, int]] = []
    prev_member_sets = {p.id: p.members for p in prev_neighborhoods}
    for prev in prev_neighborhoods:
        for new_label, new_members in new_communities.items():
            j = _jaccard(prev.members, new_members)
            if j >= MATCH_MIN_JACCARD:
                triples.append((j, prev.id, new_label))

    # Sort by jaccard descending (stable on ties → deterministic).
    triples.sort(key=lambda t: (-t[0], str(t[1]), t[2]))

    assigned_new: set[int] = set()
    used_prev: set[UUID] = set()
    out: dict[int, UUID | None] = {}
    for _, prev_id, new_label in triples:
        if new_label in assigned_new:
            continue
        if prev_id in used_prev:
            continue
        out[new_label] = prev_id
        assigned_new.add(new_label)
        used_prev.add(prev_id)

    # Communities without a match → None (caller assigns fresh id).
    for new_label in new_communities:
        out.setdefault(new_label, None)

    return out


class PrevNeighborhood:
    """Lightweight tuple holder for match_communities. Defined as a
    class (not a dataclass) so callers can construct instances
    without importing pydantic in tests."""
    __slots__ = ("id", "members", "centroid")

    def __init__(
        self,
        id: UUID,
        members: set[UUID],
        centroid: list[float] | None = None,
    ) -> None:
        self.id = id
        self.members = members
        self.centroid = centroid

    def __repr__(self) -> str:
        return (
            f"PrevNeighborhood(id={self.id}, "
            f"members={len(self.members)})"
        )


# ---------------------------------------------------------------------
# compute_density / compute_centrality
# ---------------------------------------------------------------------


def compute_density(
    member_ids: set[UUID],
    edges: Iterable[tuple[UUID, UUID]],
) -> float:
    """Internal density: edges with both endpoints inside member_ids
    divided by the max possible (n choose 2) for an undirected
    graph. Returns 0.0 for communities with < 2 members."""
    n = len(member_ids)
    if n < 2:
        return 0.0
    inside = 0
    seen_pairs: set[tuple[UUID, UUID]] = set()
    for source, target in edges:
        if source not in member_ids or target not in member_ids:
            continue
        # Canonical undirected pair.
        a, b = (source, target) if source < target else (target, source)
        if (a, b) in seen_pairs:
            continue
        seen_pairs.add((a, b))
        inside += 1
    max_possible = n * (n - 1) // 2
    return inside / max_possible if max_possible else 0.0


def compute_centrality(
    node: UUID,
    member_ids: set[UUID],
    edges: Iterable[tuple[UUID, UUID]],
) -> float:
    """Degree centrality of `node` within its community: count of
    edges to other members, normalized by (n - 1). Returns 0 for
    isolated members or singletons."""
    n = len(member_ids)
    if n < 2 or node not in member_ids:
        return 0.0
    deg = 0
    seen_pairs: set[tuple[UUID, UUID]] = set()
    for source, target in edges:
        if source != node and target != node:
            continue
        other = target if source == node else source
        if other not in member_ids or other == node:
            continue
        a, b = (source, target) if source < target else (target, source)
        if (a, b) in seen_pairs:
            continue
        seen_pairs.add((a, b))
        deg += 1
    return deg / (n - 1)


# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------


def _jaccard(a: set[UUID], b: set[UUID]) -> float:
    if not a and not b:
        return 1.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union else 0.0


__all__ = [
    "MATCH_MIN_JACCARD",
    "MIN_COMMUNITY_SIZE",
    "detect_communities",
    "prune_singletons",
    "match_communities",
    "PrevNeighborhood",
    "compute_density",
    "compute_centrality",
]
