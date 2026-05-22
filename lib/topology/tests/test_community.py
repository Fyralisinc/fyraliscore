"""
lib/topology/tests/test_community.py — pure-Python tests for the
community-detection / matching / density / centrality math (S2).
"""
from __future__ import annotations

from uuid import UUID

from lib.shared.ids import uuid7
from lib.topology.community import (
    PrevNeighborhood,
    compute_centrality,
    compute_density,
    detect_communities,
    match_communities,
    prune_singletons,
)


# ---------------------------------------------------------------------
# detect_communities — connected components
# ---------------------------------------------------------------------


def test_detect_isolated_nodes_each_get_own_label():
    a, b, c = uuid7(), uuid7(), uuid7()
    labels = detect_communities([], [a, b, c])
    # Each node is its own component; three distinct labels.
    assert len(set(labels.values())) == 3


def test_detect_two_connected_nodes_share_label():
    a, b, c = uuid7(), uuid7(), uuid7()
    labels = detect_communities([(a, b)], [a, b, c])
    assert labels[a] == labels[b]
    assert labels[c] != labels[a]


def test_detect_chain_collapses_to_one_component():
    """A → B → C → D forms one connected component."""
    a, b, c, d = uuid7(), uuid7(), uuid7(), uuid7()
    labels = detect_communities([(a, b), (b, c), (c, d)], [a, b, c, d])
    assert labels[a] == labels[b] == labels[c] == labels[d]


def test_detect_two_clusters_separated():
    """Two disjoint pairs → two components."""
    a, b, c, d = uuid7(), uuid7(), uuid7(), uuid7()
    labels = detect_communities([(a, b), (c, d)], [a, b, c, d])
    assert labels[a] == labels[b]
    assert labels[c] == labels[d]
    assert labels[a] != labels[c]


def test_detect_skips_edges_to_unknown_nodes():
    """An edge to a node not in all_nodes is silently skipped
    (e.g. an archived Model). Doesn't crash."""
    a, b = uuid7(), uuid7()
    ghost = uuid7()
    labels = detect_communities([(a, ghost)], [a, b])
    # a and b are isolated; ghost is ignored.
    assert a in labels
    assert b in labels
    assert ghost not in labels


def test_detect_deterministic():
    """Same input → same labels (modulo label naming, but we hash
    by component membership so the partition is identical)."""
    a, b, c = uuid7(), uuid7(), uuid7()
    edges = [(a, b)]
    nodes = [a, b, c]
    labels1 = detect_communities(edges, nodes)
    labels2 = detect_communities(edges, nodes)
    # Convert to sets-of-frozenset partition for comparison.
    p1 = _partition(labels1)
    p2 = _partition(labels2)
    assert p1 == p2


# ---------------------------------------------------------------------
# prune_singletons
# ---------------------------------------------------------------------


def test_prune_drops_below_min_size():
    a, b, c = uuid7(), uuid7(), uuid7()
    labels = {a: 0, b: 0, c: 1}  # community 1 is singleton
    pruned = prune_singletons(labels, min_size=2)
    assert a in pruned and b in pruned
    assert c not in pruned


def test_prune_keeps_larger_communities():
    a, b, c, d = uuid7(), uuid7(), uuid7(), uuid7()
    labels = {a: 0, b: 0, c: 0, d: 1}
    pruned = prune_singletons(labels, min_size=2)
    assert pruned[a] == 0 and pruned[b] == 0 and pruned[c] == 0
    assert d not in pruned


# ---------------------------------------------------------------------
# match_communities — stable IDs across recomputes
# ---------------------------------------------------------------------


def test_match_high_overlap_inherits_id():
    """A new community with 100% member overlap to a previous
    neighborhood inherits the previous id."""
    nb_id = uuid7()
    a, b, c = uuid7(), uuid7(), uuid7()
    prev = [PrevNeighborhood(id=nb_id, members={a, b, c})]
    new = {0: {a, b, c}}
    matches = match_communities(prev, new)
    assert matches[0] == nb_id


def test_match_no_overlap_returns_none():
    """A new community with NO overlap to any previous neighborhood
    gets None — caller assigns a fresh id."""
    nb_id = uuid7()
    a, b, c, d = uuid7(), uuid7(), uuid7(), uuid7()
    prev = [PrevNeighborhood(id=nb_id, members={a, b})]
    new = {0: {c, d}}
    matches = match_communities(prev, new)
    assert matches[0] is None


def test_match_partial_overlap_above_threshold_inherits():
    """66% Jaccard overlap (above default threshold 0.3) inherits."""
    nb_id = uuid7()
    a, b, c = uuid7(), uuid7(), uuid7()
    prev = [PrevNeighborhood(id=nb_id, members={a, b, c})]
    # New community shares 2/3 members; jaccard = 2/4 = 0.5
    extra = uuid7()
    new = {0: {a, b, extra}}
    matches = match_communities(prev, new)
    assert matches[0] == nb_id


def test_match_below_threshold_returns_none():
    """<30% Jaccard overlap → no match."""
    nb_id = uuid7()
    a, b, c, d = uuid7(), uuid7(), uuid7(), uuid7()
    prev = [PrevNeighborhood(id=nb_id, members={a, b, c, d})]
    # 1/4 jaccard = 0.25 < 0.3
    new = {0: {a, uuid7(), uuid7(), uuid7()}}
    matches = match_communities(prev, new)
    assert matches[0] is None


def test_match_greedy_no_double_assignment():
    """Each previous neighborhood id is claimed by at most one new
    community; ties broken deterministically."""
    nb1, nb2 = uuid7(), uuid7()
    a, b = uuid7(), uuid7()
    prev = [
        PrevNeighborhood(id=nb1, members={a, b}),
        PrevNeighborhood(id=nb2, members={a, b}),
    ]
    new = {0: {a, b}, 1: {a, b}}
    matches = match_communities(prev, new)
    # Both new communities want both prev ids; greedy assigns one
    # each (in some deterministic order).
    assigned = [v for v in matches.values() if v is not None]
    assert sorted(assigned) == sorted([nb1, nb2])


# ---------------------------------------------------------------------
# compute_density
# ---------------------------------------------------------------------


def test_density_singleton_zero():
    a = uuid7()
    assert compute_density({a}, []) == 0.0


def test_density_complete_graph_one():
    """3 nodes, all edges present → density 1.0."""
    a, b, c = uuid7(), uuid7(), uuid7()
    edges = [(a, b), (b, c), (a, c)]
    assert compute_density({a, b, c}, edges) == 1.0


def test_density_partial_graph():
    """3 nodes, 1 edge → density = 1/3."""
    a, b, c = uuid7(), uuid7(), uuid7()
    edges = [(a, b)]
    d = compute_density({a, b, c}, edges)
    assert abs(d - 1.0 / 3.0) < 1e-9


def test_density_ignores_external_edges():
    """An edge with one endpoint outside member set doesn't count."""
    a, b, c = uuid7(), uuid7(), uuid7()
    outsider = uuid7()
    edges = [(a, b), (a, outsider)]
    d = compute_density({a, b, c}, edges)
    assert abs(d - 1.0 / 3.0) < 1e-9


def test_density_dedups_directed_pairs():
    """Edges are undirected for density purposes."""
    a, b = uuid7(), uuid7()
    edges = [(a, b), (b, a)]
    assert compute_density({a, b}, edges) == 1.0


# ---------------------------------------------------------------------
# compute_centrality
# ---------------------------------------------------------------------


def test_centrality_isolated_zero():
    a = uuid7()
    assert compute_centrality(a, {a}, []) == 0.0


def test_centrality_full_connection():
    """A is connected to both B and C in a 3-member community →
    centrality = 2 / (3-1) = 1.0."""
    a, b, c = uuid7(), uuid7(), uuid7()
    edges = [(a, b), (a, c)]
    assert compute_centrality(a, {a, b, c}, edges) == 1.0


def test_centrality_partial():
    """A connected to 1 of 2 others in a 3-community → 0.5."""
    a, b, c = uuid7(), uuid7(), uuid7()
    edges = [(a, b)]
    assert compute_centrality(a, {a, b, c}, edges) == 0.5


def test_centrality_node_not_in_members():
    """If the node isn't in the community, centrality is 0."""
    a, b, c = uuid7(), uuid7(), uuid7()
    outsider = uuid7()
    edges = [(a, b), (a, c)]
    assert compute_centrality(outsider, {a, b, c}, edges) == 0.0


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _partition(labels: dict[UUID, int]) -> set[frozenset[UUID]]:
    """Convert label assignment to a set of member sets — gives an
    invariant comparison ignoring label naming."""
    groups: dict[int, set[UUID]] = {}
    for node, label in labels.items():
        groups.setdefault(label, set()).add(node)
    return {frozenset(g) for g in groups.values()}
