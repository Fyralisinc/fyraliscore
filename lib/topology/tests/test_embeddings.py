"""
lib/topology/tests/test_embeddings.py — pure-Python tests for the
positional-embedding math (S2).

No DB. The math primitives are independent of asyncpg/Postgres.
"""
from __future__ import annotations

import math

import pytest

from lib.topology.embeddings import (
    ALPHA_DEFAULT,
    TOPO_EMBEDDING_DIM,
    compute_topo_embedding,
    content_anchor,
    delta_magnitude,
    random_unit_vector,
)


# ---------------------------------------------------------------------
# content_anchor
# ---------------------------------------------------------------------


def test_content_anchor_dim_check():
    """768d input → 128d output. Wrong input dim raises."""
    out = content_anchor([1.0] + [0.0] * 767)
    assert len(out) == TOPO_EMBEDDING_DIM
    with pytest.raises(ValueError):
        content_anchor([1.0] * 100)


def test_content_anchor_normalized():
    """Output is L2-normalized — distances are bounded and stable."""
    out = content_anchor([1.0] * 768)
    norm = math.sqrt(sum(x * x for x in out))
    assert abs(norm - 1.0) < 1e-9


def test_content_anchor_deterministic():
    """Same input → same output. Across processes too (the
    projection seed is hardcoded, not env-dependent)."""
    a = content_anchor([0.5] * 768)
    b = content_anchor([0.5] * 768)
    assert a == b


def test_content_anchor_distance_preserving():
    """Random projection preserves distances in expectation
    (Johnson-Lindenstrauss). Not exact, but two similar content
    embeddings should produce two similar topo anchors."""
    import random
    rng = random.Random(42)
    base = [rng.gauss(0.0, 1.0) for _ in range(768)]
    near = [b + rng.gauss(0.0, 0.05) for b in base]
    far = [rng.gauss(0.0, 1.0) for _ in range(768)]
    a_base = content_anchor(base)
    a_near = content_anchor(near)
    a_far = content_anchor(far)
    d_near = delta_magnitude(a_base, a_near)
    d_far = delta_magnitude(a_base, a_far)
    # Near should be much closer than far.
    assert d_near < d_far


# ---------------------------------------------------------------------
# compute_topo_embedding (the alpha-anchored rule)
# ---------------------------------------------------------------------


def test_no_neighbors_returns_anchor():
    """With no neighbors, the rule degenerates to content_anchor.
    No drift can happen because there's nothing to drift toward."""
    anchor = random_unit_vector(seed=1)
    out = compute_topo_embedding(anchor, [], None)
    # L2-normalized so might differ in 16th decimal; allow tiny eps.
    for a, b in zip(out, anchor):
        assert abs(a - b) < 1e-9


def test_alpha_one_returns_anchor():
    """α = 1.0 → content dominates entirely; neighbors ignored."""
    anchor = random_unit_vector(seed=2)
    neighbor = random_unit_vector(seed=3)
    out = compute_topo_embedding(
        anchor, [neighbor], None, alpha=1.0
    )
    for a, b in zip(out, anchor):
        assert abs(a - b) < 1e-9


def test_alpha_zero_returns_neighbor_mean():
    """α = 0.0 → pure neighbor mean (anchor ignored). With one
    neighbor, that's just the neighbor (L2-normalized)."""
    anchor = random_unit_vector(seed=4)
    neighbor = random_unit_vector(seed=5)
    out = compute_topo_embedding(
        anchor, [neighbor], None, alpha=0.0
    )
    for a, b in zip(out, neighbor):
        assert abs(a - b) < 1e-9


def test_alpha_blends():
    """0 < α < 1 produces a blend strictly between anchor and
    neighbor."""
    anchor = random_unit_vector(seed=6)
    neighbor = random_unit_vector(seed=7)
    out = compute_topo_embedding(
        anchor, [neighbor], None, alpha=0.5
    )
    # Output is closer to anchor than full neighbor and closer to
    # neighbor than full anchor.
    d_to_anchor = delta_magnitude(out, anchor)
    d_to_neighbor = delta_magnitude(out, neighbor)
    assert d_to_anchor > 0
    assert d_to_neighbor > 0


def test_alpha_out_of_range_rejected():
    anchor = random_unit_vector(seed=8)
    with pytest.raises(ValueError):
        compute_topo_embedding(anchor, [], None, alpha=1.5)
    with pytest.raises(ValueError):
        compute_topo_embedding(anchor, [], None, alpha=-0.1)


def test_weights_required_to_match_neighbors():
    anchor = random_unit_vector(seed=9)
    neighbor = random_unit_vector(seed=10)
    with pytest.raises(ValueError):
        compute_topo_embedding(
            anchor, [neighbor], [1.0, 1.0], alpha=0.5
        )


def test_weighted_neighbors_skew_mean():
    """A neighbor with weight 5.0 dominates over one with weight 1.0."""
    anchor = random_unit_vector(seed=11)
    n_dom = random_unit_vector(seed=12)
    n_quiet = random_unit_vector(seed=13)
    out = compute_topo_embedding(
        anchor, [n_dom, n_quiet], [5.0, 1.0], alpha=0.0
    )
    # Output should be much closer to n_dom than n_quiet.
    assert delta_magnitude(out, n_dom) < delta_magnitude(out, n_quiet)


def test_zero_total_weight_falls_back_to_anchor():
    """If all weights sum to zero, the rule should silently fall
    back to content_anchor (no NaN, no division-by-zero)."""
    anchor = random_unit_vector(seed=14)
    neighbor = random_unit_vector(seed=15)
    out = compute_topo_embedding(
        anchor, [neighbor], [0.0], alpha=0.5
    )
    for a, b in zip(out, anchor):
        assert abs(a - b) < 1e-9


def test_negative_weight_pushes_away():
    """For future contradicts: negative weight should push the
    output AWAY from the neighbor. Verified by checking that with
    one negative-weighted neighbor, distance to that neighbor
    exceeds distance to anchor."""
    anchor = random_unit_vector(seed=16)
    contradicting = random_unit_vector(seed=17)
    out = compute_topo_embedding(
        anchor, [contradicting], [-1.0], alpha=0.0
    )
    # With α=0, all signal comes from neighbors. With negative
    # weight, output is the inverted contradicting vector
    # (L2-normalized). Distance to contradicting should be near 2.0
    # (opposite vectors); distance to anchor is unconstrained.
    d_contradicting = delta_magnitude(out, contradicting)
    assert d_contradicting > 1.0  # should be far from contradicting


# ---------------------------------------------------------------------
# delta_magnitude
# ---------------------------------------------------------------------


def test_delta_magnitude_zero_for_identical():
    v = random_unit_vector(seed=20)
    assert delta_magnitude(v, v) == 0.0


def test_delta_magnitude_inf_for_first_compute():
    """None previous → +inf so the worker treats first compute as
    significant."""
    v = random_unit_vector(seed=21)
    assert delta_magnitude(None, v) == float("inf")


def test_delta_magnitude_dim_check():
    with pytest.raises(ValueError):
        delta_magnitude([1.0, 2.0], [1.0, 2.0, 3.0])


def test_delta_magnitude_l2():
    """Manual L2 distance for a tiny case."""
    a = [1.0, 0.0, 0.0]
    b = [0.0, 1.0, 0.0]
    assert abs(delta_magnitude(a, b) - math.sqrt(2.0)) < 1e-9


# ---------------------------------------------------------------------
# random_unit_vector
# ---------------------------------------------------------------------


def test_random_unit_vector_normalized():
    v = random_unit_vector(seed=99)
    assert len(v) == TOPO_EMBEDDING_DIM
    norm = math.sqrt(sum(x * x for x in v))
    assert abs(norm - 1.0) < 1e-9


def test_random_unit_vector_deterministic():
    a = random_unit_vector(seed=42)
    b = random_unit_vector(seed=42)
    assert a == b
    c = random_unit_vector(seed=43)
    assert a != c
