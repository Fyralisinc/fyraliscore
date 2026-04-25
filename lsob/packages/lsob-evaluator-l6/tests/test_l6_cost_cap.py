"""The judge call cap uniformly samples when triggers exceed the cap."""

from __future__ import annotations

from lsob_evaluator_l6.sampling import sample_uniform


def test_sample_respects_cap_and_is_uniform():
    total = 1000
    cap = 100
    indices = sample_uniform(n_total=total, cap=cap, seed=42)
    assert len(indices) == cap
    assert len(set(indices)) == cap
    assert all(0 <= i < total for i in indices)
    # Distribution over four quartiles: with uniform sampling we expect roughly
    # 25 indices per quartile; allow ±10 absolute slack.
    quartiles = [0, 0, 0, 0]
    for i in indices:
        quartiles[min(i * 4 // total, 3)] += 1
    for q in quartiles:
        assert 15 <= q <= 35, quartiles


def test_sample_when_under_cap_returns_all():
    indices = sample_uniform(n_total=10, cap=500, seed=0)
    assert indices == list(range(10))


def test_sample_is_deterministic_for_seed():
    a = sample_uniform(n_total=1000, cap=50, seed=7)
    b = sample_uniform(n_total=1000, cap=50, seed=7)
    assert a == b


def test_sample_handles_edge_cases():
    assert sample_uniform(n_total=0, cap=100) == []
    assert sample_uniform(n_total=100, cap=0) == []
