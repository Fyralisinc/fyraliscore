"""Unit tests for pure metric helpers + bootstrap.

Every example is computed by hand in the docstring so future readers can
re-verify without running code.
"""

from __future__ import annotations

import math

import pytest

from lsob_evaluator_l1.bootstrap import _bootstrap_ci
from lsob_evaluator_l1.metrics import kendall_tau, mrr, ndcg_at_k, recall_at_k


class TestRecallAtK:
    def test_perfect_recall(self):
        # Two relevant items, both in top 3.
        assert recall_at_k(["a", "b", "c"], ["a", "b"], 3) == 1.0

    def test_partial_recall(self):
        # Relevant = {a,b,d}. Top-3 = {a,b,c}. Hits = 2 / 3.
        assert recall_at_k(["a", "b", "c", "d"], ["a", "b", "d"], 3) == pytest.approx(
            2 / 3
        )

    def test_empty_relevant_returns_zero(self):
        assert recall_at_k(["a", "b"], [], 5) == 0.0

    def test_k_truncates(self):
        # Relevant d is only at rank 4 -> excluded at k=3.
        assert recall_at_k(["a", "b", "c", "d"], ["d"], 3) == 0.0

    def test_invalid_k(self):
        with pytest.raises(ValueError):
            recall_at_k(["a"], ["a"], 0)


class TestMRR:
    def test_first_relevant_at_rank_1(self):
        assert mrr(["a", "b", "c"], ["a"]) == 1.0

    def test_first_relevant_at_rank_3(self):
        # 1/3.
        assert mrr(["x", "y", "a", "b"], ["a"]) == pytest.approx(1 / 3)

    def test_no_relevant_returns_zero(self):
        assert mrr(["x", "y"], ["a"]) == 0.0

    def test_multiple_relevant_uses_first(self):
        # Relevant set {b, c}; b appears at rank 2 -> 1/2.
        assert mrr(["a", "b", "c"], ["b", "c"]) == 0.5


class TestNDCG:
    def test_perfect_ranking(self):
        # All gains 1; any non-zero retrieval -> DCG == IDCG -> 1.0.
        rel = {"a": 1.0, "b": 1.0}
        assert ndcg_at_k(["a", "b"], rel, 2) == pytest.approx(1.0)

    def test_hand_computed_graded(self):
        # relevance = {a:3, b:2, c:1}. Retrieved order: [b, a, c].
        # DCG = 2/log2(2) + 3/log2(3) + 1/log2(4) = 2 + 3/log2(3) + 0.5.
        # IDCG = 3/log2(2) + 2/log2(3) + 1/log2(4) = 3 + 2/log2(3) + 0.5.
        rel = {"a": 3.0, "b": 2.0, "c": 1.0}
        dcg = 2.0 + 3.0 / math.log2(3) + 0.5
        idcg = 3.0 + 2.0 / math.log2(3) + 0.5
        assert ndcg_at_k(["b", "a", "c"], rel, 3) == pytest.approx(dcg / idcg)

    def test_empty_relevance(self):
        assert ndcg_at_k(["a"], {}, 5) == 0.0

    def test_no_hits_is_zero(self):
        rel = {"a": 1.0}
        assert ndcg_at_k(["x", "y"], rel, 5) == 0.0


class TestKendallTau:
    def test_identical_is_one(self):
        assert kendall_tau(["a", "b", "c"], ["a", "b", "c"]) == 1.0

    def test_reversed_is_minus_one(self):
        assert kendall_tau(["a", "b", "c"], ["c", "b", "a"]) == -1.0

    def test_single_swap(self):
        # Swap one adjacent pair: a,b,c vs b,a,c.
        # Pairs (a,b): discordant. (a,c): concordant. (b,c): concordant.
        # tau = (2 - 1) / 3 = 1/3.
        assert kendall_tau(["a", "b", "c"], ["b", "a", "c"]) == pytest.approx(1 / 3)

    def test_size_mismatch_raises(self):
        with pytest.raises(ValueError):
            kendall_tau(["a", "b"], ["a", "c"])

    def test_singleton(self):
        assert kendall_tau(["a"], ["a"]) == 1.0


class TestBootstrapCI:
    def test_determinism(self):
        values = [0.1, 0.5, 0.9, 0.4, 0.6]
        ci1 = _bootstrap_ci(values, n_iters=200, seed=0)
        ci2 = _bootstrap_ci(values, n_iters=200, seed=0)
        assert ci1 == ci2

    def test_monotone_bounds(self):
        lo, hi = _bootstrap_ci([0.0, 1.0, 0.5, 0.5, 0.5], n_iters=200, seed=1)
        assert 0.0 <= lo <= hi <= 1.0

    def test_single_value_collapses(self):
        assert _bootstrap_ci([0.42]) == (0.42, 0.42)

    def test_empty(self):
        assert _bootstrap_ci([]) == (0.0, 0.0)
