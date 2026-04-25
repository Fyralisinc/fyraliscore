"""Unit tests for lsob_evaluator_l2.metrics — hand-computed values."""

from __future__ import annotations

import math

import pytest

from lsob_evaluator_l2 import metrics as M


def test_state_accuracy_perfect():
    assert M.state_accuracy(["a", "b", "c"], ["a", "b", "c"]) == 1.0


def test_state_accuracy_partial():
    assert M.state_accuracy(["a", "b", "c"], ["a", "x", "c"]) == pytest.approx(2 / 3)


def test_state_accuracy_empty_is_zero():
    assert M.state_accuracy([], []) == 0.0


def test_state_accuracy_mismatched_length_raises():
    with pytest.raises(ValueError):
        M.state_accuracy(["a"], ["a", "b"])


def test_ordinal_distance_zero_for_equal():
    assert M.ordinal_distance("healthy", "healthy") == 0


def test_ordinal_distance_one_step():
    assert M.ordinal_distance("healthy", "warning") == 1
    assert M.ordinal_distance("warning", "degraded") == 1


def test_ordinal_distance_endpoints():
    assert M.ordinal_distance("healthy", "churned") == 4


def test_ordinal_distance_symmetry():
    assert M.ordinal_distance("critical", "warning") == M.ordinal_distance(
        "warning", "critical"
    )


def test_ordinal_distance_unknown_raises():
    with pytest.raises(ValueError):
        M.ordinal_distance("nope", "healthy")


def test_mean_ordinal_distance_hand_computed():
    predicted = ["healthy", "warning", "critical"]
    actual = ["warning", "warning", "healthy"]
    # distances: 1, 0, 3 -> mean 4/3
    assert M.mean_ordinal_distance(predicted, actual) == pytest.approx(4 / 3)


def test_binary_confusion_counts_hand():
    p = [True, True, False, False, True]
    a = [True, False, False, True, True]
    tp, fp, tn, fn = M.binary_confusion_counts(p, a)
    assert (tp, fp, tn, fn) == (2, 1, 1, 1)


def test_accuracy_fpr_fnr_hand():
    # 5 samples. tp=2 fp=1 tn=1 fn=1 from above.
    p = [True, True, False, False, True]
    a = [True, False, False, True, True]
    acc, fpr, fnr = M.accuracy_fpr_fnr(p, a)
    assert acc == pytest.approx(3 / 5)
    # fp=1 tn=1 -> fpr 0.5
    assert fpr == pytest.approx(0.5)
    # fn=1 tp=2 -> fnr 1/3
    assert fnr == pytest.approx(1 / 3)


def test_accuracy_fpr_fnr_all_negatives():
    p = [False, False]
    a = [False, False]
    acc, fpr, fnr = M.accuracy_fpr_fnr(p, a)
    assert acc == 1.0 and fpr == 0.0 and fnr == 0.0


def test_precision_recall_f1_hand():
    p, r, f = M.precision_recall_f1(tp=4, fp=1, fn=2)
    assert p == pytest.approx(4 / 5)
    assert r == pytest.approx(4 / 6)
    assert f == pytest.approx(2 * (4 / 5) * (4 / 6) / ((4 / 5) + (4 / 6)))


def test_precision_recall_f1_zero_safe():
    assert M.precision_recall_f1(0, 0, 0) == (0.0, 0.0, 0.0)


def test_false_pattern_rate_hand():
    # 3 true patterns, 1 false -> 0.25
    assert M.false_pattern_rate(3, 1) == 0.25


def test_false_pattern_rate_no_patterns_is_zero():
    assert M.false_pattern_rate(0, 0) == 0.0


def test_bootstrap_ci_determinism():
    vals = [1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0]
    a1 = M.bootstrap_ci(vals, iters=200, seed=42)
    a2 = M.bootstrap_ci(vals, iters=200, seed=42)
    assert a1 == a2


def test_bootstrap_ci_none_for_tiny_sample():
    assert M.bootstrap_ci([0.5]) is None
    assert M.bootstrap_ci([]) is None


def test_bootstrap_ci_bounds_sane():
    # All ones -> CI should be tight around 1.0
    vals = [1.0] * 10
    lo, hi = M.bootstrap_ci(vals, iters=100, seed=7)
    assert lo == 1.0 and hi == 1.0


def test_bootstrap_ci_contains_mean():
    vals = [0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0]
    mean = sum(vals) / len(vals)
    lo, hi = M.bootstrap_ci(vals, iters=500, seed=11)
    # The sample mean should fall inside its own 95% bootstrap CI with
    # overwhelming probability at this sample size.
    assert lo - 1e-6 <= mean <= hi + 1e-6
    assert not math.isnan(lo) and not math.isnan(hi)
