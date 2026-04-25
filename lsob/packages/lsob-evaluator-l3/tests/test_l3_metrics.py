"""Hand-checked unit tests for the pure-function calibration metrics."""

from __future__ import annotations

import math

import pytest

from lsob_evaluator_l3.metrics import (
    bin_stats,
    brier_score,
    ece,
    linear_regression,
    sharpness,
    wilson_interval,
)


def test_brier_single_prediction_false_at_07():
    # Single prediction: conf=0.7, outcome=False => (0.7 - 0)^2 = 0.49
    assert math.isclose(brier_score([(0.7, False)]), 0.49, abs_tol=1e-12)


def test_brier_perfect_predictions_are_zero():
    assert brier_score([(1.0, True), (0.0, False)]) == 0.0


def test_brier_mean_of_squared_errors():
    # conf=0.9 out=True  -> 0.01
    # conf=0.4 out=False -> 0.16
    # conf=0.5 out=True  -> 0.25
    # mean = (0.01 + 0.16 + 0.25) / 3
    expected = (0.01 + 0.16 + 0.25) / 3
    got = brier_score([(0.9, True), (0.4, False), (0.5, True)])
    assert math.isclose(got, expected, abs_tol=1e-12)


def test_brier_empty_returns_zero():
    assert brier_score([]) == 0.0


def test_ece_perfectly_calibrated_single_bin_is_zero():
    # 10 preds at conf=0.7; with a single bin, mean_conf=0.7 and acc=0.7 -> gap 0.
    preds = [(0.7, True)] * 7 + [(0.7, False)] * 3
    assert ece(preds, n_bins=1) == pytest.approx(0.0, abs=1e-12)


def test_ece_all_wrong_at_one_bucket():
    # All predictions at conf=0.8, outcome always False: gap=0.8, weighted abs sum = 0.8
    preds = [(0.8, False)] * 10
    assert ece(preds, n_bins=5) == pytest.approx(0.8, abs=1e-12)


def test_ece_equal_frequency_split_hand_value():
    # Two bins by frequency.
    # Low bin (conf 0.1, 0.2, 0.3, 0.4): mean_conf=0.25, 1 true of 4 => acc=0.25 => gap=0
    # High bin (conf 0.6, 0.7, 0.8, 0.9): mean_conf=0.75, outcomes [T,F,T,T] acc=0.75 => gap=0
    preds = [
        (0.1, False),
        (0.2, True),
        (0.3, False),
        (0.4, False),
        (0.6, True),
        (0.7, False),
        (0.8, True),
        (0.9, True),
    ]
    assert ece(preds, n_bins=2, mode="equal_frequency") == pytest.approx(0.0, abs=1e-12)


def test_bin_stats_gap_direction():
    stats = bin_stats([(0.9, False), (0.9, False), (0.9, False)], n_bins=1)
    assert len(stats) == 1
    assert stats[0].mean_confidence == pytest.approx(0.9)
    assert stats[0].empirical_accuracy == pytest.approx(0.0)
    assert stats[0].gap == pytest.approx(0.9)


def test_wilson_interval_zero_trials():
    lo, hi = wilson_interval(0, 0)
    assert (lo, hi) == (0.0, 1.0)


def test_wilson_interval_known_value():
    # 40/100 at 95% confidence: Wilson CI ~ (0.3094, 0.4980).
    lo, hi = wilson_interval(40, 100, confidence=0.95)
    assert lo == pytest.approx(0.3094, abs=2e-3)
    assert hi == pytest.approx(0.4980, abs=2e-3)


def test_wilson_interval_bounds_contain_proportion():
    lo, hi = wilson_interval(7, 10)
    assert 0.0 <= lo <= 0.7 <= hi <= 1.0


def test_wilson_invalid_successes_raises():
    with pytest.raises(ValueError):
        wilson_interval(11, 10)


def test_sharpness_basic_stats():
    rep = sharpness([0.1, 0.2, 0.3, 0.4, 0.5])
    assert rep.mean == pytest.approx(0.3)
    # population variance of 0.1..0.5 spaced by 0.1 is 0.02
    assert rep.variance == pytest.approx(0.02, abs=1e-9)
    assert sum(rep.histogram_counts) == 5
    assert not rep.collapsed_near_half


def test_sharpness_collapsed_flag():
    rep = sharpness([0.5, 0.5, 0.5, 0.5])
    assert rep.collapsed_near_half


def test_linear_regression_perfect_negative_slope():
    slope, intercept, r2 = linear_regression([0, 1, 2, 3], [3, 2, 1, 0])
    assert slope == pytest.approx(-1.0)
    assert intercept == pytest.approx(3.0)
    assert r2 == pytest.approx(1.0)


def test_linear_regression_degenerate_single_point():
    slope, intercept, r2 = linear_regression([1.0], [5.0])
    assert slope == 0.0
    assert intercept == 5.0
    assert r2 == 0.0
