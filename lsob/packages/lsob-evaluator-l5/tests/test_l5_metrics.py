"""Hand-computed unit tests for the linear regression helper."""

from __future__ import annotations

import math

from lsob_evaluator_l5.metrics import linear_regression, mean_median_p90


def test_linear_regression_perfect_positive_line() -> None:
    # y = 2x + 1
    xs = [0.0, 1.0, 2.0, 3.0]
    ys = [1.0, 3.0, 5.0, 7.0]
    slope, r2 = linear_regression(xs, ys)
    assert math.isclose(slope, 2.0, abs_tol=1e-9)
    assert math.isclose(r2, 1.0, abs_tol=1e-9)


def test_linear_regression_perfect_negative_line() -> None:
    # y = -0.5x + 4
    xs = [0.0, 2.0, 4.0, 6.0]
    ys = [4.0, 3.0, 2.0, 1.0]
    slope, r2 = linear_regression(xs, ys)
    assert math.isclose(slope, -0.5, abs_tol=1e-9)
    assert math.isclose(r2, 1.0, abs_tol=1e-9)


def test_linear_regression_noisy_fit_reduces_r2() -> None:
    xs = [0.0, 1.0, 2.0, 3.0]
    ys = [0.0, 1.0, 1.9, 3.1]  # roughly y = x
    slope, r2 = linear_regression(xs, ys)
    assert slope > 0.9 and slope < 1.1
    assert 0.9 < r2 <= 1.0


def test_linear_regression_degenerate_cases() -> None:
    assert linear_regression([], []) == (0.0, 0.0)
    assert linear_regression([1.0], [5.0]) == (0.0, 0.0)
    # Zero variance in x → flat fallback.
    slope, r2 = linear_regression([2.0, 2.0, 2.0], [1.0, 3.0, 5.0])
    assert slope == 0.0
    assert r2 == 0.0


def test_mean_median_p90_basic() -> None:
    mean, median, p90 = mean_median_p90([1.0, 2.0, 3.0, 4.0, 100.0])
    assert math.isclose(mean, 22.0, abs_tol=1e-9)
    assert math.isclose(median, 3.0, abs_tol=1e-9)
    assert p90 >= 4.0  # at least the fourth value


def test_mean_median_p90_empty_is_zero() -> None:
    assert mean_median_p90([]) == (0.0, 0.0, 0.0)
