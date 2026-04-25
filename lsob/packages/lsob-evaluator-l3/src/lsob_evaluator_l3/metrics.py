"""Pure-function calibration metrics for Layer 3.

Every helper is deterministic and side-effect free so tests can hit hand-computed values directly.

A *prediction* in this module is represented as a 2-tuple ``(confidence, outcome)`` where
``confidence`` is a float in [0, 1] and ``outcome`` is a boolean (True = the asserted proposition
resolved true).
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

import numpy as np

Prediction = tuple[float, bool]


def _as_arrays(predictions: Iterable[Prediction]) -> tuple[np.ndarray, np.ndarray]:
    confs: list[float] = []
    outs: list[int] = []
    for conf, outcome in predictions:
        confs.append(float(conf))
        outs.append(1 if bool(outcome) else 0)
    if not confs:
        return np.zeros(0, dtype=float), np.zeros(0, dtype=int)
    return np.asarray(confs, dtype=float), np.asarray(outs, dtype=int)


def brier_score(predictions: Iterable[Prediction]) -> float:
    """Mean squared error between asserted confidence and realized outcome.

    Returns 0.0 for empty inputs (caller should filter first if they prefer NaN).
    """
    confs, outs = _as_arrays(predictions)
    if confs.size == 0:
        return 0.0
    return float(np.mean((confs - outs) ** 2))


@dataclass(frozen=True)
class BinStat:
    lower: float
    upper: float
    count: int
    mean_confidence: float
    empirical_accuracy: float

    @property
    def gap(self) -> float:
        return self.mean_confidence - self.empirical_accuracy


def _equal_frequency_bins(confs: np.ndarray, n_bins: int) -> list[np.ndarray]:
    """Return a list of index-arrays, one per bin, splitting sorted confidences into near-equal groups."""
    n = confs.size
    if n == 0:
        return []
    order = np.argsort(confs, kind="stable")
    # np.array_split handles non-divisible sizes by making the first (n % n_bins) bins one bigger.
    return [idx for idx in np.array_split(order, min(n_bins, n)) if idx.size > 0]


def _equal_width_bins(confs: np.ndarray, n_bins: int) -> list[np.ndarray]:
    if confs.size == 0:
        return []
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    groups: list[np.ndarray] = []
    for i in range(n_bins):
        lo, hi = edges[i], edges[i + 1]
        if i == n_bins - 1:
            mask = (confs >= lo) & (confs <= hi)
        else:
            mask = (confs >= lo) & (confs < hi)
        idx = np.where(mask)[0]
        if idx.size > 0:
            groups.append(idx)
    return groups


def bin_stats(
    predictions: Iterable[Prediction],
    n_bins: int = 10,
    mode: Literal["equal_frequency", "equal_width"] = "equal_frequency",
) -> list[BinStat]:
    confs, outs = _as_arrays(predictions)
    if confs.size == 0:
        return []
    if mode == "equal_frequency":
        groups = _equal_frequency_bins(confs, n_bins)
    elif mode == "equal_width":
        groups = _equal_width_bins(confs, n_bins)
    else:  # pragma: no cover - defensive
        raise ValueError(f"unknown mode: {mode}")
    stats: list[BinStat] = []
    for idx in groups:
        bconf = confs[idx]
        bout = outs[idx]
        stats.append(
            BinStat(
                lower=float(bconf.min()),
                upper=float(bconf.max()),
                count=int(idx.size),
                mean_confidence=float(bconf.mean()),
                empirical_accuracy=float(bout.mean()),
            )
        )
    return stats


def ece(
    predictions: Iterable[Prediction],
    n_bins: int = 10,
    mode: Literal["equal_frequency", "equal_width"] = "equal_frequency",
) -> float:
    """Expected Calibration Error.

    Weighted mean of |mean_confidence - empirical_accuracy| across bins.
    Returns 0.0 for empty inputs.
    """
    preds = list(predictions)
    if not preds:
        return 0.0
    stats = bin_stats(preds, n_bins=n_bins, mode=mode)
    total = sum(s.count for s in stats)
    if total == 0:
        return 0.0
    return float(sum(s.count * abs(s.gap) for s in stats) / total)


def wilson_interval(
    successes: int,
    trials: int,
    confidence: float = 0.95,
) -> tuple[float, float]:
    """Wilson score confidence interval for a binomial proportion.

    Returns (lower, upper). For trials == 0, returns (0.0, 1.0).
    """
    if trials <= 0:
        return (0.0, 1.0)
    if successes < 0 or successes > trials:
        raise ValueError("successes must be in [0, trials]")
    # two-sided z from the normal distribution
    # inverse CDF: avoid scipy dependency -- precompute for 95% (1.959964) else approximate
    alpha = 1.0 - confidence
    # rational approximation of inverse normal CDF (Beasley-Springer-Moro)
    z = _inv_norm_cdf(1.0 - alpha / 2.0)
    p = successes / trials
    denom = 1.0 + (z * z) / trials
    center = (p + (z * z) / (2.0 * trials)) / denom
    half = (z * math.sqrt((p * (1.0 - p) + (z * z) / (4.0 * trials)) / trials)) / denom
    return (max(0.0, center - half), min(1.0, center + half))


def _inv_norm_cdf(p: float) -> float:
    """Beasley-Springer-Moro approximation of the inverse standard normal CDF."""
    if not 0.0 < p < 1.0:
        raise ValueError("p must be in (0, 1)")
    # Coefficients
    a = [
        -3.969683028665376e01,
        2.209460984245205e02,
        -2.759285104469687e02,
        1.383577518672690e02,
        -3.066479806614716e01,
        2.506628277459239e00,
    ]
    b = [
        -5.447609879822406e01,
        1.615858368580409e02,
        -1.556989798598866e02,
        6.680131188771972e01,
        -1.328068155288572e01,
    ]
    c = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e00,
        -2.549732539343734e00,
        4.374664141464968e00,
        2.938163982698783e00,
    ]
    d = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e00,
        3.754408661907416e00,
    ]
    p_low = 0.02425
    p_high = 1.0 - p_low
    if p < p_low:
        q = math.sqrt(-2.0 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0
        )
    if p <= p_high:
        q = p - 0.5
        r = q * q
        return (
            (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5])
            * q
            / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
        )
    q = math.sqrt(-2.0 * math.log(1.0 - p))
    return -(
        ((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]
    ) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)


@dataclass(frozen=True)
class SharpnessReport:
    mean: float
    variance: float
    histogram_counts: tuple[int, ...]
    histogram_edges: tuple[float, ...]
    collapsed_near_half: bool


def sharpness(
    confidences: Sequence[float],
    n_bins: int = 10,
    collapse_tolerance: float = 0.05,
) -> SharpnessReport:
    """Descriptive statistics over the asserted confidences.

    ``collapsed_near_half`` flags when variance is small AND mean is within tolerance of 0.5.
    """
    arr = np.asarray([float(c) for c in confidences], dtype=float)
    if arr.size == 0:
        return SharpnessReport(
            mean=0.0,
            variance=0.0,
            histogram_counts=tuple([0] * n_bins),
            histogram_edges=tuple(np.linspace(0.0, 1.0, n_bins + 1).tolist()),
            collapsed_near_half=False,
        )
    mean = float(arr.mean())
    # Population variance (ddof=0) so a single point gives 0 rather than NaN.
    variance = float(arr.var(ddof=0))
    counts, edges = np.histogram(arr, bins=n_bins, range=(0.0, 1.0))
    collapsed = variance < 0.01 and abs(mean - 0.5) < collapse_tolerance
    return SharpnessReport(
        mean=mean,
        variance=variance,
        histogram_counts=tuple(int(c) for c in counts),
        histogram_edges=tuple(float(e) for e in edges),
        collapsed_near_half=collapsed,
    )


def linear_regression(
    xs: Sequence[float], ys: Sequence[float]
) -> tuple[float, float, float]:
    """Return (slope, intercept, r_squared) using ordinary least squares.

    Returns (0.0, ys_mean, 0.0) for degenerate inputs (fewer than 2 points or zero variance in x).
    """
    x = np.asarray(list(xs), dtype=float)
    y = np.asarray(list(ys), dtype=float)
    if x.size < 2:
        return (0.0, float(y.mean()) if y.size else 0.0, 0.0)
    x_mean = float(x.mean())
    y_mean = float(y.mean())
    x_var = float(np.sum((x - x_mean) ** 2))
    if x_var == 0.0:
        return (0.0, y_mean, 0.0)
    slope = float(np.sum((x - x_mean) * (y - y_mean)) / x_var)
    intercept = y_mean - slope * x_mean
    ss_tot = float(np.sum((y - y_mean) ** 2))
    if ss_tot == 0.0:
        # All y identical; perfect fit iff slope is also zero.
        return (slope, intercept, 1.0 if slope == 0.0 else 0.0)
    ss_res = float(np.sum((y - (slope * x + intercept)) ** 2))
    r2 = 1.0 - ss_res / ss_tot
    return (slope, intercept, r2)
