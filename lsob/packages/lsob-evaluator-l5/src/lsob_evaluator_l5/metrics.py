"""Pure metric helpers for Layer 5 (temporal dynamics).

Everything here is deterministic, numpy-backed, and side-effect-free.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np


def linear_regression(
    x: Sequence[float], y: Sequence[float]
) -> tuple[float, float]:
    """Ordinary least squares fit of y = slope * x + intercept.

    Returns (slope, r_squared). With fewer than two points or when x has zero
    variance, slope falls back to 0.0 and r_squared to 0.0 (degenerate fits
    are conveyed via flat-line coefficients rather than exceptions — Layer 5
    needs to run on tiny corpora without crashing).
    """
    xs = np.asarray(list(x), dtype=float)
    ys = np.asarray(list(y), dtype=float)
    if xs.shape != ys.shape:
        raise ValueError(
            f"length mismatch: x has {xs.size} points, y has {ys.size}"
        )
    if xs.size < 2:
        return 0.0, 0.0
    x_mean = xs.mean()
    y_mean = ys.mean()
    x_var = float(((xs - x_mean) ** 2).sum())
    if x_var == 0.0:
        return 0.0, 0.0
    cov = float(((xs - x_mean) * (ys - y_mean)).sum())
    slope = cov / x_var
    intercept = y_mean - slope * x_mean
    y_pred = slope * xs + intercept
    ss_res = float(((ys - y_pred) ** 2).sum())
    ss_tot = float(((ys - y_mean) ** 2).sum())
    if ss_tot == 0.0:
        # Perfectly flat y; treat as perfect fit if residuals are zero.
        r2 = 1.0 if ss_res == 0.0 else 0.0
    else:
        r2 = 1.0 - ss_res / ss_tot
    return slope, r2


def mean_median_p90(values: Sequence[float]) -> tuple[float, float, float]:
    """Return (mean, median, 90th-percentile) with empty-safe fallbacks."""
    if not values:
        return 0.0, 0.0, 0.0
    arr = np.asarray(list(values), dtype=float)
    return (
        float(arr.mean()),
        float(np.median(arr)),
        float(np.quantile(arr, 0.9)),
    )
