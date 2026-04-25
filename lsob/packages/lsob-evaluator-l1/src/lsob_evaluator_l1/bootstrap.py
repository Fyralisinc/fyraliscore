"""Deterministic bootstrap confidence intervals.

We use numpy's `default_rng(seed)` so results are reproducible across runs.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np


def _bootstrap_ci(
    values: Sequence[float],
    n_iters: int = 200,
    seed: int = 0,
    alpha: float = 0.05,
) -> tuple[float, float]:
    """Return a (lower, upper) 95% percentile-bootstrap CI for the mean.

    - `values`: observed per-query metric values.
    - `n_iters`: number of bootstrap resamples (default 200 — plan default).
    - `seed`: deterministic RNG seed.
    - `alpha`: two-sided tail mass (default 0.05 = 95% CI).

    For empty or single-value inputs the CI collapses to (mean, mean).
    """
    if not values:
        return (0.0, 0.0)
    arr = np.asarray(values, dtype=float)
    if arr.size == 1:
        v = float(arr[0])
        return (v, v)
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, arr.size, size=(n_iters, arr.size))
    samples = arr[idx].mean(axis=1)
    lower = float(np.quantile(samples, alpha / 2))
    upper = float(np.quantile(samples, 1 - alpha / 2))
    return (lower, upper)
