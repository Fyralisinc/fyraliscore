"""Pure metric helpers for Layer 2 belief-correctness evaluation.

All functions here are deterministic, side-effect-free, and unit tested.
They accept plain Python primitives so they can be reused across sub-evaluators.
"""

from __future__ import annotations

from typing import Iterable, Sequence

import numpy as np

# Ordinal health ladder used by customer-health correctness.
# Each adjacent step counts as distance 1.
HEALTH_LADDER: tuple[str, ...] = (
    "healthy",
    "warning",
    "degraded",
    "critical",
    "churned",
)


def state_accuracy(predicted: Sequence[str], actual: Sequence[str]) -> float:
    """Fraction of predictions that exactly match the actual state."""
    if len(predicted) != len(actual):
        raise ValueError(
            f"length mismatch: predicted={len(predicted)} actual={len(actual)}"
        )
    if not predicted:
        return 0.0
    hits = sum(1 for p, a in zip(predicted, actual) if p == a)
    return hits / len(predicted)


def ordinal_distance(a: str, b: str, ladder: Sequence[str] = HEALTH_LADDER) -> int:
    """Absolute ordinal distance between two ladder values."""
    if a not in ladder:
        raise ValueError(f"{a!r} not in ladder {ladder!r}")
    if b not in ladder:
        raise ValueError(f"{b!r} not in ladder {ladder!r}")
    return abs(ladder.index(a) - ladder.index(b))


def mean_ordinal_distance(
    predicted: Sequence[str],
    actual: Sequence[str],
    ladder: Sequence[str] = HEALTH_LADDER,
) -> float:
    """Mean absolute ordinal distance between predicted/actual pairs."""
    if len(predicted) != len(actual):
        raise ValueError("length mismatch")
    if not predicted:
        return 0.0
    dists = [ordinal_distance(p, a, ladder) for p, a in zip(predicted, actual)]
    return sum(dists) / len(dists)


def binary_confusion_counts(
    predicted: Sequence[bool], actual: Sequence[bool]
) -> tuple[int, int, int, int]:
    """Return (tp, fp, tn, fn) for aligned boolean sequences."""
    if len(predicted) != len(actual):
        raise ValueError("length mismatch")
    tp = fp = tn = fn = 0
    for p, a in zip(predicted, actual):
        if p and a:
            tp += 1
        elif p and not a:
            fp += 1
        elif not p and not a:
            tn += 1
        else:
            fn += 1
    return tp, fp, tn, fn


def accuracy_fpr_fnr(
    predicted: Sequence[bool], actual: Sequence[bool]
) -> tuple[float, float, float]:
    """Accuracy, false_positive_rate, false_negative_rate.

    FPR = fp / (fp + tn); FNR = fn / (fn + tp).
    Returns 0.0 for any rate whose denominator is zero (undefined).
    """
    tp, fp, tn, fn = binary_confusion_counts(predicted, actual)
    total = tp + fp + tn + fn
    accuracy = (tp + tn) / total if total else 0.0
    fpr = fp / (fp + tn) if (fp + tn) else 0.0
    fnr = fn / (fn + tp) if (fn + tp) else 0.0
    return accuracy, fpr, fnr


def precision_recall_f1(
    tp: int, fp: int, fn: int
) -> tuple[float, float, float]:
    """Standard P/R/F1 with zero-denominator safety."""
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall)
        else 0.0
    )
    return precision, recall, f1


def false_pattern_rate(true_count: int, false_count: int) -> float:
    """Fraction of reported patterns that did not match any ground truth."""
    total = true_count + false_count
    return false_count / total if total else 0.0


def bootstrap_ci(
    values: Iterable[float],
    *,
    iters: int = 200,
    seed: int = 1729,
    alpha: float = 0.05,
) -> tuple[float, float] | None:
    """Deterministic bootstrap 95% CI on the sample mean.

    Returns None when fewer than two values are supplied, since the CI is
    not meaningful.
    """
    arr = np.asarray(list(values), dtype=float)
    if arr.size < 2:
        return None
    rng = np.random.default_rng(seed)
    n = arr.size
    draws = rng.integers(0, n, size=(iters, n))
    means = arr[draws].mean(axis=1)
    lo = float(np.quantile(means, alpha / 2.0))
    hi = float(np.quantile(means, 1.0 - alpha / 2.0))
    return lo, hi
