"""Pure IR metric helpers used by Layer 1 sub-evaluators.

All functions are deterministic and side-effect-free. They operate on plain
Python lists to keep them easy to hand-verify in unit tests.
"""

from __future__ import annotations

import math
from collections.abc import Sequence


def recall_at_k(
    retrieved: Sequence[str], relevant: Sequence[str], k: int
) -> float:
    """Fraction of relevant items present in the top-k retrieved items.

    If `relevant` is empty the metric is undefined; we return 0.0 so the caller
    can skip the example or aggregate safely.
    """
    if k <= 0:
        raise ValueError("k must be positive")
    if not relevant:
        return 0.0
    top_k = set(retrieved[:k])
    hits = sum(1 for r in relevant if r in top_k)
    return hits / len(relevant)


def mrr(retrieved: Sequence[str], relevant: Sequence[str]) -> float:
    """Reciprocal rank of the first relevant item in `retrieved` (1-indexed).

    Returns 0.0 if no relevant item appears.
    """
    relevant_set = set(relevant)
    for i, item in enumerate(retrieved, start=1):
        if item in relevant_set:
            return 1.0 / i
    return 0.0


def _dcg(gains: Sequence[float]) -> float:
    # DCG with log2(i+1) discount, 1-indexed.
    return sum(g / math.log2(i + 1) for i, g in enumerate(gains, start=1))


def ndcg_at_k(
    retrieved: Sequence[str],
    relevance: dict[str, float],
    k: int,
) -> float:
    """Normalized DCG at k using binary or graded relevance.

    `relevance` maps item id to a non-negative gain. Items not in the map get
    gain 0. nDCG = DCG(retrieved[:k]) / IDCG(top-k best gains).
    """
    if k <= 0:
        raise ValueError("k must be positive")
    if not relevance:
        return 0.0
    gains = [relevance.get(item, 0.0) for item in retrieved[:k]]
    dcg = _dcg(gains)
    ideal_gains = sorted(relevance.values(), reverse=True)[:k]
    idcg = _dcg(ideal_gains)
    if idcg == 0.0:
        return 0.0
    return dcg / idcg


def kendall_tau(a: Sequence[str], b: Sequence[str]) -> float:
    """Kendall tau-a rank correlation between two orderings of the same items.

    Both sequences must contain the same set of items. Returns a value in
    [-1, 1]: 1.0 for identical ordering, -1.0 for complete reversal, 0.0 for
    no correlation.
    """
    if set(a) != set(b):
        raise ValueError("kendall_tau inputs must contain identical elements")
    n = len(a)
    if n < 2:
        return 1.0
    rank_b = {item: i for i, item in enumerate(b)}
    concordant = 0
    discordant = 0
    for i in range(n):
        for j in range(i + 1, n):
            # a orders a[i] before a[j].
            if rank_b[a[i]] < rank_b[a[j]]:
                concordant += 1
            else:
                discordant += 1
    total_pairs = n * (n - 1) / 2
    return (concordant - discordant) / total_pairs
