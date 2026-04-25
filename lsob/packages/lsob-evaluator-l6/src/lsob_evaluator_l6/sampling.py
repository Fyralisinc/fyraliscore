"""Uniform sampling used to cap LLM judge calls per run."""

from __future__ import annotations

import random


def sample_uniform(n_total: int, cap: int, seed: int = 0) -> list[int]:
    """Return a sorted list of up to `cap` distinct indices in [0, n_total).

    When `n_total <= cap` we return every index in order. Otherwise we sample
    `cap` indices uniformly without replacement using a seeded RNG so the
    selection is reproducible.
    """
    if n_total <= 0 or cap <= 0:
        return []
    if n_total <= cap:
        return list(range(n_total))
    rng = random.Random(seed)
    return sorted(rng.sample(range(n_total), cap))


__all__ = ["sample_uniform"]
