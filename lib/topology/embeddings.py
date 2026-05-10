"""
lib/topology/embeddings.py — pure-Python math for the positional
embedding layer (S2, migration 0032).

What this module owns
---------------------

  - `content_anchor(content_embedding) -> topo_embedding`
        Deterministic projection from the 768-d content embedding to
        the 128-d topo space. Used as the gravitational anchor in
        the update rule: even if the neighbor mean drifts wildly,
        the content_anchor pulls every Model back toward its
        intrinsic semantic position.

  - `compute_topo_embedding(content_anchor, neighbor_topos,
                            neighbor_weights, alpha=ALPHA_DEFAULT)
                            -> topo_embedding`
        The alpha-anchored neighbor-mean rule:
            topo(M) = (1 - α) · weighted_mean(neighbor_topos)
                    + α · content_anchor(M)
        With no neighbors, returns content_anchor (the rule
        degenerates to pure content). Returns L2-normalized.

  - `delta_magnitude(prev, new) -> float`
        L2 distance between two topo embeddings. Used by the
        propagation worker to decide whether to enqueue neighbors
        (‖Δ‖ > ε).

  - `random_unit_vector(seed) -> list[float]`
        Deterministic random topo embedding. Used by tests when
        content embedding isn't available.

Why a fixed projection (and not a learned one)
----------------------------------------------
Random projections are distance-preserving in expectation
(Johnson-Lindenstrauss): a 768→128 projection preserves cosine
similarity within ~10% relative error. That's good enough for the
content_anchor's job (pulling Models toward their semantic
neighborhood). A LEARNED projection would be more compact but
would also be unstable across deploys — every retraining would
shift every Model's anchor and force a full topology recompute.
The fixed projection is reproducible, cheap, and "good enough."

The projection matrix is generated at import time from a known
seed, lives in memory, never persisted. If we ever change the
seed, we must regenerate every Model's topo_embedding (one-shot
backfill akin to scripts/backfill_model_edges.py).

Why no learned GNN
------------------
The plan explicitly chose Option A (alpha-anchored neighbor mean)
over Option B (GNN-style learned update) for v1. The simple rule
is interpretable, deterministic given a retrieval log, fast to
compute, and has a single tuning knob (α). GNNs are deferred
until we have a concrete reason to need their additional
expressiveness.

Tunable
-------
  - ALPHA_DEFAULT (env: TOPO_ALPHA, default 0.3) — gravitational
    pull of content over arrangement. Lower = more drift toward
    neighbors; higher = content dominates. The plan's acceptance
    criterion is empirical α-tuning over the soak period.
  - DELTA_EPSILON (env: TOPO_DELTA_EPSILON, default 0.05) —
    threshold to enqueue neighbors. Below this, we treat the
    update as too small to propagate.

See migrations 0032 + V1_PR_PROMPTS.md + the plan file.
"""
from __future__ import annotations

import math
import os
import random
from typing import Sequence

from lib.shared.types import TOPO_EMBEDDING_DIM


# Source content-embedding dimension. nomic-embed-text:v1.5 → 768.
# Hardcoded here rather than imported from lib/embeddings/ollama.py
# to keep this module dependency-free (matters for testing).
_CONTENT_DIM = 768


# Seed for the random-projection matrix. Changing this requires a
# topology backfill — see module docstring.
_PROJECTION_SEED = 0xF00DCAFE


# Tuning knobs (plan section 6.7 in the design discussion).
ALPHA_DEFAULT = float(os.environ.get("TOPO_ALPHA", "0.3"))
DELTA_EPSILON = float(os.environ.get("TOPO_DELTA_EPSILON", "0.05"))
DELTA_TERMINATE_EPSILON = float(
    os.environ.get("TOPO_DELTA_TERMINATE_EPSILON", "0.005")
)
DAMPING_GAMMA = float(os.environ.get("TOPO_DAMPING_GAMMA", "0.5"))


# ---------------------------------------------------------------------
# Random projection matrix (768 × 128), built once at import time.
# Each row of P is an L2-normalized random unit vector; the matrix
# is fully deterministic given _PROJECTION_SEED.
# ---------------------------------------------------------------------


def _build_projection_matrix() -> list[list[float]]:
    """Build the 768×128 projection matrix (rows = source dim,
    columns = target dim). Each entry is a Gaussian sample,
    column-normalized so the projection preserves L2 distances in
    expectation."""
    rng = random.Random(_PROJECTION_SEED)
    # Build column-major then transpose: each of the 128 target
    # columns is a 768-vec sampled and L2-normalized.
    columns: list[list[float]] = []
    for _ in range(TOPO_EMBEDDING_DIM):
        col = [rng.gauss(0.0, 1.0) for _ in range(_CONTENT_DIM)]
        norm = math.sqrt(sum(x * x for x in col))
        if norm > 0:
            col = [x / norm for x in col]
        columns.append(col)
    # Convert to row-major: matrix[i][j] = columns[j][i].
    matrix = [[columns[j][i] for j in range(TOPO_EMBEDDING_DIM)]
              for i in range(_CONTENT_DIM)]
    return matrix


_PROJECTION = _build_projection_matrix()


# ---------------------------------------------------------------------
# content_anchor: 768d → 128d projection
# ---------------------------------------------------------------------


def content_anchor(content_embedding: Sequence[float]) -> list[float]:
    """Project a content embedding (768d) to the topo space (128d).

    Output is L2-normalized so distances are bounded and the update
    rule's vector arithmetic stays stable.

    Raises ValueError if input dim doesn't match _CONTENT_DIM.
    """
    if len(content_embedding) != _CONTENT_DIM:
        raise ValueError(
            f"content_anchor expects {_CONTENT_DIM}-d input, got "
            f"{len(content_embedding)}"
        )
    out = [0.0] * TOPO_EMBEDDING_DIM
    for i, x in enumerate(content_embedding):
        if x == 0.0:
            continue
        row = _PROJECTION[i]
        for j in range(TOPO_EMBEDDING_DIM):
            out[j] += x * row[j]
    return _l2_normalize(out)


# ---------------------------------------------------------------------
# compute_topo_embedding: the alpha-anchored update rule
# ---------------------------------------------------------------------


def compute_topo_embedding(
    content_anchor_vec: Sequence[float],
    neighbor_topos: Sequence[Sequence[float]],
    neighbor_weights: Sequence[float] | None = None,
    *,
    alpha: float = ALPHA_DEFAULT,
) -> list[float]:
    """The core update rule:

        topo(M) = (1 - α) · weighted_mean(neighbor_topos)
                + α · content_anchor

    With no neighbors, returns content_anchor. With α = 1.0, returns
    content_anchor regardless of neighbors. With α = 0.0, returns
    pure neighbor mean.

    `neighbor_weights` parallels `neighbor_topos`. Defaults to
    uniform weights.

    Output is L2-normalized.

    Edge weights from `model_edges` (e.g. `supports.weight`) flow
    naturally into `neighbor_weights`: an edge with weight 0.7
    contributes 0.7× the influence of an edge with weight 1.0.
    Future `contradicts` edges contribute NEGATIVE weight (callers
    pass -|weight| so the contradicting Model's topo pushes this
    Model AWAY rather than toward).
    """
    if not (0.0 <= alpha <= 1.0):
        raise ValueError(f"alpha must be in [0, 1]; got {alpha}")
    anchor = list(content_anchor_vec)
    if len(anchor) != TOPO_EMBEDDING_DIM:
        raise ValueError(
            f"content_anchor must be {TOPO_EMBEDDING_DIM}-d; got "
            f"{len(anchor)}"
        )

    # No neighbors → content_anchor wins entirely.
    if not neighbor_topos:
        return _l2_normalize(anchor)

    # Default uniform weights.
    if neighbor_weights is None:
        neighbor_weights = [1.0] * len(neighbor_topos)
    if len(neighbor_weights) != len(neighbor_topos):
        raise ValueError(
            f"neighbor_weights length {len(neighbor_weights)} != "
            f"neighbor_topos length {len(neighbor_topos)}"
        )

    # Weighted mean. Allow negative weights (for future contradicts).
    total_weight = sum(abs(w) for w in neighbor_weights)
    if total_weight == 0.0:
        # All zero weights → no neighbor signal; content_anchor wins.
        return _l2_normalize(anchor)

    mean = [0.0] * TOPO_EMBEDDING_DIM
    for topo, w in zip(neighbor_topos, neighbor_weights):
        if len(topo) != TOPO_EMBEDDING_DIM:
            raise ValueError(
                f"neighbor topo dim {len(topo)} != "
                f"{TOPO_EMBEDDING_DIM}"
            )
        for j in range(TOPO_EMBEDDING_DIM):
            mean[j] += topo[j] * w
    for j in range(TOPO_EMBEDDING_DIM):
        mean[j] /= total_weight

    # Mix.
    one_minus = 1.0 - alpha
    out = [
        one_minus * mean[j] + alpha * anchor[j]
        for j in range(TOPO_EMBEDDING_DIM)
    ]
    return _l2_normalize(out)


# ---------------------------------------------------------------------
# delta_magnitude: L2 distance between two topo embeddings
# ---------------------------------------------------------------------


def delta_magnitude(
    prev: Sequence[float] | None,
    new: Sequence[float],
) -> float:
    """L2 distance ‖new - prev‖. If `prev` is None, returns +inf so
    the worker treats first-time computes as significant updates."""
    if prev is None:
        return float("inf")
    if len(prev) != len(new):
        raise ValueError(
            f"delta_magnitude: dim mismatch {len(prev)} vs {len(new)}"
        )
    s = 0.0
    for a, b in zip(prev, new):
        d = float(a) - float(b)
        s += d * d
    return math.sqrt(s)


# ---------------------------------------------------------------------
# random_unit_vector: test helper
# ---------------------------------------------------------------------


def random_unit_vector(seed: int) -> list[float]:
    """Deterministic 128-d unit vector. For tests + the topology
    backfill when a Model has no content embedding."""
    rng = random.Random(seed)
    vec = [rng.gauss(0.0, 1.0) for _ in range(TOPO_EMBEDDING_DIM)]
    return _l2_normalize(vec)


# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------


def _l2_normalize(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(x * x for x in vec))
    if norm == 0.0:
        return list(vec)
    return [x / norm for x in vec]


__all__ = [
    "TOPO_EMBEDDING_DIM",
    "ALPHA_DEFAULT",
    "DELTA_EPSILON",
    "DELTA_TERMINATE_EPSILON",
    "DAMPING_GAMMA",
    "content_anchor",
    "compute_topo_embedding",
    "delta_magnitude",
    "random_unit_vector",
]
