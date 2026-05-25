"""
services/topology/anchor.py — deterministic content -> topo projection.

This module owns the pure-Python projection from a Model's 768-d
content embedding into the 128-d positional topo space used by the
UMAP map view (db migration 0032's `models.topo_embedding`).

Why this exists
---------------
Models are written with a content embedding (768-d from
nomic-embed-text:v1.5). The map view and any near-neighbor topology
queries (Pathway F) consume the 128-d `topo_embedding`. A model
inserted without `topo_embedding` set is invisible to those
consumers until a sweep backfills it — so we initialize
`topo_embedding` synchronously on insert from the content
embedding, using this deterministic projection.

Why a fixed random projection (and not learned)
-----------------------------------------------
Random projections preserve cosine distance in expectation
(Johnson-Lindenstrauss). A 768->128 projection preserves cosine
similarity within ~10% relative error, which is well within the
tolerance the map and Pathway F need (they care about ordering,
not absolute distance). A learned projection would be more
compact, but every retrain would shift every Model's anchor and
force a full backfill — the fixed seed below buys reproducibility
across deploys.

Changing `_PROJECTION_SEED` requires a full topology backfill
(re-running this function over every active Model's embedding and
writing the result to `models.topo_embedding`).

The projection matrix is built once at import time, lives in
process memory, and is never persisted.

Pure module: no DB dependency, no asyncio, no logging — safe to
import from anywhere in the codebase including tests.
"""
from __future__ import annotations

import math
import random
from typing import Sequence

from lib.shared.types import TOPO_EMBEDDING_DIM


# Source content-embedding dimension. nomic-embed-text:v1.5 -> 768.
# Hardcoded so this module stays dependency-free of the embedder.
_CONTENT_DIM = 768


# Seed for the random-projection matrix. See module docstring for
# the backfill implication of changing this value.
_PROJECTION_SEED = 0xF00DCAFE


# ---------------------------------------------------------------------
# Projection matrix (768 x 128), built once at import time. Each of
# the 128 columns is an L2-normalized 768-vec sampled from a
# Gaussian; the matrix is fully deterministic given _PROJECTION_SEED.
# ---------------------------------------------------------------------


def _build_projection_matrix() -> list[list[float]]:
    rng = random.Random(_PROJECTION_SEED)
    columns: list[list[float]] = []
    for _ in range(TOPO_EMBEDDING_DIM):
        col = [rng.gauss(0.0, 1.0) for _ in range(_CONTENT_DIM)]
        norm = math.sqrt(sum(x * x for x in col))
        if norm > 0:
            col = [x / norm for x in col]
        columns.append(col)
    # Convert to row-major: matrix[i][j] = columns[j][i].
    return [
        [columns[j][i] for j in range(TOPO_EMBEDDING_DIM)]
        for i in range(_CONTENT_DIM)
    ]


_PROJECTION = _build_projection_matrix()


def _l2_normalize(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(x * x for x in vec))
    if norm == 0.0:
        return list(vec)
    return [x / norm for x in vec]


def content_anchor(content_embedding: Sequence[float]) -> list[float]:
    """Project a 768-d content embedding to the 128-d topo space.

    Output is L2-normalized so cosine distances stay bounded.

    Raises ValueError if `content_embedding` has the wrong dim.
    """
    if len(content_embedding) != _CONTENT_DIM:
        raise ValueError(
            f"content_anchor expects {_CONTENT_DIM}-d input, "
            f"got {len(content_embedding)}"
        )
    out = [0.0] * TOPO_EMBEDDING_DIM
    for i, x in enumerate(content_embedding):
        if x == 0.0:
            continue
        row = _PROJECTION[i]
        for j in range(TOPO_EMBEDDING_DIM):
            out[j] += x * row[j]
    return _l2_normalize(out)


__all__ = [
    "TOPO_EMBEDDING_DIM",
    "content_anchor",
]
