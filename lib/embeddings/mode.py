"""lib/embeddings/mode.py — staged rollout flag for the observation-embedding pipeline.

Every ingested signal historically had its ``content_text`` embedded and the
768-d vector persisted on ``observations.embedding``. Verification showed that
column is never used as a *search target* in production — the only reader is the
T1 retrieval seed (``services/reasoning/retrieval/primary.py``), which already
re-embeds ``content_text`` on demand when the vector is absent
(``pathways.py::_pathway_b_resolve_vector``). We are decommissioning the
per-signal *write* while KEEPING the column (a later, separate migration drops
it, after a bake).

``OBS_EMBEDDING_MODE`` stages that cutover behind a single env flag so the
runtime can progress by flag flip (no redeploy):

  ``eager``   (default) — current behaviour: embed at ingest, seed T1 retrieval
                          from the persisted obs vector.
  ``shadow``            — still embed + persist at ingest (so the stored vector
                          exists) and T1 retrieval still seeds from it, but
                          ADDITIONALLY re-embed ``content_text`` on demand and
                          log the cosine between the stored vector and the
                          re-embed. Pure measurement — retrieval is unchanged —
                          so we can confirm the lazy seed matches before flipping.
  ``cutover``           — stop embedding at ingest (insert ``NULL`` /
                          ``embedding_pending = FALSE``) and seed T1 retrieval by
                          re-embedding ``content_text`` on demand. The column
                          stays in the schema; only the write-path is off.

Note: only the T1 seed (derived from ``observations.embedding``) is governed by
this flag. ``models.embedding`` (the actual ANN search target) and the T2 seed
derived from it are unaffected.
"""
from __future__ import annotations

import os
from enum import Enum

_ENV = "OBS_EMBEDDING_MODE"


class ObsEmbeddingMode(str, Enum):
    EAGER = "eager"
    SHADOW = "shadow"
    CUTOVER = "cutover"


def obs_embedding_mode() -> ObsEmbeddingMode:
    """Resolve the current mode from the environment (defaults to ``eager``)."""
    raw = os.environ.get(_ENV, "eager").strip().lower()
    try:
        return ObsEmbeddingMode(raw)
    except ValueError:
        return ObsEmbeddingMode.EAGER


def write_obs_embeddings() -> bool:
    """True when ingest should compute and persist observation embeddings.

    False under ``cutover`` — new rows are inserted with ``embedding = NULL`` and
    ``embedding_pending = FALSE`` so no backfill worker chases them.
    """
    return obs_embedding_mode() in (ObsEmbeddingMode.EAGER, ObsEmbeddingMode.SHADOW)


def seed_from_stored_obs_vector() -> bool:
    """True when T1 retrieval seeds pathway B from the persisted obs vector.

    False under ``cutover``, where the seed is re-embedded from ``content_text``.
    """
    return obs_embedding_mode() in (ObsEmbeddingMode.EAGER, ObsEmbeddingMode.SHADOW)


def shadow_compare_seed() -> bool:
    """True only under ``shadow``: measure re-embed vs stored without changing reads."""
    return obs_embedding_mode() is ObsEmbeddingMode.SHADOW


__all__ = [
    "ObsEmbeddingMode",
    "obs_embedding_mode",
    "write_obs_embeddings",
    "seed_from_stored_obs_vector",
    "shadow_compare_seed",
]
