"""
services/retrieval/scoring.py — Reciprocal Rank Fusion (RRF) scoring
for retrieval merge.

Source: RETRIEVAL-DESIGN-AUDIT §6 arguments 1-3. Replaces the linear
weighted-sum in `services/retrieval/primary.py::_merge_and_rank_models`.

Why RRF? The prior scheme computed
  score = w_structural * decay(rank_A) + w_semantic * decay(rank_B) + ...
where w_* are hardcoded per-trigger constants and `decay` is a
monotonic rank-position decay. Problems, per the audit:
  1. Dimension-scale mismatch. If one dimension produces dense, tightly
     packed candidates and another produces sparse long-tail ones, the
     linear sum over-weights the dense dimension.
  2. Correlated dimensions double-count. A Model surfaced by BOTH
     pathway A and pathway B gets w_A + w_B added; with correlation
     this over-rewards items that any pipeline would have found.
  3. `trust_tier` + `source_boost` were distinct dimensions but both
     really come from the Model's provenance. The audit recommends
     folding them into one "provenance" dimension.

RRF:
  score(item) = Σ_dim  w_dim / (k + rank_dim(item))
where `rank_dim(item) = ∞` if the item isn't ranked in that dimension
(contribution is then 0). `k` is a smoothing constant, canonically 60.
Larger k flattens the dimension-to-dimension influence; smaller k
amplifies top ranks.

Our dimensions:
  - "structural" — pathway A rank
  - "semantic"   — pathway B rank
  - "temporal"   — pathway C rank
  - "pattern"    — pathway D rank
  - "activation" — Model.activation (rank by activation DESC)
  - "provenance" — the merged trust_tier + source_boost dimension
                   (RETRIEVAL-DESIGN-AUDIT §6 arg 3). Rank by
                   merged trust weight DESC.

Dimension WEIGHTS are applied as RRF multipliers (per the plan §2 RA-3
spec). Defaults chosen to approximate the prior linear-weighting
emphasis:

  structural    1.0
  semantic      0.85
  temporal      0.5
  pattern       0.5
  activation    0.5
  provenance    0.5

A per-trigger override map sits alongside (same concept as the prior
`_TRIGGER_WEIGHTS`) — e.g., T2 downweights "semantic" etc.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping
from uuid import UUID


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------


RRF_K_DEFAULT = 60

# Canonical dimension names. If you add one, update DIMENSION_WEIGHTS.
DIMENSION_STRUCTURAL = "structural"
DIMENSION_SEMANTIC = "semantic"
DIMENSION_TEMPORAL = "temporal"
DIMENSION_PATTERN = "pattern"
DIMENSION_ACTIVATION = "activation"
DIMENSION_PROVENANCE = "provenance"  # merged trust_tier + source_boost

DIMENSION_WEIGHTS: dict[str, float] = {
    DIMENSION_STRUCTURAL: 1.0,
    DIMENSION_SEMANTIC: 0.85,
    DIMENSION_TEMPORAL: 0.5,
    DIMENSION_PATTERN: 0.5,
    DIMENSION_ACTIVATION: 0.5,
    DIMENSION_PROVENANCE: 0.5,
}

# Pathway → dimension mapping. Used by `rankings_from_pathway_results`.
PATHWAY_TO_DIMENSION: dict[str, str] = {
    "A": DIMENSION_STRUCTURAL,
    "B": DIMENSION_SEMANTIC,
    "C": DIMENSION_TEMPORAL,
    "D": DIMENSION_PATTERN,
}


# Trust-tier numeric weights. Higher = more trusted.
# These folded into `provenance` along with any explicit
# `source_boost` attribute on the Model (default 1.0). The merged value
# is what we rank by.
_TRUST_TIER_WEIGHTS: dict[str, float] = {
    "authoritative": 1.0,
    "attested_agent": 0.9,
    "authoritative_external": 0.85,
    "reputable": 0.75,
    "inferential": 0.55,
    "inferential_external": 0.45,
    "unvetted": 0.2,
}


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _as_uuid(v: Any) -> UUID | None:
    if isinstance(v, UUID):
        return v
    try:
        return UUID(str(v))
    except (ValueError, TypeError, AttributeError):
        return None


def compute_rrf_score(
    item: Any,
    rankings_per_dimension: Mapping[str, float],
    *,
    k: int = RRF_K_DEFAULT,
    dimension_weights: Mapping[str, float] | None = None,
) -> float:
    """
    Reciprocal Rank Fusion score for a single item.

    `rankings_per_dimension` maps dimension-name → rank-position
    (1-indexed best). Use `float('inf')` for "item not ranked in this
    dimension" (contribution = 0).

    Applying dimension weights as RRF multipliers:
        score = Σ_dim  w_dim / (k + rank_dim)
    """
    weights = dimension_weights if dimension_weights is not None else DIMENSION_WEIGHTS
    total = 0.0
    for dim, rank in rankings_per_dimension.items():
        if rank is None:
            continue
        r = float(rank)
        if r == float("inf"):
            continue
        w = weights.get(dim, 1.0)
        total += w / (float(k) + r)
    # Return value includes `item` in the name (tests may ignore it).
    # We keep the signature on `item` for API parity with the plan's
    # spec even though `item` is unused here — callers that pre-compute
    # rankings naturally thread the item in as the dict key.
    _ = item  # kept for API clarity
    return total


# ---------------------------------------------------------------------
# Building rankings from pathway results / from Model attributes
# ---------------------------------------------------------------------


def rankings_from_pathway_results(
    pathway_results: Iterable[Any],
    *,
    pathway_to_dimension: Mapping[str, str] | None = None,
) -> dict[UUID, dict[str, float]]:
    """
    Given an iterable of PathwayResult objects (each has
    `source_pathway` and `models` list), build a
    `{model_id: {dimension_name: rank}}` dict.

    If a Model appears in multiple pathways that map to the same
    dimension (shouldn't happen under current pipeline, but is defended
    here), the better (smaller) rank wins.

    Models that a pathway did NOT rank are simply absent from that
    dimension in the returned dict. Callers downstream treat "absent"
    as rank=inf.
    """
    mapping = pathway_to_dimension or PATHWAY_TO_DIMENSION
    rankings: dict[UUID, dict[str, float]] = {}

    for pr in pathway_results:
        pathway = getattr(pr, "source_pathway", None)
        if pathway is None:
            continue
        dim = mapping.get(str(pathway))
        if dim is None:
            continue
        for rank_pos, m in enumerate(getattr(pr, "models", []) or [], start=1):
            mid = getattr(m, "id", None)
            mid_uuid = _as_uuid(mid)
            if mid_uuid is None:
                continue
            bucket = rankings.setdefault(mid_uuid, {})
            prev = bucket.get(dim)
            if prev is None or rank_pos < prev:
                bucket[dim] = float(rank_pos)
    return rankings


def rankings_from_model_attributes(
    items: Iterable[Any],
    *,
    include_activation: bool = True,
    include_provenance: bool = True,
) -> dict[UUID, dict[str, float]]:
    """
    Compute activation-ranking and provenance-ranking (merged
    trust_tier + source_boost, per audit §6 arg 3) over a pool of
    Models. Produces per-dimension ranks alongside the pathway-based
    ones.

    `items` must have `.id`, `.activation` (float), and optionally
    `.trust_tier` / `.source_boost` attributes. Rows that lack these
    fall back to midpoint scores.
    """
    items_list = list(items)
    rankings: dict[UUID, dict[str, float]] = {}

    if include_activation:
        act_sorted = sorted(
            items_list,
            key=lambda m: (-(getattr(m, "activation", 0.0) or 0.0), str(getattr(m, "id", ""))),
        )
        for rank_pos, m in enumerate(act_sorted, start=1):
            mid = _as_uuid(getattr(m, "id", None))
            if mid is None:
                continue
            rankings.setdefault(mid, {})[DIMENSION_ACTIVATION] = float(rank_pos)

    if include_provenance:
        def _prov_weight(m: Any) -> float:
            tier = getattr(m, "trust_tier", None)
            base = _TRUST_TIER_WEIGHTS.get(str(tier), 0.5) if tier is not None else 0.5
            boost = getattr(m, "source_boost", None)
            try:
                boost = float(boost) if boost is not None else 1.0
            except (TypeError, ValueError):
                boost = 1.0
            return base * boost

        prov_sorted = sorted(
            items_list,
            key=lambda m: (-_prov_weight(m), str(getattr(m, "id", ""))),
        )
        for rank_pos, m in enumerate(prov_sorted, start=1):
            mid = _as_uuid(getattr(m, "id", None))
            if mid is None:
                continue
            rankings.setdefault(mid, {})[DIMENSION_PROVENANCE] = float(rank_pos)

    return rankings


def merge_rankings(
    *sources: Mapping[UUID, Mapping[str, float]],
) -> dict[UUID, dict[str, float]]:
    """
    Merge multiple {id: {dim: rank}} dicts into one. Per-(id, dim)
    collisions keep the smaller rank. Callers typically merge
    pathway-derived rankings with attribute-derived ones.
    """
    out: dict[UUID, dict[str, float]] = {}
    for src in sources:
        for mid, dims in src.items():
            bucket = out.setdefault(mid, {})
            for dim, r in dims.items():
                prev = bucket.get(dim)
                if prev is None or float(r) < prev:
                    bucket[dim] = float(r)
    return out


# ---------------------------------------------------------------------
# Top-level merge-and-rank via RRF
# ---------------------------------------------------------------------


@dataclass
class RRFRankResult:
    """
    The output of `merge_and_rank_rrf`: ordered list of items plus the
    per-id score map. Shape mirrors the legacy linear-weighted return
    so callers can drop in RRF with minimal surgery.
    """

    ordered_items: list[Any] = field(default_factory=list)
    scores: dict[UUID, float] = field(default_factory=dict)
    rankings: dict[UUID, dict[str, float]] = field(default_factory=dict)


def merge_and_rank_rrf(
    pathway_results: Iterable[Any],
    *,
    per_trigger_dimension_weights: Mapping[str, float] | None = None,
    include_activation: bool = True,
    include_provenance: bool = True,
    k: int = RRF_K_DEFAULT,
    top_n: int | None = None,
) -> RRFRankResult:
    """
    Drop-in replacement for `primary._merge_and_rank_models` using RRF.

    - Builds pathway-dimension rankings from the passed PathwayResult
      list.
    - Augments with activation + provenance rankings from the union of
      Models surfaced by any pathway.
    - Scores every unique Model via `compute_rrf_score` with
      dimension weights.
    - Returns (ordered, scores). Ties on score break on activation
      DESC then id ASC (same tiebreakers as the legacy function).
    """
    by_id: dict[UUID, Any] = {}
    pathway_list = list(pathway_results)
    for pr in pathway_list:
        for m in getattr(pr, "models", []) or []:
            mid = _as_uuid(getattr(m, "id", None))
            if mid is None:
                continue
            by_id.setdefault(mid, m)

    pathway_rankings = rankings_from_pathway_results(pathway_list)
    attr_rankings = rankings_from_model_attributes(
        by_id.values(),
        include_activation=include_activation,
        include_provenance=include_provenance,
    )
    rankings = merge_rankings(pathway_rankings, attr_rankings)

    scores: dict[UUID, float] = {}
    for mid, dims in rankings.items():
        scores[mid] = compute_rrf_score(
            by_id[mid],
            dims,
            k=k,
            dimension_weights=per_trigger_dimension_weights or DIMENSION_WEIGHTS,
        )

    def _sort_key(mid: UUID) -> tuple[float, float, str]:
        item = by_id[mid]
        return (
            -scores.get(mid, 0.0),
            -(getattr(item, "activation", 0.0) or 0.0),
            str(mid),
        )

    ordered_ids = sorted(scores.keys(), key=_sort_key)
    if top_n is not None and top_n >= 0:
        ordered_ids = ordered_ids[:top_n]
    ordered = [by_id[mid] for mid in ordered_ids]

    return RRFRankResult(
        ordered_items=ordered,
        scores=scores,
        rankings=rankings,
    )


__all__ = [
    "RRF_K_DEFAULT",
    "DIMENSION_STRUCTURAL",
    "DIMENSION_SEMANTIC",
    "DIMENSION_TEMPORAL",
    "DIMENSION_PATTERN",
    "DIMENSION_ACTIVATION",
    "DIMENSION_PROVENANCE",
    "DIMENSION_WEIGHTS",
    "PATHWAY_TO_DIMENSION",
    "compute_rrf_score",
    "rankings_from_pathway_results",
    "rankings_from_model_attributes",
    "merge_rankings",
    "merge_and_rank_rrf",
    "RRFRankResult",
]
