"""
services/retrieval/primary.py — `primary_retrieve` + TriggerContext +
RetrievalResult.

Spec reference: ARCHITECTURE-FINAL.md §8 "Primary pathway resolver",
BUILD-PLAN §4 Prompt 3.A item 2.

Per-trigger pathway mix (source of truth: `_TRIGGER_WEIGHTS` below):
  - T1 (new signal)          : A + B + C, weights 0.4 / 0.4 / 0.2
  - T2 (prediction due)      : A + B + D, weights 0.4 / 0.4 / 0.2
  - T3 (anomaly)             : A + B + C, weights 0.5 / 0.3 / 0.2
  - T4 (background / pattern): D + A,     weights 0.6 / 0.4

Ranking: each item (Model, Observation, etc.) is scored with
`pathway_weight * position_decay(position)`. The same Model surfacing
in multiple pathways sums its weights. Returned sorted by that score,
capped at `top_n` (default 80 Models).

Reconsolidation: the returned Models are passed to
`ModelsRepo.retrieve(ids, conn=conn)` which bumps activation by 0.15
(clipped to 1.0), increments retrieval_count, and sets
last_retrieved_at = now(). Confidence is NOT touched. The call happens
inside the CALLER's transaction (we never open our own transaction
here — Think opens one for its whole run and we live inside it).

Deviation (a) [documented in BUILD-LOG]: the merge/de-dup/score
function lives here as a private helper, not on PathwayResult, because
scoring is trigger-dependent. PathwayResult itself is trigger-agnostic.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal
from uuid import UUID

import asyncpg

from lib.embeddings.ollama import OllamaClient
from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.types import (
    CommitmentRow,
    DecisionRow,
    GoalRow,
    ModelRow,
    ObservationRow,
    ResourceRow,
)

from services.models.repo import ModelsRepo

from .config import CONFIG, RetrievalConfig
from .pathways import (
    PathwayResult,
    RetrievalPathwayError,
    pathway_a_structural,
    pathway_b_semantic,
    pathway_c_temporal,
    pathway_d_pattern,
)
from .scoring import merge_and_rank_rrf


TriggerKind = Literal["T1", "T2", "T3", "T4"]

# Per-spec-§8 weighting mix. These are the fixed weights from
# BUILD-PLAN §4 Prompt 3.A. If callers want different weights they
# can call the individual pathways directly.
_TRIGGER_WEIGHTS: dict[TriggerKind, dict[str, float]] = {
    "T1": {"A": 0.4, "B": 0.4, "C": 0.2},
    "T2": {"A": 0.4, "B": 0.4, "D": 0.2},
    "T3": {"A": 0.5, "B": 0.3, "C": 0.2},
    "T4": {"D": 0.6, "A": 0.4},
}


_DEFAULT_TOP_N = 80


class RetrievalError(CompanyOSError):
    default_code = "retrieval_error"


@dataclass
class TriggerContext:
    """
    The common trigger payload passed into `primary_retrieve`. Each
    trigger kind uses a subset of the fields:

      T1: observation_id, seed_entity_ids, seed_natural_text,
          seed_occurred_at, scope_actors
      T2: model_id (the prediction whose evaluate_at is due)
      T3: region_spec (anomaly region descriptor); typically carries
          seed_entity_ids + seed_natural_text under the hood (populated
          by the Anomaly processor's enqueue path)
      T4: subkind, seed_signature (from a Precipitation proposal)
    """

    kind: TriggerKind
    tenant_id: UUID

    # T1
    observation_id: UUID | None = None
    seed_entity_ids: list[dict[str, Any]] = field(default_factory=list)
    seed_natural_text: str | None = None
    seed_occurred_at: datetime | None = None
    scope_actors: list[UUID] = field(default_factory=list)

    # T2
    model_id: UUID | None = None

    # T3
    region_spec: dict[str, Any] | None = None

    # T4
    subkind: str | None = None
    seed_signature: dict[str, Any] | None = None

    # Pre-computed embedding (optional; tests pass one to skip Ollama)
    precomputed_seed_vector: list[float] | None = None

    # Hop cap for pathway A (2 is the spec default)
    max_hops: int = 2

    # Temporal window for pathway C
    temporal_window: timedelta = timedelta(days=7)

    # Pathway B k
    semantic_k: int = 40


@dataclass
class RetrievalResult:
    """
    The merged + scored output of `primary_retrieve`.

    `pathway_results` retains the raw per-pathway return so the caller
    can inspect which pathway surfaced which item (used by assembler
    for compression tie-breaks and by tests to prove trigger-specific
    weighting produced different sets).

    `model_scores` is a dict of model_id → summed weighted score. The
    `models` list is sorted descending by this score; ties break by
    Model.activation then id.
    """

    trigger: TriggerContext
    observations: list[ObservationRow] = field(default_factory=list)
    models: list[ModelRow] = field(default_factory=list)
    acts: dict[str, list] = field(
        default_factory=lambda: {"goals": [], "commitments": [], "decisions": []}
    )
    resources: list[ResourceRow] = field(default_factory=list)
    pathway_results: list[PathwayResult] = field(default_factory=list)
    notes: dict[str, Any] = field(default_factory=dict)
    model_scores: dict[UUID, float] = field(default_factory=dict)


# ---------------------------------------------------------------------
# Scoring + de-dup
# ---------------------------------------------------------------------


def _position_decay(rank: int) -> float:
    """
    1-indexed rank decays as 1/(1 + ln(rank)). rank=1 → 1.0,
    rank=10 → ~0.30, rank=40 → ~0.21. Cheap monotonic decay that
    respects ordering within a pathway without nuking long tails.
    """
    return 1.0 / (1.0 + math.log1p(rank - 1))


def _merge_and_rank_models(
    pathway_results: list[PathwayResult],
    weights: dict[str, float],
    top_n: int,
    *,
    scoring_mode: str = "linear",
) -> tuple[list[ModelRow], dict[UUID, float]]:
    """
    Given the per-pathway results and the trigger's weight map, compute
    the summed weighted score for each Model and return the top_n rows
    sorted descending by score (ties on activation, then id).

    `scoring_mode` selects the ranking algorithm:
      - "linear": legacy pathway-weighted sum (position decay). Default
        for back-compat; preserved so operators can roll back via
        `RETRIEVAL_SCORING_MODE=linear`.
      - "rrf": Reciprocal Rank Fusion via `scoring.merge_and_rank_rrf`.
        Dimension weights are the per-trigger pathway weights (mapped
        pathway→dimension), folded with activation + provenance ranks.
    """
    if scoring_mode == "rrf":
        return _merge_and_rank_models_rrf(pathway_results, weights, top_n)
    # "linear" (legacy) — unchanged path.
    scores: dict[UUID, float] = {}
    by_id: dict[UUID, ModelRow] = {}
    for pr in pathway_results:
        w = weights.get(pr.source_pathway, 0.0)
        if w <= 0.0 or not pr.models:
            continue
        for rank, m in enumerate(pr.models, start=1):
            # Tests may have duplicate Models across pathways; we sum the
            # contributions so a Model retrieved by both A and B scores
            # higher than one retrieved by only one of them.
            score = w * _position_decay(rank)
            prev = scores.get(m.id, 0.0)
            scores[m.id] = prev + score
            by_id.setdefault(m.id, m)

    ordered_ids = sorted(
        scores.keys(),
        key=lambda mid: (
            -scores[mid],
            -by_id[mid].activation,
            str(mid),
        ),
    )
    chosen = [by_id[mid] for mid in ordered_ids[:top_n]]
    return chosen, scores


def _merge_and_rank_models_rrf(
    pathway_results: list[PathwayResult],
    weights: dict[str, float],
    top_n: int,
) -> tuple[list[ModelRow], dict[UUID, float]]:
    """RRF-backed merge + rank.

    Maps the per-trigger pathway weights onto RRF dimension weights
    (A→structural, B→semantic, C→temporal, D→pattern). Keeps
    activation + provenance dimensions at the scoring module's defaults
    so they don't get zero-weighted when a trigger only mixes two
    pathways (e.g. T2 = A+D). Preserves the `(score, -activation, id)`
    tiebreak via `merge_and_rank_rrf`'s own sort key.
    """
    from .scoring import (
        DIMENSION_ACTIVATION,
        DIMENSION_PATTERN,
        DIMENSION_PROVENANCE,
        DIMENSION_SEMANTIC,
        DIMENSION_STRUCTURAL,
        DIMENSION_TEMPORAL,
        DIMENSION_WEIGHTS,
    )

    # Map pathway weights → dimension weights. Pathway not in `weights`
    # falls to 0 (dimension contributes nothing for that trigger).
    dim_weights = {
        DIMENSION_STRUCTURAL: weights.get("A", 0.0),
        DIMENSION_SEMANTIC: weights.get("B", 0.0),
        DIMENSION_TEMPORAL: weights.get("C", 0.0),
        DIMENSION_PATTERN: weights.get("D", 0.0),
        # Activation / provenance stay at the scoring module's defaults
        # so RRF's implicit priors don't vanish on 2-pathway triggers.
        DIMENSION_ACTIVATION: DIMENSION_WEIGHTS[DIMENSION_ACTIVATION],
        DIMENSION_PROVENANCE: DIMENSION_WEIGHTS[DIMENSION_PROVENANCE],
    }
    rrf = merge_and_rank_rrf(
        pathway_results,
        per_trigger_dimension_weights=dim_weights,
        top_n=top_n,
    )
    # Preserve the legacy return shape: list[ModelRow], dict[UUID, float].
    return list(rrf.ordered_items), dict(rrf.scores)


def _merge_observations(pathway_results: list[PathwayResult]) -> list[ObservationRow]:
    """Observations are only surfaced by pathway C; de-dup on id and
    order by occurred_at DESC."""
    seen: set[UUID] = set()
    out: list[ObservationRow] = []
    for pr in pathway_results:
        for o in pr.observations:
            if o.id in seen:
                continue
            seen.add(o.id)
            out.append(o)
    out.sort(key=lambda o: (o.occurred_at, o.id), reverse=True)
    return out


def _merge_acts(
    pathway_results: list[PathwayResult],
) -> dict[str, list]:
    """De-dup every kind by id across pathways."""
    goals_by_id: dict[UUID, GoalRow] = {}
    commits_by_id: dict[UUID, CommitmentRow] = {}
    decisions_by_id: dict[UUID, DecisionRow] = {}
    for pr in pathway_results:
        for g in pr.acts.get("goals", []):
            goals_by_id.setdefault(g.id, g)
        for c in pr.acts.get("commitments", []):
            commits_by_id.setdefault(c.id, c)
        for d in pr.acts.get("decisions", []):
            decisions_by_id.setdefault(d.id, d)
    return {
        "goals": sorted(goals_by_id.values(), key=lambda x: x.created_at, reverse=True),
        "commitments": sorted(
            commits_by_id.values(),
            key=lambda x: x.last_state_change_at,
            reverse=True,
        ),
        "decisions": sorted(
            decisions_by_id.values(),
            key=lambda x: x.last_state_change_at,
            reverse=True,
        ),
    }


def _merge_resources(pathway_results: list[PathwayResult]) -> list[ResourceRow]:
    seen: dict[UUID, ResourceRow] = {}
    for pr in pathway_results:
        for r in pr.resources:
            seen.setdefault(r.id, r)
    return sorted(seen.values(), key=lambda r: r.last_updated_at, reverse=True)


# ---------------------------------------------------------------------
# primary_retrieve
# ---------------------------------------------------------------------


async def primary_retrieve(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    models_repo: ModelsRepo | None = None,
    embedder: OllamaClient | None = None,
    top_n: int = _DEFAULT_TOP_N,
    config: RetrievalConfig | None = None,
) -> RetrievalResult:
    """
    Run the per-trigger pathway mix, merge results, reconsolidate the
    returned Models via `ModelsRepo.retrieve`, and return a
    `RetrievalResult`.

    `conn` MUST be the caller's transaction connection — we call
    `ModelsRepo.retrieve(..., conn=conn)` so the activation bump lands
    in that transaction. If Think rolls back, reconsolidation rolls
    back with it.

    `models_repo` is optional; if omitted, we construct one bound to
    the connection's pool. (Callers that want explicit control over
    the repo's embedder can pass their own.)

    `config` (RA-5) supplies tunable defaults (semantic_k,
    hnsw_ef_search, temporal_include_entity_mentions). When None we
    use the module-level CONFIG which is loaded from env at import.
    Trigger fields (e.g. `trigger.semantic_k`) still win when set
    explicitly — config only fills in defaults.
    """
    cfg = config or CONFIG
    weights = _TRIGGER_WEIGHTS.get(trigger.kind)
    if weights is None:
        raise ValidationError(
            f"unknown trigger kind {trigger.kind!r}",
            kind=trigger.kind,
        )

    pathway_results: list[PathwayResult] = []
    notes: dict[str, Any] = {
        "kind": trigger.kind,
        "weights": dict(weights),
        "pathways_run": [],
        "pathways_skipped": [],
        "config_summary": {
            "semantic_k": cfg.semantic_k,
            "semantic_hnsw_ef_search": cfg.semantic_hnsw_ef_search,
            "temporal_include_entity_mentions": cfg.temporal_include_entity_mentions,
            "scoring_mode": cfg.scoring_mode,
            "assembler_use_mmr": cfg.assembler_use_mmr,
        },
    }

    # ------ Pathway A (all triggers) ------
    if "A" in weights:
        # For T2, the seed entities come from the model's scope.
        seeds = list(trigger.seed_entity_ids)
        # For T1 event_arrival the ingestion enqueuer puts the author
        # actor UUID(s) in trigger.scope_actors but does not synthesise
        # seed_entity_ids. Without entity seeds pathway A bails early
        # ("empty_seed"), which silently strips the Acts graph walk for
        # every user-authored signal. Synthesise actor seeds so short
        # messages ("actually i only need 1 day of work") still retrieve
        # the author's scoped models / commitments.
        if trigger.kind == "T1" and trigger.scope_actors and not seeds:
            for a in trigger.scope_actors:
                seeds.append({"type": "actor", "id": str(a)})
        if trigger.kind == "T2" and trigger.model_id is not None and not seeds:
            # Fetch the Model's scope_entities and scope_actors.
            row = await conn.fetchrow(
                """
                SELECT scope_entities, scope_actors
                FROM models
                WHERE id = $1 AND tenant_id = $2
                """,
                trigger.model_id,
                trigger.tenant_id,
            )
            if row is not None:
                import json
                raw_se = row["scope_entities"]
                if isinstance(raw_se, (bytes, bytearray)):
                    raw_se = raw_se.decode()
                if isinstance(raw_se, str):
                    try:
                        raw_se = json.loads(raw_se)
                    except json.JSONDecodeError:
                        raw_se = []
                if isinstance(raw_se, list):
                    for e in raw_se:
                        if isinstance(e, dict):
                            seeds.append(e)
                actors = row["scope_actors"] or []
                for a in actors:
                    seeds.append({"type": "actor", "id": str(a)})

        try:
            pr_a = await pathway_a_structural(
                seeds,
                trigger.tenant_id,
                conn,
                max_hops=trigger.max_hops,
            )
            pathway_results.append(pr_a)
            notes["pathways_run"].append("A")
        except Exception as e:
            notes["pathways_skipped"].append({"pathway": "A", "reason": str(e)})

    # ------ Pathway B ------
    if "B" in weights:
        text = trigger.seed_natural_text or ""
        # Resolve k: trigger field wins if it differs from the legacy
        # default; otherwise fall back to the config-supplied k.
        b_k = trigger.semantic_k if trigger.semantic_k != 40 else cfg.semantic_k
        try:
            pr_b = await pathway_b_semantic(
                text,
                trigger.tenant_id,
                conn,
                k=b_k,
                embedder=embedder,
                precomputed_vector=trigger.precomputed_seed_vector,
                hnsw_ef_search=cfg.semantic_hnsw_ef_search,
            )
            pathway_results.append(pr_b)
            notes["pathways_run"].append("B")
        except RetrievalPathwayError as e:
            notes["pathways_skipped"].append({"pathway": "B", "reason": str(e)})

    # ------ Pathway C ------
    if "C" in weights:
        if trigger.seed_occurred_at is not None:
            try:
                pr_c = await pathway_c_temporal(
                    trigger.seed_occurred_at,
                    trigger.temporal_window,
                    trigger.tenant_id,
                    conn,
                    scope_actors=trigger.scope_actors,
                    include_entity_mentions=cfg.temporal_include_entity_mentions,
                )
                pathway_results.append(pr_c)
                notes["pathways_run"].append("C")
            except Exception as e:
                notes["pathways_skipped"].append({"pathway": "C", "reason": str(e)})
        else:
            notes["pathways_skipped"].append(
                {"pathway": "C", "reason": "no_seed_occurred_at"}
            )

    # ------ Pathway D ------
    if "D" in weights:
        try:
            pr_d = await pathway_d_pattern(
                trigger.seed_signature,
                trigger.tenant_id,
                conn,
            )
            pathway_results.append(pr_d)
            notes["pathways_run"].append("D")
        except Exception as e:
            notes["pathways_skipped"].append({"pathway": "D", "reason": str(e)})

    # ------ Merge + rank ------
    models, scores = _merge_and_rank_models(
        pathway_results, weights, top_n=top_n,
        scoring_mode=cfg.scoring_mode,
    )
    observations = _merge_observations(pathway_results)
    acts = _merge_acts(pathway_results)
    resources = _merge_resources(pathway_results)

    notes["models_merged"] = len(models)
    notes["observations_merged"] = len(observations)
    notes["acts_merged"] = {k: len(v) for k, v in acts.items()}
    notes["resources_merged"] = len(resources)

    # ------ Reconsolidation ------
    if models:
        if models_repo is None:
            # We don't need the embedder for retrieve().
            models_repo = ModelsRepo(pool=None)  # type: ignore[arg-type]
        # ModelsRepo.retrieve signs: retrieve(ids, *, conn=None).
        # We always pass the caller's conn so the UPDATE lands in the
        # caller's transaction.
        reconsolidated = await models_repo.retrieve(
            [m.id for m in models], conn=conn
        )
        # Post-reconsolidation ModelRow reflects the bumped activation +
        # retrieval_count + last_retrieved_at. We prefer these fresh
        # rows so the caller sees the latest values (they will have
        # been updated within this same tx).
        by_id = {m.id: m for m in reconsolidated}
        models = [by_id.get(m.id, m) for m in models]
        notes["reconsolidated_count"] = len(reconsolidated)

    return RetrievalResult(
        trigger=trigger,
        observations=observations,
        models=models,
        acts=acts,
        resources=resources,
        pathway_results=pathway_results,
        notes=notes,
        model_scores=scores,
    )


__all__ = [
    "TriggerKind",
    "TriggerContext",
    "RetrievalResult",
    "primary_retrieve",
    "RetrievalError",
]
