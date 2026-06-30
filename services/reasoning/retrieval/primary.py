"""
services/reasoning/retrieval/primary.py — `primary_retrieve` + TriggerContext +
RetrievalResult.

Spec reference: ARCHITECTURE-FINAL.md §8 "Primary pathway resolver",
BUILD-PLAN §4 Prompt 3.A item 2.

Per-trigger pathway mix:
  - T1 (new signal)          : A + B + L + C + G
  - T2 (prediction due)      : A + B + L + D + G
  - T3 (anomaly)             : A + B + L + C + G
  - T4 (background/pattern)  : D + A + L + G
  - T6 (legacy topology row) : A + B + L + G

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

import asyncio
import math
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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

from services.domain.models.repo import ModelsRepo
from services.reasoning.sage.retrieval_policy import (
    SageRetrievalPolicy,
    SageRouteUtility,
    plan_primary_retrieval,
    summarize_primary_observation,
)

from .config import CONFIG, RetrievalConfig
from .pathways import (
    PathwayResult,
    RetrievalPathwayError,
    pathway_a_structural,
    pathway_b_semantic,
    pathway_b_representation_tags,
    pathway_c_temporal,
    pathway_d_pattern,
    pathway_g_model_edges,
    pathway_l_semantic_terms,
)
from .projection_pathway import pathway_projection_context
from .read_fanout import ReadFanoutBudget
from .scoring import merge_and_rank_rrf

from lib.observability import counter, histogram


TriggerKind = Literal["T1", "T2", "T3", "T4", "T6"]


# Retrieval-layer Prometheus families (one per primary-retrieval stage;
# pgvector query timing lives in pathways.py). Exposed by whichever worker
# process serves /metrics (think worker today).
_STAGE_DURATION = histogram(
    "retrieval_stage_duration_seconds",
    "Primary-retrieval stage latency (derive_scope, pathway_A..G, merge/rank).",
    ("stage",),
)
_STAGE_TOTAL = counter(
    "retrieval_stage_total",
    "Primary-retrieval stage executions by outcome (ok|skipped).",
    ("stage", "status"),
)


def _raise_if_postgres_error(exc: Exception) -> None:
    """Do not swallow SQL errors inside the caller's transaction."""
    if isinstance(exc, asyncpg.PostgresError):
        raise exc


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def _coerce_trigger_seed_occurred_at(trigger: TriggerContext) -> None:
    """Normalize queue JSON timestamps before temporal retrieval.

    TriggerContext is a dataclass, so callers can still pass the JSONB
    payload's ISO string despite the type hint. Normalize here because this
    is the shared boundary before Pathway C, inquiry question planning, and
    prompt rendering consume the value.
    """
    value = trigger.seed_occurred_at
    if value is None:
        return
    if isinstance(value, datetime):
        if value.tzinfo is None:
            trigger.seed_occurred_at = value.replace(tzinfo=timezone.utc)
        return
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            trigger.seed_occurred_at = None
            return
        trigger.seed_occurred_at = (
            parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        )
        return
    trigger.seed_occurred_at = None


def _pathway_counts(result: PathwayResult | None) -> dict[str, Any]:
    if result is None:
        return {}
    return {
        "models": len(result.models),
        "observations": len(result.observations),
        "acts": {k: len(v) for k, v in (result.acts or {}).items()},
        "resources": len(result.resources),
        "pathway_notes": result.notes or {},
    }


def _append_pathway_timing(
    timings: list[dict[str, Any]],
    stage: str,
    started: float,
    **extra: Any,
) -> None:
    elapsed_ms = _elapsed_ms(started)
    note = {
        "stage": stage,
        "elapsed_ms": elapsed_ms,
    }
    for key, value in extra.items():
        if value is not None:
            note[key] = value
    timings.append(note)
    # Prometheus twin of the debug-notes timing. `stage` is a bounded
    # literal set (derive_scope, pathway_A..L, merge/rank stages).
    _STAGE_DURATION.observe(elapsed_ms / 1000.0, stage=stage)
    _STAGE_TOTAL.inc(stage=stage, status="skipped" if extra.get("skipped") else "ok")


# Per-spec-§8 weighting mix. Live topology now writes relationship/
# situation candidates, and those candidates reach Think through T4
# `latent_relationship_candidate` member_model_ids.
_TRIGGER_WEIGHTS: dict[TriggerKind, dict[str, float]] = {
    "T1": {"A": 0.30, "B": 0.26, "L": 0.12, "C": 0.16, "G": 0.16},
    "T2": {"A": 0.16, "B": 0.15, "L": 0.12, "D": 0.12, "G": 0.45},
    "T3": {"A": 0.30, "B": 0.20, "L": 0.12, "C": 0.16, "G": 0.22},
    "T4": {"D": 0.38, "A": 0.25, "L": 0.12, "G": 0.25},
    "T6": {"A": 0.28, "B": 0.22, "L": 0.12, "G": 0.38},
}

_TRIGGER_SUBKIND_WEIGHTS: dict[tuple[TriggerKind, str], dict[str, float]] = {
    # Open questions are deliberately system-wide: semantic vector and lexical
    # term search should dominate, with graph/pattern context as support.
    ("T4", "open_question_search"): {
        "B": 0.34,
        "L": 0.22,
        "D": 0.18,
        "G": 0.16,
        "A": 0.10,
    },
}


_DEFAULT_TOP_N = 80


def _trigger_weights(
    kind: TriggerKind,
    cfg: RetrievalConfig,
    *,
    subkind: str | None = None,
) -> dict[str, float] | None:
    weights = (
        _TRIGGER_SUBKIND_WEIGHTS.get((kind, subkind))
        if subkind is not None
        else None
    )
    if weights is None:
        weights = _TRIGGER_WEIGHTS.get(kind)
    if weights is None:
        return None
    if not cfg.trigger_weights_json:
        return dict(weights)
    try:
        payload = json.loads(cfg.trigger_weights_json)
    except json.JSONDecodeError:
        return dict(weights)
    if not isinstance(payload, dict):
        return dict(weights)
    override = payload.get(f"{kind}:{subkind}") if subkind else None
    if override is None:
        override = payload.get(kind)
    if override is None and all(pathway in payload for pathway in weights):
        override = payload
    if not isinstance(override, dict):
        return dict(weights)
    merged = dict(weights)
    for pathway, raw_weight in override.items():
        key = str(pathway).upper()
        if key not in weights:
            continue
        try:
            merged[key] = max(0.0, float(raw_weight))
        except (TypeError, ValueError):
            continue
    total = sum(merged.values())
    if total <= 0:
        return dict(weights)
    return {key: value / total for key, value in merged.items()}


class RetrievalError(CompanyOSError):
    default_code = "retrieval_error"


@dataclass
class TriggerContext:
    """
    The common trigger payload passed into `primary_retrieve`. Each
    trigger kind uses a subset of the fields:

      T1: observation_id, seed_entity_ids, seed_natural_text,
          seed_occurred_at, scope_actors
      T1:event_batch: observation_ids + member_trigger_ids, with
          observation_id preserved as the primary/oldest signal for
          backwards-compatible provenance and deterministic fallbacks
      T2: model_id; batched belief updates also pass additional model
          seeds in member_model_ids
      T3: region_spec (anomaly region descriptor); typically carries
          seed_entity_ids + seed_natural_text under the hood (populated
          by the Anomaly processor's enqueue path)
      T4: subkind, seed_signature (from a Precipitation proposal)
    """

    kind: TriggerKind
    tenant_id: UUID

    # T1
    observation_id: UUID | None = None
    observation_ids: list[UUID] = field(default_factory=list)
    member_trigger_ids: list[UUID] = field(default_factory=list)
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

    # T6 — topology phase event (S3)
    topology_event_id: UUID | None = None
    topology_event_kind: str | None = None
    neighborhood_id: UUID | None = None
    member_model_ids: list[UUID] = field(default_factory=list)

    # Pre-computed embedding (optional; tests pass one to skip Ollama)
    precomputed_seed_vector: list[float] | None = None

    # Hop cap for pathway A (2 is the spec default)
    max_hops: int = 2

    # Temporal window for pathway C
    temporal_window: timedelta = timedelta(days=7)

    # Pathway B k
    semantic_k: int = 40

    @property
    def is_batch(self) -> bool:
        return bool(self.observation_ids or self.member_trigger_ids)


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


@dataclass(frozen=True, slots=True)
class _PathwayFanoutResult:
    name: str
    result: PathwayResult | None
    notes: dict[str, Any]
    timings: list[dict[str, Any]]


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
    rrf_k: int = 60,
    recency_decay_half_life_days: float = 0.0,
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
        return _merge_and_rank_models_rrf(
            pathway_results,
            weights,
            top_n,
            rrf_k=rrf_k,
            recency_decay_half_life_days=recency_decay_half_life_days,
        )
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
            -_score_with_recency_decay(
                scores[mid],
                by_id[mid],
                half_life_days=recency_decay_half_life_days,
            ),
            -by_id[mid].activation,
            str(mid),
        ),
    )
    if recency_decay_half_life_days > 0:
        scores = {
            mid: _score_with_recency_decay(
                score,
                by_id[mid],
                half_life_days=recency_decay_half_life_days,
            )
            for mid, score in scores.items()
        }
    chosen = [by_id[mid] for mid in ordered_ids[:top_n]]
    return chosen, scores


def _merge_and_rank_models_rrf(
    pathway_results: list[PathwayResult],
    weights: dict[str, float],
    top_n: int,
    *,
    rrf_k: int,
    recency_decay_half_life_days: float = 0.0,
) -> tuple[list[ModelRow], dict[UUID, float]]:
    """RRF-backed merge + rank.

    Maps the per-trigger pathway weights onto RRF dimension weights
    (A→structural, B→semantic, C→temporal, D→pattern, G→model-edge). Keeps
    activation + provenance dimensions at the scoring module's defaults
    so they don't get zero-weighted when a trigger only mixes two
    pathways (e.g. T2 = A+D). Preserves the `(score, -activation, id)`
    tiebreak via `merge_and_rank_rrf`'s own sort key.
    """
    from .scoring import (
        DIMENSION_ACTIVATION,
        DIMENSION_LEXICAL,
        DIMENSION_MODEL_EDGE,
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
        DIMENSION_LEXICAL: weights.get("L", 0.0),
        DIMENSION_TEMPORAL: weights.get("C", 0.0),
        DIMENSION_PATTERN: weights.get("D", 0.0),
        DIMENSION_MODEL_EDGE: weights.get("G", 0.0),
        # Activation / provenance stay at the scoring module's defaults
        # so RRF's implicit priors don't vanish on 2-pathway triggers.
        DIMENSION_ACTIVATION: DIMENSION_WEIGHTS[DIMENSION_ACTIVATION],
        DIMENSION_PROVENANCE: DIMENSION_WEIGHTS[DIMENSION_PROVENANCE],
    }
    rrf = merge_and_rank_rrf(
        pathway_results,
        per_trigger_dimension_weights=dim_weights,
        k=max(1, int(rrf_k)),
        top_n=None,
    )
    by_id = {m.id: m for m in rrf.ordered_items}
    scores = dict(rrf.scores)
    if recency_decay_half_life_days > 0:
        scores = {
            mid: _score_with_recency_decay(
                score,
                by_id[mid],
                half_life_days=recency_decay_half_life_days,
            )
            for mid, score in scores.items()
            if mid in by_id
        }
    ordered_ids = sorted(
        scores,
        key=lambda mid: (
            -scores[mid],
            -(getattr(by_id[mid], "activation", 0.0) or 0.0),
            str(mid),
        ),
    )
    ordered = [by_id[mid] for mid in ordered_ids[:top_n]]
    # Preserve the legacy return shape: list[ModelRow], dict[UUID, float].
    return ordered, scores


def _score_with_recency_decay(
    score: float,
    model: ModelRow,
    *,
    half_life_days: float,
) -> float:
    if half_life_days <= 0:
        return score
    created_at = getattr(model, "created_at", None)
    if not isinstance(created_at, datetime):
        return score
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    age_days = max(
        0.0,
        (
            datetime.now(timezone.utc) - created_at.astimezone(timezone.utc)
        ).total_seconds()
        / 86400.0,
    )
    multiplier = math.exp(-math.log(2.0) * age_days / max(half_life_days, 0.001))
    return score * multiplier


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


def _append_seed_once(
    seeds: list[dict[str, Any]],
    seen: set[tuple[str, str]],
    seed: dict[str, Any],
) -> None:
    if not isinstance(seed, dict):
        return
    etype = seed.get("type")
    eid = seed.get("id")
    if not etype or eid is None:
        return
    if str(etype) == "customer":
        etype = "customer_resource"
    key = (str(etype), str(eid))
    if key in seen:
        return
    seen.add(key)
    seeds.append({"type": str(etype), "id": str(eid)})


def _append_actor_once(
    actors: list[UUID],
    seen: set[UUID],
    actor_id: Any,
) -> None:
    if actor_id is None:
        return
    try:
        aid = UUID(str(actor_id))
    except (ValueError, TypeError):
        return
    if aid in seen:
        return
    seen.add(aid)
    actors.append(aid)


def _coerce_vector(value: Any) -> list[float] | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return None
    try:
        return [float(x) for x in value]
    except (TypeError, ValueError):
        return None


def _hydrate_observation(record: asyncpg.Record) -> ObservationRow:
    raw = dict(record)
    for key in ("content", "entities_mentioned"):
        value = raw.get(key)
        if isinstance(value, (bytes, bytearray)):
            value = value.decode()
        if isinstance(value, str):
            try:
                raw[key] = json.loads(value)
            except json.JSONDecodeError:
                raw[key] = {} if key == "content" else []
    raw["embedding"] = _coerce_vector(raw.get("embedding"))
    return ObservationRow.model_validate(raw)


async def _fetch_trigger_observations(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
) -> list[ObservationRow]:
    if trigger.kind != "T1":
        return []
    observation_ids: list[UUID] = []
    seen: set[UUID] = set()
    if trigger.observation_id is not None:
        observation_ids.append(trigger.observation_id)
        seen.add(trigger.observation_id)
    for oid in trigger.observation_ids:
        if oid not in seen:
            observation_ids.append(oid)
            seen.add(oid)
    if not observation_ids:
        return []
    rows = await conn.fetch(
        """
        SELECT id, tenant_id, occurred_at, ingested_at, kind, source_channel,
               source_actor_ref, actor_id, content, content_text, embedding,
               embedding_pending, trust_tier, external_id, cause_id,
               sequence_num, entities_mentioned
        FROM observations
        WHERE id = ANY($1::uuid[]) AND tenant_id = $2
        ORDER BY occurred_at DESC
        """,
        observation_ids,
        trigger.tenant_id,
    )
    return [_hydrate_observation(row) for row in rows]


def _coerce_entity_refs(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    if not isinstance(value, list):
        return []
    return [e for e in value if isinstance(e, dict)]


async def _derive_trigger_scope(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
) -> tuple[list[dict[str, Any]], list[UUID], str | None, list[float] | None]:
    """
    Build the effective scope once and share it across pathways.

    The queue payload may provide only actor scope, only entity scope,
    or for T2 just a model_id. Pulling these into one normalized shape
    prevents Pathway A and Pathway B from seeing different worlds.
    """
    seeds: list[dict[str, Any]] = []
    seen_seeds: set[tuple[str, str]] = set()
    for e in trigger.seed_entity_ids:
        _append_seed_once(seeds, seen_seeds, e)

    actors: list[UUID] = []
    seen_actors: set[UUID] = set()
    for a in trigger.scope_actors:
        _append_actor_once(actors, seen_actors, a)

    model_natural: str | None = None
    model_embedding: list[float] | None = None

    if trigger.kind == "T1":
        observation_ids: list[UUID] = []
        seen_observation_ids: set[UUID] = set()
        if trigger.observation_id is not None:
            observation_ids.append(trigger.observation_id)
            seen_observation_ids.add(trigger.observation_id)
        for oid in trigger.observation_ids:
            if oid not in seen_observation_ids:
                observation_ids.append(oid)
                seen_observation_ids.add(oid)
        if observation_ids:
            rows = await conn.fetch(
                """
                SELECT entities_mentioned, actor_id, content_text, embedding
                FROM observations
                WHERE id = ANY($1::uuid[]) AND tenant_id = $2
                ORDER BY occurred_at ASC
                """,
                observation_ids,
                trigger.tenant_id,
            )
            for row in rows:
                for e in _coerce_entity_refs(row["entities_mentioned"]):
                    _append_seed_once(seeds, seen_seeds, e)
                if row["actor_id"] is not None:
                    _append_actor_once(actors, seen_actors, row["actor_id"])
                if (
                    not model_natural
                    and isinstance(row["content_text"], str)
                    and row["content_text"].strip()
                ):
                    model_natural = row["content_text"]
                if model_embedding is None:
                    model_embedding = _coerce_vector(row["embedding"])

    if trigger.kind == "T2":
        t2_model_ids: list[UUID] = []
        if trigger.model_id is not None:
            t2_model_ids.append(trigger.model_id)
        for mid in trigger.member_model_ids:
            if mid not in t2_model_ids:
                t2_model_ids.append(mid)
        rows = (
            await conn.fetch(
                """
            SELECT scope_entities, scope_actors, "natural", embedding
            FROM models
            WHERE id = ANY($1::uuid[]) AND tenant_id = $2
            ORDER BY array_position($1::uuid[], id)
            """,
                t2_model_ids,
                trigger.tenant_id,
            )
            if t2_model_ids
            else []
        )
        for row in rows:
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
                    _append_seed_once(seeds, seen_seeds, e)
            for a in row["scope_actors"] or []:
                _append_actor_once(actors, seen_actors, a)
            if (
                model_natural is None
                and isinstance(row["natural"], str)
                and row["natural"].strip()
            ):
                model_natural = row["natural"]
            if model_embedding is None:
                model_embedding = _coerce_vector(row["embedding"])

    return seeds, actors, model_natural, model_embedding


def _primary_notes(
    trigger: TriggerContext,
    cfg: RetrievalConfig,
    weights: dict[str, float],
    pathway_timings: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "kind": trigger.kind,
        "weights": dict(weights),
        "pathways_run": [],
        "pathways_skipped": [],
        "pathway_timings": pathway_timings,
        "config_summary": {
            "semantic_k": cfg.semantic_k,
            "semantic_hnsw_ef_search": cfg.semantic_hnsw_ef_search,
            "semantic_terms_enabled": cfg.semantic_terms_enabled,
            "semantic_terms_k": cfg.semantic_terms_k,
            "temporal_max_observations": cfg.temporal_max_observations,
            "temporal_max_models": cfg.temporal_max_models,
            "temporal_include_entity_mentions": cfg.temporal_include_entity_mentions,
            "scoring_mode": cfg.scoring_mode,
            "rrf_k": cfg.rrf_k,
            "trigger_weights_overridden": bool(cfg.trigger_weights_json),
            "recency_decay_half_life_days": cfg.recency_decay_half_life_days,
            "projection_context_enabled": cfg.projection_context_enabled,
            "projection_context_max_snapshots": cfg.projection_context_max_snapshots,
            "projection_context_max_models": cfg.projection_context_max_models,
            "assembler_use_mmr": cfg.assembler_use_mmr,
            "assembler_budget_models": cfg.assembler_budget_models,
            "assembler_budget_observations": cfg.assembler_budget_observations,
        },
    }


def _plan_sage_primary_policy(
    *,
    trigger: TriggerContext,
    cfg: RetrievalConfig,
    weights: dict[str, float],
    effective_seed_entities: list[dict[str, Any]],
    effective_scope_actors: list[UUID],
    notes: dict[str, Any],
    route_utilities: tuple[SageRouteUtility, ...] = (),
) -> tuple[dict[str, float], SageRetrievalPolicy | None]:
    if not bool(getattr(cfg, "sage_retrieval_policy_enabled", True)):
        notes["sage_retrieval_policy"] = {"enabled": False}
        return weights, None

    policy = plan_primary_retrieval(
        trigger=trigger,
        weights=weights,
        effective_seed_entities=effective_seed_entities,
        effective_scope_actors=effective_scope_actors,
        projection_enabled=bool(cfg.projection_context_enabled),
        semantic_terms_enabled=bool(cfg.semantic_terms_enabled),
        semantic_k=int(cfg.semantic_k),
        shadow=bool(getattr(cfg, "sage_retrieval_policy_shadow_mode", False)),
        exploration_rate=float(
            getattr(cfg, "sage_retrieval_policy_exploration_rate", 0.05)
        ),
        route_utilities=route_utilities,
    )
    adjusted_weights = policy.apply_primary_weights(weights)
    policy_notes = policy.notes()
    policy_notes["original_weights"] = dict(weights)
    policy_notes["applied_weights"] = dict(adjusted_weights)
    notes["sage_retrieval_policy"] = policy_notes
    if not policy.shadow:
        for decision in policy.decisions:
            if decision.mode == "skip" and decision.path in weights:
                notes["pathways_skipped"].append(
                    {
                        "pathway": decision.path,
                        "reason": decision.reason,
                        "source": "sage_retrieval_policy",
                    }
                )
    return adjusted_weights, policy


async def _prepare_effective_trigger_scope(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    notes: dict[str, Any],
    pathway_timings: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[UUID], str | None, list[float] | None]:
    stage_started = time.perf_counter()
    (
        effective_seed_entities,
        effective_scope_actors,
        t2_model_natural,
        t2_model_embedding,
    ) = await _derive_trigger_scope(trigger, conn)
    _append_pathway_timing(
        pathway_timings,
        "derive_scope",
        stage_started,
        seed_entities=len(effective_seed_entities),
        scope_actors=len(effective_scope_actors),
        row_text_fallback=bool(t2_model_natural),
        row_embedding_fallback=bool(t2_model_embedding),
    )
    trigger.seed_entity_ids = list(effective_seed_entities)
    trigger.scope_actors = list(effective_scope_actors)
    if not trigger.seed_natural_text and t2_model_natural:
        trigger.seed_natural_text = t2_model_natural
    if trigger.precomputed_seed_vector is None and t2_model_embedding is not None:
        trigger.precomputed_seed_vector = t2_model_embedding
    notes["effective_scope"] = {
        "seed_entities": len(effective_seed_entities),
        "scope_actors": len(effective_scope_actors),
        "row_text_fallback": bool(t2_model_natural),
        "row_embedding_fallback": bool(t2_model_embedding),
        "t2_model_text_fallback": (trigger.kind == "T2" and bool(t2_model_natural)),
        "t2_model_embedding_fallback": (
            trigger.kind == "T2" and bool(t2_model_embedding)
        ),
    }
    return (
        effective_seed_entities,
        effective_scope_actors,
        t2_model_natural,
        t2_model_embedding,
    )


async def _run_projection_context(
    *,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    cfg: RetrievalConfig,
    effective_seed_entities: list[dict[str, Any]],
    effective_scope_actors: list[UUID],
    notes: dict[str, Any],
    pathway_timings: list[dict[str, Any]],
) -> PathwayResult | None:
    stage_started = time.perf_counter()
    if not cfg.projection_context_enabled:
        notes["projection_context"] = {"enabled": False}
        _append_pathway_timing(
            pathway_timings,
            "projection_context",
            stage_started,
            skipped=True,
            reason="disabled",
        )
        return None

    try:
        result = await pathway_projection_context(
            trigger,
            trigger.tenant_id,
            conn,
            effective_seed_entities=effective_seed_entities,
            effective_scope_actors=effective_scope_actors,
            max_snapshots=cfg.projection_context_max_snapshots,
            max_models=cfg.projection_context_max_models,
        )
        notes["projection_context"] = result.notes
        _append_pathway_timing(
            pathway_timings,
            "projection_context",
            stage_started,
            **_pathway_counts(result),
        )
        return result if result.models else None
    except Exception as exc:
        _raise_if_postgres_error(exc)
        notes["projection_context"] = {
            "enabled": True,
            "skipped": True,
            "reason": str(exc),
        }
        _append_pathway_timing(
            pathway_timings,
            "projection_context",
            stage_started,
            skipped=True,
            reason=str(exc),
        )
        return None


async def _run_pathway_a(
    *,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    read_pool: asyncpg.Pool | None,
    read_fanout_enabled: bool,
    read_fanout_min_seeds: int,
    read_fanout_chunk_size: int,
    read_fanout_budget: ReadFanoutBudget | None = None,
    effective_seed_entities: list[dict[str, Any]],
    effective_scope_actors: list[UUID],
    notes: dict[str, Any],
    pathway_timings: list[dict[str, Any]],
) -> PathwayResult | None:
    seeds = list(effective_seed_entities)
    if trigger.kind == "T1" and effective_scope_actors and not seeds:
        for actor_id in effective_scope_actors:
            seeds.append({"type": "actor", "id": str(actor_id)})

    stage_started = time.perf_counter()
    try:
        result = await pathway_a_structural(
            seeds,
            trigger.tenant_id,
            conn,
            max_hops=trigger.max_hops,
            read_pool=read_pool,
            read_fanout_enabled=read_fanout_enabled,
            read_fanout_min_seeds=read_fanout_min_seeds,
            read_fanout_chunk_size=read_fanout_chunk_size,
            read_fanout_budget=read_fanout_budget,
        )
        notes["pathways_run"].append("A")
        _append_pathway_timing(
            pathway_timings,
            "pathway_A",
            stage_started,
            **_pathway_counts(result),
        )
        return result
    except Exception as exc:
        _raise_if_postgres_error(exc)
        notes["pathways_skipped"].append({"pathway": "A", "reason": str(exc)})
        _append_pathway_timing(
            pathway_timings,
            "pathway_A",
            stage_started,
            skipped=True,
            reason=str(exc),
        )
        return None


async def _run_pathway_b(
    *,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    cfg: RetrievalConfig,
    embedder: OllamaClient | None,
    sage_policy: SageRetrievalPolicy | None = None,
    effective_seed_entities: list[dict[str, Any]],
    effective_scope_actors: list[UUID],
    t2_model_natural: str | None,
    t2_model_embedding: list[float] | None,
    notes: dict[str, Any],
    pathway_timings: list[dict[str, Any]],
) -> PathwayResult | None:
    text = trigger.seed_natural_text or t2_model_natural or ""
    vector = trigger.precomputed_seed_vector or t2_model_embedding
    b_k = trigger.semantic_k if trigger.semantic_k != 40 else cfg.semantic_k
    if sage_policy is not None:
        b_k = sage_policy.budget_for("B", b_k)
    stage_started = time.perf_counter()
    try:
        result = await pathway_b_semantic(
            text,
            trigger.tenant_id,
            conn,
            k=b_k,
            embedder=embedder,
            precomputed_vector=vector,
            event_actors=effective_scope_actors,
            event_entities=effective_seed_entities,
            hnsw_ef_search=cfg.semantic_hnsw_ef_search,
        )
        tag_result = await pathway_b_representation_tags(
            text,
            trigger.tenant_id,
            conn,
            seed_signature=(
                trigger.seed_signature
                if isinstance(trigger.seed_signature, dict)
                else None
            ),
            limit=max(20, min(120, b_k * 2)),
        )
        result.notes["representation_tag_rescue"] = tag_result.notes
        if tag_result.models:
            seen = {model.id for model in result.models}
            rescued = [model for model in tag_result.models if model.id not in seen]
            if rescued:
                result.models = [*result.models, *rescued]
                result.notes["models_returned"] = len(result.models)
        notes["pathways_run"].append("B")
        _append_pathway_timing(
            pathway_timings,
            "pathway_B",
            stage_started,
            **_pathway_counts(result),
        )
        return result
    except (RetrievalPathwayError, ValidationError) as exc:
        notes["pathways_skipped"].append({"pathway": "B", "reason": str(exc)})
        _append_pathway_timing(
            pathway_timings,
            "pathway_B",
            stage_started,
            skipped=True,
            reason=str(exc),
        )
        return None


async def _run_pathway_l(
    *,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    cfg: RetrievalConfig,
    effective_seed_entities: list[dict[str, Any]],
    effective_scope_actors: list[UUID],
    t2_model_natural: str | None,
    notes: dict[str, Any],
    pathway_timings: list[dict[str, Any]],
) -> PathwayResult | None:
    stage_started = time.perf_counter()
    if not cfg.semantic_terms_enabled:
        notes["pathways_skipped"].append(
            {"pathway": "L", "reason": "semantic_terms_disabled"}
        )
        _append_pathway_timing(
            pathway_timings,
            "pathway_L",
            stage_started,
            skipped=True,
            reason="semantic_terms_disabled",
        )
        return None

    text = trigger.seed_natural_text or t2_model_natural or ""
    try:
        result = await pathway_l_semantic_terms(
            text,
            trigger.tenant_id,
            conn,
            seed_signature=(
                trigger.seed_signature
                if isinstance(trigger.seed_signature, dict)
                else None
            ),
            scope_actors=effective_scope_actors,
            scope_entities=effective_seed_entities,
            limit=cfg.semantic_terms_k,
        )
        if result.notes.get("reason") == "no_terms":
            notes["pathways_skipped"].append({"pathway": "L", "reason": "no_terms"})
            _append_pathway_timing(
                pathway_timings,
                "pathway_L",
                stage_started,
                skipped=True,
                reason="no_terms",
                **_pathway_counts(result),
            )
            return None
        notes["pathways_run"].append("L")
        _append_pathway_timing(
            pathway_timings,
            "pathway_L",
            stage_started,
            **_pathway_counts(result),
        )
        return result
    except Exception as exc:
        _raise_if_postgres_error(exc)
        notes["pathways_skipped"].append({"pathway": "L", "reason": str(exc)})
        _append_pathway_timing(
            pathway_timings,
            "pathway_L",
            stage_started,
            skipped=True,
            reason=str(exc),
        )
        return None


async def _run_pathway_c(
    *,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    cfg: RetrievalConfig,
    effective_seed_entities: list[dict[str, Any]],
    effective_scope_actors: list[UUID],
    notes: dict[str, Any],
    pathway_timings: list[dict[str, Any]],
) -> PathwayResult | None:
    if trigger.seed_occurred_at is None:
        notes["pathways_skipped"].append(
            {"pathway": "C", "reason": "no_seed_occurred_at"}
        )
        _append_pathway_timing(
            pathway_timings,
            "pathway_C",
            time.perf_counter(),
            skipped=True,
            reason="no_seed_occurred_at",
        )
        return None

    stage_started = time.perf_counter()
    temporal_max_observations = int(cfg.temporal_max_observations)
    if cfg.model_first_context_enabled:
        temporal_max_observations = min(
            temporal_max_observations,
            max(0, int(cfg.historical_observation_cap)),
        )
    try:
        result = await pathway_c_temporal(
            trigger.seed_occurred_at,
            trigger.temporal_window,
            trigger.tenant_id,
            conn,
            scope_actors=effective_scope_actors,
            scope_entities=effective_seed_entities,
            max_observations=temporal_max_observations,
            max_models=cfg.temporal_max_models,
            include_entity_mentions=cfg.temporal_include_entity_mentions,
        )
        notes["pathways_run"].append("C")
        _append_pathway_timing(
            pathway_timings,
            "pathway_C",
            stage_started,
            **_pathway_counts(result),
        )
        return result
    except Exception as exc:
        _raise_if_postgres_error(exc)
        notes["pathways_skipped"].append({"pathway": "C", "reason": str(exc)})
        _append_pathway_timing(
            pathway_timings,
            "pathway_C",
            stage_started,
            skipped=True,
            reason=str(exc),
        )
        return None


async def _run_pathway_d(
    *,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    notes: dict[str, Any],
    pathway_timings: list[dict[str, Any]],
) -> PathwayResult | None:
    stage_started = time.perf_counter()
    try:
        result = await pathway_d_pattern(
            trigger.seed_signature,
            trigger.tenant_id,
            conn,
        )
        notes["pathways_run"].append("D")
        _append_pathway_timing(
            pathway_timings,
            "pathway_D",
            stage_started,
            **_pathway_counts(result),
        )
        return result
    except Exception as exc:
        _raise_if_postgres_error(exc)
        notes["pathways_skipped"].append({"pathway": "D", "reason": str(exc)})
        _append_pathway_timing(
            pathway_timings,
            "pathway_D",
            stage_started,
            skipped=True,
            reason=str(exc),
        )
        return None


async def _run_pathway_g(
    *,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    effective_seed_entities: list[dict[str, Any]],
    effective_scope_actors: list[UUID],
    notes: dict[str, Any],
    pathway_timings: list[dict[str, Any]],
) -> PathwayResult | None:
    seed_model_ids: list[UUID] = []
    if trigger.model_id is not None:
        seed_model_ids.append(trigger.model_id)
    for model_id in trigger.member_model_ids:
        if model_id not in seed_model_ids:
            seed_model_ids.append(model_id)
    g_seed_entities = [] if seed_model_ids else effective_seed_entities
    g_scope_actors = [] if seed_model_ids else effective_scope_actors
    stage_started = time.perf_counter()
    try:
        result = await pathway_g_model_edges(
            trigger.tenant_id,
            conn,
            seed_model_ids=seed_model_ids,
            seed_entity_ids=g_seed_entities,
            scope_actors=g_scope_actors,
            max_hops=min(max(trigger.max_hops, 0), 3),
        )
        notes["pathways_run"].append("G")
        _append_pathway_timing(
            pathway_timings,
            "pathway_G",
            stage_started,
            **_pathway_counts(result),
        )
        return result
    except Exception as exc:
        _raise_if_postgres_error(exc)
        notes["pathways_skipped"].append({"pathway": "G", "reason": str(exc)})
        _append_pathway_timing(
            pathway_timings,
            "pathway_G",
            stage_started,
            skipped=True,
            reason=str(exc),
        )
        return None


async def _merge_primary_results(
    *,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    cfg: RetrievalConfig,
    weights: dict[str, float],
    top_n: int,
    pathway_results: list[PathwayResult],
    notes: dict[str, Any],
    pathway_timings: list[dict[str, Any]],
) -> tuple[
    list[ModelRow],
    list[ObservationRow],
    dict[str, list],
    list[ResourceRow],
    dict[UUID, float],
]:
    stage_started = time.perf_counter()
    models, scores = _merge_and_rank_models(
        pathway_results,
        weights,
        top_n=top_n,
        scoring_mode=cfg.scoring_mode,
        rrf_k=cfg.rrf_k,
        recency_decay_half_life_days=cfg.recency_decay_half_life_days,
    )
    observations = _merge_observations(pathway_results)
    trigger_observations = await _fetch_trigger_observations(trigger, conn)
    if trigger_observations:
        seen_observation_ids = {o.id for o in trigger_observations}
        observations = [
            *trigger_observations,
            *[o for o in observations if o.id not in seen_observation_ids],
        ]
    acts = _merge_acts(pathway_results)
    resources = _merge_resources(pathway_results)
    _append_pathway_timing(
        pathway_timings,
        "merge_rank",
        stage_started,
        models=len(models),
        observations=len(observations),
        acts={k: len(v) for k, v in acts.items()},
        resources=len(resources),
        pathway_results=len(pathway_results),
    )

    notes["models_merged"] = len(models)
    notes["observations_merged"] = len(observations)
    notes["observation_policy"] = {
        "model_first_context_enabled": bool(cfg.model_first_context_enabled),
        "trigger_observations": len(trigger_observations),
        "historical_observations": max(
            0, len(observations) - len(trigger_observations)
        ),
        "historical_observation_cap": (
            int(cfg.historical_observation_cap)
            if cfg.model_first_context_enabled
            else int(cfg.temporal_max_observations)
        ),
    }
    notes["acts_merged"] = {k: len(v) for k, v in acts.items()}
    notes["resources_merged"] = len(resources)
    return models, observations, acts, resources, scores


async def _reconsolidate_primary_models(
    *,
    models: list[ModelRow],
    conn: asyncpg.Connection,
    models_repo: ModelsRepo | None,
    pathway_timings: list[dict[str, Any]],
    notes: dict[str, Any],
) -> list[ModelRow]:
    stage_started = time.perf_counter()
    if not models:
        _append_pathway_timing(
            pathway_timings,
            "reconsolidation",
            stage_started,
            skipped=True,
            models=0,
        )
        return models

    if models_repo is None:
        models_repo = ModelsRepo(pool=None)  # type: ignore[arg-type]
    reconsolidated = await models_repo.retrieve([m.id for m in models], conn=conn)
    by_id = {m.id: m for m in reconsolidated}
    notes["reconsolidated_count"] = len(reconsolidated)
    _append_pathway_timing(
        pathway_timings,
        "reconsolidation",
        stage_started,
        models=len(reconsolidated),
    )
    return [by_id.get(m.id, m) for m in models]


def _primary_pathway_fanout_enabled(
    *,
    cfg: RetrievalConfig,
    conn: asyncpg.Connection,
    read_pool: asyncpg.Pool | None,
) -> bool:
    if not bool(getattr(cfg, "primary_pathway_parallel_enabled", True)):
        return False
    if read_pool is None:
        return False
    in_transaction = getattr(conn, "is_in_transaction", None)
    if callable(in_transaction) and in_transaction():
        return False
    max_size = getattr(read_pool, "get_max_size", None)
    if callable(max_size):
        try:
            return int(max_size()) > 1
        except (TypeError, ValueError):
            return False
    return True


async def _run_primary_pathways_fanout(
    *,
    trigger: TriggerContext,
    cfg: RetrievalConfig,
    weights: dict[str, float],
    sage_policy: SageRetrievalPolicy | None,
    read_pool: asyncpg.Pool,
    read_fanout_budget: ReadFanoutBudget,
    embedder: OllamaClient | None,
    structural_read_fanout_enabled: bool,
    structural_read_fanout_min_seeds: int,
    structural_read_fanout_chunk_size: int,
    effective_seed_entities: list[dict[str, Any]],
    effective_scope_actors: list[UUID],
    t2_model_natural: str | None,
    t2_model_embedding: list[float] | None,
    notes: dict[str, Any],
    pathway_timings: list[dict[str, Any]],
) -> list[PathwayResult]:
    slots: list[tuple[str, Any]] = []

    def policy_allows(path: str) -> bool:
        return sage_policy is None or sage_policy.allows(path)

    async def run_projection(
        read_conn: asyncpg.Connection,
        local_notes: dict[str, Any],
        local_timings: list[dict[str, Any]],
    ) -> PathwayResult | None:
        return await _run_projection_context(
            trigger=trigger,
            conn=read_conn,
            cfg=cfg,
            effective_seed_entities=effective_seed_entities,
            effective_scope_actors=effective_scope_actors,
            notes=local_notes,
            pathway_timings=local_timings,
        )

    if policy_allows("projection_context"):
        slots.append(("projection_context", run_projection))

    if "A" in weights and policy_allows("A"):
        async def run_a(
            read_conn: asyncpg.Connection,
            local_notes: dict[str, Any],
            local_timings: list[dict[str, Any]],
        ) -> PathwayResult | None:
            return await _run_pathway_a(
                trigger=trigger,
                conn=read_conn,
                read_pool=read_pool,
                read_fanout_enabled=structural_read_fanout_enabled,
                read_fanout_min_seeds=structural_read_fanout_min_seeds,
                read_fanout_chunk_size=structural_read_fanout_chunk_size,
                read_fanout_budget=read_fanout_budget,
                effective_seed_entities=effective_seed_entities,
                effective_scope_actors=effective_scope_actors,
                notes=local_notes,
                pathway_timings=local_timings,
            )

        slots.append(("A", run_a))

    if "B" in weights and policy_allows("B"):
        async def run_b(
            read_conn: asyncpg.Connection,
            local_notes: dict[str, Any],
            local_timings: list[dict[str, Any]],
        ) -> PathwayResult | None:
            return await _run_pathway_b(
                trigger=trigger,
                conn=read_conn,
                cfg=cfg,
                embedder=embedder,
                sage_policy=sage_policy,
                effective_seed_entities=effective_seed_entities,
                effective_scope_actors=effective_scope_actors,
                t2_model_natural=t2_model_natural,
                t2_model_embedding=t2_model_embedding,
                notes=local_notes,
                pathway_timings=local_timings,
            )

        slots.append(("B", run_b))

    if "L" in weights and policy_allows("L"):
        async def run_l(
            read_conn: asyncpg.Connection,
            local_notes: dict[str, Any],
            local_timings: list[dict[str, Any]],
        ) -> PathwayResult | None:
            return await _run_pathway_l(
                trigger=trigger,
                conn=read_conn,
                cfg=cfg,
                effective_seed_entities=effective_seed_entities,
                effective_scope_actors=effective_scope_actors,
                t2_model_natural=t2_model_natural,
                notes=local_notes,
                pathway_timings=local_timings,
            )

        slots.append(("L", run_l))

    if "C" in weights and policy_allows("C"):
        async def run_c(
            read_conn: asyncpg.Connection,
            local_notes: dict[str, Any],
            local_timings: list[dict[str, Any]],
        ) -> PathwayResult | None:
            return await _run_pathway_c(
                trigger=trigger,
                conn=read_conn,
                cfg=cfg,
                effective_seed_entities=effective_seed_entities,
                effective_scope_actors=effective_scope_actors,
                notes=local_notes,
                pathway_timings=local_timings,
            )

        slots.append(("C", run_c))

    if "D" in weights and policy_allows("D"):
        async def run_d(
            read_conn: asyncpg.Connection,
            local_notes: dict[str, Any],
            local_timings: list[dict[str, Any]],
        ) -> PathwayResult | None:
            return await _run_pathway_d(
                trigger=trigger,
                conn=read_conn,
                notes=local_notes,
                pathway_timings=local_timings,
            )

        slots.append(("D", run_d))

    if "G" in weights and policy_allows("G"):
        async def run_g(
            read_conn: asyncpg.Connection,
            local_notes: dict[str, Any],
            local_timings: list[dict[str, Any]],
        ) -> PathwayResult | None:
            return await _run_pathway_g(
                trigger=trigger,
                conn=read_conn,
                effective_seed_entities=effective_seed_entities,
                effective_scope_actors=effective_scope_actors,
                notes=local_notes,
                pathway_timings=local_timings,
            )

        slots.append(("G", run_g))

    async def run_slot(name: str, runner: Any) -> _PathwayFanoutResult:
        local_notes: dict[str, Any] = {"pathways_run": [], "pathways_skipped": []}
        local_timings: list[dict[str, Any]] = []
        async with read_fanout_budget.connection() as read_conn:
            result = await runner(read_conn, local_notes, local_timings)
        return _PathwayFanoutResult(name, result, local_notes, local_timings)

    tasks: list[asyncio.Task[_PathwayFanoutResult]] = []
    async with asyncio.TaskGroup() as task_group:
        for name, runner in slots:
            tasks.append(task_group.create_task(run_slot(name, runner)))

    pathway_results: list[PathwayResult] = []
    for task in tasks:
        item = task.result()
        if item.name == "projection_context" and "projection_context" in item.notes:
            notes["projection_context"] = item.notes["projection_context"]
        notes["pathways_run"].extend(item.notes.get("pathways_run", []))
        notes["pathways_skipped"].extend(item.notes.get("pathways_skipped", []))
        pathway_timings.extend(item.timings)
        if item.result is not None:
            pathway_results.append(item.result)
    budget_snapshot = read_fanout_budget.snapshot()
    notes["primary_read_fanout_budget"] = {
        "max_concurrency": budget_snapshot.max_concurrency,
        "peak_in_use": budget_snapshot.peak_in_use,
        "acquired": budget_snapshot.acquired,
        "denied": budget_snapshot.denied,
    }
    return pathway_results


# ---------------------------------------------------------------------
# primary_retrieve
# ---------------------------------------------------------------------


async def primary_retrieve(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    models_repo: ModelsRepo | None = None,
    embedder: OllamaClient | None = None,
    read_pool: asyncpg.Pool | None = None,
    structural_read_fanout_enabled: bool = False,
    structural_read_fanout_min_seeds: int = 16,
    structural_read_fanout_chunk_size: int = 8,
    top_n: int = _DEFAULT_TOP_N,
    config: RetrievalConfig | None = None,
    sage_route_utilities: tuple[SageRouteUtility, ...] = (),
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
    _coerce_trigger_seed_occurred_at(trigger)
    weights = _trigger_weights(trigger.kind, cfg, subkind=trigger.subkind)
    if weights is None:
        raise ValidationError(
            f"unknown trigger kind {trigger.kind!r}",
            kind=trigger.kind,
        )

    pathway_results: list[PathwayResult] = []
    pathway_timings: list[dict[str, Any]] = []
    notes = _primary_notes(trigger, cfg, weights, pathway_timings)
    (
        effective_seed_entities,
        effective_scope_actors,
        t2_model_natural,
        t2_model_embedding,
    ) = await _prepare_effective_trigger_scope(
        trigger,
        conn,
        notes,
        pathway_timings,
    )
    weights, sage_policy = _plan_sage_primary_policy(
        trigger=trigger,
        cfg=cfg,
        weights=weights,
        effective_seed_entities=effective_seed_entities,
        effective_scope_actors=effective_scope_actors,
        notes=notes,
        route_utilities=sage_route_utilities,
    )
    notes["weights"] = dict(weights)

    if _primary_pathway_fanout_enabled(cfg=cfg, conn=conn, read_pool=read_pool):
        assert read_pool is not None
        read_fanout_budget = ReadFanoutBudget.from_pool(read_pool)
        pathway_results = await _run_primary_pathways_fanout(
            trigger=trigger,
            cfg=cfg,
            weights=weights,
            sage_policy=sage_policy,
            read_pool=read_pool,
            read_fanout_budget=read_fanout_budget,
            embedder=embedder,
            structural_read_fanout_enabled=structural_read_fanout_enabled,
            structural_read_fanout_min_seeds=structural_read_fanout_min_seeds,
            structural_read_fanout_chunk_size=structural_read_fanout_chunk_size,
            effective_seed_entities=effective_seed_entities,
            effective_scope_actors=effective_scope_actors,
            t2_model_natural=t2_model_natural,
            t2_model_embedding=t2_model_embedding,
            notes=notes,
            pathway_timings=pathway_timings,
        )
    else:
        if sage_policy is None or sage_policy.allows("projection_context"):
            projection_result = await _run_projection_context(
                trigger=trigger,
                conn=conn,
                cfg=cfg,
                effective_seed_entities=effective_seed_entities,
                effective_scope_actors=effective_scope_actors,
                notes=notes,
                pathway_timings=pathway_timings,
            )
            if projection_result is not None:
                pathway_results.append(projection_result)

        if "A" in weights and (sage_policy is None or sage_policy.allows("A")):
            result = await _run_pathway_a(
                trigger=trigger,
                conn=conn,
                read_pool=read_pool,
                read_fanout_enabled=structural_read_fanout_enabled,
                read_fanout_min_seeds=structural_read_fanout_min_seeds,
                read_fanout_chunk_size=structural_read_fanout_chunk_size,
                read_fanout_budget=None,
                effective_seed_entities=effective_seed_entities,
                effective_scope_actors=effective_scope_actors,
                notes=notes,
                pathway_timings=pathway_timings,
            )
            if result is not None:
                pathway_results.append(result)

        if "B" in weights and (sage_policy is None or sage_policy.allows("B")):
            result = await _run_pathway_b(
                trigger=trigger,
                conn=conn,
                cfg=cfg,
                embedder=embedder,
                sage_policy=sage_policy,
                effective_seed_entities=effective_seed_entities,
                effective_scope_actors=effective_scope_actors,
                t2_model_natural=t2_model_natural,
                t2_model_embedding=t2_model_embedding,
                notes=notes,
                pathway_timings=pathway_timings,
            )
            if result is not None:
                pathway_results.append(result)

        if "L" in weights and (sage_policy is None or sage_policy.allows("L")):
            result = await _run_pathway_l(
                trigger=trigger,
                conn=conn,
                cfg=cfg,
                effective_seed_entities=effective_seed_entities,
                effective_scope_actors=effective_scope_actors,
                t2_model_natural=t2_model_natural,
                notes=notes,
                pathway_timings=pathway_timings,
            )
            if result is not None:
                pathway_results.append(result)

        if "C" in weights and (sage_policy is None or sage_policy.allows("C")):
            result = await _run_pathway_c(
                trigger=trigger,
                conn=conn,
                cfg=cfg,
                effective_seed_entities=effective_seed_entities,
                effective_scope_actors=effective_scope_actors,
                notes=notes,
                pathway_timings=pathway_timings,
            )
            if result is not None:
                pathway_results.append(result)

        if "D" in weights and (sage_policy is None or sage_policy.allows("D")):
            result = await _run_pathway_d(
                trigger=trigger,
                conn=conn,
                notes=notes,
                pathway_timings=pathway_timings,
            )
            if result is not None:
                pathway_results.append(result)

        if "G" in weights and (sage_policy is None or sage_policy.allows("G")):
            result = await _run_pathway_g(
                trigger=trigger,
                conn=conn,
                effective_seed_entities=effective_seed_entities,
                effective_scope_actors=effective_scope_actors,
                notes=notes,
                pathway_timings=pathway_timings,
            )
            if result is not None:
                pathway_results.append(result)

    models, observations, acts, resources, scores = await _merge_primary_results(
        trigger=trigger,
        conn=conn,
        cfg=cfg,
        weights=weights,
        top_n=top_n,
        pathway_results=pathway_results,
        notes=notes,
        pathway_timings=pathway_timings,
    )
    models = await _reconsolidate_primary_models(
        models=models,
        conn=conn,
        models_repo=models_repo,
        pathway_timings=pathway_timings,
        notes=notes,
    )
    if sage_policy is not None:
        notes["sage_retrieval_policy_observation"] = summarize_primary_observation(
            policy=sage_policy,
            notes=notes,
            models=len(models),
            observations=len(observations),
        ).notes()

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
