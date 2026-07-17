"""
services/retrieval/assembler.py — context assembler + access control
stub.

Spec reference: ARCHITECTURE-FINAL.md §8 "Context assembler" + §26
"Access control". BUILD-PLAN reference: §4 Prompt 3.A item 4.
RA-4 reference: RETRIEVAL-DESIGN-AUDIT §7 args 1-2 (MMR diversity +
don't-truncate-mid-item).

Converts a RetrievalResult into a ContextBundle:
  - Applies access control (Wave 3-A stub: visibility check on
    `visible_to_subjects` + `scope_actors` membership; real roles /
    materialized views are Wave 5-A).
  - Compresses to configured size budgets:
        * observations  ≤ model-gap fallback / explicit observation caps
        * models        ≤ 24 by default
        * acts          ≤ 10 (across goals + commitments + decisions
                             combined, deviation (c) documented below)
        * resources     ≤ 5
  - Attaches a customer_context dict if any commitment has
    `external_counterparty_ref` set.

Compression ordering (deviation (c) BUILD-LOG):
  - Models — by `model_scores` descending (from primary_retrieve).
    Tie-break on activation descending.
  - Observations — in model-first mode, raw rows are suppressed when
    selected Models provide usable context. If Models are absent or the
    policy is overridden, current trigger observations come first, then
    a small historical evidence tail by occurred_at descending.
  - Acts — we flatten the three kinds into one list and take the top
    10 by last_state_change_at / created_at descending. The cap of 10
    is per BUILD-PLAN (not 10 per kind).
  - Resources — prefer those with an active customer_commitments
    linkage first; then by last_updated_at
    descending.

MMR diversity (RA-4): `mmr_select(items_with_scores, budget_tokens,
lambda_diversity)` is a public helper that combines an item's
relevance score with a diversity penalty (1 - max cosine similarity
to already-selected). Items that don't fit the remaining budget are
SKIPPED — we never truncate mid-item (audit §7 arg 2).
"""
from __future__ import annotations

import asyncio
import math
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Protocol, Sequence
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError
from lib.shared.types import (
    CommitmentRow,
    ModelRow,
    ObservationRow,
    ResourceRow,
)

from .config import CONFIG, RetrievalConfig
from .primary import RetrievalResult
from .read_fanout import ReadFanoutBudget


_BUDGET_OBSERVATIONS = 12
_BUDGET_MODELS = 24
_BUDGET_ACTS_TOTAL = 10   # combined across goals + commitments + decisions
_BUDGET_RESOURCES = 5

# RA-4 default MMR diversity trade-off. 0.5 balances relevance and
# diversity; 1.0 reduces to pure greedy-by-score; 0.0 prefers pure
# novelty at the expense of relevance.
_MMR_LAMBDA_DEFAULT = 0.5

# Keep the strongest retrieval hits before applying diversity pressure.
# MMR is a context-shaping tool, not permission to drop the best
# evidence; these anchors protect high-value graph/structural hits
# while the remaining slots still diversify.
_MMR_RELEVANCE_ANCHOR_COUNT = 5
_MMR_GRAPH_ANCHOR_COUNT = 3

# Cheap-and-correct token estimator used for the MMR path (FU-1). Real
# tokenization would cost a per-row tokenizer call; 4 chars/token is
# the industry rule-of-thumb and is well within the accuracy needed
# for a context-budget bound. Minimum 1 token so items with no
# `natural` text never get skipped as "too big".
_CHARS_PER_TOKEN = 4
_WS_RE = re.compile(r"\s+")
_LEXICAL_ANCHOR_RE = re.compile(r"[a-z0-9][a-z0-9_.:#/-]{3,}", re.I)
_HEX_RE = re.compile(r"\b[0-9a-f]{7,40}\b", re.I)
_NUMBER_RE = re.compile(r"[$]?[0-9][0-9,]*([.][0-9]+)?")
_URL_RE = re.compile(r"https?://[^\s)]+", re.I)
_RAW_REOPENING_REASON_CODES = frozenset(
    {"uncertainty", "contradiction", "correction", "provenance"}
)
_LEXICAL_ANCHOR_STOPWORDS = frozenset(
    {
        "about", "after", "again", "against", "batch", "before", "being",
        "between", "company", "could", "current", "evidence", "facet",
        "facets", "from", "have", "into", "model", "observation", "only",
        "other", "signal", "signals", "subject", "their", "there", "these",
        "this", "those", "through", "under", "using", "where", "which",
        "while", "with", "would",
    }
)


def _estimate_model_tokens(m: ModelRow) -> int:
    """Rough token estimate for a Model row. Uses the `natural` text
    (the LLM-facing narrative) as the token cost proxy. Callers pack
    these estimates into `mmr_select` to stay under
    `context_budget_tokens`."""
    nat = getattr(m, "natural", None) or ""
    # Fall back to proposition if natural is empty.
    if not nat:
        nat = getattr(m, "proposition", None) or ""
    return max(1, len(str(nat)) // _CHARS_PER_TOKEN)


def _model_selection_notes(
    retrieval_result: RetrievalResult,
    *,
    visible_models: list[ModelRow],
    selected_models: list[ModelRow],
) -> dict[str, Any]:
    """Summarize which retrieved Models survived into the context.

    This is intentionally lightweight prompt-survival telemetry. It
    lets tests and production traces answer the question that matters
    most after retrieval: did a pathway's useful candidates actually
    reach the LLM-facing bundle?
    """
    retrieved_ids = [m.id for m in retrieval_result.models]
    visible_ids = [m.id for m in visible_models]
    selected_ids = [m.id for m in selected_models]
    visible_set = set(visible_ids)
    selected_set = set(selected_ids)

    pathway_survival: dict[str, dict[str, Any]] = {}
    for pr in retrieval_result.pathway_results:
        pathway = str(pr.source_pathway)
        candidate_ids = []
        seen: set[UUID] = set()
        for m in pr.models:
            if m.id in seen:
                continue
            seen.add(m.id)
            candidate_ids.append(m.id)
        selected_from_pathway = [mid for mid in candidate_ids if mid in selected_set]
        visible_from_pathway = [mid for mid in candidate_ids if mid in visible_set]
        pathway_survival[pathway] = {
            "candidate_count": len(candidate_ids),
            "visible_count": len(visible_from_pathway),
            "selected_count": len(selected_from_pathway),
            "dropped_after_visibility_count": (
                len(visible_from_pathway) - len(selected_from_pathway)
            ),
            "selected_model_ids": [str(mid) for mid in selected_from_pathway[:20]],
        }

    return {
        "retrieved_count": len(retrieved_ids),
        "visible_count": len(visible_ids),
        "selected_count": len(selected_ids),
        "dropped_count": len(visible_ids) - len(selected_ids),
        "selected_model_ids": [str(mid) for mid in selected_ids],
        "dropped_model_ids": [
            str(mid) for mid in visible_ids if mid not in selected_set
        ],
        "pathway_survival": pathway_survival,
    }


def _trigger_observation_ids(retrieval_result: RetrievalResult) -> set[UUID]:
    trigger = retrieval_result.trigger
    ids: set[UUID] = set()
    if trigger.observation_id is not None:
        ids.add(trigger.observation_id)
    ids.update(trigger.observation_ids or [])
    return ids


def _is_explicit_t1_event_batch(retrieval_result: RetrievalResult) -> bool:
    trigger = retrieval_result.trigger
    if trigger.kind != "T1":
        return False
    signature = trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    return bool(
        trigger.subkind == "event_batch"
        or signature.get("signal_type") == "event_batch"
        or signature.get("batch") is True
        or isinstance(signature.get("batch_signal_fragments"), list)
    )


def _observation_signature(observation: ObservationRow) -> tuple[str, str, str, str]:
    source = str(getattr(observation, "source_channel", "") or "unknown")
    actor = str(getattr(observation, "source_actor_ref", "") or "unknown_actor")
    content = getattr(observation, "content", None)
    thread = ""
    if isinstance(content, dict):
        for key in (
            "thread_id",
            "conversation_id",
            "channel_id",
            "issue_id",
            "pull_request_id",
            "file_id",
        ):
            raw = content.get(key)
            if raw is not None and str(raw).strip():
                thread = f"{key}:{str(raw).strip()}"
                break
    text = str(getattr(observation, "content_text", "") or "").casefold()
    text = _URL_RE.sub("<url>", text)
    text = _HEX_RE.sub("<hex>", text)
    text = _NUMBER_RE.sub("<num>", text)
    text = _WS_RE.sub(" ", text).strip()[:96]
    return source, actor, thread, text


def _select_diverse_observation_floor(
    observations: list[ObservationRow],
    *,
    floor: int,
    source_floor: int,
    total_cap: int,
) -> list[ObservationRow]:
    """Pick a source/actor/text-diverse raw evidence floor for event batches."""
    limit = min(max(0, int(floor)), max(0, int(total_cap)))
    if limit <= 0 or not observations:
        return []

    by_source: dict[str, list[ObservationRow]] = defaultdict(list)
    for observation in observations:
        source = str(getattr(observation, "source_channel", "") or "unknown")
        by_source[source].append(observation)

    selected: list[ObservationRow] = []
    selected_ids: set[UUID] = set()
    selected_signatures: set[tuple[str, str, str, str]] = set()
    source_counts: dict[str, int] = defaultdict(int)
    sources = sorted(
        by_source,
        key=lambda source: (-len(by_source[source]), source),
    )

    def try_add(observation: ObservationRow, *, enforce_signature: bool) -> bool:
        if len(selected) >= limit:
            return False
        if observation.id in selected_ids:
            return False
        source = str(getattr(observation, "source_channel", "") or "unknown")
        per_source_cap = max(1, int(source_floor)) if len(sources) > 1 else limit
        if source_counts[source] >= per_source_cap and len(selected) < limit:
            if any(source_counts[other] < per_source_cap for other in sources):
                return False
        signature = _observation_signature(observation)
        if enforce_signature and signature in selected_signatures:
            return False
        selected.append(observation)
        selected_ids.add(observation.id)
        selected_signatures.add(signature)
        source_counts[source] += 1
        return True

    for enforce_signature in (True, False):
        made_progress = True
        while made_progress and len(selected) < limit:
            made_progress = False
            for source in sources:
                for observation in by_source[source]:
                    if try_add(observation, enforce_signature=enforce_signature):
                        made_progress = True
                        break
                if len(selected) >= limit:
                    break
    return selected


def _batch_semantic_memory_state(
    *,
    selected_model_count: int,
    selected_model_quality_mass: float,
    trigger_candidate_count: int,
    configured_floor: int,
    total_cap: int,
) -> dict[str, Any]:
    """Describe how far an event batch can lean on established Models.

    A single retrieved Model is not sufficient evidence that semantic memory
    covers a batch.  Use a continuous sufficiency score against a bounded
    coverage target so cold-start batches retain broad raw evidence while a
    mature model layer keeps only a small verification sample.
    """
    trigger_count = max(0, int(trigger_candidate_count))
    model_count = max(0, int(selected_model_count))
    quality_mass = max(0.0, min(float(model_count), selected_model_quality_mass))
    target_models = max(4, min(12, (trigger_count + 1) // 2))
    sufficiency = min(1.0, quality_mass / target_models)
    if sufficiency <= 0.0:
        maturity = "cold_start"
    elif sufficiency < 0.75:
        maturity = "developing"
    else:
        maturity = "mature"

    configured = min(
        max(0, int(configured_floor)),
        max(0, int(total_cap)),
        trigger_count,
    )
    verification_floor = min(2, configured)
    effective = verification_floor + round(
        (configured - verification_floor) * (1.0 - sufficiency)
    )
    return {
        "maturity": maturity,
        "semantic_memory_sufficiency": round(sufficiency, 4),
        "semantic_memory_target_models": target_models,
        "selected_model_quality_mass": round(quality_mass, 4),
        "configured_raw_floor": configured,
        "effective_raw_floor": max(0, min(configured, effective)),
    }


def _selected_model_quality_mass(
    selected_models: list[ModelRow],
    model_scores: dict[UUID, float],
    *,
    score_ceiling: float | None = None,
) -> float:
    """Return an effective count that discounts weak or unreliable Models."""
    positive_scores = [
        max(0.0, float(model_scores.get(model.id, 0.0)))
        for model in selected_models
        if float(model_scores.get(model.id, 0.0)) > 0.0
    ]
    if not positive_scores:
        return 0.0
    strongest = max(positive_scores)
    normalization_ceiling = max(
        strongest,
        float(score_ceiling or 0.0),
    )
    quality_mass = 0.0
    for model in selected_models:
        score_ratio = max(
            0.0,
            min(
                1.0,
                float(model_scores.get(model.id, 0.0)) / normalization_ceiling,
            ),
        )
        # Candidates far below the strongest retrieval result are reservoir
        # tail, not evidence that semantic memory covers this batch.
        if score_ratio < 0.2:
            continue
        reliability = (
            max(0.0, min(1.0, float(model.confidence)))
            * max(0.0, min(1.0, float(model.activation)))
        ) ** 0.5
        quality_mass += score_ratio * reliability
    return quality_mass


def _retrieval_score_ceiling(retrieval_result: RetrievalResult) -> float | None:
    notes = retrieval_result.notes if isinstance(retrieval_result.notes, dict) else {}
    config = notes.get("config_summary")
    weights = notes.get("weights")
    if not isinstance(config, dict) or not isinstance(weights, dict):
        return None
    pathway_weight = sum(
        max(0.0, float(weights.get(pathway, 0.0)))
        for pathway in ("A", "M", "B", "L", "C", "D", "G")
    )
    if str(config.get("scoring_mode", "")).lower() == "rrf":
        rrf_k = max(1, int(config.get("rrf_k", 60)))
        # Activation and provenance each retain a 0.5 RRF prior.
        return (pathway_weight + 1.0) / (rrf_k + 1.0)
    if str(config.get("scoring_mode", "")).lower() == "linear":
        return max(1.0, pathway_weight)
    return None


def _raw_evidence_reopening(
    *,
    selected: list[ObservationRow],
    reason_codes: list[str],
    memory_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    note: dict[str, Any] = {
        "opened": bool(selected),
        "reason_codes": list(reason_codes) if selected else [],
        "selected_observation_ids": [str(row.id) for row in selected],
    }
    if memory_state is not None:
        note.update(memory_state)
    return note


def _requested_raw_reopening_reasons(
    retrieval_result: RetrievalResult,
) -> list[str]:
    signature = retrieval_result.trigger.seed_signature
    if not isinstance(signature, dict):
        return []
    raw = signature.get("raw_observation_reopening_reasons") or []
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    return sorted(
        {
            str(reason).strip().lower()
            for reason in raw
            if str(reason).strip().lower() in _RAW_REOPENING_REASON_CODES
        }
    )


def _select_observations(
    retrieval_result: RetrievalResult,
    observations: list[ObservationRow],
    *,
    cfg: RetrievalConfig,
    budget_observations: int,
    explicit_budget: bool,
    selected_model_count: int = 0,
    selected_model_quality_mass: float | None = None,
) -> tuple[list[ObservationRow], dict[str, Any]]:
    observations.sort(key=lambda o: (o.occurred_at, o.id), reverse=True)
    if not cfg.model_first_context_enabled:
        selected = observations[:budget_observations]
        return selected, {
            "model_first_context_enabled": False,
            "retrieved_count": len(observations),
            "selected_count": len(selected),
            "selected_trigger_count": 0,
            "selected_historical_count": len(selected),
            "trigger_candidate_count": 0,
            "historical_candidate_count": len(observations),
            "trigger_cap": 0,
            "historical_cap": budget_observations,
            "observation_context_mode": "legacy",
            "observation_context_min_models": 0,
            "selected_model_count": int(selected_model_count),
            "model_context_sufficient": False,
            "suppressed_reason": None,
            "raw_evidence_reopening": _raw_evidence_reopening(
                selected=selected,
                reason_codes=["legacy_observation_context_mode"],
            ),
        }

    trigger_ids = _trigger_observation_ids(retrieval_result)
    trigger_observations: list[ObservationRow] = []
    historical_observations: list[ObservationRow] = []
    for observation in observations:
        if observation.id in trigger_ids:
            trigger_observations.append(observation)
        else:
            historical_observations.append(observation)

    mode = str(getattr(cfg, "observation_context_mode", "model_gap")).strip().lower()
    if mode not in {"always", "model_gap", "none"}:
        mode = "model_gap"
    min_models = max(0, int(getattr(cfg, "observation_context_min_models", 1)))
    quality_mass = (
        float(selected_model_count)
        if selected_model_quality_mass is None
        else max(0.0, float(selected_model_quality_mass))
    )
    model_context_sufficient = quality_mass >= max(1, min_models)
    suppressed_reason: str | None = None
    if mode == "none":
        suppressed_reason = "mode_none"
    elif mode == "model_gap" and model_context_sufficient:
        suppressed_reason = "model_context_sufficient"
    if suppressed_reason is not None:
        floor_selected: list[ObservationRow] = []
        memory_state: dict[str, Any] | None = None
        if (
            suppressed_reason == "model_context_sufficient"
            and _is_explicit_t1_event_batch(retrieval_result)
        ):
            memory_state = _batch_semantic_memory_state(
                selected_model_count=selected_model_count,
                selected_model_quality_mass=quality_mass,
                trigger_candidate_count=len(trigger_observations),
                configured_floor=int(
                    getattr(cfg, "t1_event_batch_raw_observation_floor", 0)
                ),
                total_cap=budget_observations,
            )
            floor_selected = _select_diverse_observation_floor(
                trigger_observations,
                floor=int(memory_state["effective_raw_floor"]),
                source_floor=int(getattr(cfg, "t1_event_batch_raw_source_floor", 0)),
                total_cap=budget_observations,
            )
        if floor_selected:
            requested_reasons = _requested_raw_reopening_reasons(retrieval_result)
            historical_reopened = (
                historical_observations[
                    : min(
                        len(historical_observations),
                        max(0, budget_observations - len(floor_selected)),
                        max(
                            0,
                            int(getattr(cfg, "historical_observation_cap", 0)),
                        ),
                    )
                ]
                if requested_reasons
                else []
            )
            selected = [*floor_selected, *historical_reopened]
            reason = (
                "fresh_trigger_verification_sample"
                if memory_state
                and memory_state["semantic_memory_sufficiency"] >= 0.75
                else "semantic_memory_partial_batch_coverage"
            )
            return selected, {
                "model_first_context_enabled": True,
                "retrieved_count": len(observations),
                "selected_count": len(selected),
                "selected_trigger_count": len(floor_selected),
                "selected_historical_count": len(historical_reopened),
                "trigger_candidate_count": len(trigger_observations),
                "historical_candidate_count": len(historical_observations),
                "trigger_cap": len(floor_selected),
                "historical_cap": len(historical_reopened),
                "dropped_trigger_count": max(
                    0, len(trigger_observations) - len(floor_selected)
                ),
                "dropped_historical_count": max(
                    0, len(historical_observations) - len(historical_reopened)
                ),
                "observation_context_mode": mode,
                "observation_context_min_models": min_models,
                "selected_model_count": int(selected_model_count),
                "model_context_sufficient": bool(model_context_sufficient),
                "suppressed_reason": None,
                "floor_reason": "explicit_t1_event_batch_raw_evidence_floor",
                "floor_requested": int(
                    getattr(cfg, "t1_event_batch_raw_observation_floor", 0)
                ),
                "source_floor": int(
                    getattr(cfg, "t1_event_batch_raw_source_floor", 0)
                ),
                "raw_evidence_reopening": _raw_evidence_reopening(
                    selected=selected,
                    reason_codes=[reason, *requested_reasons],
                    memory_state=memory_state,
                ),
            }
        return [], {
            "model_first_context_enabled": True,
            "retrieved_count": len(observations),
            "selected_count": 0,
            "selected_trigger_count": 0,
            "selected_historical_count": 0,
            "trigger_candidate_count": len(trigger_observations),
            "historical_candidate_count": len(historical_observations),
            "trigger_cap": 0,
            "historical_cap": 0,
            "dropped_trigger_count": len(trigger_observations),
            "dropped_historical_count": len(historical_observations),
            "observation_context_mode": mode,
            "observation_context_min_models": min_models,
            "selected_model_count": int(selected_model_count),
            "model_context_sufficient": bool(model_context_sufficient),
            "suppressed_reason": suppressed_reason,
            "raw_evidence_reopening": _raw_evidence_reopening(
                selected=[],
                reason_codes=[],
                memory_state=memory_state,
            ),
        }

    trigger_cap = min(
        max(0, int(cfg.trigger_observation_cap)),
        max(0, int(budget_observations)),
    )
    historical_cap = min(
        max(0, int(cfg.historical_observation_cap)),
        max(0, int(budget_observations)),
    )
    selected_trigger = trigger_observations[:trigger_cap]
    historical_cap = min(
        historical_cap,
        max(0, int(budget_observations) - len(selected_trigger)),
    )
    selected_historical = historical_observations[:historical_cap]
    selected = [*selected_trigger, *selected_historical]
    return selected, {
        "model_first_context_enabled": True,
        "retrieved_count": len(observations),
        "selected_count": len(selected),
        "selected_trigger_count": len(selected_trigger),
        "selected_historical_count": len(selected_historical),
        "trigger_candidate_count": len(trigger_observations),
        "historical_candidate_count": len(historical_observations),
        "trigger_cap": trigger_cap,
        "historical_cap": historical_cap,
        "observation_context_mode": mode,
        "observation_context_min_models": min_models,
        "selected_model_count": int(selected_model_count),
        "model_context_sufficient": bool(model_context_sufficient),
        "suppressed_reason": None,
        "dropped_trigger_count": max(
            0, len(trigger_observations) - len(selected_trigger)
        ),
        "dropped_historical_count": max(
            0, len(historical_observations) - len(selected_historical)
        ),
        "raw_evidence_reopening": _raw_evidence_reopening(
            selected=selected,
            reason_codes=(
                ["semantic_memory_gap"]
                if mode == "model_gap"
                else ["observation_context_always"]
            ),
        ),
    }


def _top_pathway_model_ids(
    retrieval_result: RetrievalResult,
    pathway: str,
    *,
    limit: int,
) -> list[UUID]:
    """Return de-duped Model ids from a pathway in pathway rank order."""
    out: list[UUID] = []
    seen: set[UUID] = set()
    for pr in retrieval_result.pathway_results:
        if str(pr.source_pathway) != pathway:
            continue
        for model in pr.models:
            if model.id in seen:
                continue
            seen.add(model.id)
            out.append(model.id)
            if len(out) >= limit:
                return out
    return out


@dataclass
class _MMRModelWrapper:
    """Adapter: `ModelRow` lacks `score`/`tokens`; MMR needs both.
    Wraps the row alongside its retrieval score + token estimate +
    embedding (already on the row)."""
    model: ModelRow
    score: float
    tokens: int
    embedding: Any  # list[float] | None
    # Expose id for tests / debugging.
    @property
    def id(self) -> UUID:
        return self.model.id


class AssemblerError(CompanyOSError):
    default_code = "assembler_error"


# ---------------------------------------------------------------------
# RA-4 — MMR (Maximal Marginal Relevance) selection
# ---------------------------------------------------------------------


class _HasScoreTokensEmbedding(Protocol):
    score: float
    tokens: int
    embedding: Sequence[float] | None


def _cosine_similarity(a: Sequence[float] | None, b: Sequence[float] | None) -> float:
    """Cosine similarity in [-1, 1]. Returns 0 when either vector is
    missing or zero-norm. Safe for short inputs (defensive numerics
    for unit tests with tiny dims)."""
    if a is None or b is None:
        return 0.0
    na = 0.0
    nb = 0.0
    dot = 0.0
    for x, y in zip(a, b):
        dot += float(x) * float(y)
        na += float(x) * float(x)
        nb += float(y) * float(y)
    if na == 0.0 or nb == 0.0:
        return 0.0
    denom = math.sqrt(na) * math.sqrt(nb)
    if denom == 0.0:
        return 0.0
    return dot / denom


def mmr_select(
    items_with_scores: Iterable[Any],
    budget_tokens: int,
    *,
    lambda_diversity: float = _MMR_LAMBDA_DEFAULT,
) -> list[Any]:
    """
    Maximal Marginal Relevance selection under a token budget.

    Each item must expose:
      - `score` (float): relevance score
      - `tokens` (int): the item's size in tokens
      - `embedding` (sequence[float] | None): used for the diversity
        penalty (optional; missing embeddings treated as "dissimilar
        to everything" → diversity penalty contributes 0)

    Procedure:
      1. Sort items by relevance DESC.
      2. Pick the highest-scoring item that fits remaining budget.
      3. For each remaining item compute
             mmr = λ·score - (1-λ)·max_sim_to_selected
         Pick the best-mmr item that fits budget.
      4. Continue until no remaining item fits or all consumed.

    Items that don't fit remaining budget are SKIPPED (never
    truncated mid-item, per RETRIEVAL-DESIGN-AUDIT §7 arg 2).

    Edge cases:
      - λ=1.0 reduces to pure greedy-by-score.
      - λ=0.0 maximizes diversity (ignores relevance after the first
        pick).
      - Non-positive budget returns []; any item with tokens==0 is
        skipped (ambiguous — would fit an empty budget infinitely
        many times).

    Performance: pre-normalizes the embedding matrix once, then
    incrementally tracks per-remaining-item `max_sim_to_selected` so
    each pick costs O(n_remaining) rather than O(n_remaining *
    n_selected). 100 items / 100K budget completes in ~1-2ms.
    """
    if budget_tokens <= 0:
        return []
    if not (0.0 <= lambda_diversity <= 1.0):
        raise ValueError(
            f"lambda_diversity must be in [0, 1]; got {lambda_diversity}"
        )

    # Materialize and sort by score desc.
    items = sorted(
        items_with_scores,
        key=lambda it: -(float(getattr(it, "score", 0.0) or 0.0)),
    )
    n = len(items)
    if n == 0:
        return []

    # Pre-extract per-item arrays. Use numpy where possible.
    try:
        import numpy as _np  # local import — keeps assembler import
                              # cheap when MMR isn't called.
    except ImportError:
        _np = None

    scores = [float(getattr(it, "score", 0.0) or 0.0) for it in items]
    tokens_arr = [int(getattr(it, "tokens", 0) or 0) for it in items]

    # Build embedding matrix; rows of zeros mark "missing embedding".
    embeddings_raw = [getattr(it, "embedding", None) for it in items]
    has_emb = [emb is not None for emb in embeddings_raw]
    emb_matrix = None
    if _np is not None and any(has_emb):
        # Determine common dim from first non-None embedding.
        dim = next((len(list(e)) for e in embeddings_raw if e is not None), 0)
        if dim > 0:
            emb_matrix = _np.zeros((n, dim), dtype=_np.float32)
            for i, e in enumerate(embeddings_raw):
                if e is None:
                    continue
                ev = list(e)
                if len(ev) != dim:
                    # Mismatched dim — treat as missing.
                    has_emb[i] = False
                    continue
                emb_matrix[i, :] = _np.asarray(ev, dtype=_np.float32)
            # Normalize rows (safe handling of zero rows).
            norms = _np.linalg.norm(emb_matrix, axis=1, keepdims=True)
            norms[norms == 0.0] = 1.0
            emb_matrix = emb_matrix / norms

    # Track which indices remain.
    remaining_mask = [True] * n
    max_sim = [0.0] * n  # current max sim to any selected item
    used_tokens = 0
    selected_indices: list[int] = []

    def _fits(idx: int) -> bool:
        tk = tokens_arr[idx]
        return tk > 0 and used_tokens + tk <= budget_tokens

    # First pick: highest-score feasible item.
    first = None
    for i in range(n):
        if remaining_mask[i] and _fits(i):
            first = i
            break
    if first is None:
        return []
    selected_indices.append(first)
    remaining_mask[first] = False
    used_tokens += tokens_arr[first]

    # Update max_sim using the chosen one.
    if emb_matrix is not None and has_emb[first]:
        sims = emb_matrix @ emb_matrix[first]
        for i in range(n):
            if not remaining_mask[i] or not has_emb[i]:
                continue
            s = float(sims[i])
            if s > max_sim[i]:
                max_sim[i] = s

    # Subsequent picks.
    while True:
        best_idx = -1
        best_mmr = -float("inf")
        for i in range(n):
            if not remaining_mask[i]:
                continue
            if not _fits(i):
                continue
            mmr = lambda_diversity * scores[i] - (1.0 - lambda_diversity) * max_sim[i]
            if mmr > best_mmr:
                best_mmr = mmr
                best_idx = i
        if best_idx < 0:
            break
        selected_indices.append(best_idx)
        remaining_mask[best_idx] = False
        used_tokens += tokens_arr[best_idx]
        # Incrementally fold the new pick into max_sim.
        if emb_matrix is not None and has_emb[best_idx]:
            sims = emb_matrix @ emb_matrix[best_idx]
            for i in range(n):
                if not remaining_mask[i] or not has_emb[i]:
                    continue
                s = float(sims[i])
                if s > max_sim[i]:
                    max_sim[i] = s

    return [items[i] for i in selected_indices]


@dataclass
class AccessContext:
    """
    Thin access-control context. Wave 3-A stub — only `tenant_id` +
    `requestor_actor_id` + `roles` are used. Real role-based
    enforcement / materialized visibility views arrive in Wave 5-A.
    """

    tenant_id: UUID
    requestor_actor_id: UUID | None = None
    roles: list[str] = field(default_factory=list)


@dataclass
class ContextBundle:
    """
    The caller-facing return. Size bounds are hard caps (not target
    budgets); items over the cap are dropped (ordered by score).

    `customer_context` is a dict of customer_resource_id → customer summary
    OR None when no commitment in `acts_summary` has an
    `external_counterparty_ref`.

    `access_redactions` is the count of Models filtered out for
    visibility; the caller uses this for observability.

    `topology_context` is retained as a compatibility field. Active
    topology now persists relationship/situation candidates and sends
    their member_model_ids through the trigger payload; the assembler no
    longer reads accepted-memory neighborhood tables for prompt context.
    """

    observations: list[ObservationRow] = field(default_factory=list)
    models: list[ModelRow] = field(default_factory=list)
    acts_summary: dict[str, list] = field(
        default_factory=lambda: {"goals": [], "commitments": [], "decisions": []}
    )
    resources_summary: list[ResourceRow] = field(default_factory=list)
    customer_context: dict[str, Any] | None = None
    topology_context: dict[str, Any] | None = None
    access_redactions: int = 0
    notes: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------
# Access control — Wave 5-A full impl (replaces Wave 3-A stub)
# ---------------------------------------------------------------------


def _model_is_visible_fast(
    model: ModelRow,
    access: AccessContext,
) -> bool:
    """Fast in-memory pre-check for Models the caller loaded.

    Matches the cheap clauses in `services.platform.access_control.checks`:
      - system identity (requestor None) bypasses all checks.
      - visible_to_subjects=True → visible.
      - scope_actors membership → visible.

    Additional clauses (pattern-scope via actor_visible_{commitments,
    goals}, admin/leadership override) require a DB round-trip and
    live in `_filter_models_via_db` below. The two functions together
    implement the full §26 Layer-5 rule-set; `_filter_models_via_db`
    is the authoritative one when there are any models that fail the
    fast path.
    """
    if access.requestor_actor_id is None:
        return True
    if model.visible_to_subjects:
        return True
    return access.requestor_actor_id in model.scope_actors


async def _filter_models_via_db(
    models: list[ModelRow],
    access: AccessContext,
    conn: asyncpg.Connection,
) -> tuple[list[ModelRow], int, dict[str, int]]:
    """Run the full Wave-5-A can_read check against each Model that
    failed the fast in-memory pre-check.

    Returns (visible, redacted_count, reason_counts). `reason_counts`
    groups denial reasons for observability (BUILD-PLAN §6 "Count
    redactions per filter kind").
    """
    from services.platform.access_control.checks import can_read  # local import

    if access.requestor_actor_id is None:
        # System identity sees everything in the tenant.
        return models, 0, {}
    visible: list[ModelRow] = []
    redactions = 0
    reasons: dict[str, int] = {}
    for m in models:
        if _model_is_visible_fast(m, access):
            visible.append(m)
            continue
        # Fast path said no — consult full check (handles pattern-scope
        # and admin/leadership overrides).
        entity = {
            "kind": "model",
            "id": m.id,
            "tenant_id": m.tenant_id,
            "visible_to_subjects": m.visible_to_subjects,
            "scope_actors": list(m.scope_actors),
            "scope_entities": m.scope_entities,
        }
        decision = await can_read(
            access.requestor_actor_id,
            entity,
            conn=conn,
            tenant_id=access.tenant_id,
        )
        if decision.allowed:
            visible.append(m)
        else:
            redactions += 1
            reasons[decision.reason] = reasons.get(decision.reason, 0) + 1
    return visible, redactions, reasons


# ---------------------------------------------------------------------
# Customer-commitment traversal
# ---------------------------------------------------------------------


async def _compute_customer_context(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    commitments: list[CommitmentRow],
) -> dict[str, Any] | None:
    """
    For each Commitment with `external_counterparty_ref` set, compute
    the customer summary: linked commitments and at-risk commitment ids.

    Returns None if no commitment has a counterparty ref.
    """
    customer_commits: dict[UUID, list[UUID]] = {}
    # The ref might point at a customer via JSONB shape
    # `{"type": "customer_resource", "id": "<uuid>"}` OR directly hold
    # the customer id. We prefer the canonical shape. We also look up
    # customer_commitments rows directly as a cross-check.
    for c in commitments:
        if c.external_counterparty_ref is None:
            continue
        ref = c.external_counterparty_ref
        if isinstance(ref, dict):
            # Canonical: {"type": "customer_resource", "id": "<uuid>"}
            if ref.get("type") in ("customer_resource", "customer"):
                raw_id = ref.get("id")
                if raw_id is not None:
                    try:
                        customer_id = UUID(str(raw_id))
                        customer_commits.setdefault(customer_id, []).append(c.id)
                    except (ValueError, TypeError):
                        pass

    # Cross-check via the customer_commitments table; add discovered
    # Customer Resources that the canonical ref might have missed.
    commit_ids = [c.id for c in commitments]
    if commit_ids:
        rows = await conn.fetch(
            """
            SELECT customer_resource_id, commitment_id
            FROM customer_commitments
            WHERE commitment_id = ANY($1::uuid[])
            """,
            commit_ids,
        )
        for r in rows:
            cust = r["customer_resource_id"]
            cid = r["commitment_id"]
            customer_commits.setdefault(cust, []).append(cid)

    if not customer_commits:
        return None

    summaries: list[dict[str, Any]] = []
    for customer_id, cids in customer_commits.items():
        at_risk: list[UUID] = []
        for c in commitments:
            if c.id in cids and c.state in {"blocked", "paused", "doneunverified"}:
                at_risk.append(c.id)
        summaries.append(
            {
                "customer_resource_id": customer_id,
                "linked_commitment_ids": [str(x) for x in cids],
                "at_risk_commitment_ids": [str(x) for x in at_risk],
            }
        )
    return {"customers": summaries}


# ---------------------------------------------------------------------
# assemble_context — public entry point
# ---------------------------------------------------------------------


async def assemble_context(
    retrieval_result: RetrievalResult,
    access_context: AccessContext,
    conn: asyncpg.Connection,
    *,
    budget_observations: int | None = None,
    budget_models: int | None = None,
    budget_acts: int | None = None,
    budget_resources: int | None = None,
    config: RetrievalConfig | None = None,
    read_pool: asyncpg.Pool | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
) -> ContextBundle:
    """
    Compose a size-bounded ContextBundle from the retrieval result.

    Follow-up FU-1 (RA-4 wire): when `config.assembler_use_mmr=True`
    (env `RETRIEVAL_ASSEMBLER_USE_MMR=1`), the Models bucket is
    selected via `mmr_select` under `config.context_budget_tokens`
    with `config.mmr_lambda_diversity`. Retrieval scores drive the
    relevance term; ModelRow embeddings drive the diversity term. Items
    that don't fit the token budget are skipped (never truncated).
    Default False — count-cap path is unchanged.
    """
    cfg = config or CONFIG
    budgets = _resolve_assembler_budgets(
        cfg,
        budget_observations=budget_observations,
        budget_models=budget_models,
        budget_acts=budget_acts,
        budget_resources=budget_resources,
    )
    acts_cap = _select_context_acts(
        retrieval_result,
        tenant_id=access_context.tenant_id,
        budget_acts=budgets["acts"],
    )
    model_selection, resources_cap, customer_context = (
        await _select_context_db_facets(
            retrieval_result,
            access_context,
            conn,
            cfg=cfg,
            budgets=budgets,
            acts_cap=acts_cap,
            read_pool=read_pool,
            read_fanout_budget=read_fanout_budget,
        )
    )
    obs_tenant = [
        o for o in retrieval_result.observations
        if o.tenant_id == access_context.tenant_id
    ]
    observations_cap, observation_selection = _select_observations(
        retrieval_result,
        obs_tenant,
        cfg=cfg,
        budget_observations=budgets["observations"],
        explicit_budget=budgets["explicit_observation_budget"],
        selected_model_count=len(model_selection["models"]),
        selected_model_quality_mass=_selected_model_quality_mass(
            model_selection["models"],
            retrieval_result.model_scores,
            score_ceiling=_retrieval_score_ceiling(retrieval_result),
        ),
    )
    topology_context = None
    notes = _build_context_notes(
        retrieval_result,
        cfg=cfg,
        budgets=budgets,
        observations_cap=observations_cap,
        observation_selection=observation_selection,
        model_selection=model_selection,
        acts_cap=acts_cap,
        resources_cap=resources_cap,
    )

    return ContextBundle(
        observations=observations_cap,
        models=model_selection["models"],
        acts_summary=acts_cap,
        resources_summary=resources_cap,
        customer_context=customer_context,
        topology_context=topology_context,
        access_redactions=model_selection["redactions"],
        notes=notes,
    )


async def _select_context_db_facets(
    retrieval_result: RetrievalResult,
    access_context: AccessContext,
    conn: asyncpg.Connection,
    *,
    cfg: RetrievalConfig,
    budgets: dict[str, Any],
    acts_cap: dict[str, list],
    read_pool: asyncpg.Pool | None,
    read_fanout_budget: ReadFanoutBudget | None,
) -> tuple[dict[str, Any], list[ResourceRow], dict[str, Any] | None]:
    if not _assembler_read_fanout_enabled(conn, read_pool):
        return (
            await _select_context_models(
                retrieval_result,
                access_context,
                conn,
                cfg=cfg,
                budget_models=budgets["models"],
            ),
            await _select_context_resources(
                retrieval_result,
                conn,
                tenant_id=access_context.tenant_id,
                budget_resources=budgets["resources"],
            ),
            await _compute_customer_context(
                conn,
                access_context.tenant_id,
                acts_cap["commitments"],
            ),
        )

    assert read_pool is not None
    facet_read_budget = read_fanout_budget or ReadFanoutBudget.from_pool(read_pool)

    async def select_models() -> dict[str, Any]:
        async with facet_read_budget.connection() as read_conn:
            return await _select_context_models(
                retrieval_result,
                access_context,
                read_conn,
                cfg=cfg,
                budget_models=budgets["models"],
            )

    async def select_resources() -> list[ResourceRow]:
        async with facet_read_budget.connection() as read_conn:
            return await _select_context_resources(
                retrieval_result,
                read_conn,
                tenant_id=access_context.tenant_id,
                budget_resources=budgets["resources"],
            )

    async def select_customer_context() -> dict[str, Any] | None:
        async with facet_read_budget.connection() as read_conn:
            return await _compute_customer_context(
                read_conn,
                access_context.tenant_id,
                acts_cap["commitments"],
            )

    async with asyncio.TaskGroup() as task_group:
        models_task = task_group.create_task(select_models())
        resources_task = task_group.create_task(select_resources())
        customer_task = task_group.create_task(select_customer_context())
    model_selection = models_task.result()
    budget_snapshot = facet_read_budget.snapshot()
    model_selection["read_fanout_budget"] = {
        "max_concurrency": budget_snapshot.max_concurrency,
        "peak_in_use": budget_snapshot.peak_in_use,
        "acquired": budget_snapshot.acquired,
        "denied": budget_snapshot.denied,
    }
    return model_selection, resources_task.result(), customer_task.result()


def _assembler_read_fanout_enabled(
    conn: asyncpg.Connection,
    read_pool: asyncpg.Pool | None,
) -> bool:
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


def _resolve_assembler_budgets(
    cfg: RetrievalConfig,
    *,
    budget_observations: int | None,
    budget_models: int | None,
    budget_acts: int | None,
    budget_resources: int | None,
) -> dict[str, Any]:
    return {
        "explicit_observation_budget": budget_observations is not None,
        "observations": int(
            budget_observations
            if budget_observations is not None
            else cfg.assembler_budget_observations
        ),
        "models": int(
            budget_models
            if budget_models is not None
            else cfg.assembler_budget_models
        ),
        "acts": int(
            budget_acts
            if budget_acts is not None
            else cfg.assembler_budget_acts_total
        ),
        "resources": int(
            budget_resources
            if budget_resources is not None
            else cfg.assembler_budget_resources
        ),
    }


async def _select_context_models(
    retrieval_result: RetrievalResult,
    access_context: AccessContext,
    conn: asyncpg.Connection,
    *,
    cfg: RetrievalConfig,
    budget_models: int,
) -> dict[str, Any]:
    tenant_scoped, cross_tenant_redactions = _tenant_scope_models(
        retrieval_result.models, access_context.tenant_id
    )
    visible_models, redactions_inner, reason_counts = await _filter_models_via_db(
        tenant_scoped,
        access_context,
        conn,
    )
    _sort_models_by_retrieval_score(visible_models, retrieval_result)
    models_cap, mmr_notes = _select_ranked_models(
        retrieval_result,
        visible_models,
        cfg=cfg,
        budget_models=budget_models,
    )
    return {
        "models": models_cap,
        "visible_models": visible_models,
        "redactions": cross_tenant_redactions + redactions_inner,
        "redaction_reasons": reason_counts,
        "cross_tenant_redactions": cross_tenant_redactions,
        "mmr": mmr_notes,
    }


def _tenant_scope_models(
    models: list[ModelRow],
    tenant_id: UUID,
) -> tuple[list[ModelRow], int]:
    tenant_scoped: list[ModelRow] = []
    cross_tenant_redactions = 0
    for model in models:
        if model.tenant_id != tenant_id:
            cross_tenant_redactions += 1
            continue
        tenant_scoped.append(model)
    return tenant_scoped, cross_tenant_redactions


def _sort_models_by_retrieval_score(
    models: list[ModelRow],
    retrieval_result: RetrievalResult,
) -> None:
    scores = retrieval_result.model_scores
    anchor_tokens = _current_batch_lexical_anchors(retrieval_result)
    model_tokens = {model.id: _model_lexical_tokens(model) for model in models}
    token_frequency = {
        token: sum(token in tokens for tokens in model_tokens.values())
        for token in anchor_tokens
    }

    def anchor_rank(model: ModelRow) -> tuple[int, float]:
        if getattr(model, "status", None) != "active" or not anchor_tokens:
            return 0, 0.0
        matching = anchor_tokens & model_tokens[model.id]
        # A token repeated throughout the retrieved reservoir is vocabulary,
        # not an entity/subject anchor. Rare exact matches are the useful
        # discriminator when activation otherwise dominates the ordering.
        rare = {
            token for token in matching
            if token_frequency[token] <= max(1, len(models) // 4)
        }
        if not rare:
            return 0, 0.0
        specificity = sum(1.0 / token_frequency[token] for token in rare)
        return len(rare), specificity

    models.sort(
        key=lambda m: (
            -anchor_rank(m)[0],
            -anchor_rank(m)[1],
            -scores.get(m.id, 0.0),
            -m.activation,
            str(m.id),
        )
    )


def _current_batch_lexical_anchors(
    retrieval_result: RetrievalResult,
) -> set[str]:
    """Extract explicit lexical anchors from only the current T1 batch.

    This is deliberately an assembler rerank, not a new retrieval pathway:
    it can reorder already retrieved and authorized active Models but cannot
    widen tenant, visibility, maturity, or authority scope.
    """
    if not _is_explicit_t1_event_batch(retrieval_result):
        return set()
    trigger = retrieval_result.trigger
    trigger_ids = _trigger_observation_ids(retrieval_result)
    texts = [
        str(observation.content_text or "")
        for observation in retrieval_result.observations
        if observation.id in trigger_ids
    ]
    if trigger.seed_natural_text:
        texts.append(trigger.seed_natural_text)
    signature = trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    fragments = signature.get("batch_signal_fragments")
    if isinstance(fragments, list):
        texts.extend(
            str(fragment.get("text") or "")
            for fragment in fragments
            if isinstance(fragment, dict)
        )
    for entity in trigger.seed_entity_ids:
        if isinstance(entity, dict):
            texts.extend(str(value) for value in entity.values() if value is not None)
    return _lexical_tokens(" ".join(texts))


def _model_lexical_tokens(model: ModelRow) -> set[str]:
    return _lexical_tokens(f"{model.natural or ''} {model.proposition or ''}")


def _lexical_tokens(text: str) -> set[str]:
    return {
        token.casefold().strip("._-/:#")
        for token in _LEXICAL_ANCHOR_RE.findall(text)
        if token.casefold().strip("._-/:#") not in _LEXICAL_ANCHOR_STOPWORDS
    }


def _select_ranked_models(
    retrieval_result: RetrievalResult,
    visible_models: list[ModelRow],
    *,
    cfg: RetrievalConfig,
    budget_models: int,
) -> tuple[list[ModelRow], dict[str, Any]]:
    if not cfg.assembler_use_mmr or not visible_models:
        return visible_models[:budget_models], {"used": False}
    wrappers = _wrap_models_for_mmr(visible_models, retrieval_result.model_scores)
    anchors, used_anchor_tokens, relevance_anchor_candidate_ids, graph_anchor_ids = (
        _select_mmr_anchors(
            retrieval_result,
            wrappers,
            budget_models=budget_models,
            token_budget=int(cfg.context_budget_tokens),
        )
    )
    anchored_ids = {wrapper.model.id for wrapper in anchors}
    remaining_wrappers = [w for w in wrappers if w.model.id not in anchored_ids]
    remaining_slots = max(0, budget_models - len(anchors))
    remaining_budget = max(0, int(cfg.context_budget_tokens) - used_anchor_tokens)
    diverse_tail = (
        mmr_select(
            remaining_wrappers,
            budget_tokens=remaining_budget,
            lambda_diversity=float(cfg.mmr_lambda_diversity),
        )[:remaining_slots]
        if remaining_slots > 0 and remaining_budget > 0
        else []
    )
    selected = [w.model for w in [*anchors, *diverse_tail]][:budget_models]
    return selected, {
        "used": True,
        "lambda_diversity": float(cfg.mmr_lambda_diversity),
        "budget_tokens": int(cfg.context_budget_tokens),
        "relevance_anchor_count": len(
            {w.model.id for w in anchors} & relevance_anchor_candidate_ids
        ),
        "graph_anchor_count": len({w.model.id for w in anchors} & set(graph_anchor_ids)),
        "relevance_anchor_tokens": used_anchor_tokens,
        "selected_count": len(selected),
        "candidate_count": len(visible_models),
    }


def _wrap_models_for_mmr(
    models: list[ModelRow],
    scores: dict[UUID, float],
) -> list[_MMRModelWrapper]:
    return [
        _MMRModelWrapper(
            model=model,
            score=float(scores.get(model.id, 0.0)),
            tokens=_estimate_model_tokens(model),
            embedding=(list(model.embedding) if model.embedding is not None else None),
        )
        for model in models
    ]


def _select_mmr_anchors(
    retrieval_result: RetrievalResult,
    wrappers: list[_MMRModelWrapper],
    *,
    budget_models: int,
    token_budget: int,
) -> tuple[list[_MMRModelWrapper], int, set[UUID], list[UUID]]:
    wrappers_by_id = {w.model.id: w for w in wrappers}
    anchor_limit = min(_MMR_RELEVANCE_ANCHOR_COUNT, budget_models, len(wrappers))
    relevance_anchor_candidate_ids = {w.model.id for w in wrappers[:anchor_limit]}
    graph_anchor_ids = _top_pathway_model_ids(
        retrieval_result,
        "G",
        limit=_MMR_GRAPH_ANCHOR_COUNT,
    )
    anchor_candidates = [*wrappers[:anchor_limit]]
    anchor_candidates.extend(
        wrappers_by_id[mid] for mid in graph_anchor_ids if mid in wrappers_by_id
    )
    anchors: list[_MMRModelWrapper] = []
    anchored_ids: set[UUID] = set()
    used_anchor_tokens = 0
    for wrapper in anchor_candidates:
        if len(anchors) >= budget_models:
            break
        if wrapper.model.id in anchored_ids:
            continue
        if used_anchor_tokens + wrapper.tokens > token_budget:
            continue
        anchors.append(wrapper)
        anchored_ids.add(wrapper.model.id)
        used_anchor_tokens += wrapper.tokens
    return anchors, used_anchor_tokens, relevance_anchor_candidate_ids, graph_anchor_ids


def _select_context_acts(
    retrieval_result: RetrievalResult,
    *,
    tenant_id: UUID,
    budget_acts: int,
) -> dict[str, list]:
    flat_acts: list[tuple[str, Any, Any]] = []
    for goal in retrieval_result.acts.get("goals", []):
        if goal.tenant_id == tenant_id:
            flat_acts.append(("goals", goal, goal.last_state_change_at or goal.created_at))
    for commitment in retrieval_result.acts.get("commitments", []):
        if commitment.tenant_id == tenant_id:
            flat_acts.append((
                "commitments",
                commitment,
                commitment.last_state_change_at or commitment.created_at,
            ))
    for decision in retrieval_result.acts.get("decisions", []):
        if decision.tenant_id == tenant_id:
            flat_acts.append((
                "decisions",
                decision,
                decision.last_state_change_at or decision.created_at,
            ))
    flat_acts.sort(key=lambda item: item[2], reverse=True)
    acts_cap: dict[str, list] = {"goals": [], "commitments": [], "decisions": []}
    for kind, row, _ts in flat_acts[:budget_acts]:
        acts_cap[kind].append(row)
    return acts_cap


async def _select_context_resources(
    retrieval_result: RetrievalResult,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    budget_resources: int,
) -> list[ResourceRow]:
    resources = [
        resource
        for resource in retrieval_result.resources
        if resource.tenant_id == tenant_id
    ]
    if not resources:
        return []
    linked = await _load_customer_linked_resource_ids(conn, resources)
    resources.sort(
        key=lambda resource: (
            0 if resource.id in linked else 1,
            -(resource.last_updated_at.timestamp() if resource.last_updated_at else 0),
        )
    )
    return resources[:budget_resources]


async def _load_customer_linked_resource_ids(
    conn: asyncpg.Connection,
    resources: list[ResourceRow],
) -> set[UUID]:
    resource_ids = [resource.id for resource in resources]
    if not resource_ids:
        return set()
    link_rows = await conn.fetch(
        """
        SELECT DISTINCT customer_resource_id
        FROM customer_commitments
        WHERE customer_resource_id = ANY($1::uuid[])
        """,
        resource_ids,
    )
    return {row["customer_resource_id"] for row in link_rows}


def _build_context_notes(
    retrieval_result: RetrievalResult,
    *,
    cfg: RetrievalConfig,
    budgets: dict[str, Any],
    observations_cap: list[ObservationRow],
    observation_selection: dict[str, Any],
    model_selection: dict[str, Any],
    acts_cap: dict[str, list],
    resources_cap: list[ResourceRow],
) -> dict[str, Any]:
    notes: dict[str, Any] = {
        "budgets": _context_budget_notes(cfg, budgets, observation_selection),
        "budget_overflow": {
            "observations": len(retrieval_result.observations) - len(observations_cap),
            "models": len(retrieval_result.models) - len(model_selection["models"]),
            "acts": sum(len(v) for v in retrieval_result.acts.values())
            - sum(len(v) for v in acts_cap.values()),
            "resources": len(retrieval_result.resources) - len(resources_cap),
        },
        "access_redactions": model_selection["redactions"],
        "access_redaction_reasons": model_selection["redaction_reasons"],
        "access_redactions_cross_tenant": model_selection["cross_tenant_redactions"],
        "retrieval_trigger_kind": retrieval_result.trigger.kind,
        "observation_selection": observation_selection,
        "mmr": model_selection["mmr"],
        "model_selection": _model_selection_notes(
            retrieval_result,
            visible_models=model_selection["visible_models"],
            selected_models=model_selection["models"],
        ),
    }
    if "read_fanout_budget" in model_selection:
        notes["read_fanout_budget"] = model_selection["read_fanout_budget"]
    if isinstance(retrieval_result.notes, dict):
        inquiry_notes = retrieval_result.notes.get("inquiry")
        if isinstance(inquiry_notes, dict):
            notes["inquiry"] = inquiry_notes
            if isinstance(inquiry_notes.get("context_packet"), dict):
                notes["inquiry_context_packet"] = inquiry_notes["context_packet"]
    return notes


def _context_budget_notes(
    cfg: RetrievalConfig,
    budgets: dict[str, Any],
    observation_selection: dict[str, Any],
) -> dict[str, int]:
    return {
        "observations": int(budgets["observations"]),
        "trigger_observations": int(observation_selection["trigger_cap"]),
        "historical_observations": (
            int(observation_selection["historical_cap"])
            if cfg.model_first_context_enabled
            else int(budgets["observations"])
        ),
        "models": int(budgets["models"]),
        "acts_total": int(budgets["acts"]),
        "resources": int(budgets["resources"]),
    }


__all__ = [
    "AccessContext",
    "ContextBundle",
    "AssemblerError",
    "assemble_context",
    "mmr_select",
]
