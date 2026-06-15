"""Inquiry retrieval result composition and relevance selection."""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import asdict
from datetime import datetime
from typing import Any
from uuid import UUID

from lib.shared.ids import uuid7
from lib.shared.types import (
    CommitmentRow,
    DecisionRow,
    GoalRow,
    ModelRow,
    ObservationRow,
    ResourceRow,
)
from services.domain.models.address import belief_address_from_model_like
from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext

from .answer_evaluation import classify_hypothesis_links as _classify_hypothesis_links
from .config import InquiryConfig
from .evidence_utils import (
    compact as _compact,
    declares_unrelated_to_trigger as _declares_unrelated_to_trigger,
    estimate_tokens as _estimate_tokens,
    has_material_trigger_overlap as _has_material_trigger_overlap,
    jsonable as _jsonable,
    sensitivity as _sensitivity,
    trust_score as _trust_score,
)
from .language_signals import (
    has_act_affecting_language as _has_act_affecting_language,
    has_broad_signal_language as _has_broad_signal_language,
    has_risk_language as _has_risk_language,
    mentions_recurrence as _mentions_recurrence,
    signal_has_material_update_intent as _signal_has_material_update_intent,
)
from .lexical_terms import relevance_tokens as _relevance_tokens
from .routing import trigger_text as _trigger_text
from .types import EvidenceCard, Hypothesis, ModelRelevance, RetrievalAction


def _result_from_pathway(
    trigger: TriggerContext,
    pr: PathwayResult,
    action: RetrievalAction,
) -> RetrievalResult:
    scores: dict[UUID, float] = {}
    for rank, model in enumerate(pr.models, start=1):
        scores[model.id] = max(0.01, 1.0 / (rank + 1))
    return RetrievalResult(
        trigger=trigger,
        observations=list(pr.observations),
        models=list(pr.models),
        acts={k: list(v) for k, v in pr.acts.items()},
        resources=list(pr.resources),
        pathway_results=[pr],
        notes={
            "action": _jsonable(asdict(action)),
            "pathways_run": [pr.source_pathway],
            "models_merged": len(pr.models),
            "observations_merged": len(pr.observations),
        },
        model_scores=scores,
    )


def _merge_results(
    trigger: TriggerContext,
    results: list[RetrievalResult],
    *,
    top_n: int,
    note_prefix: str,
    config: InquiryConfig | None = None,
    relevance_gate: bool = False,
) -> RetrievalResult:
    models_by_id: dict[UUID, ModelRow] = {}
    model_scores: dict[UUID, float] = {}
    model_pathways: dict[UUID, set[str]] = {}
    model_questions: dict[UUID, set[str]] = {}
    observations_by_id: dict[UUID, ObservationRow] = {}
    resources_by_id: dict[UUID, ResourceRow] = {}
    goals_by_id: dict[UUID, GoalRow] = {}
    commitments_by_id: dict[UUID, CommitmentRow] = {}
    decisions_by_id: dict[UUID, DecisionRow] = {}
    pathway_results: list[PathwayResult] = []
    pathways_run: list[str] = []
    skipped: list[Any] = []

    for result in results:
        pathway_results.extend(result.pathway_results)
        action_note = result.notes.get("action")
        action_question = None
        action_path = None
        if isinstance(action_note, dict):
            action_question = action_note.get("question_id")
            action_path = action_note.get("path")
        for pr in result.pathway_results:
            pathway = pr.source_pathway
            for model in pr.models:
                model_pathways.setdefault(model.id, set()).add(pathway)
                if isinstance(action_path, str):
                    model_pathways[model.id].add(action_path)
                if isinstance(action_question, str):
                    model_questions.setdefault(model.id, set()).add(action_question)
        for pathway in result.notes.get("pathways_run", []):
            if pathway not in pathways_run:
                pathways_run.append(pathway)
        skipped.extend(result.notes.get("pathways_skipped", []))
        for model in result.models:
            models_by_id.setdefault(model.id, model)
            model_scores[model.id] = model_scores.get(model.id, 0.0) + float(
                result.model_scores.get(model.id, 0.01)
            )
        for obs in result.observations:
            observations_by_id.setdefault(obs.id, obs)
        for res in result.resources:
            resources_by_id.setdefault(res.id, res)
        for goal in result.acts.get("goals", []):
            goals_by_id.setdefault(goal.id, goal)
        for commitment in result.acts.get("commitments", []):
            commitments_by_id.setdefault(commitment.id, commitment)
        for decision in result.acts.get("decisions", []):
            decisions_by_id.setdefault(decision.id, decision)

    ranked_models = sorted(
        models_by_id.values(),
        key=lambda m: (-model_scores.get(m.id, 0.0), -m.activation, str(m.id)),
    )
    relevance_notes: dict[str, Any] | None = None
    if relevance_gate:
        ranked_models, relevance_notes = _select_relevant_models(
            trigger,
            ranked_models,
            model_scores,
            top_n=top_n,
            config=config or InquiryConfig(),
            model_pathways=model_pathways,
            model_questions=model_questions,
        )
    else:
        ranked_models = ranked_models[:top_n]
    models = ranked_models
    observations = sorted(
        observations_by_id.values(),
        key=lambda o: (o.occurred_at, o.id),
        reverse=True,
    )
    resources = sorted(
        resources_by_id.values(),
        key=lambda r: (r.last_updated_at, r.id),
        reverse=True,
    )
    acts = {
        "goals": sorted(goals_by_id.values(), key=lambda g: g.created_at, reverse=True),
        "commitments": sorted(
            commitments_by_id.values(),
            key=lambda c: c.last_state_change_at,
            reverse=True,
        ),
        "decisions": sorted(
            decisions_by_id.values(),
            key=lambda d: d.last_state_change_at,
            reverse=True,
        ),
    }
    return RetrievalResult(
        trigger=trigger,
        observations=observations,
        models=models,
        acts=acts,
        resources=resources,
        pathway_results=pathway_results,
        notes={
            "kind": trigger.kind,
            "pathways_run": pathways_run,
            "pathways_skipped": skipped,
            "models_merged": len(models),
            "observations_merged": len(observations),
            "acts_merged": {k: len(v) for k, v in acts.items()},
            "resources_merged": len(resources),
            "merge_source": note_prefix,
            "candidate_model_count": len(models_by_id),
            **(
                {"relevance_gate": relevance_notes}
                if relevance_notes is not None
                else {}
            ),
        },
        model_scores={
            mid: score
            for mid, score in model_scores.items()
            if mid in {m.id for m in models}
        },
    )


def _select_relevant_models(
    trigger: TriggerContext,
    ranked_models: list[ModelRow],
    model_scores: dict[UUID, float],
    *,
    top_n: int,
    config: InquiryConfig,
    model_pathways: dict[UUID, set[str]],
    model_questions: dict[UUID, set[str]],
) -> tuple[list[ModelRow], dict[str, Any]]:
    if not ranked_models or top_n <= 0:
        return [], {
            "used": True,
            "candidate_count": len(ranked_models),
            "selected_count": 0,
            "reason": "no candidates or non-positive top_n",
        }

    trigger_text = _trigger_text(trigger)
    lower = trigger_text.casefold()
    material_signal = trigger.kind != "T1" or _signal_has_material_update_intent(lower)
    broad_signal = _has_broad_signal_language(lower)
    weak_signal = not material_signal and not broad_signal
    threshold = (
        float(config.relevance_broad_signal_min_score)
        if broad_signal
        else (
            float(config.relevance_weak_signal_min_score)
            if weak_signal
            else float(config.relevance_min_score)
        )
    )
    max_raw = max(
        (float(model_scores.get(m.id, 0.0)) for m in ranked_models), default=0.0
    )
    scored: list[tuple[ModelRow, ModelRelevance]] = []
    for model in ranked_models:
        rel = _score_model_relevance(
            trigger,
            model,
            raw_score=float(model_scores.get(model.id, 0.0)),
            max_raw_score=max_raw,
            model_pathways=model_pathways.get(model.id, set()),
            model_questions=model_questions.get(model.id, set()),
            weak_signal=weak_signal,
            broad_signal=broad_signal,
        )
        scored.append((model, rel))
    scored.sort(
        key=lambda item: (-item[1].final_score, -item[0].activation, str(item[0].id))
    )

    min_material = (
        min(max(0, int(config.relevance_min_material_models)), top_n, len(scored))
        if material_signal or broad_signal
        else 0
    )
    diversity_candidate_cap = _relevance_diversity_candidate_cap(
        len(scored),
        top_n,
        weak_signal=weak_signal,
        broad_signal=broad_signal,
    )
    selected_pairs: list[tuple[ModelRow, ModelRelevance]] = []
    dropped_below_threshold = 0
    cutoff_reason = "candidate list exhausted"
    prev_score: float | None = None
    for idx, pair in enumerate(scored):
        score = pair[1].final_score
        below_threshold = score < threshold
        if below_threshold and len(selected_pairs) >= min_material:
            dropped_below_threshold += 1
            cutoff_reason = "score below relevance threshold"
            continue
        if (
            prev_score is not None
            and not broad_signal
            and len(selected_pairs) >= min_material
            and float(config.relevance_score_cliff) > 0
            and prev_score - score >= float(config.relevance_score_cliff)
        ):
            cutoff_reason = "score cliff detected"
            if weak_signal or len(selected_pairs) >= diversity_candidate_cap:
                dropped_below_threshold += len(scored) - idx
                break
        selected_pairs.append(pair)
        prev_score = score
        if len(selected_pairs) >= diversity_candidate_cap:
            cutoff_reason = (
                "top_n cap reached after relevance gate"
                if diversity_candidate_cap == top_n
                else "diversity reservoir cap reached after relevance gate"
            )
            break

    selected_pairs_before_compaction = len(selected_pairs)
    selected_pairs, duplicate_drops, compaction_notes = _apply_relevance_diversity(
        selected_pairs,
        top_n=top_n,
        weak_signal=weak_signal,
        broad_signal=broad_signal,
        threshold=threshold,
        min_keep=min_material,
        model_pathways=model_pathways,
        model_questions=model_questions,
    )
    selected_pairs, closure_notes = _append_structural_closure(
        selected_pairs,
        scored,
        top_n=top_n,
        weak_signal=weak_signal,
        broad_signal=broad_signal,
        threshold=threshold,
        model_pathways=model_pathways,
        model_questions=model_questions,
    )
    selected_pairs, packing_notes = _pack_structural_links(selected_pairs)
    selected = [model for model, _ in selected_pairs]
    notes = {
        "used": True,
        "candidate_count": len(ranked_models),
        "selected_count": len(selected),
        "threshold": round(threshold, 4),
        "signal_class": (
            "broad" if broad_signal else ("weak" if weak_signal else "material")
        ),
        "min_material_models": min_material,
        "dropped_below_threshold": dropped_below_threshold,
        "dropped_redundant": duplicate_drops,
        "cutoff_reason": cutoff_reason,
        "selected_before_compaction": selected_pairs_before_compaction,
        "diversity_candidate_cap": diversity_candidate_cap,
        "coverage_compaction": compaction_notes,
        "structural_closure": closure_notes,
        "structural_packing": packing_notes,
        "top_scores": [
            _jsonable(
                {
                    "model_id": rel.model_id,
                    "score": round(rel.final_score, 4),
                    "base": round(rel.base_score, 4),
                    "lexical": round(rel.lexical_score, 4),
                    "scope": round(rel.scope_score, 4),
                    "path": round(rel.path_score, 4),
                    "evidence": round(rel.evidence_score, 4),
                    "provenance": round(rel.provenance_score, 4),
                    "penalty": round(rel.penalty, 4),
                    "reasons": list(rel.reasons),
                }
            )
            for _, rel in scored[:12]
        ],
        "selected_model_ids": [str(model.id) for model in selected],
    }
    return selected, notes


def _pack_structural_links(
    selected_pairs: list[tuple[ModelRow, ModelRelevance]],
) -> tuple[list[tuple[ModelRow, ModelRelevance]], dict[str, Any]]:
    """Place explanatory relation/counterevidence models next to their anchor."""
    notes: dict[str, Any] = {
        "used": True,
        "moved": 0,
        "moved_model_ids": [],
    }
    if len(selected_pairs) < 3:
        return selected_pairs, notes

    positions = {model.id: idx for idx, (model, _rel) in enumerate(selected_pairs)}
    selected_by_id = {model.id: model for model, _rel in selected_pairs}
    dependents_by_anchor: dict[UUID, list[tuple[ModelRow, ModelRelevance]]] = {}
    moved_ids: set[UUID] = set()

    for model, rel in selected_pairs:
        if not _is_structural_detail_model(model):
            continue
        anchors = [
            anchor_id
            for anchor_id in _linked_anchor_ids(model, selected_by_id)
            if anchor_id in positions and anchor_id != model.id
        ]
        if not anchors:
            continue
        anchor_id = min(anchors, key=lambda mid: positions[mid])
        if positions[anchor_id] + 1 == positions[model.id]:
            continue
        dependents_by_anchor.setdefault(anchor_id, []).append((model, rel))
        moved_ids.add(model.id)

    if not moved_ids:
        return selected_pairs, notes

    repacked: list[tuple[ModelRow, ModelRelevance]] = []
    emitted: set[UUID] = set()
    for pair in selected_pairs:
        model, _rel = pair
        if model.id in moved_ids:
            continue
        repacked.append(pair)
        emitted.add(model.id)
        for dependent_pair in sorted(
            dependents_by_anchor.get(model.id, []),
            key=lambda item: positions[item[0].id],
        ):
            dependent = dependent_pair[0]
            if dependent.id in emitted:
                continue
            repacked.append(dependent_pair)
            emitted.add(dependent.id)

    # Preserve any model whose anchor was itself moved behind another anchor.
    for pair in selected_pairs:
        if pair[0].id not in emitted:
            repacked.append(pair)
            emitted.add(pair[0].id)

    notes["moved"] = len(moved_ids)
    notes["moved_model_ids"] = [str(mid) for mid in sorted(moved_ids, key=str)]
    return repacked, notes


def _is_structural_detail_model(model: ModelRow) -> bool:
    role = str(getattr(model, "claim_role", "") or "").casefold()
    level = str(getattr(model, "abstraction_level", "") or "").casefold()
    polarity = str(getattr(model, "polarity", "") or "").casefold()
    text = " ".join(
        str(part)
        for part in (
            getattr(model, "natural", "") or "",
            json.dumps(getattr(model, "proposition", {}) or {}, default=str),
        )
    ).casefold()
    return (
        role == "relation"
        or level in {"relationship", "composite"}
        or polarity == "mixed"
        or bool(_model_member_ids(model))
        or _has_counterevidence_qualifier_language(text)
    )


def _linked_anchor_ids(
    model: ModelRow, selected_by_id: dict[UUID, ModelRow]
) -> set[UUID]:
    anchors: set[UUID] = set()
    for raw in getattr(model, "supporting_model_ids", []) or ():
        try:
            anchors.add(UUID(str(raw)))
        except (TypeError, ValueError):
            continue
    anchors.update(_model_member_ids(model))
    for selected_id, selected_model in selected_by_id.items():
        if model.id in set(getattr(selected_model, "supporting_model_ids", []) or []):
            anchors.add(selected_id)
        if model.id in _model_member_ids(selected_model):
            anchors.add(selected_id)
    return anchors


def _relevance_diversity_candidate_cap(
    scored_count: int,
    top_n: int,
    *,
    weak_signal: bool,
    broad_signal: bool,
) -> int:
    if top_n <= 0:
        return 0
    if weak_signal:
        return min(scored_count, top_n)
    multiplier = 3 if broad_signal else 2
    additive_floor = 48 if broad_signal else 32
    return min(
        scored_count, max(top_n, min(top_n * multiplier, top_n + additive_floor))
    )


def _append_structural_closure(
    selected_pairs: list[tuple[ModelRow, ModelRelevance]],
    candidate_pairs: list[tuple[ModelRow, ModelRelevance]],
    *,
    top_n: int,
    weak_signal: bool,
    broad_signal: bool,
    threshold: float,
    model_pathways: dict[UUID, set[str]] | None = None,
    model_questions: dict[UUID, set[str]] | None = None,
) -> tuple[list[tuple[ModelRow, ModelRelevance]], dict[str, Any]]:
    """Keep structurally necessary belief siblings in the final model list.

    The relevance scorer is intentionally conservative: a graph-only relation
    or counterevidence model may have weak surface text even when it explains
    or qualifies a selected belief. This pass is a small closure over already
    retrieved candidates, not an expansion query.
    """
    notes: dict[str, Any] = {
        "used": True,
        "added": 0,
        "added_model_ids": [],
        "reasons": {},
    }
    if weak_signal or not selected_pairs or top_n <= len(selected_pairs):
        return selected_pairs, notes

    model_pathways = model_pathways or {}
    model_questions = model_questions or {}
    selected_by_id = {model.id: (model, rel) for model, rel in selected_pairs}
    candidate_by_id = {model.id: (model, rel) for model, rel in candidate_pairs}
    max_added = 2 if broad_signal else 4

    for model, rel in candidate_pairs:
        if model.id in selected_by_id:
            continue
        if len(selected_by_id) >= top_n or notes["added"] >= max_added:
            break
        reason = _structural_closure_reason(
            model,
            rel,
            selected_by_id,
            model_pathways=model_pathways.get(model.id, set()),
            model_questions=model_questions.get(model.id, set()),
            threshold=threshold,
        )
        if reason is None:
            continue
        selected_pairs.append(candidate_by_id[model.id])
        selected_by_id[model.id] = candidate_by_id[model.id]
        mid = str(model.id)
        notes["added"] += 1
        notes["added_model_ids"].append(mid)
        notes["reasons"][mid] = reason

    return selected_pairs, notes


def _structural_closure_reason(
    model: ModelRow,
    rel: ModelRelevance,
    selected_by_id: dict[UUID, tuple[ModelRow, ModelRelevance]],
    *,
    model_pathways: set[str],
    model_questions: set[str],
    threshold: float,
) -> str | None:
    if not _has_selected_model_link(model, selected_by_id):
        return None

    focused_graph_path = bool(model_pathways & {"G", "model_edge", "sage_reader"})
    text = " ".join(
        str(part)
        for part in (
            getattr(model, "natural", "") or "",
            json.dumps(getattr(model, "proposition", {}) or {}, default=str),
        )
    ).casefold()
    role = str(getattr(model, "claim_role", "") or "").casefold()
    level = str(getattr(model, "abstraction_level", "") or "").casefold()
    polarity = str(getattr(model, "polarity", "") or "").casefold()
    is_relation = (
        role == "relation"
        or level in {"relationship", "composite"}
        or bool(_model_member_ids(model))
    )
    if is_relation and focused_graph_path:
        return "linked_relation"

    is_counter = (
        "Q_COUNTEREVIDENCE" in model_questions
        or polarity == "mixed"
        or _has_counterevidence_qualifier_language(text)
    )
    if is_counter and (
        focused_graph_path or rel.final_score >= max(0.20, threshold * 0.75)
    ):
        return "linked_counterevidence"

    return None


def _has_selected_model_link(
    model: ModelRow,
    selected_by_id: dict[UUID, tuple[ModelRow, ModelRelevance]],
) -> bool:
    selected_ids = set(selected_by_id)
    if set(getattr(model, "supporting_model_ids", []) or []) & selected_ids:
        return True
    candidate_members = _model_member_ids(model)
    if candidate_members & selected_ids:
        return True
    for selected_model, _rel in selected_by_id.values():
        if model.id in set(getattr(selected_model, "supporting_model_ids", []) or []):
            return True
        if model.id in _model_member_ids(selected_model):
            return True
    return False


def _model_member_ids(model: ModelRow) -> set[UUID]:
    prop = getattr(model, "proposition", {}) or {}
    if not isinstance(prop, dict):
        return set()
    out: set[UUID] = set()
    for raw in prop.get("member_model_ids") or ():
        try:
            out.add(UUID(str(raw)))
        except (TypeError, ValueError):
            continue
    return out


def _has_counterevidence_qualifier_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b("
            r"counterevidence|mitigation\s+exists|mitigated\s+but|"
            r"does\s+not\s+remove|doesn't\s+remove|should\s+not\s+erase|"
            r"risk\s+remains|blocker\s+remains|alternate\s+explanation|"
            r"weaken(?:s|ed)?|contradict(?:s|ed)?|premise\s+(?:is\s+)?"
            r"(?:stale|incomplete|unsupported)"
            r")\b",
            lower,
        )
    )


def _score_model_relevance(
    trigger: TriggerContext,
    model: ModelRow,
    *,
    raw_score: float,
    max_raw_score: float,
    model_pathways: set[str],
    model_questions: set[str],
    weak_signal: bool,
    broad_signal: bool,
) -> ModelRelevance:
    reasons: list[str] = []
    trigger_text = _trigger_text(trigger)
    model_text = " ".join(
        str(part)
        for part in (
            getattr(model, "natural", "") or "",
            json.dumps(getattr(model, "proposition", {}) or {}, default=str),
        )
    )
    raw_norm = raw_score / max_raw_score if max_raw_score > 0 else 0.0
    base_score = min(0.22, 0.22 * raw_norm)
    lexical_score = _lexical_relevance_score(trigger_text, model_text)
    scope_score, scope_reasons = _scope_relevance_score(trigger, model)
    path_score = min(
        0.14,
        0.035 * len(model_pathways) + 0.025 * len(model_questions),
    )
    explicit_model_ids = set(trigger.member_model_ids or [])
    if trigger.model_id is not None:
        explicit_model_ids.add(trigger.model_id)
    explicit_model_score = 0.0
    if model.id in explicit_model_ids:
        explicit_model_score = 0.55
        reasons.append("explicit trigger model")
    elif explicit_model_ids and (
        "G" in model_pathways or "model_edge" in model_pathways
    ):
        explicit_model_score = 0.26
        reasons.append("graph neighbor of explicit trigger model")
    evidence_score = _model_evidence_relevance_score(trigger_text, model_text)
    provenance_score = min(
        0.10,
        0.035 * min(len(getattr(model, "supporting_event_ids", []) or []), 2)
        + 0.025 * min(len(getattr(model, "supporting_model_ids", []) or []), 2)
        + 0.025 * max(0.0, min(1.0, float(getattr(model, "confidence", 0.0) or 0.0))),
    )
    penalty = 0.0
    if lexical_score <= 0.0 and scope_score <= 0.0:
        penalty += 0.28
        reasons.append("no lexical or scope overlap")
    if model_pathways and model_pathways <= {"C", "temporal"} and lexical_score < 0.08:
        penalty += 0.16
        reasons.append("temporal-only weak lexical match")
    if _declares_unrelated_to_trigger(model_text.casefold()):
        penalty += 0.40
        reasons.append("declares unrelated to trigger")

    if weak_signal:
        base_score *= 0.45
        scope_score *= 0.55
        path_score *= 0.50
        evidence_score *= 0.65
        reasons.append("weak signal dampening")

    if lexical_score > 0:
        reasons.append("material lexical overlap")
    reasons.extend(scope_reasons)
    if path_score > 0:
        reasons.append("retrieved by focused inquiry path")
    if evidence_score > 0:
        reasons.append("hypothesis/counterevidence language")
    if provenance_score > 0:
        reasons.append("model provenance/confidence")

    final_score = max(
        0.0,
        base_score
        + lexical_score
        + scope_score
        + path_score
        + explicit_model_score
        + evidence_score
        + provenance_score
        - penalty,
    )
    if (
        not weak_signal
        and not broad_signal
        and explicit_model_score <= 0.0
        and scope_score > 0.0
        and lexical_score < 0.08
        and evidence_score <= 0.0
    ):
        final_score = min(final_score, 0.24)
        reasons.append("scope-only match capped")
    return ModelRelevance(
        model_id=model.id,
        final_score=final_score,
        base_score=base_score,
        lexical_score=lexical_score,
        scope_score=scope_score,
        path_score=path_score,
        evidence_score=evidence_score,
        provenance_score=provenance_score,
        penalty=penalty,
        reasons=tuple(reasons[:8]),
    )


def _scope_relevance_score(
    trigger: TriggerContext,
    model: ModelRow,
) -> tuple[float, list[str]]:
    trigger_entities = _canonical_entity_pairs(trigger.seed_entity_ids)
    model_entities = _canonical_entity_pairs(getattr(model, "scope_entities", []) or [])
    score = 0.0
    reasons: list[str] = []
    overlap = trigger_entities & model_entities
    if overlap:
        type_priority = {etype for etype, _ in overlap}
        if "commitment" in type_priority:
            score += 0.42
            reasons.append("same commitment")
        if type_priority & {"customer", "customer_resource", "resource"}:
            score += 0.32
            reasons.append("same customer/resource")
        if "goal" in type_priority:
            score += 0.24
            reasons.append("same goal")
        if "decision" in type_priority:
            score += 0.22
            reasons.append("same decision")
    actor_overlap = set(trigger.scope_actors or []) & set(
        getattr(model, "scope_actors", []) or []
    )
    if actor_overlap:
        score += 0.12
        reasons.append("same actor")
    return min(0.52, score), reasons


def _canonical_entity_pairs(raw_entities: Any) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    if not isinstance(raw_entities, list):
        return pairs
    for raw in raw_entities:
        if not isinstance(raw, dict):
            continue
        etype = raw.get("type")
        eid = raw.get("id")
        if etype is None or eid is None:
            continue
        t = str(etype)
        i = str(eid)
        if t in {"customer", "customer_resource", "resource"}:
            pairs.add(("customer", i))
            pairs.add(("customer_resource", i))
            pairs.add(("resource", i))
        else:
            pairs.add((t, i))
    return pairs


def _lexical_relevance_score(trigger_text: str, model_text: str) -> float:
    trigger_tokens = _relevance_tokens(trigger_text)
    if not trigger_tokens:
        return 0.0
    model_tokens = _relevance_tokens(model_text)
    if not model_tokens:
        return 0.0
    overlap = trigger_tokens & model_tokens
    if not overlap:
        return 0.0
    recall = len(overlap) / max(1, len(trigger_tokens))
    precision = len(overlap) / max(1, len(model_tokens))
    score = 0.22 * recall + 0.10 * min(1.0, precision * 3.0)
    if len(overlap) >= 3:
        score += 0.06
    return min(0.34, score)


def _model_evidence_relevance_score(trigger_text: str, model_text: str) -> float:
    lower = model_text.casefold()
    trigger_lower = trigger_text.casefold()
    if not _has_material_trigger_overlap(lower, trigger_lower):
        return 0.0
    score = 0.0
    if _has_risk_language(lower):
        score += 0.08
    if _has_act_affecting_language(lower):
        score += 0.06
    if _mentions_recurrence(lower):
        score += 0.05
    if re.search(r"\b(resolved|unblocked|not blocked|launched|mitigated)\b", lower):
        score += 0.07
    return min(0.18, score)


def _apply_relevance_diversity(
    selected_pairs: list[tuple[ModelRow, ModelRelevance]],
    *,
    top_n: int,
    weak_signal: bool,
    broad_signal: bool,
    threshold: float,
    min_keep: int,
    model_pathways: dict[UUID, set[str]] | None = None,
    model_questions: dict[UUID, set[str]] | None = None,
) -> tuple[list[tuple[ModelRow, ModelRelevance]], int, dict[str, Any]]:
    if not selected_pairs or top_n <= 0:
        return (
            [],
            len(selected_pairs),
            {
                "strategy": "coverage_aware",
                "target_limit": 0,
                "selected_before": len(selected_pairs),
                "selected_after": 0,
            },
        )

    target_limit = min(
        top_n,
        _coverage_compaction_target(
            len(selected_pairs), top_n, weak_signal, broad_signal
        ),
    )
    floor = min(target_limit, max(1, int(min_keep or 0)))
    if broad_signal:
        # Broad portfolio questions need representative breadth before
        # redundancy pruning. A same-cluster set can still describe many
        # independent customers, constraints, or instances of a trend.
        floor = min(target_limit, max(floor, min(20, len(selected_pairs))))
    model_pathways = model_pathways or {}
    model_questions = model_questions or {}
    remaining = list(selected_pairs)
    out: list[tuple[ModelRow, ModelRelevance]] = []
    covered: Counter[str] = Counter()
    cluster_counts: Counter[tuple[Any, ...]] = Counter()

    def add_pair(pair: tuple[ModelRow, ModelRelevance]) -> None:
        model, rel = pair
        out.append(pair)
        cluster_counts[_model_relevance_cluster_key(model)] += 1
        for feature, _weight in _model_coverage_features(
            model,
            model_pathways.get(model.id, set()),
            model_questions.get(model.id, set()),
        ):
            covered[feature] += 1

    while remaining and len(out) < floor:
        add_pair(remaining.pop(0))

    while remaining and len(out) < target_limit:
        best_idx = 0
        best_utility = float("-inf")
        for idx, pair in enumerate(remaining):
            model, rel = pair
            utility = _coverage_selection_utility(
                model,
                rel,
                covered,
                cluster_counts,
                broad_signal=broad_signal,
                weak_signal=weak_signal,
                model_pathways=model_pathways.get(model.id, set()),
                model_questions=model_questions.get(model.id, set()),
            )
            # A tiny position prior keeps ties stable and favors the relevance
            # ordering produced by the scorer.
            utility -= idx * 0.0005
            if utility > best_utility:
                best_utility = utility
                best_idx = idx

        if (
            len(out) >= floor
            and len(out) >= 8
            and best_utility < max(0.20, threshold + (0.03 if broad_signal else 0.05))
            and not _has_uncovered_answer_obligation(
                remaining[best_idx][0],
                covered,
                model_questions=model_questions.get(remaining[best_idx][0].id, set()),
            )
        ):
            break
        add_pair(remaining.pop(best_idx))

    dropped = max(0, len(selected_pairs) - len(out))
    notes = {
        "strategy": "coverage_aware",
        "target_limit": target_limit,
        "floor": floor,
        "selected_before": len(selected_pairs),
        "selected_after": len(out),
        "dropped": dropped,
        "coverage_features": len(covered),
        "cluster_count": len(cluster_counts),
    }
    return out, dropped, notes


def _coverage_compaction_target(
    selected_count: int,
    top_n: int,
    weak_signal: bool,
    broad_signal: bool,
) -> int:
    if weak_signal:
        return min(top_n, 8)
    if broad_signal:
        return min(top_n, max(20, min(32, selected_count)))
    if selected_count >= 32:
        return min(top_n, 18)
    return min(top_n, selected_count)


def _coverage_selection_utility(
    model: ModelRow,
    rel: ModelRelevance,
    covered: Counter[str],
    cluster_counts: Counter[tuple[Any, ...]],
    *,
    broad_signal: bool,
    weak_signal: bool,
    model_pathways: set[str],
    model_questions: set[str],
) -> float:
    features = _model_coverage_features(model, model_pathways, model_questions)
    novelty = sum(weight / (1 + covered[feature]) for feature, weight in features)
    cluster_count = cluster_counts[_model_relevance_cluster_key(model)]
    answer_novelty = _has_uncovered_answer_obligation(
        model,
        covered,
        model_questions=model_questions,
    )
    redundancy_penalty = 0.0
    if cluster_count:
        redundancy_penalty += 0.07 * cluster_count
    if weak_signal and cluster_count:
        redundancy_penalty += 0.12
    entity_pressure = _entity_coverage_pressure(model, covered)
    role_pressure = _role_coverage_pressure(model, covered)
    if entity_pressure:
        redundancy_penalty += (0.012 if answer_novelty else 0.04) * entity_pressure
    if role_pressure and not broad_signal and not answer_novelty:
        redundancy_penalty += 0.03 * role_pressure
    return rel.final_score + min(0.28, novelty) - redundancy_penalty


def _has_uncovered_answer_obligation(
    model: ModelRow,
    covered: Counter[str],
    *,
    model_questions: set[str] | None = None,
) -> bool:
    for feature in _model_answer_obligation_features(model, model_questions or set()):
        if covered[feature] <= 0:
            return True
    return False


def _model_answer_obligation_features(
    model: ModelRow,
    model_questions: set[str],
) -> tuple[str, ...]:
    """Coarse answer slots a Model can satisfy for coverage-aware stopping."""
    features: list[str] = []

    def add(value: str) -> None:
        clean = value.strip()
        if clean and clean not in features:
            features.append(clean)

    belief_address = belief_address_from_model_like(model)
    role = str(
        getattr(model, "claim_role", "") or belief_address.get("claim_role") or ""
    ).casefold()
    level = str(
        getattr(model, "abstraction_level", "")
        or belief_address.get("abstraction_level")
        or ""
    ).casefold()
    polarity = str(
        getattr(model, "polarity", "") or belief_address.get("polarity") or ""
    ).casefold()
    primitives = tuple(
        str(primitive).upper()
        for primitive in (belief_address.get("answerable_primitives") or ())
    )

    for primitive in primitives[:6]:
        add(f"answer_slot:primitive:{primitive}")
        if role:
            add(f"answer_slot:role_primitive:{role}:{primitive}")
    if role:
        add(f"answer_slot:role:{role}")
    if level in {"relationship", "composite"}:
        add(f"answer_slot:level:{level}")
    for entity_type, entity_id in sorted(
        _canonical_entity_pairs(getattr(model, "scope_entities", []) or [])
    )[:8]:
        add(f"answer_slot:entity:{entity_type}:{entity_id}")
    for question in sorted(str(question) for question in model_questions)[:6]:
        add(f"answer_slot:question:{question}")
    for key in tuple(belief_address.get("obligation_keys") or ())[:12]:
        key_text = str(key)
        if key_text.startswith(("spo:", "qualifier:")):
            add(f"answer_slot:object_obligation:{key_text[:140]}")

    structural = _is_structural_detail_model(model)
    link_tokens: set[str] = set()
    for raw in getattr(model, "supporting_model_ids", []) or ():
        try:
            link_tokens.add(str(UUID(str(raw))))
        except (TypeError, ValueError):
            continue
    link_tokens.update(str(mid) for mid in _model_member_ids(model))
    if structural:
        add("answer_slot:structural_detail")
        for linked_id in sorted(link_tokens)[:4]:
            add(f"answer_slot:structural_link:{linked_id}")

    text = " ".join(
        str(part)
        for part in (
            getattr(model, "natural", "") or "",
            json.dumps(getattr(model, "proposition", {}) or {}, default=str),
        )
    ).casefold()
    if polarity == "mixed" or _has_counterevidence_qualifier_language(text):
        subject = str(belief_address.get("subject") or "").strip().casefold()
        add(f"answer_slot:counterevidence:{subject[:96] or 'linked'}")

    subject = str(belief_address.get("subject") or "").strip().casefold()
    predicate = str(belief_address.get("predicate") or "").strip().casefold()
    if subject and (
        structural
        or not getattr(model, "scope_entities", None)
        or role
        in {"pattern", "prediction", "recommendation", "capability", "situation"}
    ):
        add(f"answer_slot:subject:{subject[:96]}")
        if predicate:
            add(f"answer_slot:subject_predicate:{subject[:96]}:{predicate[:64]}")
    return tuple(features)


def _model_coverage_features(
    model: ModelRow,
    model_pathways: set[str],
    model_questions: set[str],
) -> list[tuple[str, float]]:
    features: list[tuple[str, float]] = []
    belief_address = belief_address_from_model_like(model)
    fingerprint = str(belief_address.get("fingerprint") or "").strip()
    if fingerprint:
        features.append((f"belief_fingerprint:{fingerprint}", 0.055))
    for key in tuple(belief_address.get("obligation_keys") or ())[:12]:
        features.append((f"belief_obligation:{key}", 0.095))
    for primitive in tuple(belief_address.get("answerable_primitives") or ())[:6]:
        features.append((f"answerable:{primitive}", 0.045))
    for feature in _model_answer_obligation_features(model, model_questions):
        features.append((feature, 0.075))
    kind = getattr(model, "proposition_kind", None)
    if kind:
        features.append((f"kind:{kind}", 0.035))
    role = getattr(model, "claim_role", None)
    if role:
        features.append((f"role:{role}", 0.055))
    level = getattr(model, "abstraction_level", None)
    if level:
        features.append((f"level:{level}", 0.025))
    time_mode = getattr(model, "time_mode", None)
    if time_mode:
        features.append((f"time:{time_mode}", 0.02))
    polarity = getattr(model, "polarity", None)
    if polarity:
        features.append((f"polarity:{polarity}", 0.02))
    for tag in sorted(str(tag) for tag in (getattr(model, "domain_tags", []) or []))[
        :5
    ]:
        features.append((f"domain:{tag}", 0.035))
    for entity_type, entity_id in sorted(
        _canonical_entity_pairs(getattr(model, "scope_entities", []) or [])
    )[:8]:
        features.append((f"entity:{entity_type}:{entity_id}", 0.075))
        features.append((f"entity_type:{entity_type}", 0.025))
    for actor_id in sorted(
        str(actor) for actor in (getattr(model, "scope_actors", []) or [])
    )[:4]:
        features.append((f"actor:{actor_id}", 0.035))
    for support_id in sorted(
        str(mid) for mid in (getattr(model, "supporting_model_ids", []) or [])
    )[:4]:
        features.append((f"support:{support_id}", 0.06))
    for path in sorted(str(path) for path in model_pathways)[:6]:
        features.append((f"path:{path}", 0.04))
    for question in sorted(str(question) for question in model_questions)[:6]:
        features.append((f"question:{question}", 0.035))
    if not features:
        token_key = tuple(
            sorted(_relevance_tokens(getattr(model, "natural", "") or ""))[:3]
        )
        if token_key:
            features.append((f"text:{token_key}", 0.02))
    return features


def _entity_coverage_pressure(model: ModelRow, covered: Counter[str]) -> int:
    pairs = _canonical_entity_pairs(getattr(model, "scope_entities", []) or [])
    return max(
        (
            covered[f"entity:{entity_type}:{entity_id}"]
            for entity_type, entity_id in pairs
        ),
        default=0,
    )


def _role_coverage_pressure(model: ModelRow, covered: Counter[str]) -> int:
    role = getattr(model, "claim_role", None)
    if role:
        return covered[f"role:{role}"]
    kind = getattr(model, "proposition_kind", None)
    if kind:
        return covered[f"kind:{kind}"]
    return 0


def _model_relevance_cluster_key(model: ModelRow) -> tuple[Any, ...]:
    entities = sorted(
        _canonical_entity_pairs(getattr(model, "scope_entities", []) or [])
    )[:3]
    belief_address = belief_address_from_model_like(model)
    fingerprint = str(belief_address.get("fingerprint") or "").strip()
    if fingerprint:
        return (
            getattr(model, "proposition_kind", None),
            tuple(entities),
            fingerprint,
        )
    text_tokens = sorted(_relevance_tokens(getattr(model, "natural", "") or ""))[:4]
    return (
        getattr(model, "proposition_kind", None),
        tuple(entities),
        tuple(text_tokens),
    )


def _add_result_to_reservoir(
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
    result: RetrievalResult,
    *,
    path: str,
    question_id: str,
    hypotheses: tuple[Hypothesis, ...],
    score_hint: float = 0.0,
) -> None:
    trigger_text = _trigger_text(result.trigger)
    for model in result.models:
        _upsert_evidence(
            evidence_by_key,
            key=("model", str(model.id)),
            source_type="model",
            source_ref_id=model.id,
            summary=model.natural or json.dumps(model.proposition, default=str),
            trust_tier="model",
            timestamp=model.created_at,
            path=path,
            question_id=question_id,
            hypotheses=hypotheses,
            score=score_hint + float(result.model_scores.get(model.id, 0.0)),
            raw_content_ref=f"model:{model.id}",
            trigger_text=trigger_text,
        )
    for obs in result.observations:
        _upsert_evidence(
            evidence_by_key,
            key=("observation", str(obs.id)),
            source_type="observation",
            source_ref_id=obs.id,
            summary=obs.content_text,
            trust_tier=obs.trust_tier,
            timestamp=obs.occurred_at,
            path=path,
            question_id=question_id,
            hypotheses=hypotheses,
            score=score_hint + _trust_score(obs.trust_tier),
            raw_content_ref=f"observation:{obs.id}",
            trigger_text=trigger_text,
        )
    for kind, rows in result.acts.items():
        for row in rows:
            title = getattr(row, "title", None) or str(getattr(row, "id", ""))
            if kind == "commitments":
                owner = getattr(row, "owner_id", None)
                title = (
                    f"{title} owner={owner}" if owner else f"{title} owner=unassigned"
                )
            _upsert_evidence(
                evidence_by_key,
                key=(kind.rstrip("s"), str(row.id)),
                source_type=kind.rstrip("s"),
                source_ref_id=row.id,
                summary=f"{kind.rstrip('s')} {title}",
                trust_tier="authoritative",
                timestamp=getattr(row, "last_state_change_at", None)
                or getattr(row, "created_at", None),
                path=path,
                question_id=question_id,
                hypotheses=hypotheses,
                score=score_hint + 0.4,
                raw_content_ref=f"{kind.rstrip('s')}:{row.id}",
                trigger_text=trigger_text,
            )
    for res in result.resources:
        _upsert_evidence(
            evidence_by_key,
            key=("resource", str(res.id)),
            source_type="resource",
            source_ref_id=res.id,
            summary=f"{res.kind} resource {res.identity}: {res.description or ''}",
            trust_tier="authoritative",
            timestamp=res.last_updated_at,
            path=path,
            question_id=question_id,
            hypotheses=hypotheses,
            score=score_hint + 0.32,
            raw_content_ref=f"resource:{res.id}",
            trigger_text=trigger_text,
        )


def _upsert_evidence(
    evidence_by_key: dict[tuple[str, str], EvidenceCard],
    *,
    key: tuple[str, str],
    source_type: str,
    source_ref_id: UUID | None,
    summary: str,
    trust_tier: str | None,
    timestamp: datetime | None,
    path: str,
    question_id: str,
    hypotheses: tuple[Hypothesis, ...],
    score: float,
    raw_content_ref: str,
    trigger_text: str | None = None,
) -> None:
    supports, weakens, contradicts = _classify_hypothesis_links(
        summary,
        hypotheses,
        trigger_text=trigger_text,
    )
    if key not in evidence_by_key:
        evidence_by_key[key] = EvidenceCard(
            evidence_id=uuid7(),
            source_type=source_type,
            source_ref=f"{source_type}:{key[1]}",
            source_ref_id=source_ref_id,
            summary=_compact(summary, 700),
            trust_tier=trust_tier,
            timestamp=timestamp,
            raw_content_ref=raw_content_ref,
            token_estimate=_estimate_tokens(summary),
            sensitivity=_sensitivity(summary),
        )
    evidence_by_key[key].merge(
        path=path,
        question_id=question_id,
        supports=supports,
        weakens=weakens,
        contradicts=contradicts,
        score=score,
    )


__all__ = [
    "_add_result_to_reservoir",
    "_append_structural_closure",
    "_apply_relevance_diversity",
    "_canonical_entity_pairs",
    "_coverage_compaction_target",
    "_coverage_selection_utility",
    "_entity_coverage_pressure",
    "_has_counterevidence_qualifier_language",
    "_has_selected_model_link",
    "_has_uncovered_answer_obligation",
    "_is_structural_detail_model",
    "_lexical_relevance_score",
    "_linked_anchor_ids",
    "_merge_results",
    "_model_answer_obligation_features",
    "_model_coverage_features",
    "_model_evidence_relevance_score",
    "_model_member_ids",
    "_model_relevance_cluster_key",
    "_pack_structural_links",
    "_relevance_diversity_candidate_cap",
    "_result_from_pathway",
    "_role_coverage_pressure",
    "_scope_relevance_score",
    "_score_model_relevance",
    "_select_relevant_models",
    "_structural_closure_reason",
    "_upsert_evidence",
]
