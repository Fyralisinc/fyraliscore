"""Reconstructive memory-state helpers for adaptive inquiry retrieval."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Any

from services.reasoning.retrieval.primary import TriggerContext

from .evidence_utils import compact, material_tokens, timestamp_sort_value
from .types import (
    EvidenceCard,
    Hypothesis,
    QuestionAnswer,
    ReconstructionState,
    RetrievalAction,
)

_CHEAP_STAGE_PATHS = frozenset({"focused_index", "structural", "model_edge"})
_BINDABLE_STAGE_PATHS = frozenset({"semantic", "temporal", "model_edge"})
_MAX_CUES = 8
_MAX_RECENT_EVIDENCE = 5
_MAX_ACTION_CUES = 4
_MAX_READER_CUES = 4
_MAX_PLANNER_CUES = 4
_MAX_READER_KNOWN_IDS = 8
_MAX_CONTEXT_KNOWN_IDS = 12
_GENERIC_CUES = frozenset(
    {
        "counterevidence",
        "constraint",
        "constraints",
        "dependency",
        "evidence",
        "goal",
        "impact",
        "issue",
        "model",
        "owner",
        "ownership",
        "premise",
        "question",
        "recurrence",
        "resource",
        "risk",
        "signal",
        "support",
    }
)
_VALUE_PURPOSES = ("planner", "reader", "actions")


def build_reconstruction_state(
    *,
    trigger: TriggerContext,
    hypotheses: tuple[Hypothesis, ...],
    evidence: list[EvidenceCard] | tuple[EvidenceCard, ...],
    answers: list[QuestionAnswer] | tuple[QuestionAnswer, ...],
    unknowns: set[str] | frozenset[str] | tuple[str, ...],
    round_index: int,
) -> ReconstructionState:
    """Summarize accumulated evidence into the next memory-access state."""
    ordered = sorted(
        evidence,
        key=lambda card: (
            -float(card.score),
            -timestamp_sort_value(card.timestamp),
            card.source_type,
            card.source_ref,
        ),
    )
    unresolved = tuple(sorted({str(slot) for slot in unknowns if str(slot).strip()}))
    active_cues = _active_cues(trigger, ordered, unresolved)
    active_tags = _active_tags(ordered)
    known_model_ids = tuple(
        str(card.source_ref_id)
        for card in ordered
        if card.source_type == "model" and card.source_ref_id is not None
    )[:24]
    known_observation_ids = tuple(
        str(card.source_ref_id)
        for card in ordered
        if card.source_type == "observation" and card.source_ref_id is not None
    )[:24]
    supporting_refs = tuple(
        card.source_ref for card in ordered if card.supports_hypotheses
    )[:24]
    counter_refs = tuple(
        card.source_ref
        for card in ordered
        if card.weakens_hypotheses or card.contradicts_hypotheses
    )[:24]
    answered = tuple(
        answer.question_id
        for answer in answers
        if answer.answer_status in {"supported", "partially_supported"}
    )
    inconclusive = tuple(
        answer.question_id
        for answer in answers
        if answer.answer_status in {"unanswered", "inconclusive"}
    )
    recent = tuple(_evidence_summary(card) for card in ordered[:_MAX_RECENT_EVIDENCE])
    summary = _state_summary(
        unresolved=unresolved,
        active_cues=active_cues,
        supporting=len(supporting_refs),
        counter=len(counter_refs),
        recent=recent,
    )
    return ReconstructionState(
        round_index=max(0, int(round_index)),
        summary=summary,
        active_cues=active_cues,
        active_tags=active_tags,
        unresolved_slots=unresolved,
        known_model_ids=_dedupe_strings(known_model_ids),
        known_observation_ids=_dedupe_strings(known_observation_ids),
        supporting_refs=_dedupe_strings(supporting_refs),
        counterevidence_refs=_dedupe_strings(counter_refs),
        answered_questions=answered,
        inconclusive_questions=inconclusive,
        operator_bias=_operator_bias(unresolved, ordered, answers),
        hypothesis_status=_hypothesis_status(hypotheses, ordered),
        recent_evidence=recent,
    )


def reconstruction_state_payload(state: ReconstructionState | None) -> dict[str, Any]:
    if state is None:
        return {}
    payload = {
        "round_index": state.round_index,
        "summary": compact(state.summary, 700),
        "active_cues": list(state.active_cues[:_MAX_CUES]),
        "active_tags": list(state.active_tags[:12]),
        "unresolved_slots": list(state.unresolved_slots[:6]),
        "known_model_ids": list(state.known_model_ids[:_MAX_CONTEXT_KNOWN_IDS]),
        "known_model_count": len(state.known_model_ids),
        "known_observation_ids": list(
            state.known_observation_ids[:_MAX_CONTEXT_KNOWN_IDS]
        ),
        "known_observation_count": len(state.known_observation_ids),
        "supporting_ref_count": len(state.supporting_refs),
        "counterevidence_ref_count": len(state.counterevidence_refs),
        "answered_questions": list(state.answered_questions[:8]),
        "inconclusive_questions": list(state.inconclusive_questions[:8]),
        "operator_bias": list(state.operator_bias[:6]),
        "hypothesis_status": _compact_hypothesis_status(state.hypothesis_status),
        "recent_evidence": [
            _compact_recent_evidence(item, summary_limit=140)
            for item in state.recent_evidence[:4]
        ],
    }
    return payload


def planner_reconstruction_payload(
    state: ReconstructionState | None,
) -> dict[str, Any]:
    """Purpose-built state for the question planner prompt."""
    if state is None:
        return {}
    return {
        "round_index": state.round_index,
        "summary": compact(state.summary, 420),
        "active_cues": list(state.active_cues[:_MAX_PLANNER_CUES]),
        "unresolved_slots": list(state.unresolved_slots[:4]),
        "inconclusive_questions": list(state.inconclusive_questions[:4]),
        "operator_bias": list(state.operator_bias[:4]),
        "known_model_count": len(state.known_model_ids),
        "known_observation_count": len(state.known_observation_ids),
        "counterevidence_ref_count": len(state.counterevidence_refs),
        "hypothesis_status": _compact_hypothesis_status(
            state.hypothesis_status,
            limit=3,
            needs_limit=2,
        ),
        "recent_evidence": [
            _compact_recent_evidence(item, summary_limit=100)
            for item in state.recent_evidence[:2]
        ],
    }


def reconstruction_state_note(
    state: ReconstructionState | None,
    *,
    trigger: TriggerContext | None = None,
) -> dict[str, Any]:
    if state is None:
        return {}
    gates = reconstruction_gate_decision(state, trigger=trigger)
    return {
        "round_index": state.round_index,
        "active_cues": list(state.active_cues),
        "active_tags": list(state.active_tags),
        "unresolved_slots": list(state.unresolved_slots),
        "known_model_count": len(state.known_model_ids),
        "known_observation_count": len(state.known_observation_ids),
        "operator_bias": list(state.operator_bias),
        "recent_evidence_count": len(state.recent_evidence),
        "payload_chars": serialized_payload_size(reconstruction_state_payload(state)),
        "planner_payload_chars": serialized_payload_size(
            planner_reconstruction_payload(state)
        ),
        "reader_payload_chars": serialized_payload_size(
            reader_reconstruction_payload(state)
        ),
        "gates": gates,
    }


def reader_reconstruction_payload(
    state: ReconstructionState | None,
) -> dict[str, Any]:
    """Purpose-built state for SAGE reader cue/intent extraction."""
    if state is None:
        return {}
    return {
        "summary": state.summary,
        "known_model_ids": list(state.known_model_ids[:_MAX_READER_KNOWN_IDS]),
        "known_model_count": len(state.known_model_ids),
        "known_observation_ids": list(
            state.known_observation_ids[:_MAX_READER_KNOWN_IDS]
        ),
        "known_observation_count": len(state.known_observation_ids),
        "active_cues": list(state.active_cues[:_MAX_READER_CUES]),
        "active_tags": list(state.active_tags[:6]),
        "unresolved_slots": list(state.unresolved_slots[:4]),
        "operator_bias": list(state.operator_bias[:4]),
        "counterevidence_ref_count": len(state.counterevidence_refs),
        "recent_evidence": [
            _compact_recent_evidence(item, summary_limit=120)
            for item in state.recent_evidence[:3]
        ],
        "payload_kind": "reader_compact",
        "reconstruction_state": {
            "round_index": state.round_index,
            "summary": compact(state.summary, 320),
            "active_cues": list(state.active_cues[:_MAX_READER_CUES]),
            "unresolved_slots": list(state.unresolved_slots[:4]),
            "known_model_count": len(state.known_model_ids),
            "known_observation_count": len(state.known_observation_ids),
        },
    }


def evidence_state_for_reader(
    state: ReconstructionState | None,
) -> dict[str, Any] | None:
    if state is None:
        return None
    return reader_reconstruction_payload(state)


def reconstruction_state_for_purpose(
    state: ReconstructionState | None,
    *,
    trigger: TriggerContext | None = None,
    purpose: str,
) -> ReconstructionState | None:
    decision = reconstruction_gate_decision(state, trigger=trigger)
    if decision.get(purpose, {}).get("enabled"):
        return state
    return None


def reconstruction_gate_decision(
    state: ReconstructionState | None,
    *,
    trigger: TriggerContext | None = None,
) -> dict[str, Any]:
    if state is None:
        return {
            purpose: {"enabled": False, "reason": "missing_state"}
            for purpose in _VALUE_PURPOSES
        }
    flags = _value_flags(state, trigger)
    decisions: dict[str, Any] = {}
    for purpose in _VALUE_PURPOSES:
        enabled, reason = _purpose_gate(state, flags, purpose)
        decisions[purpose] = {
            "enabled": enabled,
            "reason": reason,
        }
        if purpose == "actions":
            decisions[purpose]["cue_count"] = len(_action_cues(state))
    decisions["flags"] = flags
    return decisions


def serialized_payload_size(payload: dict[str, Any] | None) -> int:
    if not payload:
        return 0
    return len(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")))


def apply_reconstruction_to_actions(
    actions: list[RetrievalAction],
    *,
    state: ReconstructionState | None,
) -> list[RetrievalAction]:
    """Make later-round retrieval stateful without changing first-round breadth."""
    if state is None or state.round_index <= 1:
        return actions
    cues = list(_action_cues(state))
    if not cues:
        return actions
    out: list[RetrievalAction] = []
    for action in actions:
        filters = dict(action.filters)
        filters["_reconstruction_stage"] = _stage_for_action(action)
        filters["_reconstruction_active_cues"] = cues
        filters["_reconstruction_cue_count"] = len(cues)
        filters["_reconstruction_round"] = state.round_index
        if (
            filters["_reconstruction_stage"] > 1
            and action.path in _BINDABLE_STAGE_PATHS
        ):
            filters["_bind_previous_scope"] = True
        query = (
            _query_with_cues(action.query, cues)
            if action.path == "semantic"
            else action.query
        )
        if action.path == "focused_index":
            terms = filters.get("terms")
            raw_terms = [str(term) for term in terms] if isinstance(terms, list) else []
            filters["terms"] = list(_dedupe_strings([*raw_terms, *cues])[:8])
        out.append(replace(action, query=query, filters=filters))
    return out


def _active_cues(
    trigger: TriggerContext,
    evidence: list[EvidenceCard],
    unresolved: tuple[str, ...],
) -> tuple[str, ...]:
    raw: list[str] = []
    raw.extend(_seed_entity_labels(trigger))
    raw.extend(unresolved)
    for card in evidence[:8]:
        raw.extend(sorted(material_tokens(card.summary.casefold()))[:6])
    return _dedupe_strings(
        cue
        for cue in raw
        if 2 <= len(cue) <= 48 and cue not in {"evidence", "model", "question"}
    )[:_MAX_CUES]


def _seed_entity_labels(trigger: TriggerContext) -> list[str]:
    labels: list[str] = []
    for entity in trigger.seed_entity_ids or []:
        if isinstance(entity, dict):
            value = entity.get("label") or entity.get("name") or entity.get("type")
            if value:
                labels.append(str(value))
        elif entity is not None:
            labels.append(str(entity))
    return labels


def _active_tags(evidence: list[EvidenceCard]) -> tuple[str, ...]:
    tags: list[str] = []
    for card in evidence:
        tags.append(card.source_type)
        tags.extend(sorted(card.retrieval_paths))
        if card.supports_hypotheses:
            tags.append("support")
        if card.weakens_hypotheses:
            tags.append("weakener")
        if card.contradicts_hypotheses:
            tags.append("contradiction")
    return _dedupe_strings(tags)[:16]


def _operator_bias(
    unresolved: tuple[str, ...],
    evidence: list[EvidenceCard],
    answers: list[QuestionAnswer] | tuple[QuestionAnswer, ...],
) -> tuple[str, ...]:
    text = " ".join([*unresolved, *(answer.summary for answer in answers)]).casefold()
    bias: list[str] = []
    if "counterevidence" in text or "premise" in text:
        bias.extend(["semantic:counterevidence", "temporal:recent_counterevidence"])
    if "owner" in text or "responsible" in text:
        bias.extend(["structural:ownership_graph", "semantic:owner_evidence"])
    if "constraint" in text or "resource" in text:
        bias.extend(
            ["model_edge:constraint_resource_edges", "semantic:constraint_evidence"]
        )
    if evidence and not any(card.source_type == "observation" for card in evidence[:8]):
        bias.append("temporal:recent_observations")
    return _dedupe_strings(bias)[:8]


def _hypothesis_status(
    hypotheses: tuple[Hypothesis, ...],
    evidence: list[EvidenceCard],
) -> dict[str, dict[str, Any]]:
    status: dict[str, dict[str, Any]] = {}
    for hypothesis in hypotheses:
        support = sum(
            1 for card in evidence if hypothesis.id in card.supports_hypotheses
        )
        weak = sum(1 for card in evidence if hypothesis.id in card.weakens_hypotheses)
        contradict = sum(
            1 for card in evidence if hypothesis.id in card.contradicts_hypotheses
        )
        status[hypothesis.id] = {
            "support": support,
            "weakens": weak,
            "contradicts": contradict,
            "needs": list(hypothesis.evidence_needed[:4]),
        }
    return status


def _evidence_summary(card: EvidenceCard) -> dict[str, Any]:
    return {
        "source_type": card.source_type,
        "source_ref": card.source_ref,
        "summary": compact(card.summary, 180),
        "paths": sorted(card.retrieval_paths),
        "supports": sorted(card.supports_hypotheses),
        "weakens": sorted(card.weakens_hypotheses),
        "contradicts": sorted(card.contradicts_hypotheses),
        "score": round(float(card.score), 4),
    }


def _state_summary(
    *,
    unresolved: tuple[str, ...],
    active_cues: tuple[str, ...],
    supporting: int,
    counter: int,
    recent: tuple[dict[str, Any], ...],
) -> str:
    parts = [
        f"unresolved: {', '.join(unresolved[:6]) or 'none'}",
        f"active cues: {', '.join(active_cues[:8]) or 'none'}",
        f"supporting refs: {supporting}",
        f"counter refs: {counter}",
    ]
    if recent:
        parts.append(
            "recent evidence: "
            + "; ".join(str(item.get("summary") or "") for item in recent[:3])
        )
    return compact(" | ".join(parts), 900)


def _stage_for_action(action: RetrievalAction) -> int:
    return 1 if action.path in _CHEAP_STAGE_PATHS else 2


def _query_with_cues(query: str | None, cues: list[str]) -> str | None:
    if not query:
        return " ".join(cues) or None
    lower = query.casefold()
    additions = [cue for cue in cues if cue.casefold() not in lower]
    if not additions:
        return query
    return f"{query} {' '.join(additions)}".strip()


def _compact_recent_evidence(
    item: dict[str, Any],
    *,
    summary_limit: int,
) -> dict[str, Any]:
    return {
        "source_type": item.get("source_type"),
        "source_ref": compact(item.get("source_ref"), 80),
        "summary": compact(item.get("summary"), summary_limit),
        "paths": list(item.get("paths") or [])[:3],
        "supports": list(item.get("supports") or [])[:3],
        "weakens": list(item.get("weakens") or [])[:3],
        "contradicts": list(item.get("contradicts") or [])[:3],
        "score": item.get("score"),
    }


def _compact_hypothesis_status(
    status: dict[str, dict[str, Any]],
    *,
    limit: int = 4,
    needs_limit: int = 3,
) -> dict[str, dict[str, Any]]:
    compacted: dict[str, dict[str, Any]] = {}
    for hypothesis_id, item in list(status.items())[:limit]:
        compacted[str(hypothesis_id)] = {
            "support": int(item.get("support") or 0),
            "weakens": int(item.get("weakens") or 0),
            "contradicts": int(item.get("contradicts") or 0),
            "needs": list(item.get("needs") or [])[:needs_limit],
        }
    return compacted


def _value_flags(
    state: ReconstructionState,
    trigger: TriggerContext | None,
) -> dict[str, bool]:
    has_hypothesis_tension = any(
        int(item.get("weakens") or 0) > 0 or int(item.get("contradicts") or 0) > 0
        for item in state.hypothesis_status.values()
    )
    return {
        "later_round": state.round_index > 1,
        "non_t1_trigger": bool(trigger is not None and trigger.kind != "T1"),
        "has_prior_scope": bool(state.known_model_ids or state.known_observation_ids),
        "has_unresolved": bool(state.unresolved_slots or state.inconclusive_questions),
        "has_counterevidence": bool(state.counterevidence_refs or has_hypothesis_tension),
        "has_action_cues": bool(_action_cues(state)),
    }


def _purpose_gate(
    state: ReconstructionState,
    flags: dict[str, bool],
    purpose: str,
) -> tuple[bool, str]:
    if purpose == "actions" and not flags["later_round"]:
        return False, "first_round_actions_stay_parallel"
    if not state.active_cues and not flags["has_prior_scope"]:
        return False, "empty_state"
    if purpose == "actions" and not flags["has_action_cues"]:
        return False, "no_specific_action_cues"
    if purpose == "actions":
        enabled = flags["has_prior_scope"] or flags["has_unresolved"] or flags[
            "has_counterevidence"
        ]
        return (enabled, "valuable_later_round_state" if enabled else "low_value")
    if purpose == "reader":
        enabled = (
            flags["later_round"]
            or flags["non_t1_trigger"]
            or flags["has_unresolved"]
            or flags["has_counterevidence"]
        ) and (
            flags["has_prior_scope"]
            or flags["non_t1_trigger"]
            or flags["later_round"]
        )
        return (enabled, "reader_has_scope_or_open_slots" if enabled else "low_value")
    if purpose == "planner":
        enabled = (
            flags["later_round"]
            or flags["non_t1_trigger"]
            or flags["has_unresolved"]
            or flags["has_counterevidence"]
        ) and (
            flags["has_prior_scope"]
            or bool(state.answered_questions)
            or bool(state.inconclusive_questions)
            or flags["non_t1_trigger"]
        )
        return (
            enabled,
            "planner_has_unresolved_frontier" if enabled else "low_value",
        )
    return False, "unknown_purpose"


def _action_cues(state: ReconstructionState) -> tuple[str, ...]:
    return _dedupe_strings(
        cue for cue in state.active_cues if _is_specific_action_cue(cue)
    )[:_MAX_ACTION_CUES]


def _is_specific_action_cue(cue: str) -> bool:
    text = str(cue or "").strip()
    if not text:
        return False
    folded = text.casefold()
    if folded in _GENERIC_CUES:
        return False
    parts = [part for part in material_tokens(folded) if part not in _GENERIC_CUES]
    if parts:
        return True
    return any(ch.isupper() for ch in text) or any(ch.isdigit() for ch in text)


def _dedupe_strings(values: Any) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return tuple(out)


__all__ = [
    "apply_reconstruction_to_actions",
    "build_reconstruction_state",
    "evidence_state_for_reader",
    "planner_reconstruction_payload",
    "reader_reconstruction_payload",
    "reconstruction_gate_decision",
    "reconstruction_state_for_purpose",
    "reconstruction_state_note",
    "reconstruction_state_payload",
    "serialized_payload_size",
]
