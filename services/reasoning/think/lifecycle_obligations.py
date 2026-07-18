"""Batch-aware lifecycle obligations for Think.

Event batches compress many signals into one prompt. That is good for cost, but
it makes rare write surfaces easy to miss: prediction deadlines, resource
constraints, question-policy learning, evidence attachment, and memory review
debt. This module adds narrow deterministic obligations when the batch contains
explicit lifecycle language, then lets the normal validator/applier path decide
what can be persisted.
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext

from .diff_schema import (
    ClaimOp,
    MemoryLifecycleOp,
    OpenQuestionOp,
    RawDiff,
    ResourceOp,
)


_PREDICTION_RE = re.compile(
    r"(?is)\b("
    r"forecast|predict(?:ion|ed)?|expected|likely|eta|deadline|due|"
    r"target(?:ing)?|"
    r"will\s+(?:ship|slip|delay|miss|deliver|merge|deploy|launch|"
    r"renew|churn|finish|complete|move|close|resolve|happen)|"
    r"by\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday|"
    r"tomorrow|today|tonight|next\s+week|\d{1,2}(?::\d{2})?)"
    r")\b"
)
_RESOURCE_RE = re.compile(
    r"(?is)\b("
    r"capacity|bandwidth|budget|hours?|quota|limit|resource|availability|"
    r"staff(?:ing)?|headcount|constrained|overloaded|under[-\s]?resourced|"
    r"depleted|exhausted|no\s+room|down\s+to\s+\w+\s+hours?"
    r")\b"
)
_QUESTION_POLICY_RE = re.compile(
    r"(?is)\b("
    r"missing\s+context|needs?\s+clarification|unclear|unknown|ambiguous|"
    r"who\s+owns|which\s+owner|owner\s+(?:is\s+)?(?:unclear|unknown|missing)|"
    r"approval\s+owner|approver\s+(?:is\s+)?(?:unclear|unknown|missing)|"
    r"confirm\s+before|ask\s+before|source\s+of\s+truth"
    r")\b"
)
_EVIDENCE_ATTACHMENT_RE = re.compile(
    r"(?is)\b("
    r"felt|feels|review|retro|feedback|concern|worried|rough|pushback|"
    r"complaint|friction|again|repeated|yesterday|today|sentiment"
    r")\b"
)
_STALE_RE = re.compile(
    r"(?is)\b("
    r"stale|outdated|obsolete|no\s+longer\s+true|superseded|replaced|"
    r"retire|archive|changed\s+since"
    r")\b"
)
_AMBIGUITY_RE = re.compile(
    r"(?is)\b("
    r"alias|same\s+as|different\s+from|not\s+the\s+same|same\s+customer|"
    r"counterfactual|what\s+if|if\s+.+\s+had|ambiguit(?:y|ies)|ambiguous"
    r")\b"
)
_RELATIVE_DEADLINE_RE = re.compile(
    r"(?i)\b(?:in|within)\s+"
    r"(a|an|one|two|three|four|five|six|seven|eight|nine|ten|\d+)\s+"
    r"(day|days|week|weeks|month|months)\b"
)
_WEEKDAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}
_NUMBER_WORDS = {
    "a": 1,
    "an": 1,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
}

_SYNTHESIS_PHASES = (
    "weak_initial",
    "corroboration",
    "contradiction",
    "correction",
    "external_outcome",
)


def maybe_inject_synthesis_evolution_obligations(
    raw_diff: RawDiff,
    bundle: ContextBundle,
) -> RawDiff:
    """Deterministically evolve a same-scope synthesis on explicit phase evidence.

    Only an inserted atomic with an explicit ``lifecycle_phase`` can trigger this
    path.  Batch membership is never evidence, and scope-local exactness is
    required before the existing synthesis is touched.
    """

    existing_keys = {
        (op.model_id, str(op.metadata.get("synthesis_phase_transition") or ""))
        for op in raw_diff.memory_lifecycle_ops
    }
    additions: list[MemoryLifecycleOp] = []
    for claim_op in raw_diff.claim_ops:
        if claim_op.op != "insert" or not isinstance(claim_op.entry, dict):
            continue
        entry = claim_op.entry
        proposition = entry.get("proposition")
        if not isinstance(proposition, dict):
            continue
        if proposition.get("claim_role") in {"situation", "synthesis"}:
            continue
        phase = str(proposition.get("lifecycle_phase") or "")
        if phase not in _SYNTHESIS_PHASES:
            continue
        atomic_id = _uuid_or_none(entry.get("id") or entry.get("model_id"))
        if atomic_id is None:
            continue
        atomic_scope = _scope_identity(entry.get("scope_entities"))
        if not atomic_scope:
            continue
        event_ids = [
            value for value in (
                _uuid_or_none(raw) for raw in (
                    entry.get("supporting_event_ids") or ()
                )
            ) if value is not None
        ]
        for model in _active_models(bundle):
            model_id = _uuid_or_none(getattr(model, "id", None))
            current = getattr(model, "proposition", None)
            if model_id is None or not isinstance(current, dict):
                continue
            if current.get("claim_role") not in {"situation", "synthesis"}:
                continue
            if _scope_identity(getattr(model, "scope_entities", None)) != atomic_scope:
                continue
            history = [
                value for value in current.get("lifecycle_phase_history") or ()
                if value in _SYNTHESIS_PHASES
            ]
            current_phase = str(current.get("current_lifecycle_phase") or "")
            if current_phase in _SYNTHESIS_PHASES and current_phase not in history:
                history.append(current_phase)
            if phase in history:
                continue
            prior_rank = max(
                (_SYNTHESIS_PHASES.index(value) for value in history), default=-1
            )
            if _SYNTHESIS_PHASES.index(phase) <= prior_rank:
                continue
            key = (model_id, phase)
            if key in existing_keys:
                continue
            members = list(dict.fromkeys([
                *(str(value) for value in current.get("member_model_ids") or ()),
                str(atomic_id),
            ]))
            next_proposition = {
                **current,
                "member_model_ids": members,
                "lifecycle_phase_history": [*history, phase],
                "current_lifecycle_phase": phase,
                "lifecycle_state": {
                    "contradiction": "contested",
                    "correction": "revised",
                    "external_outcome": "resolved",
                }.get(phase, "active"),
            }
            additions.append(MemoryLifecycleOp(
                model_id=model_id,
                action="revise",
                evidence_event_ids=event_ids,
                claim_local_evidence_event_ids=event_ids,
                evidence_model_ids=[atomic_id],
                rationale=(
                    f"Exact same-scope atomic evidence advances the coherent "
                    f"synthesis from {current_phase or 'unphased'} to {phase}."
                ),
                reason=f"synthesis_phase_transition:{phase}",
                metadata={
                    "source": "deterministic_synthesis_evolution",
                    "synthesis_phase_transition": phase,
                    "prior_phase": current_phase or None,
                    "next_proposition": next_proposition,
                    "exact_atomic_model_id": str(atomic_id),
                },
            ))
            existing_keys.add(key)
            break
    if not additions:
        return raw_diff
    trace = (raw_diff.reasoning_trace or "").rstrip()
    note = "synthesis_evolution: " + ", ".join(
        str(op.metadata["synthesis_phase_transition"]) for op in additions
    )
    return raw_diff.model_copy(update={
        "memory_lifecycle_ops": [*raw_diff.memory_lifecycle_ops, *additions],
        "reasoning_trace": f"{trace}\n{note}".strip(),
    })


def _scope_identity(value: Any) -> frozenset[tuple[str, str]]:
    aliases = {
        "workstream": "project",
        "workflow": "project",
        "company": "organization",
        "org": "organization",
    }
    result: set[tuple[str, str]] = set()
    for item in value or ():
        if not isinstance(item, dict):
            continue
        raw = item.get("canonical_ref") or item.get("id") or item.get("referent_id")
        if raw:
            kind = str(item.get("type") or "other").casefold()
            result.add((aliases.get(kind, kind), str(raw).casefold()))
    return frozenset(result)


def maybe_inject_lifecycle_obligations(
    raw_diff: RawDiff,
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> RawDiff:
    """Append typed lifecycle obligations for explicit batch evidence."""

    if trigger.kind != "T1":
        return raw_diff
    if trigger.subkind != "event_batch" and not trigger.member_trigger_ids:
        return raw_diff

    fragments = _ordinary_batch_fragments(trigger)
    if not fragments:
        return raw_diff

    now = _seed_time(trigger)
    event_ids = _fragment_event_ids(fragments)
    cause_event_id = event_ids[0] if event_ids else trigger.observation_id
    models = _active_models(bundle)
    anchor_model = models[0] if models else None

    injected: list[str] = []
    prediction = _first_matching_fragment(fragments, _PREDICTION_RE)
    if prediction and cause_event_id and not _has_prediction(raw_diff):
        prediction_ids = _fragment_event_ids([prediction])
        _inject_prediction(
            raw_diff,
            prediction,
            prediction_ids[0] if prediction_ids else cause_event_id,
            [str(event_id) for event_id in prediction_ids],
            trigger,
            now,
        )
        injected.append("prediction")

    resource = _first_matching_fragment(fragments, _RESOURCE_RE)
    if resource and cause_event_id and not raw_diff.resource_ops:
        resource_ids = _fragment_event_ids([resource])
        _inject_resource(
            raw_diff, resource,
            resource_ids[0] if resource_ids else cause_event_id,
            [str(event_id) for event_id in resource_ids],
        )
        injected.append("resource")

    question_policy = _first_matching_fragment(fragments, _QUESTION_POLICY_RE)
    if question_policy and cause_event_id and not _has_question_policy(raw_diff):
        question_ids = _fragment_event_ids([question_policy])
        _inject_question_policy_marker(
            raw_diff,
            question_policy,
            question_ids[0] if question_ids else cause_event_id,
            [str(event_id) for event_id in question_ids],
            trigger,
            now,
        )
        injected.append("question_policy")

    evidence = _first_matching_fragment(fragments, _EVIDENCE_ATTACHMENT_RE)
    if (
        evidence
        and anchor_model is not None
        and cause_event_id
        and not _has_evidence_attachment_candidate(raw_diff)
    ):
        evidence_ids = _fragment_event_ids([evidence])
        _inject_evidence_attachment(
            raw_diff,
            evidence,
            anchor_model,
            evidence_ids[0] if evidence_ids else cause_event_id,
            evidence_ids,
            now,
        )
        injected.append("evidence_attachment")

    stale = _first_matching_fragment(fragments, _STALE_RE)
    if stale and anchor_model is not None and not _has_open_question(
        raw_diff, "temporal_status"
    ):
        stale_ids = _fragment_event_ids([stale])
        if _inject_open_question(
            raw_diff,
            stale,
            anchor_model,
            "temporal_status",
            "Is this memory still current, or should it be revised or archived?",
            stale_ids,
        ):
            injected.append("staleness_review")

    ambiguity = _first_matching_fragment(fragments, _AMBIGUITY_RE)
    if ambiguity and anchor_model is not None and not _has_open_question(
        raw_diff, "contradiction_check"
    ):
        ambiguity_ids = _fragment_event_ids([ambiguity])
        if _inject_open_question(
            raw_diff,
            ambiguity,
            anchor_model,
            "contradiction_check",
            "Does this evidence refer to the same memory/entity or a distinct case?",
            ambiguity_ids,
        ):
            injected.append("ambiguity_review")

    if injected:
        trace = raw_diff.reasoning_trace or ""
        note = "lifecycle_obligations: injected " + ", ".join(injected)
        raw_diff.reasoning_trace = f"{trace}\n{note}".strip() if trace else note
    return raw_diff


def _ordinary_batch_fragments(trigger: TriggerContext) -> list[dict[str, Any]]:
    signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    raw_fragments = signature.get("batch_signal_fragments")
    fragments: list[dict[str, Any]] = []
    if isinstance(raw_fragments, list):
        for fragment in raw_fragments:
            if not isinstance(fragment, dict):
                continue
            text = _clean_text(str(fragment.get("text") or ""))
            if not text or "capability_probe" in text.lower():
                continue
            fragments.append(
                {
                    "text": text,
                    "observation_id": _uuid_or_none(fragment.get("observation_id")),
                }
            )
    if fragments:
        return fragments

    text = _clean_text(trigger.seed_natural_text or "")
    if not text or "capability_probe" in text.lower():
        return []
    return [{"text": text, "observation_id": trigger.observation_id}]


def _first_matching_fragment(
    fragments: list[dict[str, Any]],
    pattern: re.Pattern[str],
) -> dict[str, Any] | None:
    return next(
        (fragment for fragment in fragments if pattern.search(str(fragment["text"]))),
        None,
    )


def _inject_prediction(
    raw_diff: RawDiff,
    fragment: dict[str, Any],
    cause_event_id: UUID,
    evidence_event_ids: list[str],
    trigger: TriggerContext,
    now: datetime,
) -> None:
    text = _clip(str(fragment["text"]), 180)
    evaluate_at = _prediction_evaluate_at(text, now)
    expected = text
    raw_diff.claim_ops.append(
        ClaimOp(
            op="insert",
            entry={
                "born_from_event_id": str(cause_event_id),
                "supporting_event_ids": evidence_event_ids,
                "proposition": {
                    "kind": "prediction",
                    "expected": expected,
                    "resolution": (
                        "Later evidence confirms, delays, revises, or falsifies "
                        "the stated future outcome."
                    ),
                },
                "natural": f"Prediction to verify: {expected}",
                "scope_actors": [str(actor) for actor in (trigger.scope_actors or [])],
                "scope_entities": _scope_entities(trigger),
                "scope_temporal": {
                    "valid_from": now.isoformat(),
                    "valid_until": evaluate_at.isoformat(),
                },
                "evaluate_at": evaluate_at.isoformat(),
                "resolution_criteria": {
                    "source": "lifecycle_obligation",
                    "kind": "observation_pattern",
                    "falsification_rule": (
                        "Look for later evidence that confirms, delays, revises, "
                        "or contradicts this predicted outcome."
                    ),
                },
                "confidence": 0.62,
                "confidence_at_assertion": 0.62,
                "falsifier": {
                    "kind": "prediction_deadline",
                    "evaluate_at": evaluate_at.isoformat(),
                    "check": (
                        "Resolve this prediction against later observations after "
                        "the evaluation deadline."
                    ),
                },
                "domain_tags": ["prediction", "lifecycle_obligation"],
            },
        )
    )


def _inject_resource(
    raw_diff: RawDiff,
    fragment: dict[str, Any],
    cause_event_id: UUID,
    evidence_event_ids: list[str],
) -> None:
    text = _clip(str(fragment["text"]), 180)
    identity = "lifecycle_obligation:" + hashlib.sha256(
        text.lower().encode()
    ).hexdigest()[:12]
    raw_diff.resource_ops.append(
        ResourceOp(
            op="create",
            payload={
                "kind": "capacity",
                "identity": identity,
                "description": f"Lifecycle resource constraint: {text}",
                "current_value": {
                    "status": "constrained",
                    "evidence": text,
                    "evidence_event_ids": evidence_event_ids,
                },
                "utilization_state": "committed",
                "controllability": "limited",
                "temporal_character": "time_limited",
                "valuation_confidence": 0.62,
                "created_by_event_id": str(cause_event_id),
                "metadata": {
                    "source": "lifecycle_obligation",
                    "evidence_event_ids": evidence_event_ids,
                },
            },
        )
    )


def _inject_question_policy_marker(
    raw_diff: RawDiff,
    fragment: dict[str, Any],
    cause_event_id: UUID,
    evidence_event_ids: list[str],
    trigger: TriggerContext,
    now: datetime,
) -> None:
    text = _clip(str(fragment["text"]), 220)
    natural = (
        "Question-policy learning: asking a clarification question before "
        f"writing strong memory would improve precision for: {text}"
    )
    raw_diff.claim_ops.append(
        ClaimOp(
            op="insert",
            entry={
                "born_from_event_id": str(cause_event_id),
                "supporting_event_ids": evidence_event_ids,
                "proposition": {
                    "kind": "belief",
                    "claim_role": "capability",
                    "abstraction_level": "atomic",
                    "capability_id": "question_policy_missing_context_precision",
                    "subject": "question policy",
                    "assessment": natural,
                },
                "natural": natural,
                "scope_actors": [str(actor) for actor in (trigger.scope_actors or [])],
                "scope_entities": _scope_entities(trigger),
                "scope_temporal": {"valid_from": now.isoformat(), "valid_until": None},
                "confidence": 0.6,
                "confidence_at_assertion": 0.6,
                "falsifier": {
                    "kind": "observation_pattern",
                    "pattern": (
                        "Future similar cases show clarification questions do "
                        "not improve write precision."
                    ),
                    "within_window": "P30D",
                },
                "domain_tags": [
                    "question_policy",
                    "learning",
                    "lifecycle_obligation",
                ],
            },
        )
    )


def _inject_evidence_attachment(
    raw_diff: RawDiff,
    fragment: dict[str, Any],
    model: Any,
    cause_event_id: UUID,
    event_ids: list[UUID],
    now: datetime,
) -> None:
    observation_id = _uuid_or_none(fragment.get("observation_id"))
    if observation_id is None:
        return
    natural = _clip(str(fragment.get("text") or ""), 500)
    if not natural:
        return
    raw_diff.claim_ops.append(
        ClaimOp(
            op="insert",
            entry={
                "born_from_event_id": str(cause_event_id),
                "supporting_event_ids": [str(observation_id)],
                "proposition": {
                    "kind": "belief",
                    "claim_role": "fact",
                    "subject": "lifecycle review evidence",
                    "assertion": natural,
                },
                "natural": natural,
                "scope_actors": _model_scope_actors(model),
                "scope_entities": _model_scope_entities(model),
                "scope_temporal": {"valid_from": now.isoformat(), "valid_until": None},
                "confidence": 0.5,
                "confidence_at_assertion": 0.5,
                "falsifier": None,
                "domain_tags": ["memory_quality", "lifecycle_obligation"],
            },
        )
    )


def _inject_open_question(
    raw_diff: RawDiff,
    fragment: dict[str, Any],
    model: Any,
    question_type: str,
    question: str,
    event_ids: list[UUID],
) -> bool:
    model_id = _uuid_or_none(getattr(model, "id", None))
    if model_id is None:
        return False
    text = _clip(str(fragment["text"]), 220)
    raw_diff.open_question_ops.append(
        OpenQuestionOp(
            op="insert",
            model_id=model_id,
            question=question,
            question_type=question_type,
            rationale=f"Lifecycle-obligation evidence needs review: {text}",
            priority=0.68,
            expected_resolution_signal={
                "source": "lifecycle_obligation",
                "evidence_event_ids": [str(event_id) for event_id in event_ids],
                "evidence_text": text,
            },
            search_signature={
                "kind": question_type,
                "source": "lifecycle_obligation",
                "terms": _search_terms(text),
            },
            source_model_ids=[model_id],
        )
    )
    return True


def _has_prediction(raw_diff: RawDiff) -> bool:
    for op in [*raw_diff.claim_ops, *raw_diff.new_predictions]:
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        prop = op.entry.get("proposition") or {}
        if isinstance(prop, dict) and prop.get("kind") == "prediction":
            return True
    return False


def _has_question_policy(raw_diff: RawDiff) -> bool:
    for op in raw_diff.claim_ops:
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        tags = {str(tag) for tag in (op.entry.get("domain_tags") or [])}
        text = str(op.entry.get("natural") or "").lower()
        if "question_policy" in tags or "question-policy" in text:
            return True
    return False


def _has_evidence_attachment_candidate(raw_diff: RawDiff) -> bool:
    for op in raw_diff.claim_ops:
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        tags = {str(tag) for tag in (op.entry.get("domain_tags") or [])}
        if "memory_quality" in tags or "evidence_attachment" in tags:
            return True
    return False


def _has_open_question(raw_diff: RawDiff, question_type: str) -> bool:
    return any(
        op.op == "insert" and op.question_type == question_type
        for op in raw_diff.open_question_ops
    )


def _active_models(bundle: ContextBundle) -> list[Any]:
    models = [
        model
        for model in (getattr(bundle, "models", None) or [])
        if str(getattr(model, "status", "active")) == "active"
        and _uuid_or_none(getattr(model, "id", None)) is not None
    ]
    return sorted(
        models,
        key=lambda model: float(getattr(model, "confidence", 0.0) or 0.0),
        reverse=True,
    )


def _fragment_event_ids(fragments: list[dict[str, Any]]) -> list[UUID]:
    event_ids: list[UUID] = []
    seen: set[UUID] = set()
    for fragment in fragments:
        event_id = _uuid_or_none(fragment.get("observation_id"))
        if event_id is None or event_id in seen:
            continue
        seen.add(event_id)
        event_ids.append(event_id)
    return event_ids


def _prediction_evaluate_at(text: str, seed_time: datetime) -> datetime:
    lower = text.lower()
    if "tomorrow" in lower:
        return seed_time + timedelta(days=1)
    if "next week" in lower:
        return seed_time + timedelta(days=7)
    if any(term in lower for term in ("today", "tonight")):
        return seed_time + timedelta(days=1)

    relative = _RELATIVE_DEADLINE_RE.search(lower)
    if relative:
        qty_raw = relative.group(1).lower()
        qty = int(qty_raw) if qty_raw.isdigit() else _NUMBER_WORDS.get(qty_raw, 1)
        unit = relative.group(2).lower()
        if unit.startswith("day"):
            return seed_time + timedelta(days=qty)
        if unit.startswith("week"):
            return seed_time + timedelta(weeks=qty)
        if unit.startswith("month"):
            return seed_time + timedelta(days=30 * qty)

    weekday = re.search(
        r"\bby\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
        lower,
    )
    if weekday:
        target = _WEEKDAYS[weekday.group(1)]
        days = (target - seed_time.weekday()) % 7
        return seed_time + timedelta(days=days or 7)

    return seed_time + timedelta(days=3)


def _seed_time(trigger: TriggerContext) -> datetime:
    seed_time = trigger.seed_occurred_at or datetime.now(timezone.utc)
    if seed_time.tzinfo is None:
        return seed_time.replace(tzinfo=timezone.utc)
    return seed_time


def _scope_entities(trigger: TriggerContext) -> list[dict[str, Any]]:
    return [
        dict(entity)
        for entity in (trigger.seed_entity_ids or [])
        if isinstance(entity, dict)
    ]


def _model_scope_actors(model: Any) -> list[str]:
    return [str(actor_id) for actor_id in (getattr(model, "scope_actors", None) or [])]


def _model_scope_entities(model: Any) -> list[dict[str, Any]]:
    return [
        dict(entity)
        for entity in (getattr(model, "scope_entities", None) or [])
        if isinstance(entity, dict)
    ]


def _search_terms(text: str) -> list[str]:
    terms: list[str] = []
    for token in re.findall(r"[a-z0-9_]{4,}", text.lower()):
        if token in terms:
            continue
        terms.append(token)
        if len(terms) >= 8:
            break
    return terms


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _clip(text: str, limit: int) -> str:
    text = _clean_text(text)
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _uuid_or_none(value: Any) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


__all__ = [
    "maybe_inject_lifecycle_obligations",
    "maybe_inject_synthesis_evolution_obligations",
]
