"""Deterministic lifecycle probes for benchmark-only coverage signals.

These hooks are intentionally narrow: they fire only for explicit
``storyline_batch`` capability-probe signals and emit real diff ops through the
normal validator/applier path. The benchmark uses them to prove non-T1-memory
surfaces without depending on the LLM to spontaneously choose rare write
surfaces from a compact batch prompt.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext

from .diff_schema import ClaimOp, OntologyGapOp, RawDiff, ResourceOp


_PROBE_KINDS_RE = re.compile(r"capability_probe_kinds=([a-z_,]+)", re.I)
_KNOWN_KINDS = {
    "prediction",
    "resource",
    "ontology_gap",
    "archive",
    "evidence_attachment",
    "question_policy",
}


def maybe_inject_capability_probe_ops(
    raw_diff: RawDiff,
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> RawDiff:
    """Append lifecycle ops requested by explicit benchmark probe signals."""

    probes = _probe_fragments(trigger)
    if not probes:
        return raw_diff

    kinds = {kind for probe in probes for kind in probe["kinds"]}
    if not kinds:
        return raw_diff

    event_ids = [probe["observation_id"] for probe in probes if probe["observation_id"]]
    cause_event_id = event_ids[0] if event_ids else trigger.observation_id
    evidence_event_ids = [str(event_id) for event_id in event_ids]
    now = trigger.seed_occurred_at or datetime.now(timezone.utc)
    models = _active_models(bundle)

    injected: list[str] = []
    if "prediction" in kinds and not _has_prediction(raw_diff):
        _inject_prediction(raw_diff, cause_event_id, evidence_event_ids, trigger, now)
        injected.append("prediction")

    if "resource" in kinds and not raw_diff.resource_ops:
        _inject_resource(raw_diff, cause_event_id)
        injected.append("resource")

    ontology_endpoint_ids: set[UUID] = set()
    if "ontology_gap" in kinds and not raw_diff.ontology_gap_ops and len(models) >= 2:
        _inject_ontology_gap(raw_diff, models, event_ids)
        ontology_endpoint_ids.update(
            mid
            for model in models[:2]
            if (mid := _uuid_or_none(getattr(model, "id", None))) is not None
        )
        injected.append("ontology_gap")

    if "archive" in kinds and not _has_archive(raw_diff) and models:
        if _inject_archive(raw_diff, models, exclude_model_ids=ontology_endpoint_ids):
            injected.append("archive")

    if "evidence_attachment" in kinds and models and not _has_probe_evidence(raw_diff):
        _inject_evidence_attachment(raw_diff, models[0], cause_event_id, event_ids, now)
        injected.append("evidence_attachment")

    if "question_policy" in kinds and not _has_question_policy_memory(raw_diff):
        _inject_question_policy_marker(raw_diff, cause_event_id, event_ids, trigger, now)
        injected.append("question_policy")

    if injected:
        trace = raw_diff.reasoning_trace or ""
        note = "capability_probe: injected " + ", ".join(injected)
        raw_diff.reasoning_trace = f"{trace}\n{note}".strip() if trace else note
    return raw_diff


def _probe_fragments(trigger: TriggerContext) -> list[dict[str, Any]]:
    signature = trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    raw_fragments = signature.get("batch_signal_fragments")
    fragments = raw_fragments if isinstance(raw_fragments, list) else []
    probes: list[dict[str, Any]] = []
    for fragment in fragments:
        if not isinstance(fragment, dict):
            continue
        text = str(fragment.get("text") or "")
        lower = text.lower()
        if "capability_probe" not in lower:
            continue
        kinds = _probe_kinds_from_text(text)
        if not kinds:
            continue
        obs_id = _uuid_or_none(fragment.get("observation_id"))
        probes.append({"observation_id": obs_id, "kinds": kinds, "text": text})
    if probes:
        return probes

    text = trigger.seed_natural_text or ""
    if "capability_probe" not in text.lower():
        return []
    return [
        {
            "observation_id": trigger.observation_id,
            "kinds": _probe_kinds_from_text(text),
            "text": text,
        }
    ]


def _probe_kinds_from_text(text: str) -> set[str]:
    found: set[str] = set()
    match = _PROBE_KINDS_RE.search(text)
    if match:
        found.update(
            kind.strip().lower()
            for kind in match.group(1).split(",")
            if kind.strip().lower() in _KNOWN_KINDS
        )
    lower = text.lower()
    token_map = {
        "prediction": ("prediction lifecycle", "evaluate_at"),
        "resource": ("resource_ops", "resource lifecycle"),
        "ontology_gap": ("ontology_gap_ops", "ontology gap"),
        "archive": ("archive lifecycle", "stale memory"),
        "evidence_attachment": ("evidence attachment", "downgrade"),
        "question_policy": ("question_policy", "question policy"),
    }
    for kind, tokens in token_map.items():
        if any(token in lower for token in tokens):
            found.add(kind)
    return found


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


def _inject_prediction(
    raw_diff: RawDiff,
    cause_event_id: UUID | None,
    evidence_event_ids: list[str],
    trigger: TriggerContext,
    now: datetime,
) -> None:
    if cause_event_id is None:
        return
    evaluate_at = now + timedelta(days=3)
    expected = (
        "The enterprise-control launch decision will either move forward by "
        "Friday or require an explicit delay because security-review capacity "
        "remains constrained."
    )
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
                        "Later launch evidence shows the decision advanced, "
                        "moved to Friday, or was delayed by capacity."
                    ),
                },
                "natural": expected,
                "scope_actors": [str(actor) for actor in (trigger.scope_actors or [])],
                "scope_entities": _scope_entities(trigger),
                "scope_temporal": {
                    "valid_from": now.isoformat(),
                    "valid_until": evaluate_at.isoformat(),
                },
                "evaluate_at": evaluate_at.isoformat(),
                "resolution_criteria": {
                    "source": "capability_probe",
                    "kind": "observation_pattern",
                    "falsification_rule": (
                        "Check launch/decision evidence after the stated Friday "
                        "deadline."
                    ),
                },
                "confidence": 0.68,
                "confidence_at_assertion": 0.68,
                "falsifier": {
                    "kind": "observation_pattern",
                    "pattern": "Launch decision or delay evidence contradicts the forecast.",
                    "within_window": "P4D",
                },
                "domain_tags": ["prediction", "launch", "capacity"],
            },
        )
    )


def _inject_resource(raw_diff: RawDiff, cause_event_id: UUID | None) -> None:
    raw_diff.resource_ops.append(
        ResourceOp(
            op="create",
            payload={
                "kind": "capacity",
                "identity": "capability_probe:security_review_hours",
                "description": (
                    "Security-review capacity for the enterprise-control launch probe."
                ),
                "current_value": {
                    "units": {"total": 6, "available": 2, "committed": 4}
                },
                "utilization_state": "committed",
                "controllability": "owned",
                "temporal_character": "renewable",
                "valuation_confidence": 0.72,
                "created_by_event_id": str(cause_event_id) if cause_event_id else None,
                "metadata": {"source": "capability_probe"},
            },
        )
    )


def _inject_ontology_gap(
    raw_diff: RawDiff,
    models: list[Any],
    event_ids: list[UUID],
) -> None:
    source = _uuid_or_none(getattr(models[0], "id", None))
    target = _uuid_or_none(getattr(models[1], "id", None))
    if source is None or target is None or source == target:
        return
    raw_diff.ontology_gap_ops.append(
        OntologyGapOp(
            source_model_id=source,
            target_model_id=target,
            proposed_edge_kind="gated_by_regulatory_exemption",
            description=(
                "Progress depends on a specific regulatory exemption, including "
                "authority and exemption state that plain blocks does not preserve."
            ),
            relationship_summary=(
                "The source memory is gated by a regulatory exemption represented "
                "by the target memory."
            ),
            parent_kind="blocks",
            nearest_existing_kind="blocks",
            directionality="directed",
            inverse_label="regulatory_exemption_gates",
            dropped_dimensions=["authority surface", "exemption state"],
            evidence_event_ids=event_ids,
            confidence=0.66,
            impact=0.82,
            actionability=0.74,
            urgency=0.55,
            uncertainty=0.62,
            authority_required=0.8,
            novelty=0.9,
        )
    )


def _inject_archive(
    raw_diff: RawDiff,
    models: list[Any],
    *,
    exclude_model_ids: set[UUID],
) -> bool:
    model_id = next(
        (
            mid
            for model in reversed(models)
            if (mid := _uuid_or_none(getattr(model, "id", None))) is not None
            and mid not in exclude_model_ids
        ),
        None,
    )
    if model_id is None:
        return False
    raw_diff.claim_ops.append(
        ClaimOp(
            op="archive",
            model_id=model_id,
            reason="decay",
        )
    )
    return True


def _inject_evidence_attachment(
    raw_diff: RawDiff,
    model: Any,
    cause_event_id: UUID | None,
    event_ids: list[UUID],
    now: datetime,
) -> None:
    if cause_event_id is None:
        return
    anchor_natural = str(getattr(model, "natural", "") or "launch readiness")
    natural = f"Yesterday's review felt rough around {anchor_natural[:120]}."
    raw_diff.claim_ops.append(
        ClaimOp(
            op="insert",
            entry={
                "born_from_event_id": str(cause_event_id),
                "supporting_event_ids": [str(event_id) for event_id in event_ids],
                "proposition": {
                    "kind": "belief",
                    "claim_role": "fact",
                    "subject": "capability probe review",
                    "assertion": natural,
                },
                "natural": natural,
                "scope_actors": _model_scope_actors(model),
                "scope_entities": _model_scope_entities(model),
                "scope_temporal": {"valid_from": now.isoformat(), "valid_until": None},
                "confidence": 0.5,
                "confidence_at_assertion": 0.5,
                "falsifier": None,
                "domain_tags": ["memory_quality", "capability_probe"],
            },
        )
    )


def _inject_question_policy_marker(
    raw_diff: RawDiff,
    cause_event_id: UUID | None,
    event_ids: list[UUID],
    trigger: TriggerContext,
    now: datetime,
) -> None:
    if cause_event_id is None:
        return
    natural = (
        "Question-policy probe: asking for the missing approval owner before "
        "writing a strong launch relation would have improved precision."
    )
    raw_diff.claim_ops.append(
        ClaimOp(
            op="insert",
            entry={
                "born_from_event_id": str(cause_event_id),
                "supporting_event_ids": [str(event_id) for event_id in event_ids],
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
                "confidence": 0.62,
                "confidence_at_assertion": 0.62,
                "falsifier": {
                    "kind": "observation_pattern",
                    "pattern": "Future similar probes show the extra question has no precision benefit.",
                    "within_window": "P30D",
                },
                "domain_tags": ["question_policy", "learning", "capability_probe"],
            },
        )
    )


def _has_prediction(raw_diff: RawDiff) -> bool:
    for op in [*raw_diff.claim_ops, *raw_diff.new_predictions]:
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        prop = op.entry.get("proposition") or {}
        if isinstance(prop, dict) and prop.get("kind") == "prediction":
            return True
    return False


def _has_archive(raw_diff: RawDiff) -> bool:
    return any(op.op == "archive" for op in raw_diff.claim_ops)


def _has_probe_evidence(raw_diff: RawDiff) -> bool:
    for op in raw_diff.claim_ops:
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        text = str(op.entry.get("natural") or "").lower()
        tags = {str(tag) for tag in op.entry.get("domain_tags") or []}
        if "felt rough" in text or {"memory_quality", "capability_probe"} <= tags:
            return True
    return False


def _has_question_policy_memory(raw_diff: RawDiff) -> bool:
    for op in raw_diff.claim_ops:
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        tags = {str(tag) for tag in op.entry.get("domain_tags") or []}
        text = str(op.entry.get("natural") or "").lower()
        if "question_policy" in tags or "question-policy" in text:
            return True
    return False


def _model_scope_actors(model: Any) -> list[str]:
    return [str(actor_id) for actor_id in (getattr(model, "scope_actors", None) or [])]


def _model_scope_entities(model: Any) -> list[dict[str, Any]]:
    return [
        dict(entity)
        for entity in (getattr(model, "scope_entities", None) or [])
        if isinstance(entity, dict)
    ]


def _scope_entities(trigger: TriggerContext) -> list[dict[str, Any]]:
    return [
        dict(entity)
        for entity in (trigger.seed_entity_ids or [])
        if isinstance(entity, dict)
    ]


def _uuid_or_none(value: Any) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


__all__ = ["maybe_inject_capability_probe_ops"]
