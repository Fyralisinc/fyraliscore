"""Context-use evaluation for Think outputs.

Retrieval quality is only useful if the LLM-facing context is actually
used by the resulting diff. This module computes a lightweight,
JSON-safe summary from a ContextBundle plus a RawDiff/ValidatedDiff.
It is intentionally conservative: referencing a Model means the diff
uses its id as an updated/archive target, an edge endpoint, an edge
evidence model, or an act confidence basis.
"""
from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from services.reasoning.retrieval.assembler import ContextBundle

from .diff_schema import RawDiff, ValidatedDiff


_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)

_MODEL_REFERENCE_KEYS = {
    "about_model_id",
    "confidence_basis_model_id",
    "model_id",
    "resolution_model_id",
    "source_model_id",
    "superseded_by_model_id",
    "target_model_id",
}

_MODEL_REFERENCE_LIST_KEYS = {
    "evidence_model_ids",
    "member_model_ids",
    "model_ids",
    "related_model_ids",
    "source_model_ids",
    "subject_model_ids",
    "supporting_model_ids",
    "target_model_ids",
}


def _coerce_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _uuid_set_from_strings(values: Any) -> set[UUID]:
    if not isinstance(values, list):
        return set()
    out: set[UUID] = set()
    for value in values:
        uid = _coerce_uuid(value)
        if uid is not None:
            out.add(uid)
    return out


def _add_model_reference_value(value: Any, referenced: set[UUID]) -> None:
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _add_model_reference_value(item, referenced)
        return
    model_id = _coerce_uuid(value)
    if model_id is not None:
        referenced.add(model_id)


def _model_references_from_mapping(mapping: Any) -> set[UUID]:
    if not isinstance(mapping, dict):
        return set()
    referenced: set[UUID] = set()
    for key, value in mapping.items():
        if key in _MODEL_REFERENCE_KEYS or key in _MODEL_REFERENCE_LIST_KEYS:
            _add_model_reference_value(value, referenced)
            continue
        if isinstance(value, dict):
            referenced |= _model_references_from_mapping(value)
        elif isinstance(value, list):
            for item in value:
                referenced |= _model_references_from_mapping(item)
    return referenced


def _uuid_strings(values: set[UUID]) -> list[str]:
    return sorted(str(value) for value in values)


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 1.0


def _selection_from_bundle(bundle: ContextBundle) -> tuple[set[UUID], set[UUID]]:
    notes = bundle.notes.get("model_selection") if bundle.notes else None
    if not isinstance(notes, dict):
        selected = {m.id for m in bundle.models}
        return selected, set()

    selected = _uuid_set_from_strings(notes.get("selected_model_ids"))
    pathway_survival = notes.get("pathway_survival")
    graph_selected: set[UUID] = set()
    if isinstance(pathway_survival, dict):
        g = pathway_survival.get("G")
        if isinstance(g, dict):
            graph_selected = _uuid_set_from_strings(g.get("selected_model_ids"))
    return selected, graph_selected


def _observations_from_bundle(bundle: ContextBundle) -> set[UUID]:
    return {o.id for o in bundle.observations}


def _observation_selection_notes(bundle: ContextBundle) -> dict[str, Any]:
    notes = bundle.notes if isinstance(bundle.notes, dict) else {}
    selection = notes.get("observation_selection")
    return selection if isinstance(selection, dict) else {}


def _raw_observation_reopening(selection: dict[str, Any]) -> dict[str, Any]:
    reopening = selection.get("raw_evidence_reopening")
    return reopening if isinstance(reopening, dict) else {}


def _referenced_model_ids(diff: RawDiff | ValidatedDiff) -> set[UUID]:
    referenced: set[UUID] = set()

    for op in diff.claim_ops:
        model_id = _coerce_uuid(getattr(op, "model_id", None))
        if model_id is not None:
            referenced.add(model_id)
        entry = getattr(op, "entry", None)
        if isinstance(entry, dict):
            referenced |= _model_references_from_mapping(entry)

    for op in getattr(diff, "memory_lifecycle_ops", []) or []:
        for value in (
            getattr(op, "model_id", None),
            getattr(op, "superseded_by_model_id", None),
        ):
            model_id = _coerce_uuid(value)
            if model_id is not None:
                referenced.add(model_id)
        for value in getattr(op, "evidence_model_ids", None) or []:
            model_id = _coerce_uuid(value)
            if model_id is not None:
                referenced.add(model_id)

    for op in diff.edge_ops:
        for value in (
            getattr(op, "source_model_id", None),
            getattr(op, "target_model_id", None),
        ):
            model_id = _coerce_uuid(value)
            if model_id is not None:
                referenced.add(model_id)
        for value in getattr(op, "evidence_model_ids", None) or []:
            model_id = _coerce_uuid(value)
            if model_id is not None:
                referenced.add(model_id)

    for op in getattr(diff, "relation_claim_ops", []) or []:
        for value in (
            getattr(op, "source_model_id", None),
            getattr(op, "target_model_id", None),
        ):
            model_id = _coerce_uuid(value)
            if model_id is not None:
                referenced.add(model_id)
        for value in getattr(op, "evidence_model_ids", None) or []:
            model_id = _coerce_uuid(value)
            if model_id is not None:
                referenced.add(model_id)

    for op in getattr(diff, "relation_frame_ops", []) or []:
        for participant in getattr(op, "participants", None) or []:
            model_id = _coerce_uuid(getattr(participant, "model_id", None))
            if model_id is not None:
                referenced.add(model_id)
        for value in getattr(op, "evidence_model_ids", None) or []:
            model_id = _coerce_uuid(value)
            if model_id is not None:
                referenced.add(model_id)

    for op in getattr(diff, "ontology_gap_ops", []) or []:
        for value in (
            getattr(op, "source_model_id", None),
            getattr(op, "target_model_id", None),
        ):
            model_id = _coerce_uuid(value)
            if model_id is not None:
                referenced.add(model_id)
        for value in getattr(op, "evidence_model_ids", None) or []:
            model_id = _coerce_uuid(value)
            if model_id is not None:
                referenced.add(model_id)

    for op in getattr(diff, "open_question_ops", []) or []:
        for value in (
            getattr(op, "model_id", None),
            getattr(op, "resolution_model_id", None),
            *(getattr(op, "source_model_ids", None) or []),
        ):
            model_id = _coerce_uuid(value)
            if model_id is not None:
                referenced.add(model_id)

    for op in getattr(diff, "formation_resolutions", []) or []:
        for value in getattr(op, "output_model_ids", None) or []:
            model_id = _coerce_uuid(value)
            if model_id is not None:
                referenced.add(model_id)

    for op in diff.act_ops:
        model_id = _coerce_uuid(getattr(op, "confidence_basis", None))
        if model_id is not None:
            referenced.add(model_id)

    return referenced


def _referenced_observation_ids(diff: RawDiff | ValidatedDiff) -> set[UUID]:
    referenced: set[UUID] = set()
    for op in diff.claim_ops:
        entry = getattr(op, "entry", None)
        if isinstance(entry, dict):
            obs_id = _coerce_uuid(entry.get("born_from_event_id"))
            if obs_id is not None:
                referenced.add(obs_id)
    for op in getattr(diff, "memory_lifecycle_ops", []) or []:
        for value in getattr(op, "evidence_event_ids", None) or []:
            obs_id = _coerce_uuid(value)
            if obs_id is not None:
                referenced.add(obs_id)
    for op in diff.edge_ops:
        for value in getattr(op, "evidence_event_ids", None) or []:
            obs_id = _coerce_uuid(value)
            if obs_id is not None:
                referenced.add(obs_id)
    for op in getattr(diff, "relation_claim_ops", []) or []:
        for value in getattr(op, "evidence_event_ids", None) or []:
            obs_id = _coerce_uuid(value)
            if obs_id is not None:
                referenced.add(obs_id)
    for op in getattr(diff, "relation_frame_ops", []) or []:
        for value in getattr(op, "evidence_event_ids", None) or []:
            obs_id = _coerce_uuid(value)
            if obs_id is not None:
                referenced.add(obs_id)
    for op in getattr(diff, "ontology_gap_ops", []) or []:
        for value in getattr(op, "evidence_event_ids", None) or []:
            obs_id = _coerce_uuid(value)
            if obs_id is not None:
                referenced.add(obs_id)
    return referenced


def _ids_from_reasoning_trace(diff: RawDiff | ValidatedDiff) -> set[UUID]:
    trace = getattr(diff, "reasoning_trace", None)
    if not trace:
        return set()
    return {UUID(match.group(0)) for match in _UUID_RE.finditer(str(trace))}


_TRACE_DECISION_RATIONALE_MARKERS = (
    "no edge",
    "no-edge",
    "does not warrant an edge",
    "did not warrant an edge",
    "edge is not warranted",
    "edge not warranted",
    "different customer",
    "different mechanism",
    "not materially changed",
    "did not receive a sharper state change",
    "already captures",
    "adds no new state transition",
)


def _trace_has_no_edge_rationale(
    diff: RawDiff | ValidatedDiff,
    graph_model_ids: set[UUID],
) -> bool:
    trace = getattr(diff, "reasoning_trace", None)
    if not trace or not graph_model_ids:
        return False
    text = str(trace).lower()
    if not any(
        marker in text
        for marker in (
            "no edge",
            "no-edge",
            "does not warrant an edge",
            "did not warrant an edge",
            "edge is not warranted",
            "edge not warranted",
        )
    ):
        return False
    trace_ids = _ids_from_reasoning_trace(diff)
    return bool(trace_ids & graph_model_ids)


def _trace_uses_selected_context_for_decision(
    diff: RawDiff | ValidatedDiff,
    selected_context_ids: set[UUID],
) -> bool:
    trace = getattr(diff, "reasoning_trace", None)
    if not trace or not selected_context_ids:
        return False
    text = str(trace).lower()
    if not any(marker in text for marker in _TRACE_DECISION_RATIONALE_MARKERS):
        return False
    trace_ids = _ids_from_reasoning_trace(diff)
    return bool(trace_ids & selected_context_ids)


def _relation_op_counts(
    ops: Any,
    *,
    selected_model_ids: set[UUID],
    graph_model_ids: set[UUID],
) -> tuple[int, int]:
    between_selected = 0
    touching_graph = 0
    for op in ops or []:
        source = _coerce_uuid(getattr(op, "source_model_id", None))
        target = _coerce_uuid(getattr(op, "target_model_id", None))
        endpoints = {x for x in (source, target) if x is not None}
        if len(endpoints) == 2 and endpoints <= selected_model_ids:
            between_selected += 1
        if endpoints & graph_model_ids:
            touching_graph += 1
    return between_selected, touching_graph


def _relation_frame_op_counts(
    ops: Any,
    *,
    selected_model_ids: set[UUID],
    graph_model_ids: set[UUID],
) -> tuple[int, int]:
    between_selected = 0
    touching_graph = 0
    for op in ops or []:
        participants = getattr(op, "participants", None) or []
        endpoints = {
            model_id
            for participant in participants
            if (model_id := _coerce_uuid(getattr(participant, "model_id", None)))
            is not None
        }
        if len(endpoints) >= 2 and endpoints <= selected_model_ids:
            between_selected += 1
        if endpoints & graph_model_ids:
            touching_graph += 1
    return between_selected, touching_graph


def _context_use_grade(
    *,
    selected_context_count: int,
    reasoning_trace_context_used: bool,
    total_ops: int,
    graph_context_used: bool,
    model_context_used: bool,
    observation_context_used: bool,
    reasoning_trace_context_accounted: bool,
) -> str:
    if selected_context_count == 0:
        return "no_selected_context"
    if reasoning_trace_context_used and total_ops == 0:
        return "justified_noop_context_used"
    if graph_context_used:
        return "graph_context_used"
    if model_context_used:
        return "model_context_used"
    if observation_context_used:
        return "observation_context_used"
    if reasoning_trace_context_accounted:
        return "selected_context_accounted"
    return "unused_selected_context"


def _graph_relation_contract_basis(
    *,
    graph_count: int,
    graph_relation_op_count: int,
    graph_no_edge_rationale_present: bool,
    graph_non_relation_op_count: int,
    graph_trace_accounted: bool,
) -> str:
    if graph_count == 0:
        return "no_graph_selected"
    if graph_relation_op_count > 0:
        return "relation_op"
    if graph_no_edge_rationale_present:
        return "no_edge_rationale"
    if graph_non_relation_op_count > 0:
        return "model_or_act_mutation"
    if graph_trace_accounted:
        return "noop_trace_accounted"
    return "missing"


def _selected_context_accounted_count(
    *,
    selected_context_reference_count: int,
    reasoning_trace_context_accounted: bool,
    trace_referenced_models: set[UUID],
    trace_referenced_observations: set[UUID],
) -> int:
    if selected_context_reference_count > 0 or not reasoning_trace_context_accounted:
        return selected_context_reference_count
    return len(trace_referenced_models) + len(trace_referenced_observations)


def _graph_relation_contract_satisfied(
    *,
    graph_count: int,
    graph_relation_op_count: int,
    graph_no_edge_rationale_present: bool,
    graph_non_relation_op_count: int,
    graph_trace_accounted: bool,
) -> bool:
    return (
        graph_count == 0
        or graph_relation_op_count > 0
        or graph_no_edge_rationale_present
        or graph_non_relation_op_count > 0
        or graph_trace_accounted
    )


def _context_use_report(values: dict[str, Any]) -> dict[str, Any]:
    diff = values["diff"]
    observation_selection = values["observation_selection"]
    raw_reopening = _raw_observation_reopening(observation_selection)
    selected_context_reference_count = values["selected_context_reference_count"]
    selected_context_count = values["selected_context_count"]
    selected_count = values["selected_count"]
    selected_referenced = values["selected_referenced"]
    graph_referenced = values["graph_referenced"]
    graph_count = values["graph_count"]
    observation_referenced = values["observation_referenced"]
    selected_observation_count = values["selected_observation_count"]
    relation_claim_ops = values["relation_claim_ops"]
    relation_frame_ops = values["relation_frame_ops"]
    ontology_gap_ops = values["ontology_gap_ops"]
    memory_lifecycle_ops = values["memory_lifecycle_ops"]
    open_question_ops = values["open_question_ops"]
    formation_resolutions = values["formation_resolutions"]

    return {
        "context_use_grade": values["context_use_grade"],
        "selected_context_used": selected_context_reference_count > 0,
        "selected_context_accounted_for": values["selected_context_accounted_count"] > 0,
        "graph_context_used": values["graph_context_used"],
        "model_context_used": values["model_context_used"],
        "observation_context_used": values["observation_context_used"],
        "selected_context_count": selected_context_count,
        "selected_context_reference_count": selected_context_reference_count,
        "selected_context_accounted_count": values["selected_context_accounted_count"],
        "reasoning_trace_context_used": values["reasoning_trace_context_used"],
        "reasoning_trace_context_accounted": values["reasoning_trace_context_accounted"],
        "reasoning_trace_context_decision_used": (
            values["trace_selected_context_decision_used"]
        ),
        "trace_referenced_model_ids": _uuid_strings(values["trace_referenced_models"]),
        "trace_referenced_observation_ids": _uuid_strings(
            values["trace_referenced_observations"]
        ),
        "selected_context_reference_ratio": _ratio(
            selected_context_reference_count, selected_context_count
        ),
        "selected_model_count": selected_count,
        "selected_model_reference_count": len(selected_referenced),
        "selected_model_reference_ratio": _ratio(len(selected_referenced), selected_count),
        "selected_model_ids": _uuid_strings(values["selected_model_ids"]),
        "referenced_model_ids": _uuid_strings(values["referenced_models"]),
        "op_referenced_model_ids": _uuid_strings(values["op_referenced_models"]),
        "unused_selected_model_ids": _uuid_strings(
            values["selected_model_ids"] - values["referenced_models"]
        ),
        "graph_selected_model_count": graph_count,
        "graph_selected_reference_count": len(graph_referenced),
        "graph_selected_reference_ratio": _ratio(len(graph_referenced), graph_count),
        "graph_selected_model_ids": _uuid_strings(values["graph_model_ids"]),
        "unused_graph_model_ids": _uuid_strings(
            values["graph_model_ids"] - values["referenced_models"]
        ),
        "selected_observation_count": selected_observation_count,
        "selected_trigger_observation_count": int(
            observation_selection.get("selected_trigger_count") or 0
        ),
        "selected_historical_observation_count": int(
            observation_selection.get("selected_historical_count") or 0
        ),
        "raw_observation_reopening_reasons": [
            str(reason)
            for reason in (raw_reopening.get("reason_codes") or [])
            if str(reason).strip()
        ],
        "raw_observation_reopening": raw_reopening,
        "historical_observation_cap": int(
            observation_selection.get("historical_cap") or 0
        ),
        "selected_observation_reference_count": len(observation_referenced),
        "selected_observation_reference_ratio": _ratio(
            len(observation_referenced), selected_observation_count
        ),
        "selected_observation_ids": _uuid_strings(values["selected_observation_ids"]),
        "referenced_observation_ids": _uuid_strings(values["referenced_observations"]),
        "op_referenced_observation_ids": _uuid_strings(
            values["op_referenced_observations"]
        ),
        "unused_selected_observation_ids": _uuid_strings(
            values["selected_observation_ids"] - values["referenced_observations"]
        ),
        "edge_ops_count": len(diff.edge_ops),
        "edge_ops_between_selected_models": values["edge_ops_between_selected"],
        "edge_ops_touching_graph_models": values["edge_ops_touching_graph"],
        "relation_claim_ops_count": len(relation_claim_ops),
        "relation_claim_ops_between_selected_models": (
            values["relation_claim_ops_between_selected"]
        ),
        "relation_claim_ops_touching_graph_models": (
            values["relation_claim_ops_touching_graph"]
        ),
        "relation_frame_ops_count": len(relation_frame_ops),
        "relation_frame_ops_between_selected_models": (
            values["relation_frame_ops_between_selected"]
        ),
        "relation_frame_ops_touching_graph_models": (
            values["relation_frame_ops_touching_graph"]
        ),
        "ontology_gap_ops_count": len(ontology_gap_ops),
        "ontology_gap_ops_between_selected_models": (
            values["ontology_gap_ops_between_selected"]
        ),
        "ontology_gap_ops_touching_graph_models": (
            values["ontology_gap_ops_touching_graph"]
        ),
        "graph_relation_op_count": values["graph_relation_op_count"],
        "graph_non_relation_op_count": values["graph_non_relation_op_count"],
        "graph_claim_op_reference_count": values["graph_claim_op_reference_count"],
        "graph_memory_lifecycle_op_reference_count": (
            values["graph_memory_lifecycle_op_reference_count"]
        ),
        "graph_act_op_reference_count": values["graph_act_op_reference_count"],
        "graph_open_question_reference_count": (
            values["graph_open_question_reference_count"]
        ),
        "graph_formation_resolution_reference_count": (
            values["graph_formation_resolution_reference_count"]
        ),
        "graph_trace_reference_count": values["graph_trace_reference_count"],
        "graph_selected_without_relation_ops": (
            values["graph_selected_without_relation_ops"]
        ),
        "graph_no_edge_rationale_present": values["graph_no_edge_rationale_present"],
        "graph_relation_contract_satisfied": (
            values["graph_relation_contract_satisfied"]
        ),
        "graph_relation_contract_basis": values["graph_relation_contract_basis"],
        "claim_ops_count": len(diff.claim_ops),
        "memory_lifecycle_ops_count": len(memory_lifecycle_ops),
        "open_question_ops_count": len(open_question_ops),
        "formation_resolutions_count": len(formation_resolutions),
        "act_ops_count": len(diff.act_ops),
        "resource_ops_count": len(diff.resource_ops),
    }


def summarize_context_use(
    bundle: ContextBundle,
    diff: RawDiff | ValidatedDiff,
) -> dict[str, Any]:
    """Return JSON-safe context-use telemetry for a Think diff."""
    selected_model_ids, graph_model_ids = _selection_from_bundle(bundle)
    selected_observation_ids = _observations_from_bundle(bundle)
    op_referenced_models = _referenced_model_ids(diff)
    op_referenced_observations = _referenced_observation_ids(diff)
    trace_referenced_ids = _ids_from_reasoning_trace(diff)
    trace_referenced_models = trace_referenced_ids & selected_model_ids
    trace_referenced_observations = trace_referenced_ids & selected_observation_ids
    graph_no_edge_rationale_present = _trace_has_no_edge_rationale(
        diff,
        graph_model_ids,
    )
    total_ops = (
        len(diff.claim_ops)
        + len(getattr(diff, "memory_lifecycle_ops", []) or [])
        + len(getattr(diff, "relation_claim_ops", []) or [])
        + len(getattr(diff, "relation_frame_ops", []) or [])
        + len(diff.edge_ops)
        + len(getattr(diff, "ontology_gap_ops", []) or [])
        + len(getattr(diff, "open_question_ops", []) or [])
        + len(getattr(diff, "formation_resolutions", []) or [])
        + len(diff.act_ops)
        + len(diff.resource_ops)
    )
    trace_selected_context_decision_used = _trace_uses_selected_context_for_decision(
        diff,
        selected_model_ids | selected_observation_ids,
    )
    reasoning_trace_context_used = (
        (total_ops == 0 or trace_selected_context_decision_used)
        and bool(trace_referenced_models or trace_referenced_observations)
    )
    reasoning_trace_context_accounted = bool(
        trace_referenced_models or trace_referenced_observations
    )
    referenced_models = set(op_referenced_models)
    referenced_observations = set(op_referenced_observations)
    if reasoning_trace_context_used:
        referenced_models |= trace_referenced_models
        referenced_observations |= trace_referenced_observations

    selected_referenced = selected_model_ids & referenced_models
    graph_referenced = graph_model_ids & referenced_models
    observation_referenced = selected_observation_ids & referenced_observations
    edge_ops_between_selected, edge_ops_touching_graph = _relation_op_counts(
        diff.edge_ops,
        selected_model_ids=selected_model_ids,
        graph_model_ids=graph_model_ids,
    )
    relation_claim_ops = getattr(diff, "relation_claim_ops", []) or []
    memory_lifecycle_ops = getattr(diff, "memory_lifecycle_ops", []) or []
    (
        relation_claim_ops_between_selected,
        relation_claim_ops_touching_graph,
    ) = _relation_op_counts(
        relation_claim_ops,
        selected_model_ids=selected_model_ids,
        graph_model_ids=graph_model_ids,
    )
    relation_frame_ops = getattr(diff, "relation_frame_ops", []) or []
    (
        relation_frame_ops_between_selected,
        relation_frame_ops_touching_graph,
    ) = _relation_frame_op_counts(
        relation_frame_ops,
        selected_model_ids=selected_model_ids,
        graph_model_ids=graph_model_ids,
    )
    ontology_gap_ops = getattr(diff, "ontology_gap_ops", []) or []
    (
        ontology_gap_ops_between_selected,
        ontology_gap_ops_touching_graph,
    ) = _relation_op_counts(
        ontology_gap_ops,
        selected_model_ids=selected_model_ids,
        graph_model_ids=graph_model_ids,
    )

    selected_count = len(selected_model_ids)
    graph_count = len(graph_model_ids)
    selected_observation_count = len(selected_observation_ids)
    selected_context_count = selected_count + selected_observation_count
    selected_context_reference_count = len(selected_referenced) + len(
        observation_referenced
    )
    selected_context_accounted_count = _selected_context_accounted_count(
        selected_context_reference_count=selected_context_reference_count,
        reasoning_trace_context_accounted=reasoning_trace_context_accounted,
        trace_referenced_models=trace_referenced_models,
        trace_referenced_observations=trace_referenced_observations,
    )
    graph_context_used = (
        bool(graph_referenced)
        or relation_claim_ops_touching_graph > 0
        or relation_frame_ops_touching_graph > 0
        or edge_ops_touching_graph > 0
        or ontology_gap_ops_touching_graph > 0
    )
    model_context_used = bool(selected_referenced)
    observation_context_used = bool(observation_referenced)
    context_use_grade = _context_use_grade(
        selected_context_count=selected_context_count,
        reasoning_trace_context_used=reasoning_trace_context_used,
        total_ops=total_ops,
        graph_context_used=graph_context_used,
        model_context_used=model_context_used,
        observation_context_used=observation_context_used,
        reasoning_trace_context_accounted=reasoning_trace_context_accounted,
    )

    observation_selection = _observation_selection_notes(bundle)
    graph_claim_op_reference_count = 0
    for op in diff.claim_ops:
        claim_refs: set[UUID] = set()
        model_id = _coerce_uuid(getattr(op, "model_id", None))
        if model_id is not None:
            claim_refs.add(model_id)
        entry = getattr(op, "entry", None)
        if isinstance(entry, dict):
            claim_refs |= _model_references_from_mapping(entry)
        if claim_refs & graph_model_ids:
            graph_claim_op_reference_count += 1
    graph_memory_lifecycle_op_reference_count = sum(
        1
        for op in memory_lifecycle_ops
        if _coerce_uuid(getattr(op, "model_id", None)) in graph_model_ids
    )
    graph_act_op_reference_count = sum(
        1
        for op in diff.act_ops
        if _coerce_uuid(getattr(op, "confidence_basis", None)) in graph_model_ids
    )
    open_question_ops = getattr(diff, "open_question_ops", []) or []
    formation_resolutions = getattr(diff, "formation_resolutions", []) or []
    graph_open_question_reference_count = sum(
        1
        for op in open_question_ops
        if _coerce_uuid(getattr(op, "model_id", None)) in graph_model_ids
    )
    graph_formation_resolution_reference_count = sum(
        1
        for op in formation_resolutions
        if any(
            _coerce_uuid(model_id) in graph_model_ids
            for model_id in getattr(op, "output_model_ids", None) or []
        )
    )
    graph_non_relation_op_count = (
        graph_claim_op_reference_count
        + graph_memory_lifecycle_op_reference_count
        + graph_act_op_reference_count
        + graph_open_question_reference_count
        + graph_formation_resolution_reference_count
    )
    graph_trace_reference_count = len(trace_referenced_models & graph_model_ids)
    graph_trace_accounted = graph_trace_reference_count > 0 and total_ops == 0
    graph_relation_op_count = (
        relation_claim_ops_touching_graph
        + relation_frame_ops_touching_graph
        + edge_ops_touching_graph
        + ontology_gap_ops_touching_graph
    )
    graph_selected_without_relation_ops = (
        graph_count > 0 and total_ops > 0 and graph_relation_op_count == 0
    )
    graph_relation_contract_satisfied = _graph_relation_contract_satisfied(
        graph_count=graph_count,
        graph_relation_op_count=graph_relation_op_count,
        graph_no_edge_rationale_present=graph_no_edge_rationale_present,
        graph_non_relation_op_count=graph_non_relation_op_count,
        graph_trace_accounted=graph_trace_accounted,
    )
    graph_relation_contract_basis = _graph_relation_contract_basis(
        graph_count=graph_count,
        graph_relation_op_count=graph_relation_op_count,
        graph_no_edge_rationale_present=graph_no_edge_rationale_present,
        graph_non_relation_op_count=graph_non_relation_op_count,
        graph_trace_accounted=graph_trace_accounted,
    )
    return _context_use_report(locals())


__all__ = ["summarize_context_use"]
