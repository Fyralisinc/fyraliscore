"""Context-use evaluation for Think outputs.

Retrieval quality is only useful if the LLM-facing context is actually
used by the resulting diff. This module computes a lightweight,
JSON-safe summary from a ContextBundle plus a RawDiff/ValidatedDiff.
It is intentionally conservative: referencing a Model means the diff
uses its id as an updated/archive/relocate target, an edge endpoint, an
edge evidence model, or an act confidence basis.
"""
from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from services.retrieval.assembler import ContextBundle

from .diff_schema import RawDiff, ValidatedDiff


_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)


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


def _referenced_model_ids(diff: RawDiff | ValidatedDiff) -> set[UUID]:
    referenced: set[UUID] = set()

    for op in diff.claim_ops:
        model_id = _coerce_uuid(getattr(op, "model_id", None))
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
    for op in diff.edge_ops:
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
    total_ops = (
        len(diff.claim_ops)
        + len(diff.edge_ops)
        + len(diff.act_ops)
        + len(diff.resource_ops)
    )
    trace_referenced_models = trace_referenced_ids & selected_model_ids
    trace_referenced_observations = trace_referenced_ids & selected_observation_ids
    reasoning_trace_context_used = (
        total_ops == 0
        and bool(trace_referenced_models or trace_referenced_observations)
    )
    referenced_models = set(op_referenced_models)
    referenced_observations = set(op_referenced_observations)
    if reasoning_trace_context_used:
        referenced_models |= trace_referenced_models
        referenced_observations |= trace_referenced_observations

    selected_referenced = selected_model_ids & referenced_models
    graph_referenced = graph_model_ids & referenced_models
    observation_referenced = selected_observation_ids & referenced_observations

    edge_ops_between_selected = 0
    edge_ops_touching_graph = 0
    for op in diff.edge_ops:
        source = _coerce_uuid(getattr(op, "source_model_id", None))
        target = _coerce_uuid(getattr(op, "target_model_id", None))
        endpoints = {x for x in (source, target) if x is not None}
        if len(endpoints) == 2 and endpoints <= selected_model_ids:
            edge_ops_between_selected += 1
        if endpoints & graph_model_ids:
            edge_ops_touching_graph += 1

    selected_count = len(selected_model_ids)
    graph_count = len(graph_model_ids)
    selected_observation_count = len(selected_observation_ids)
    selected_context_count = selected_count + selected_observation_count
    selected_context_reference_count = (
        len(selected_referenced) + len(observation_referenced)
    )
    graph_context_used = (
        bool(graph_referenced) or edge_ops_touching_graph > 0
    )
    model_context_used = bool(selected_referenced)
    observation_context_used = bool(observation_referenced)
    if selected_context_count == 0:
        context_use_grade = "no_selected_context"
    elif reasoning_trace_context_used and total_ops == 0:
        context_use_grade = "justified_noop_context_used"
    elif graph_context_used:
        context_use_grade = "graph_context_used"
    elif model_context_used:
        context_use_grade = "model_context_used"
    elif observation_context_used:
        context_use_grade = "observation_context_used"
    else:
        context_use_grade = "unused_selected_context"

    return {
        "context_use_grade": context_use_grade,
        "selected_context_used": selected_context_reference_count > 0,
        "graph_context_used": graph_context_used,
        "model_context_used": model_context_used,
        "observation_context_used": observation_context_used,
        "selected_context_count": selected_context_count,
        "selected_context_reference_count": selected_context_reference_count,
        "reasoning_trace_context_used": reasoning_trace_context_used,
        "trace_referenced_model_ids": sorted(
            str(mid) for mid in trace_referenced_models
        ),
        "trace_referenced_observation_ids": sorted(
            str(oid) for oid in trace_referenced_observations
        ),
        "selected_context_reference_ratio": (
            selected_context_reference_count / selected_context_count
            if selected_context_count
            else 1.0
        ),
        "selected_model_count": selected_count,
        "selected_model_reference_count": len(selected_referenced),
        "selected_model_reference_ratio": (
            len(selected_referenced) / selected_count if selected_count else 1.0
        ),
        "selected_model_ids": sorted(str(mid) for mid in selected_model_ids),
        "referenced_model_ids": sorted(str(mid) for mid in referenced_models),
        "op_referenced_model_ids": sorted(str(mid) for mid in op_referenced_models),
        "unused_selected_model_ids": sorted(
            str(mid) for mid in selected_model_ids - referenced_models
        ),
        "graph_selected_model_count": graph_count,
        "graph_selected_reference_count": len(graph_referenced),
        "graph_selected_reference_ratio": (
            len(graph_referenced) / graph_count if graph_count else 1.0
        ),
        "graph_selected_model_ids": sorted(str(mid) for mid in graph_model_ids),
        "unused_graph_model_ids": sorted(
            str(mid) for mid in graph_model_ids - referenced_models
        ),
        "selected_observation_count": selected_observation_count,
        "selected_observation_reference_count": len(observation_referenced),
        "selected_observation_reference_ratio": (
            len(observation_referenced) / selected_observation_count
            if selected_observation_count
            else 1.0
        ),
        "selected_observation_ids": sorted(
            str(oid) for oid in selected_observation_ids
        ),
        "referenced_observation_ids": sorted(
            str(oid) for oid in referenced_observations
        ),
        "op_referenced_observation_ids": sorted(
            str(oid) for oid in op_referenced_observations
        ),
        "unused_selected_observation_ids": sorted(
            str(oid)
            for oid in selected_observation_ids - referenced_observations
        ),
        "edge_ops_count": len(diff.edge_ops),
        "edge_ops_between_selected_models": edge_ops_between_selected,
        "edge_ops_touching_graph_models": edge_ops_touching_graph,
        "claim_ops_count": len(diff.claim_ops),
        "act_ops_count": len(diff.act_ops),
        "resource_ops_count": len(diff.resource_ops),
    }


__all__ = ["summarize_context_use"]
