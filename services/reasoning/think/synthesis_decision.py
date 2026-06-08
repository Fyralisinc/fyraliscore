"""Structural synthesis-decision summaries for Think diffs.

The LLM-facing diff schema is still the execution surface. This module
derives the higher-level memory decision contract from that diff so each
run can be audited in terms of substrate intent: belief, edge, composite,
forecast artifact, or operational artifact.
"""
from __future__ import annotations

from typing import Any, Literal

from lib.shared.memory_grammar import derive_memory_grammar

from .diff_schema import ClaimOp, ValidatedDiff


SynthesisDecisionKind = Literal[
    "discard_as_noise",
    "attach_evidence_to_existing_model",
    "update_existing_model",
    "archive_model",
    "create_atomic_model",
    "create_or_update_edge",
    "create_or_update_situation",
    "create_action_proposal",
    "apply_operational_artifact",
    "publish_forecast_artifact",
]

_EVIDENCE_UPDATE_FIELDS = {
    "confidence",
    "activation",
    "signal_readings",
    "supporting_event_ids",
    "supporting_model_ids",
    "evidential_weight",
    "confirmed_count",
    "last_confirmed_at",
}


def summarize_synthesis_decisions(diff: ValidatedDiff) -> list[dict[str, Any]]:
    """Return a compact, JSON-safe decision contract for observability."""
    decisions: list[dict[str, Any]] = []

    for index, op in enumerate(diff.claim_ops):
        decisions.append(_claim_decision(op, index=index, bucket="claim_ops"))

    for index, op in enumerate(diff.edge_ops):
        decisions.append({
            "bucket": "edge_ops",
            "index": index,
            "decision": (
                "create_or_update_edge"
                if op.op == "add"
                else "update_existing_model"
            ),
            "edge_kind": op.edge_kind,
            "source_model_id": str(op.source_model_id),
            "target_model_id": str(op.target_model_id),
        })

    for index, op in enumerate(diff.ontology_gap_ops):
        decisions.append({
            "bucket": "ontology_gap_ops",
            "index": index,
            "decision": "propose_edge_type_candidate",
            "proposed_edge_kind": op.proposed_edge_kind,
            "parent_kind": op.parent_kind,
            "nearest_existing_kind": op.nearest_existing_kind,
            "source_model_id": str(op.source_model_id),
            "target_model_id": str(op.target_model_id),
        })

    for index, op in enumerate(diff.act_ops):
        decisions.append({
            "bucket": "act_ops",
            "index": index,
            "decision": "apply_operational_artifact",
            "op": op.op,
            "confidence_basis": (
                str(op.confidence_basis) if op.confidence_basis else None
            ),
        })

    for index, op in enumerate(diff.resource_ops):
        decisions.append({
            "bucket": "resource_ops",
            "index": index,
            "decision": "apply_operational_artifact",
            "op": op.op,
            "resource_id": str(op.resource_id) if op.resource_id else None,
        })

    for index, op in enumerate(diff.new_predictions):
        decisions.append(_claim_decision(
            op,
            index=index,
            bucket="new_predictions",
            override="publish_forecast_artifact",
        ))

    if not decisions:
        decisions.append({
            "bucket": "diff",
            "index": None,
            "decision": "discard_as_noise",
            "reason": "validated_diff_has_no_mutating_ops",
        })

    return decisions


def _claim_decision(
    op: ClaimOp,
    *,
    index: int,
    bucket: str,
    override: SynthesisDecisionKind | None = None,
) -> dict[str, Any]:
    if override is not None:
        decision: SynthesisDecisionKind = override
    elif op.op == "archive":
        decision = "archive_model"
    elif op.op == "update":
        changed = set((op.changes or {}).keys())
        decision = (
            "attach_evidence_to_existing_model"
            if changed and changed.issubset(_EVIDENCE_UPDATE_FIELDS)
            else "update_existing_model"
        )
    else:
        entry = op.entry or {}
        prop = entry.get("proposition") if isinstance(entry, dict) else {}
        grammar = derive_memory_grammar(
            prop if isinstance(prop, dict) else {},
            natural=str(entry.get("natural") or ""),
            scope_entities=entry.get("scope_entities") or [],
        )
        if grammar.claim_role == "situation":
            decision = "create_or_update_situation"
        elif grammar.claim_role == "recommendation":
            decision = "create_action_proposal"
        else:
            decision = "create_atomic_model"

    out: dict[str, Any] = {
        "bucket": bucket,
        "index": index,
        "decision": decision,
        "op": op.op,
    }
    if op.model_id is not None:
        out["model_id"] = str(op.model_id)
    if isinstance(op.entry, dict):
        prop = op.entry.get("proposition")
        if isinstance(prop, dict):
            grammar = derive_memory_grammar(
                prop,
                natural=str(op.entry.get("natural") or ""),
                scope_entities=op.entry.get("scope_entities") or [],
            )
            out.update({
                "proposition_kind": prop.get("kind"),
                "claim_role": grammar.claim_role,
                "abstraction_level": grammar.abstraction_level,
                "time_mode": grammar.time_mode,
                "modality": grammar.modality,
                "polarity": grammar.polarity,
                "domain_tags": list(grammar.domain_tags),
            })
    return out


__all__ = ["SynthesisDecisionKind", "summarize_synthesis_decisions"]
