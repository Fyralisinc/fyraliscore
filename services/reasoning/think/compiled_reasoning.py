"""Compiled reasoning paths for narrow, code-emittable Think decisions.

The broad Think prompt is still the right fallback for open-ended evidence
synthesis. This module handles cases where upstream code has already produced
explicit candidate state transitions and the LLM only needs to adjudicate them.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from lib.shared.edge_registry import EDGE_REGISTRY
from lib.shared.ids import uuid7
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext

from .diff_schema import ActOp, ClaimOp, EdgeOp, RawDiff


DecisionKind = Literal["accept", "reject"]
BatchMemoryOperation = Literal[
    "claim",
    "claim_update",
    "situation",
    "edge",
    "claim_and_edge",
    "situation_and_edge",
    "claim_and_act",
    "act",
    "no_op",
]
BatchClaimRole = Literal["fact", "concern", "pattern", "situation"]
BatchActType = Literal["commitment", "goal", "decision"]


class RelationshipCandidateDecision(BaseModel):
    """LLM decision over one pre-truth relationship candidate."""

    model_config = ConfigDict(extra="forbid")

    candidate_id: UUID
    decision: DecisionKind
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str = Field(min_length=1, max_length=600)


class RelationshipCandidateDecisionSet(BaseModel):
    """Compact output shape for compiled relationship-candidate reasoning."""

    model_config = ConfigDict(extra="forbid")

    decisions: list[RelationshipCandidateDecision] = Field(default_factory=list)
    reasoning_trace: str | None = Field(default=None, max_length=1200)


class BatchMemoryCandidateDecision(BaseModel):
    """Compact LLM decision over one inquiry memory-decision candidate."""

    model_config = ConfigDict(extra="forbid")

    candidate_id: str = Field(min_length=1, max_length=120)
    decision: DecisionKind
    operation: BatchMemoryOperation = "claim"
    confidence: float = Field(ge=0.0, le=1.0)
    claim_role: BatchClaimRole = "concern"
    claim_text: str | None = Field(default=None, max_length=500)
    model_id: UUID | None = None
    situation_member_model_ids: list[UUID] = Field(default_factory=list, max_length=8)
    pressure_type: str | None = Field(default=None, max_length=32)
    edge_kind: str | None = Field(default=None, max_length=80)
    source_model_id: UUID | None = None
    target_model_id: UUID | None = None
    act_type: BatchActType | None = None
    act_target_id: UUID | None = None
    act_new_state: str | None = Field(default=None, max_length=40)
    reason: str = Field(min_length=1, max_length=700)


class BatchMemoryDecisionSet(BaseModel):
    """Compact output shape for compiled T1 batch memory reasoning."""

    model_config = ConfigDict(extra="forbid")

    decisions: list[BatchMemoryCandidateDecision] = Field(default_factory=list)
    reasoning_trace: str | None = Field(default=None, max_length=1400)


@dataclass(frozen=True)
class CompiledRelationshipCandidateRequest:
    system: str
    user: str
    candidates: tuple[dict[str, Any], ...]

    def to_raw_diff(
        self,
        decisions: RelationshipCandidateDecisionSet,
        *,
        trigger: TriggerContext,
        trigger_ref: UUID,
    ) -> RawDiff:
        by_id = {
            candidate_id: candidate
            for candidate in self.candidates
            if (candidate_id := _coerce_uuid(candidate.get("id"))) is not None
        }
        edge_ops: list[EdgeOp] = []
        trace_parts: list[str] = []
        accepted = 0
        rejected = 0
        blocked = 0

        for decision in decisions.decisions:
            candidate = by_id.get(decision.candidate_id)
            if candidate is None:
                blocked += 1
                trace_parts.append(
                    f"{decision.candidate_id}: ignored unknown candidate id"
                )
                continue
            if decision.decision != "accept":
                rejected += 1
                trace_parts.append(
                    f"{decision.candidate_id}: rejected - {decision.reason}"
                )
                continue
            edge_op, block_reason = _edge_op_from_candidate(
                candidate,
                decision,
            )
            if edge_op is None:
                blocked += 1
                trace_parts.append(
                    f"{decision.candidate_id}: not promoted - {block_reason}"
                )
                continue
            accepted += 1
            edge_ops.append(edge_op)
            trace_parts.append(
                f"{decision.candidate_id}: accepted {edge_op.edge_kind} "
                f"{edge_op.source_model_id}->{edge_op.target_model_id}"
            )

        if decisions.reasoning_trace:
            trace_parts.insert(0, decisions.reasoning_trace)
        trace_parts.append(
            "compiled_relationship_candidate_decisions="
            f"accepted:{accepted},rejected:{rejected},blocked:{blocked}"
        )

        return RawDiff(
            trigger_ref=trigger_ref,
            tenant_id=trigger.tenant_id,
            claim_ops=[],
            edge_ops=edge_ops,
            ontology_gap_ops=[],
            act_ops=[],
            resource_ops=[],
            new_predictions=[],
            reasoning_trace="; ".join(part for part in trace_parts if part),
        )


@dataclass(frozen=True)
class CompiledBatchMemoryDecisionRequest:
    system: str
    user: str
    candidates: tuple[dict[str, Any], ...]

    def to_raw_diff(
        self,
        decisions: BatchMemoryDecisionSet,
        *,
        trigger: TriggerContext,
        trigger_ref: UUID,
    ) -> RawDiff:
        by_id = {
            str(candidate.get("candidate_id")): candidate
            for candidate in self.candidates
            if candidate.get("candidate_id")
        }
        claim_ops: list[ClaimOp] = []
        edge_ops: list[EdgeOp] = []
        act_ops: list[ActOp] = []
        accepted = 0
        rejected = 0
        blocked = 0
        trace_parts: list[str] = []
        candidate_claim_placeholders: dict[str, UUID] = {}

        for decision in decisions.decisions:
            candidate = by_id.get(decision.candidate_id)
            if candidate is None:
                blocked += 1
                trace_parts.append(
                    f"{decision.candidate_id}: ignored unknown candidate id"
                )
                continue
            if decision.decision != "accept" or decision.operation == "no_op":
                rejected += 1
                trace_parts.append(
                    f"{decision.candidate_id}: rejected - {decision.reason}"
                )
                continue

            claim_placeholder: UUID | None = None
            if decision.operation == "claim_update":
                claim_update_op, claim_placeholder, block_reason = (
                    _claim_update_op_from_batch_decision(candidate, decision)
                )
                if claim_update_op is None or claim_placeholder is None:
                    blocked += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: update not promoted - "
                        f"{block_reason}"
                    )
                    continue
                claim_ops.append(claim_update_op)
            elif decision.operation in {"situation", "situation_and_edge"}:
                claim_op, claim_placeholder, block_reason = _claim_op_from_batch_decision(
                    candidate,
                    decision,
                    trigger,
                    force_role="situation",
                )
                if claim_op is None or claim_placeholder is None:
                    blocked += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: situation not promoted - "
                        f"{block_reason}"
                    )
                    continue
                claim_ops.append(claim_op)
                candidate_claim_placeholders[decision.candidate_id] = claim_placeholder
            elif decision.operation in {"claim", "claim_and_edge", "claim_and_act"}:
                claim_op, claim_placeholder, block_reason = _claim_op_from_batch_decision(
                    candidate,
                    decision,
                    trigger,
                )
                if claim_op is None or claim_placeholder is None:
                    blocked += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: claim not promoted - "
                        f"{block_reason}"
                    )
                    continue
                claim_ops.append(claim_op)
                candidate_claim_placeholders[decision.candidate_id] = claim_placeholder

            if decision.operation in {"edge", "claim_and_edge", "situation_and_edge"}:
                edge_op, block_reason = _edge_op_from_batch_decision(
                    candidate,
                    decision,
                    claim_placeholder=claim_placeholder,
                )
                if edge_op is None:
                    blocked += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: edge not promoted - "
                        f"{block_reason}"
                    )
                    continue
                edge_ops.append(edge_op)

            if decision.operation in {"act", "claim_and_act"}:
                act_op, block_reason = _act_op_from_batch_decision(
                    candidate,
                    decision,
                    confidence_basis=(
                        claim_placeholder
                        or _first_uuid(candidate.get("evidence_model_ids"))
                        or _first_uuid(candidate.get("target_model_ids"))
                    ),
                )
                if act_op is None:
                    blocked += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: act not promoted - "
                        f"{block_reason}"
                    )
                    continue
                act_ops.append(act_op)

            accepted += 1
            trace_parts.append(
                f"{decision.candidate_id}: accepted {decision.operation} "
                f"confidence={decision.confidence:.2f}"
            )

        if not edge_ops:
            no_edge_line = _batch_no_edge_accountability_line(self.candidates)
            if no_edge_line:
                trace_parts.append(no_edge_line)

        if decisions.reasoning_trace:
            trace_parts.insert(0, decisions.reasoning_trace)
        trace_parts.append(
            "compiled_batch_memory_decisions="
            f"accepted:{accepted},rejected:{rejected},blocked:{blocked}"
        )
        if candidate_claim_placeholders:
            trace_parts.append(
                "new_claim_placeholders="
                + ",".join(
                    f"{cid}:{placeholder}"
                    for cid, placeholder in sorted(candidate_claim_placeholders.items())
                )
            )

        return RawDiff(
            trigger_ref=trigger_ref,
            tenant_id=trigger.tenant_id,
            claim_ops=claim_ops,
            edge_ops=edge_ops,
            ontology_gap_ops=[],
            act_ops=act_ops,
            resource_ops=[],
            new_predictions=[],
            reasoning_trace="; ".join(part for part in trace_parts if part),
        )


def compiled_relationship_candidate_enabled() -> bool:
    return os.environ.get(
        "THINK_COMPILED_RELATIONSHIP_REASONING",
        "0",
    ).strip().lower() in {"1", "true", "yes", "on"}


def compiled_relationship_candidate_max_tokens(default: int = 768) -> int:
    raw = os.environ.get("THINK_COMPILED_RELATIONSHIP_MAX_TOKENS")
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(256, int(raw))
    except ValueError:
        return default


def compiled_batch_memory_decision_enabled() -> bool:
    return os.environ.get(
        "THINK_COMPILED_BATCH_MEMORY_REASONING",
        "0",
    ).strip().lower() in {"1", "true", "yes", "on"}


def compiled_batch_memory_decision_max_tokens(default: int = 1200) -> int:
    raw = os.environ.get("THINK_COMPILED_BATCH_MEMORY_MAX_TOKENS")
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(512, int(raw))
    except ValueError:
        return default


def build_compiled_batch_memory_decision_request(
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> CompiledBatchMemoryDecisionRequest | None:
    if not compiled_batch_memory_decision_enabled():
        return None
    if trigger.kind != "T1" or not trigger.is_batch:
        return None
    packet = _inquiry_context_packet(bundle)
    if packet is None:
        return None
    candidates = _memory_candidates_from_packet(packet)
    if not candidates:
        return None
    max_candidates = _env_int("THINK_COMPILED_BATCH_MEMORY_MAX_CANDIDATES", 5)
    candidates = candidates[:max_candidates]
    if _compiled_batch_requires_open_writer_surface(packet, candidates):
        return None
    system = (
        "You adjudicate closed-world memory-decision candidates for a Fyralis "
        "T1 event batch. You do not author RawDiff JSON. For each listed "
        "candidate, decide whether code should emit a durable memory mutation. "
        "The batch is the unit of reasoning: accept compact synthesis when "
        "multiple signals or selected Models jointly change the world model. "
        "Prefer updates over duplicate inserts, situations for composite "
        "batch-level understanding, and edges when selected graph context is "
        "decision-relevant. Reject/no-op only when uncertainty is decisive, "
        "evidence is merely background, or ids would need to be invented."
    )
    user = _build_batch_memory_decision_user_prompt(trigger, bundle, packet, candidates)
    return CompiledBatchMemoryDecisionRequest(
        system=system,
        user=user,
        candidates=tuple(candidates),
    )


def _inquiry_context_packet(bundle: ContextBundle) -> dict[str, Any] | None:
    notes = bundle.notes if isinstance(bundle.notes, dict) else {}
    packet = notes.get("inquiry_context_packet")
    return packet if isinstance(packet, dict) else None


def _memory_candidates_from_packet(packet: dict[str, Any]) -> list[dict[str, Any]]:
    raw = packet.get("memory_decision_candidates") or []
    return [dict(candidate) for candidate in raw if isinstance(candidate, dict)]


_OPEN_WRITER_SURFACE_TOKENS = (
    "resource_ops",
    "resource op",
    "resource transaction",
    "resource pool",
    "resource_id",
    "resource threshold",
    "headcount",
    "staffing",
    "budget",
    "runway",
    "prediction",
    "prediction_deadline",
    "evaluate_at",
    "future validation",
    "future_validation",
    "forecast validation",
)


def _compiled_batch_requires_open_writer_surface(
    packet: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> bool:
    probe = _jsonish(
        {
            "signal_summary": packet.get("signal_summary"),
            "important_unknowns": packet.get("important_unknowns"),
            "memory_decision_candidates": candidates,
            "tiers": packet.get("tiers"),
        }
    ).lower()
    return any(token in probe for token in _OPEN_WRITER_SURFACE_TOKENS)


def _build_batch_memory_decision_user_prompt(
    trigger: TriggerContext,
    bundle: ContextBundle,
    packet: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> str:
    lines = [
        "<compiled_batch_memory_task>",
        f"tenant_id: {trigger.tenant_id}",
        f"trigger_kind: {trigger.kind}:event_batch",
        f"candidate_count: {len(candidates)}",
        "Return one decision for each candidate_id. Use only ids shown below.",
        "operation choices:",
        "- claim: emit one atomic Model claim from claim_text",
        "- claim_update: attach evidence/confidence to model_id or a target_model_id",
        "- situation: emit one composite situation Model with member_model_ids",
        "- claim_and_edge: emit one atomic claim, then one edge from that new claim to target_model_id",
        "- situation_and_edge: emit one situation, then one edge from it to target_model_id",
        "- claim_and_act: emit one atomic claim, then one act transition for act_target_id",
        "- edge: emit one edge only between source_model_id and target_model_id",
        "- act: emit one act transition only",
        "- no_op: no world-model mutation",
        "Allowed edge kinds: " + ", ".join(sorted(EDGE_REGISTRY.keys())),
        "Do not emit resource writes, prediction lifecycle ops, or ontology gaps here.",
        "</compiled_batch_memory_task>",
        "<batch_summary>",
        _trunc(str(packet.get("signal_summary") or ""), 1000),
        "</batch_summary>",
    ]
    verdict = packet.get("sufficiency_verdict")
    if isinstance(verdict, dict):
        lines.extend(
            [
                "<sufficiency>",
                _trunc(_jsonish(verdict), 900),
                "</sufficiency>",
            ]
        )
    unknowns = packet.get("important_unknowns")
    if unknowns:
        lines.extend(
            [
                "<remaining_uncertainty>",
                _trunc(_jsonish(unknowns), 700),
                "</remaining_uncertainty>",
            ]
        )

    lines.append("<memory_decision_candidates>")
    for candidate in candidates:
        lines.extend(_batch_candidate_lines(candidate))
    lines.append("</memory_decision_candidates>")

    evidence_lines = _batch_candidate_evidence_lines(packet, candidates)
    if evidence_lines:
        lines.append("<candidate_evidence>")
        lines.extend(evidence_lines)
        lines.append("</candidate_evidence>")

    model_lines = _batch_model_card_lines(bundle, candidates)
    if model_lines:
        lines.append("<allowed_model_cards>")
        lines.extend(model_lines)
        lines.append("</allowed_model_cards>")

    act_lines = _batch_act_card_lines(bundle, candidates)
    if act_lines:
        lines.append("<allowed_act_cards>")
        lines.extend(act_lines)
        lines.append("</allowed_act_cards>")

    lines.extend(
        [
            "<decision_rules>",
            "For claim/claim_and_edge/claim_and_act, claim_text must be a single durable atomic sentence under 500 chars.",
            "Use claim_update when the candidate mainly confirms, weakens, or adds evidence to an existing target/evidence Model.",
            "Use situation when the batch exposes a composite condition across multiple signals or selected Models; provide 2-8 situation_member_model_ids when available.",
            "Use claim_role=concern for blockers, risks, waiting states, churn, trust, or negative pressure.",
            "Use claim_role=fact for neutral observed progress or state.",
            "Use claim_role=pattern only for repeated behavior directly supported in candidate_evidence.",
            "Use claim_role=situation only with operation=situation or situation_and_edge.",
            "For edge ops, target/source ids must be candidate target/evidence Model ids; if unsure, choose the sharpest edge_kind and explain uncertainty.",
            "Prefer blocks for blockers/dependencies, early_warning_for for account risk, contributes_to_resolution for mitigating evidence, explains for causal interpretation, weakens for contradiction, and supports for evidence support.",
            "For act transitions, act_type and act_target_id must match an allowed act card; choose only a legal-looking next state.",
            "Do not use doneverified unless decisive authoritative completion evidence is shown.",
            "If selected graph/target Models are relevant but no edge is warranted, reject/no_op with phrase 'no edge warranted' and cite the full Model UUIDs.",
            "</decision_rules>",
        ]
    )
    return "\n".join(lines)


def _batch_candidate_lines(candidate: dict[str, Any]) -> list[str]:
    keys = (
        "candidate_id",
        "op_family",
        "confidence",
        "proposed_text",
        "target_model_ids",
        "target_act_ids",
        "evidence_model_ids",
        "source_observation_ids",
        "supporting_evidence_ids",
        "counterevidence_ids",
        "uncertainty_slots",
        "retrieval_targets",
        "reason",
    )
    lines = ["  <candidate>"]
    for key in keys:
        value = candidate.get(key)
        if value in (None, [], {}, ""):
            continue
        lines.append(f"    {key}: {_trunc(_jsonish(value), 900)}")
    lines.append("  </candidate>")
    return lines


def _batch_candidate_evidence_lines(
    packet: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> list[str]:
    wanted = {
        str(evidence_id)
        for candidate in candidates
        for key in ("supporting_evidence_ids", "counterevidence_ids")
        for evidence_id in (candidate.get(key) or [])
        if evidence_id
    }
    tiers = packet.get("tiers") if isinstance(packet.get("tiers"), dict) else {}
    decisive = tiers.get("decisive_evidence") or []
    supporting = tiers.get("supporting_evidence_groups") or []
    lines: list[str] = []
    seen: set[str] = set()
    for item in decisive:
        if not isinstance(item, dict):
            continue
        evidence_id = str(item.get("evidence_id") or "")
        if wanted and evidence_id not in wanted:
            continue
        if evidence_id in seen:
            continue
        seen.add(evidence_id)
        compact_item = {
            "evidence_id": evidence_id,
            "source_type": item.get("source_type"),
            "source_ref": item.get("source_ref"),
            "summary": item.get("summary"),
            "supports": item.get("supports_hypotheses"),
            "weakens": item.get("weakens_hypotheses"),
            "contradicts": item.get("contradicts_hypotheses"),
        }
        lines.append("  - " + _trunc(_jsonish(compact_item), 750))
        if len(lines) >= 8:
            return lines
    for group in supporting:
        if not isinstance(group, dict):
            continue
        evidence_ids = {str(eid) for eid in (group.get("evidence_ids") or [])}
        if wanted and not (wanted & evidence_ids):
            continue
        key = ",".join(sorted(evidence_ids))
        if key in seen:
            continue
        seen.add(key)
        compact_group = {
            "claim_supported": group.get("claim_supported"),
            "evidence_count": group.get("evidence_count"),
            "sources": group.get("sources"),
            "evidence_ids": sorted(evidence_ids)[:8],
            "source_refs": (group.get("source_refs") or [])[:6],
            "summary": group.get("summary"),
        }
        lines.append("  - " + _trunc(_jsonish(compact_group), 750))
        if len(lines) >= 8:
            return lines
    return lines


def _batch_model_card_lines(
    bundle: ContextBundle,
    candidates: list[dict[str, Any]],
) -> list[str]:
    wanted = {
        str(model_id)
        for candidate in candidates
        for key in (
            "target_model_ids",
            "evidence_model_ids",
            "situation_member_model_ids",
        )
        for model_id in (candidate.get(key) or [])
        if model_id
    }
    if not wanted:
        return []
    lines: list[str] = []
    shown = 0
    for model in bundle.models or []:
        model_id = str(getattr(model, "id", ""))
        if model_id not in wanted:
            continue
        lines.extend(_model_card_lines("model", model))
        shown += 1
        if shown >= 10:
            break
    return lines


def _batch_act_card_lines(
    bundle: ContextBundle,
    candidates: list[dict[str, Any]],
) -> list[str]:
    wanted = {
        str(act_id)
        for candidate in candidates
        for act_id in (candidate.get("target_act_ids") or [])
        if act_id
    }
    if not wanted:
        return []
    lines: list[str] = []
    for label, items in (
        ("commitment", bundle.acts_summary.get("commitments", [])),
        ("goal", bundle.acts_summary.get("goals", [])),
        ("decision", bundle.acts_summary.get("decisions", [])),
    ):
        for item in items:
            item_id = str(getattr(item, "id", ""))
            if item_id not in wanted:
                continue
            title = getattr(item, "title", None)
            state = getattr(item, "state", None)
            lines.append(
                "  - "
                + _trunc(
                    _jsonish(
                        {
                            "type": label,
                            "id": item_id,
                            "state": state,
                            "title": title,
                        }
                    ),
                    700,
                )
            )
    return lines


def _claim_op_from_batch_decision(
    candidate: dict[str, Any],
    decision: BatchMemoryCandidateDecision,
    trigger: TriggerContext,
    *,
    force_role: BatchClaimRole | None = None,
) -> tuple[ClaimOp | None, UUID | None, str]:
    confidence_floor = _env_float("THINK_COMPILED_BATCH_ACCEPT_MIN_CONFIDENCE", 0.55)
    if decision.confidence < confidence_floor:
        return None, None, "decision confidence below promotion floor"
    text = str(decision.claim_text or candidate.get("proposed_text") or "").strip()
    if len(text) < 12:
        return None, None, "claim_text is too short"
    born_event = uuid7()
    role = force_role or decision.claim_role
    proposition = _batch_claim_proposition(role, text, candidate, decision)
    evidence_event_ids = [
        str(value) for value in _uuid_values(candidate.get("source_observation_ids"))
    ]
    if evidence_event_ids:
        proposition["evidence_event_ids"] = evidence_event_ids
    entry = {
        "tenant_id": str(trigger.tenant_id),
        "born_from_event_id": str(born_event),
        "proposition": proposition,
        "natural": _trunc(text, 1000),
        "confidence": min(0.69, max(0.35, float(decision.confidence))),
        "confidence_at_assertion": min(0.69, max(0.35, float(decision.confidence))),
        "scope_actors": [str(actor_id) for actor_id in (trigger.scope_actors or [])],
        "scope_entities": _scope_entities(trigger),
        "scope_temporal": {},
        "falsifier": None,
    }
    return ClaimOp(op="insert", entry=entry), born_event, ""


def _claim_update_op_from_batch_decision(
    candidate: dict[str, Any],
    decision: BatchMemoryCandidateDecision,
) -> tuple[ClaimOp | None, UUID | None, str]:
    confidence_floor = _env_float("THINK_COMPILED_BATCH_UPDATE_MIN_CONFIDENCE", 0.52)
    if decision.confidence < confidence_floor:
        return None, None, "decision confidence below update floor"
    model_id = (
        decision.model_id
        or decision.target_model_id
        or _first_uuid(candidate.get("target_model_ids"))
        or _first_uuid(candidate.get("evidence_model_ids"))
    )
    if model_id is None:
        return None, None, "missing model_id for update"
    evidence_event_ids = _uuid_values(candidate.get("source_observation_ids"))
    changes: dict[str, Any] = {
        "confidence": min(0.74, max(0.35, float(decision.confidence))),
    }
    if evidence_event_ids:
        changes["supporting_event_ids"] = evidence_event_ids
    return ClaimOp(op="update", model_id=model_id, changes=changes), model_id, ""


def _batch_claim_proposition(
    role: BatchClaimRole,
    text: str,
    candidate: dict[str, Any],
    decision: BatchMemoryCandidateDecision,
) -> dict[str, Any]:
    evidence_event_ids = [
        str(value) for value in _uuid_values(candidate.get("source_observation_ids"))
    ]
    if role == "situation":
        members = _situation_member_ids(candidate, decision)
        return {
            "kind": "belief",
            "claim_role": "situation",
            "abstraction_level": "composite",
            "situation": _trunc(text, 180),
            "summary": text,
            "member_model_ids": [str(member) for member in members],
            "relationship_summary": _trunc(decision.reason, 360),
            "status": "forming",
            "pressure_type": _pressure_type(decision.pressure_type, text),
            "shared_mechanism": _trunc(
                str(candidate.get("reason") or decision.reason),
                280,
            ),
            "judgment_change": _trunc(text, 280),
            "affected_decisions": [],
            "affected_customers": _affected_customers(candidate),
            "affected_teams": [],
            "evidence_event_ids": evidence_event_ids,
            "open_falsifier": _situation_falsifier(text),
            "compiled_memory_candidate_id": str(candidate.get("candidate_id") or ""),
        }
    base = {
        "kind": "belief",
        "claim_role": role,
        "abstraction_level": "pattern" if role == "pattern" else "atomic",
        "time_mode": "recurring" if role == "pattern" else "current",
        "modality": "inferred",
        "polarity": "negative" if role == "concern" else "neutral",
        "compiled_memory_candidate_id": str(candidate.get("candidate_id") or ""),
    }
    if role == "concern":
        base.update({"about": _claim_about(candidate), "nature": text})
    elif role == "pattern":
        base.update(
            {
                "signature": _trunc(text, 180),
                "observed_tendency": text,
                "trigger_conditions": _trunc(str(candidate.get("reason") or ""), 240),
            }
        )
    else:
        base.update({"subject": _claim_about(candidate), "assertion": text})
    return base


def _situation_member_ids(
    candidate: dict[str, Any],
    decision: BatchMemoryCandidateDecision,
) -> list[UUID]:
    members = list(decision.situation_member_model_ids or [])
    if len(members) < 2:
        members.extend(_uuid_values(candidate.get("target_model_ids")))
    if len(members) < 2:
        members.extend(_uuid_values(candidate.get("evidence_model_ids")))
    deduped = _dedupe_uuids(members)
    return deduped[:8]


def _pressure_type(raw: str | None, text: str) -> str:
    allowed = {
        "capacity",
        "trust",
        "revenue",
        "compliance",
        "decision",
        "execution",
        "market",
        "resource",
    }
    value = str(raw or "").strip().lower()
    if value in allowed:
        return value
    lower = text.lower()
    if any(token in lower for token in ("soc2", "audit", "security", "compliance")):
        return "compliance"
    if any(token in lower for token in ("churn", "renewal", "forecast", "revenue")):
        return "revenue"
    if any(token in lower for token in ("capacity", "handoff", "owner", "staff")):
        return "capacity"
    if any(token in lower for token in ("trust", "confidence", "sponsor")):
        return "trust"
    if any(token in lower for token in ("approval", "decision", "procurement", "legal")):
        return "decision"
    return "execution"


def _affected_customers(candidate: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for value in candidate.get("retrieval_targets") or []:
        text = str(value).strip()
        if text and text not in values:
            values.append(text)
    return values[:4]


def _situation_falsifier(text: str) -> str:
    return (
        "Invalid if later authoritative evidence shows the batch-level "
        f"condition is not true: {_trunc(text, 180)}"
    )


def _claim_about(candidate: dict[str, Any]) -> str:
    targets = [
        str(value)
        for key in ("target_model_ids", "target_act_ids", "evidence_model_ids")
        for value in (candidate.get(key) or [])
        if value
    ]
    if targets:
        return targets[0]
    return "batch"


def _edge_op_from_batch_decision(
    candidate: dict[str, Any],
    decision: BatchMemoryCandidateDecision,
    *,
    claim_placeholder: UUID | None,
) -> tuple[EdgeOp | None, str]:
    edge_kind = _default_batch_edge_kind(decision, candidate)
    if edge_kind not in EDGE_REGISTRY:
        return None, "edge_kind is not writable by compiled path"
    target_model_id = decision.target_model_id
    if target_model_id is None:
        target_model_id = _first_uuid(candidate.get("target_model_ids")) or _first_uuid(
            candidate.get("evidence_model_ids")
        )
    source_model_id = decision.source_model_id
    if source_model_id is None:
        source_model_id = claim_placeholder
    if source_model_id is None and target_model_id is not None:
        for candidate_source in _candidate_model_ids(candidate):
            if candidate_source != target_model_id:
                source_model_id = candidate_source
                break
    if target_model_id is None and source_model_id is not None:
        for candidate_target in _candidate_model_ids(candidate):
            if candidate_target != source_model_id:
                target_model_id = candidate_target
                break
    if source_model_id is None or target_model_id is None:
        return None, "missing concrete edge endpoint"
    if source_model_id == target_model_id:
        return None, "self-edge candidate"
    confidence_floor = _env_float("THINK_COMPILED_BATCH_EDGE_MIN_CONFIDENCE", 0.6)
    if decision.confidence < confidence_floor:
        return None, "decision confidence below edge floor"
    evidence_models = _uuid_values(candidate.get("evidence_model_ids"))
    evidence_models.extend(_uuid_values(candidate.get("target_model_ids")))
    evidence_events = _uuid_values(candidate.get("source_observation_ids"))
    metadata = {
        "memory_decision_candidate_id": decision.candidate_id,
        "compiled_reasoning": True,
        "compiled_decision_confidence": decision.confidence,
        "memory_decision_family": candidate.get("op_family"),
    }
    return (
        EdgeOp(
            op="add",
            source_model_id=source_model_id,
            target_model_id=target_model_id,
            edge_kind=edge_kind,
            confidence=float(decision.confidence),
            evidence_event_ids=_dedupe_uuids(evidence_events),
            evidence_model_ids=_dedupe_uuids(evidence_models),
            explanation=_trunc(decision.reason, 1000),
            metadata={k: v for k, v in metadata.items() if v is not None},
            review_status="accepted" if decision.confidence >= 0.7 else "candidate",
            detected_by="think_compiled_batch_memory_candidate",
        ),
        "",
    )


def _default_batch_edge_kind(
    decision: BatchMemoryCandidateDecision,
    candidate: dict[str, Any],
) -> str:
    explicit = str(decision.edge_kind or "").strip()
    if explicit:
        return explicit
    text = " ".join(
        str(part or "")
        for part in (
            decision.claim_text,
            decision.reason,
            candidate.get("proposed_text"),
            candidate.get("reason"),
            candidate.get("op_family"),
        )
    ).lower()
    choices = (
        (("block", "blocked", "dependency", "depends", "constraint", "waiting"), "blocks"),
        (("risk", "warning", "churn", "renewal", "forecast"), "early_warning_for"),
        (("resolve", "mitigate", "unlock", "remediate"), "contributes_to_resolution"),
        (("contradict", "stale", "weakens", "undermine"), "weakens"),
        (("because", "caus", "explain"), "explains"),
    )
    for tokens, edge_kind in choices:
        if any(token in text for token in tokens) and edge_kind in EDGE_REGISTRY:
            return edge_kind
    return "supports"


def _candidate_model_ids(candidate: dict[str, Any]) -> list[UUID]:
    values: list[UUID] = []
    values.extend(_uuid_values(candidate.get("target_model_ids")))
    values.extend(_uuid_values(candidate.get("evidence_model_ids")))
    return _dedupe_uuids(values)


def _batch_no_edge_accountability_line(
    candidates: tuple[dict[str, Any], ...],
) -> str | None:
    model_ids: list[UUID] = []
    for candidate in candidates:
        model_ids.extend(_candidate_model_ids(candidate))
    model_ids = _dedupe_uuids(model_ids)
    if not model_ids:
        return None
    shown = ", ".join(str(model_id) for model_id in model_ids[:10])
    suffix = "..." if len(model_ids) > 10 else ""
    return f"no edge warranted for candidate/selected model ids: {shown}{suffix}"


def _act_op_from_batch_decision(
    candidate: dict[str, Any],
    decision: BatchMemoryCandidateDecision,
    *,
    confidence_basis: UUID | None,
) -> tuple[ActOp | None, str]:
    if confidence_basis is None:
        return None, "missing confidence basis"
    if decision.act_type is None or decision.act_target_id is None:
        return None, "missing act target"
    new_state = str(decision.act_new_state or "").strip()
    if not _act_state_allowed(decision.act_type, new_state):
        return None, "unsupported act target state"
    confidence_floor = _env_float("THINK_COMPILED_BATCH_ACT_MIN_CONFIDENCE", 0.6)
    if decision.confidence < confidence_floor:
        return None, "decision confidence below act floor"
    entity: dict[str, Any] = {
        "id": decision.act_target_id,
        "new_state": new_state,
        "reason": _trunc(decision.reason, 500),
    }
    evidence_events = _uuid_values(candidate.get("source_observation_ids"))
    if new_state == "doneverified" and evidence_events:
        entity["resolved_by_event_ids"] = evidence_events
    return (
        ActOp(
            op=f"transition_{decision.act_type}",  # type: ignore[arg-type]
            confidence_basis=confidence_basis,
            entity=entity,
        ),
        "",
    )


def _act_state_allowed(act_type: BatchActType, state: str) -> bool:
    allowed = {
        "commitment": {
            "active",
            "blocked",
            "paused",
            "doneunverified",
            "doneverified",
            "closed",
        },
        "goal": {"active", "paused", "achieved", "abandoned"},
        "decision": {"active", "revisited", "archived"},
    }
    return state in allowed[act_type]


def _scope_entities(trigger: TriggerContext) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for entity in trigger.seed_entity_ids or []:
        if not isinstance(entity, dict):
            continue
        entity_type = entity.get("type")
        entity_id = entity.get("id")
        if entity_type and entity_id:
            out.append({"type": str(entity_type), "id": str(entity_id)})
    return out[:12]


def build_compiled_relationship_candidate_request(
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> CompiledRelationshipCandidateRequest | None:
    """Return a compiled request for T4 relationship candidates, if safe.

    Situation candidates still need open-ended synthesis into a new Model, so
    they intentionally fall back to the broad RawDiff prompt.
    """
    if (
        trigger.kind != "T4"
        or trigger.subkind != "latent_relationship_candidate"
    ):
        return None
    candidates = _relationship_candidates_from_trigger(trigger)
    if not candidates:
        return None
    if any(candidate.get("candidate_kind") == "situation" for candidate in candidates):
        return None

    max_candidates = _env_int("THINK_COMPILED_RELATIONSHIP_MAX_CANDIDATES", 8)
    if len(candidates) > max_candidates:
        return None

    system = (
        "You adjudicate pre-truth relationship candidates for the Think "
        "process. Decide only whether each listed candidate should become a "
        "durable Model edge now. Accept only when the candidate and model "
        "cards show a specific, useful, non-duplicative relationship. Reject "
        "weak overlap, topical similarity, speculative topology, or candidates "
        "that need a new ontology/situation rather than an existing edge. "
        "Do not author claims, actions, resources, or new edge kinds."
    )
    user = _build_relationship_candidate_user_prompt(
        trigger,
        bundle,
        candidates,
    )
    return CompiledRelationshipCandidateRequest(
        system=system,
        user=user,
        candidates=tuple(candidates),
    )


def _build_relationship_candidate_user_prompt(
    trigger: TriggerContext,
    bundle: ContextBundle,
    candidates: list[dict[str, Any]],
) -> str:
    models_by_id = {
        str(getattr(model, "id", "")): model
        for model in (bundle.models or [])
        if getattr(model, "id", None) is not None
    }
    lines = [
        "<compiled_relationship_candidate_task>",
        f"tenant_id: {trigger.tenant_id}",
        f"candidate_count: {len(candidates)}",
        "For every candidate_id listed below, return exactly one decision: "
        "accept or reject.",
        "Accept means code will emit an edge using only the candidate's "
        "existing source_model_id, target_model_id, and edge_kind.",
        "Reject means no world-model mutation is applied for that candidate.",
        "</compiled_relationship_candidate_task>",
        "<candidate_decision_rules>",
        "accept only if the source and target are concrete existing Models",
        "accept only if edge_kind is already named on the candidate",
        "accept only if the explanation names an actual mechanism, dependency, "
        "prediction link, contradiction, or blocking/enabling relation",
        "reject if evidence is merely co-occurrence, shared topic, shared actor, "
        "or similar pressure without a durable relation",
        "reject edge_type candidates; they belong to the ontology workflow",
        "keep each reason short and factual",
        "</candidate_decision_rules>",
        "<relationship_candidates>",
    ]
    for candidate in candidates:
        lines.extend(_candidate_lines(candidate, models_by_id))
    lines.append("</relationship_candidates>")
    return "\n".join(lines)


def _candidate_lines(
    candidate: dict[str, Any],
    models_by_id: dict[str, Any],
) -> list[str]:
    candidate_id = candidate.get("id")
    lines = ["  <candidate>"]
    for key in (
        "id",
        "candidate_kind",
        "basis",
        "edge_kind",
        "source_model_id",
        "target_model_id",
        "member_model_ids",
        "evidence_model_ids",
        "judgment_leverage_score",
    ):
        value = candidate.get(key)
        if value not in (None, [], {}):
            lines.append(f"    {key}: {_trunc(_jsonish(value), 900)}")
    explanation = candidate.get("explanation")
    if explanation:
        lines.append(f"    explanation: {_trunc(str(explanation), 900)}")
    metadata = candidate.get("metadata")
    if isinstance(metadata, dict):
        structural = {
            key: metadata.get(key)
            for key in (
                "mechanism",
                "dependency_basis",
                "lead_time_evidence",
                "historical_basis",
            )
            if metadata.get(key) not in (None, [], {})
        }
        causal = metadata.get("causal")
        if isinstance(causal, dict):
            structural["causal"] = {
                key: causal.get(key)
                for key in ("mechanism_summary", "direction", "lag")
                if causal.get(key) not in (None, [], {})
            }
        rule = metadata.get("rule")
        if isinstance(rule, dict):
            structural["rule"] = {
                key: rule.get(key)
                for key in (
                    "dependency_basis",
                    "lead_time_evidence",
                    "historical_basis",
                )
                if rule.get(key) not in (None, [], {})
            }
        if structural:
            lines.append(
                "    structural_evidence: "
                f"{_trunc(_jsonish(structural), 1200)}"
            )
        topology = metadata.get("topology")
        if isinstance(topology, dict):
            compact_topology = {
                key: topology.get(key)
                for key in ("kind", "object_type", "score_components")
                if topology.get(key) not in (None, [], {})
            }
            if compact_topology:
                lines.append(
                    "    topology_evidence: "
                    f"{_trunc(_jsonish(compact_topology), 1200)}"
                )
    for label, model_id in (
        ("source_model", candidate.get("source_model_id")),
        ("target_model", candidate.get("target_model_id")),
    ):
        model = models_by_id.get(str(model_id))
        if model is not None:
            lines.extend(_model_card_lines(label, model))
    if candidate_id is not None:
        lines.append(f"    decision_required_for: {candidate_id}")
    lines.append("  </candidate>")
    return lines


def _model_card_lines(label: str, model: Any) -> list[str]:
    model_id = getattr(model, "id", None)
    lines = [f"    <{label}>"]
    lines.append(f"      id: {model_id}")
    for attr in ("proposition_kind", "status", "confidence", "activation"):
        value = getattr(model, attr, None)
        if value not in (None, [], {}):
            lines.append(f"      {attr}: {_trunc(str(value), 300)}")
    natural = getattr(model, "natural", None)
    if natural:
        lines.append(f"      natural: {_trunc(str(natural), 1000)}")
    proposition = getattr(model, "proposition", None)
    if proposition not in (None, [], {}):
        lines.append(f"      proposition: {_trunc(_jsonish(proposition), 1000)}")
    lines.append(f"    </{label}>")
    return lines


def _relationship_candidates_from_trigger(
    trigger: TriggerContext,
) -> list[dict[str, Any]]:
    signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    raw_candidates = signature.get("relationship_candidates")
    if isinstance(raw_candidates, list):
        return [dict(c) for c in raw_candidates if isinstance(c, dict)]
    raw_candidate = signature.get("relationship_candidate")
    if isinstance(raw_candidate, dict):
        return [dict(raw_candidate)]
    return []


def _edge_op_from_candidate(
    candidate: dict[str, Any],
    decision: RelationshipCandidateDecision,
) -> tuple[EdgeOp | None, str]:
    if candidate.get("candidate_kind") != "edge":
        return None, "candidate_kind is not code-emittable edge"

    source_model_id = _coerce_uuid(candidate.get("source_model_id"))
    target_model_id = _coerce_uuid(candidate.get("target_model_id"))
    edge_kind = candidate.get("edge_kind")
    if source_model_id is None or target_model_id is None:
        return None, "missing concrete edge endpoints"
    if source_model_id == target_model_id:
        return None, "self-edge candidate"
    if not isinstance(edge_kind, str) or not edge_kind.strip():
        return None, "missing edge_kind"
    edge_kind = edge_kind.strip()
    if edge_kind not in EDGE_REGISTRY:
        return None, "edge_kind is not writable by compiled path"

    confidence_floor = _env_float(
        "THINK_COMPILED_RELATIONSHIP_ACCEPT_MIN_CONFIDENCE",
        0.65,
    )
    if decision.confidence < confidence_floor:
        return None, "decision confidence below promotion floor"

    missing_structural = _structural_missing_fields(edge_kind, candidate)
    if missing_structural:
        return None, "missing structural evidence: " + ",".join(missing_structural)

    spec = EDGE_REGISTRY[edge_kind]
    weight = _candidate_weight(candidate)
    if weight is None and spec.weight_required:
        weight = min(1.0, max(0.05, float(decision.confidence)))
    elif weight is not None and not spec.weight_allowed:
        weight = None

    evidence_model_ids = _candidate_uuid_list(candidate, "evidence_model_ids")
    if not evidence_model_ids:
        evidence_model_ids = [
            value
            for value in (
                source_model_id,
                target_model_id,
                *_candidate_uuid_list(candidate, "member_model_ids"),
            )
            if value is not None
        ]
    explanation = _edge_explanation(candidate, decision)
    metadata = {
        "relationship_candidate_id": str(decision.candidate_id),
        "compiled_reasoning": True,
        "compiled_decision": decision.decision,
        "compiled_decision_confidence": decision.confidence,
        "candidate_basis": candidate.get("basis"),
        "judgment_leverage_score": candidate.get("judgment_leverage_score"),
    }
    return (
        EdgeOp(
            op="add",
            source_model_id=source_model_id,
            target_model_id=target_model_id,
            edge_kind=edge_kind,
            weight=weight,
            confidence=float(decision.confidence),
            evidence_event_ids=[],
            evidence_model_ids=_dedupe_uuids(evidence_model_ids),
            explanation=explanation,
            metadata={k: v for k, v in metadata.items() if v is not None},
            review_status="accepted",
            detected_by="think_compiled_relationship_candidate",
        ),
        "",
    )


def _edge_explanation(
    candidate: dict[str, Any],
    decision: RelationshipCandidateDecision,
) -> str:
    parts = []
    candidate_explanation = candidate.get("explanation")
    if candidate_explanation:
        parts.append(str(candidate_explanation).strip())
    parts.append(decision.reason.strip())
    return _trunc(" ".join(part for part in parts if part), 1000)


def _structural_missing_fields(
    edge_kind: str,
    candidate: dict[str, Any],
) -> list[str]:
    metadata = candidate.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    if edge_kind == "blocks":
        if _has_mechanism(metadata) or _has_dependency_basis(metadata):
            return []
        return ["mechanism_or_dependency_basis"]
    if edge_kind in {"explains", "enables"}:
        return [] if _has_mechanism(metadata) else ["mechanism"]
    if edge_kind == "early_warning_for":
        return [] if _has_lead_time_evidence(metadata) else ["lead_time_evidence"]
    return []


def _has_mechanism(metadata: dict[str, Any]) -> bool:
    if isinstance(metadata.get("mechanism"), str) and metadata["mechanism"].strip():
        return True
    causal = metadata.get("causal")
    if isinstance(causal, dict):
        summary = causal.get("mechanism_summary")
        return isinstance(summary, str) and summary.strip() != ""
    return False


def _has_dependency_basis(metadata: dict[str, Any]) -> bool:
    if metadata.get("dependency_basis"):
        return True
    rule = metadata.get("rule")
    return isinstance(rule, dict) and bool(rule.get("dependency_basis"))


def _has_lead_time_evidence(metadata: dict[str, Any]) -> bool:
    if metadata.get("lead_time_evidence") or metadata.get("historical_basis"):
        return True
    rule = metadata.get("rule")
    return isinstance(rule, dict) and bool(
        rule.get("lead_time_evidence") or rule.get("historical_basis")
    )


def _candidate_weight(candidate: dict[str, Any]) -> float | None:
    for source in (candidate, candidate.get("metadata")):
        if not isinstance(source, dict):
            continue
        for key in ("weight", "strength", "edge_weight"):
            value = source.get(key)
            try:
                if value is None:
                    continue
                return min(1.0, max(0.0, float(value)))
            except (TypeError, ValueError):
                continue
    return None


def _candidate_uuid_list(candidate: dict[str, Any], key: str) -> list[UUID]:
    raw = candidate.get(key)
    if not isinstance(raw, (list, tuple)):
        return []
    out: list[UUID] = []
    for value in raw:
        coerced = _coerce_uuid(value)
        if coerced is not None:
            out.append(coerced)
    return out


def _uuid_values(raw: Any) -> list[UUID]:
    if not isinstance(raw, (list, tuple)):
        return []
    out: list[UUID] = []
    for value in raw:
        coerced = _coerce_uuid(value)
        if coerced is not None:
            out.append(coerced)
    return out


def _first_uuid(raw: Any) -> UUID | None:
    values = _uuid_values(raw)
    return values[0] if values else None


def _dedupe_uuids(values: list[UUID]) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _coerce_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _jsonish(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except TypeError:
        return str(value)


def _trunc(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


__all__ = [
    "BatchMemoryCandidateDecision",
    "BatchMemoryDecisionSet",
    "CompiledBatchMemoryDecisionRequest",
    "CompiledRelationshipCandidateRequest",
    "RelationshipCandidateDecision",
    "RelationshipCandidateDecisionSet",
    "build_compiled_batch_memory_decision_request",
    "build_compiled_relationship_candidate_request",
    "compiled_batch_memory_decision_enabled",
    "compiled_batch_memory_decision_max_tokens",
    "compiled_relationship_candidate_enabled",
    "compiled_relationship_candidate_max_tokens",
]
