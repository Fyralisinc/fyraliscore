"""Compiled reasoning paths for narrow, code-emittable Think decisions.

The broad Think prompt is still the right fallback for open-ended evidence
synthesis. This module handles cases where upstream code has already produced
explicit candidate state transitions and the LLM only needs to adjudicate them.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID, NAMESPACE_URL, uuid5

from pydantic import BaseModel, ConfigDict, Field

from lib.shared.edge_registry import EDGE_REGISTRY
from lib.shared.ids import uuid7
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext

from .diff_schema import (
    ActOp,
    ClaimOp,
    EdgeOp,
    MemoryLifecycleAction,
    MemoryLifecycleOp,
    OntologyGapOp,
    RawDiff,
    RelationClaimOp,
    RelationFrameOp,
    RelationFrameParticipantOp,
)


DecisionKind = Literal[
    "accept",
    "reject",
    "candidate",
    "needs_review",
    "ontology_gap",
    "no_edge",
    "noise",
]
BatchMemoryOperation = Literal[
    "claim",
    "claim_update",
    "situation",
    "edge",
    "claim_and_edge",
    "situation_and_edge",
    "claim_and_act",
    "act",
    "memory_lifecycle",
    "no_op",
]
BatchClaimRole = Literal["fact", "concern", "hypothesis", "pattern", "situation"]
BatchActType = Literal["commitment", "goal", "decision"]


class RelationshipCandidateDecision(BaseModel):
    """LLM decision over one pre-truth relationship candidate."""

    model_config = ConfigDict(extra="forbid")

    candidate_id: UUID
    decision: DecisionKind
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str = Field(min_length=1, max_length=600)
    proposed_edge_kind: str | None = Field(default=None, max_length=80)
    dropped_dimensions: list[str] = Field(default_factory=list, max_length=8)


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
    lifecycle_action: MemoryLifecycleAction | None = None
    claim_local_evidence_event_ids: list[UUID] = Field(
        default_factory=list,
        max_length=12,
    )
    confidence_delta: float | None = Field(default=None, ge=-1.0, le=1.0)
    resolution_outcome: bool | None = None
    archive_reason: str | None = Field(default=None, max_length=60)
    superseded_by_model_id: UUID | None = None
    reason: str = Field(min_length=1, max_length=700)


class BatchMemoryDecisionSet(BaseModel):
    """Compact output shape for compiled T1 batch memory reasoning."""

    model_config = ConfigDict(extra="forbid")

    decisions: list[BatchMemoryCandidateDecision] = Field(default_factory=list)
    reasoning_trace: str | None = Field(default=None, max_length=1400)


@dataclass(frozen=True)
class RelationObligation:
    """A relation-bearing fact the batch packet already made visible."""

    candidate_id: str
    edge_kind: str
    confidence: float
    source_model_id: UUID | None
    target_model_id: UUID | None
    evidence_event_ids: tuple[UUID, ...]
    evidence_model_ids: tuple[UUID, ...]
    evidence_text: str
    explanation: str
    matched_markers: tuple[str, ...] = ()


@dataclass(frozen=True)
class RelationFrameParticipantObligation:
    """One model/role binding inside a compiled N-ary frame obligation."""

    role: str
    model_id: UUID
    binding_confidence: float


@dataclass(frozen=True)
class RelationFrameObligation:
    """A role-bound N-ary relation the packet already made visible."""

    relation_kind: str
    confidence: float
    participants: tuple[RelationFrameParticipantObligation, ...]
    evidence_event_ids: tuple[UUID, ...]
    evidence_model_ids: tuple[UUID, ...]
    evidence_text: str
    explanation: str
    source_candidate_ids: tuple[str, ...]


@dataclass(frozen=True)
class BatchGroundingObligation:
    """A compact current-batch situation that must survive as a Model."""

    claim_text: str
    confidence: float
    evidence_event_ids: tuple[UUID, ...]
    evidence_model_ids: tuple[UUID, ...]
    grounding_tokens: tuple[str, ...]
    entity_tokens: tuple[str, ...]
    pressure_type: str
    explanation: str


@dataclass(frozen=True)
class _FrameRoleCandidate:
    """One existing Model that can fill a missing role in a relation frame."""

    model_id: UUID
    text: str
    candidate_ids: tuple[str, ...]
    suggested_edge_kinds: tuple[str, ...]
    confidence: float
    source: str


@dataclass(frozen=True)
class CompiledRelationshipCandidateRequest:
    system: str
    user: str
    candidates: tuple[dict[str, Any], ...]
    llm_candidate_ids: tuple[UUID, ...] = ()
    gated_decisions: tuple[RelationshipCandidateDecision, ...] = ()

    @property
    def requires_llm(self) -> bool:
        return bool(self.llm_candidate_ids)

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
        relation_claim_ops: list[RelationClaimOp] = []
        ontology_gap_ops: list[OntologyGapOp] = []
        trace_parts: list[str] = []
        accepted = 0
        rejected = 0
        blocked = 0
        review = 0
        ontology_gap = 0
        no_edge = 0

        all_decisions = [*self.gated_decisions, *decisions.decisions]
        for decision in all_decisions:
            candidate = by_id.get(decision.candidate_id)
            if candidate is None:
                blocked += 1
                trace_parts.append(
                    f"{decision.candidate_id}: ignored unknown candidate id"
                )
                continue
            if decision.decision in {"reject", "noise"}:
                rejected += 1
                trace_parts.append(
                    f"{decision.candidate_id}: rejected - {decision.reason}"
                )
                continue
            if decision.decision == "no_edge":
                op, block_reason = _relation_claim_op_from_candidate_decision(
                    candidate,
                    decision,
                    write_policy="no_edge",
                    status="rejected",
                )
                if op is None:
                    no_edge += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: no edge - {decision.reason}"
                    )
                    continue
                no_edge += 1
                relation_claim_ops.append(op)
                trace_parts.append(
                    f"{decision.candidate_id}: recorded no-edge relation - "
                    f"{decision.reason if decision.reason else block_reason}"
                )
                continue
            if decision.decision in {"candidate", "needs_review"}:
                if candidate.get("candidate_kind") == "edge_type":
                    review += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: edge_type candidate routed "
                        "to ontology workflow"
                    )
                    continue
                op, block_reason = _relation_claim_op_from_candidate_decision(
                    candidate,
                    decision,
                    write_policy="needs_review",
                    status="needs_review",
                )
                if op is None:
                    blocked += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: relation not promoted - "
                        f"{block_reason}"
                    )
                    continue
                review += 1
                relation_claim_ops.append(op)
                trace_parts.append(
                    f"{decision.candidate_id}: kept as needs_review relation - "
                    f"{decision.reason}"
                )
                continue
            if decision.decision == "ontology_gap":
                op, block_reason = _ontology_gap_op_from_candidate_decision(
                    candidate,
                    decision,
                )
                if op is None:
                    blocked += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: ontology gap not promoted - "
                        f"{block_reason}"
                    )
                    continue
                ontology_gap += 1
                ontology_gap_ops.append(op)
                trace_parts.append(
                    f"{decision.candidate_id}: promoted ontology gap "
                    f"{op.proposed_edge_kind}"
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
            relation_claim_ops.append(
                _relation_claim_op_from_edge_op(
                    edge_op,
                    candidate=candidate,
                    origin="compiled_relationship_candidate",
                )
            )
            trace_parts.append(
                f"{decision.candidate_id}: accepted relation {edge_op.edge_kind} "
                f"{edge_op.source_model_id}->{edge_op.target_model_id}"
            )

        if decisions.reasoning_trace:
            trace_parts.insert(0, decisions.reasoning_trace)
        trace_parts.append(
            "compiled_relationship_candidate_decisions="
            f"accepted:{accepted},rejected:{rejected},blocked:{blocked}"
            f",needs_review:{review},ontology_gap:{ontology_gap},no_edge:{no_edge}"
        )

        return RawDiff(
            trigger_ref=trigger_ref,
            tenant_id=trigger.tenant_id,
            claim_ops=[],
            relation_claim_ops=relation_claim_ops,
            edge_ops=edge_ops,
            ontology_gap_ops=ontology_gap_ops,
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
    relation_obligations: tuple[RelationObligation, ...] = ()
    relation_frame_obligations: tuple[RelationFrameObligation, ...] = ()
    grounding_obligations: tuple[BatchGroundingObligation, ...] = ()
    packet_obligation_gate: dict[str, Any] | None = None

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
        relation_claim_ops: list[RelationClaimOp] = []
        act_ops: list[ActOp] = []
        memory_lifecycle_ops: list[MemoryLifecycleOp] = []
        accepted = 0
        rejected = 0
        blocked = 0
        trace_parts: list[str] = []
        candidate_claim_placeholders: dict[str, UUID] = {}

        # Closed atomics are compiler-proven direct assertions. The LLM may
        # comment on them, but cannot turn them into a silent no-op.
        for candidate in self.candidates:
            if not str(candidate.get("entailed_claim_text") or "").strip():
                continue
            claim_op, lifecycle_op, placeholder, block_reason = (
                _closed_atomic_durable_fate(candidate, trigger=trigger)
            )
            candidate_id = str(candidate.get("candidate_id") or "closed_atomic")
            if block_reason:
                blocked += 1
                trace_parts.append(f"{candidate_id}: closed atomic blocked - {block_reason}")
                continue
            if lifecycle_op is not None:
                memory_lifecycle_ops.append(lifecycle_op)
                trace_parts.append(f"{candidate_id}: deterministic exact-bound confirm")
            elif claim_op is not None and placeholder is not None:
                claim_ops.append(claim_op)
                candidate_claim_placeholders[candidate_id] = placeholder
                trace_parts.append(f"{candidate_id}: deterministic atomic insert")
            accepted += 1

        for decision in decisions.decisions:
            candidate = by_id.get(decision.candidate_id)
            if candidate is None:
                blocked += 1
                trace_parts.append(
                    f"{decision.candidate_id}: ignored unknown candidate id"
                )
                continue
            if candidate.get("entailed_claim_text"):
                # Its durable fate was compiled above. Ignore an LLM accept,
                # rejection, or no-op so it cannot duplicate or erase it.
                continue
            if decision.decision != "accept" or decision.operation == "no_op":
                rejected += 1
                trace_parts.append(
                    f"{decision.candidate_id}: rejected - {decision.reason}"
                )
                continue
            claim_placeholder: UUID | None = None
            synthesis_candidate = candidate.get("candidate_kind") == "synthesis"
            if synthesis_candidate:
                obligation = next((
                    item for item in self.relation_obligations
                    if item.candidate_id == decision.candidate_id
                ), None)
                if obligation is None:
                    blocked += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: synthesis not promoted - "
                        "missing explicit relation obligation"
                    )
                    continue
                candidate = {
                    **candidate,
                    "explicit_relation_obligation": {
                        "edge_kind": obligation.edge_kind,
                        "source_model_id": (
                            str(obligation.source_model_id)
                            if obligation.source_model_id is not None else None
                        ),
                        "target_model_id": (
                            str(obligation.target_model_id)
                            if obligation.target_model_id is not None else None
                        ),
                        "evidence_event_ids": [str(x) for x in obligation.evidence_event_ids],
                        "evidence_model_ids": [str(x) for x in obligation.evidence_model_ids],
                        "evidence_text": obligation.evidence_text,
                    },
                }
                if decision.operation != "situation_and_edge":
                    blocked += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: synthesis not promoted - "
                        "coupled relation operation required"
                    )
                    continue
                if _supported_synthesis_relation(candidate, decision) is None:
                    blocked += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: synthesis not promoted - "
                        "invalid or stale closed-set relation binding"
                    )
                    continue
                claim_op, claim_placeholder, block_reason = (
                    _claim_op_from_batch_decision(
                        candidate,
                        decision,
                        trigger,
                        force_role="situation",
                    )
                )
                if claim_op is None or claim_placeholder is None:
                    blocked += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: synthesis not promoted - "
                        f"{block_reason}"
                    )
                    continue
                claim_ops.append(claim_op)
                candidate_claim_placeholders[decision.candidate_id] = claim_placeholder
            elif decision.operation == "memory_lifecycle":
                lifecycle_op, block_reason = _memory_lifecycle_op_from_batch_decision(
                    candidate,
                    decision,
                )
                if lifecycle_op is None:
                    blocked += 1
                    trace_parts.append(
                        f"{decision.candidate_id}: lifecycle not promoted - "
                        f"{block_reason}"
                    )
                    continue
                memory_lifecycle_ops.append(lifecycle_op)
                retired_relation_op = _retired_supported_relation_op(
                    candidate,
                    decision,
                    lifecycle_op=lifecycle_op,
                )
                if retired_relation_op is not None:
                    relation_claim_ops.append(retired_relation_op)
                accepted += 1
                trace_parts.append(
                    f"{decision.candidate_id}: accepted {decision.operation} "
                    f"confidence={decision.confidence:.2f}"
                )
                continue
            elif decision.operation == "claim_update":
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
                claim_op, claim_placeholder, block_reason = (
                    _claim_op_from_batch_decision(
                        candidate,
                        decision,
                        trigger,
                        force_role="situation",
                    )
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
                claim_op, claim_placeholder, block_reason = (
                    _claim_op_from_batch_decision(
                        candidate,
                        decision,
                        trigger,
                        force_role=(
                            "fact" if candidate.get("entailed_claim_text") else None
                        ),
                    )
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

            emitted_relation_for_decision = False
            if (
                not synthesis_candidate
                and decision.operation in {"edge", "claim_and_edge", "situation_and_edge"}
            ):
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
                relation_claim_ops.append(
                    _relation_claim_op_from_edge_op(
                        edge_op,
                        candidate=candidate,
                        origin="compiled_batch_memory_candidate",
                    )
                )
                emitted_relation_for_decision = True

            if not emitted_relation_for_decision:
                relation_op, block_reason = (
                    _relation_claim_op_from_relation_hinted_batch_decision(
                        candidate,
                        decision,
                        claim_placeholder=claim_placeholder,
                    )
                )
                if relation_op is not None:
                    relation_claim_ops.append(relation_op)
                elif block_reason:
                    trace_parts.append(
                        f"{decision.candidate_id}: relation not promoted - "
                        f"{block_reason}"
                    )

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

        # A composite binds immutable member-version evidence at admission.
        # Advancing one of those member heads later in this same diff makes the
        # just-created composite immediately disappear from
        # ``accepted_current_models``.  Preserve both durable fates by turning
        # compiler-owned exact confirms into standalone atomic inserts whenever
        # their target is also a member of a new synthesis.  This is deliberately
        # limited to compiler-owned confirms; user/LLM-authored lifecycle work
        # keeps its original semantics and must be coordinated explicitly.
        synthesis_member_ids = {
            member_id
            for claim_op in claim_ops
            if claim_op.op == "insert" and isinstance(claim_op.entry, dict)
            for proposition in [claim_op.entry.get("proposition")]
            if isinstance(proposition, dict)
            and proposition.get("synthesis_contract") is True
            for member_id in _uuid_values(proposition.get("member_model_ids"))
        }
        if synthesis_member_ids:
            retained_lifecycle_ops: list[MemoryLifecycleOp] = []
            for lifecycle_op in memory_lifecycle_ops:
                metadata = lifecycle_op.metadata or {}
                if (
                    lifecycle_op.model_id not in synthesis_member_ids
                    or metadata.get("source") != "closed_atomic_durable_fate"
                ):
                    retained_lifecycle_ops.append(lifecycle_op)
                    continue
                candidate_id = str(metadata.get("candidate_id") or "")
                closed_candidate = by_id.get(candidate_id)
                if closed_candidate is None:
                    raise ValueError(
                        "compiler-owned synthesis member confirm lost its candidate"
                    )
                claim_op, _ignored_lifecycle, placeholder, block_reason = (
                    _closed_atomic_durable_fate(
                        closed_candidate,
                        trigger=trigger,
                        force_insert=True,
                    )
                )
                if claim_op is None or placeholder is None:
                    raise ValueError(
                        "compiler-owned synthesis member confirm could not be "
                        f"preserved as an atomic insert: {block_reason}"
                    )
                claim_ops.append(claim_op)
                candidate_claim_placeholders[candidate_id] = placeholder
                trace_parts.append(
                    f"{candidate_id}: exact confirm emitted as atomic insert to "
                    "preserve a new synthesis member head"
                )
            memory_lifecycle_ops = retained_lifecycle_ops

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

        base_trace = "; ".join(part for part in trace_parts if part)
        base_diff = RawDiff(
            trigger_ref=trigger_ref,
            tenant_id=trigger.tenant_id,
            claim_ops=claim_ops,
            relation_claim_ops=relation_claim_ops,
            relation_frame_ops=[],
            edge_ops=edge_ops,
            ontology_gap_ops=[],
            act_ops=act_ops,
            resource_ops=[],
            new_predictions=[],
            memory_lifecycle_ops=memory_lifecycle_ops,
            reasoning_trace=base_trace,
        )
        if _relation_lifecycle_should_skip_packet_obligations(
            base_diff,
            packet=self.packet_obligation_gate or {},
        ):
            trace = "; ".join(
                part
                for part in (
                    base_trace,
                    "relation_lifecycle_kernel=packet_obligations_skipped:explicit_noop",
                )
                if part
            )
            return base_diff.model_copy(
                update={
                    "edge_ops": [],
                    "reasoning_trace": trace,
                }
            )

        grounding_ops, grounding_summary = grounding_claim_ops_from_obligations(
            self.grounding_obligations,
            trigger=trigger,
            existing_ops=claim_ops,
        )
        if grounding_ops:
            claim_ops.extend(grounding_ops)
        if grounding_summary:
            trace_parts.append(grounding_summary)
        obligation_ops, obligation_summary = relation_claim_ops_from_obligations(
            self.relation_obligations,
            decisions=decisions,
            claim_placeholders=candidate_claim_placeholders,
            existing_ops=relation_claim_ops,
            covered_edges=_frame_obligation_projection_keys(
                self.relation_frame_obligations
            ),
        )
        if obligation_ops:
            relation_claim_ops.extend(obligation_ops)
        if obligation_summary:
            trace_parts.append(obligation_summary)
        relation_claim_ops, suppressed_relation_reassertions = (
            _apply_authoritative_relation_retirements(relation_claim_ops)
        )
        if suppressed_relation_reassertions:
            trace_parts.append(
                "authoritative_relation_retirement_suppressed="
                f"{suppressed_relation_reassertions}"
            )
        frame_ops, frame_summary = relation_frame_ops_from_obligations(
            self.relation_frame_obligations,
            tenant_id=trigger.tenant_id,
        )
        if frame_ops:
            trace_parts.append(frame_summary)
        if not edge_ops and not relation_claim_ops and not frame_ops:
            no_edge_line = _batch_no_edge_accountability_line(self.candidates)
            if no_edge_line:
                trace_parts.append(no_edge_line)

        return RawDiff(
            trigger_ref=trigger_ref,
            tenant_id=trigger.tenant_id,
            claim_ops=claim_ops,
            relation_claim_ops=relation_claim_ops,
            relation_frame_ops=frame_ops,
            edge_ops=edge_ops,
            ontology_gap_ops=[],
            act_ops=act_ops,
            resource_ops=[],
            new_predictions=[],
            memory_lifecycle_ops=memory_lifecycle_ops,
            reasoning_trace="; ".join(part for part in trace_parts if part),
        )


def compiled_relationship_candidate_enabled() -> bool:
    return os.environ.get(
        "THINK_COMPILED_RELATIONSHIP_REASONING",
        "1",
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
        "1",
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
    candidates = _bind_synthesis_endpoint_versions(candidates, packet=packet)
    candidates = _bind_exact_closed_atomic_targets(
        candidates,
        models=bundle.models,
        tenant_id=trigger.tenant_id,
    )
    max_candidates = _env_int("THINK_COMPILED_BATCH_MEMORY_MAX_CANDIDATES", 6)
    atomic_count = sum(
        bool(str(candidate.get("entailed_claim_text") or "").strip())
        for candidate in candidates
    )
    if atomic_count:
        max_candidates = max(
            max_candidates,
            min(
                len(candidates),
                _env_int("THINK_COMPILED_BATCH_ATOMIC_MAX_CANDIDATES", 24)
                + _env_int("THINK_COMPILED_BATCH_SYNTHESIS_MAX_CANDIDATES", 4),
            ),
        )
    candidates = candidates[:max_candidates]
    if _compiled_batch_requires_open_writer_surface(packet, candidates):
        return None
    relation_obligations = relation_obligations_from_packet(packet, candidates)
    relation_frame_obligations = relation_frame_obligations_from_obligations(
        relation_obligations,
        candidates=candidates,
        packet=packet,
        model_cards=bundle.models,
    )
    grounding_obligations = grounding_obligations_from_packet(
        packet,
        candidates,
        model_cards=bundle.models,
    )
    system = (
        "You adjudicate closed-world memory-decision candidates for a Fyralis "
        "T1 event batch. You do not author RawDiff JSON. For each listed "
        "candidate, decide whether code should emit a durable memory mutation. "
        "The batch is one physical transport unit, never a semantic unit. "
        "Reason independently within each candidate's explicit semantic scope "
        "and member observations; never combine unrelated candidates. "
        "For candidates with entailed_claim_text, the compiler owns their "
        "durable insert-or-confirm fate, immutable wording, and exact evidence; "
        "your response cannot suppress or widen them. "
        "Prefer updates over duplicate inserts, situations for composite "
        "candidate-local understanding, and edges when selected graph context is "
        "decision-relevant. Reject/no-op only when uncertainty is decisive, "
        "evidence is merely background, or ids would need to be invented."
    )
    user = _build_batch_memory_decision_user_prompt(
        trigger,
        bundle,
        packet,
        candidates,
        relation_obligations=relation_obligations,
        relation_frame_obligations=relation_frame_obligations,
        grounding_obligations=grounding_obligations,
    )
    return CompiledBatchMemoryDecisionRequest(
        system=system,
        user=user,
        candidates=tuple(candidates),
        relation_obligations=relation_obligations,
        relation_frame_obligations=relation_frame_obligations,
        grounding_obligations=grounding_obligations,
        packet_obligation_gate={
            "signal_summary": packet.get("signal_summary"),
            "sufficiency_verdict": packet.get("sufficiency_verdict"),
            "important_unknowns": packet.get("important_unknowns"),
        },
    )


def _bind_synthesis_endpoint_versions(
    candidates: list[dict[str, Any]], *, packet: dict[str, Any],
) -> list[dict[str, Any]]:
    """Bind scoped synthesis members to the exact accepted heads hydrated upstream."""

    receipt = packet.get("synthesis_scope_hydration") or {}
    versions = receipt.get("endpoint_model_versions") or {}
    cards = receipt.get("endpoint_model_cards") or {}
    if not isinstance(versions, dict):
        versions = {}
    bound: list[dict[str, Any]] = []
    for candidate in candidates:
        if candidate.get("candidate_kind") != "synthesis":
            bound.append(candidate)
            continue
        member_ids = _dedupe_uuids(
            _uuid_values(candidate.get("evidence_model_ids"))
        )
        exact = {
            str(model_id): str(version_id)
            for model_id in member_ids
            if (version_id := _coerce_uuid(versions.get(str(model_id)))) is not None
        }
        enriched = dict(candidate)
        enriched["endpoint_model_versions"] = exact
        enriched["endpoint_model_cards"] = [
            cards[str(model_id)] for model_id in member_ids
            if isinstance(cards, dict) and isinstance(cards.get(str(model_id)), dict)
        ]
        card_scope_refs = {
            str((card.get("canonical_scope") or {}).get("ref") or "")
            for card in enriched["endpoint_model_cards"]
            if isinstance(card, dict)
        }
        card_scope_refs.discard("")
        candidate_ref = str(candidate.get("canonical_scope_ref") or "")
        if len(card_scope_refs) != 1 or (
            candidate_ref and candidate_ref not in card_scope_refs
        ):
            enriched["endpoint_model_versions"] = {}
            enriched["endpoint_model_cards"] = []
        elif not candidate_ref:
            enriched["canonical_scope_ref"] = next(iter(card_scope_refs))
        bound.append(enriched)
    return bound


def _bind_exact_closed_atomic_targets(
    candidates: list[dict[str, Any]],
    *,
    models: list[Any],
    tenant_id: UUID,
) -> list[dict[str, Any]]:
    """Bind closed atomics only to exact, same-scope accepted memory.

    This deliberately does not perform fuzzy semantic reconciliation. A weak
    match must remain a new atomic claim; only an exact natural-language
    identity plus exact scope is authority to confirm an existing Model.
    """

    bound: list[dict[str, Any]] = []
    for candidate in candidates:
        if not str(candidate.get("entailed_claim_text") or "").strip():
            bound.append(candidate)
            continue
        exact_targets = [
            model
            for model in models
            if _model_is_exact_closed_atomic_target(
                model,
                candidate=candidate,
                tenant_id=tenant_id,
            )
        ]
        if len(exact_targets) != 1:
            bound.append(candidate)
            continue
        row = dict(candidate)
        row["target_model_ids"] = [str(exact_targets[0].id)]
        row["allowed_operations"] = ["memory_lifecycle"]
        bound.append(row)
    return bound


def _model_is_exact_closed_atomic_target(
    model: Any,
    *,
    candidate: dict[str, Any],
    tenant_id: UUID,
) -> bool:
    if getattr(model, "tenant_id", None) != tenant_id:
        return False
    if str(getattr(model, "status", "")) != "active":
        return False
    if str(getattr(model, "abstraction_level", "atomic") or "atomic") != "atomic":
        return False
    expected = " ".join(
        str(candidate.get("entailed_claim_text") or "").casefold().split()
    )
    actual = " ".join(str(getattr(model, "natural", "")).casefold().split())
    if not expected or actual != expected:
        return False
    candidate_scopes = {
        " ".join(str(value).casefold().split())
        for value in candidate.get("semantic_scope") or ()
        if str(value).strip()
    }
    model_scopes = {
        " ".join(str(value).casefold().split())
        for entity in getattr(model, "scope_entities", ()) or ()
        if isinstance(entity, dict)
        for value in (
            entity.get("display_label"),
            entity.get("canonical_ref"),
            entity.get("label"),
        )
        if value
    }
    proposition = getattr(model, "proposition", None)
    if isinstance(proposition, dict):
        model_scopes.update(
            " ".join(str(value).casefold().split())
            for value in (
                proposition.get("scope_label"),
                proposition.get("scope_ref"),
                proposition.get("subject"),
            )
            if value
        )
    return bool(candidate_scopes) and bool(candidate_scopes & model_scopes)


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
    "capability_probe",
    "ontology_gap_ops",
    "ontology gap",
    "archive lifecycle",
    "evidence attachment",
    "question_policy",
    "question policy",
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
    *,
    relation_obligations: tuple[RelationObligation, ...] = (),
    relation_frame_obligations: tuple[RelationFrameObligation, ...] = (),
    grounding_obligations: tuple[BatchGroundingObligation, ...] = (),
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
        "- claim_and_edge: emit one atomic claim, then one relation claim from that new claim to target_model_id",
        "- situation_and_edge: emit one situation, then one relation claim from it to target_model_id",
        "- claim_and_act: emit one atomic claim, then one act transition for act_target_id",
        "- edge: emit one relation claim only between source_model_id and target_model_id",
        "- act: emit one act transition only",
        "- memory_lifecycle: reconcile an existing model_id with new evidence",
        "- no_op: no world-model mutation",
        "Operational edge kinds to prefer when evidenced: blocks, explains, weakens, contradicts, early_warning_for, contributes_to_resolution, enables, supports.",
        "Similarity edge kinds are weak/review-only: same_issue_as, analogous_to, co_occurs_with. Use them only when no operational relation is true.",
        "Allowed edge kinds: " + ", ".join(sorted(EDGE_REGISTRY.keys())),
        "Do not emit resource writes or ontology gaps here.",
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

    uncertainty_signals = packet.get("uncertainty_signals")
    if isinstance(uncertainty_signals, list) and uncertainty_signals:
        lines.extend([
            "<uncertainty_signals>",
            "These signals are accounted outside accepted truth. Do not emit a "
            "claim or decision for them in this closed-world pass.",
        ])
        for signal in uncertainty_signals[:24]:
            if isinstance(signal, dict):
                lines.append("  - " + _trunc(_jsonish(signal), 700))
        lines.append("</uncertainty_signals>")

    lines.append("<memory_decision_candidates>")
    for candidate in candidates:
        lines.extend(_batch_candidate_lines(candidate))
    lines.append("</memory_decision_candidates>")

    evidence_lines = _batch_candidate_evidence_lines(packet, candidates)
    if evidence_lines:
        lines.append("<candidate_evidence>")
        lines.extend(evidence_lines)
        lines.append("</candidate_evidence>")

    if relation_obligations:
        lines.append("<mandatory_relation_obligations>")
        for obligation in relation_obligations:
            lines.append(
                "  - "
                + _trunc(
                    _jsonish(
                        {
                            "candidate_id": obligation.candidate_id,
                            "edge_kind": obligation.edge_kind,
                            "confidence": round(obligation.confidence, 3),
                            "source_model_id": (
                                str(obligation.source_model_id)
                                if obligation.source_model_id is not None
                                else None
                            ),
                            "target_model_id": (
                                str(obligation.target_model_id)
                                if obligation.target_model_id is not None
                                else None
                            ),
                            "matched_markers": list(obligation.matched_markers),
                            "evidence": obligation.evidence_text,
                        }
                    ),
                    900,
                )
            )
        lines.append("</mandatory_relation_obligations>")

    if relation_frame_obligations:
        lines.append("<mandatory_relation_frame_obligations>")
        for obligation in relation_frame_obligations:
            lines.append(
                "  - "
                + _trunc(
                    _jsonish(
                        {
                            "relation_kind": obligation.relation_kind,
                            "confidence": round(obligation.confidence, 3),
                            "participants": [
                                {
                                    "role": participant.role,
                                    "model_id": str(participant.model_id),
                                    "binding_confidence": round(
                                        participant.binding_confidence,
                                        3,
                                    ),
                                }
                                for participant in obligation.participants
                            ],
                            "source_candidate_ids": list(
                                obligation.source_candidate_ids
                            ),
                            "evidence": obligation.evidence_text,
                        }
                    ),
                    1200,
                )
            )
        lines.append("</mandatory_relation_frame_obligations>")

    if grounding_obligations:
        lines.append("<mandatory_grounding_obligations>")
        for obligation in grounding_obligations:
            lines.append(
                "  - "
                + _trunc(
                    _jsonish(
                        {
                            "claim_text": obligation.claim_text,
                            "confidence": round(obligation.confidence, 3),
                            "grounding_tokens": list(obligation.grounding_tokens),
                            "entity_tokens": list(obligation.entity_tokens),
                            "pressure_type": obligation.pressure_type,
                        }
                    ),
                    1000,
                )
            )
        lines.append("</mandatory_grounding_obligations>")

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
            "Use memory_lifecycle when the candidate tests, confirms, falsifies, revises, archives, or supersedes an existing model_id; provide lifecycle_action.",
            "For claim_update or memory_lifecycle, claim_local_evidence_event_ids must contain only candidate observation UUIDs whose text directly supports the exact target Model proposition. Omit siblings, background, uncertainty, and distractors; an empty list means review without attaching new claim evidence.",
            "Use situation when one candidate-local episode exposes a composite condition across its member signals or selected Models; provide 2-8 situation_member_model_ids when available.",
            "For candidate_kind=synthesis, accept only with situation or situation_and_edge and author a scope-level claim_text; an edge may accompany but never replace the synthesis Model.",
            "Use claim_role=concern for blockers, risks, waiting states, churn, trust, or negative pressure.",
            "Use claim_role=fact for neutral observed progress or state.",
            "Use claim_role=pattern only for repeated behavior directly supported in candidate_evidence.",
            "Use claim_role=situation only with operation=situation or situation_and_edge.",
            "For relation claims, target/source ids must be candidate target/evidence Model ids; if unsure, use suggested_edge_kinds as the first-pass menu.",
            "For synthesis relations, source_model_id is the prerequisite, driver, or earlier condition and target_model_id is the blocked, affected, or later outcome. Choose both only from endpoint_model_cards. The compiler owns relation kind, mechanism, and evidence; you may bind endpoints but may not alter those semantics.",
            "Mandatory grounding obligations are code-perceived current-batch facts; code may insert one compact situation Model if accepted writes only update old Models or omit concrete batch anchors.",
            "Mandatory relation obligations are already perceived from the batch evidence; code may persist them when the batch has durable write intent, but an explicit background/duplicate no_op vetoes obligation persistence.",
            "Mandatory relation frame obligations are code-perceived N-ary relations; code may persist and project them only when the batch has durable write intent.",
            "Prefer blocks for blockers/dependencies, early_warning_for for account risk, contributes_to_resolution for mitigating evidence, explains for causal interpretation, weakens for contradiction, and supports for evidence support.",
            "Do not emit same_issue_as or analogous_to merely to acknowledge retrieved context. If the relation is only similarity, choose no_op unless storing a review-only similarity edge is decision-relevant.",
            "If proposed_text or candidate_evidence says blocked by, waiting on, prerequisite, dependency, critical path, or cannot proceed, test blocks before any similarity edge.",
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
        "candidate_kind",
        "allowed_operations",
        "op_family",
        "confidence",
        "proposed_text",
        "entailed_claim_text",
        "target_model_ids",
        "target_act_ids",
        "evidence_model_ids",
        "source_observation_ids",
        "member_observation_ids",
        "relation_evidence_observation_ids",
        "relation_observation_evidence",
        "endpoint_model_cards",
        "semantic_scope",
        "canonical_scope_ref",
        "observation_evidence",
        "supporting_evidence_ids",
        "counterevidence_ids",
        "uncertainty_slots",
        "retrieval_targets",
        "suggested_edge_kinds",
        "write_preconditions",
        "answer_summary",
        "reason",
    )
    lines = ["  <candidate>"]
    for key in keys:
        if key == "source_observation_ids" and candidate.get(
            "member_observation_ids"
        ):
            continue
        value = candidate.get(key)
        if value in (None, [], {}, ""):
            continue
        if key == "endpoint_model_cards" and isinstance(value, list):
            lines.append("    endpoint_model_cards:")
            for card in value[:8]:
                lines.append(f"      - {_endpoint_model_card_json(card)}")
            continue
        lines.append(f"    {key}: {_trunc(_jsonish(value), 900)}")
    lines.append("  </candidate>")
    return lines


def _endpoint_model_card_json(card: Any) -> str:
    """Render one bounded endpoint card without truncating serialized JSON.

    Endpoint coordinates and scope remain exact.  Potentially large semantic
    fields are bounded before serialization so the resulting line is always a
    complete JSON object that request-only decision adapters can parse.
    """

    if not isinstance(card, dict):
        return _jsonish(card)
    compact = {
        key: card[key]
        for key in ("id", "version_id", "canonical_scope")
        if card.get(key) not in (None, "", {}, [])
    }
    natural = str(card.get("natural") or "").strip()
    if natural:
        compact["natural"] = _trunc(natural, 360)
    proposition = card.get("proposition")
    if proposition not in (None, "", {}, []):
        if isinstance(proposition, dict):
            assertion = next(
                (
                    str(proposition[key]).strip()
                    for key in ("assertion", "statement", "claim", "text", "summary")
                    if proposition.get(key) not in (None, "", {}, [])
                ),
                "",
            )
            semantic = assertion or _jsonish(proposition)
        else:
            semantic = str(proposition)
        compact["proposition"] = _trunc(semantic, 420)
    return _jsonish(compact)


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


_GENERIC_RELATION_KINDS = {
    "supports",
    "same_issue_as",
    "analogous_to",
    "co_occurs_with",
    "alternative_to",
}

_RELATION_OBLIGATION_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "blocks",
        (
            "blocked by",
            "blocks ",
            "blocker",
            "blocking",
            "waiting on",
            "waiting status",
            "depends on",
            "dependency",
            "prerequisite",
            "cannot proceed",
            "can't proceed",
            "gates ",
            "gating",
            "critical path",
            "links ",
            "linked ",
            "connects ",
            "connected ",
            "ties ",
            "associated ",
        ),
    ),
    (
        "early_warning_for",
        (
            "early warning",
            "warning for",
            "risk for",
            "risk signal",
            "churn risk",
            "renewal risk",
            "forecast risk",
            "health risk",
            "usage is down",
            "usage decay",
        ),
    ),
    (
        "contributes_to_resolution",
        (
            "contributes to resolution",
            "helps resolve",
            "helps settle",
            "mitigate",
            "mitigates",
            "mitigating",
            "remediate",
            "remediates",
            "resolved by",
            "unblock approval",
            "unblock",
            "unblocks",
            "unlock approval",
        ),
    ),
    (
        "weakens",
        (
            "weakens",
            "contradict",
            "contradicted",
            "counterevidence",
            "undermines",
            "despite",
            "even though",
            "not the blocker",
            "not blocking",
            "stale",
            "opacity",
        ),
    ),
    (
        "explains",
        (
            "explains",
            "explain why",
            "helps explain",
            "because",
            "due to",
            "root cause",
            "mechanism",
            "reason why",
            "why ",
        ),
    ),
    (
        "enables",
        (
            "enables",
            "enabled by",
            "makes possible",
            "make possible",
            "clears the path",
        ),
    ),
    (
        "supports",
        (
            "supports",
            "reinforces",
            "confirms",
            "adds evidence",
            "evidence for",
        ),
    ),
)


_GROUNDING_OPERATIONAL_TOKENS = {
    "approval",
    "audit",
    "capacity",
    "churn",
    "connector",
    "coverage",
    "deadline",
    "decay",
    "dependency",
    "freshness",
    "handoff",
    "implementation",
    "incident",
    "integration",
    "onboarding",
    "packet",
    "procurement",
    "reliability",
    "renewal",
    "repeat",
    "risk",
    "security",
    "slip",
    "throughput",
    "usage",
    "workflow",
}


def grounding_obligations_from_packet(
    packet: dict[str, Any],
    candidates: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    model_cards: list[Any] | tuple[Any, ...] = (),
) -> tuple[BatchGroundingObligation, ...]:
    """Compile one durable current-batch grounding Model when anchors are at risk.

    This is not an extraction system. It is a bounded anti-collapse backstop:
    when the packet exposes concrete current-batch anchors but downstream
    reasoning might only update older retrieved Models, preserve the batch as
    one compact situation before relation work projects away the specifics.
    """

    if not candidates:
        return ()
    text = _batch_grounding_text(packet, candidates)
    if len(text) < 24:
        return ()
    entity_tokens = _grounding_entity_tokens(text)
    grounding_tokens = _grounding_tokens(text)
    operational_tokens = tuple(
        token for token in grounding_tokens if token in _GROUNDING_OPERATIONAL_TOKENS
    )
    if not entity_tokens:
        return ()
    if len(operational_tokens) < 2:
        return ()
    evidence_event_ids = tuple(
        _dedupe_uuids(
            [
                event_id
                for candidate in candidates
                for event_id in _uuid_values(candidate.get("source_observation_ids"))
            ]
        )
    )
    evidence_model_ids = tuple(
        _dedupe_uuids(
            [
                model_id
                for candidate in candidates
                for model_id in _candidate_model_ids(candidate)
            ]
            + [
                model_id
                for model in model_cards
                if (model_id := _coerce_uuid(getattr(model, "id", None))) is not None
            ][:8]
        )
    )
    if not evidence_event_ids and not evidence_model_ids:
        return ()
    claim_text = _grounding_claim_text(
        text,
        entity_tokens=entity_tokens,
        operational_tokens=operational_tokens,
    )
    return (
        BatchGroundingObligation(
            claim_text=claim_text,
            confidence=0.64,
            evidence_event_ids=evidence_event_ids,
            evidence_model_ids=evidence_model_ids[:8],
            grounding_tokens=grounding_tokens,
            entity_tokens=entity_tokens,
            pressure_type=_pressure_type(None, claim_text),
            explanation=(
                "Mandatory batch grounding: preserve concrete current-batch "
                "anchors before updates or edges collapse them into older Models."
            ),
        ),
    )


def grounding_claim_ops_from_obligations(
    obligations: tuple[BatchGroundingObligation, ...],
    *,
    trigger: TriggerContext,
    existing_ops: list[ClaimOp] | tuple[ClaimOp, ...] = (),
) -> tuple[list[ClaimOp], str | None]:
    if not obligations:
        return [], None
    emitted: list[ClaimOp] = []
    deduped = 0
    for obligation in obligations:
        if _grounding_obligation_already_preserved(
            obligation,
            [*existing_ops, *emitted],
        ):
            deduped += 1
            continue
        emitted.append(_grounding_claim_op_from_obligation(obligation, trigger=trigger))
    summary = (
        "mandatory_grounding_obligations="
        f"perceived:{len(obligations)},emitted:{len(emitted)},deduped:{deduped}"
    )
    return emitted, summary


def _grounding_claim_op_from_obligation(
    obligation: BatchGroundingObligation,
    *,
    trigger: TriggerContext,
) -> ClaimOp:
    born_event = uuid7()
    text = _trunc(obligation.claim_text, 1000)
    proposition = {
        "kind": "belief",
        "claim_role": "situation",
        "abstraction_level": "composite",
        "situation": _trunc(text, 180),
        "summary": text,
        "member_model_ids": [str(model_id) for model_id in obligation.evidence_model_ids],
        "relationship_summary": _trunc(obligation.explanation, 360),
        "status": "forming",
        "pressure_type": obligation.pressure_type,
        "shared_mechanism": _trunc(text, 280),
        "judgment_change": _trunc(text, 280),
        "affected_decisions": [],
        "affected_customers": list(obligation.entity_tokens),
        "affected_teams": [],
        "evidence_event_ids": [
            str(event_id) for event_id in obligation.evidence_event_ids
        ],
        "grounding_tokens": list(obligation.grounding_tokens),
        "compiled_grounding_obligation": True,
    }
    return ClaimOp(
        op="insert",
        entry={
            "tenant_id": str(trigger.tenant_id),
            "born_from_event_id": str(born_event),
            "proposition": proposition,
            "natural": text,
            "confidence": obligation.confidence,
            "confidence_at_assertion": obligation.confidence,
            "scope_actors": [str(actor_id) for actor_id in (trigger.scope_actors or [])],
            "scope_entities": _scope_entities(trigger),
            "scope_temporal": {},
            "falsifier": None,
        },
    )


def _grounding_obligation_already_preserved(
    obligation: BatchGroundingObligation,
    ops: list[ClaimOp] | tuple[ClaimOp, ...],
) -> bool:
    required = set(obligation.entity_tokens) | set(
        token
        for token in obligation.grounding_tokens
        if token in _GROUNDING_OPERATIONAL_TOKENS
    )
    if not required:
        return True
    for op in ops:
        if op.op != "insert" or not op.entry:
            continue
        text = _jsonish(
            {
                "natural": op.entry.get("natural"),
                "proposition": op.entry.get("proposition"),
            }
        ).lower()
        tokens = _grounding_tokens(text)
        if set(obligation.entity_tokens) - set(tokens):
            continue
        covered_operational = set(tokens) & _GROUNDING_OPERATIONAL_TOKENS
        needed_operational = set(required) & _GROUNDING_OPERATIONAL_TOKENS
        if len(covered_operational & needed_operational) >= min(
            2,
            len(needed_operational),
        ):
            return True
    return False


def _batch_grounding_text(
    packet: dict[str, Any],
    candidates: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> str:
    parts: list[str] = []
    for value in (
        packet.get("signal_summary"),
        packet.get("important_unknowns"),
    ):
        if value not in (None, "", [], {}):
            parts.append(_jsonish(value))
    for candidate in candidates:
        for value in (
            candidate.get("proposed_text"),
            candidate.get("answer_summary"),
            candidate.get("reason"),
            candidate.get("retrieval_targets"),
        ):
            if value not in (None, "", [], {}):
                parts.append(_jsonish(value))
    tiers = packet.get("tiers") if isinstance(packet.get("tiers"), dict) else {}
    for group in tiers.get("supporting_evidence_groups") or []:
        if isinstance(group, dict):
            summary = group.get("summary") or group.get("claim_supported")
            if summary:
                parts.append(str(summary))
    for item in tiers.get("decisive_evidence") or []:
        if isinstance(item, dict) and item.get("summary"):
            parts.append(str(item["summary"]))
    return _trunc(" | ".join(parts), 2400)


def _grounding_claim_text(
    text: str,
    *,
    entity_tokens: tuple[str, ...],
    operational_tokens: tuple[str, ...],
) -> str:
    summary = " ".join(str(text or "").split())
    if len(summary) <= 520:
        return summary
    anchors = ", ".join([*entity_tokens[:2], *operational_tokens[:5]])
    return _trunc(f"{summary[:460].rstrip()}... Anchors: {anchors}.", 620)


def _grounding_entity_tokens(text: str) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in _grounding_raw_words(text):
        token = _normalize_grounding_token(raw)
        if not token or token in seen:
            continue
        if _is_grounding_entity_word(raw, token):
            seen.add(token)
            out.append(token)
    return tuple(out[:4])


def _grounding_tokens(text: str) -> tuple[str, ...]:
    tokens = sorted(_frame_tokens(text))
    return tuple(token for token in tokens if token not in _GROUNDING_STOPWORDS)[:32]


def _grounding_raw_words(text: str) -> list[str]:
    words: list[str] = []
    current: list[str] = []
    for char in str(text or ""):
        if char.isalnum() or char in {"-", "_"}:
            current.append(char)
        elif current:
            words.append("".join(current).strip("-_"))
            current = []
    if current:
        words.append("".join(current).strip("-_"))
    return [word for word in words if word]


def _normalize_grounding_token(raw: str) -> str:
    return "".join(char.lower() for char in raw if char.isalnum())


def _is_grounding_entity_word(raw: str, token: str) -> bool:
    if len(token) < 5 or token in _GROUNDING_STOPWORDS:
        return False
    has_inner_upper = any(char.isupper() for char in raw[1:])
    has_lower = any(char.islower() for char in raw)
    return has_inner_upper and has_lower


_GROUNDING_STOPWORDS = {
    "accepted",
    "answer",
    "batch",
    "candidate",
    "confidence",
    "concrete",
    "customer",
    "durable",
    "evidence",
    "existing",
    "hypothesis",
    "local",
    "memory",
    "model",
    "models",
    "question",
    "reason",
    "relation",
    "selected",
    "signal",
    "signals",
    "status",
    "supporting",
    "target",
    "uncertainty",
}


def relation_obligations_from_packet(
    packet: dict[str, Any],
    candidates: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> tuple[RelationObligation, ...]:
    """Compile relation-bearing batch facts before the model can hide them."""

    if not candidates:
        return ()
    evidence_by_id = _packet_evidence_summaries(packet)
    obligations: list[RelationObligation] = []
    seen: set[tuple[str, str, UUID | None, UUID | None]] = set()
    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_id") or "").strip()
        if not candidate_id:
            continue
        if not _candidate_model_ids(candidate):
            continue
        relation_clauses = _relation_obligation_clauses(
            packet,
            candidate,
            evidence_by_id=evidence_by_id,
        )
        edge_kind, markers, evidence_text = _infer_relation_obligation_edge_kind(
            candidate,
            relation_clauses,
        )
        if edge_kind is None:
            continue
        source_model_id, target_model_id = _relation_obligation_endpoints(
            candidate,
            edge_kind=edge_kind,
        )
        confidence = _relation_obligation_confidence(
            candidate,
            edge_kind=edge_kind,
            markers=markers,
        )
        evidence_event_ids = tuple(_dedupe_uuids(_uuid_values(
            candidate.get("relation_evidence_observation_ids")
            or candidate.get("source_observation_ids")
        )))
        evidence_model_ids = tuple(_candidate_model_ids(candidate))
        key = (candidate_id, edge_kind, source_model_id, target_model_id)
        if key in seen:
            continue
        seen.add(key)
        obligations.append(
            RelationObligation(
                candidate_id=candidate_id,
                edge_kind=edge_kind,
                confidence=confidence,
                source_model_id=source_model_id,
                target_model_id=target_model_id,
                evidence_event_ids=evidence_event_ids,
                evidence_model_ids=evidence_model_ids,
                evidence_text=_trunc(evidence_text, 1000),
                explanation=_trunc(
                    "Mandatory relation perception from candidate-local evidence: "
                    + evidence_text,
                    1000,
                ),
                matched_markers=markers,
            )
        )
    return tuple(obligations)


def relation_claim_ops_from_obligations(
    obligations: tuple[RelationObligation, ...],
    *,
    decisions: BatchMemoryDecisionSet | None = None,
    claim_placeholders: dict[str, UUID] | None = None,
    existing_ops: list[RelationClaimOp] | tuple[RelationClaimOp, ...] = (),
    covered_edges: set[tuple[str, UUID, UUID]] | None = None,
) -> tuple[list[RelationClaimOp], str | None]:
    if not obligations:
        return [], None

    covered_edges = covered_edges or set()
    decisions_by_id = {
        decision.candidate_id: decision for decision in (decisions.decisions if decisions else [])
    }
    claim_placeholders = claim_placeholders or {}
    existing_keys = _relation_claim_existing_keys(existing_ops)
    emitted: list[RelationClaimOp] = []
    blocked = 0
    accepted_edge_intent = 0
    review_intent = 0
    for obligation in obligations:
        decision = decisions_by_id.get(obligation.candidate_id)
        if any(
            str((existing.metadata or {}).get("memory_decision_candidate_id") or "")
            == obligation.candidate_id
            and existing.edge_kind == obligation.edge_kind
            and set(existing.evidence_event_ids) == set(obligation.evidence_event_ids)
            for existing in existing_ops
        ):
            blocked += 1
            continue
        if (
            decision is not None
            and decision.operation == "situation_and_edge"
            and obligation.candidate_id not in claim_placeholders
        ):
            # A governed synthesis relation cannot outlive a failed composite
            # compilation as an unbound pre-truth row.
            blocked += 1
            continue
        op = _relation_claim_op_from_obligation(
            obligation,
            decision=decision,
            claim_placeholder=claim_placeholders.get(obligation.candidate_id),
        )
        if (
            op.source_model_id is not None
            and op.target_model_id is not None
            and (op.edge_kind, op.source_model_id, op.target_model_id)
            in covered_edges
        ):
            blocked += 1
            continue
        key = _relation_claim_dedup_key(op)
        if key in existing_keys:
            blocked += 1
            continue
        existing_keys.add(key)
        emitted.append(op)
        if op.write_policy == "accepted_edge":
            accepted_edge_intent += 1
        else:
            review_intent += 1

    summary = (
        "mandatory_relation_obligations="
        f"perceived:{len(obligations)},emitted:{len(emitted)},"
        f"accepted_edge_intent:{accepted_edge_intent},"
        f"review_intent:{review_intent},deduped:{blocked}"
    )
    return emitted, summary


def _apply_authoritative_relation_retirements(
    ops: list[RelationClaimOp],
) -> tuple[list[RelationClaimOp], int]:
    """Let an explicit correction retirement dominate same-diff inference."""

    retirement_keys = {
        key
        for op in ops
        if op.status == "retired"
        and op.write_policy == "no_edge"
        and (op.metadata or {}).get("relation_claim_origin")
        == "composite_correction_retirement"
        if (key := _effective_relation_identity(op)) is not None
    }
    if not retirement_keys:
        return ops, 0
    retained: list[RelationClaimOp] = []
    suppressed = 0
    for op in ops:
        key = _effective_relation_identity(op)
        authoritative_retirement = bool(
            op.status == "retired"
            and op.write_policy == "no_edge"
            and (op.metadata or {}).get("relation_claim_origin")
            == "composite_correction_retirement"
        )
        if key in retirement_keys and not authoritative_retirement:
            suppressed += 1
            continue
        retained.append(op)
    return retained, suppressed


def _effective_relation_identity(
    op: RelationClaimOp,
) -> tuple[str, UUID, UUID] | None:
    if op.source_model_id is None or op.target_model_id is None:
        return None
    kind = {
        "dependency_constraint": "blocks",
        "enablement": "enables",
        "causal_influence": "causes",
        "predictive_indicator": "predicts",
    }.get(op.edge_kind, op.edge_kind)
    source, target = op.source_model_id, op.target_model_id
    if op.direction == "target_to_source":
        source, target = target, source
    return kind, source, target


def relation_frame_obligations_from_obligations(
    obligations: tuple[RelationObligation, ...],
    *,
    candidates: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    packet: dict[str, Any] | None = None,
    model_cards: list[Any] | tuple[Any, ...] = (),
) -> tuple[RelationFrameObligation, ...]:
    """Compile role-bound N-ary frames from already-perceived pair relations."""

    bound = [
        obligation
        for obligation in obligations
        if obligation.source_model_id is not None
        and obligation.target_model_id is not None
    ]
    blockers = [
        obligation
        for obligation in bound
        if obligation.edge_kind == "blocks"
        and obligation.source_model_id != obligation.target_model_id
    ]
    if not blockers:
        return ()

    candidates_by_id = {
        str(candidate.get("candidate_id")): candidate
        for candidate in candidates
        if candidate.get("candidate_id")
    }
    evidence_by_id = _packet_evidence_summaries(packet or {})
    role_candidates = _frame_role_candidates(
        candidates,
        model_cards=model_cards,
        evidence_by_id=evidence_by_id,
    )
    model_text_by_id = _frame_model_text_by_id(model_cards)
    packet_context_text = _frame_packet_text(packet or {})
    frames: list[RelationFrameObligation] = []
    seen_keys: set[tuple[tuple[str, str], ...]] = set()
    for blocker_obligation in blockers:
        blocker = blocker_obligation.source_model_id
        blocked_work = blocker_obligation.target_model_id
        if blocker is None or blocked_work is None:
            continue
        cluster = _relation_frame_cluster(blocker_obligation, bound)
        roles: dict[str, UUID] = {
            "blocker": blocker,
            "blocked_work": blocked_work,
        }
        if not _frame_endpoints_anchor_current_evidence(
            blocker_obligation,
            blocker=blocker,
            blocked_work=blocked_work,
            packet_context_text=packet_context_text,
            model_text_by_id=model_text_by_id,
        ):
            continue
        downstream_risk = _frame_downstream_risk(
            cluster,
            blocker=blocker,
            blocked_work=blocked_work,
        )
        if downstream_risk is None:
            downstream_risk = _frame_complete_role_from_candidates(
                "downstream_risk",
                role_candidates,
                cluster=cluster,
                blocker=blocker,
                blocked_work=blocked_work,
                used_models=set(roles.values()),
                packet_context_text=packet_context_text,
            )
        if downstream_risk is not None:
            roles["downstream_risk"] = downstream_risk
        possible_resolution = _frame_possible_resolution(
            cluster,
            blocker=blocker,
            blocked_work=blocked_work,
        )
        if possible_resolution is None:
            possible_resolution = _frame_complete_role_from_candidates(
                "possible_resolution",
                role_candidates,
                cluster=cluster,
                blocker=blocker,
                blocked_work=blocked_work,
                used_models=set(roles.values()),
                packet_context_text=packet_context_text,
            )
        if possible_resolution is not None:
            roles["possible_resolution"] = possible_resolution
        owner = _frame_owner_candidate(
            cluster,
            candidates_by_id=candidates_by_id,
            used_models=set(roles.values()),
        )
        if owner is not None:
            roles["owner"] = owner

        distinct_models = set(roles.values())
        if len(distinct_models) < 3:
            continue
        if not ({"downstream_risk", "possible_resolution", "owner"} & set(roles)):
            continue

        participants = tuple(
            RelationFrameParticipantObligation(
                role=role,
                model_id=model_id,
                binding_confidence=_frame_role_binding_confidence(role, cluster),
            )
            for role, model_id in sorted(roles.items())
        )
        key = tuple((participant.role, str(participant.model_id)) for participant in participants)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        evidence_event_ids = _dedupe_uuids(
            [
                event_id
                for obligation in cluster
                for event_id in obligation.evidence_event_ids
            ]
        )
        evidence_model_ids = _dedupe_uuids(
            [
                model_id
                for obligation in cluster
                for model_id in obligation.evidence_model_ids
            ]
            + list(distinct_models)
        )
        evidence_text = _trunc(
            " | ".join(
                obligation.evidence_text
                for obligation in cluster
                if obligation.evidence_text
            ),
            1000,
        )
        confidence = min(
            0.92,
            max(0.7, sum(obligation.confidence for obligation in cluster) / len(cluster)),
        )
        frames.append(
            RelationFrameObligation(
                relation_kind="blocked_workstream",
                confidence=confidence,
                participants=participants,
                evidence_event_ids=tuple(evidence_event_ids),
                evidence_model_ids=tuple(evidence_model_ids),
                evidence_text=evidence_text,
                explanation=_trunc(
                    "Compiled frame obligation: blocker, blocked work, and "
                    "downstream/resolution roles are jointly present in the "
                    "same relation-bearing evidence cluster.",
                    1000,
                ),
                source_candidate_ids=tuple(
                    sorted({obligation.candidate_id for obligation in cluster})
                ),
            )
        )
    return tuple(frames)


def relation_frame_ops_from_obligations(
    obligations: tuple[RelationFrameObligation, ...],
    *,
    tenant_id: UUID,
    existing_ops: list[RelationFrameOp] | tuple[RelationFrameOp, ...] = (),
) -> tuple[list[RelationFrameOp], str | None]:
    if not obligations:
        return [], None

    existing_keys = {
        _relation_frame_op_key(op)
        for op in existing_ops
    }
    emitted: list[RelationFrameOp] = []
    deduped = 0
    for obligation in obligations:
        op = _relation_frame_op_from_obligation(
            obligation,
            tenant_id=tenant_id,
        )
        key = _relation_frame_op_key(op)
        if key in existing_keys:
            deduped += 1
            continue
        existing_keys.add(key)
        emitted.append(op)
    summary = (
        "mandatory_relation_frame_obligations="
        f"perceived:{len(obligations)},emitted:{len(emitted)},deduped:{deduped}"
    )
    return emitted, summary


def _relation_frame_cluster(
    seed: RelationObligation,
    obligations: list[RelationObligation],
) -> tuple[RelationObligation, ...]:
    seed_models = {
        model_id
        for model_id in (seed.source_model_id, seed.target_model_id)
        if model_id is not None
    } | set(seed.evidence_model_ids)
    cluster: list[RelationObligation] = []
    for obligation in obligations:
        models = {
            model_id
            for model_id in (obligation.source_model_id, obligation.target_model_id)
            if model_id is not None
        } | set(obligation.evidence_model_ids)
        if seed_models & models:
            cluster.append(obligation)
    return tuple(cluster)


def _frame_downstream_risk(
    cluster: tuple[RelationObligation, ...],
    *,
    blocker: UUID,
    blocked_work: UUID,
) -> UUID | None:
    warning_obligations = [
        obligation
        for obligation in cluster
        if obligation.edge_kind == "early_warning_for"
    ]
    for obligation in warning_obligations:
        if obligation.source_model_id == blocked_work:
            candidate = obligation.target_model_id
            if candidate not in {None, blocker, blocked_work}:
                return candidate
    for obligation in warning_obligations:
        for candidate in (obligation.target_model_id, obligation.source_model_id):
            if candidate not in {None, blocker, blocked_work}:
                return candidate
    return None


def _frame_possible_resolution(
    cluster: tuple[RelationObligation, ...],
    *,
    blocker: UUID,
    blocked_work: UUID,
) -> UUID | None:
    resolution_obligations = [
        obligation
        for obligation in cluster
        if obligation.edge_kind == "contributes_to_resolution"
    ]
    for obligation in resolution_obligations:
        if obligation.target_model_id == blocker:
            candidate = obligation.source_model_id
            if candidate not in {None, blocker, blocked_work}:
                return candidate
    for obligation in resolution_obligations:
        for candidate in (obligation.source_model_id, obligation.target_model_id):
            if candidate not in {None, blocker, blocked_work}:
                return candidate
    return None


_FRAME_DOWNSTREAM_RISK_MARKERS = (
    "downstream risk",
    "launch risk",
    "renewal risk",
    "churn risk",
    "forecast risk",
    "implementation risk",
    "risk",
    "may slip",
    "might slip",
    "could slip",
    "slip",
    "delay",
    "delayed",
    "deadline",
    "threatens",
    "threaten",
    "exposure",
    "pressure",
)

_FRAME_RESOLUTION_MARKERS = (
    "possible resolution",
    "resolution",
    "resolve",
    "resolves",
    "resolved",
    "helps resolve",
    "remediate",
    "remediation",
    "mitigate",
    "mitigates",
    "mitigation",
    "unblock",
    "unblocks",
    "unblocking",
    "unlock",
    "clears",
    "security packet",
    "audit packet",
    "soc2",
    "soc 2",
    "evidence packet",
    "approval packet",
)


def _frame_role_candidates(
    candidates: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    model_cards: list[Any] | tuple[Any, ...],
    evidence_by_id: dict[str, str],
) -> tuple[_FrameRoleCandidate, ...]:
    records: dict[UUID, dict[str, Any]] = {}

    def add_record(
        model_id: UUID,
        *,
        text: str,
        candidate_id: str | None = None,
        suggested_edge_kinds: tuple[str, ...] = (),
        confidence: float = 0.65,
        source: str,
    ) -> None:
        record = records.setdefault(
            model_id,
            {
                "text_parts": [],
                "candidate_ids": set(),
                "suggested_edge_kinds": set(),
                "confidence": 0.0,
                "sources": set(),
            },
        )
        if text:
            record["text_parts"].append(_trunc(text, 800))
        if candidate_id:
            record["candidate_ids"].add(candidate_id)
        record["suggested_edge_kinds"].update(suggested_edge_kinds)
        record["confidence"] = max(float(record["confidence"]), confidence)
        record["sources"].add(source)

    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_id") or "").strip()
        if not candidate_id:
            continue
        suggested_edge_kinds = tuple(
            sorted(
                {
                    str(kind or "").strip()
                    for kind in (candidate.get("suggested_edge_kinds") or [])
                    if str(kind or "").strip() in EDGE_REGISTRY
                }
            )
        )
        confidence = _candidate_confidence(candidate, default=0.65)
        text = _frame_candidate_text(candidate, evidence_by_id=evidence_by_id)
        for model_id in _candidate_model_ids(candidate):
            add_record(
                model_id,
                text=text,
                candidate_id=candidate_id,
                suggested_edge_kinds=suggested_edge_kinds,
                confidence=confidence,
                source="memory_decision_candidate",
            )

    for model in model_cards:
        model_id = _coerce_uuid(getattr(model, "id", None))
        if model_id is None:
            continue
        add_record(
            model_id,
            text=_frame_model_card_text(model),
            confidence=_model_confidence(model, default=0.65),
            source="selected_model_card",
        )

    role_candidates: list[_FrameRoleCandidate] = []
    for model_id, record in records.items():
        text = " | ".join(dict.fromkeys(record["text_parts"]))
        if not text:
            continue
        role_candidates.append(
            _FrameRoleCandidate(
                model_id=model_id,
                text=_trunc(text, 1800),
                candidate_ids=tuple(sorted(record["candidate_ids"])),
                suggested_edge_kinds=tuple(sorted(record["suggested_edge_kinds"])),
                confidence=float(record["confidence"] or 0.65),
                source="+".join(sorted(record["sources"])),
            )
        )
    return tuple(role_candidates)


def _frame_complete_role_from_candidates(
    role: Literal["downstream_risk", "possible_resolution"],
    role_candidates: tuple[_FrameRoleCandidate, ...],
    *,
    cluster: tuple[RelationObligation, ...],
    blocker: UUID,
    blocked_work: UUID,
    used_models: set[UUID],
    packet_context_text: str = "",
) -> UUID | None:
    if not role_candidates:
        return None
    cluster_text = " | ".join(
        part for part in (_frame_cluster_text(cluster), packet_context_text) if part
    )
    cluster_candidate_ids = {obligation.candidate_id for obligation in cluster}
    cluster_model_ids = {
        model_id
        for obligation in cluster
        for model_id in (
            obligation.source_model_id,
            obligation.target_model_id,
            *obligation.evidence_model_ids,
        )
        if model_id is not None
    }
    scored: list[tuple[float, float, str, UUID]] = []
    for candidate in role_candidates:
        if candidate.model_id in used_models or candidate.model_id in {
            blocker,
            blocked_work,
        }:
            continue
        score = _frame_role_candidate_score(
            role,
            candidate,
            cluster_text=cluster_text,
            cluster_candidate_ids=cluster_candidate_ids,
            cluster_model_ids=cluster_model_ids,
        )
        threshold = 5.0 if role == "possible_resolution" else 4.5
        if score < threshold:
            continue
        scored.append((score, candidate.confidence, str(candidate.model_id), candidate.model_id))
    if not scored:
        return None
    scored.sort(reverse=True)
    return scored[0][3]


def _frame_role_candidate_score(
    role: Literal["downstream_risk", "possible_resolution"],
    candidate: _FrameRoleCandidate,
    *,
    cluster_text: str,
    cluster_candidate_ids: set[str],
    cluster_model_ids: set[UUID],
) -> float:
    text = candidate.text.lower()
    cluster_text_lower = cluster_text.lower()
    if role == "downstream_risk":
        markers = _FRAME_DOWNSTREAM_RISK_MARKERS
        projected_edge_kind = "early_warning_for"
    else:
        markers = _FRAME_RESOLUTION_MARKERS
        projected_edge_kind = "contributes_to_resolution"

    marker_hits = [marker for marker in markers if marker in text]
    if not marker_hits:
        return 0.0

    score = 2.5 + min(3.0, float(len(marker_hits)))
    if projected_edge_kind in candidate.suggested_edge_kinds:
        score += 2.0
    if cluster_candidate_ids & set(candidate.candidate_ids):
        score += 1.5
    if candidate.model_id in cluster_model_ids:
        score += 1.0

    overlap = _frame_token_overlap(candidate.text, cluster_text)
    if overlap >= 2:
        score += min(1.5, 0.35 * overlap)
    if any(marker in cluster_text_lower for marker in marker_hits):
        score += 0.75
    if candidate.source == "selected_model_card" and overlap < 2:
        score -= 1.0
    score += min(0.8, max(0.0, candidate.confidence - 0.55))
    return score


def _frame_candidate_text(
    candidate: dict[str, Any],
    *,
    evidence_by_id: dict[str, str],
) -> str:
    parts: list[str] = []
    for value in (
        candidate.get("proposed_text"),
        candidate.get("answer_summary"),
        candidate.get("reason"),
        candidate.get("op_family"),
        candidate.get("suggested_edge_kinds"),
    ):
        if value not in (None, "", [], {}):
            parts.append(_jsonish(value))
    for evidence_id in [
        str(value)
        for key in ("supporting_evidence_ids", "counterevidence_ids")
        for value in (candidate.get(key) or [])
        if value
    ]:
        summary = evidence_by_id.get(evidence_id)
        if summary:
            parts.append(summary)
    return " | ".join(parts)


def _frame_model_card_text(model: Any) -> str:
    parts: list[str] = []
    for value in (
        getattr(model, "natural", None),
        getattr(model, "proposition", None),
        getattr(model, "proposition_kind", None),
        getattr(model, "claim_role", None),
        getattr(model, "polarity", None),
        getattr(model, "domain_tags", None),
    ):
        if value not in (None, "", [], {}):
            parts.append(_jsonish(value))
    return " | ".join(parts)


def _frame_model_text_by_id(
    model_cards: list[Any] | tuple[Any, ...],
) -> dict[UUID, str]:
    texts: dict[UUID, str] = {}
    for model in model_cards:
        model_id = _coerce_uuid(getattr(model, "id", None))
        if model_id is None:
            continue
        text = _frame_model_card_text(model)
        if text:
            texts[model_id] = text
    return texts


def _frame_endpoints_anchor_current_evidence(
    blocker_obligation: RelationObligation,
    *,
    blocker: UUID,
    blocked_work: UUID,
    packet_context_text: str,
    model_text_by_id: dict[UUID, str],
) -> bool:
    if not model_text_by_id:
        return True
    context_text = " | ".join(
        part
        for part in (
            blocker_obligation.evidence_text,
            blocker_obligation.explanation,
            packet_context_text,
        )
        if part
    )
    blocker_text = model_text_by_id.get(blocker, "")
    blocked_work_text = model_text_by_id.get(blocked_work, "")
    if blocker_text and _frame_endpoint_token_overlap(blocker_text, context_text) == 0:
        return False
    if (
        blocked_work_text
        and _frame_endpoint_token_overlap(blocked_work_text, context_text) == 0
    ):
        return False
    return True


def _frame_cluster_text(cluster: tuple[RelationObligation, ...]) -> str:
    return " | ".join(
        part
        for obligation in cluster
        for part in (
            obligation.evidence_text,
            obligation.explanation,
            obligation.edge_kind,
            " ".join(obligation.matched_markers),
        )
        if part
    )


def _frame_packet_text(packet: dict[str, Any]) -> str:
    parts: list[str] = []
    for value in (
        packet.get("signal_summary"),
        packet.get("important_unknowns"),
        packet.get("sufficiency_verdict"),
    ):
        if value not in (None, "", [], {}):
            parts.append(_jsonish(value))
    tiers = packet.get("tiers") if isinstance(packet.get("tiers"), dict) else {}
    for item in tiers.get("decisive_evidence") or []:
        if not isinstance(item, dict):
            continue
        summary = item.get("summary")
        if summary:
            parts.append(str(summary))
    for group in tiers.get("supporting_evidence_groups") or []:
        if not isinstance(group, dict):
            continue
        summary = group.get("summary") or group.get("claim_supported")
        if summary:
            parts.append(str(summary))
    return _trunc(" | ".join(parts), 1800)


def _frame_token_overlap(left: str, right: str) -> int:
    return len(_frame_tokens(left) & _frame_tokens(right))


def _frame_endpoint_token_overlap(left: str, right: str) -> int:
    left_tokens = _frame_tokens(left) - _FRAME_ENDPOINT_STOPWORDS
    right_tokens = _frame_tokens(right) - _FRAME_ENDPOINT_STOPWORDS
    return len(left_tokens & right_tokens)


def _frame_tokens(text: str) -> set[str]:
    tokens: set[str] = set()
    current: list[str] = []
    for char in text.lower():
        if char.isalnum():
            current.append(char)
            continue
        if current:
            token = "".join(current)
            if _frame_token_allowed(token):
                tokens.add(token)
            current = []
    if current:
        token = "".join(current)
        if _frame_token_allowed(token):
            tokens.add(token)
    return tokens


def _frame_token_allowed(token: str) -> bool:
    return (
        (len(token) >= 4 or token in {"dpa", "sso", "soc2"})
        and token not in _FRAME_STOPWORDS
    )


_FRAME_STOPWORDS = {
    "about",
    "active",
    "already",
    "because",
    "candidate",
    "claim",
    "confidence",
    "evidence",
    "from",
    "into",
    "model",
    "reason",
    "same",
    "signal",
    "status",
    "that",
    "this",
    "with",
}

_FRAME_ENDPOINT_STOPWORDS = _FRAME_STOPWORDS | {
    "account",
    "accounts",
    "approval",
    "batch",
    "blocked",
    "blocking",
    "candidate",
    "customer",
    "customers",
    "edge",
    "relation",
    "renewal",
    "risk",
    "risks",
}


def _candidate_confidence(candidate: dict[str, Any], *, default: float) -> float:
    try:
        return min(0.95, max(0.05, float(candidate.get("confidence"))))
    except (TypeError, ValueError):
        return default


def _model_confidence(model: Any, *, default: float) -> float:
    try:
        return min(0.95, max(0.05, float(getattr(model, "confidence", default))))
    except (TypeError, ValueError):
        return default


def _frame_owner_candidate(
    cluster: tuple[RelationObligation, ...],
    *,
    candidates_by_id: dict[str, dict[str, Any]],
    used_models: set[UUID],
) -> UUID | None:
    owner_markers = ("owner", "owns", "accountable", "legal", "priya")
    for obligation in cluster:
        candidate = candidates_by_id.get(obligation.candidate_id, {})
        text = _jsonish(
            {
                "proposed_text": candidate.get("proposed_text"),
                "answer_summary": candidate.get("answer_summary"),
                "reason": candidate.get("reason"),
            }
        ).lower()
        if not any(marker in text for marker in owner_markers):
            continue
        for model_id in _candidate_model_ids(candidate):
            if model_id not in used_models:
                return model_id
    return None


def _frame_role_binding_confidence(
    role: str,
    cluster: tuple[RelationObligation, ...],
) -> float:
    if role in {"blocker", "blocked_work"}:
        return 0.9
    confidence = max((obligation.confidence for obligation in cluster), default=0.75)
    return min(0.88, max(0.72, confidence))


def _relation_frame_op_from_obligation(
    obligation: RelationFrameObligation,
    *,
    tenant_id: UUID,
) -> RelationFrameOp:
    frame_id = _stable_relation_frame_id(tenant_id, obligation)
    return RelationFrameOp(
        id=frame_id,
        relation_kind=obligation.relation_kind,
        participants=[
            RelationFrameParticipantOp(
                model_id=participant.model_id,
                role=participant.role,
                binding_confidence=participant.binding_confidence,
                metadata={
                    "compiled_relation_frame_obligation": True,
                },
            )
            for participant in obligation.participants
        ],
        participant_binding_status="bound",
        write_policy="project_edges",
        status="accepted",
        confidence=obligation.confidence,
        evidence_event_ids=list(obligation.evidence_event_ids),
        evidence_model_ids=list(obligation.evidence_model_ids),
        evidence_text=obligation.evidence_text,
        explanation=obligation.explanation,
        metadata={
            "relation_frame_origin": "mandatory_relation_frame_obligation",
            "source_candidate_ids": list(obligation.source_candidate_ids),
        },
    )


def _stable_relation_frame_id(
    tenant_id: UUID,
    obligation: RelationFrameObligation,
) -> UUID:
    participant_key = ",".join(
        f"{participant.role}:{participant.model_id}"
        for participant in obligation.participants
    )
    return uuid5(
        NAMESPACE_URL,
        f"fyralis:relation_frame:{tenant_id}:{obligation.relation_kind}:{participant_key}",
    )


def _relation_frame_op_key(
    op: RelationFrameOp,
) -> tuple[str, tuple[tuple[str, UUID], ...]]:
    return (
        op.relation_kind,
        tuple(
            sorted(
                (participant.role, participant.model_id)
                for participant in op.participants
            )
        ),
    )


def _frame_obligation_projection_keys(
    obligations: tuple[RelationFrameObligation, ...],
) -> set[tuple[str, UUID, UUID]]:
    keys: set[tuple[str, UUID, UUID]] = set()
    for obligation in obligations:
        roles = {
            participant.role: participant.model_id
            for participant in obligation.participants
        }
        blocker = roles.get("blocker")
        blocked_work = roles.get("blocked_work")
        downstream_risk = roles.get("downstream_risk")
        possible_resolution = roles.get("possible_resolution")
        if blocker is not None and blocked_work is not None:
            keys.add(("blocks", blocker, blocked_work))
        if blocked_work is not None and downstream_risk is not None:
            keys.add(("early_warning_for", blocked_work, downstream_risk))
        if possible_resolution is not None and blocker is not None:
            keys.add(("contributes_to_resolution", possible_resolution, blocker))
    return keys


def apply_relation_lifecycle_kernel(
    diff: RawDiff,
    *,
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> RawDiff:
    """Enforce the single Think relation lifecycle boundary.

    Raw edge writes are accepted as legacy input, then canonicalized into
    RelationClaimOps before validation/apply. T1 packet-derived grounding,
    relation claims, and N-ary frames are applied in the same pass so every
    Think path has one final graph contract.
    """

    relation_claim_ops = [*diff.relation_claim_ops]
    edge_relation_ops, edge_lifecycle_summary = (
        _relation_claim_ops_from_legacy_edge_ops(
            diff.edge_ops,
            existing_ops=relation_claim_ops,
        )
    )
    relation_claim_ops.extend(edge_relation_ops)

    grounding_ops: list[ClaimOp] = []
    grounding_summary: str | None = None
    new_relation_ops: list[RelationClaimOp] = []
    relation_summary: str | None = None
    frame_ops: list[RelationFrameOp] = []
    frame_summary: str | None = None

    packet = (
        _inquiry_context_packet(bundle)
        if trigger.kind == "T1"
        and not _relation_lifecycle_packet_pass_already_applied(diff.reasoning_trace)
        else None
    )
    if packet is None:
        if not edge_relation_ops:
            return diff
        trace = "; ".join(
            part for part in (diff.reasoning_trace, edge_lifecycle_summary) if part
        )
        return diff.model_copy(
            update={
                "relation_claim_ops": relation_claim_ops,
                "edge_ops": [],
                "reasoning_trace": trace,
            }
        )
    candidates = _memory_candidates_from_packet(packet)
    if not candidates:
        if not edge_relation_ops:
            return diff
        trace = "; ".join(
            part for part in (diff.reasoning_trace, edge_lifecycle_summary) if part
        )
        return diff.model_copy(
            update={
                "relation_claim_ops": relation_claim_ops,
                "edge_ops": [],
                "reasoning_trace": trace,
            }
        )
    if _relation_lifecycle_should_skip_packet_obligations(diff, packet=packet):
        trace = "; ".join(
            part
            for part in (
                diff.reasoning_trace,
                edge_lifecycle_summary,
                "relation_lifecycle_kernel=packet_obligations_skipped:explicit_noop",
            )
            if part
        )
        return diff.model_copy(
            update={
                "relation_claim_ops": relation_claim_ops,
                "edge_ops": [],
                "reasoning_trace": trace,
            }
        )
    grounding_obligations = grounding_obligations_from_packet(
        packet,
        candidates,
        model_cards=bundle.models,
    )
    grounding_ops, grounding_summary = grounding_claim_ops_from_obligations(
        grounding_obligations,
        trigger=trigger,
        existing_ops=diff.claim_ops,
    )
    obligations = relation_obligations_from_packet(packet, candidates)
    frame_obligations = relation_frame_obligations_from_obligations(
        obligations,
        candidates=candidates,
        packet=packet,
        model_cards=bundle.models,
    )
    frame_ops, frame_summary = relation_frame_ops_from_obligations(
        frame_obligations,
        tenant_id=trigger.tenant_id,
        existing_ops=diff.relation_frame_ops,
    )
    new_ops, summary = relation_claim_ops_from_obligations(
        obligations,
        existing_ops=relation_claim_ops,
        covered_edges=_frame_obligation_projection_keys(frame_obligations),
    )
    new_relation_ops = new_ops
    relation_summary = summary
    relation_claim_ops.extend(new_relation_ops)
    if not grounding_ops and not edge_relation_ops and not new_relation_ops and not frame_ops:
        return diff
    trace = "; ".join(
        part
        for part in (
            diff.reasoning_trace,
            edge_lifecycle_summary,
            grounding_summary,
            relation_summary,
            frame_summary,
        )
        if part
    )
    return diff.model_copy(
        update={
            "claim_ops": [*diff.claim_ops, *grounding_ops],
            "relation_claim_ops": relation_claim_ops,
            "relation_frame_ops": [*diff.relation_frame_ops, *frame_ops],
            "edge_ops": [],
            "reasoning_trace": trace,
        }
    )


def augment_raw_diff_with_relation_obligations(
    diff: RawDiff,
    *,
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> RawDiff:
    """Compatibility wrapper for the enforced relation lifecycle kernel."""

    return apply_relation_lifecycle_kernel(diff, trigger=trigger, bundle=bundle)


def _relation_lifecycle_packet_pass_already_applied(trace: str | None) -> bool:
    if not trace:
        return False
    markers = (
        "mandatory_grounding_obligations=",
        "mandatory_relation_obligations=",
        "mandatory_relation_frame_obligations=",
    )
    return any(marker in trace for marker in markers)


_RELATION_LIFECYCLE_EXPLICIT_NOOP_MARKERS = (
    "empty diff",
    "no durable diff",
    "no durable diff emitted",
    "no durable writes",
    "no durable evidenced operational claim",
    "no durable operational claim",
    "no durable model",
    "no durable relation",
    "does not provide a durable",
    "does not materially update",
    "no world-model mutation",
    "no world model mutation",
)

def _relation_lifecycle_should_skip_packet_obligations(
    diff: RawDiff,
    *,
    packet: dict[str, Any],
) -> bool:
    """Honor an explicit no-op decision before deterministic relation repair.

    The lifecycle kernel exists to prevent relation-bearing facts from vanishing
    when an LLM under-emits. It must not override a reasoner's explicit decision
    that the current evidence supports no durable mutation. This rule is based
    only on the returned decision, never fixture labels or input phrases.
    """

    del packet
    if _raw_diff_has_write_intent(diff):
        return False
    trace = (diff.reasoning_trace or "").lower()
    return any(marker in trace for marker in _RELATION_LIFECYCLE_EXPLICIT_NOOP_MARKERS)


def _raw_diff_has_write_intent(diff: RawDiff) -> bool:
    return any(
        (
            diff.claim_ops,
            diff.memory_lifecycle_ops,
            diff.relation_claim_ops,
            diff.relation_frame_ops,
            diff.edge_ops,
            diff.ontology_gap_ops,
            diff.act_ops,
            diff.resource_ops,
            diff.new_predictions,
        )
    )


def _relation_claim_ops_from_legacy_edge_ops(
    edge_ops: list[EdgeOp] | tuple[EdgeOp, ...],
    *,
    existing_ops: list[RelationClaimOp] | tuple[RelationClaimOp, ...] = (),
) -> tuple[list[RelationClaimOp], str | None]:
    if not edge_ops:
        return [], None
    existing_keys = _relation_claim_existing_keys(existing_ops)
    emitted: list[RelationClaimOp] = []
    add_count = 0
    retire_count = 0
    deduped = 0
    for index, edge_op in enumerate(edge_ops):
        op = _relation_claim_op_from_legacy_edge_op(edge_op, index=index)
        key = _relation_claim_dedup_key(op)
        if key in existing_keys:
            deduped += 1
            continue
        existing_keys.add(key)
        emitted.append(op)
        if edge_op.op == "retire":
            retire_count += 1
        else:
            add_count += 1
    summary = (
        "relation_lifecycle_kernel="
        f"legacy_edge_ops:{len(edge_ops)},canonicalized:{len(emitted)},"
        f"adds:{add_count},retires:{retire_count},deduped:{deduped}"
    )
    return emitted, summary


def _relation_claim_op_from_legacy_edge_op(
    edge_op: EdgeOp,
    *,
    index: int,
) -> RelationClaimOp:
    accepted_add = edge_op.op == "add" and edge_op.review_status == "accepted"
    is_retire = edge_op.op == "retire"
    lifecycle_id = f"legacy_edge_op:{index}:{edge_op.op}"
    metadata = {
        **dict(edge_op.metadata or {}),
        "relation_claim_origin": "relation_lifecycle_kernel_legacy_edge_op",
        "legacy_edge_op": True,
        "legacy_edge_op_index": index,
        "legacy_edge_op_detected_by": edge_op.detected_by,
        "legacy_edge_op_review_status": edge_op.review_status,
        "legacy_edge_op_reason": edge_op.reason,
        "candidate_id": lifecycle_id,
    }
    return RelationClaimOp(
        op="upsert",
        source_model_id=edge_op.source_model_id,
        target_model_id=edge_op.target_model_id,
        subject_ref={
            "kind": "model",
            "model_id": str(edge_op.source_model_id),
            "candidate_id": lifecycle_id,
        },
        object_ref={
            "kind": "model",
            "model_id": str(edge_op.target_model_id),
            "candidate_id": lifecycle_id,
        },
        predicate=edge_op.edge_kind,
        edge_kind=edge_op.edge_kind,
        direction="source_to_target",
        endpoint_binding_status="bound",
        write_policy=(
            "no_edge" if is_retire else "accepted_edge" if accepted_add else "candidate"
        ),
        status="retired" if is_retire else "accepted" if accepted_add else "candidate",
        confidence=edge_op.confidence,
        weight=edge_op.weight
        if edge_op.weight is not None
        else _relation_claim_weight(edge_op.edge_kind, edge_op.confidence),
        binding_confidence=0.9,
        evidence_event_ids=edge_op.evidence_event_ids,
        evidence_model_ids=edge_op.evidence_model_ids,
        evidence_text=edge_op.explanation,
        explanation=edge_op.reason or edge_op.explanation,
        metadata={k: v for k, v in metadata.items() if v not in (None, [], {})},
    )


def _packet_evidence_summaries(packet: dict[str, Any]) -> dict[str, str]:
    tiers = packet.get("tiers") if isinstance(packet.get("tiers"), dict) else {}
    evidence_by_id: dict[str, str] = {}
    for item in tiers.get("decisive_evidence") or []:
        if not isinstance(item, dict):
            continue
        evidence_id = str(item.get("evidence_id") or "").strip()
        summary = str(item.get("summary") or "").strip()
        if evidence_id and summary:
            evidence_by_id[evidence_id] = summary
    for group in tiers.get("supporting_evidence_groups") or []:
        if not isinstance(group, dict):
            continue
        summary = str(
            group.get("summary")
            or group.get("claim_supported")
            or ""
        ).strip()
        if not summary:
            continue
        for evidence_id in group.get("evidence_ids") or []:
            key = str(evidence_id or "").strip()
            if key:
                evidence_by_id.setdefault(key, summary)
    return evidence_by_id


def _relation_obligation_clauses(
    _packet: dict[str, Any],
    candidate: dict[str, Any],
    *,
    evidence_by_id: dict[str, str],
) -> tuple[str, ...]:
    base_parts: list[str] = []
    for value in _candidate_relation_evidence_parts(candidate):
        if value not in (None, "", [], {}):
            base_parts.append(str(value))
    base_clauses: list[str] = []
    for part in base_parts:
        base_clauses.extend(_split_relation_clauses(part))

    evidence_parts: list[str] = []
    for evidence_id in [
        str(value)
        for key in ("supporting_evidence_ids", "counterevidence_ids")
        for value in (candidate.get(key) or [])
        if value
    ]:
        summary = evidence_by_id.get(evidence_id)
        if summary:
            evidence_parts.append(summary)
    clauses = list(base_clauses)
    if (
        _clauses_have_relation_marker(base_clauses)
        or candidate.get("op_family") == "edge_insert"
        or bool(candidate.get("suggested_edge_kinds"))
    ):
        for part in evidence_parts:
            clauses.extend(_split_relation_clauses(part))
    return tuple(clause for clause in clauses if clause)


def _candidate_relation_evidence_parts(candidate: dict[str, Any]) -> tuple[Any, ...]:
    observation_parts = tuple(
        row.get("body")
        for row in candidate.get("relation_observation_evidence") or ()
        if isinstance(row, dict) and row.get("body")
        and str(row.get("observation_id")) in {
            str(value) for value in candidate.get("relation_evidence_observation_ids") or ()
        }
    )
    return (
        candidate.get("proposed_text"),
        candidate.get("answer_summary"),
        candidate.get("reason"),
        candidate.get("op_family"),
        *observation_parts,
    )


def _split_relation_clauses(text: str) -> list[str]:
    cleaned = " ".join(str(text or "").split())
    if not cleaned:
        return []
    separators = (
        ". ",
        "; ",
        " | ",
        "\n",
        " and ",
        " but ",
        " while ",
    )
    clauses = [cleaned]
    for separator in separators:
        next_clauses: list[str] = []
        for clause in clauses:
            next_clauses.extend(part.strip(" .;") for part in clause.split(separator))
        clauses = [clause for clause in next_clauses if clause]
    return [_trunc(clause, 500) for clause in clauses if len(clause) >= 8]


def _clauses_have_relation_marker(clauses: list[str]) -> bool:
    text = " ".join(clauses).lower()
    return any(
        marker in text
        for _edge_kind, markers in _RELATION_OBLIGATION_PATTERNS
        for marker in markers
    )


def _infer_relation_obligation_edge_kind(
    candidate: dict[str, Any],
    relation_clauses: tuple[str, ...],
) -> tuple[str | None, tuple[str, ...], str]:
    if not relation_clauses:
        return None, (), ""
    hinted = [
        str(kind or "").strip()
        for kind in (candidate.get("suggested_edge_kinds") or [])
        if str(kind or "").strip() in EDGE_REGISTRY
    ]

    scored: list[tuple[float, int, str, tuple[str, ...], str]] = []
    for clause_index, clause in enumerate(relation_clauses):
        text = clause.lower()
        for edge_kind, markers in _RELATION_OBLIGATION_PATTERNS:
            hits = tuple(marker for marker in markers if marker in text)
            if not hits:
                continue
            score = _relation_marker_score(edge_kind, hits)
            if edge_kind in hinted:
                score += 2.0
            elif hinted and edge_kind in _GENERIC_RELATION_KINDS:
                score -= 1.5
            if candidate.get("op_family") == "edge_insert":
                score += 0.75
            scored.append((score, -clause_index, edge_kind, hits[:5], clause))

    if scored:
        scored.sort(reverse=True)
        _score, _neg_index, edge_kind, hits, clause = scored[0]
        return edge_kind, hits, clause
    for edge_kind in hinted:
        if edge_kind not in _GENERIC_RELATION_KINDS:
            return edge_kind, ("suggested_edge_kind",), relation_clauses[0]
    if candidate.get("op_family") == "edge_insert":
        return "supports", ("edge_insert",), relation_clauses[0]
    return None, (), ""


def _relation_marker_score(edge_kind: str, markers: tuple[str, ...]) -> float:
    score = float(len(markers))
    if edge_kind not in _GENERIC_RELATION_KINDS:
        score += 2.0
    marker_text = " ".join(markers)
    if edge_kind == "blocks" and any(
        marker in marker_text
        for marker in (
            "blocked by",
            "blocks ",
            "blocker",
            "waiting on",
            "waiting status",
            "gates ",
            "critical path",
        )
    ):
        score += 2.0
    if edge_kind == "explains" and any(
        marker in marker_text
        for marker in ("helps explain", "explain why", "because", "root cause")
    ):
        score += 2.5
    if edge_kind == "weakens" and any(
        marker in marker_text
        for marker in ("contradict", "contradicted", "counterevidence", "despite")
    ):
        score += 2.5
    if edge_kind == "contributes_to_resolution" and any(
        marker in marker_text
        for marker in ("unblocks", "unblock approval", "helps resolve", "mitigate")
    ):
        score += 2.5
    if edge_kind == "early_warning_for" and any(
        marker in marker_text
        for marker in ("early warning", "risk signal", "usage is down", "usage decay")
    ):
        score += 2.0
    return score


def _relation_obligation_confidence(
    candidate: dict[str, Any],
    *,
    edge_kind: str,
    markers: tuple[str, ...],
) -> float:
    raw = candidate.get("confidence")
    try:
        base = float(raw)
    except (TypeError, ValueError):
        base = 0.7 if edge_kind not in _GENERIC_RELATION_KINDS else 0.62
    if markers and edge_kind not in _GENERIC_RELATION_KINDS:
        base = max(base, 0.7)
    if candidate.get("op_family") == "edge_insert":
        base = max(base, 0.72)
    return min(0.92, max(0.35, base))


def _relation_obligation_endpoints(
    candidate: dict[str, Any],
    *,
    edge_kind: str,
) -> tuple[UUID | None, UUID | None]:
    if candidate.get("candidate_kind") == "synthesis":
        source = _coerce_uuid(candidate.get("relation_source_model_id"))
        target = _coerce_uuid(candidate.get("relation_target_model_id"))
        members = set(_candidate_model_ids(candidate))
        if (
            source is None or target is None or source == target
            or not {source, target}.issubset(members)
        ):
            return None, None
        return source, target
    target_ids = _dedupe_uuids(_uuid_values(candidate.get("target_model_ids")))
    evidence_ids = _dedupe_uuids(_uuid_values(candidate.get("evidence_model_ids")))
    all_ids = _candidate_model_ids(candidate)
    target_model_id = target_ids[0] if target_ids else None
    source_model_id = next(
        (model_id for model_id in evidence_ids if model_id != target_model_id),
        None,
    )
    if source_model_id is None and len(target_ids) >= 2:
        source_model_id = next(
            (model_id for model_id in target_ids[1:] if model_id != target_model_id),
            None,
        )
    if source_model_id is None and target_model_id is not None:
        source_model_id = next(
            (model_id for model_id in all_ids if model_id != target_model_id),
            None,
        )
    if target_model_id is None and source_model_id is not None:
        target_model_id = next(
            (model_id for model_id in all_ids if model_id != source_model_id),
            None,
        )
    if (
        edge_kind == "contributes_to_resolution"
        and len(all_ids) >= 2
        and source_model_id == target_model_id
    ):
        source_model_id, target_model_id = all_ids[0], all_ids[1]
    return source_model_id, target_model_id


def _relation_claim_op_from_obligation(
    obligation: RelationObligation,
    *,
    decision: BatchMemoryCandidateDecision | None,
    claim_placeholder: UUID | None,
) -> RelationClaimOp:
    source_model_id = obligation.source_model_id
    target_model_id = obligation.target_model_id
    if (
        source_model_id is None
        and claim_placeholder is not None
        and claim_placeholder != target_model_id
    ):
        source_model_id = claim_placeholder
    if target_model_id is None and source_model_id is not None:
        for candidate_model_id in obligation.evidence_model_ids:
            if candidate_model_id != source_model_id:
                target_model_id = candidate_model_id
                break
    if source_model_id == target_model_id:
        if claim_placeholder is not None and claim_placeholder != target_model_id:
            source_model_id = claim_placeholder
        else:
            source_model_id = None

    has_bound_endpoints = source_model_id is not None and target_model_id is not None
    decision_rejected = (
        decision is not None
        and (decision.decision != "accept" or decision.operation == "no_op")
    )
    # Only a relation-bearing decision can promote an inferred obligation into
    # accepted truth. A claim/lifecycle/act decision may change a Model without
    # deciding that the candidate relation remains true.
    relation_authorized = (
        decision is None
        or decision.operation in {"edge", "claim_and_edge", "situation_and_edge"}
    )
    confidence = obligation.confidence
    if decision is not None:
        confidence = max(confidence, float(decision.confidence))
    precise_kind = obligation.edge_kind not in _GENERIC_RELATION_KINDS
    if (
        has_bound_endpoints
        and precise_kind
        and confidence >= 0.68
        and not decision_rejected
        and relation_authorized
    ):
        write_policy = "accepted_edge"
        status = "accepted"
    elif decision_rejected or not has_bound_endpoints or not relation_authorized:
        write_policy = "needs_review"
        status = "needs_review"
    else:
        write_policy = "candidate"
        status = "candidate"

    metadata = {
        "relation_claim_origin": "mandatory_relation_obligation",
        "memory_decision_candidate_id": obligation.candidate_id,
        "relation_obligation": True,
        "matched_markers": list(obligation.matched_markers),
        "compiled_decision": decision.decision if decision is not None else None,
        "compiled_decision_operation": decision.operation if decision is not None else None,
        "compiled_decision_confidence": (
            decision.confidence if decision is not None else None
        ),
        **({
            "review_status_downgraded_by": "relation_authorization_guard",
            "relation_authorization_guard": True,
        } if not relation_authorized else {}),
    }
    return RelationClaimOp(
        op="upsert",
        source_model_id=source_model_id,
        target_model_id=target_model_id,
        subject_ref=_relation_ref(
            model_id=source_model_id,
            candidate_id=obligation.candidate_id,
            fallback_text=obligation.evidence_text,
        ),
        object_ref=_relation_ref(
            model_id=target_model_id,
            candidate_id=obligation.candidate_id,
            fallback_text=obligation.evidence_text,
        ),
        predicate=obligation.edge_kind,
        edge_kind=obligation.edge_kind,
        direction="source_to_target" if has_bound_endpoints else "unknown",
        endpoint_binding_status=(
            "bound"
            if has_bound_endpoints
            else ("partially_bound" if source_model_id or target_model_id else "unbound")
        ),
        write_policy=write_policy,  # type: ignore[arg-type]
        status=status,  # type: ignore[arg-type]
        confidence=confidence,
        weight=_relation_claim_weight(obligation.edge_kind, confidence),
        binding_confidence=0.9 if has_bound_endpoints else 0.45,
        evidence_event_ids=list(obligation.evidence_event_ids),
        evidence_model_ids=list(obligation.evidence_model_ids),
        evidence_text=obligation.evidence_text,
        explanation=obligation.explanation,
        metadata={k: v for k, v in metadata.items() if v not in (None, [], {})},
    )


def _relation_ref(
    *,
    model_id: UUID | None,
    candidate_id: str,
    fallback_text: str,
) -> dict[str, Any]:
    if model_id is not None:
        return {
            "kind": "model",
            "model_id": str(model_id),
            "candidate_id": candidate_id,
        }
    return {
        "kind": "relation_phrase",
        "candidate_id": candidate_id,
        "text": _trunc(fallback_text, 240),
    }


def _relation_claim_weight(edge_kind: str, confidence: float) -> float | None:
    spec = EDGE_REGISTRY.get(edge_kind)
    if spec is None or not spec.weight_allowed:
        return None
    if not spec.weight_required:
        return None
    return min(1.0, max(0.05, float(confidence)))


def _relation_claim_existing_keys(
    ops: list[RelationClaimOp] | tuple[RelationClaimOp, ...],
) -> set[tuple[str | None, str, UUID | None, UUID | None]]:
    return {_relation_claim_dedup_key(op) for op in ops}


def _relation_claim_dedup_key(
    op: RelationClaimOp,
) -> tuple[str | None, str, UUID | None, UUID | None]:
    candidate_id = None
    for ref in (op.subject_ref, op.object_ref, op.metadata):
        raw = ref.get("candidate_id") or ref.get("memory_decision_candidate_id")
        if raw:
            candidate_id = str(raw)
            break
    return (candidate_id, op.edge_kind, op.source_model_id, op.target_model_id)


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
    text = str(
        candidate.get("entailed_claim_text")
        or decision.claim_text
        or candidate.get("proposed_text")
        or ""
    ).strip()
    if len(text) < 12:
        return None, None, "claim_text is too short"
    born_event = uuid7()
    role = force_role or decision.claim_role
    proposition = _batch_claim_proposition(role, text, candidate, decision)
    evidence_event_ids = [str(value) for value in _candidate_event_ids(candidate)]
    if role == "situation":
        if len(evidence_event_ids) != 1:
            return None, None, "synthesis requires exactly one conclusion opener"
        if proposition.get("supported_relation") is None:
            return None, None, "synthesis relation lacks exact accepted endpoints"
    if evidence_event_ids:
        proposition["evidence_event_ids"] = evidence_event_ids
    evidence_model_ids = (
        [decision.model_id]
        if decision.model_id is not None
        else _dedupe_uuids([
            *_uuid_values(candidate.get("evidence_model_ids")),
            *_uuid_values(candidate.get("target_model_ids")),
        ])
    )
    entry = {
        "tenant_id": str(trigger.tenant_id),
        "born_from_event_id": str(born_event),
        "proposition": proposition,
        "natural": _trunc(text, 1000),
        "confidence": min(0.69, max(0.35, float(decision.confidence))),
        "confidence_at_assertion": min(0.69, max(0.35, float(decision.confidence))),
        "scope_actors": [str(actor_id) for actor_id in (trigger.scope_actors or [])],
        "scope_entities": _candidate_scope_entities(candidate) or _scope_entities(trigger),
        "scope_temporal": {},
        "falsifier": None,
    }
    if evidence_model_ids:
        entry["supporting_model_ids"] = [
            str(model_id) for model_id in evidence_model_ids
        ]
    if evidence_event_ids:
        entry["supporting_event_ids"] = evidence_event_ids
    observation_manifest = _candidate_observation_manifest(
        candidate,
        allowed_ids=set(evidence_event_ids),
    )
    if observation_manifest:
        entry["evidence_observation_manifest"] = observation_manifest
        # Preserve the compiler-authorized superset through splitter/applier
        # transformations. Canonical admission reopens these exact observation
        # bodies before allowing any redistributed support ID into truth.
        proposition["evidence_observation_manifest"] = observation_manifest
    return ClaimOp(op="insert", entry=entry), born_event, ""


def _closed_atomic_durable_fate(
    candidate: dict[str, Any],
    *,
    trigger: TriggerContext,
    force_insert: bool = False,
) -> tuple[ClaimOp | None, MemoryLifecycleOp | None, UUID | None, str]:
    evidence_event_ids = _candidate_event_ids(candidate)
    if len(evidence_event_ids) != 1:
        return None, None, None, "closed atomic evidence must be exactly singleton"
    target_ids = _dedupe_uuids(_uuid_values(candidate.get("target_model_ids")))
    exact_bound = (
        len(target_ids) == 1
        and set(candidate.get("allowed_operations") or ()) == {"memory_lifecycle"}
    )
    confidence = min(
        0.95,
        max(0.55, float(candidate.get("confidence") or 0.58)),
    )
    candidate_id = str(candidate.get("candidate_id") or "closed_atomic")
    if exact_bound and not force_insert:
        op = MemoryLifecycleOp(
            model_id=target_ids[0],
            action="confirm",
            evidence_event_ids=evidence_event_ids,
            claim_local_evidence_event_ids=evidence_event_ids,
            rationale=(
                "Compiler-confirmed exact same-tenant, same-scope atomic identity "
                f"for {candidate_id}."
            ),
            metadata={
                "source": "closed_atomic_durable_fate",
                "candidate_id": candidate_id,
                "binding": "exact_natural_and_scope",
            },
        )
        return None, op, None, ""

    decision = BatchMemoryCandidateDecision(
        candidate_id=candidate_id,
        decision="accept",
        operation="claim",
        confidence=confidence,
        claim_role="fact",
        claim_text=str(candidate.get("entailed_claim_text") or ""),
        reason="Compiler-proven closed atomic direct assertion.",
    )
    claim_op, placeholder, block_reason = _claim_op_from_batch_decision(
        candidate,
        decision,
        trigger,
        force_role="fact",
    )
    return claim_op, None, placeholder, block_reason


def _candidate_observation_manifest(
    candidate: dict[str, Any],
    *,
    allowed_ids: set[str],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in candidate.get("observation_evidence") or []:
        if not isinstance(raw, dict):
            continue
        observation_id = str(raw.get("observation_id") or "").strip()
        body = str(raw.get("body") or "").strip()
        if (
            not observation_id
            or observation_id not in allowed_ids
            or observation_id in seen
            or not body
        ):
            continue
        seen.add(observation_id)
        rows.append(
            {
                "observation_id": observation_id,
                "body": body,
                "source_channel": str(raw.get("source_channel") or ""),
            }
        )
    return rows[:12]


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
    candidate_event_ids = _candidate_event_ids(candidate)
    candidate_event_id_set = set(candidate_event_ids)
    evidence_event_ids = [
        event_id
        for event_id in _dedupe_uuids(decision.claim_local_evidence_event_ids)
        if event_id in candidate_event_id_set
    ]
    changes: dict[str, Any] = {
        "confidence": min(0.74, max(0.35, float(decision.confidence))),
    }
    if evidence_event_ids:
        changes["supporting_event_ids"] = evidence_event_ids
    return ClaimOp(op="update", model_id=model_id, changes=changes), model_id, ""


def _memory_lifecycle_op_from_batch_decision(
    candidate: dict[str, Any],
    decision: BatchMemoryCandidateDecision,
) -> tuple[MemoryLifecycleOp | None, str]:
    confidence_floor = _env_float("THINK_COMPILED_BATCH_LIFECYCLE_MIN_CONFIDENCE", 0.52)
    if decision.confidence < confidence_floor:
        return None, "decision confidence below lifecycle floor"
    model_id = (
        decision.model_id
        or decision.target_model_id
        or _first_uuid(candidate.get("target_model_ids"))
        or _first_uuid(candidate.get("evidence_model_ids"))
    )
    if model_id is None:
        return None, "missing model_id for lifecycle reconciliation"
    action = decision.lifecycle_action or _infer_batch_lifecycle_action(candidate, decision)
    evidence_event_ids = _candidate_event_ids(candidate)
    candidate_event_id_set = set(evidence_event_ids)
    claim_local_evidence_event_ids = [
        event_id
        for event_id in _dedupe_uuids(decision.claim_local_evidence_event_ids)
        if event_id in candidate_event_id_set
    ]
    evidence_model_ids = _dedupe_uuids(
        [
            *_uuid_values(candidate.get("evidence_model_ids")),
            *_uuid_values(candidate.get("target_model_ids")),
        ]
    )
    if model_id in evidence_model_ids:
        evidence_model_ids = [value for value in evidence_model_ids if value != model_id]
    metadata = {
        "source": "compiled_batch_memory_candidate",
        "candidate_id": decision.candidate_id,
        "operation": decision.operation,
    }
    if action == "revise" and str(decision.claim_text or "").strip():
        revised_text = str(decision.claim_text).strip()
        next_proposition = _batch_claim_proposition(
            decision.claim_role or "situation",
            revised_text,
            candidate,
            decision,
        )
        prior_proposition = candidate.get("target_proposition")
        if isinstance(prior_proposition, dict):
            # Revision changes the judgment while preserving the identity and
            # compositional contract of the accepted situation it updates.
            for key in (
                "kind",
                "claim_role",
                "abstraction_level",
                "synthesis_contract",
                "subject",
                "scope_label",
                "scope_ref",
                "member_model_ids",
                "pressure_type",
                "affected_decisions",
                "affected_customers",
                "affected_teams",
            ):
                if key in prior_proposition:
                    next_proposition[key] = prior_proposition[key]
            prior_relation = prior_proposition.get("supported_relation")
            if isinstance(prior_relation, dict):
                next_proposition["supported_relation"] = {
                    **prior_relation,
                    "mechanism": revised_text,
                    "evidence_event_ids": [
                        str(value) for value in claim_local_evidence_event_ids
                    ],
                    **({"lifecycle": "retired"}
                       if _revision_retires_supported_relation(
                           prior_relation,
                           revised_text,
                           lifecycle_phase=str(
                               candidate.get("lifecycle_phase") or ""
                           ),
                       ) else {}),
                }
        next_proposition["lifecycle_phase"] = (
            candidate.get("lifecycle_phase") or "correction"
        )
        metadata["next_proposition"] = next_proposition
        metadata["next_natural"] = revised_text
    op = MemoryLifecycleOp(
        op="reconcile",
        model_id=model_id,
        action=action,
        evidence_event_ids=evidence_event_ids,
        claim_local_evidence_event_ids=claim_local_evidence_event_ids,
        evidence_model_ids=evidence_model_ids,
        confidence_delta=decision.confidence_delta,
        confidence=min(0.95, max(0.05, float(decision.confidence))),
        resolution_outcome=decision.resolution_outcome,
        rationale=_trunc(decision.reason, 700),
        reason=decision.archive_reason,
        superseded_by_model_id=decision.superseded_by_model_id,
        metadata=metadata,
    )
    return op, ""


def _revision_retires_supported_relation(
    relation: dict[str, Any],
    revised_text: str,
    *,
    lifecycle_phase: str,
) -> bool:
    kind = str(relation.get("kind") or "").casefold()
    text = revised_text.casefold()
    if lifecycle_phase != "correction":
        return False
    if kind in {"blocks", "dependency_constraint"}:
        return any(phrase in text for phrase in (
            "no longer blocked",
            "is unblocked",
            "blocker cleared",
            "blocker was removed",
            "prerequisite was completed",
            "prerequisite is complete",
        ))
    return False


def _retired_supported_relation_op(
    candidate: dict[str, Any],
    decision: BatchMemoryCandidateDecision,
    *,
    lifecycle_op: MemoryLifecycleOp,
) -> RelationClaimOp | None:
    if lifecycle_op.action != "revise":
        return None
    prior_proposition = candidate.get("target_proposition")
    if not isinstance(prior_proposition, dict):
        return None
    relation = prior_proposition.get("supported_relation")
    revised_text = str(decision.claim_text or "").strip()
    if (
        not isinstance(relation, dict)
        or not revised_text
        or not _revision_retires_supported_relation(
            relation,
            revised_text,
            lifecycle_phase=str(candidate.get("lifecycle_phase") or ""),
        )
    ):
        return None
    source_model_id = _coerce_uuid(relation.get("source_model_id"))
    target_model_id = _coerce_uuid(relation.get("target_model_id"))
    source_version_id = _coerce_uuid(relation.get("source_model_version_id"))
    target_version_id = _coerce_uuid(relation.get("target_model_version_id"))
    if None in {
        source_model_id,
        target_model_id,
        source_version_id,
        target_version_id,
    }:
        return None
    prior_kind = str(relation.get("kind") or "")
    edge_kind = {
        "dependency_constraint": "blocks",
        "enablement": "enables",
        "causal_influence": "causes",
        "predictive_indicator": "predicts",
    }.get(prior_kind, prior_kind)
    evidence_ids = list(lifecycle_op.claim_local_evidence_event_ids)
    return RelationClaimOp(
        op="upsert",
        source_model_id=source_model_id,
        target_model_id=target_model_id,
        source_model_version_id=source_version_id,
        target_model_version_id=target_version_id,
        subject_ref={"kind": "model", "model_id": str(source_model_id)},
        object_ref={"kind": "model", "model_id": str(target_model_id)},
        predicate=edge_kind,
        edge_kind=edge_kind,
        direction="source_to_target",
        endpoint_binding_status="bound",
        write_policy="no_edge",
        status="retired",
        confidence=float(decision.confidence),
        binding_confidence=1.0,
        evidence_event_ids=evidence_ids,
        evidence_model_ids=[source_model_id, target_model_id],
        evidence_text=revised_text,
        explanation=(
            "Authoritative composite correction retires the previously "
            f"supported {edge_kind} relation."
        ),
        metadata={
            "relation_claim_origin": "composite_correction_retirement",
            "governing_composite_model_id": str(lifecycle_op.model_id),
            "memory_decision_candidate_id": decision.candidate_id,
        },
    )


def _infer_batch_lifecycle_action(
    candidate: dict[str, Any],
    decision: BatchMemoryCandidateDecision,
) -> MemoryLifecycleAction:
    text = " ".join(
        str(value or "")
        for value in (
            decision.reason,
            candidate.get("proposed_text"),
            candidate.get("evidence_text"),
            candidate.get("summary"),
        )
    ).lower()
    if any(token in text for token in ("supersed", "replaced by", "obsolete because")):
        return "supersede"
    if any(token in text for token in ("archive", "stale", "no longer relevant")):
        return "archive"
    if any(token in text for token in ("falsif", "contradict", "violat", "disprov")):
        return "falsify"
    if any(token in text for token in ("revise", "partial", "weaken", "less likely")):
        return "revise"
    if any(token in text for token in ("confirm", "reinforce", "resolved", "holds")):
        return "confirm"
    return "unchanged"


def _batch_claim_proposition(
    role: BatchClaimRole,
    text: str,
    candidate: dict[str, Any],
    decision: BatchMemoryCandidateDecision,
) -> dict[str, Any]:
    evidence_event_ids = [str(value) for value in _candidate_event_ids(candidate)]
    if role == "situation":
        members = _situation_member_ids(candidate, decision)
        scope = _claim_about(candidate)
        scope_coordinate = _candidate_scope_coordinate(candidate)
        supported_relation = _supported_synthesis_relation(candidate, decision)
        return {
            "kind": "belief",
            "claim_role": "situation",
            "abstraction_level": "composite",
            "synthesis_contract": True,
            "situation": _trunc(text, 180),
            "summary": text,
            "subject": scope,
            "scope_label": scope,
            "scope_ref": scope_coordinate[1] if scope_coordinate else None,
            "member_model_ids": [str(member) for member in members],
            "supported_relation": supported_relation,
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
            "lifecycle_phase": candidate.get("lifecycle_phase"),
        }
    base = {
        "kind": "belief",
        "claim_role": role,
        "abstraction_level": "pattern" if role == "pattern" else "atomic",
        "time_mode": "recurring" if role == "pattern" else "current",
        "modality": "inferred",
        "polarity": "negative" if role == "concern" else "neutral",
        "compiled_memory_candidate_id": str(candidate.get("candidate_id") or ""),
        "lifecycle_phase": candidate.get("lifecycle_phase"),
    }
    if (
        str(candidate.get("entailed_claim_text") or "").strip()
        and len(evidence_event_ids) == 1
    ):
        base["closed_atomic_contract"] = {
            "version": "v1",
            "compiler_entails_exact_text": True,
            "evidence_cardinality": "singleton",
        }
    scope_coordinate = _candidate_scope_coordinate(candidate)
    if scope_coordinate:
        base.update(
            {
                "scope_label": _claim_about(candidate),
                "scope_ref": scope_coordinate[1],
            }
        )
    if role == "concern":
        base.update({"about": _claim_about(candidate), "nature": text})
    elif role == "hypothesis":
        base.update({
            "subject": _claim_about(candidate),
            "hypothesis_text": text,
            "test_conditions": (
                "Confirm, revise, or reject this synthesis as additional "
                "scope-local evidence and outcomes arrive."
            ),
            "member_model_ids": [
                str(value)
                for value in _dedupe_uuids([
                    *_uuid_values(candidate.get("evidence_model_ids")),
                    *_uuid_values(candidate.get("target_model_ids")),
                ])
            ][:8],
            "synthesis_contract": True,
        })
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


def _supported_synthesis_relation(
    candidate: dict[str, Any], decision: BatchMemoryCandidateDecision,
) -> dict[str, Any] | None:
    obligation = candidate.get("explicit_relation_obligation")
    if not isinstance(obligation, dict):
        return None
    edge_kind = str(obligation.get("edge_kind") or "").strip()
    kind = {
        "blocks": "dependency_constraint", "depends_on": "dependency_constraint",
        "dependency_constraint": "dependency_constraint",
        "causes": "causal_influence", "influences": "causal_influence",
        "causal_influence": "causal_influence",
        "predicts": "predictive_indicator",
        "predictive_indicator": "predictive_indicator",
    }.get(edge_kind)
    members = _situation_member_ids(candidate, decision)
    source_id = _coerce_uuid(obligation.get("source_model_id")) or decision.source_model_id
    target_id = _coerce_uuid(obligation.get("target_model_id")) or decision.target_model_id
    obligation_event_ids = set(_uuid_values(obligation.get("evidence_event_ids")))
    allowed_relation_event_ids = set(_uuid_values(
        candidate.get("relation_evidence_observation_ids")
    ))
    obligation_model_ids = set(_uuid_values(obligation.get("evidence_model_ids")))
    if (
        kind is None
        or source_id is None
        or target_id is None
        or source_id == target_id
        or source_id not in members
        or target_id not in members
        or not obligation_event_ids
        or not obligation_event_ids.issubset(allowed_relation_event_ids)
        or not {source_id, target_id}.issubset(obligation_model_ids)
    ):
        return None
    source_version_id, target_version_id = _candidate_relation_endpoint_versions(
        candidate, source_id, target_id,
    )
    if source_version_id is None or target_version_id is None:
        return None
    return {
        "kind": kind,
        "mechanism": _trunc(
            str(obligation.get("evidence_text") or ""),
            280,
        ),
        "source_model_id": str(source_id),
        "target_model_id": str(target_id),
        "source_model_version_id": str(source_version_id),
        "target_model_version_id": str(target_version_id),
        "evidence_event_ids": [str(value) for value in sorted(obligation_event_ids, key=str)],
    }


def _candidate_event_ids(candidate: dict[str, Any]) -> list[UUID]:
    """Return only the semantic candidate's exact observation members.

    New packets expose ``member_observation_ids`` explicitly. The legacy key is
    retained solely for compatibility with older persisted packets; it is never
    unioned with the member set because that could reintroduce transport-batch
    observations into a candidate-local claim.
    """

    member_ids = candidate.get("member_observation_ids")
    if member_ids:
        return _uuid_values(member_ids)
    return _uuid_values(candidate.get("source_observation_ids"))


def _situation_member_ids(
    candidate: dict[str, Any],
    decision: BatchMemoryCandidateDecision,
) -> list[UUID]:
    closed = _dedupe_uuids([
        *_uuid_values(candidate.get("evidence_model_ids")),
        *_uuid_values(candidate.get("target_model_ids")),
    ])[:8]
    selected = _dedupe_uuids(decision.situation_member_model_ids or [])
    if len(selected) >= 2 and set(selected).issubset(closed):
        return selected[:8]
    return closed


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
    if any(
        token in lower for token in ("approval", "decision", "procurement", "legal")
    ):
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
        "Invalid if later authoritative evidence shows the candidate-local "
        f"condition is not true: {_trunc(text, 180)}"
    )


def _claim_about(candidate: dict[str, Any]) -> str:
    semantic_scope = [
        str(value).strip()
        for value in (candidate.get("semantic_scope") or [])
        if str(value).strip()
    ]
    if semantic_scope:
        return semantic_scope[0]
    targets = [
        str(value)
        for key in ("target_model_ids", "target_act_ids", "evidence_model_ids")
        for value in (candidate.get(key) or [])
        if value
    ]
    if targets:
        return targets[0]
    return "unscoped_candidate"


def _candidate_scope_entities(candidate: dict[str, Any]) -> list[dict[str, str]]:
    """Translate the compiled candidate coordinate into canonical Model scope."""

    coordinate = _candidate_scope_coordinate(candidate)
    if coordinate is None:
        return []
    scope_type, scope_ref = coordinate
    return [{"type": scope_type, "id": scope_ref}]


def _candidate_scope_coordinate(
    candidate: dict[str, Any],
) -> tuple[str, str] | None:
    values = [
        str(value).strip()
        for value in (candidate.get("semantic_scope") or [])
        if str(value).strip()
    ]
    if not values:
        return None
    label = values[0]
    words = set(re.findall(r"[a-z0-9]+", label.casefold()))
    if words.intersection({"renewal", "contract", "commitment"}):
        scope_type = "commitment"
    elif words.intersection({"customer", "account"}):
        scope_type = "customer"
    elif words.intersection({"actor", "owner", "person"}):
        scope_type = "actor"
    else:
        # Releases, migrations, handoffs, projects, and unnamed operational
        # episodes are workstreams until the entity substrate promotes a more
        # specific typed coordinate.
        scope_type = "workstream"
    slug = "-".join(re.findall(r"[a-z0-9]+", label.casefold()))
    if not slug:
        return None
    return scope_type, f"{scope_type}:{slug}"


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


def _relation_claim_op_from_edge_op(
    edge_op: EdgeOp,
    *,
    candidate: dict[str, Any],
    origin: str,
) -> RelationClaimOp:
    source_version_id, target_version_id = _candidate_relation_endpoint_versions(
        candidate, edge_op.source_model_id, edge_op.target_model_id,
    )
    semantic_scope = _relation_semantic_scope(candidate)
    evidence_events = _dedupe_uuids([
        *edge_op.evidence_event_ids,
        *_uuid_values(candidate.get("member_observation_ids")),
        *_uuid_values(candidate.get("source_observation_ids")),
    ])
    evidence_models = _dedupe_uuids(edge_op.evidence_model_ids)
    accepted = bool(
        edge_op.review_status == "accepted"
        and _is_governed_batch_relation(edge_op.edge_kind)
        and source_version_id is not None
        and target_version_id is not None
        and semantic_scope
        and evidence_events
        and edge_op.source_model_id in evidence_models
        and edge_op.target_model_id in evidence_models
    )
    subject_ref = {
        "kind": "model",
        "model_id": str(edge_op.source_model_id),
        "candidate_id": candidate.get("candidate_id") or candidate.get("id"),
    }
    object_ref = {
        "kind": "model",
        "model_id": str(edge_op.target_model_id),
        "candidate_id": candidate.get("candidate_id") or candidate.get("id"),
    }
    proposed_text = str(
        candidate.get("proposed_text")
        or candidate.get("explanation")
        or edge_op.explanation
        or ""
    ).strip()
    metadata = {
        **dict(edge_op.metadata or {}),
        "relation_claim_origin": origin,
        "candidate_op_family": candidate.get("op_family"),
        "suggested_edge_kinds": candidate.get("suggested_edge_kinds"),
    }
    return RelationClaimOp(
        op="upsert",
        source_model_id=edge_op.source_model_id,
        target_model_id=edge_op.target_model_id,
        source_model_version_id=source_version_id,
        target_model_version_id=target_version_id,
        subject_ref={k: v for k, v in subject_ref.items() if v is not None},
        object_ref={k: v for k, v in object_ref.items() if v is not None},
        predicate=edge_op.edge_kind,
        edge_kind=edge_op.edge_kind,
        direction="source_to_target",
        endpoint_binding_status="bound",
        write_policy="accepted_edge" if accepted else "candidate",
        status="accepted" if accepted else "candidate",
        confidence=edge_op.confidence,
        weight=edge_op.weight
        if edge_op.weight is not None
        else _relation_claim_weight(edge_op.edge_kind, edge_op.confidence),
        binding_confidence=0.9,
        evidence_event_ids=evidence_events,
        evidence_model_ids=evidence_models,
        evidence_text=_trunc(proposed_text, 1000) or edge_op.explanation,
        explanation=edge_op.explanation,
        semantic_scope=semantic_scope,
        metadata={k: v for k, v in metadata.items() if v not in (None, [], {})},
    )


_GOVERNED_BATCH_RELATIONS = {
    "blocks", "depends_on", "enables", "supports", "causes", "influences",
    "predicts", "causal_influence", "dependency_constraint", "enablement",
    "predictive_indicator",
}


def _is_governed_batch_relation(edge_kind: str) -> bool:
    return edge_kind in _GOVERNED_BATCH_RELATIONS


def _relation_semantic_scope(candidate: dict[str, Any]) -> list[str]:
    return list(dict.fromkeys(
        str(value).strip()
        for value in candidate.get("semantic_scope") or ()
        if str(value).strip()
    ))


def _candidate_relation_endpoint_versions(
    candidate: dict[str, Any],
    source_model_id: UUID,
    target_model_id: UUID,
) -> tuple[UUID | None, UUID | None]:
    versions = candidate.get("endpoint_model_versions") or {}
    if not isinstance(versions, dict):
        versions = {}
    source = _first_uuid([
        candidate.get("source_model_version_id"),
        versions.get(str(source_model_id)),
    ])
    target = _first_uuid([
        candidate.get("target_model_version_id"),
        versions.get(str(target_model_id)),
    ])
    return source, target


def _relation_claim_op_from_relation_hinted_batch_decision(
    candidate: dict[str, Any],
    decision: BatchMemoryCandidateDecision,
    *,
    claim_placeholder: UUID | None,
) -> tuple[RelationClaimOp | None, str]:
    if decision.operation in {"act", "no_op"}:
        return None, ""
    if candidate.get("candidate_kind") == "synthesis":
        relation = _supported_synthesis_relation(candidate, decision)
        if relation is None:
            return None, "synthesis relation lacks exact accepted endpoints"
        source_id = UUID(relation["source_model_id"])
        target_id = UUID(relation["target_model_id"])
        evidence_events = _uuid_values(relation.get("evidence_event_ids") or ())
        members = _situation_member_ids(candidate, decision)
        if not evidence_events or not {source_id, target_id}.issubset(members):
            return None, "synthesis relation is not coupled to its opener and members"
        confidence = min(1.0, max(0.05, float(decision.confidence)))
        return RelationClaimOp(
            op="upsert",
            source_model_id=source_id,
            target_model_id=target_id,
            source_model_version_id=UUID(relation["source_model_version_id"]),
            target_model_version_id=UUID(relation["target_model_version_id"]),
            subject_ref={"kind": "model", "model_id": str(source_id),
                         "candidate_id": decision.candidate_id},
            object_ref={"kind": "model", "model_id": str(target_id),
                        "candidate_id": decision.candidate_id},
            predicate=str(relation["kind"]),
            edge_kind={
                "dependency_constraint": "blocks",
                "causal_influence": "causes",
                "predictive_indicator": "predicts",
            }[str(relation["kind"])],
            direction="source_to_target",
            endpoint_binding_status="bound",
            write_policy="accepted_edge",
            status="accepted",
            confidence=confidence,
            weight=_relation_claim_weight(str(relation["kind"]), confidence),
            binding_confidence=1.0,
            evidence_event_ids=evidence_events,
            evidence_model_ids=members,
            evidence_text=str(candidate.get("proposed_text") or ""),
            explanation=str(relation["mechanism"]),
            semantic_scope=_relation_semantic_scope(candidate),
            metadata={
                "memory_decision_candidate_id": decision.candidate_id,
                "relation_claim_origin": "accepted_composite_synthesis",
                "synthesis_contract": True,
                "atomic_with_synthesis": True,
            },
        ), ""
    if not _candidate_has_relation_hint(candidate):
        return None, ""

    edge_kind = _relation_hinted_batch_edge_kind(decision, candidate)
    if edge_kind not in EDGE_REGISTRY:
        return None, "edge_kind is not writable by compiled path"

    source_model_id = decision.source_model_id
    if source_model_id is None and decision.operation != "claim_update":
        source_model_id = claim_placeholder
    target_model_id = decision.target_model_id or _first_uuid(
        candidate.get("target_model_ids")
    )
    if source_model_id is None:
        for candidate_source in _candidate_model_ids(candidate):
            if candidate_source != target_model_id:
                source_model_id = candidate_source
                break
    if target_model_id is None:
        for candidate_target in _candidate_model_ids(candidate):
            if candidate_target != source_model_id:
                target_model_id = candidate_target
                break
    if source_model_id is None or target_model_id is None:
        return None, "missing concrete relation endpoints"
    if source_model_id == target_model_id:
        return None, "self-relation candidate"

    confidence_floor = _env_float(
        "THINK_COMPILED_BATCH_RELATION_HINT_MIN_CONFIDENCE",
        0.58,
    )
    if decision.confidence < confidence_floor:
        return None, "decision confidence below relation floor"

    evidence_models = _uuid_values(candidate.get("evidence_model_ids"))
    evidence_models.extend(_uuid_values(candidate.get("target_model_ids")))
    evidence_events = _uuid_values(candidate.get("source_observation_ids"))
    semantic_text = _trunc(
        " ".join(
            part
            for part in (
                decision.claim_text or "",
                decision.reason,
                str(candidate.get("proposed_text") or ""),
                str(candidate.get("answer_summary") or ""),
            )
            if str(part or "").strip()
        ),
        1000,
    )
    metadata = {
        "memory_decision_candidate_id": decision.candidate_id,
        "compiled_reasoning": True,
        "compiled_decision_confidence": decision.confidence,
        "memory_decision_family": candidate.get("op_family"),
        "relation_claim_origin": "compiled_batch_relation_hint",
        "suggested_edge_kinds": candidate.get("suggested_edge_kinds"),
    }
    return (
        RelationClaimOp(
            op="upsert",
            source_model_id=source_model_id,
            target_model_id=target_model_id,
            subject_ref={
                "kind": "model",
                "model_id": str(source_model_id),
                "candidate_id": decision.candidate_id,
            },
            object_ref={
                "kind": "model",
                "model_id": str(target_model_id),
                "candidate_id": decision.candidate_id,
            },
            predicate=edge_kind,
            edge_kind=edge_kind,
            direction="source_to_target",
            endpoint_binding_status="bound",
            write_policy="candidate",
            status="candidate",
            confidence=float(decision.confidence),
            weight=_relation_claim_weight(edge_kind, float(decision.confidence)),
            binding_confidence=0.85,
            evidence_event_ids=_dedupe_uuids(evidence_events),
            evidence_model_ids=_dedupe_uuids(evidence_models),
            evidence_text=semantic_text,
            explanation=_trunc(decision.reason, 1000),
            metadata={k: v for k, v in metadata.items() if v not in (None, [], {})},
        ),
        "",
    )


def _candidate_has_relation_hint(candidate: dict[str, Any]) -> bool:
    if candidate.get("op_family") == "edge_insert":
        return True
    return bool(candidate.get("suggested_edge_kinds"))


def _relation_hinted_batch_edge_kind(
    decision: BatchMemoryCandidateDecision,
    candidate: dict[str, Any],
) -> str:
    inferred = _default_batch_edge_kind(decision, candidate)
    if inferred != "supports":
        return inferred
    for hint in candidate.get("suggested_edge_kinds") or ():
        edge_kind = str(hint or "").strip()
        if edge_kind in EDGE_REGISTRY and edge_kind not in {
            "supports",
            "same_issue_as",
            "analogous_to",
            "co_occurs_with",
            "alternative_to",
        }:
            return edge_kind
    return inferred


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
        (
            ("block", "blocked", "dependency", "depends", "constraint", "waiting"),
            "blocks",
        ),
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
    if trigger.kind != "T4" or trigger.subkind != "latent_relationship_candidate":
        return None
    candidates = _relationship_candidates_from_trigger(trigger)
    if not candidates:
        return None
    if any(candidate.get("candidate_kind") == "situation" for candidate in candidates):
        return None

    max_candidates = _env_int("THINK_COMPILED_RELATIONSHIP_MAX_CANDIDATES", 8)
    if len(candidates) > max_candidates:
        return None
    gated_decisions: list[RelationshipCandidateDecision] = []
    llm_candidates: list[dict[str, Any]] = []
    for candidate in candidates:
        gate_decision = _gate_relationship_candidate_before_llm(candidate)
        if gate_decision is None:
            llm_candidates.append(candidate)
        else:
            gated_decisions.append(gate_decision)

    system = (
        "You adjudicate pre-truth relationship candidates for the Think "
        "process. Decide only the relation outcome for each listed candidate. "
        "Use accept only when the candidate and model cards show a specific, "
        "useful, non-duplicative relationship that can become an accepted edge. "
        "Use needs_review/candidate when the relation is valuable but endpoint, "
        "direction, or mechanism confidence is not high enough for graph truth. "
        "Use ontology_gap for edge_type candidates whose proposed kind should "
        "stay in the ontology workflow. Use no_edge/noise for weak overlap, "
        "topical similarity, speculative topology, or distractors. Do not author "
        "claims, actions, resources, or free-form graph mutations."
    )
    user = _build_relationship_candidate_user_prompt(
        trigger,
        bundle,
        llm_candidates,
        gated_count=len(gated_decisions),
    )
    return CompiledRelationshipCandidateRequest(
        system=system,
        user=user,
        candidates=tuple(candidates),
        llm_candidate_ids=tuple(
            candidate_id
            for candidate in llm_candidates
            if (candidate_id := _coerce_uuid(candidate.get("id"))) is not None
        ),
        gated_decisions=tuple(gated_decisions),
    )


def _build_relationship_candidate_user_prompt(
    trigger: TriggerContext,
    bundle: ContextBundle,
    candidates: list[dict[str, Any]],
    *,
    gated_count: int = 0,
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
        f"pre_llm_gated_candidate_count: {gated_count}",
        "For every candidate_id listed below, return exactly one decision: "
        "accept, needs_review, candidate, ontology_gap, no_edge, noise, or reject.",
        "Accept means code will emit an edge using only the candidate's "
        "existing source_model_id, target_model_id, and edge_kind.",
        "Needs_review/candidate means code will preserve a relation claim but "
        "will not write accepted graph truth.",
        "Ontology_gap means code will preserve a proposed edge kind through the "
        "ontology workflow.",
        "No_edge/noise/reject means no accepted graph mutation is applied for "
        "that candidate.",
        "</compiled_relationship_candidate_task>",
        "<candidate_decision_rules>",
        "accept only if the source and target are concrete existing Models",
        "accept only if edge_kind is already named on the candidate",
        "accept only if the explanation names an actual mechanism, dependency, "
        "prediction link, contradiction, or blocking/enabling relation",
        "reject if evidence is merely co-occurrence, shared topic, shared actor, "
        "or similar pressure without a durable relation",
        "use ontology_gap for edge_type candidates that carry concrete example "
        "endpoints and a proposed edge kind",
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
                "    structural_evidence: " f"{_trunc(_jsonish(structural), 1200)}"
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


def _gate_relationship_candidate_before_llm(
    candidate: dict[str, Any],
) -> RelationshipCandidateDecision | None:
    candidate_id = _coerce_uuid(candidate.get("id"))
    if candidate_id is None:
        return None
    candidate_kind = candidate.get("candidate_kind")
    if candidate_kind == "edge_type":
        return RelationshipCandidateDecision(
            candidate_id=candidate_id,
            decision="needs_review",
            confidence=max(0.5, _candidate_score(candidate)),
            reason="edge_type candidate is routed to ontology review without LLM",
        )
    if candidate_kind != "edge":
        return RelationshipCandidateDecision(
            candidate_id=candidate_id,
            decision="noise",
            confidence=0.2,
            reason="candidate kind is not code-emittable by the lean relation path",
        )

    source_model_id = _coerce_uuid(candidate.get("source_model_id"))
    target_model_id = _coerce_uuid(candidate.get("target_model_id"))
    edge_kind = candidate.get("edge_kind")
    if source_model_id is None or target_model_id is None:
        return RelationshipCandidateDecision(
            candidate_id=candidate_id,
            decision="no_edge",
            confidence=0.9,
            reason="candidate lacks concrete model endpoints",
        )
    if source_model_id == target_model_id:
        return RelationshipCandidateDecision(
            candidate_id=candidate_id,
            decision="no_edge",
            confidence=0.95,
            reason="candidate would create a self-edge",
        )
    if not isinstance(edge_kind, str) or edge_kind.strip() not in EDGE_REGISTRY:
        return RelationshipCandidateDecision(
            candidate_id=candidate_id,
            decision="no_edge",
            confidence=0.85,
            reason="candidate lacks a writable registered edge kind",
        )

    score = _candidate_score(candidate)
    min_score = _env_float("THINK_RELATION_CANDIDATE_GATE_MIN_SCORE", 0.32)
    if score < min_score:
        return RelationshipCandidateDecision(
            candidate_id=candidate_id,
            decision="noise",
            confidence=max(0.5, 1.0 - score),
            reason="candidate score is below the pre-LLM usefulness floor",
        )

    edge_kind = edge_kind.strip()
    missing_structural = _structural_missing_fields(edge_kind, candidate)
    if missing_structural:
        return RelationshipCandidateDecision(
            candidate_id=candidate_id,
            decision="needs_review",
            confidence=max(0.55, score),
            reason="candidate is relation-like but missing structural evidence: "
            + ",".join(missing_structural),
        )

    generic_floor = _env_float(
        "THINK_RELATION_CANDIDATE_GENERIC_LLM_MIN_SCORE",
        0.72,
    )
    if edge_kind in _GENERIC_RELATION_KINDS and score < generic_floor:
        return RelationshipCandidateDecision(
            candidate_id=candidate_id,
            decision="no_edge",
            confidence=max(0.55, 1.0 - score),
            reason="generic similarity candidate is not decision-relevant enough",
        )
    return None


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


def _relation_claim_op_from_candidate_decision(
    candidate: dict[str, Any],
    decision: RelationshipCandidateDecision,
    *,
    write_policy: str,
    status: str,
) -> tuple[RelationClaimOp | None, str]:
    if candidate.get("candidate_kind") != "edge":
        return None, "candidate_kind is not edge"
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

    confidence = max(float(decision.confidence), _candidate_score(candidate))
    evidence_model_ids = _candidate_uuid_list(candidate, "evidence_model_ids")
    if not evidence_model_ids:
        evidence_model_ids = [source_model_id, target_model_id]
    metadata = {
        "relation_claim_origin": "compiled_relationship_candidate_decision",
        "relationship_candidate_id": str(decision.candidate_id),
        "compiled_reasoning": True,
        "compiled_decision": decision.decision,
        "compiled_decision_confidence": decision.confidence,
        "candidate_basis": candidate.get("basis"),
        "judgment_leverage_score": candidate.get("judgment_leverage_score"),
    }
    return (
        RelationClaimOp(
            op="upsert",
            source_model_id=source_model_id,
            target_model_id=target_model_id,
            subject_ref={
                "kind": "model",
                "model_id": str(source_model_id),
                "candidate_id": str(decision.candidate_id),
            },
            object_ref={
                "kind": "model",
                "model_id": str(target_model_id),
                "candidate_id": str(decision.candidate_id),
            },
            predicate=edge_kind,
            edge_kind=edge_kind,
            direction="source_to_target",
            endpoint_binding_status="bound",
            write_policy=write_policy,  # type: ignore[arg-type]
            status=status,  # type: ignore[arg-type]
            confidence=min(1.0, max(0.05, confidence)),
            weight=_relation_claim_weight(edge_kind, confidence),
            binding_confidence=0.9,
            evidence_event_ids=_candidate_uuid_list(candidate, "evidence_event_ids"),
            evidence_model_ids=_dedupe_uuids(evidence_model_ids),
            evidence_text=str(candidate.get("explanation") or ""),
            explanation=_edge_explanation(candidate, decision),
            metadata={k: v for k, v in metadata.items() if v not in (None, [], {})},
        ),
        "",
    )


def _ontology_gap_op_from_candidate_decision(
    candidate: dict[str, Any],
    decision: RelationshipCandidateDecision,
) -> tuple[OntologyGapOp | None, str]:
    if candidate.get("candidate_kind") != "edge_type":
        return None, "candidate_kind is not edge_type"
    proposed = candidate.get("proposed_proposition")
    if not isinstance(proposed, dict):
        proposed = {}
    proposed_edge_kind = (
        decision.proposed_edge_kind
        or proposed.get("proposed_edge_kind")
        or candidate.get("proposed_edge_kind")
    )
    if not isinstance(proposed_edge_kind, str) or not proposed_edge_kind.strip():
        return None, "missing proposed_edge_kind"

    source_model_id = _coerce_uuid(candidate.get("source_model_id"))
    target_model_id = _coerce_uuid(candidate.get("target_model_id"))
    member_model_ids = _candidate_uuid_list(candidate, "member_model_ids")
    if source_model_id is None and len(member_model_ids) >= 2:
        source_model_id = member_model_ids[0]
        target_model_id = member_model_ids[1]
    if source_model_id is None or target_model_id is None:
        return None, "missing example endpoints"
    if source_model_id == target_model_id:
        return None, "self-edge ontology example"

    dropped_dimensions = [
        str(item).strip()
        for item in (decision.dropped_dimensions or proposed.get("dropped_dimensions") or [])
        if str(item).strip()
    ]
    if not dropped_dimensions:
        dropped_dimensions = ["registered edge ontology loses this relation's semantics"]
    score = _candidate_score(candidate)
    return (
        OntologyGapOp(
            source_model_id=source_model_id,
            target_model_id=target_model_id,
            proposed_edge_kind=proposed_edge_kind.strip(),
            description=str(
                proposed.get("description")
                or candidate.get("description")
                or decision.reason
            ),
            relationship_summary=str(
                proposed.get("relationship_summary")
                or candidate.get("explanation")
                or decision.reason
            ),
            parent_kind=proposed.get("parent_kind") or proposed.get("nearest_existing_kind"),
            nearest_existing_kind=proposed.get("nearest_existing_kind"),
            directionality=proposed.get("directionality") or "unknown",
            inverse_label=proposed.get("inverse_label"),
            dropped_dimensions=dropped_dimensions,
            evidence_event_ids=_candidate_uuid_list(candidate, "evidence_event_ids"),
            evidence_model_ids=_candidate_uuid_list(candidate, "evidence_model_ids"),
            confidence=max(float(decision.confidence), score),
            impact=_candidate_score_field(candidate, "impact_score", default=score),
            actionability=_candidate_score_field(
                candidate,
                "actionability_score",
                default=0.65,
            ),
            urgency=_candidate_score_field(candidate, "urgency_score", default=0.5),
            uncertainty=_candidate_score_field(
                candidate,
                "uncertainty_score",
                default=0.7,
            ),
            authority_required=_candidate_score_field(
                candidate,
                "authority_required_score",
                default=0.5,
            ),
            novelty=_candidate_score_field(candidate, "novelty_score", default=0.8),
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


def _candidate_score(candidate: dict[str, Any]) -> float:
    return _candidate_score_field(
        candidate,
        "judgment_leverage_score",
        default=_candidate_score_field(candidate, "confidence_score", default=0.5),
    )


def _candidate_score_field(
    candidate: dict[str, Any],
    key: str,
    *,
    default: float,
) -> float:
    value = candidate.get(key)
    try:
        return min(1.0, max(0.0, float(value)))
    except (TypeError, ValueError):
        return default


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
