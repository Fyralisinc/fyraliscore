"""services/reasoning/think/diff_schema.py — validated diff schema.

Spec §7 "Diff schema". This is what the LLM is asked to produce, and
what `validator.py` → `applier.py` consume.

Pydantic discriminated unions on `op` so the schema hint the LLM
sees is precise. The validator downstream adds falsifier adequacy /
threshold / trust-tier / region-containment checks on top of the
pure-Pydantic shape.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from lib.shared.types import ResourceTransactionType


# =====================================================================
# ClaimOp — Model insert / update / archive.
# =====================================================================


class ClaimOp(BaseModel):
    """
    A mutation over the Models surface.

    - op='insert': `entry` MUST be ModelCreate-compatible dict (the
      validator wraps it in a `ModelCreate`).
    - op='update': `model_id` required; `changes` is a shallow-merge
      dict of (column → new value). Allowed columns enumerated in
      `applier.py._ALLOWED_MODEL_UPDATE_COLUMNS`.
    - op='archive': `model_id` + `reason` required; reason is a
      `ModelArchiveReason` literal.
    """

    model_config = ConfigDict(extra="forbid")

    op: Literal["insert", "update", "archive"]
    # For insert:
    entry: dict[str, Any] | None = None
    # For update / archive:
    model_id: UUID | None = None
    changes: dict[str, Any] | None = None
    reason: str | None = None


# =====================================================================
# MemoryLifecycleOp — explicit reconciliation of existing memory.
# =====================================================================


MemoryLifecycleAction = Literal[
    "confirm",
    "falsify",
    "revise",
    "unchanged",
    "archive",
    "supersede",
]


class MemoryLifecycleOp(BaseModel):
    """
    A typed lifecycle decision over an existing Model.

    This is intentionally narrower than `claim_ops.update`: it says why an
    existing memory is being touched by new evidence. Apply compiles it into
    existing model update/archive machinery so lifecycle accountability is
    first-class without introducing a second model store.
    """

    model_config = ConfigDict(extra="forbid")

    op: Literal["reconcile"] = "reconcile"
    model_id: UUID
    action: MemoryLifecycleAction
    # All observations considered by the reconciliation decision.  This is
    # audit provenance, not authority to attach those observations to truth.
    evidence_event_ids: list[UUID] = Field(default_factory=list)
    # Exact claim-local observations authorized to become canonical support.
    # Keeping this separate prevents a lifecycle review over a transport batch
    # from laundering sibling, uncertainty, or distractor observations into a
    # Model's supporting_event_ids.
    claim_local_evidence_event_ids: list[UUID] = Field(default_factory=list)
    evidence_model_ids: list[UUID] = Field(default_factory=list)
    confidence_delta: float | None = None
    confidence: float | None = None
    resolution_outcome: bool | None = None
    rationale: str
    reason: str | None = None
    superseded_by_model_id: UUID | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


# =====================================================================
# ActOp — Goal / Commitment / Decision create, transition, edge adds.
# =====================================================================


# Enumerated subset of the spec's act-op vocabulary that we currently
# support end-to-end. Wave 5 can add more (delete_edge, ambition_change,
# etc.) when UI needs them.
ActOpKind = Literal[
    "create_goal",
    "update_goal",
    "transition_goal",
    "create_commitment",
    "transition_commitment",
    "create_decision",
    "transition_decision",
    "add_edge_contributes_to",
    "add_edge_depends_on",
    "add_edge_constrained_by",
]


class ActOp(BaseModel):
    """
    A mutation over the Acts surface.

    `confidence_basis` is the Model id whose confidence justifies the
    Act. `compute_threshold` (services/reasoning/think/thresholds.py) computes
    the minimum confidence; the validator rejects the op if
    basis.confidence < threshold.

    `entity` holds the operation-specific payload:
      - create_*:      the row to insert (fields mirror the repo signature)
      - update_*:      { id, ...changes }
      - transition_*:  { id, new_state, resolved_by_event_ids? }
      - add_edge_*:    { commitment_id, goal_id, ... } per edge kind
    """

    model_config = ConfigDict(extra="forbid")

    op: ActOpKind
    confidence_basis: UUID | None = None
    entity: dict[str, Any] = Field(default_factory=dict)


# =====================================================================
# ResourceOp — Resource create / update / deploy / release / transaction.
# =====================================================================


ResourceOpKind = Literal[
    "create",
    "transaction",
    "deploy",
    "release",
    "update",
]


class ResourceOp(BaseModel):
    """
    A mutation over the Resources surface.

    - op='create':     `payload` is the create kwargs (kind / identity /
                        current_value / ...).
    - op='update':     `resource_id` + `patch`.
    - op='transaction': `resource_id` + `kind` ('acquire'/'deploy'/...)
                        + `delta` (jsonb).
    - op='deploy':     `resource_id` + `commitment_id` + `quantity`.
    - op='release':    `resource_id` + `commitment_id` [+ `actual_quantity`].
    """

    model_config = ConfigDict(extra="forbid")

    op: ResourceOpKind
    resource_id: UUID | None = None
    commitment_id: UUID | None = None
    payload: dict[str, Any] | None = None
    patch: dict[str, Any] | None = None
    kind: ResourceTransactionType | None = None  # for op='transaction'
    delta: dict[str, Any] | None = None
    quantity: dict[str, Any] | None = None
    actual_quantity: dict[str, Any] | None = None


# =====================================================================
# EdgeOp — first-class Model graph mutations.
# =====================================================================


class EdgeOp(BaseModel):
    """
    A mutation over the Model-to-Model memory graph.

    `add` creates or reconfirms a typed relationship through EdgesRepo.
    `retire` marks an active edge inert without deleting audit history.
    """

    model_config = ConfigDict(extra="forbid")

    op: Literal["add", "retire"]
    source_model_id: UUID
    target_model_id: UUID
    edge_kind: str
    weight: float | None = None
    confidence: float = 1.0
    evidence_event_ids: list[UUID] = Field(default_factory=list)
    evidence_model_ids: list[UUID] = Field(default_factory=list)
    explanation: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    review_status: Literal[
        "accepted", "candidate", "needs_review", "disputed", "rejected", "retired"
    ] = "accepted"
    detected_by: str | None = None
    decay_after: datetime | None = None
    expires_at: datetime | None = None
    reason: str | None = None


# =====================================================================
# RelationClaimOp — first-class relation-bearing facts.
# =====================================================================


class RelationClaimOp(BaseModel):
    """
    A first-class relation-bearing write plan.

    These ops persist `relation_claims` rows. When endpoints are concrete and
    policy permits, apply also creates the corresponding `model_edges` row in
    the same transaction. This keeps relation extraction, endpoint binding,
    adjudication, and edge creation in one auditable lifecycle.
    """

    model_config = ConfigDict(extra="forbid")

    op: Literal["upsert"] = "upsert"
    id: UUID | None = None
    source_model_id: UUID | None = None
    target_model_id: UUID | None = None
    subject_ref: dict[str, Any] = Field(default_factory=dict)
    object_ref: dict[str, Any] = Field(default_factory=dict)
    predicate: str
    edge_kind: str
    direction: Literal[
        "source_to_target",
        "target_to_source",
        "symmetric",
        "unknown",
    ] = "source_to_target"
    endpoint_binding_status: Literal[
        "bound",
        "partially_bound",
        "unbound",
        "ambiguous",
    ] = "unbound"
    write_policy: Literal[
        "accepted_edge",
        "candidate",
        "needs_review",
        "no_edge",
    ] = "candidate"
    status: Literal[
        "active",
        "accepted",
        "candidate",
        "needs_review",
        "rejected",
        "retired",
    ] = "active"
    confidence: float = 0.5
    weight: float | None = None
    binding_confidence: float = 0.0
    evidence_event_ids: list[UUID] = Field(default_factory=list)
    evidence_model_ids: list[UUID] = Field(default_factory=list)
    evidence_text: str | None = None
    explanation: str | None = None
    temporal_bounds: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


# =====================================================================
# RelationFrameOp — N-ary relation frames with typed participants.
# =====================================================================


class RelationFrameParticipantOp(BaseModel):
    """One model bound into a typed role inside an N-ary relation frame."""

    model_config = ConfigDict(extra="forbid")

    model_id: UUID
    role: str
    binding_confidence: float = 0.5
    cardinality_group: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RelationFrameOp(BaseModel):
    """
    A multi-model semantic relation frame.

    Frames are persisted as relation_instances plus relation_participants.
    Accepted/projectable frames can deterministically compile into multiple
    model_edges while preserving the frame as the source of truth.
    """

    model_config = ConfigDict(extra="forbid")

    op: Literal["upsert"] = "upsert"
    id: UUID | None = None
    relation_kind: str
    participants: list[RelationFrameParticipantOp] = Field(default_factory=list)
    participant_binding_status: Literal[
        "bound",
        "partially_bound",
        "unbound",
        "ambiguous",
    ] = "unbound"
    write_policy: Literal[
        "project_edges",
        "candidate",
        "needs_review",
        "no_projection",
    ] = "candidate"
    status: Literal[
        "active",
        "candidate",
        "accepted",
        "needs_review",
        "disputed",
        "rejected",
        "retired",
    ] = "candidate"
    confidence: float = 0.5
    evidence_event_ids: list[UUID] = Field(default_factory=list)
    evidence_model_ids: list[UUID] = Field(default_factory=list)
    evidence_text: str | None = None
    explanation: str | None = None
    temporal_bounds: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


# =====================================================================
# OntologyGapOp — pre-truth edge-type proposals.
# =====================================================================


class OntologyGapOp(BaseModel):
    """
    A proposal for a relationship the current edge ontology cannot represent.

    These ops do NOT create `model_edges`. They persist an inspectable
    `relationship_candidates(candidate_kind='edge_type')` row that can be used
    as retrieval structure immediately via `parent_kind` / `nearest_existing_kind`,
    then promoted later through an ontology/registry workflow.
    """

    model_config = ConfigDict(extra="forbid")

    op: Literal["propose_edge_type"] = "propose_edge_type"
    source_model_id: UUID
    target_model_id: UUID
    proposed_edge_kind: str
    description: str
    relationship_summary: str
    parent_kind: str | None = None
    nearest_existing_kind: str | None = None
    directionality: Literal["directed", "symmetric", "unknown"] = "unknown"
    inverse_label: str | None = None
    dropped_dimensions: list[str] = Field(default_factory=list)
    evidence_event_ids: list[UUID] = Field(default_factory=list)
    evidence_model_ids: list[UUID] = Field(default_factory=list)
    confidence: float = 0.6
    impact: float = 0.75
    actionability: float = 0.65
    urgency: float = 0.5
    uncertainty: float = 0.7
    authority_required: float = 0.5
    novelty: float = 1.0


# =====================================================================
# OpenQuestionOp — unresolved uncertainty attached to a Model.
# =====================================================================


class OpenQuestionOp(BaseModel):
    """
    A mutation over the Model open-question facet.

    Open questions are not proposition kinds. They attach to a concrete Model
    and describe evidence that would materially improve, falsify, scope, or
    project that belief. Post-commit workers can turn open questions into T4
    system-wide search triggers.
    """

    model_config = ConfigDict(extra="forbid")

    op: Literal["insert", "resolve", "archive"]
    # For insert, `id` may predeclare the question row id. For resolve/archive,
    # `question_id` is preferred and `id` is accepted as a compatibility alias.
    id: UUID | None = None
    question_id: UUID | None = None
    model_id: UUID | None = None
    question: str | None = None
    question_type: str = "evidence_gap"
    rationale: str | None = None
    priority: float = 0.5
    expected_resolution_signal: dict[str, Any] = Field(default_factory=dict)
    search_signature: dict[str, Any] = Field(default_factory=dict)
    source_model_ids: list[UUID] = Field(default_factory=list)
    resolution_model_id: UUID | None = None
    resolution_note: str | None = None
    status: Literal[
        "resolved",
        "stale",
        "superseded",
        "duplicate",
        "archived",
    ] | None = None


# =====================================================================
# FormationResolutionOp — explicit resolution of formation candidates.
# =====================================================================


FormationResolutionDecision = Literal[
    "formed",
    "updated",
    "deferred",
    "rejected",
    "already_covered",
]


class FormationResolutionOp(BaseModel):
    """
    A non-mutating resolution for a Model Formation Contract candidate.

    Formation candidates are generated from retrieved evidence before Think.
    This op records how Think resolved that obligation. Durable belief changes
    still flow through ordinary claim_ops or memory_lifecycle_ops; this object
    is accountability, not a second Model store.
    """

    model_config = ConfigDict(extra="forbid")

    op: Literal["resolve"] = "resolve"
    candidate_id: str
    resolution: FormationResolutionDecision
    rationale: str
    output_model_ids: list[UUID] = Field(default_factory=list)
    follow_up_question: str | None = None


# =====================================================================
# ValidatedDiff — the top-level container.
# =====================================================================


class ValidatedDiff(BaseModel):
    """
    A fully validated diff ready for apply. The LLM produces this shape
    directly (via `LLMProvider.structured(schema=ValidatedDiff)`); the
    validator re-checks each op and drops the ones that fail.

    `trigger_ref` MUST be the `trigger_id` from the trigger queue row.
    This is the idempotency key that `applied_triggers` is keyed on.
    """

    model_config = ConfigDict(extra="forbid")

    trigger_ref: UUID
    tenant_id: UUID
    claim_ops: list[ClaimOp] = Field(default_factory=list)
    memory_lifecycle_ops: list[MemoryLifecycleOp] = Field(default_factory=list)
    relation_claim_ops: list[RelationClaimOp] = Field(default_factory=list)
    relation_frame_ops: list[RelationFrameOp] = Field(default_factory=list)
    edge_ops: list[EdgeOp] = Field(default_factory=list)
    ontology_gap_ops: list[OntologyGapOp] = Field(default_factory=list)
    open_question_ops: list[OpenQuestionOp] = Field(default_factory=list)
    formation_resolutions: list[FormationResolutionOp] = Field(default_factory=list)
    act_ops: list[ActOp] = Field(default_factory=list)
    resource_ops: list[ResourceOp] = Field(default_factory=list)
    # Predictions that should be scheduled with the deadline resolver
    # post-commit. Must be ClaimOps with op='insert' and an
    # `evaluate_at` in their entry.
    new_predictions: list[ClaimOp] = Field(default_factory=list)
    # Freeform reasoning trace — stored on think_runs.ops_applied so the
    # LLM's chain-of-thought is reconstructable if debugging a bad run.
    reasoning_trace: str | None = None
    # Partial-accept bookkeeping: the validator keeps good ops and drops
    # bad ones rather than rejecting the whole diff. These fields let
    # reason.py record how many ops were dropped + why, without breaking
    # the surface the applier consumes.
    dropped_op_count: int = 0
    dropped_op_errors: list[str] = Field(default_factory=list)


# =====================================================================
# RawDiff — what the LLM produces and what deterministic handlers return
# =====================================================================


class RawDiff(BaseModel):
    """
    Pre-validation diff shape. Identical fields to ValidatedDiff but
    used to make the "before validation" stage explicit in type
    signatures. The LLM returns this; the validator converts it to a
    ValidatedDiff after filtering invalid ops.
    """

    model_config = ConfigDict(extra="forbid")

    trigger_ref: UUID
    tenant_id: UUID
    claim_ops: list[ClaimOp] = Field(default_factory=list)
    memory_lifecycle_ops: list[MemoryLifecycleOp] = Field(default_factory=list)
    relation_claim_ops: list[RelationClaimOp] = Field(default_factory=list)
    relation_frame_ops: list[RelationFrameOp] = Field(default_factory=list)
    edge_ops: list[EdgeOp] = Field(default_factory=list)
    ontology_gap_ops: list[OntologyGapOp] = Field(default_factory=list)
    open_question_ops: list[OpenQuestionOp] = Field(default_factory=list)
    formation_resolutions: list[FormationResolutionOp] = Field(default_factory=list)
    act_ops: list[ActOp] = Field(default_factory=list)
    resource_ops: list[ResourceOp] = Field(default_factory=list)
    new_predictions: list[ClaimOp] = Field(default_factory=list)
    reasoning_trace: str | None = None


class RawDiffClaimsOnly(BaseModel):
    """
    Compact LLM output shape for invocations where first-class graph edges
    are not available or not the target surface. It parses into `RawDiff`
    before validation/apply, with edge/act/resource/prediction buckets empty.
    """

    model_config = ConfigDict(extra="ignore")

    trigger_ref: UUID
    tenant_id: UUID
    claim_ops: list[ClaimOp] = Field(default_factory=list)
    formation_resolutions: list[FormationResolutionOp] = Field(default_factory=list)
    reasoning_trace: str | None = None


__all__ = [
    "ClaimOp",
    "MemoryLifecycleAction",
    "MemoryLifecycleOp",
    "ActOp",
    "ActOpKind",
    "EdgeOp",
    "RelationClaimOp",
    "RelationFrameOp",
    "RelationFrameParticipantOp",
    "OntologyGapOp",
    "OpenQuestionOp",
    "FormationResolutionDecision",
    "FormationResolutionOp",
    "ResourceOp",
    "ResourceOpKind",
    "ValidatedDiff",
    "RawDiff",
    "RawDiffClaimsOnly",
]
