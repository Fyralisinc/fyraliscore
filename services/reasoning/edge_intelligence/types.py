"""Typed inputs and aggregate rows for the Edge Intelligence Kernel."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


RelationDirection = Literal[
    "source_to_target",
    "target_to_source",
    "symmetric",
    "unknown",
]


PairDirectionVote = Literal[
    "a_to_b",
    "b_to_a",
    "symmetric",
    "unknown",
]


EndpointBindingStatus = Literal[
    "bound",
    "partially_bound",
    "unbound",
    "ambiguous",
]


RelationWritePolicy = Literal[
    "accepted_edge",
    "candidate",
    "needs_review",
    "no_edge",
]


RelationClaimStatus = Literal[
    "active",
    "accepted",
    "candidate",
    "needs_review",
    "rejected",
    "retired",
]


RelationFrameStatus = Literal[
    "active",
    "candidate",
    "accepted",
    "needs_review",
    "disputed",
    "rejected",
    "retired",
]


RelationFrameWritePolicy = Literal[
    "project_edges",
    "candidate",
    "needs_review",
    "no_projection",
]


RelationProjectionStatus = Literal["active", "retired", "failed"]


def canonical_model_pair(left: UUID, right: UUID) -> tuple[UUID, UUID]:
    """Return a stable unordered pair for aggregate storage."""
    if left == right:
        raise ValueError("model pair evidence cannot use the same model twice")
    return (left, right) if str(left) < str(right) else (right, left)


def normalize_primitive(value: str | None) -> str:
    primitive = str(value or "UNKNOWN").strip().upper()
    return primitive or "UNKNOWN"


@dataclass(frozen=True)
class RelationEvidence:
    """One explicit relation-bearing claim before accepted graph truth."""

    tenant_id: UUID
    predicate: str
    id: UUID | None = None
    source_observation_id: UUID | None = None
    think_run_id: UUID | None = None
    source_model_id: UUID | None = None
    target_model_id: UUID | None = None
    subject_ref: dict[str, Any] = field(default_factory=dict)
    object_ref: dict[str, Any] = field(default_factory=dict)
    edge_kind_hint: str | None = None
    direction: RelationDirection = "unknown"
    scope_entities: list[dict[str, Any]] = field(default_factory=list)
    temporal_bounds: dict[str, Any] = field(default_factory=dict)
    evidence_text: str | None = None
    confidence: float = 0.5
    extraction_method: str = "unknown"
    status: str = "active"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RelationClaim:
    """A first-class relation write-plan before or alongside edge truth."""

    tenant_id: UUID
    predicate: str
    edge_kind: str
    id: UUID | None = None
    source_observation_id: UUID | None = None
    think_run_id: UUID | None = None
    source_model_id: UUID | None = None
    target_model_id: UUID | None = None
    subject_ref: dict[str, Any] = field(default_factory=dict)
    object_ref: dict[str, Any] = field(default_factory=dict)
    direction: RelationDirection = "source_to_target"
    endpoint_binding_status: EndpointBindingStatus = "unbound"
    write_policy: RelationWritePolicy = "candidate"
    status: RelationClaimStatus = "active"
    confidence: float = 0.5
    weight: float | None = None
    binding_confidence: float = 0.0
    evidence_event_ids: tuple[UUID, ...] = ()
    evidence_model_ids: tuple[UUID, ...] = ()
    evidence_text: str | None = None
    explanation: str | None = None
    accepted_edge_ids: tuple[UUID, ...] = ()
    temporal_bounds: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    decided_at: datetime | None = None


@dataclass(frozen=True)
class RelationParticipant:
    """One role-bound participant in an N-ary relation frame."""

    model_id: UUID
    role: str
    id: UUID | None = None
    relation_id: UUID | None = None
    tenant_id: UUID | None = None
    binding_confidence: float = 0.5
    cardinality_group: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class RelationFrame:
    """An N-ary semantic relation instance over multiple Models."""

    tenant_id: UUID
    relation_kind: str
    id: UUID | None = None
    source_observation_id: UUID | None = None
    think_run_id: UUID | None = None
    status: RelationFrameStatus = "candidate"
    participant_binding_status: EndpointBindingStatus = "unbound"
    write_policy: RelationFrameWritePolicy = "candidate"
    confidence: float = 0.5
    evidence_event_ids: tuple[UUID, ...] = ()
    evidence_model_ids: tuple[UUID, ...] = ()
    evidence_text: str | None = None
    explanation: str | None = None
    temporal_bounds: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    decided_at: datetime | None = None


@dataclass(frozen=True)
class RelationEdgeProjection:
    """Audit row linking a relation frame to one projected binary edge."""

    relation_id: UUID
    tenant_id: UUID
    edge_id: UUID
    projection_rule: str
    source_role: str
    target_role: str
    source_model_id: UUID
    target_model_id: UUID
    edge_kind: str
    id: UUID | None = None
    status: RelationProjectionStatus = "active"
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class PairEvidenceObservation:
    """Delta to fold into the aggregate evidence for one model pair."""

    tenant_id: UUID
    left_model_id: UUID
    right_model_id: UUID
    primitive: str = "UNKNOWN"
    co_retrieved_delta: int = 0
    co_used_valid_diff_delta: int = 0
    explicit_relation_delta: int = 0
    think_edge_op_delta: int = 0
    t4_accept_delta: int = 0
    t4_reject_delta: int = 0
    no_edge_delta: int = 0
    positive_outcome_delta: int = 0
    negative_outcome_delta: int = 0
    directed_source_model_id: UUID | None = None
    directed_target_model_id: UUID | None = None
    edge_kind_hint: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ModelPairEvidence:
    """Current aggregate row for one model pair under one primitive."""

    id: UUID
    tenant_id: UUID
    model_a_id: UUID
    model_b_id: UUID
    primitive: str
    co_retrieved_count: int = 0
    co_used_valid_diff_count: int = 0
    explicit_relation_count: int = 0
    think_edge_op_count: int = 0
    t4_accept_count: int = 0
    t4_reject_count: int = 0
    no_edge_count: int = 0
    positive_outcome_count: int = 0
    negative_outcome_count: int = 0
    direction_votes: dict[str, int] = field(default_factory=dict)
    edge_kind_votes: dict[str, int] = field(default_factory=dict)
    confidence_score: float = 0.0
    last_seen_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def strongest_edge_kind(self) -> str | None:
        return _max_vote(self.edge_kind_votes)

    @property
    def strongest_direction(self) -> PairDirectionVote:
        vote = _max_vote(self.direction_votes)
        if vote in {"a_to_b", "b_to_a", "symmetric", "unknown"}:
            return vote  # type: ignore[return-value]
        return "unknown"


def _max_vote(votes: dict[str, int]) -> str | None:
    if not votes:
        return None
    ordered = sorted(votes.items(), key=lambda item: (-int(item[1]), item[0]))
    return ordered[0][0] if ordered and int(ordered[0][1]) > 0 else None
