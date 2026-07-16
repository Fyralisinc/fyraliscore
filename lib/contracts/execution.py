"""Pure workflow, work-fencing, and external-effect contracts.

These types preserve the semantic distinction between business workflow state,
runtime scheduling/leases, and externally observed effects.  They contain no
provider calls or persistence behavior; named appliers own those transitions.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .agency import AgencyWriteContext
from .kernel import canonical_sha256
from .runtime import ProcessingClass


class _ExecutionContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


class WorkflowRunState(StrEnum):
    PLANNED = "planned"
    ACTIVE = "active"
    BLOCKED = "blocked"
    SUSPENDED = "suspended"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"

    @property
    def terminal(self) -> bool:
        return self in {
            self.COMPLETED,
            self.FAILED,
            self.CANCELLED,
            self.EXPIRED,
        }


class TaskState(StrEnum):
    PLANNED = "planned"
    READY = "ready"
    IN_PROGRESS = "in_progress"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"
    EXPIRED = "expired"

    @property
    def terminal(self) -> bool:
        return self in {
            self.COMPLETED,
            self.FAILED,
            self.SKIPPED,
            self.CANCELLED,
            self.EXPIRED,
        }


class WorkflowRunSnapshot(_ExecutionContract):
    workflow_run_id: UUID
    tenant_id: UUID
    episode_id: UUID
    intervention_spec_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    workflow_spec_version_ref: str = Field(min_length=1)
    state: WorkflowRunState
    authorization_decision_id: UUID
    authorization_decision_version: int = Field(default=1, ge=1)
    prerequisite_refs: tuple[str, ...] = ()
    required_task_ids: tuple[UUID, ...] = ()
    completion_predicate: str = Field(min_length=1)
    completion_evidence_refs: tuple[str, ...] = ()
    transition_reason: str = Field(min_length=1)
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def completion_is_evidenced(self) -> Self:
        if self.updated_at < self.created_at:
            raise ValueError("workflow update cannot precede creation")
        if self.state is WorkflowRunState.COMPLETED and not self.completion_evidence_refs:
            raise ValueError("completed workflow requires predicate evidence")
        if len(self.required_task_ids) != len(set(self.required_task_ids)):
            raise ValueError("workflow required task ids must be unique")
        return self

    @property
    def snapshot_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class WorkflowRunCommand(_ExecutionContract):
    context: AgencyWriteContext
    expected_version: int = Field(ge=0)
    snapshot: WorkflowRunSnapshot

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.snapshot.tenant_id != self.context.tenant_id:
            raise ValueError("workflow command tenant mismatch")
        if self.snapshot.updated_at != self.context.issued_at:
            raise ValueError("workflow transition cannot be backdated")
        if self.expected_version == 0 and (
            self.snapshot.state is not WorkflowRunState.PLANNED
            or self.snapshot.created_at != self.snapshot.updated_at
        ):
            raise ValueError("new workflow runs begin planned at creation time")
        self.context.require_writer(
            owner="AgencyStateApplier", responsibility="workflow_run"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class TaskSnapshot(_ExecutionContract):
    task_id: UUID
    tenant_id: UUID
    workflow_run_id: UUID
    episode_id: UUID
    intervention_spec_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    task_kind: str = Field(min_length=1)
    state: TaskState
    prerequisite_task_ids: tuple[UUID, ...] = ()
    target_grounding_refs: tuple[str, ...] = Field(min_length=1)
    authorization_decision_id: UUID
    authorization_decision_version: int = Field(default=1, ge=1)
    external_effect_required: bool = False
    effect_attempt_id: UUID | None = None
    execution_receipt_id: UUID | None = None
    completion_evidence_refs: tuple[str, ...] = ()
    transition_reason: str = Field(min_length=1)
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def terminal_state_has_real_evidence(self) -> Self:
        if self.updated_at < self.created_at:
            raise ValueError("task update cannot precede creation")
        if self.task_id in self.prerequisite_task_ids:
            raise ValueError("task cannot depend on itself")
        if len(self.prerequisite_task_ids) != len(set(self.prerequisite_task_ids)):
            raise ValueError("task prerequisites must be unique")
        if self.state is TaskState.COMPLETED:
            if not self.completion_evidence_refs:
                raise ValueError("completed task requires completion evidence")
            if self.external_effect_required and not (
                self.effect_attempt_id and self.execution_receipt_id
            ):
                raise ValueError(
                    "external-effect task completion requires attempt and receipt"
                )
        if not self.external_effect_required and (
            self.effect_attempt_id or self.execution_receipt_id
        ):
            raise ValueError("pure-computation task cannot claim an external effect")
        return self

    @property
    def snapshot_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class TaskCommand(_ExecutionContract):
    context: AgencyWriteContext
    expected_version: int = Field(ge=0)
    snapshot: TaskSnapshot

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.snapshot.tenant_id != self.context.tenant_id:
            raise ValueError("task command tenant mismatch")
        if self.snapshot.updated_at != self.context.issued_at:
            raise ValueError("task transition cannot be backdated")
        if self.expected_version == 0 and (
            self.snapshot.state is not TaskState.PLANNED
            or self.snapshot.created_at != self.snapshot.updated_at
        ):
            raise ValueError("new tasks begin planned at creation time")
        self.context.require_writer(owner="AgencyStateApplier", responsibility="task")
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class WorkObligationState(StrEnum):
    REGISTERED = "registered"
    ELIGIBLE = "eligible"
    DEFERRED = "deferred"
    SUPPRESSED = "suppressed"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    LEASED = "leased"
    COMPLETED = "completed"
    NO_OP = "no_op"
    RETRY_WAIT = "retry_wait"
    QUARANTINED = "quarantined"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    LEASE_LOST = "lease_lost"
    REDRIVE_AUTHORIZED = "redrive_authorized"
    SUPERSEDED_BY_NEW_GENERATION = "superseded_by_new_generation"
    OWNER_TERMINALIZATION_PENDING = "owner_terminalization_pending"
    EXHAUSTED = "exhausted"
    ESCALATED = "escalated"

    @property
    def terminal(self) -> bool:
        return self in {
            self.SUPPRESSED,
            self.REJECTED,
            self.CANCELLED,
            self.EXPIRED,
            self.COMPLETED,
            self.NO_OP,
            self.SUPERSEDED_BY_NEW_GENERATION,
            self.EXHAUSTED,
            self.ESCALATED,
        }


class WorkObligation(_ExecutionContract):
    obligation_id: UUID
    lineage_id: UUID
    tenant_id: UUID
    generation: int = Field(ge=1)
    parent_obligation_id: UUID | None = None
    semantic_dedupe_key: str = Field(min_length=1)
    causal_parent_ref: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    target_object_type: str = Field(min_length=1)
    target_object_id: UUID
    owner_writer_id: str = Field(min_length=1)
    purpose: str = Field(min_length=1)
    risk_tier: str = Field(min_length=1)
    expected_value: float
    correctness_priority: float = Field(ge=0.0, le=1.0)
    intent_relevance: float = Field(ge=0.0, le=1.0)
    uncertainty_reduction_estimate: float = Field(ge=0.0, le=1.0)
    minimum_processing_class: ProcessingClass
    maximum_processing_class: ProcessingClass
    economic_envelope_ref: str = Field(min_length=1)
    maximum_attempts: int = Field(ge=1)
    deadline: datetime
    generation_depth: int = Field(ge=0)
    terminal_condition: str = Field(min_length=1)
    effect_possible: bool
    governing_criterion_refs: tuple[str, ...] = ()
    attention_governance_binding_refs: tuple[str, ...] = ()
    registered_at: datetime

    @field_validator("deadline", "registered_at")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def generation_and_envelope_are_closed(self) -> Self:
        if self.deadline <= self.registered_at:
            raise ValueError("work deadline must follow registration")
        if self.generation == 1 and self.parent_obligation_id is not None:
            raise ValueError("first work generation cannot name a parent")
        if self.generation > 1 and self.parent_obligation_id is None:
            raise ValueError("successor work generation requires its exact parent")
        if self.minimum_processing_class.rank > self.maximum_processing_class.rank:
            raise ValueError("work processing-class range is inverted")
        if self.generation_depth != self.generation - 1:
            raise ValueError("work generation depth must follow generation number")
        return self

    @property
    def obligation_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class WorkObligationRegistrationCommand(_ExecutionContract):
    context: AgencyWriteContext
    obligation: WorkObligation

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.obligation.tenant_id != self.context.tenant_id:
            raise ValueError("work registration tenant mismatch")
        if self.obligation.registered_at != self.context.issued_at:
            raise ValueError("work registration cannot be backdated")
        self.context.require_writer(
            owner="WorkLedgerApplier", responsibility="work_obligation"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class WorkDecision(_ExecutionContract):
    decision_id: UUID
    tenant_id: UUID
    obligation_id: UUID
    obligation_generation: int = Field(ge=1)
    from_state: WorkObligationState
    to_state: WorkObligationState
    selected_processing_class: ProcessingClass
    policy_version_ref: str = Field(min_length=1)
    why_no_cheaper_class_is_safe: str = Field(min_length=1)
    useful_safe_fate_ref: str | None = None
    next_eligible_at: datetime | None = None
    wake_predicate: str | None = None
    reason: str = Field(min_length=1)
    decided_at: datetime

    @field_validator("next_eligible_at", "decided_at")
    @classmethod
    def times_are_aware(cls, value: datetime | None, info) -> datetime | None:
        return _aware(value, field_name=info.field_name) if value else None

    @model_validator(mode="after")
    def decision_is_explicit(self) -> Self:
        if self.to_state is WorkObligationState.DEFERRED and not (
            self.next_eligible_at or self.wake_predicate
        ):
            raise ValueError("deferred work requires a wake time or predicate")
        if self.to_state in {
            WorkObligationState.SUPPRESSED,
            WorkObligationState.REJECTED,
        } and not self.useful_safe_fate_ref:
            raise ValueError("suppressed or rejected work requires UsefulSafeFate")
        return self

    @property
    def decision_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class WorkDecisionCommand(_ExecutionContract):
    context: AgencyWriteContext
    expected_version: int = Field(ge=1)
    decision: WorkDecision

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.decision.tenant_id != self.context.tenant_id:
            raise ValueError("work decision tenant mismatch")
        if self.decision.decided_at != self.context.issued_at:
            raise ValueError("work decision cannot be backdated")
        self.context.require_writer(
            owner="WorkLedgerApplier", responsibility="work_obligation"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class WorkStateTransition(_ExecutionContract):
    transition_id: UUID
    tenant_id: UUID
    obligation_id: UUID
    obligation_generation: int = Field(ge=1)
    from_state: WorkObligationState
    to_state: WorkObligationState
    reason: str = Field(min_length=1)
    result_evidence_refs: tuple[str, ...] = ()
    no_op_predicate_ref: str | None = None
    owner_terminal_result_ref: str | None = None
    next_eligible_at: datetime | None = None
    wake_predicate: str | None = None
    transitioned_at: datetime

    @field_validator("next_eligible_at", "transitioned_at")
    @classmethod
    def times_are_aware(cls, value: datetime | None, info) -> datetime | None:
        return _aware(value, field_name=info.field_name) if value else None

    @model_validator(mode="after")
    def terminal_claims_have_evidence(self) -> Self:
        if self.to_state is WorkObligationState.COMPLETED and not self.result_evidence_refs:
            raise ValueError("completed work requires result evidence")
        if self.to_state is WorkObligationState.NO_OP and not self.no_op_predicate_ref:
            raise ValueError("no-op work requires a checked predicate reference")
        if self.from_state is WorkObligationState.LEASED:
            raise ValueError("leased work must transition through its exact lease fence")
        if self.to_state is WorkObligationState.DEFERRED and not (
            self.next_eligible_at or self.wake_predicate
        ):
            raise ValueError("deferred work requires a wake time or predicate")
        return self


class WorkStateTransitionCommand(_ExecutionContract):
    context: AgencyWriteContext
    expected_version: int = Field(ge=1)
    transition: WorkStateTransition

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.transition.tenant_id != self.context.tenant_id:
            raise ValueError("work transition tenant mismatch")
        if self.transition.transitioned_at != self.context.issued_at:
            raise ValueError("work transition cannot be backdated")
        self.context.require_writer(
            owner="WorkLedgerApplier", responsibility="work_obligation"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class LeaseState(StrEnum):
    ACTIVE = "active"
    COMPLETED = "completed"
    RELEASED = "released"
    EXPIRED = "expired"
    REVOKED = "revoked"
    SUPERSEDED_BY_NEW_LEASE = "superseded_by_new_lease"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    TERMINAL = "terminal"

    @property
    def terminal(self) -> bool:
        return self is not self.ACTIVE


class LeaseToken(_ExecutionContract):
    lease_token_id: UUID
    tenant_id: UUID
    obligation_id: UUID
    obligation_generation: int = Field(ge=1)
    fence: int = Field(ge=1)
    attempt: int = Field(ge=1)
    owner_ref: str = Field(min_length=1)
    state: Literal[LeaseState.ACTIVE] = LeaseState.ACTIVE
    heartbeat_deadline: datetime
    expires_at: datetime
    effect_possible: bool
    granted_at: datetime

    @field_validator("heartbeat_deadline", "expires_at", "granted_at")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def lease_window_is_valid(self) -> Self:
        if not (self.granted_at < self.heartbeat_deadline <= self.expires_at):
            raise ValueError("lease heartbeat/expiry interval is invalid")
        return self

    @property
    def lease_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class LeaseHeartbeat(_ExecutionContract):
    heartbeat_id: UUID
    tenant_id: UUID
    lease_token_id: UUID
    obligation_id: UUID
    obligation_generation: int = Field(ge=1)
    fence: int = Field(ge=1)
    owner_ref: str = Field(min_length=1)
    expected_heartbeat_deadline: datetime
    extended_heartbeat_deadline: datetime
    lease_expires_at: datetime
    heartbeat_at: datetime

    @field_validator(
        "expected_heartbeat_deadline",
        "extended_heartbeat_deadline",
        "lease_expires_at",
        "heartbeat_at",
    )
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def heartbeat_extends_only_a_live_window(self) -> Self:
        if self.heartbeat_at >= self.expected_heartbeat_deadline:
            raise ValueError("heartbeat must arrive before its current deadline")
        if not (
            self.expected_heartbeat_deadline
            < self.extended_heartbeat_deadline
            <= self.lease_expires_at
        ):
            raise ValueError("heartbeat extension must advance within lease expiry")
        return self


class LeaseHeartbeatCommand(_ExecutionContract):
    context: AgencyWriteContext
    expected_lease_version: int = Field(ge=1)
    heartbeat: LeaseHeartbeat

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.heartbeat.tenant_id != self.context.tenant_id:
            raise ValueError("lease heartbeat tenant mismatch")
        if self.heartbeat.heartbeat_at != self.context.issued_at:
            raise ValueError("lease heartbeat cannot be backdated")
        self.context.require_writer(
            owner="WorkLedgerApplier", responsibility="work_obligation"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class LeaseTakeover(_ExecutionContract):
    takeover_id: UUID
    tenant_id: UUID
    obligation_id: UUID
    obligation_generation: int = Field(ge=1)
    predecessor_lease_token_id: UUID
    predecessor_fence: int = Field(ge=1)
    predecessor_attempt: int = Field(ge=1)
    predecessor_owner_ref: str = Field(min_length=1)
    predecessor_heartbeat_deadline: datetime
    successor: LeaseToken
    no_effect_evidence_refs: tuple[str, ...] = ()
    reason: str = Field(min_length=1)
    taken_over_at: datetime

    @field_validator("predecessor_heartbeat_deadline", "taken_over_at")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def successor_is_a_strictly_fenced_new_owner(self) -> Self:
        successor = self.successor
        if self.taken_over_at < self.predecessor_heartbeat_deadline:
            raise ValueError("lease takeover requires a missed heartbeat")
        if (
            successor.tenant_id != self.tenant_id
            or successor.obligation_id != self.obligation_id
            or successor.obligation_generation != self.obligation_generation
            or successor.granted_at != self.taken_over_at
        ):
            raise ValueError("takeover successor changed work identity or time")
        if successor.lease_token_id == self.predecessor_lease_token_id:
            raise ValueError("takeover requires a new lease token")
        if successor.fence != self.predecessor_fence + 1:
            raise ValueError("takeover successor fence must advance exactly once")
        if successor.attempt != self.predecessor_attempt + 1:
            raise ValueError("takeover successor attempt must advance exactly once")
        if successor.owner_ref == self.predecessor_owner_ref:
            raise ValueError("takeover requires a distinct worker owner")
        if successor.effect_possible and not self.no_effect_evidence_refs:
            raise ValueError("effect-capable takeover requires exact no-effect evidence")
        return self


class LeaseTakeoverCommand(_ExecutionContract):
    context: AgencyWriteContext
    expected_obligation_version: int = Field(ge=1)
    expected_predecessor_lease_version: int = Field(ge=1)
    takeover: LeaseTakeover

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.takeover.tenant_id != self.context.tenant_id:
            raise ValueError("lease takeover tenant mismatch")
        if self.takeover.taken_over_at != self.context.issued_at:
            raise ValueError("lease takeover cannot be backdated")
        self.context.require_writer(
            owner="WorkLedgerApplier", responsibility="work_obligation"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class LeaseGrantCommand(_ExecutionContract):
    context: AgencyWriteContext
    expected_obligation_version: int = Field(ge=1)
    lease: LeaseToken

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.lease.tenant_id != self.context.tenant_id:
            raise ValueError("lease grant tenant mismatch")
        if self.lease.granted_at != self.context.issued_at:
            raise ValueError("lease grant cannot be backdated")
        self.context.require_writer(
            owner="WorkLedgerApplier", responsibility="work_obligation"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class LeaseResolution(_ExecutionContract):
    lease_token_id: UUID
    tenant_id: UUID
    obligation_id: UUID
    obligation_generation: int = Field(ge=1)
    fence: int = Field(ge=1)
    from_state: Literal[LeaseState.ACTIVE] = LeaseState.ACTIVE
    to_lease_state: LeaseState
    to_work_state: WorkObligationState
    effect_may_have_occurred: bool
    result_evidence_refs: tuple[str, ...] = ()
    no_op_predicate_ref: str | None = None
    next_eligible_at: datetime | None = None
    reason: str = Field(min_length=1)
    resolved_at: datetime

    @field_validator("next_eligible_at", "resolved_at")
    @classmethod
    def times_are_aware(cls, value: datetime | None, info) -> datetime | None:
        return _aware(value, field_name=info.field_name) if value else None

    @model_validator(mode="after")
    def possible_effect_is_never_retried_blindly(self) -> Self:
        if self.to_lease_state is LeaseState.ACTIVE:
            raise ValueError("lease resolution must leave active state")
        if self.effect_may_have_occurred and self.to_work_state not in {
            WorkObligationState.RECONCILIATION_REQUIRED,
            WorkObligationState.COMPLETED,
            WorkObligationState.NO_OP,
            WorkObligationState.QUARANTINED,
            WorkObligationState.CANCELLED,
        }:
            raise ValueError(
                "possible external effect requires reconciliation or evidenced completion"
            )
        if self.to_work_state is WorkObligationState.COMPLETED and not (
            self.result_evidence_refs
        ):
            raise ValueError("completed lease result requires evidence")
        if self.to_work_state is WorkObligationState.NO_OP and not (
            self.no_op_predicate_ref
        ):
            raise ValueError("no-op lease result requires predicate evidence")
        if self.to_work_state is WorkObligationState.RETRY_WAIT and not (
            self.next_eligible_at
        ):
            raise ValueError("retry wait requires next eligible time")
        return self


class LeaseResolutionCommand(_ExecutionContract):
    context: AgencyWriteContext
    expected_obligation_version: int = Field(ge=1)
    expected_lease_version: int = Field(ge=1)
    resolution: LeaseResolution

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.resolution.tenant_id != self.context.tenant_id:
            raise ValueError("lease resolution tenant mismatch")
        if self.resolution.resolved_at != self.context.issued_at:
            raise ValueError("lease resolution cannot be backdated")
        self.context.require_writer(
            owner="WorkLedgerApplier", responsibility="work_obligation"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ActionAdapterCapabilities(_ExecutionContract):
    capability_id: UUID
    tenant_id: UUID
    capability_version: str = Field(min_length=1)
    adapter_name: str = Field(min_length=1)
    provider_name: str = Field(min_length=1)
    permitted_operations: frozenset[str] = Field(min_length=1)
    request_canonicalization_version: str = Field(min_length=1)
    idempotency_supported: bool
    idempotency_scope: str | None = None
    idempotency_retention_until: datetime | None = None
    reconciliation_supported: bool
    reconciliation_consistency_window_seconds: int | None = Field(default=None, ge=0)
    cancellation_supported: bool
    partial_effect_observable: bool
    compensation_supported: bool
    verified_at: datetime
    expires_at: datetime

    @field_validator("idempotency_retention_until", "verified_at", "expires_at")
    @classmethod
    def times_are_aware(cls, value: datetime | None, info) -> datetime | None:
        return _aware(value, field_name=info.field_name) if value else None

    @model_validator(mode="after")
    def claimed_provider_guarantees_are_complete(self) -> Self:
        if self.expires_at <= self.verified_at:
            raise ValueError("adapter capability expiry must follow verification")
        if self.idempotency_supported != bool(
            self.idempotency_scope and self.idempotency_retention_until
        ):
            raise ValueError(
                "idempotency support requires scope and retention, and vice versa"
            )
        if self.reconciliation_supported != (
            self.reconciliation_consistency_window_seconds is not None
        ):
            raise ValueError(
                "reconciliation support requires a consistency window, and vice versa"
            )
        return self

    @property
    def autonomous_repeat_safe(self) -> bool:
        return self.idempotency_supported or self.reconciliation_supported

    @property
    def capability_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class AdapterCapabilityRegistrationCommand(_ExecutionContract):
    context: AgencyWriteContext
    expected_version: int = Field(ge=0)
    capabilities: ActionAdapterCapabilities

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.capabilities.tenant_id != self.context.tenant_id:
            raise ValueError("adapter capability tenant mismatch")
        if self.capabilities.verified_at != self.context.issued_at:
            raise ValueError("adapter capability registration cannot be backdated")
        self.context.require_writer(
            owner="ExecutionLedgerApplier", responsibility="action_adapter_capability"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ExternalEffectState(StrEnum):
    RESERVED = "reserved"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    DISPATCH_INTENT_RECORDED = "dispatch_intent_recorded"
    ACKNOWLEDGED = "acknowledged"
    REJECTED = "rejected"
    UNKNOWN = "unknown"
    RECONCILING = "reconciling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PARTIALLY_EXECUTED = "partially_executed"
    RECONCILED_NO_EFFECT = "reconciled_no_effect"
    TERMINAL_PARTIAL = "terminal_partial"
    COMPENSATION_PROPOSED = "compensation_proposed"
    COMPENSATION_AUTHORIZED = "compensation_authorized"
    COMPENSATION_REJECTED = "compensation_rejected"
    COMPENSATION_EXPIRED = "compensation_expired"
    COMPENSATION_ATTEMPT_LINKED = "compensation_attempt_linked"
    COMPENSATED = "compensated"
    COMPENSATION_FAILED = "compensation_failed"
    COMPENSATION_UNKNOWN = "compensation_unknown"
    COMPENSATION_RECONCILING = "compensation_reconciling"

    @property
    def terminal(self) -> bool:
        return self in {
            self.CANCELLED,
            self.EXPIRED,
            self.REJECTED,
            self.SUCCEEDED,
            self.FAILED,
            self.RECONCILED_NO_EFFECT,
            self.TERMINAL_PARTIAL,
            self.COMPENSATED,
            self.COMPENSATION_FAILED,
            self.COMPENSATION_REJECTED,
            self.COMPENSATION_EXPIRED,
        }


class ExternalEffectAttempt(_ExecutionContract):
    effect_attempt_id: UUID
    lineage_id: UUID
    tenant_id: UUID
    generation: int = Field(ge=1)
    prior_attempt_id: UUID | None = None
    compensates_effect_attempt_id: UUID | None = None
    episode_id: UUID
    task_id: UUID
    intervention_spec_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    authorization_decision_id: UUID
    authorization_decision_version: int = Field(default=1, ge=1)
    capability_id: UUID
    capability_version: str = Field(min_length=1)
    capability_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    operation: str = Field(min_length=1)
    canonical_request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    provider_idempotency_key: str = Field(min_length=1)
    target_grounding_refs: tuple[str, ...] = Field(min_length=1)
    live_precondition_refs: tuple[str, ...] = Field(min_length=1)
    work_obligation_id: UUID
    work_obligation_generation: int = Field(ge=1)
    lease_token_id: UUID
    lease_fence: int = Field(ge=1)
    dispatch_deadline: datetime
    reconciliation_owner_ref: str = Field(min_length=1)
    compensation_policy_ref: str = Field(min_length=1)
    duplicate_or_unknown_risk_authorization_ref: str | None = None
    state: Literal[ExternalEffectState.RESERVED] = ExternalEffectState.RESERVED
    reserved_at: datetime

    @field_validator("dispatch_deadline", "reserved_at")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def attempt_generation_is_explicit(self) -> Self:
        if self.dispatch_deadline <= self.reserved_at:
            raise ValueError("effect dispatch deadline must follow reservation")
        if self.generation == 1 and self.prior_attempt_id is not None:
            raise ValueError("first effect attempt cannot name a prior attempt")
        if self.generation > 1 and self.prior_attempt_id is None:
            raise ValueError("later effect attempt requires exact prior attempt")
        if self.compensates_effect_attempt_id == self.effect_attempt_id:
            raise ValueError("effect attempt cannot compensate itself")
        return self

    @property
    def attempt_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class EffectReservationCommand(_ExecutionContract):
    context: AgencyWriteContext
    attempt: ExternalEffectAttempt

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.attempt.tenant_id != self.context.tenant_id:
            raise ValueError("effect reservation tenant mismatch")
        if self.attempt.reserved_at != self.context.issued_at:
            raise ValueError("effect reservation cannot be backdated")
        self.context.require_writer(
            owner="ExecutionLedgerApplier", responsibility="external_effect"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class EffectObservation(_ExecutionContract):
    receipt_id: UUID
    tenant_id: UUID
    effect_attempt_id: UUID
    from_state: ExternalEffectState
    to_state: ExternalEffectState
    reason: str = Field(min_length=1)
    provider_observation_refs: tuple[str, ...] = ()
    external_state_evidence_refs: tuple[str, ...] = ()
    compensation_intervention_spec_digest: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    compensation_authorization_ref: str | None = None
    compensation_authorization_decision_id: UUID | None = None
    compensation_attempt_id: UUID | None = None
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="observed_at")

    @model_validator(mode="after")
    def material_effect_claims_have_observation(self) -> Self:
        evidence_required = {
            ExternalEffectState.ACKNOWLEDGED,
            ExternalEffectState.REJECTED,
            ExternalEffectState.SUCCEEDED,
            ExternalEffectState.FAILED,
            ExternalEffectState.PARTIALLY_EXECUTED,
            ExternalEffectState.RECONCILED_NO_EFFECT,
            ExternalEffectState.COMPENSATED,
            ExternalEffectState.COMPENSATION_FAILED,
            ExternalEffectState.COMPENSATION_UNKNOWN,
        }
        if self.to_state in evidence_required and not (
            self.provider_observation_refs or self.external_state_evidence_refs
        ):
            raise ValueError("effect observation requires provider or external evidence")
        if self.to_state is ExternalEffectState.COMPENSATION_PROPOSED and not (
            self.compensation_intervention_spec_digest
        ):
            raise ValueError("compensation proposal requires exact intervention spec")
        if self.to_state in {
            ExternalEffectState.COMPENSATION_REJECTED,
            ExternalEffectState.COMPENSATION_EXPIRED,
        } and not self.external_state_evidence_refs:
            raise ValueError(
                "compensation proposal terminal fate requires exact review evidence"
            )
        spec_states = {
            ExternalEffectState.COMPENSATION_PROPOSED,
            ExternalEffectState.COMPENSATION_AUTHORIZED,
        }
        if self.compensation_intervention_spec_digest and self.to_state not in spec_states:
            raise ValueError(
                "compensation spec digest is valid only on proposal/authorization"
            )
        if self.to_state is ExternalEffectState.COMPENSATION_ATTEMPT_LINKED and not (
            self.compensation_attempt_id
        ):
            raise ValueError("compensation link requires separate effect attempt")
        if self.compensation_attempt_id and self.to_state is not (
            ExternalEffectState.COMPENSATION_ATTEMPT_LINKED
        ):
            raise ValueError("compensation attempt id is valid only on link transition")
        if self.to_state is ExternalEffectState.COMPENSATION_AUTHORIZED and not (
            self.compensation_authorization_ref
            and self.compensation_authorization_decision_id
            and self.compensation_intervention_spec_digest
            and self.compensation_authorization_ref
            == f"authorization-decision:{self.compensation_authorization_decision_id}"
        ):
            raise ValueError(
                "compensation authorization requires exact spec and decision"
            )
        if (
            self.compensation_authorization_ref
            or self.compensation_authorization_decision_id
        ) and self.to_state is not ExternalEffectState.COMPENSATION_AUTHORIZED:
            raise ValueError(
                "compensation authorization is valid only on authorization transition"
            )
        return self


class EffectTransitionCommand(_ExecutionContract):
    context: AgencyWriteContext
    expected_version: int = Field(ge=1)
    observation: EffectObservation

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.observation.tenant_id != self.context.tenant_id:
            raise ValueError("effect transition tenant mismatch")
        if self.observation.observed_at > self.context.issued_at:
            raise ValueError("effect observation cannot be future-dated")
        self.context.require_writer(
            owner="ExecutionLedgerApplier", responsibility="external_effect"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ExecutionReceipt(_ExecutionContract):
    receipt_id: UUID
    tenant_id: UUID
    effect_attempt_id: UUID
    effect_version: int = Field(ge=2)
    effect_state: ExternalEffectState
    canonical_request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    provider_idempotency_key: str = Field(min_length=1)
    requested: Literal[True] = True
    provider_accepted: bool | None
    externally_observed: bool
    partial: bool
    reconciled: bool
    compensated: bool
    provider_observation_refs: tuple[str, ...] = ()
    external_state_evidence_refs: tuple[str, ...] = ()
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="observed_at")

    @property
    def receipt_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


_WORKFLOW_RUN_TRANSITIONS = {
    WorkflowRunState.PLANNED: frozenset(
        {
            WorkflowRunState.ACTIVE,
            WorkflowRunState.BLOCKED,
            WorkflowRunState.CANCELLED,
            WorkflowRunState.EXPIRED,
        }
    ),
    WorkflowRunState.ACTIVE: frozenset(
        {
            WorkflowRunState.SUSPENDED,
            WorkflowRunState.COMPLETED,
            WorkflowRunState.FAILED,
            WorkflowRunState.CANCELLED,
            WorkflowRunState.EXPIRED,
        }
    ),
    WorkflowRunState.BLOCKED: frozenset(
        {
            WorkflowRunState.ACTIVE,
            WorkflowRunState.FAILED,
            WorkflowRunState.CANCELLED,
            WorkflowRunState.EXPIRED,
        }
    ),
    WorkflowRunState.SUSPENDED: frozenset(
        {
            WorkflowRunState.ACTIVE,
            WorkflowRunState.FAILED,
            WorkflowRunState.CANCELLED,
            WorkflowRunState.EXPIRED,
        }
    ),
}

_TASK_TRANSITIONS = {
    TaskState.PLANNED: frozenset(
        {
            TaskState.READY,
            TaskState.BLOCKED,
            TaskState.SKIPPED,
            TaskState.CANCELLED,
            TaskState.EXPIRED,
        }
    ),
    TaskState.READY: frozenset(
        {
            TaskState.IN_PROGRESS,
            TaskState.BLOCKED,
            TaskState.SKIPPED,
            TaskState.CANCELLED,
            TaskState.EXPIRED,
        }
    ),
    TaskState.IN_PROGRESS: frozenset(
        {
            TaskState.COMPLETED,
            TaskState.FAILED,
            TaskState.BLOCKED,
            TaskState.CANCELLED,
            TaskState.EXPIRED,
        }
    ),
    TaskState.BLOCKED: frozenset(
        {
            TaskState.READY,
            TaskState.SKIPPED,
            TaskState.CANCELLED,
            TaskState.EXPIRED,
        }
    ),
}

_WORK_OBLIGATION_TRANSITIONS = {
    WorkObligationState.REGISTERED: frozenset(
        {
            WorkObligationState.ELIGIBLE,
            WorkObligationState.DEFERRED,
            WorkObligationState.SUPPRESSED,
            WorkObligationState.REJECTED,
            WorkObligationState.CANCELLED,
            WorkObligationState.EXPIRED,
        }
    ),
    WorkObligationState.DEFERRED: frozenset(
        {
            WorkObligationState.ELIGIBLE,
            WorkObligationState.DEFERRED,
            WorkObligationState.SUPPRESSED,
            WorkObligationState.CANCELLED,
            WorkObligationState.EXPIRED,
        }
    ),
    WorkObligationState.ELIGIBLE: frozenset(
        {
            WorkObligationState.LEASED,
            WorkObligationState.DEFERRED,
            WorkObligationState.SUPPRESSED,
            WorkObligationState.REJECTED,
            WorkObligationState.CANCELLED,
            WorkObligationState.EXPIRED,
        }
    ),
    WorkObligationState.LEASED: frozenset(
        {
            WorkObligationState.COMPLETED,
            WorkObligationState.NO_OP,
            WorkObligationState.RETRY_WAIT,
            WorkObligationState.QUARANTINED,
            WorkObligationState.RECONCILIATION_REQUIRED,
            WorkObligationState.LEASE_LOST,
            WorkObligationState.CANCELLED,
        }
    ),
    WorkObligationState.LEASE_LOST: frozenset(
        {
            WorkObligationState.ELIGIBLE,
            WorkObligationState.RECONCILIATION_REQUIRED,
            WorkObligationState.OWNER_TERMINALIZATION_PENDING,
            WorkObligationState.EXHAUSTED,
            WorkObligationState.ESCALATED,
        }
    ),
    WorkObligationState.RETRY_WAIT: frozenset(
        {
            WorkObligationState.ELIGIBLE,
            WorkObligationState.OWNER_TERMINALIZATION_PENDING,
            WorkObligationState.EXHAUSTED,
            WorkObligationState.EXPIRED,
        }
    ),
    WorkObligationState.QUARANTINED: frozenset(
        {
            WorkObligationState.REDRIVE_AUTHORIZED,
            WorkObligationState.OWNER_TERMINALIZATION_PENDING,
            WorkObligationState.EXHAUSTED,
            WorkObligationState.ESCALATED,
        }
    ),
    WorkObligationState.REDRIVE_AUTHORIZED: frozenset(
        {WorkObligationState.SUPERSEDED_BY_NEW_GENERATION}
    ),
    WorkObligationState.RECONCILIATION_REQUIRED: frozenset(
        {
            WorkObligationState.COMPLETED,
            WorkObligationState.NO_OP,
            WorkObligationState.RETRY_WAIT,
            WorkObligationState.QUARANTINED,
            WorkObligationState.OWNER_TERMINALIZATION_PENDING,
            WorkObligationState.EXHAUSTED,
            WorkObligationState.ESCALATED,
        }
    ),
    WorkObligationState.OWNER_TERMINALIZATION_PENDING: frozenset(
        {
            WorkObligationState.COMPLETED,
            WorkObligationState.NO_OP,
            WorkObligationState.RETRY_WAIT,
            WorkObligationState.QUARANTINED,
            WorkObligationState.EXHAUSTED,
            WorkObligationState.ESCALATED,
        }
    ),
}

_LEASE_TRANSITIONS = {
    LeaseState.ACTIVE: frozenset(
        {
            LeaseState.COMPLETED,
            LeaseState.RELEASED,
            LeaseState.EXPIRED,
            LeaseState.REVOKED,
            LeaseState.SUPERSEDED_BY_NEW_LEASE,
            LeaseState.RECONCILIATION_REQUIRED,
            LeaseState.TERMINAL,
        }
    )
}

_EFFECT_TRANSITIONS = {
    ExternalEffectState.RESERVED: frozenset(
        {
            ExternalEffectState.CANCELLED,
            ExternalEffectState.EXPIRED,
            ExternalEffectState.DISPATCH_INTENT_RECORDED,
        }
    ),
    ExternalEffectState.DISPATCH_INTENT_RECORDED: frozenset(
        {
            ExternalEffectState.ACKNOWLEDGED,
            ExternalEffectState.REJECTED,
            ExternalEffectState.UNKNOWN,
        }
    ),
    ExternalEffectState.UNKNOWN: frozenset({ExternalEffectState.RECONCILING}),
    ExternalEffectState.RECONCILING: frozenset(
        {
            ExternalEffectState.SUCCEEDED,
            ExternalEffectState.FAILED,
            ExternalEffectState.PARTIALLY_EXECUTED,
            ExternalEffectState.RECONCILED_NO_EFFECT,
        }
    ),
    ExternalEffectState.ACKNOWLEDGED: frozenset(
        {
            ExternalEffectState.SUCCEEDED,
            ExternalEffectState.FAILED,
            ExternalEffectState.PARTIALLY_EXECUTED,
            ExternalEffectState.UNKNOWN,
        }
    ),
    ExternalEffectState.PARTIALLY_EXECUTED: frozenset(
        {
            ExternalEffectState.TERMINAL_PARTIAL,
            ExternalEffectState.COMPENSATION_PROPOSED,
        }
    ),
    ExternalEffectState.COMPENSATION_PROPOSED: frozenset(
        {
            ExternalEffectState.COMPENSATION_AUTHORIZED,
            ExternalEffectState.COMPENSATION_REJECTED,
            ExternalEffectState.COMPENSATION_EXPIRED,
        }
    ),
    ExternalEffectState.COMPENSATION_AUTHORIZED: frozenset(
        {ExternalEffectState.COMPENSATION_ATTEMPT_LINKED}
    ),
    ExternalEffectState.COMPENSATION_ATTEMPT_LINKED: frozenset(
        {
            ExternalEffectState.COMPENSATED,
            ExternalEffectState.COMPENSATION_FAILED,
            ExternalEffectState.COMPENSATION_UNKNOWN,
        }
    ),
    ExternalEffectState.COMPENSATION_UNKNOWN: frozenset(
        {ExternalEffectState.COMPENSATION_RECONCILING}
    ),
    ExternalEffectState.COMPENSATION_RECONCILING: frozenset(
        {
            ExternalEffectState.COMPENSATED,
            ExternalEffectState.COMPENSATION_FAILED,
        }
    ),
}


def workflow_run_transition_allowed(
    current: WorkflowRunState | None,
    target: WorkflowRunState,
) -> bool:
    return (
        target is WorkflowRunState.PLANNED
        if current is None
        else target in _WORKFLOW_RUN_TRANSITIONS.get(current, frozenset())
    )


def task_transition_allowed(current: TaskState | None, target: TaskState) -> bool:
    return (
        target is TaskState.PLANNED
        if current is None
        else target in _TASK_TRANSITIONS.get(current, frozenset())
    )


def work_obligation_transition_allowed(
    current: WorkObligationState,
    target: WorkObligationState,
) -> bool:
    return target in _WORK_OBLIGATION_TRANSITIONS.get(current, frozenset())


def lease_transition_allowed(current: LeaseState, target: LeaseState) -> bool:
    return target in _LEASE_TRANSITIONS.get(current, frozenset())


def external_effect_transition_allowed(
    current: ExternalEffectState,
    target: ExternalEffectState,
) -> bool:
    return target in _EFFECT_TRANSITIONS.get(current, frozenset())


__all__ = [
    "ActionAdapterCapabilities",
    "AdapterCapabilityRegistrationCommand",
    "EffectObservation",
    "EffectReservationCommand",
    "EffectTransitionCommand",
    "ExecutionReceipt",
    "ExternalEffectAttempt",
    "ExternalEffectState",
    "LeaseGrantCommand",
    "LeaseHeartbeat",
    "LeaseHeartbeatCommand",
    "LeaseResolution",
    "LeaseResolutionCommand",
    "LeaseState",
    "LeaseTakeover",
    "LeaseTakeoverCommand",
    "LeaseToken",
    "TaskCommand",
    "TaskSnapshot",
    "TaskState",
    "WorkflowRunCommand",
    "WorkflowRunSnapshot",
    "WorkflowRunState",
    "WorkDecision",
    "WorkDecisionCommand",
    "WorkObligation",
    "WorkObligationRegistrationCommand",
    "WorkObligationState",
    "WorkStateTransition",
    "WorkStateTransitionCommand",
    "external_effect_transition_allowed",
    "lease_transition_allowed",
    "task_transition_allowed",
    "work_obligation_transition_allowed",
    "workflow_run_transition_allowed",
]
