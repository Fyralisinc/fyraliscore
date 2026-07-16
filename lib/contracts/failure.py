"""Pure failure, quarantine, and semantic-owner terminalization contracts."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .agency import AgencyWriteContext
from .execution import WorkObligationState, work_obligation_transition_allowed
from .kernel import canonical_sha256


class _FailureContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


class FailureClassification(StrEnum):
    TRANSIENT_DEPENDENCY = "transient_dependency"
    POISON_INPUT = "poison_input"
    INVALID_INPUT = "invalid_input"
    AUTHORITY_OR_POLICY = "authority_or_policy"
    OPTIMISTIC_CONFLICT = "optimistic_conflict"
    PROVIDER_UNKNOWN = "provider_unknown"
    PARTIAL_EFFECT = "partial_effect"
    INVARIANT_VIOLATION = "invariant_violation"
    OWNER_REJECTED = "owner_rejected"
    UNCLASSIFIED = "unclassified"


class EffectUncertainty(StrEnum):
    NONE = "none"
    POSSIBLE = "possible"
    KNOWN_EFFECT = "known_effect"
    KNOWN_NO_EFFECT = "known_no_effect"


class FailureState(StrEnum):
    DETECTED = "detected"
    CLASSIFIED = "classified"
    RETRY_SCHEDULED = "retry_scheduled"
    QUARANTINED = "quarantined"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    TERMINAL_REJECTED = "terminal_rejected"
    REDRIVE_AUTHORIZED = "redrive_authorized"
    REDRIVE_IN_PROGRESS = "redrive_in_progress"
    OWNER_TERMINALIZATION_PENDING = "owner_terminalization_pending"
    RESOLVED = "resolved"
    EXHAUSTED = "exhausted"
    ESCALATED = "escalated"

    @property
    def terminal(self) -> bool:
        return self in {
            self.TERMINAL_REJECTED,
            self.RESOLVED,
            self.EXHAUSTED,
            self.ESCALATED,
        }


class FailureRecord(_FailureContract):
    failure_id: UUID
    lineage_id: UUID
    tenant_id: UUID
    generation: int = Field(ge=1)
    parent_failure_id: UUID | None = None
    work_obligation_id: UUID
    work_obligation_generation: int = Field(ge=1)
    causal_operation: str = Field(min_length=1)
    classification: FailureClassification
    owner_writer_id: str = Field(min_length=1)
    semantic_owner_writer_id: str = Field(min_length=1)
    target_object_type: str = Field(min_length=1)
    target_object_id: UUID
    original_semantic_idempotency_key: str = Field(min_length=1)
    attempt: int = Field(ge=1)
    maximum_attempts: int = Field(ge=1)
    deadline: datetime
    next_action: str = Field(min_length=1)
    next_eligible_at: datetime | None = None
    effect_uncertainty: EffectUncertainty
    remediation_evidence_refs: tuple[str, ...] = ()
    state: FailureState
    reason: str = Field(min_length=1)
    owner_terminalization_request_id: UUID | None = None
    created_at: datetime
    updated_at: datetime

    @field_validator("deadline", "next_eligible_at", "created_at", "updated_at")
    @classmethod
    def times_are_aware(cls, value: datetime | None, info) -> datetime | None:
        return _aware(value, field_name=info.field_name) if value else None

    @model_validator(mode="after")
    def lifecycle_is_closed(self) -> Self:
        if self.generation == 1 and self.parent_failure_id is not None:
            raise ValueError("first failure generation cannot name a parent")
        if self.generation > 1 and self.parent_failure_id is None:
            raise ValueError("successor failure generation requires a parent")
        if self.updated_at < self.created_at or self.created_at >= self.deadline:
            raise ValueError("failure creation/update interval is invalid")
        if self.attempt > self.maximum_attempts:
            raise ValueError("failure attempt cannot exceed its budget")
        if self.state is FailureState.RETRY_SCHEDULED and not self.next_eligible_at:
            raise ValueError("scheduled retry requires next eligible time")
        if self.state is FailureState.RECONCILIATION_REQUIRED and (
            self.effect_uncertainty is EffectUncertainty.NONE
        ):
            raise ValueError("reconciliation requires possible or known effect state")
        if self.state is FailureState.OWNER_TERMINALIZATION_PENDING and not (
            self.owner_terminalization_request_id
        ):
            raise ValueError("owner-terminalization pending requires exact request")
        if self.owner_terminalization_request_id and self.state is not (
            FailureState.OWNER_TERMINALIZATION_PENDING
        ):
            raise ValueError(
                "owner-terminalization request is valid only while pending"
            )
        return self

    @property
    def record_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class FailureRecordCommand(_FailureContract):
    context: AgencyWriteContext
    expected_version: int = Field(ge=0)
    record: FailureRecord

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.record.tenant_id != self.context.tenant_id:
            raise ValueError("failure command tenant mismatch")
        if self.record.updated_at != self.context.issued_at:
            raise ValueError("failure transition cannot be backdated")
        if self.expected_version == 0 and (
            self.record.state is not FailureState.DETECTED
            or self.record.created_at != self.record.updated_at
        ):
            raise ValueError("new failure records begin detected at creation time")
        self.context.require_writer(
            owner="WorkLedgerApplier", responsibility="failure_record"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class OwnerTerminalizationRequest(_FailureContract):
    request_id: UUID
    tenant_id: UUID
    failure_id: UUID
    failure_generation: int = Field(ge=1)
    from_failure_state: FailureState
    work_obligation_id: UUID
    work_obligation_generation: int = Field(ge=1)
    from_work_state: WorkObligationState
    semantic_owner_writer_id: str = Field(min_length=1)
    target_object_type: str = Field(min_length=1)
    target_object_id: UUID
    acceptable_owner_terminal_states: frozenset[str] = Field(min_length=1)
    terminal_reason: str = Field(min_length=1)
    requested_at: datetime

    @field_validator("requested_at")
    @classmethod
    def time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="requested_at")

    @model_validator(mode="after")
    def request_is_nonterminal(self) -> Self:
        if self.from_failure_state.terminal:
            raise ValueError("terminal failure cannot request owner terminalization")
        if self.from_work_state.terminal:
            raise ValueError("terminal work cannot request owner terminalization")
        if not work_obligation_transition_allowed(
            self.from_work_state,
            WorkObligationState.OWNER_TERMINALIZATION_PENDING,
        ):
            raise ValueError("work state cannot enter owner terminalization")
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class OwnerTerminalizationRequestCommand(_FailureContract):
    context: AgencyWriteContext
    expected_failure_version: int = Field(ge=1)
    expected_work_version: int = Field(ge=1)
    request: OwnerTerminalizationRequest

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.request.tenant_id != self.context.tenant_id:
            raise ValueError("owner-terminalization request tenant mismatch")
        if self.request.requested_at != self.context.issued_at:
            raise ValueError("owner-terminalization request cannot be backdated")
        self.context.require_writer(
            owner="WorkLedgerApplier", responsibility="failure_record"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class OwnerTerminalizationResolution(_FailureContract):
    resolution_id: UUID
    tenant_id: UUID
    request_id: UUID
    failure_id: UUID
    failure_generation: int = Field(ge=1)
    work_obligation_id: UUID
    work_obligation_generation: int = Field(ge=1)
    owner_command_result_id: UUID
    observed_owner_writer_id: str = Field(min_length=1)
    observed_owner_object_type: str = Field(min_length=1)
    observed_owner_object_id: UUID
    observed_owner_object_version: int = Field(ge=1)
    observed_owner_terminal_state: str = Field(min_length=1)
    to_failure_state: FailureState
    to_work_state: WorkObligationState
    reason: str = Field(min_length=1)
    resolved_at: datetime

    @field_validator("resolved_at")
    @classmethod
    def time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="resolved_at")

    @model_validator(mode="after")
    def resolution_is_terminal_or_explicit_retry(self) -> Self:
        allowed_failure_states = {
            FailureState.RESOLVED,
            FailureState.CLASSIFIED,
            FailureState.EXHAUSTED,
            FailureState.ESCALATED,
        }
        allowed_work_states = {
            WorkObligationState.COMPLETED,
            WorkObligationState.NO_OP,
            WorkObligationState.RETRY_WAIT,
            WorkObligationState.QUARANTINED,
            WorkObligationState.EXHAUSTED,
            WorkObligationState.ESCALATED,
        }
        if self.to_failure_state not in allowed_failure_states:
            raise ValueError("owner resolution has invalid failure fate")
        if self.to_work_state not in allowed_work_states:
            raise ValueError("owner resolution has invalid work fate")
        return self

    @property
    def resolution_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class OwnerTerminalizationResolutionCommand(_FailureContract):
    context: AgencyWriteContext
    expected_failure_version: int = Field(ge=1)
    expected_work_version: int = Field(ge=1)
    resolution: OwnerTerminalizationResolution

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.resolution.tenant_id != self.context.tenant_id:
            raise ValueError("owner-terminalization resolution tenant mismatch")
        if self.resolution.resolved_at != self.context.issued_at:
            raise ValueError("owner-terminalization resolution cannot be backdated")
        self.context.require_writer(
            owner="WorkLedgerApplier", responsibility="failure_record"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


_FAILURE_TRANSITIONS = {
    FailureState.DETECTED: frozenset(
        {
            FailureState.CLASSIFIED,
            FailureState.OWNER_TERMINALIZATION_PENDING,
            FailureState.EXHAUSTED,
            FailureState.ESCALATED,
        }
    ),
    FailureState.CLASSIFIED: frozenset(
        {
            FailureState.RETRY_SCHEDULED,
            FailureState.QUARANTINED,
            FailureState.RECONCILIATION_REQUIRED,
            FailureState.TERMINAL_REJECTED,
            FailureState.OWNER_TERMINALIZATION_PENDING,
            FailureState.EXHAUSTED,
            FailureState.ESCALATED,
        }
    ),
    FailureState.RETRY_SCHEDULED: frozenset(
        {
            FailureState.RESOLVED,
            FailureState.CLASSIFIED,
            FailureState.OWNER_TERMINALIZATION_PENDING,
            FailureState.EXHAUSTED,
            FailureState.ESCALATED,
        }
    ),
    FailureState.QUARANTINED: frozenset(
        {
            FailureState.REDRIVE_AUTHORIZED,
            FailureState.OWNER_TERMINALIZATION_PENDING,
            FailureState.EXHAUSTED,
            FailureState.ESCALATED,
        }
    ),
    FailureState.REDRIVE_AUTHORIZED: frozenset(
        {FailureState.REDRIVE_IN_PROGRESS}
    ),
    FailureState.REDRIVE_IN_PROGRESS: frozenset(
        {
            FailureState.RESOLVED,
            FailureState.CLASSIFIED,
            FailureState.OWNER_TERMINALIZATION_PENDING,
            FailureState.EXHAUSTED,
            FailureState.ESCALATED,
        }
    ),
    FailureState.RECONCILIATION_REQUIRED: frozenset(
        {
            FailureState.RESOLVED,
            FailureState.CLASSIFIED,
            FailureState.QUARANTINED,
            FailureState.OWNER_TERMINALIZATION_PENDING,
            FailureState.EXHAUSTED,
            FailureState.ESCALATED,
        }
    ),
    FailureState.OWNER_TERMINALIZATION_PENDING: frozenset(
        {
            FailureState.RESOLVED,
            FailureState.CLASSIFIED,
            FailureState.EXHAUSTED,
            FailureState.ESCALATED,
        }
    ),
}


def failure_transition_allowed(
    current: FailureState | None, target: FailureState
) -> bool:
    return (
        target is FailureState.DETECTED
        if current is None
        else target in _FAILURE_TRANSITIONS.get(current, frozenset())
    )


__all__ = [
    "EffectUncertainty",
    "FailureClassification",
    "FailureRecord",
    "FailureRecordCommand",
    "FailureState",
    "OwnerTerminalizationRequest",
    "OwnerTerminalizationRequestCommand",
    "OwnerTerminalizationResolution",
    "OwnerTerminalizationResolutionCommand",
    "failure_transition_allowed",
]
