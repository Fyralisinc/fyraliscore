"""Pure correction invalidation, repair-lineage, and convergence contracts."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .agency import AgencyWriteContext
from .execution import WorkObligationRegistrationCommand
from .kernel import WatermarkVector, canonical_sha256


class _RepairContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


class InvalidationKind(StrEnum):
    CORRECTION = "correction"
    REVOCATION = "revocation"
    DELETION = "deletion"


class DependencyFenceClass(StrEnum):
    READ_REJECT = "read_reject"
    READ_DEGRADED = "read_degraded"
    ACTION_FENCE = "action_fence"
    POLICY_FENCE = "policy_fence"
    DELETE_CONTENT = "delete_content"


class DependencyEdge(_RepairContract):
    edge_id: UUID
    tenant_id: UUID
    source_object_type: str = Field(min_length=1)
    source_object_id: UUID
    source_object_version: int = Field(ge=1)
    source_generation: int = Field(ge=1)
    dependent_object_type: str = Field(min_length=1)
    dependent_object_id: UUID
    dependent_object_version: int = Field(ge=1)
    dependency_kind: str = Field(min_length=1)
    material: bool
    fence_class: DependencyFenceClass
    authority_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    invalidation_keys: tuple[str, ...] = Field(min_length=1)
    declared_by_writer_id: str = Field(min_length=1)
    declared_at: datetime

    @field_validator("declared_at")
    @classmethod
    def time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="declared_at")

    @property
    def edge_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class InvalidationRequestRecord(_RepairContract):
    request_id: UUID
    tenant_id: UUID
    kind: InvalidationKind
    invalidation_epoch: int = Field(ge=1)
    source_writer_id: str = Field(min_length=1)
    source_object_type: str = Field(min_length=1)
    source_object_id: UUID
    predecessor_source_version: int = Field(ge=1)
    successor_source_version: int = Field(ge=1)
    successor_source_generation: int = Field(ge=1)
    correction_or_revocation_event_ref: str = Field(min_length=1)
    immediate_fence_ref: str = Field(min_length=1)
    dependency_snapshot_token: str = Field(min_length=1)
    required_dependency_kinds: tuple[str, ...] = Field(min_length=1)
    reason: str = Field(min_length=1)
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="created_at")

    @model_validator(mode="after")
    def source_version_advances(self) -> Self:
        if self.successor_source_version <= self.predecessor_source_version:
            raise ValueError("invalidation request requires a successor source version")
        if len(set(self.required_dependency_kinds)) != len(
            self.required_dependency_kinds
        ):
            raise ValueError("required dependency kinds must be unique")
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class RepairObligationState(StrEnum):
    OPEN = "open"
    WORK_REQUESTED = "work_requested"
    DISPATCHED = "dispatched"
    RECEIPT_PENDING = "receipt_pending"
    RETRY_WAIT = "retry_wait"
    ADJUDICATION_REQUIRED = "adjudication_required"
    REPAIRED = "repaired"
    NO_OP = "no_op"
    ADJUDICATED_RESIDUE = "adjudicated_residue"
    EXHAUSTED = "exhausted"
    ESCALATED = "escalated"
    SUPERSEDED_BY_NEW_GENERATION = "superseded_by_new_generation"

    @property
    def terminal(self) -> bool:
        return self in {
            self.REPAIRED,
            self.NO_OP,
            self.ADJUDICATED_RESIDUE,
            self.EXHAUSTED,
            self.ESCALATED,
            self.SUPERSEDED_BY_NEW_GENERATION,
        }


class RepairObligation(_RepairContract):
    obligation_id: UUID
    lineage_id: UUID
    tenant_id: UUID
    generation: int = Field(ge=1)
    parent_obligation_id: UUID | None = None
    invalidation_request_id: UUID
    invalidation_epoch: int = Field(ge=1)
    source_object_type: str = Field(min_length=1)
    source_object_id: UUID
    source_generation: int = Field(ge=1)
    dependent_object_type: str = Field(min_length=1)
    dependent_object_id: UUID
    dependent_object_version: int = Field(ge=1)
    dependency_kind: str = Field(min_length=1)
    fence_class: DependencyFenceClass
    required_dependent_writer_id: str = Field(min_length=1)
    required_dependent_transition: str = Field(min_length=1)
    expected_target_version: int = Field(ge=1)
    maximum_attempts: int = Field(ge=1)
    attempt: int = Field(default=0, ge=0)
    deadline: datetime
    residue_policy_ref: str = Field(min_length=1)
    state: RepairObligationState
    child_work_obligation_id: UUID | None = None
    dependent_command_result_id: UUID | None = None
    repair_receipt_id: UUID | None = None
    no_op_proof_refs: tuple[str, ...] = ()
    residue_declaration: str | None = None
    residue_authorization_ref: str | None = None
    continuing_fence_ref: str | None = None
    redrive_authorization_ref: str | None = None
    successor_obligation_id: UUID | None = None
    next_eligible_at: datetime | None = None
    reason: str = Field(min_length=1)
    created_at: datetime
    updated_at: datetime

    @field_validator("deadline", "next_eligible_at", "created_at", "updated_at")
    @classmethod
    def times_are_aware(cls, value: datetime | None, info) -> datetime | None:
        return _aware(value, field_name=info.field_name) if value else None

    @model_validator(mode="after")
    def lifecycle_claims_are_evidenced(self) -> Self:
        if self.generation == 1 and self.parent_obligation_id is not None:
            raise ValueError("first repair generation cannot name a parent")
        if self.generation > 1 and not (
            self.parent_obligation_id and self.redrive_authorization_ref
        ):
            raise ValueError(
                "successor repair generation requires a parent and authorization"
            )
        if self.updated_at < self.created_at or self.created_at >= self.deadline:
            raise ValueError("repair creation/update interval is invalid")
        if self.attempt > self.maximum_attempts:
            raise ValueError("repair attempt cannot exceed its budget")
        if self.state in {
            RepairObligationState.WORK_REQUESTED,
            RepairObligationState.DISPATCHED,
        } and not self.child_work_obligation_id:
            raise ValueError("requested/dispatched repair requires exact child work")
        if self.state is RepairObligationState.RECEIPT_PENDING and not (
            self.child_work_obligation_id and self.dependent_command_result_id
        ):
            raise ValueError(
                "receipt-pending repair requires child work and exact dependent result"
            )
        if self.state is RepairObligationState.RETRY_WAIT and not (
            self.next_eligible_at
        ):
            raise ValueError("repair retry requires next eligible time")
        if self.state is RepairObligationState.REPAIRED and not (
            self.dependent_command_result_id and self.repair_receipt_id
        ):
            raise ValueError("repaired fate requires owner result and repair receipt")
        if self.state is RepairObligationState.NO_OP and not (
            self.no_op_proof_refs and self.repair_receipt_id
        ):
            raise ValueError("repair no-op requires proof and receipt")
        if self.state is RepairObligationState.ADJUDICATED_RESIDUE and not all(
            (
                self.residue_declaration,
                self.residue_authorization_ref,
                self.continuing_fence_ref,
                self.repair_receipt_id,
            )
        ):
            raise ValueError(
                "adjudicated residue requires declaration, authority, fence and receipt"
            )
        if self.state is RepairObligationState.SUPERSEDED_BY_NEW_GENERATION and not (
            self.successor_obligation_id
        ):
            raise ValueError("superseded repair requires exact successor")
        return self

    @property
    def obligation_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class RepairObligationCommand(_RepairContract):
    context: AgencyWriteContext
    expected_version: int = Field(ge=0)
    obligation: RepairObligation
    child_work_registration: WorkObligationRegistrationCommand | None = None

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.obligation.tenant_id != self.context.tenant_id:
            raise ValueError("repair command tenant mismatch")
        if self.obligation.updated_at != self.context.issued_at:
            raise ValueError("repair transition cannot be backdated")
        if self.expected_version == 0 and (
            self.obligation.state is not RepairObligationState.OPEN
            or self.obligation.created_at != self.obligation.updated_at
        ):
            raise ValueError("new repair obligations begin open at creation time")
        self.context.require_writer(
            owner="RepairLedgerApplier", responsibility="repair_obligation"
        )
        if self.obligation.state is RepairObligationState.WORK_REQUESTED:
            registration = self.child_work_registration
            if (
                registration is None
                or self.obligation.child_work_obligation_id
                != registration.obligation.obligation_id
            ):
                raise ValueError(
                    "work-requested repair requires its exact child Work registration"
                )
        elif self.child_work_registration is not None:
            raise ValueError(
                "child Work registration is valid only on work-requested repair"
            )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class RepairReceipt(_RepairContract):
    receipt_id: UUID
    tenant_id: UUID
    repair_obligation_id: UUID
    repair_generation: int = Field(ge=1)
    invalidation_request_id: UUID
    invalidation_epoch: int = Field(ge=1)
    source_generation: int = Field(ge=1)
    dependent_object_type: str = Field(min_length=1)
    dependent_object_id: UUID
    predecessor_dependent_version: int = Field(ge=1)
    resulting_dependent_version: int = Field(ge=1)
    dependent_writer_id: str = Field(min_length=1)
    dependent_command_result_id: UUID | None = None
    child_work_command_result_id: UUID | None = None
    fate: RepairObligationState
    proof_refs: tuple[str, ...] = Field(min_length=1)
    residue_declaration: str | None = None
    residue_authorization_ref: str | None = None
    continuing_fence_ref: str | None = None
    completed_watermark: WatermarkVector
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="observed_at")

    @model_validator(mode="after")
    def fate_has_exact_proof(self) -> Self:
        allowed = {
            RepairObligationState.REPAIRED,
            RepairObligationState.NO_OP,
            RepairObligationState.ADJUDICATED_RESIDUE,
            RepairObligationState.EXHAUSTED,
            RepairObligationState.ESCALATED,
        }
        if self.fate not in allowed:
            raise ValueError("repair receipt requires a receipt-bearing fate")
        if self.fate is RepairObligationState.REPAIRED and not (
            self.dependent_command_result_id
            and self.resulting_dependent_version
            >= self.predecessor_dependent_version
        ):
            raise ValueError("repaired receipt requires exact owner result/version")
        if self.fate is RepairObligationState.ADJUDICATED_RESIDUE and not all(
            (
                self.residue_declaration,
                self.residue_authorization_ref,
                self.continuing_fence_ref,
            )
        ):
            raise ValueError("residue receipt requires declaration, authority and fence")
        if self.fate in {
            RepairObligationState.EXHAUSTED,
            RepairObligationState.ESCALATED,
        } and (
            self.child_work_command_result_id is None
            or f"agency-command-result:{self.child_work_command_result_id}"
            not in self.proof_refs
        ):
            raise ValueError(
                "exhausted or escalated repair requires exact terminal child work result"
            )
        return self

    @property
    def receipt_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class RepairReceiptCommand(_RepairContract):
    context: AgencyWriteContext
    expected_obligation_version: int = Field(ge=1)
    receipt: RepairReceipt

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.receipt.tenant_id != self.context.tenant_id:
            raise ValueError("repair receipt command tenant mismatch")
        if self.receipt.observed_at != self.context.issued_at:
            raise ValueError("repair receipt cannot be backdated")
        self.context.require_writer(
            owner="RepairLedgerApplier", responsibility="repair_obligation"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class RepairCoverageBasis(StrEnum):
    ORACLE_COMPLETE = "oracle_complete"
    INSTRUMENTED_CONTRACT_COMPLETE = "instrumented_contract_complete"
    KNOWN_EDGE_ONLY = "known_edge_only"


class RepairEpisodeState(StrEnum):
    INVALIDATION_REQUEST_OBSERVED = "invalidation_request_observed"
    INVALIDATION_OPENED = "invalidation_opened"
    SCANNING = "scanning"
    REPAIRING = "repairing"
    CONVERGED = "converged"
    CONVERGED_WITH_ADJUDICATED_RESIDUE = "converged_with_adjudicated_residue"
    ESCALATED = "escalated"

    @property
    def terminal(self) -> bool:
        return self in {
            self.CONVERGED,
            self.CONVERGED_WITH_ADJUDICATED_RESIDUE,
            self.ESCALATED,
        }


class RepairEpisode(_RepairContract):
    episode_id: UUID
    tenant_id: UUID
    invalidation_request_id: UUID
    invalidation_epoch: int = Field(ge=1)
    kind: InvalidationKind
    state: RepairEpisodeState
    coverage_basis: RepairCoverageBasis
    known_material_dependency_count: int = Field(ge=0)
    known_covered_dependency_count: int = Field(ge=0)
    oracle_material_dependency_count: int | None = Field(default=None, ge=0)
    oracle_covered_dependency_count: int | None = Field(default=None, ge=0)
    current_tail_fate_counts: dict[str, int]
    historical_generation_count: int = Field(ge=0)
    adjudicated_residue_count: int = Field(ge=0)
    unsafe_residue_refs: tuple[str, ...] = ()
    active_unsafe_lease_or_effect_refs: tuple[str, ...] = ()
    source_fence_active: bool
    snapshot_watermark: WatermarkVector
    catchup_watermark: WatermarkVector
    stable_scan_count: int = Field(ge=0)
    reason: str = Field(min_length=1)
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def convergence_is_complete_not_merely_empty(self) -> Self:
        if self.updated_at < self.created_at:
            raise ValueError("repair episode time regressed")
        if self.known_covered_dependency_count > self.known_material_dependency_count:
            raise ValueError("known repair coverage exceeds its denominator")
        oracle_pair = (
            self.oracle_material_dependency_count,
            self.oracle_covered_dependency_count,
        )
        if (oracle_pair[0] is None) != (oracle_pair[1] is None):
            raise ValueError("oracle repair denominator and numerator are paired")
        if oracle_pair[0] is not None and oracle_pair[1] > oracle_pair[0]:
            raise ValueError("oracle repair coverage exceeds its denominator")
        if self.state in {
            RepairEpisodeState.CONVERGED,
            RepairEpisodeState.CONVERGED_WITH_ADJUDICATED_RESIDUE,
        }:
            if self.coverage_basis is RepairCoverageBasis.KNOWN_EDGE_ONLY:
                raise ValueError("known-edge-only coverage cannot prove convergence")
            if self.known_covered_dependency_count != (
                self.known_material_dependency_count
            ):
                raise ValueError("convergence requires every known material dependent")
            if oracle_pair[0] is not None and oracle_pair[1] != oracle_pair[0]:
                raise ValueError("convergence requires every oracle material dependent")
            if not self.catchup_watermark.covers(self.snapshot_watermark):
                raise ValueError("convergence requires watermark catch-up")
            if self.stable_scan_count < 2:
                raise ValueError("convergence requires repeated stable scans")
            if not self.source_fence_active:
                raise ValueError("convergence requires the source fence")
            if self.unsafe_residue_refs or self.active_unsafe_lease_or_effect_refs:
                raise ValueError("unsafe residue or effects prevent convergence")
            disallowed = {
                RepairObligationState.OPEN.value,
                RepairObligationState.WORK_REQUESTED.value,
                RepairObligationState.DISPATCHED.value,
                RepairObligationState.RECEIPT_PENDING.value,
                RepairObligationState.RETRY_WAIT.value,
                RepairObligationState.ADJUDICATION_REQUIRED.value,
                RepairObligationState.EXHAUSTED.value,
                RepairObligationState.ESCALATED.value,
            }
            if any(self.current_tail_fate_counts.get(name, 0) for name in disallowed):
                raise ValueError("failed or nonterminal repair tail prevents convergence")
            if self.state is RepairEpisodeState.CONVERGED and (
                self.adjudicated_residue_count
                or self.current_tail_fate_counts.get(
                    RepairObligationState.ADJUDICATED_RESIDUE.value, 0
                )
            ):
                raise ValueError("plain convergence cannot contain residue")
            if self.state is RepairEpisodeState.CONVERGED_WITH_ADJUDICATED_RESIDUE:
                if self.adjudicated_residue_count <= 0:
                    raise ValueError("residue convergence requires declared residue")
                if self.current_tail_fate_counts.get(
                    RepairObligationState.ADJUDICATED_RESIDUE.value, 0
                ) != self.adjudicated_residue_count:
                    raise ValueError("residue count must equal adjudicated tails")
        return self

    @property
    def episode_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class RepairEpisodeCommand(_RepairContract):
    context: AgencyWriteContext
    expected_version: int = Field(ge=0)
    episode: RepairEpisode

    @model_validator(mode="after")
    def command_binds_writer(self) -> Self:
        if self.episode.tenant_id != self.context.tenant_id:
            raise ValueError("repair episode command tenant mismatch")
        if self.episode.updated_at != self.context.issued_at:
            raise ValueError("repair episode transition cannot be backdated")
        if self.expected_version == 0 and (
            self.episode.state is not (
                RepairEpisodeState.INVALIDATION_REQUEST_OBSERVED
            )
            or self.episode.created_at != self.episode.updated_at
        ):
            raise ValueError("new repair episode begins with observed request")
        self.context.require_writer(
            owner="RepairLedgerApplier", responsibility="repair_episode"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


_REPAIR_OBLIGATION_TRANSITIONS = {
    RepairObligationState.OPEN: frozenset(
        {
            RepairObligationState.WORK_REQUESTED,
            RepairObligationState.RECEIPT_PENDING,
            RepairObligationState.NO_OP,
            RepairObligationState.SUPERSEDED_BY_NEW_GENERATION,
        }
    ),
    RepairObligationState.WORK_REQUESTED: frozenset(
        {
            RepairObligationState.DISPATCHED,
            RepairObligationState.RECEIPT_PENDING,
            RepairObligationState.RETRY_WAIT,
            RepairObligationState.EXHAUSTED,
            RepairObligationState.ESCALATED,
            RepairObligationState.SUPERSEDED_BY_NEW_GENERATION,
        }
    ),
    RepairObligationState.DISPATCHED: frozenset(
        {
            RepairObligationState.RECEIPT_PENDING,
            RepairObligationState.RETRY_WAIT,
            RepairObligationState.EXHAUSTED,
            RepairObligationState.ESCALATED,
            RepairObligationState.SUPERSEDED_BY_NEW_GENERATION,
        }
    ),
    RepairObligationState.RECEIPT_PENDING: frozenset(
        {
            RepairObligationState.REPAIRED,
            RepairObligationState.NO_OP,
            RepairObligationState.ADJUDICATION_REQUIRED,
            RepairObligationState.RETRY_WAIT,
            RepairObligationState.EXHAUSTED,
            RepairObligationState.ESCALATED,
            RepairObligationState.SUPERSEDED_BY_NEW_GENERATION,
        }
    ),
    RepairObligationState.RETRY_WAIT: frozenset(
        {
            RepairObligationState.WORK_REQUESTED,
            RepairObligationState.EXHAUSTED,
            RepairObligationState.ESCALATED,
            RepairObligationState.SUPERSEDED_BY_NEW_GENERATION,
        }
    ),
    RepairObligationState.ADJUDICATION_REQUIRED: frozenset(
        {
            RepairObligationState.RECEIPT_PENDING,
            RepairObligationState.ADJUDICATED_RESIDUE,
            RepairObligationState.ESCALATED,
            RepairObligationState.SUPERSEDED_BY_NEW_GENERATION,
        }
    ),
    RepairObligationState.EXHAUSTED: frozenset(
        {RepairObligationState.SUPERSEDED_BY_NEW_GENERATION}
    ),
    RepairObligationState.ESCALATED: frozenset(
        {RepairObligationState.SUPERSEDED_BY_NEW_GENERATION}
    ),
}

_REPAIR_EPISODE_TRANSITIONS = {
    RepairEpisodeState.INVALIDATION_REQUEST_OBSERVED: frozenset(
        {RepairEpisodeState.INVALIDATION_OPENED}
    ),
    RepairEpisodeState.INVALIDATION_OPENED: frozenset(
        {RepairEpisodeState.SCANNING, RepairEpisodeState.ESCALATED}
    ),
    RepairEpisodeState.SCANNING: frozenset(
        {RepairEpisodeState.REPAIRING, RepairEpisodeState.ESCALATED}
    ),
    RepairEpisodeState.REPAIRING: frozenset(
        {
            RepairEpisodeState.CONVERGED,
            RepairEpisodeState.CONVERGED_WITH_ADJUDICATED_RESIDUE,
            RepairEpisodeState.ESCALATED,
        }
    ),
}


def repair_obligation_transition_allowed(
    current: RepairObligationState | None,
    target: RepairObligationState,
) -> bool:
    return (
        target is RepairObligationState.OPEN
        if current is None
        else target in _REPAIR_OBLIGATION_TRANSITIONS.get(current, frozenset())
    )


def repair_episode_transition_allowed(
    current: RepairEpisodeState | None,
    target: RepairEpisodeState,
) -> bool:
    return (
        target is RepairEpisodeState.INVALIDATION_REQUEST_OBSERVED
        if current is None
        else target in _REPAIR_EPISODE_TRANSITIONS.get(current, frozenset())
    )


__all__ = [
    "DependencyEdge",
    "DependencyFenceClass",
    "InvalidationKind",
    "InvalidationRequestRecord",
    "RepairCoverageBasis",
    "RepairEpisode",
    "RepairEpisodeCommand",
    "RepairEpisodeState",
    "RepairObligation",
    "RepairObligationCommand",
    "RepairObligationState",
    "RepairReceipt",
    "RepairReceiptCommand",
    "repair_episode_transition_allowed",
    "repair_obligation_transition_allowed",
]
