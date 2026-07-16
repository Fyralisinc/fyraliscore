"""C0a semantic, authority, versioning, and transactional kernel contracts."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import StrEnum
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _KernelContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        use_enum_values=False,
    )


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _canonicalize(value):
    if isinstance(value, dict):
        return {key: _canonicalize(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        normalized = [_canonicalize(item) for item in value]
        return sorted(normalized, key=lambda item: json.dumps(item, sort_keys=True))
    return value


def canonical_sha256(value) -> str:
    encoded = json.dumps(
        _canonicalize(value),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class ContentDomain(StrEnum):
    DESCRIPTIVE_WORLD = "descriptive_world"
    INSTITUTIONAL = "institutional"
    NORMATIVE_INTENT = "normative_intent"
    OPERATIONAL_ACTION = "operational_action"
    MEASUREMENT_OUTCOME = "measurement_outcome"
    SYSTEM_CONTROL = "system_control"


class EpistemicStatus(StrEnum):
    SOURCE_EMITTED = "source_emitted"
    SOURCE_ASSERTED = "source_asserted"
    AUTHORITATIVE_RECORD = "authoritative_record"
    INFERRED_BELIEF = "inferred_belief"
    HYPOTHESIS = "hypothesis"
    ADJUDICATED = "adjudicated"
    DERIVED = "derived"


class LifecycleStatus(StrEnum):
    PROPOSED = "proposed"
    ACTIVE = "active"
    DISPUTED = "disputed"
    SUPERSEDED = "superseded"
    AUTHORIZED = "authorized"
    EXECUTING = "executing"
    OBSERVED = "observed"
    SETTLED = "settled"
    RETIRED = "retired"


class RelationSemantics(StrEnum):
    NONE = "none"
    STRUCTURAL = "structural"
    CAUSAL = "causal"
    DEPENDENCY = "dependency"
    STATISTICAL = "statistical"
    NORMATIVE = "normative"
    AUTHORITY = "authority"
    SIMILARITY = "similarity"


class SemanticPlane(StrEnum):
    EVIDENCE = "evidence"
    GROUNDING = "grounding_perception"
    PHYSICAL_STATE = "physical_institutional_state"
    BRAIN = "brain"
    INTENT = "intent_agency"
    INQUIRY = "inquiry"
    CONTROL = "control_learning"
    DERIVED = "derived_view"


class ProvenanceAndConfidence(_KernelContract):
    direct_source_refs: tuple[str, ...] = ()
    derived_from_refs: tuple[str, ...] = ()
    counterevidence_refs: tuple[str, ...] = ()
    model_or_policy_version: str | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    confidence_distribution: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def distribution_is_normalized(self) -> Self:
        if any(value < 0.0 or value > 1.0 for value in self.confidence_distribution.values()):
            raise ValueError("confidence distribution values must lie in [0, 1]")
        if self.confidence_distribution and abs(sum(self.confidence_distribution.values()) - 1.0) > 1e-6:
            raise ValueError("confidence distribution must sum to one")
        return self


class SemanticAxes(_KernelContract):
    """Independent meanings that must never be encoded as one overloaded enum."""

    content_domain: ContentDomain
    epistemic_status: EpistemicStatus
    lifecycle_status: LifecycleStatus
    relation_semantics: RelationSemantics = RelationSemantics.NONE
    plane: SemanticPlane
    provenance_and_confidence: ProvenanceAndConfidence

    @model_validator(mode="after")
    def plane_does_not_claim_another_planes_authority(self) -> Self:
        if self.plane is SemanticPlane.EVIDENCE and self.epistemic_status is not EpistemicStatus.SOURCE_EMITTED:
            raise ValueError("Evidence preserves source-emitted material, not interpretations")
        if self.plane is SemanticPlane.DERIVED and self.epistemic_status is not EpistemicStatus.DERIVED:
            raise ValueError("derived views require derived epistemic status")
        if self.plane is SemanticPlane.BRAIN and self.epistemic_status in {
            EpistemicStatus.SOURCE_EMITTED,
            EpistemicStatus.AUTHORITATIVE_RECORD,
        }:
            raise ValueError("Brain cannot acquire evidence or authoritative-state status")
        if self.plane is SemanticPlane.INTENT and self.epistemic_status is EpistemicStatus.INFERRED_BELIEF:
            raise ValueError("inferred belief cannot become company intent")
        if self.plane is SemanticPlane.PHYSICAL_STATE and self.epistemic_status in {
            EpistemicStatus.INFERRED_BELIEF,
            EpistemicStatus.HYPOTHESIS,
        }:
            raise ValueError("inference cannot directly write physical state")
        return self


class BitemporalInterval(_KernelContract):
    valid_from: datetime
    valid_to: datetime | None = None
    transaction_from: datetime
    transaction_to: datetime | None = None

    @field_validator("valid_from", "valid_to", "transaction_from", "transaction_to")
    @classmethod
    def datetimes_are_aware(cls, value: datetime | None, info) -> datetime | None:
        return _require_aware(value, field_name=info.field_name) if value else None

    @model_validator(mode="after")
    def intervals_are_forward(self) -> Self:
        if self.valid_to is not None and self.valid_to <= self.valid_from:
            raise ValueError("valid_to must be after valid_from")
        if self.transaction_to is not None and self.transaction_to <= self.transaction_from:
            raise ValueError("transaction_to must be after transaction_from")
        return self

    def visible_at(self, *, valid_at: datetime, known_at: datetime) -> bool:
        _require_aware(valid_at, field_name="valid_at")
        _require_aware(known_at, field_name="known_at")
        valid = self.valid_from <= valid_at and (
            self.valid_to is None or valid_at < self.valid_to
        )
        known = self.transaction_from <= known_at and (
            self.transaction_to is None or known_at < self.transaction_to
        )
        return valid and known


class RestrictionSet(_KernelContract):
    """An explicit universe or an explicit finite allow-set, including deny-all."""

    universe: bool = False
    values: frozenset[str] = frozenset()

    @model_validator(mode="after")
    def universe_has_no_enumerated_values(self) -> Self:
        if self.universe and self.values:
            raise ValueError("an unrestricted set cannot also enumerate values")
        return self

    @classmethod
    def unrestricted(cls) -> RestrictionSet:
        return cls(universe=True)

    @classmethod
    def only(cls, *values: str) -> RestrictionSet:
        return cls(values=frozenset(values))

    def intersect(self, other: RestrictionSet) -> RestrictionSet:
        if self.universe:
            return other
        if other.universe:
            return self
        return RestrictionSet(values=self.values & other.values)

    def is_subset_of(self, other: RestrictionSet) -> bool:
        if other.universe:
            return True
        if self.universe:
            return False
        return self.values <= other.values

    def permits(self, value: str) -> bool:
        return self.universe or value in self.values


class AuthorityContextKind(StrEnum):
    PROCESSING = "processing"
    CONSUMPTION = "consumption"


class AuthorityContext(_KernelContract):
    kind: AuthorityContextKind
    tenant_id: UUID
    principal_or_service_id: str = Field(min_length=1)
    purpose: str = Field(min_length=1)
    operation: str = Field(min_length=1)
    object_types: RestrictionSet
    object_ids: RestrictionSet
    fields: RestrictionSet
    source_labels: RestrictionSet
    authority_basis_refs: frozenset[str] = Field(min_length=1)
    revocation_refs: frozenset[str] = frozenset()
    policy_version: str = Field(min_length=1)
    authority_epoch: int = Field(ge=0)
    decision_time: datetime
    expires_at: datetime

    @field_validator("decision_time", "expires_at")
    @classmethod
    def authority_times_are_aware(cls, value: datetime, info) -> datetime:
        return _require_aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def expiry_follows_decision(self) -> Self:
        if self.expires_at <= self.decision_time:
            raise ValueError("authority expiry must follow decision time")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def is_live(self, at: datetime) -> bool:
        at = _require_aware(at, field_name="at")
        return self.decision_time <= at < self.expires_at

    def restrict_with(self, other: AuthorityContext) -> AuthorityContext:
        """Compose inputs monotonically; the result can only lose permission."""

        for field_name in ("kind", "tenant_id", "purpose", "operation"):
            if getattr(self, field_name) != getattr(other, field_name):
                raise ValueError(f"cannot compose authority with different {field_name}")
        return AuthorityContext(
            kind=self.kind,
            tenant_id=self.tenant_id,
            principal_or_service_id=f"{self.principal_or_service_id}&{other.principal_or_service_id}",
            purpose=self.purpose,
            operation=self.operation,
            object_types=self.object_types.intersect(other.object_types),
            object_ids=self.object_ids.intersect(other.object_ids),
            fields=self.fields.intersect(other.fields),
            source_labels=self.source_labels.intersect(other.source_labels),
            authority_basis_refs=self.authority_basis_refs | other.authority_basis_refs,
            revocation_refs=self.revocation_refs | other.revocation_refs,
            policy_version=f"{self.policy_version}&{other.policy_version}",
            authority_epoch=max(self.authority_epoch, other.authority_epoch),
            decision_time=max(self.decision_time, other.decision_time),
            expires_at=min(self.expires_at, other.expires_at),
        )

    def is_no_broader_than(self, other: AuthorityContext) -> bool:
        if any(
            getattr(self, field_name) != getattr(other, field_name)
            for field_name in ("kind", "tenant_id", "purpose", "operation")
        ):
            return False
        return all(
            getattr(self, field_name).is_subset_of(getattr(other, field_name))
            for field_name in ("object_types", "object_ids", "fields", "source_labels")
        )


class ProcessingAuthorityContext(AuthorityContext):
    kind: Literal[AuthorityContextKind.PROCESSING] = AuthorityContextKind.PROCESSING


class ConsumptionAuthorityContext(AuthorityContext):
    kind: Literal[AuthorityContextKind.CONSUMPTION] = AuthorityContextKind.CONSUMPTION


class EventPosition(_KernelContract):
    log_id: str = Field(min_length=1)
    partition_epoch: int = Field(ge=0)
    partition_id: str = Field(min_length=1)
    offset: int = Field(ge=0)

    @property
    def partition_key(self) -> tuple[str, int, str]:
        return (self.log_id, self.partition_epoch, self.partition_id)

    def covers(self, other: EventPosition) -> bool:
        if self.partition_key != other.partition_key:
            raise ValueError("positions in different partition epochs are incomparable")
        return self.offset >= other.offset


class WatermarkVector(_KernelContract):
    positions: tuple[EventPosition, ...]
    database_snapshot_token: str = Field(min_length=1)
    captured_at: datetime

    @field_validator("captured_at")
    @classmethod
    def captured_at_is_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="captured_at")

    @model_validator(mode="after")
    def partition_epochs_are_unique(self) -> Self:
        keys = [position.partition_key for position in self.positions]
        if len(keys) != len(set(keys)):
            raise ValueError("watermark vector must have one offset per partition epoch")
        return self

    def covers(self, other: WatermarkVector) -> bool:
        current = {position.partition_key: position.offset for position in self.positions}
        return all(current.get(position.partition_key, -1) >= position.offset for position in other.positions)


class WriterCutoverState(StrEnum):
    LEGACY = "legacy"
    ADAPTER_ENFORCED = "adapter_enforced"
    BACKFILLING = "backfilling"
    CATCH_UP = "catch_up"
    VERIFIED = "verified"
    WRITER_FENCED = "writer_fenced"
    NEW_CANONICAL = "new_canonical"
    RETIRED = "retired"


class WriterScopeEpoch(_KernelContract):
    scope_id: str = Field(min_length=1)
    tenant_id: UUID
    semantic_responsibility: str = Field(min_length=1)
    source_partition: str = Field(min_length=1)
    writer_owner: str = Field(min_length=1)
    epoch: int = Field(ge=0)
    state: WriterCutoverState
    parent_scope_id: str | None = None
    high_water: WatermarkVector | None = None

    def permits(
        self,
        *,
        writer_owner: str,
        epoch: int,
        tenant_id: UUID,
        semantic_responsibility: str,
        source_partition: str,
    ) -> bool:
        return (
            self.state not in {WriterCutoverState.WRITER_FENCED, WriterCutoverState.RETIRED}
            and self.writer_owner == writer_owner
            and self.epoch == epoch
            and self.tenant_id == tenant_id
            and self.semantic_responsibility == semantic_responsibility
            and self.source_partition == source_partition
        )


class AggregateVersionRef(_KernelContract):
    semantic_responsibility: str = Field(min_length=1)
    aggregate_id: str = Field(min_length=1)
    expected_version: int = Field(ge=0)

    @property
    def lock_key(self) -> tuple[str, str]:
        return (self.semantic_responsibility, self.aggregate_id)


class CommittedAggregateVersion(_KernelContract):
    semantic_responsibility: str = Field(min_length=1)
    aggregate_id: str = Field(min_length=1)
    committed_version: int = Field(ge=1)


class IsolationRequirement(StrEnum):
    SERIALIZABLE = "serializable"
    SERIALIZABLE_EQUIVALENT = "serializable_equivalent"


class MultiAggregateMutationPlan(_KernelContract):
    plan_id: str = Field(min_length=1)
    tenant_id: UUID
    aggregate_versions: tuple[AggregateVersionRef, ...] = Field(min_length=2)
    shared_invariant: str = Field(min_length=1)
    canonical_request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    maximum_size: int = Field(ge=2)
    dependency_edge_refs: tuple[str, ...] = Field(min_length=1)
    isolation_requirement: IsolationRequirement

    @model_validator(mode="after")
    def write_set_is_complete_bounded_and_sorted(self) -> Self:
        if len(self.aggregate_versions) > self.maximum_size:
            raise ValueError("multi-aggregate write set exceeds registered maximum")
        keys = [item.lock_key for item in self.aggregate_versions]
        if keys != sorted(keys):
            raise ValueError("aggregate write set must use deterministic lock order")
        if len(keys) != len(set(keys)):
            raise ValueError("aggregate write set cannot contain duplicate keys")
        return self


class CommandEnvelope(_KernelContract):
    tenant_id: UUID
    command_id: str = Field(min_length=1)
    semantic_operation: str = Field(min_length=1)
    target_semantic_key: str = Field(min_length=1)
    writer_scope_id: str = Field(min_length=1)
    writer_epoch: int = Field(ge=0)
    expected_aggregate: AggregateVersionRef | None = None
    mutation_plan_id: str | None = None
    semantic_idempotency_scope: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    canonical_request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    issuing_principal: str = Field(min_length=1)
    authority_decision_ref: str = Field(min_length=1)
    deadline: datetime
    schema_version: str = Field(min_length=1)
    trace_id: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    causation_id: str | None = None

    @field_validator("deadline")
    @classmethod
    def deadline_is_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="deadline")

    @model_validator(mode="after")
    def exactly_one_aggregate_boundary(self) -> Self:
        if (self.expected_aggregate is None) == (self.mutation_plan_id is None):
            raise ValueError("command requires exactly one aggregate or mutation plan")
        return self


class CommandResultStatus(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"
    REJECTED_TERMINAL = "rejected_terminal"
    REJECTED_RETRYABLE = "rejected_retryable"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"


class IdempotencyReplayDisposition(StrEnum):
    NEW = "new"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


class IdempotencyReplayDecision(_KernelContract):
    semantic_idempotency_scope: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    incoming_request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    stored_request_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    prior_result_ref: str | None = None

    @model_validator(mode="after")
    def stored_hash_has_prior_result(self) -> Self:
        if (self.stored_request_hash is None) != (self.prior_result_ref is None):
            raise ValueError("stored request hash and prior result must appear together")
        return self

    @property
    def disposition(self) -> IdempotencyReplayDisposition:
        if self.stored_request_hash is None:
            return IdempotencyReplayDisposition.NEW
        if self.stored_request_hash == self.incoming_request_hash:
            return IdempotencyReplayDisposition.DUPLICATE
        return IdempotencyReplayDisposition.CONFLICT


class CommandResult(_KernelContract):
    result_id: str = Field(min_length=1)
    command_id: str = Field(min_length=1)
    canonical_request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    writer_scope_id: str = Field(min_length=1)
    writer_epoch: int = Field(ge=0)
    status: CommandResultStatus
    committed_aggregate_versions: tuple[CommittedAggregateVersion, ...] = ()
    event_ids: tuple[str, ...] = ()
    prior_result_ref: str | None = None
    rejection_code: str | None = None
    retry_after: datetime | None = None

    @field_validator("retry_after")
    @classmethod
    def retry_after_is_aware(cls, value: datetime | None) -> datetime | None:
        return _require_aware(value, field_name="retry_after") if value else None

    @model_validator(mode="after")
    def result_shape_matches_status(self) -> Self:
        if self.status is CommandResultStatus.APPLIED and (
            not self.committed_aggregate_versions or not self.event_ids
        ):
            raise ValueError("applied result requires committed versions and events")
        if self.status is CommandResultStatus.DUPLICATE and not self.prior_result_ref:
            raise ValueError("duplicate result requires prior_result_ref")
        if self.status in {
            CommandResultStatus.REJECTED_TERMINAL,
            CommandResultStatus.REJECTED_RETRYABLE,
            CommandResultStatus.IDEMPOTENCY_CONFLICT,
        } and not self.rejection_code:
            raise ValueError("rejected or conflicting result requires rejection_code")
        if self.status is CommandResultStatus.REJECTED_RETRYABLE and not self.retry_after:
            raise ValueError("retryable rejection requires retry_after")
        return self


def validate_command_writer_scope(
    command: CommandEnvelope,
    scope: WriterScopeEpoch,
    *,
    writer_owner: str,
    semantic_responsibility: str,
    source_partition: str,
) -> None:
    if command.writer_scope_id != scope.scope_id:
        raise ValueError("command references a different writer scope")
    if not scope.permits(
        writer_owner=writer_owner,
        epoch=command.writer_epoch,
        tenant_id=command.tenant_id,
        semantic_responsibility=semantic_responsibility,
        source_partition=source_partition,
    ):
        raise ValueError("writer scope or epoch does not permit this commit")


class CanonicalEventEnvelope(_KernelContract):
    event_id: str = Field(min_length=1)
    tenant_id: UUID
    writer_scope_id: str = Field(min_length=1)
    writer_epoch: int = Field(ge=0)
    aggregate: CommittedAggregateVersion
    producer_sequence: int = Field(ge=1)
    position: EventPosition
    semantic_transition: str = Field(min_length=1)
    command_id: str = Field(min_length=1)
    result_id: str = Field(min_length=1)
    source_version_refs: tuple[str, ...]
    schema_version: str = Field(min_length=1)
    trace_id: str = Field(min_length=1)
    required_outbox_ids: tuple[str, ...] = Field(min_length=1)


class OutboxState(StrEnum):
    PENDING = "pending"
    RETRYABLE = "retryable"
    DELIVERED = "delivered"
    TERMINAL_FAILED = "terminal_failed"


class OutboxRecord(_KernelContract):
    outbox_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    destination_operation: str = Field(min_length=1)
    available_at: datetime
    deadline: datetime
    attempt_budget: int = Field(ge=1)
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: OutboxState = OutboxState.PENDING

    @field_validator("available_at", "deadline")
    @classmethod
    def outbox_times_are_aware(cls, value: datetime, info) -> datetime:
        return _require_aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def deadline_follows_availability(self) -> Self:
        if self.deadline <= self.available_at:
            raise ValueError("outbox deadline must follow availability")
        return self


class ConsumerReceiptState(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"
    RETRYABLE = "retryable"
    TERMINAL_REJECTED = "terminal_rejected"
    QUARANTINED_UNKNOWN_SCHEMA = "quarantined_unknown_schema"
    GAP_DETECTED = "gap_detected"


class ConsumerReceipt(_KernelContract):
    receipt_id: str = Field(min_length=1)
    event_or_outbox_id: str = Field(min_length=1)
    consumer_id: str = Field(min_length=1)
    consumer_operation_version: str = Field(min_length=1)
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: ConsumerReceiptState
    last_contiguous_position: EventPosition | None = None
    retry_after: datetime | None = None

    @field_validator("retry_after")
    @classmethod
    def consumer_retry_is_aware(cls, value: datetime | None) -> datetime | None:
        return _require_aware(value, field_name="retry_after") if value else None

    @model_validator(mode="after")
    def receipt_fate_is_actionable(self) -> Self:
        if self.state is ConsumerReceiptState.RETRYABLE and self.retry_after is None:
            raise ValueError("retryable receipt requires retry_after")
        return self


class SemanticDecisionRecord(_KernelContract):
    decision_id: str = Field(min_length=1)
    decision_type: str = Field(min_length=1)
    proposition_or_state_space: str = Field(min_length=1)
    distribution: dict[str, float] = Field(min_length=1)
    evidence_ref_ids: tuple[str, ...]
    policy_or_model_version: str = Field(min_length=1)

    @model_validator(mode="after")
    def distribution_is_complete(self) -> Self:
        if any(value < 0.0 or value > 1.0 for value in self.distribution.values()):
            raise ValueError("decision probabilities must lie in [0, 1]")
        if abs(sum(self.distribution.values()) - 1.0) > 1e-6:
            raise ValueError("semantic decision distribution must sum to one")
        return self


class ComputationalBundle(_KernelContract):
    bundle_id: str = Field(min_length=1)
    topology: str = Field(pattern=r"^(staged|joint|hybrid)$")
    semantic_decision_ids: tuple[str, ...] = Field(min_length=1)
    shared_feature_refs: tuple[str, ...] = ()
    model_call_refs: tuple[str, ...] = Field(min_length=1)
    intermediate_alternative_refs: tuple[str, ...] = ()

    @model_validator(mode="after")
    def decision_ids_are_unique(self) -> Self:
        if len(self.semantic_decision_ids) != len(set(self.semantic_decision_ids)):
            raise ValueError("computational bundle decision IDs must be unique")
        return self


class CommitAuthorityBinding(_KernelContract):
    semantic_decision_id: str = Field(min_length=1)
    command_id: str = Field(min_length=1)
    writer_scope_id: str = Field(min_length=1)
    writer_epoch: int = Field(ge=0)
    applier_id: str = Field(min_length=1)


class SemanticComputationalCommitBoundary(_KernelContract):
    semantic_decisions: tuple[SemanticDecisionRecord, ...] = Field(min_length=1)
    computational_bundle: ComputationalBundle
    commit_bindings: tuple[CommitAuthorityBinding, ...] = ()

    @model_validator(mode="after")
    def identities_remain_separate_and_closed(self) -> Self:
        decision_ids = [decision.decision_id for decision in self.semantic_decisions]
        if len(decision_ids) != len(set(decision_ids)):
            raise ValueError("semantic decisions require independent unique identities")
        if set(self.computational_bundle.semantic_decision_ids) != set(decision_ids):
            raise ValueError("computational bundle must name every and only its semantic decisions")
        unknown_commits = {
            binding.semantic_decision_id for binding in self.commit_bindings
        } - set(decision_ids)
        if unknown_commits:
            raise ValueError("commit bindings reference unknown semantic decisions")
        committed_ids = [binding.semantic_decision_id for binding in self.commit_bindings]
        if len(committed_ids) != len(set(committed_ids)):
            raise ValueError("one semantic decision cannot have competing commit bindings")
        return self


class CompatibilityMaturity(StrEnum):
    EXPERIMENTAL = "experimental"
    CANDIDATE = "candidate"
    STABLE = "stable"


class ContractCompatibilityManifest(_KernelContract):
    contract_id: str = Field(min_length=1)
    maturity: CompatibilityMaturity
    producer_version: str = Field(min_length=1)
    supported_reader_range: str = Field(min_length=1)
    additive_default_behavior: str = Field(min_length=1)
    semantic_migration: str = Field(min_length=1)
    dual_decode_until: datetime | None = None
    minimum_consumer_version: str = Field(min_length=1)
    activation_gate: str = Field(min_length=1)
    removal_watermark: WatermarkVector | None = None
    all_required_readers_verified: bool = False
    new_producer_enabled: bool = False
    replay_fixtures_passed: bool = False
    rollback_requires_old_decoder: bool = True
    old_decoder_removed: bool = False

    @field_validator("dual_decode_until")
    @classmethod
    def decode_window_is_aware(cls, value: datetime | None) -> datetime | None:
        return _require_aware(value, field_name="dual_decode_until") if value else None

    @model_validator(mode="after")
    def rollout_is_readers_first_and_removal_is_proven(self) -> Self:
        if self.new_producer_enabled and not self.all_required_readers_verified:
            raise ValueError("new producer cannot activate before required readers")
        if self.old_decoder_removed and (
            self.removal_watermark is None
            or not self.replay_fixtures_passed
            or self.rollback_requires_old_decoder
        ):
            raise ValueError("old decoder removal requires watermark, replay and closed rollback")
        return self


class ValidationDisposition(StrEnum):
    ACCEPT = "accept"
    REJECT = "reject"
    DEFER = "defer"


class ProposalValidationApply(_KernelContract):
    proposal_id: str = Field(min_length=1)
    proposal_version: str = Field(min_length=1)
    canonical_proposal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    validator_id: str = Field(min_length=1)
    validator_version: str = Field(min_length=1)
    disposition: ValidationDisposition
    validation_reason_codes: tuple[str, ...] = Field(min_length=1)
    applier_id: str = Field(min_length=1)
    command_result: CommandResult | None = None

    @model_validator(mode="after")
    def only_accepted_proposals_apply(self) -> Self:
        if self.disposition is ValidationDisposition.ACCEPT:
            if self.command_result is None or self.command_result.status not in {
                CommandResultStatus.APPLIED,
                CommandResultStatus.DUPLICATE,
            }:
                raise ValueError("accepted proposal requires applied or duplicate command result")
        elif self.command_result is not None:
            raise ValueError("rejected or deferred proposal cannot carry an apply result")
        return self


__all__ = [
    "AggregateVersionRef",
    "AuthorityContext",
    "AuthorityContextKind",
    "BitemporalInterval",
    "CanonicalEventEnvelope",
    "CommandEnvelope",
    "CommandResult",
    "CommandResultStatus",
    "CommittedAggregateVersion",
    "CommitAuthorityBinding",
    "CompatibilityMaturity",
    "ComputationalBundle",
    "ConsumerReceipt",
    "ConsumerReceiptState",
    "ConsumptionAuthorityContext",
    "ContentDomain",
    "ContractCompatibilityManifest",
    "EpistemicStatus",
    "EventPosition",
    "IdempotencyReplayDecision",
    "IdempotencyReplayDisposition",
    "IsolationRequirement",
    "LifecycleStatus",
    "MultiAggregateMutationPlan",
    "OutboxRecord",
    "OutboxState",
    "ProcessingAuthorityContext",
    "ProposalValidationApply",
    "ProvenanceAndConfidence",
    "RelationSemantics",
    "RestrictionSet",
    "SemanticAxes",
    "SemanticComputationalCommitBoundary",
    "SemanticDecisionRecord",
    "SemanticPlane",
    "ValidationDisposition",
    "WatermarkVector",
    "WriterCutoverState",
    "WriterScopeEpoch",
    "canonical_sha256",
    "validate_command_writer_scope",
]
