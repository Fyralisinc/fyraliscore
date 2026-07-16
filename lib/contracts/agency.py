"""C0c intent, attention, concern, and consequential-agency contracts.

These contracts keep four things mechanically separate:

* an interpretation of what someone may have meant;
* a constitutive act that changes company direction;
* an attention gap caused by applying direction to believed/observed state; and
* a proposed, authorized, executed, and measured intervention.

The module is pure vocabulary and validation.  Named domain appliers own all
durable transitions.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal, Self
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .kernel import (
    ConsumptionAuthorityContext,
    ProcessingAuthorityContext,
    WriterScopeEpoch,
    canonical_sha256,
)
from .perception import CanonicalReferent


class _AgencyContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        use_enum_values=False,
    )


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


class IntentObjectKind(StrEnum):
    GOAL = "goal"
    PRIORITY = "priority"
    DECISION = "decision"
    COMMITMENT = "commitment"
    WORKFLOW_SPEC = "workflow_spec"
    STANDING_COMPLIANCE_OBLIGATION = "standing_compliance_obligation"


class IntentOperation(StrEnum):
    CREATE = "create"
    UPDATE = "update"
    TRANSITION = "transition"
    SUPERSEDE = "supersede"
    RETIRE = "retire"


class ConstitutiveIntentAuthorityBasisKind(StrEnum):
    EXPLICIT_PRINCIPAL = "explicit_principal"
    INSTITUTIONAL_SOURCE = "institutional_source"
    DELEGATED_POLICY = "delegated_policy"


class AuthorityBasisSurvivalMode(StrEnum):
    POINT_IN_TIME_CONSTITUTIVE = "point_in_time_constitutive"
    BASIS_CONTINGENT = "basis_contingent"
    REVIEW_REQUIRED = "review_required"

    @property
    def permissiveness(self) -> int:
        return {
            AuthorityBasisSurvivalMode.BASIS_CONTINGENT: 0,
            AuthorityBasisSurvivalMode.REVIEW_REQUIRED: 1,
            AuthorityBasisSurvivalMode.POINT_IN_TIME_CONSTITUTIVE: 2,
        }[self]


class IntentMutation(_AgencyContract):
    object_kind: IntentObjectKind
    operation: IntentOperation
    target_aggregate_id: UUID | None = None
    expected_target_version: int | None = Field(default=None, ge=0)
    payload: dict[str, Any]
    schema_version: str = Field(min_length=1)
    effective_at: datetime

    @field_validator("effective_at")
    @classmethod
    def effective_time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="effective_at")

    @model_validator(mode="after")
    def target_matches_operation(self) -> Self:
        if self.operation is IntentOperation.CREATE:
            if (
                self.target_aggregate_id is not None
                or self.expected_target_version is not None
            ):
                raise ValueError(
                    "create intent cannot claim an existing target version"
                )
        elif self.target_aggregate_id is None:
            raise ValueError("non-create intent requires target_aggregate_id")
        if not self.payload:
            raise ValueError("intent mutation payload cannot be empty")
        return self

    @property
    def payload_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ConstitutiveIntentAuthorityBasis(_AgencyContract):
    kind: ConstitutiveIntentAuthorityBasisKind
    basis_id: str = Field(min_length=1)
    principal_or_actor_id: str = Field(min_length=1)
    capability_or_grant_ref: str = Field(min_length=1)
    acknowledged_payload_digest: str | None = None
    source_contract_ref: str | None = None
    evidence_record_ref: str | None = None
    delegation_ref: str | None = None
    control_policy_version_ref: str | None = None
    valid_from: datetime
    valid_until: datetime

    @field_validator("valid_from", "valid_until")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def basis_has_exact_required_evidence(self) -> Self:
        if self.valid_until <= self.valid_from:
            raise ValueError("authority basis validity interval is empty")
        if self.kind is ConstitutiveIntentAuthorityBasisKind.EXPLICIT_PRINCIPAL:
            if not self.acknowledged_payload_digest:
                raise ValueError(
                    "explicit principal basis requires exact payload acknowledgement"
                )
            if any(
                value is not None
                for value in (
                    self.source_contract_ref,
                    self.evidence_record_ref,
                    self.delegation_ref,
                    self.control_policy_version_ref,
                )
            ):
                raise ValueError(
                    "explicit principal basis cannot borrow another basis path"
                )
        elif self.kind is ConstitutiveIntentAuthorityBasisKind.INSTITUTIONAL_SOURCE:
            if not (self.source_contract_ref and self.evidence_record_ref):
                raise ValueError(
                    "institutional basis requires source contract and exact evidence"
                )
            if self.delegation_ref or self.control_policy_version_ref:
                raise ValueError(
                    "institutional basis cannot borrow delegated-policy authority"
                )
        else:
            if not (self.delegation_ref and self.control_policy_version_ref):
                raise ValueError(
                    "delegated basis requires delegation and active policy version"
                )
            if self.source_contract_ref or self.evidence_record_ref:
                raise ValueError(
                    "delegated basis cannot borrow institutional-source authority"
                )
        return self

    def is_live(self, at: datetime) -> bool:
        at = _aware(at, field_name="at")
        return self.valid_from <= at < self.valid_until


class AuthorityBasisSurvivalPolicy(_AgencyContract):
    policy_version: str = Field(min_length=1)
    mode: AuthorityBasisSurvivalMode
    maximum_mode_permitted_by_operation: AuthorityBasisSurvivalMode
    maximum_mode_permitted_by_basis: AuthorityBasisSurvivalMode
    retrospective_defect_always_fences: Literal[True] = True
    reactivation_requires_new_command: Literal[True] = True

    @model_validator(mode="after")
    def cannot_be_more_permissive_than_either_source(self) -> Self:
        maximum = min(
            self.maximum_mode_permitted_by_operation.permissiveness,
            self.maximum_mode_permitted_by_basis.permissiveness,
        )
        if self.mode.permissiveness > maximum:
            raise ValueError(
                "survival policy is more permissive than its operation or basis"
            )
        return self


class IntentGroundingDependency(_AgencyContract):
    semantic_role: str = Field(min_length=1)
    resolution_assessment_id: str = Field(min_length=1)
    resolution_assessment_version: str = Field(min_length=1)
    selected_referent: CanonicalReferent
    grounding_admission_decision_id: str = Field(min_length=1)
    grounding_admission_version: str = Field(min_length=1)
    purpose: Literal["intent_mutation"] = "intent_mutation"
    risk_tier: str = Field(min_length=1)
    expires_at: datetime

    @field_validator("expires_at")
    @classmethod
    def expiry_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="expires_at")

    def is_live(self, at: datetime) -> bool:
        return _aware(at, field_name="at") < self.expires_at


class TypedConstitutiveIntentCommand(_AgencyContract):
    command_id: UUID
    tenant_id: UUID
    mutation: IntentMutation
    declared_payload_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    authority_basis: ConstitutiveIntentAuthorityBasis
    survival_policy: AuthorityBasisSurvivalPolicy
    grounding_dependencies: tuple[IntentGroundingDependency, ...] = ()
    processing_authority: ProcessingAuthorityContext
    consumption_authority: ConsumptionAuthorityContext
    writer_scope_epoch: WriterScopeEpoch
    idempotency_key: str = Field(min_length=1)
    exact_input_anchors: tuple[str, ...] = Field(min_length=1)
    issued_at: datetime
    expires_at: datetime
    proposal_acceptance_ref: str | None = None

    @field_validator("issued_at", "expires_at")
    @classmethod
    def command_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def command_is_exact_live_and_authorized(self) -> Self:
        if self.declared_payload_digest != self.mutation.payload_digest:
            raise ValueError(
                "declared intent digest does not match the exact typed mutation"
            )
        if self.expires_at <= self.issued_at:
            raise ValueError("intent command expiry must follow issuance")
        if self.processing_authority.tenant_id != self.tenant_id:
            raise ValueError("processing authority tenant does not match command")
        if self.consumption_authority.tenant_id != self.tenant_id:
            raise ValueError("consumption authority tenant does not match command")
        if (
            self.authority_basis.kind
            is ConstitutiveIntentAuthorityBasisKind.EXPLICIT_PRINCIPAL
        ):
            if (
                self.authority_basis.acknowledged_payload_digest
                != self.declared_payload_digest
            ):
                raise ValueError(
                    "principal did not acknowledge this exact intent digest"
                )
        if not self.authority_basis.is_live(self.issued_at):
            raise ValueError("constitutive authority basis was not live at issuance")
        if not self.processing_authority.is_live(self.issued_at):
            raise ValueError("processing authority was not live at issuance")
        if not self.consumption_authority.is_live(self.issued_at):
            raise ValueError("consumption authority was not live at issuance")
        roles = [item.semantic_role for item in self.grounding_dependencies]
        if len(roles) != len(set(roles)):
            raise ValueError("intent grounding roles must be unique")
        if any(
            not item.is_live(self.issued_at) for item in self.grounding_dependencies
        ):
            raise ValueError("intent command contains expired grounding admission")
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class IntentProposalFate(StrEnum):
    OPEN = "open"
    DEFERRED = "deferred"
    ACCEPTED_FOR_AUTHORIZATION = "accepted_for_authorization"
    REJECTED = "rejected"
    EXPIRED = "expired"
    SUPERSEDED = "superseded"

    @property
    def terminal(self) -> bool:
        return self in {
            IntentProposalFate.ACCEPTED_FOR_AUTHORIZATION,
            IntentProposalFate.REJECTED,
            IntentProposalFate.EXPIRED,
            IntentProposalFate.SUPERSEDED,
        }


class InterpretedIntentProposal(_AgencyContract):
    proposal_id: UUID
    tenant_id: UUID
    proposal_version: int = Field(ge=1)
    normalized_mutation: IntentMutation
    normalized_payload_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_assertion_refs: tuple[str, ...] = Field(min_length=1)
    semantic_frame_refs: tuple[str, ...] = ()
    speech_act_refs: tuple[str, ...] = ()
    grounding_dependency_refs: tuple[str, ...] = ()
    interpretation_context_snapshot_ref: str | None = None
    uncertainty_reasons: tuple[str, ...] = Field(min_length=1)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    processing_authority: ProcessingAuthorityContext
    processing_authority_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: datetime
    review_due_at: datetime
    fate: IntentProposalFate = IntentProposalFate.OPEN

    @field_validator("created_at", "review_due_at")
    @classmethod
    def proposal_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def proposal_preserves_interpretive_status(self) -> Self:
        if self.normalized_payload_digest != self.normalized_mutation.payload_digest:
            raise ValueError("proposal digest does not match normalized mutation")
        if self.review_due_at <= self.created_at:
            raise ValueError("proposal review deadline must follow creation")
        if self.processing_authority.tenant_id != self.tenant_id:
            raise ValueError("proposal processing authority tenant mismatch")
        if (
            self.processing_authority.fingerprint
            != self.processing_authority_fingerprint
        ):
            raise ValueError("proposal processing authority fingerprint mismatch")
        if not self.processing_authority.is_live(self.created_at):
            raise ValueError(
                "proposal processing authority was not live at interpretation"
            )
        return self


class ExactProposalAcceptance(_AgencyContract):
    acceptance_id: UUID
    tenant_id: UUID
    proposal_id: UUID
    proposal_version: int = Field(ge=1)
    proposal_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    normalized_payload_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    principal_id: str = Field(min_length=1)
    capability_ref: str = Field(min_length=1)
    authority: ConsumptionAuthorityContext
    accepted_at: datetime
    expires_at: datetime

    @field_validator("accepted_at", "expires_at")
    @classmethod
    def acceptance_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def acceptance_is_live_and_tenant_scoped(self) -> Self:
        if self.authority.tenant_id != self.tenant_id:
            raise ValueError("acceptance authority tenant mismatch")
        if self.expires_at <= self.accepted_at:
            raise ValueError("acceptance expiry must follow acceptance")
        if not self.authority.is_live(self.accepted_at):
            raise ValueError("acceptance authority was not live")
        return self

    def accepts(self, proposal: InterpretedIntentProposal) -> bool:
        return (
            proposal.tenant_id == self.tenant_id
            and proposal.proposal_id == self.proposal_id
            and proposal.proposal_version == self.proposal_version
            and canonical_sha256(proposal.model_dump(mode="json"))
            == self.proposal_digest
            and proposal.normalized_payload_digest == self.normalized_payload_digest
            and not proposal.fate.terminal
        )


class AuthorityBasisChangeKind(StrEnum):
    PROSPECTIVE_EXPIRY = "prospective_expiry"
    PROSPECTIVE_REVOCATION = "prospective_revocation"
    RETROSPECTIVE_INVALIDITY = "retrospective_invalidity"
    PAYLOAD_NOT_AUTHORIZED_AT_COMMIT = "payload_not_authorized_at_commit"
    WRONG_REFERENT_AT_COMMIT = "wrong_referent_at_commit"
    GROUNDING_EXPIRED_SAME_REFERENT = "grounding_expired_same_referent"
    GROUNDING_REFERENT_CHANGED = "grounding_referent_changed"


class IntentDependentFate(StrEnum):
    RETAINED_BASIS_VALID_AT_COMMIT = "retained_basis_valid_at_commit"
    SUSPENDED_BASIS_ENDED = "suspended_basis_ended"
    RETAINED_WITH_REVALIDATED_GROUNDING = "retained_with_revalidated_grounding"
    DISPUTED_PENDING_REVIEW = "disputed_pending_review"
    SUPERSEDED = "superseded"
    CANCELLED_IF_REVERSIBLE = "cancelled_if_reversible"
    REAUTHORIZATION_REQUIRED = "reauthorization_required"
    RETROSPECTIVELY_CONTAMINATED = "retrospectively_contaminated"


class AuthorityBasisChange(_AgencyContract):
    change_id: UUID
    kind: AuthorityBasisChangeKind
    changed_at: datetime
    replacement_basis_ref: str | None = None
    revalidated_same_referent: bool = False

    @field_validator("changed_at")
    @classmethod
    def change_time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="changed_at")


def reduce_intent_basis_change(
    *,
    policy: AuthorityBasisSurvivalPolicy,
    change: AuthorityBasisChange,
) -> IntentDependentFate:
    """Total reducer for authority-basis and grounding changes."""

    if change.kind in {
        AuthorityBasisChangeKind.RETROSPECTIVE_INVALIDITY,
        AuthorityBasisChangeKind.PAYLOAD_NOT_AUTHORIZED_AT_COMMIT,
        AuthorityBasisChangeKind.WRONG_REFERENT_AT_COMMIT,
        AuthorityBasisChangeKind.GROUNDING_REFERENT_CHANGED,
    }:
        return IntentDependentFate.RETROSPECTIVELY_CONTAMINATED
    if change.kind is AuthorityBasisChangeKind.GROUNDING_EXPIRED_SAME_REFERENT:
        if change.revalidated_same_referent and change.replacement_basis_ref:
            return IntentDependentFate.RETAINED_WITH_REVALIDATED_GROUNDING
        return IntentDependentFate.REAUTHORIZATION_REQUIRED
    if policy.mode is AuthorityBasisSurvivalMode.POINT_IN_TIME_CONSTITUTIVE:
        return IntentDependentFate.RETAINED_BASIS_VALID_AT_COMMIT
    if policy.mode is AuthorityBasisSurvivalMode.BASIS_CONTINGENT:
        return IntentDependentFate.SUSPENDED_BASIS_ENDED
    return IntentDependentFate.DISPUTED_PENDING_REVIEW


class AttentionSourceKind(StrEnum):
    GOAL = "goal"
    DIRECTION_BEARING_DECISION = "direction_bearing_decision"
    COMMITMENT = "commitment"
    STANDING_COMPLIANCE_OBLIGATION = "standing_compliance_obligation"
    WORKFLOW_SPEC = "workflow_spec"
    PLATFORM_OBLIGATION = "platform_obligation"
    DISCOVERY_DUTY = "discovery_duty"


class ConcernDisposition(StrEnum):
    SUPPRESSED = "suppressed"
    ACCEPTED_RISK = "accepted_risk"
    DISMISSED = "dismissed"


class AttentionGovernanceBinding(_AgencyContract):
    binding_id: UUID
    binding_version: int = Field(ge=1)
    attention_source_ref: str = Field(min_length=1)
    attention_source_kind: AttentionSourceKind
    work_budget_units: float = Field(gt=0.0)
    interruption_budget_count: int = Field(ge=0)
    interruption_budget_minutes: float = Field(ge=0.0)
    maximum_duration_seconds: int = Field(gt=0)
    satisfaction_rule: str = Field(min_length=1)
    expiry_rule: str = Field(min_length=1)
    review_rule: str = Field(min_length=1)
    stop_rule: str = Field(min_length=1)
    permitted_priority_modifier_fields: frozenset[str] = frozenset()
    disposition_capability_refs: dict[ConcernDisposition, str] = Field(
        default_factory=dict
    )
    nonwaivable_fields: frozenset[str] = frozenset()
    valid_from: datetime
    valid_until: datetime

    @field_validator("valid_from", "valid_until")
    @classmethod
    def binding_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def protected_sources_cannot_have_implicit_waiver(self) -> Self:
        if self.valid_until <= self.valid_from:
            raise ValueError("attention binding validity interval is empty")
        if self.attention_source_kind is AttentionSourceKind.PLATFORM_OBLIGATION:
            if not self.nonwaivable_fields:
                raise ValueError(
                    "platform obligations require explicit nonwaivable fields"
                )
            if ConcernDisposition.DISMISSED in self.disposition_capability_refs:
                raise ValueError(
                    "platform obligations cannot be ordinarily dismissible"
                )
        return self

    @property
    def binding_ref(self) -> str:
        return f"attention-binding:{self.binding_id}:v{self.binding_version}"

    @property
    def binding_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def is_live(self, at: datetime) -> bool:
        at = _aware(at, field_name="at")
        return self.valid_from <= at < self.valid_until


class EffectiveAttentionGovernanceEnvelope(_AgencyContract):
    binding_refs: tuple[str, ...] = Field(min_length=1)
    attention_source_refs: tuple[str, ...] = Field(min_length=1)
    work_budget_units: float = Field(gt=0.0)
    interruption_budget_count: int = Field(ge=0)
    interruption_budget_minutes: float = Field(ge=0.0)
    maximum_duration_seconds: int = Field(gt=0)
    satisfaction_rules: tuple[str, ...] = Field(min_length=1)
    expiry_rules: tuple[str, ...] = Field(min_length=1)
    review_rules: tuple[str, ...] = Field(min_length=1)
    stop_rules: tuple[str, ...] = Field(min_length=1)
    permitted_priority_modifier_fields: frozenset[str]
    nonwaivable_fields: frozenset[str]
    disposition_capability_refs_by_source: dict[str, dict[ConcernDisposition, str]]
    valid_from: datetime
    valid_until: datetime

    @field_validator("valid_from", "valid_until")
    @classmethod
    def envelope_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def envelope_is_canonical_and_nonempty(self) -> Self:
        if self.valid_until <= self.valid_from:
            raise ValueError(
                "effective attention envelope has no common validity interval"
            )
        if tuple(sorted(self.binding_refs)) != self.binding_refs:
            raise ValueError(
                "effective attention binding refs must be canonically sorted"
            )
        if len(set(self.binding_refs)) != len(self.binding_refs):
            raise ValueError("effective attention binding refs must be unique")
        if tuple(sorted(self.attention_source_refs)) != self.attention_source_refs:
            raise ValueError(
                "effective attention source refs must be canonically sorted"
            )
        if len(set(self.attention_source_refs)) != len(self.attention_source_refs):
            raise ValueError("effective attention source refs must be unique")
        return self

    @property
    def envelope_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


def compose_attention_governance_bindings(
    bindings: tuple[AttentionGovernanceBinding, ...],
    *,
    at: datetime,
) -> EffectiveAttentionGovernanceEnvelope:
    """Return the monotone meet of every active contributing source binding."""

    at = _aware(at, field_name="at")
    if not bindings:
        raise ValueError(
            "attention governance composition requires at least one binding"
        )
    ordered = tuple(sorted(bindings, key=lambda item: item.binding_ref))
    refs = tuple(item.binding_ref for item in ordered)
    if len(set(refs)) != len(refs):
        raise ValueError(
            "attention governance composition contains a duplicate binding"
        )
    source_refs = tuple(sorted(item.attention_source_ref for item in ordered))
    if len(set(source_refs)) != len(source_refs):
        raise ValueError(
            "one attention source cannot contribute multiple binding versions"
        )
    if any(not item.is_live(at) for item in ordered):
        raise ValueError(
            "attention governance composition contains an inactive binding"
        )
    permitted = set(ordered[0].permitted_priority_modifier_fields)
    for item in ordered[1:]:
        permitted.intersection_update(item.permitted_priority_modifier_fields)
    return EffectiveAttentionGovernanceEnvelope(
        binding_refs=refs,
        attention_source_refs=source_refs,
        work_budget_units=min(item.work_budget_units for item in ordered),
        interruption_budget_count=min(
            item.interruption_budget_count for item in ordered
        ),
        interruption_budget_minutes=min(
            item.interruption_budget_minutes for item in ordered
        ),
        maximum_duration_seconds=min(item.maximum_duration_seconds for item in ordered),
        satisfaction_rules=tuple(item.satisfaction_rule for item in ordered),
        expiry_rules=tuple(item.expiry_rule for item in ordered),
        review_rules=tuple(item.review_rule for item in ordered),
        stop_rules=tuple(item.stop_rule for item in ordered),
        permitted_priority_modifier_fields=frozenset(permitted),
        nonwaivable_fields=frozenset().union(
            *(item.nonwaivable_fields for item in ordered)
        ),
        disposition_capability_refs_by_source={
            item.attention_source_ref: dict(item.disposition_capability_refs)
            for item in ordered
        },
        valid_from=max(item.valid_from for item in ordered),
        valid_until=min(item.valid_until for item in ordered),
    )


class CriterionImpact(StrEnum):
    UNKNOWN = "unknown"
    SATISFIED = "satisfied"
    NONMATERIAL_GAP = "nonmaterial_gap"
    MATERIAL_GAP = "material_gap"


class CriterionWorkEligibility(StrEnum):
    ACTIONABLE = "actionable"
    BLOCKED = "blocked"
    DEFERRED = "deferred"
    EXHAUSTED = "exhausted"


class ConcernCriterionState(_AgencyContract):
    criterion_ref: str = Field(min_length=1)
    attention_source_ref: str = Field(min_length=1)
    attention_binding_ref: str = Field(min_length=1)
    applicable: bool
    impact: CriterionImpact
    conflict: bool = False
    disposition: ConcernDisposition | None = None
    disposition_capability_ref: str | None = None
    disposition_expires_at: datetime | None = None
    work_eligibility: CriterionWorkEligibility

    @field_validator("disposition_expires_at")
    @classmethod
    def disposition_time_is_aware(cls, value: datetime | None) -> datetime | None:
        return _aware(value, field_name="disposition_expires_at") if value else None

    @model_validator(mode="after")
    def disposition_is_fully_explained(self) -> Self:
        fields = (
            self.disposition,
            self.disposition_capability_ref,
            self.disposition_expires_at,
        )
        if any(value is not None for value in fields) and not all(
            value is not None for value in fields
        ):
            raise ValueError(
                "criterion disposition requires kind, capability, and expiry"
            )
        return self


class ConcernState(StrEnum):
    CANDIDATE = "candidate"
    OPEN = "open"
    SUSPENDED = "suspended"
    SUPPRESSED = "suppressed"
    ACCEPTED_RISK = "accepted_risk"
    DISMISSED = "dismissed"
    RESOLVED = "resolved"
    INVALIDATED = "invalidated"
    EXPIRED = "expired"


class ConcernIdentity(_AgencyContract):
    tenant_id: UUID
    affected_object_or_scope: str = Field(min_length=1)
    state_dimension_or_missing_proposition: str = Field(min_length=1)
    valid_time_window: str = Field(min_length=1)
    gap_identity_policy_version: str = Field(min_length=1)

    @property
    def dedupe_key(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


def derive_concern_id(identity: ConcernIdentity) -> UUID:
    """Derive one stable aggregate identity from the scoped-gap key."""

    return uuid5(NAMESPACE_URL, f"fyralis:concern:{identity.dedupe_key}")


class ConcernSnapshot(_AgencyContract):
    concern_id: UUID
    aggregate_version: int = Field(ge=1)
    identity: ConcernIdentity
    declared_dedupe_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    originating_attention_source_ref: str | None = None
    contributing_attention_source_refs: frozenset[str] = Field(min_length=1)
    criteria: tuple[ConcernCriterionState, ...] = Field(min_length=1)
    current_state_estimate: dict[str, Any]
    materiality: float = Field(ge=0.0, le=1.0)
    uncertainty: float = Field(ge=0.0, le=1.0)
    consequence: float = Field(ge=0.0, le=1.0)
    urgency: float = Field(ge=0.0, le=1.0)
    actionability: float = Field(ge=0.0, le=1.0)
    evidence_cutoff: datetime
    validity_deadline: datetime | None = None
    next_review_at: datetime | None = None
    gap_identity_valid: bool = True
    state: ConcernState
    transition_cause: str = Field(min_length=1)

    @field_validator("evidence_cutoff", "validity_deadline", "next_review_at")
    @classmethod
    def snapshot_times_are_aware(cls, value: datetime | None, info) -> datetime | None:
        return _aware(value, field_name=info.field_name) if value else None

    @model_validator(mode="after")
    def snapshot_preserves_plural_contributors(self) -> Self:
        if self.concern_id != derive_concern_id(self.identity):
            raise ValueError("concern ID does not derive from the scoped gap identity")
        if self.declared_dedupe_key != self.identity.dedupe_key:
            raise ValueError("concern dedupe key does not match scoped gap identity")
        refs = [item.criterion_ref for item in self.criteria]
        if len(refs) != len(set(refs)):
            raise ValueError("concern criterion references must be unique")
        source_refs = {item.attention_source_ref for item in self.criteria}
        if not source_refs <= self.contributing_attention_source_refs:
            raise ValueError(
                "every criterion source must remain in contributor history"
            )
        return self


class ConcernEvaluationCommand(_AgencyContract):
    command_id: UUID
    tenant_id: UUID
    concern_id: UUID
    expected_version: int = Field(ge=0)
    identity: ConcernIdentity
    criteria: tuple[ConcernCriterionState, ...] = Field(min_length=1)
    current_state_estimate: dict[str, Any]
    materiality: float = Field(ge=0.0, le=1.0)
    uncertainty: float = Field(ge=0.0, le=1.0)
    consequence: float = Field(ge=0.0, le=1.0)
    urgency: float = Field(ge=0.0, le=1.0)
    actionability: float = Field(ge=0.0, le=1.0)
    evidence_cutoff: datetime
    validity_deadline: datetime | None = None
    next_review_at: datetime | None = None
    gap_identity_valid: bool = True
    transition_cause: str = Field(min_length=1)
    originating_attention_source_ref: str | None = None
    processing_authority: ProcessingAuthorityContext
    consumption_authority: ConsumptionAuthorityContext
    writer_scope_epoch: WriterScopeEpoch
    idempotency_key: str = Field(min_length=1)
    issued_at: datetime
    expires_at: datetime

    @field_validator(
        "evidence_cutoff",
        "validity_deadline",
        "next_review_at",
        "issued_at",
        "expires_at",
    )
    @classmethod
    def command_times_are_aware(cls, value: datetime | None, info) -> datetime | None:
        return _aware(value, field_name=info.field_name) if value else None

    @model_validator(mode="after")
    def command_is_scoped_and_authorized(self) -> Self:
        if self.identity.tenant_id != self.tenant_id:
            raise ValueError("concern identity tenant does not match command")
        if self.concern_id != derive_concern_id(self.identity):
            raise ValueError(
                "concern command ID does not derive from scoped gap identity"
            )
        if self.processing_authority.tenant_id != self.tenant_id:
            raise ValueError("concern processing authority tenant mismatch")
        if self.consumption_authority.tenant_id != self.tenant_id:
            raise ValueError("concern consumption authority tenant mismatch")
        if self.expires_at <= self.issued_at:
            raise ValueError("concern command expiry must follow issuance")
        if not self.processing_authority.is_live(self.issued_at):
            raise ValueError("concern processing authority was not live at issuance")
        if not self.consumption_authority.is_live(self.issued_at):
            raise ValueError("concern consumption authority was not live at issuance")
        if not self.processing_authority.object_types.permits("concern"):
            raise ValueError("processing authority cannot evaluate Concern")
        if not self.consumption_authority.object_types.permits("concern"):
            raise ValueError("consumption authority cannot commit Concern")
        if not self.processing_authority.object_ids.permits(str(self.concern_id)):
            raise ValueError("processing authority cannot access this Concern")
        if not self.consumption_authority.object_ids.permits(str(self.concern_id)):
            raise ValueError("consumption authority cannot commit this Concern")
        if not self.writer_scope_epoch.permits(
            writer_owner="ConcernApplier",
            epoch=self.writer_scope_epoch.epoch,
            tenant_id=self.tenant_id,
            semantic_responsibility="concern",
            source_partition=str(self.tenant_id),
        ):
            raise ValueError("writer scope does not permit ConcernApplier")
        refs = [item.criterion_ref for item in self.criteria]
        if len(refs) != len(set(refs)):
            raise ValueError("concern command criterion references must be unique")
        if self.expected_version == 0 and any(
            item.impact is not CriterionImpact.UNKNOWN for item in self.criteria
        ):
            raise ValueError("a new concern must begin as an unevaluated candidate")
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ConcernIdentityCorrectionCommand(_AgencyContract):
    command_id: UUID
    tenant_id: UUID
    predecessor_concern_id: UUID
    expected_predecessor_version: int = Field(ge=1)
    successor: ConcernEvaluationCommand
    correction_epoch: int = Field(ge=1)
    correction_reason: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)

    @model_validator(mode="after")
    def correction_has_a_new_candidate_identity(self) -> Self:
        if self.successor.tenant_id != self.tenant_id:
            raise ValueError("successor Concern tenant does not match correction")
        if self.successor.expected_version != 0:
            raise ValueError("identity correction successor must start at version zero")
        if self.successor.concern_id == self.predecessor_concern_id:
            raise ValueError("identity correction must produce a different scoped gap")
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


def reduce_concern_state(
    *,
    criteria: tuple[ConcernCriterionState, ...],
    at: datetime,
    gap_identity_valid: bool,
    validity_deadline: datetime | None,
) -> ConcernState:
    """Total global reducer; no single contributor can close a plural gap."""

    at = _aware(at, field_name="at")
    if not criteria:
        raise ValueError("concern reducer requires the complete criterion population")
    if not gap_identity_valid:
        return ConcernState.INVALIDATED
    applicable = [item for item in criteria if item.applicable]
    if any(item.impact is CriterionImpact.UNKNOWN for item in applicable):
        return ConcernState.CANDIDATE
    if validity_deadline is not None and at >= validity_deadline and not applicable:
        return ConcernState.EXPIRED
    if not applicable or all(
        item.impact is CriterionImpact.SATISFIED for item in applicable
    ):
        return ConcernState.RESOLVED
    unsatisfied = [
        item for item in applicable if item.impact is not CriterionImpact.SATISFIED
    ]
    live_dispositions = [
        item.disposition
        if item.disposition_expires_at is not None and at < item.disposition_expires_at
        else None
        for item in unsatisfied
    ]
    if all(item.impact is CriterionImpact.NONMATERIAL_GAP for item in unsatisfied):
        if all(value is ConcernDisposition.SUPPRESSED for value in live_dispositions):
            return ConcernState.SUPPRESSED
        if all(value is None for value in live_dispositions):
            return ConcernState.SUPPRESSED
    if all(item.impact is CriterionImpact.MATERIAL_GAP for item in unsatisfied):
        if all(
            value is ConcernDisposition.ACCEPTED_RISK for value in live_dispositions
        ):
            return ConcernState.ACCEPTED_RISK
        if all(value is ConcernDisposition.DISMISSED for value in live_dispositions):
            return ConcernState.DISMISSED
    actionable = any(
        item.work_eligibility is CriterionWorkEligibility.ACTIONABLE
        and live_disposition is None
        for item, live_disposition in zip(unsatisfied, live_dispositions, strict=True)
    )
    return ConcernState.OPEN if actionable else ConcernState.SUSPENDED


_LEGAL_CONCERN_TRANSITIONS: dict[ConcernState, frozenset[ConcernState]] = {
    ConcernState.CANDIDATE: frozenset(ConcernState),
    ConcernState.OPEN: frozenset(ConcernState),
    ConcernState.SUSPENDED: frozenset(ConcernState),
    ConcernState.SUPPRESSED: frozenset(ConcernState),
    ConcernState.ACCEPTED_RISK: frozenset(ConcernState),
    ConcernState.DISMISSED: frozenset(ConcernState),
    ConcernState.RESOLVED: frozenset(ConcernState),
    ConcernState.EXPIRED: frozenset(ConcernState),
    ConcernState.INVALIDATED: frozenset({ConcernState.INVALIDATED}),
}


class ConcernTransition(_AgencyContract):
    concern_id: UUID
    from_version: int = Field(ge=0)
    to_version: int = Field(ge=1)
    from_state: ConcernState | None
    to_state: ConcernState
    cause: str = Field(min_length=1)
    transitioned_at: datetime

    @field_validator("transitioned_at")
    @classmethod
    def transition_time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="transitioned_at")

    @model_validator(mode="after")
    def transition_is_legal(self) -> Self:
        if self.to_version != self.from_version + 1:
            raise ValueError(
                "concern transition must advance exactly one aggregate version"
            )
        if self.from_state is None:
            if self.from_version != 0 or self.to_state is not ConcernState.CANDIDATE:
                raise ValueError("new concerns begin at candidate version one")
        elif self.to_state not in _LEGAL_CONCERN_TRANSITIONS[self.from_state]:
            raise ValueError("illegal concern transition")
        return self


class PredictionKind(StrEnum):
    STATE_FORECAST = "state_forecast"
    EVENT_FORECAST = "event_forecast"
    INTERVENTION_EFFECT = "intervention_effect"
    COMPARATIVE_POLICY = "comparative_policy"
    SETTLEMENT_EXPECTATION = "settlement_expectation"


class Prediction(_AgencyContract):
    prediction_id: UUID
    tenant_id: UUID
    episode_id: UUID
    kind: PredictionKind
    target: dict[str, Any]
    probability_distribution: dict[str, float]
    metric_definition: str = Field(min_length=1)
    evidence_cutoff: datetime
    forecast_window_start: datetime
    forecast_window_end: datetime
    assumptions: tuple[str, ...] = ()
    censoring_rule: str = Field(min_length=1)
    intervention_spec_digest: str | None = None
    comparator: dict[str, Any] | None = None
    baseline: dict[str, Any] | None = None
    policy_version_refs: tuple[str, ...] = ()
    assignment_rule_ref: str | None = None
    preregistered_at: datetime

    @field_validator(
        "evidence_cutoff",
        "forecast_window_start",
        "forecast_window_end",
        "preregistered_at",
    )
    @classmethod
    def prediction_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def kind_specific_contract_is_complete(self) -> Self:
        if self.forecast_window_end <= self.forecast_window_start:
            raise ValueError("prediction forecast window is empty")
        if self.evidence_cutoff > self.preregistered_at:
            raise ValueError("prediction evidence cutoff cannot follow preregistration")
        if self.preregistered_at > self.forecast_window_start:
            raise ValueError(
                "prediction was registered after its exposure window began"
            )
        if any(
            value < 0.0 or value > 1.0
            for value in self.probability_distribution.values()
        ):
            raise ValueError("prediction probabilities must lie in [0, 1]")
        if abs(sum(self.probability_distribution.values()) - 1.0) > 1e-6:
            raise ValueError("prediction distribution must sum to one")
        if self.kind is PredictionKind.INTERVENTION_EFFECT:
            if not (
                self.intervention_spec_digest and self.comparator and self.baseline
            ):
                raise ValueError(
                    "intervention-effect prediction requires spec, comparator, and baseline"
                )
        if self.kind is PredictionKind.COMPARATIVE_POLICY:
            if len(self.policy_version_refs) < 2 or not self.assignment_rule_ref:
                raise ValueError(
                    "comparative-policy prediction requires policies and assignment rule"
                )
        if (
            self.kind
            not in {
                PredictionKind.INTERVENTION_EFFECT,
                PredictionKind.COMPARATIVE_POLICY,
            }
            and self.comparator is not None
        ):
            raise ValueError(
                "noncausal prediction kinds cannot imply a treatment comparator"
            )
        return self

    @property
    def prediction_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class InterventionSpec(_AgencyContract):
    spec_id: UUID
    tenant_id: UUID
    episode_id: UUID
    target_referent: CanonicalReferent
    target_version: str = Field(min_length=1)
    operation: str = Field(min_length=1)
    parameters: dict[str, Any]
    comparator: dict[str, Any]
    outcome_metric: str = Field(min_length=1)
    outcome_window_start: datetime
    outcome_window_end: datetime
    workflow_spec_version_ref: str | None = None
    action_adapter_version: str = Field(min_length=1)
    action_adapter_capability_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    safety_and_preconditions: tuple[str, ...] = Field(min_length=1)
    authority_requirement: str = Field(min_length=1)
    reversible: bool
    compensation_declaration: str
    grounding_dependency_refs: tuple[str, ...] = Field(min_length=1)
    context_dependency_manifest_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("outcome_window_start", "outcome_window_end")
    @classmethod
    def intervention_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def spec_is_executable_and_measurable(self) -> Self:
        if self.outcome_window_end <= self.outcome_window_start:
            raise ValueError("intervention outcome window is empty")
        if not self.parameters:
            raise ValueError("intervention parameters cannot be empty")
        if self.reversible and not self.compensation_declaration:
            raise ValueError(
                "reversible intervention requires compensation declaration"
            )
        return self

    @property
    def spec_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ConsequentialProposalFate(StrEnum):
    OPEN = "open"
    DEFERRED = "deferred"
    ACCEPTED_FOR_AUTHORIZATION = "accepted_for_authorization"
    REJECTED = "rejected"
    EXPIRED = "expired"
    SUPERSEDED = "superseded"

    @property
    def terminal(self) -> bool:
        return self in {
            ConsequentialProposalFate.ACCEPTED_FOR_AUTHORIZATION,
            ConsequentialProposalFate.REJECTED,
            ConsequentialProposalFate.EXPIRED,
            ConsequentialProposalFate.SUPERSEDED,
        }


class ConsequentialProposal(_AgencyContract):
    """A recommendation, never authority, over one immutable action identity."""

    proposal_id: UUID
    tenant_id: UUID
    proposal_version: int = Field(default=1, ge=1)
    episode_id: UUID
    intervention_spec: InterventionSpec
    summary: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    alternative_refs: tuple[str, ...] = Field(min_length=1)
    source_refs: tuple[str, ...] = Field(min_length=1)
    processing_authority: ProcessingAuthorityContext
    processing_authority_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: datetime
    review_due_at: datetime
    fate: ConsequentialProposalFate = ConsequentialProposalFate.OPEN

    @field_validator("created_at", "review_due_at")
    @classmethod
    def proposal_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def proposal_is_an_exact_non_authorizing_candidate(self) -> Self:
        if self.proposal_version != 1:
            raise ValueError("material proposal changes create a new proposal identity")
        if self.fate is not ConsequentialProposalFate.OPEN:
            raise ValueError("new consequential proposals begin open")
        if self.intervention_spec.tenant_id != self.tenant_id:
            raise ValueError("proposal and InterventionSpec tenant mismatch")
        if self.intervention_spec.episode_id != self.episode_id:
            raise ValueError("proposal and InterventionSpec episode mismatch")
        if self.processing_authority.tenant_id != self.tenant_id:
            raise ValueError("proposal processing authority tenant mismatch")
        if (
            self.processing_authority.fingerprint
            != self.processing_authority_fingerprint
        ):
            raise ValueError("proposal processing authority fingerprint mismatch")
        if not self.processing_authority.is_live(self.created_at):
            raise ValueError("proposal processing authority was not live")
        if self.review_due_at <= self.created_at:
            raise ValueError("proposal review deadline must follow creation")
        return self

    @property
    def intervention_spec_digest(self) -> str:
        return self.intervention_spec.spec_digest

    @property
    def proposal_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ConsequentialProposalReview(_AgencyContract):
    review_id: UUID
    tenant_id: UUID
    proposal_id: UUID
    proposal_version: int = Field(ge=1)
    proposal_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    intervention_spec_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    from_fate: ConsequentialProposalFate
    to_fate: ConsequentialProposalFate
    principal_or_policy_ref: str = Field(min_length=1)
    authority: ConsumptionAuthorityContext
    reason: str = Field(min_length=1)
    decided_at: datetime

    @field_validator("decided_at")
    @classmethod
    def decision_time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="decided_at")

    @model_validator(mode="after")
    def review_is_live_and_directional(self) -> Self:
        allowed = {
            ConsequentialProposalFate.OPEN: {
                ConsequentialProposalFate.DEFERRED,
                ConsequentialProposalFate.ACCEPTED_FOR_AUTHORIZATION,
                ConsequentialProposalFate.REJECTED,
                ConsequentialProposalFate.EXPIRED,
                ConsequentialProposalFate.SUPERSEDED,
            },
            ConsequentialProposalFate.DEFERRED: {
                ConsequentialProposalFate.OPEN,
                ConsequentialProposalFate.ACCEPTED_FOR_AUTHORIZATION,
                ConsequentialProposalFate.REJECTED,
                ConsequentialProposalFate.EXPIRED,
                ConsequentialProposalFate.SUPERSEDED,
            },
        }
        if self.from_fate not in allowed or self.to_fate not in allowed[self.from_fate]:
            raise ValueError("illegal consequential proposal review transition")
        if self.authority.tenant_id != self.tenant_id:
            raise ValueError("proposal review authority tenant mismatch")
        if not self.authority.is_live(self.decided_at):
            raise ValueError("proposal review authority was not live")
        return self


class AuthorizationDisposition(StrEnum):
    AUTHORIZED = "authorized"
    REJECTED = "rejected"


class AuthorizationDecision(_AgencyContract):
    decision_id: UUID
    tenant_id: UUID
    proposal_id: UUID
    proposal_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    intervention_spec_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    disposition: AuthorizationDisposition
    principal_or_policy_ref: str = Field(min_length=1)
    authority: ConsumptionAuthorityContext
    exact_operations: frozenset[str] = Field(min_length=1)
    exact_target_refs: frozenset[str] = Field(min_length=1)
    exact_field_paths: frozenset[str] = Field(min_length=1)
    constraints: dict[str, Any]
    use_budget: int = Field(ge=0)
    attempt_budget: int = Field(ge=0)
    decided_at: datetime
    expires_at: datetime

    @field_validator("decided_at", "expires_at")
    @classmethod
    def authorization_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def authorization_is_exact_and_live(self) -> Self:
        if self.authority.tenant_id != self.tenant_id:
            raise ValueError("authorization authority tenant mismatch")
        if self.expires_at <= self.decided_at:
            raise ValueError("authorization expiry must follow decision")
        if self.disposition is AuthorizationDisposition.AUTHORIZED:
            if self.use_budget < 1 or self.attempt_budget < 1:
                raise ValueError(
                    "authorized action requires nonzero use and attempt budgets"
                )
            if not self.authority.is_live(self.decided_at):
                raise ValueError("authorization authority was not live")
        return self

    @property
    def decision_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class SettlementDisposition(StrEnum):
    SETTLED = "settled"
    CENSORED = "censored"
    INCOMPARABLE = "incomparable"
    MEASUREMENT_UNAVAILABLE = "measurement_unavailable"


class ResidualClass(StrEnum):
    MODEL = "model"
    EXECUTION = "execution"
    MEASUREMENT = "measurement"
    TIMING = "timing"
    ASSUMPTION = "assumption"
    EXTERNAL_SHOCK = "external_shock"
    CONFOUNDING = "confounding"
    NON_IDENTIFIABLE = "non_identifiable"


class Outcome(_AgencyContract):
    outcome_id: UUID
    tenant_id: UUID
    episode_id: UUID
    metric_definition: str = Field(min_length=1)
    observed_value: Any
    observed_at: datetime
    valid_time: datetime
    source_evidence_refs: tuple[str, ...] = Field(min_length=1)
    independent_of_execution_claim: bool
    measurement_quality: float = Field(ge=0.0, le=1.0)

    @field_validator("observed_at", "valid_time")
    @classmethod
    def outcome_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def measurement_does_not_precede_valid_time(self) -> Self:
        if self.observed_at < self.valid_time:
            raise ValueError("outcome cannot be observed before its valid time")
        return self

    @property
    def outcome_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class Settlement(_AgencyContract):
    settlement_id: UUID
    prediction_id: UUID
    outcome_id: UUID | None = None
    disposition: SettlementDisposition
    settled_at: datetime
    comparison_result: dict[str, Any] | None = None
    reason_codes: tuple[str, ...] = Field(min_length=1)
    residual_distribution: dict[ResidualClass, float] = Field(default_factory=dict)

    @field_validator("settled_at")
    @classmethod
    def settlement_time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="settled_at")

    @model_validator(mode="after")
    def settlement_is_honest_about_observability(self) -> Self:
        if self.disposition is SettlementDisposition.SETTLED:
            if not (self.outcome_id and self.comparison_result):
                raise ValueError("settled prediction requires outcome and comparison")
        if self.residual_distribution:
            if any(
                value < 0.0 or value > 1.0
                for value in self.residual_distribution.values()
            ):
                raise ValueError("residual probabilities must lie in [0, 1]")
            if abs(sum(self.residual_distribution.values()) - 1.0) > 1e-6:
                raise ValueError("residual distribution must sum to one")
        return self

    @property
    def settlement_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class Attribution(_AgencyContract):
    attribution_id: UUID
    episode_id: UUID
    subject_ref: str = Field(min_length=1)
    attributed_effect_distribution: dict[str, float]
    causal_confidence: float = Field(ge=0.0, le=1.0)
    method: str = Field(min_length=1)
    evidence_refs: tuple[str, ...] = Field(min_length=1)
    withheld_credit: bool = False
    withholding_reason: str | None = None

    @model_validator(mode="after")
    def withheld_credit_is_explained(self) -> Self:
        if self.withheld_credit and not self.withholding_reason:
            raise ValueError("withheld attribution requires a reason")
        if not self.withheld_credit and self.withholding_reason:
            raise ValueError(
                "withholding reason is invalid when credit was not withheld"
            )
        return self

    @property
    def attribution_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class EpisodeStageFate(StrEnum):
    PRESENT = "present"
    REJECTED = "rejected"
    EXPIRED = "expired"
    INFEASIBLE = "infeasible"
    NOT_EXECUTED = "not_executed"
    CENSORED = "censored"
    MEASUREMENT_UNAVAILABLE = "measurement_unavailable"
    NO_INTERVENTION_SELECTED = "no_intervention_selected"


class EpisodeStageLink(_AgencyContract):
    stage: str = Field(min_length=1)
    fate: EpisodeStageFate
    object_ref: str | None = None
    writer_id: str | None = None
    reason: str | None = None

    @model_validator(mode="after")
    def stage_has_object_or_typed_absence(self) -> Self:
        if self.fate is EpisodeStageFate.PRESENT:
            if not (self.object_ref and self.writer_id) or self.reason:
                raise ValueError(
                    "present stage requires object and writer, not absence reason"
                )
        elif self.object_ref or self.writer_id or not self.reason:
            raise ValueError("absent stage requires a reason and no object claim")
        return self


class InterventionEpisode(_AgencyContract):
    episode_id: UUID
    tenant_id: UUID
    kind: str = Field(default="intervention", min_length=1)
    intervention_spec_digest: str | None = None
    stage_links: tuple[EpisodeStageLink, ...] = Field(min_length=1)
    created_at: datetime
    updated_at: datetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def episode_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def stages_are_unique_and_coherent(self) -> Self:
        if self.updated_at < self.created_at:
            raise ValueError("episode update cannot precede creation")
        names = [item.stage for item in self.stage_links]
        if len(names) != len(set(names)):
            raise ValueError("episode stage links must be unique")
        if self.intervention_spec_digest is not None and not all(
            len(self.intervention_spec_digest) == 64 and character in "0123456789abcdef"
            for character in self.intervention_spec_digest
        ):
            raise ValueError("intervention spec digest must be canonical sha256")
        return self

    @property
    def episode_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class AgencyWriteContext(_AgencyContract):
    """Shared exact command context for consequential semantic writers."""

    command_id: UUID
    tenant_id: UUID
    processing_authority: ProcessingAuthorityContext
    writer_scope_epoch: WriterScopeEpoch
    idempotency_key: str = Field(min_length=1)
    issued_at: datetime
    expires_at: datetime

    @field_validator("issued_at", "expires_at")
    @classmethod
    def command_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def command_context_is_live_and_tenant_scoped(self) -> Self:
        if self.expires_at <= self.issued_at:
            raise ValueError("agency command expiry must follow issuance")
        if self.processing_authority.tenant_id != self.tenant_id:
            raise ValueError("agency command processing authority tenant mismatch")
        if not self.processing_authority.is_live(self.issued_at):
            raise ValueError("agency command processing authority was not live")
        if self.writer_scope_epoch.tenant_id != self.tenant_id:
            raise ValueError("agency command writer scope tenant mismatch")
        return self

    def require_writer(self, *, owner: str, responsibility: str) -> None:
        if not self.writer_scope_epoch.permits(
            writer_owner=owner,
            epoch=self.writer_scope_epoch.epoch,
            tenant_id=self.tenant_id,
            semantic_responsibility=responsibility,
            source_partition=str(self.tenant_id),
        ):
            raise ValueError(f"writer scope does not permit {owner}")


class ConsequentialProposalRegistrationCommand(_AgencyContract):
    context: AgencyWriteContext
    proposal: ConsequentialProposal

    @model_validator(mode="after")
    def command_binds_proposal_writer(self) -> Self:
        if self.proposal.tenant_id != self.context.tenant_id:
            raise ValueError("proposal command tenant mismatch")
        if (
            self.proposal.processing_authority_fingerprint
            != self.context.processing_authority.fingerprint
        ):
            raise ValueError("proposal command processing authority mismatch")
        self.context.require_writer(
            owner="ProposalAppender", responsibility="consequential_proposal"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ConsequentialProposalReviewCommand(_AgencyContract):
    context: AgencyWriteContext
    review: ConsequentialProposalReview

    @model_validator(mode="after")
    def command_binds_review_writer(self) -> Self:
        if self.review.tenant_id != self.context.tenant_id:
            raise ValueError("proposal review command tenant mismatch")
        self.context.require_writer(
            owner="ProposalAppender", responsibility="consequential_proposal"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class EpisodeUpdateCommand(_AgencyContract):
    context: AgencyWriteContext
    expected_version: int = Field(ge=0)
    episode: InterventionEpisode

    @model_validator(mode="after")
    def command_binds_episode_coordinator(self) -> Self:
        if self.episode.tenant_id != self.context.tenant_id:
            raise ValueError("episode command tenant mismatch")
        if self.episode.updated_at > self.context.issued_at:
            raise ValueError("episode update cannot be future-dated")
        self.context.require_writer(
            owner="EpisodeCoordinator", responsibility="intervention_episode"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class PredictionRegistrationCommand(_AgencyContract):
    context: AgencyWriteContext
    prediction: Prediction

    @model_validator(mode="after")
    def command_binds_prediction_writer(self) -> Self:
        if self.prediction.tenant_id != self.context.tenant_id:
            raise ValueError("prediction command tenant mismatch")
        if self.prediction.preregistered_at != self.context.issued_at:
            raise ValueError("prediction preregistration cannot be backdated")
        self.context.require_writer(
            owner="PredictionWriter", responsibility="prediction"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class AuthorizationDecisionCommand(_AgencyContract):
    context: AgencyWriteContext
    decision: AuthorizationDecision

    @model_validator(mode="after")
    def command_binds_authorization_writer(self) -> Self:
        if self.decision.tenant_id != self.context.tenant_id:
            raise ValueError("authorization command tenant mismatch")
        if self.decision.decided_at != self.context.issued_at:
            raise ValueError("authorization decision cannot be backdated")
        self.context.require_writer(
            owner="AuthorizationApplier", responsibility="authorization"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class OutcomeRecordingCommand(_AgencyContract):
    context: AgencyWriteContext
    outcome: Outcome

    @model_validator(mode="after")
    def command_binds_outcome_recorder(self) -> Self:
        if self.outcome.tenant_id != self.context.tenant_id:
            raise ValueError("outcome command tenant mismatch")
        if self.outcome.observed_at > self.context.issued_at:
            raise ValueError("outcome observation cannot be future-dated")
        if not self.outcome.independent_of_execution_claim:
            raise ValueError(
                "canonical Outcome requires an independent measurement claim"
            )
        self.context.require_writer(owner="OutcomeRecorder", responsibility="outcome")
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class SettlementCommand(_AgencyContract):
    context: AgencyWriteContext
    settlement: Settlement

    @model_validator(mode="after")
    def command_binds_settlement_writer(self) -> Self:
        if self.settlement.settled_at != self.context.issued_at:
            raise ValueError("settlement cannot be backdated")
        self.context.require_writer(
            owner="SettlementApplier", responsibility="settlement"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class AttributionCommand(_AgencyContract):
    context: AgencyWriteContext
    settlement_id: UUID
    attribution: Attribution

    @model_validator(mode="after")
    def command_binds_attribution_writer(self) -> Self:
        self.context.require_writer(
            owner="AttributionApplier", responsibility="attribution"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


__all__ = [
    "AgencyWriteContext",
    "AttentionGovernanceBinding",
    "AttentionSourceKind",
    "Attribution",
    "AttributionCommand",
    "AuthorityBasisChange",
    "AuthorityBasisChangeKind",
    "AuthorityBasisSurvivalMode",
    "AuthorityBasisSurvivalPolicy",
    "AuthorizationDecision",
    "AuthorizationDecisionCommand",
    "AuthorizationDisposition",
    "ConcernCriterionState",
    "ConcernDisposition",
    "ConcernEvaluationCommand",
    "ConcernIdentity",
    "ConcernIdentityCorrectionCommand",
    "ConcernSnapshot",
    "ConcernState",
    "ConcernTransition",
    "ConsequentialProposal",
    "ConsequentialProposalFate",
    "ConsequentialProposalRegistrationCommand",
    "ConsequentialProposalReview",
    "ConsequentialProposalReviewCommand",
    "ConstitutiveIntentAuthorityBasis",
    "ConstitutiveIntentAuthorityBasisKind",
    "CriterionImpact",
    "CriterionWorkEligibility",
    "EpisodeStageFate",
    "EpisodeStageLink",
    "EpisodeUpdateCommand",
    "ExactProposalAcceptance",
    "EffectiveAttentionGovernanceEnvelope",
    "IntentDependentFate",
    "IntentGroundingDependency",
    "IntentMutation",
    "IntentObjectKind",
    "IntentOperation",
    "IntentProposalFate",
    "InterpretedIntentProposal",
    "InterventionEpisode",
    "InterventionSpec",
    "Outcome",
    "OutcomeRecordingCommand",
    "Prediction",
    "PredictionKind",
    "PredictionRegistrationCommand",
    "ResidualClass",
    "Settlement",
    "SettlementCommand",
    "SettlementDisposition",
    "TypedConstitutiveIntentCommand",
    "compose_attention_governance_bindings",
    "derive_concern_id",
    "reduce_concern_state",
    "reduce_intent_basis_change",
]
