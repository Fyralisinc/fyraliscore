"""Executable contracts for proportional work, liveness, and governed learning."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .agency import AgencyWriteContext
from .kernel import ConsumptionAuthorityContext, canonical_sha256


class _FrozenContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        use_enum_values=False,
    )


class ProcessingClass(StrEnum):
    """Ordered semantic resolution classes from preservation to agency."""

    R0_PRESERVE = "R0"
    R1_MINIMAL_INTERPRETATION = "R1"
    R2_PROVISIONAL_GROUNDING = "R2"
    R3_DURABLE_UNDERSTANDING = "R3"
    R4_CONSEQUENTIAL_DECISION_SUPPORT = "R4"
    R5_EXTERNAL_AGENCY = "R5"

    @property
    def rank(self) -> int:
        return int(self.value[1:])

    def at_least(self, other: ProcessingClass) -> bool:
        return self.rank >= other.rank


class EconomicUsage(_FrozenContract):
    source_reads: int = Field(default=0, ge=0)
    model_calls: int = Field(default=0, ge=0)
    model_tokens: int = Field(default=0, ge=0)
    latency_ms: int = Field(default=0, ge=0)
    storage_bytes: int = Field(default=0, ge=0)
    write_amplification: float = Field(default=0.0, ge=0.0)
    repair_fanout: int = Field(default=0, ge=0)
    provider_cost_micros: int = Field(default=0, ge=0)
    human_attention_seconds: int = Field(default=0, ge=0)

    def plus(self, other: EconomicUsage) -> EconomicUsage:
        return EconomicUsage(
            source_reads=self.source_reads + other.source_reads,
            model_calls=self.model_calls + other.model_calls,
            model_tokens=self.model_tokens + other.model_tokens,
            latency_ms=self.latency_ms + other.latency_ms,
            storage_bytes=self.storage_bytes + other.storage_bytes,
            write_amplification=(self.write_amplification + other.write_amplification),
            repair_fanout=self.repair_fanout + other.repair_fanout,
            provider_cost_micros=(
                self.provider_cost_micros + other.provider_cost_micros
            ),
            human_attention_seconds=(
                self.human_attention_seconds + other.human_attention_seconds
            ),
        )


class EconomicOperatingEnvelope(_FrozenContract):
    """Versioned upper bounds for the complete lawful path of one work class."""

    policy_version: str = Field(min_length=1)
    max_source_reads: int | None = Field(default=None, ge=0)
    max_model_calls: int | None = Field(default=None, ge=0)
    max_model_tokens: int | None = Field(default=None, ge=0)
    max_latency_ms: int | None = Field(default=None, ge=0)
    max_storage_bytes: int | None = Field(default=None, ge=0)
    max_write_amplification: float | None = Field(default=None, ge=0.0)
    max_repair_fanout: int | None = Field(default=None, ge=0)
    max_provider_cost_micros: int | None = Field(default=None, ge=0)
    max_human_attention_seconds: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def require_a_declared_bound(self) -> Self:
        limits = self.model_dump(exclude={"policy_version"})
        if not any(value is not None for value in limits.values()):
            raise ValueError("an economic envelope must declare at least one bound")
        return self

    def violations(self, usage: EconomicUsage) -> tuple[str, ...]:
        violations: list[str] = []
        for usage_field, limit_field in (
            ("source_reads", "max_source_reads"),
            ("model_calls", "max_model_calls"),
            ("model_tokens", "max_model_tokens"),
            ("latency_ms", "max_latency_ms"),
            ("storage_bytes", "max_storage_bytes"),
            ("write_amplification", "max_write_amplification"),
            ("repair_fanout", "max_repair_fanout"),
            ("provider_cost_micros", "max_provider_cost_micros"),
            ("human_attention_seconds", "max_human_attention_seconds"),
        ):
            limit = getattr(self, limit_field)
            if limit is not None and getattr(usage, usage_field) > limit:
                violations.append(usage_field)
        return tuple(violations)

    def permits(self, usage: EconomicUsage) -> bool:
        return not self.violations(usage)


class ProcessingFactors(_FrozenContract):
    consequence: float = Field(default=0.0, ge=0.0, le=1.0)
    uncertainty: float = Field(default=0.0, ge=0.0, le=1.0)
    irreversibility: float = Field(default=0.0, ge=0.0, le=1.0)
    authority_sensitivity: float = Field(default=0.0, ge=0.0, le=1.0)
    expected_value: float = Field(default=0.0, ge=0.0, le=1.0)
    required_floor: ProcessingClass = ProcessingClass.R0_PRESERVE
    durable_output: bool = False
    consequential_decision: bool = False
    external_effect: bool = False

    @property
    def rigor_score(self) -> float:
        return round(
            0.30 * self.consequence
            + 0.20 * self.uncertainty
            + 0.20 * self.irreversibility
            + 0.20 * self.authority_sensitivity
            + 0.10 * self.expected_value,
            6,
        )

    @property
    def semantic_floor(self) -> ProcessingClass:
        floor = self.required_floor
        if self.durable_output and not floor.at_least(
            ProcessingClass.R3_DURABLE_UNDERSTANDING
        ):
            floor = ProcessingClass.R3_DURABLE_UNDERSTANDING
        if self.consequential_decision and not floor.at_least(
            ProcessingClass.R4_CONSEQUENTIAL_DECISION_SUPPORT
        ):
            floor = ProcessingClass.R4_CONSEQUENTIAL_DECISION_SUPPORT
        if self.external_effect:
            floor = ProcessingClass.R5_EXTERNAL_AGENCY
        return floor


class ProcessingClassPolicy(_FrozenContract):
    version: str = Field(min_length=1)
    r1_threshold: float = Field(default=0.05, ge=0.0, le=1.0)
    r2_threshold: float = Field(default=0.20, ge=0.0, le=1.0)
    r3_threshold: float = Field(default=0.42, ge=0.0, le=1.0)
    r4_threshold: float = Field(default=0.68, ge=0.0, le=1.0)
    r5_threshold: float = Field(default=0.88, ge=0.0, le=1.0)
    ceiling: ProcessingClass = ProcessingClass.R5_EXTERNAL_AGENCY

    @model_validator(mode="after")
    def thresholds_are_monotone(self) -> Self:
        thresholds = (
            self.r1_threshold,
            self.r2_threshold,
            self.r3_threshold,
            self.r4_threshold,
            self.r5_threshold,
        )
        if tuple(sorted(thresholds)) != thresholds or len(set(thresholds)) != 5:
            raise ValueError("processing thresholds must be strictly increasing")
        return self


class ProcessingClassDecision(_FrozenContract):
    policy_version: str = Field(min_length=1)
    selected: ProcessingClass
    factors: ProcessingFactors
    ceiling: ProcessingClass
    reason_codes: tuple[str, ...] = ()
    economic_envelope: EconomicOperatingEnvelope
    escalated_from: ProcessingClass | None = None

    @model_validator(mode="after")
    def selected_class_is_lawful(self) -> Self:
        if not self.selected.at_least(self.factors.semantic_floor):
            raise ValueError("selected processing class is below the semantic floor")
        if self.selected.rank > self.ceiling.rank:
            raise ValueError("selected processing class exceeds the policy ceiling")
        if self.escalated_from is not None and (
            self.escalated_from.rank >= self.selected.rank
        ):
            raise ValueError("escalation must move to a strictly higher class")
        return self


def select_processing_class(
    *,
    factors: ProcessingFactors,
    policy: ProcessingClassPolicy,
    economic_envelope: EconomicOperatingEnvelope,
) -> ProcessingClassDecision:
    """Select the cheapest class satisfying both score and semantic floor."""

    score = factors.rigor_score
    if score >= policy.r5_threshold:
        selected = ProcessingClass.R5_EXTERNAL_AGENCY
    elif score >= policy.r4_threshold:
        selected = ProcessingClass.R4_CONSEQUENTIAL_DECISION_SUPPORT
    elif score >= policy.r3_threshold:
        selected = ProcessingClass.R3_DURABLE_UNDERSTANDING
    elif score >= policy.r2_threshold:
        selected = ProcessingClass.R2_PROVISIONAL_GROUNDING
    elif score >= policy.r1_threshold:
        selected = ProcessingClass.R1_MINIMAL_INTERPRETATION
    else:
        selected = ProcessingClass.R0_PRESERVE

    reasons = ["rigor_score"]
    floor = factors.semantic_floor
    if floor.rank > selected.rank:
        selected = floor
        reasons.append("semantic_floor")
    if selected.rank > policy.ceiling.rank:
        raise ValueError("policy ceiling cannot satisfy the semantic floor")
    return ProcessingClassDecision(
        policy_version=policy.version,
        selected=selected,
        factors=factors,
        ceiling=policy.ceiling,
        reason_codes=tuple(reasons),
        economic_envelope=economic_envelope,
    )


class UsefulSafeFateKind(StrEnum):
    COMPLETE = "complete"
    BOUNDED_PARTIAL = "bounded_partial"
    CLARIFICATION = "clarification"
    EXPLICIT_UNKNOWN = "explicit_unknown"
    DEFERRED = "deferred"
    IMPOSSIBLE = "impossible"
    ABSTAINED = "abstained"
    NON_INTERRUPTION = "non_interruption"
    FAILED = "failed"


class UsefulSafeFate(_FrozenContract):
    kind: UsefulSafeFateKind
    processing_class: ProcessingClass
    result_ref: str | None = None
    result_summary: str | None = None
    usefulness_ceiling: float = Field(default=0.0, ge=0.0, le=1.0)
    omissions: tuple[str, ...] = ()
    unknowns: tuple[str, ...] = ()
    stop_reason: str = Field(min_length=1)
    spent: EconomicUsage = Field(default_factory=EconomicUsage)
    capable_next_actor: str | None = None
    wake_condition: str | None = None
    material_unresolved: bool = False
    safety_incident_ids: tuple[str, ...] = ()
    terminal_at: datetime | None = None

    @model_validator(mode="after")
    def fate_is_explicit_and_actionable(self) -> Self:
        result_kinds = {
            UsefulSafeFateKind.COMPLETE,
            UsefulSafeFateKind.BOUNDED_PARTIAL,
        }
        if self.kind in result_kinds and not (self.result_ref or self.result_summary):
            raise ValueError("result fates require a result reference or summary")
        if self.kind is UsefulSafeFateKind.COMPLETE and (
            self.omissions or self.unknowns or self.material_unresolved
        ):
            raise ValueError("a complete fate cannot hide omissions or unknowns")
        if self.kind is UsefulSafeFateKind.CLARIFICATION and not (
            self.capable_next_actor and self.result_summary
        ):
            raise ValueError(
                "clarification requires a capable next actor and exact question"
            )
        if self.kind is UsefulSafeFateKind.DEFERRED and not self.wake_condition:
            raise ValueError("deferred work requires an exact wake condition")
        if self.material_unresolved and not self.wake_condition:
            raise ValueError("material unresolved work requires a wake condition")
        return self

    @property
    def is_safe(self) -> bool:
        return not self.safety_incident_ids

    @property
    def is_useful_result(self) -> bool:
        return (
            self.is_safe
            and self.usefulness_ceiling > 0.0
            and self.kind
            in {
                UsefulSafeFateKind.COMPLETE,
                UsefulSafeFateKind.BOUNDED_PARTIAL,
                UsefulSafeFateKind.CLARIFICATION,
            }
        )

    @property
    def is_justified_no_result(self) -> bool:
        return self.is_safe and self.kind in {
            UsefulSafeFateKind.EXPLICIT_UNKNOWN,
            UsefulSafeFateKind.DEFERRED,
            UsefulSafeFateKind.IMPOSSIBLE,
            UsefulSafeFateKind.ABSTAINED,
            UsefulSafeFateKind.NON_INTERRUPTION,
        }


class AdaptiveLoopState(StrEnum):
    COLD_START = "cold_start"
    SHADOW = "shadow"
    CANDIDATE = "candidate"
    ACTIVE = "active"
    FROZEN = "frozen"
    ROLLED_BACK = "rolled_back"
    EXPIRED = "expired"


class BootstrapPolicy(_FrozenContract):
    adaptive_family: str = Field(min_length=1)
    version: str = Field(min_length=1)
    governed_prior: str = Field(min_length=1)
    cold_start_behavior: str = Field(min_length=1)
    shadow_behavior: str = Field(min_length=1)
    minimum_independent_evidence: int = Field(ge=1)
    promotion_metric_id: str = Field(min_length=1)
    minimum_effect: float
    maximum_harm_rate: float = Field(ge=0.0, le=1.0)
    frozen_fallback: str = Field(min_length=1)
    rollback_trigger: str = Field(min_length=1)
    expiry_behavior: str = Field(min_length=1)
    initial_state: AdaptiveLoopState = AdaptiveLoopState.COLD_START
    independent_evidence_required: bool = True

    @model_validator(mode="after")
    def prevent_self_promoting_bootstrap(self) -> Self:
        if not self.independent_evidence_required:
            raise ValueError("bootstrap promotion must require independent evidence")
        if self.initial_state is AdaptiveLoopState.ACTIVE:
            raise ValueError(
                "an adaptive loop cannot bootstrap directly into active state"
            )
        return self

    @property
    def bootstrap_policy_ref(self) -> str:
        return f"bootstrap-policy:{self.adaptive_family}:{self.version}"

    @property
    def bootstrap_policy_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class RepresentationScopeKind(StrEnum):
    CANDIDATE = "candidate"
    FAMILY_COHORT = "family_cohort"


class RepresentationAdmissionScope(_FrozenContract):
    scope_id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    kind: RepresentationScopeKind
    relation_family: str = Field(min_length=1)
    consumer: str = Field(min_length=1)
    risk_class: str = Field(min_length=1)
    domain: str = Field(min_length=1)
    organization_cohort: str = Field(min_length=1)
    membership_version: str = Field(min_length=1)
    candidate_id: str | None = None
    exclusions: tuple[str, ...] = ()

    @model_validator(mode="after")
    def candidate_scope_has_exact_candidate(self) -> Self:
        if self.kind is RepresentationScopeKind.CANDIDATE and not self.candidate_id:
            raise ValueError("candidate scope requires candidate_id")
        if self.kind is RepresentationScopeKind.FAMILY_COHORT and self.candidate_id:
            raise ValueError("family/cohort scope cannot carry candidate_id")
        return self

    def contains(self, other: RepresentationAdmissionScope) -> bool:
        common_matches = all(
            getattr(self, field) == getattr(other, field)
            for field in (
                "relation_family",
                "consumer",
                "risk_class",
                "domain",
                "organization_cohort",
                "membership_version",
            )
        )
        if not common_matches:
            return False
        if self.kind is RepresentationScopeKind.FAMILY_COHORT:
            return other.candidate_id not in self.exclusions
        return self.candidate_id == other.candidate_id

    def is_no_broader_than(self, other: RepresentationAdmissionScope) -> bool:
        return other.contains(self)


class TenantInfluenceDisposition(StrEnum):
    UNAFFECTED_PROVEN = "unaffected_proven"
    RESTRICTED = "restricted"
    REPLACED = "replaced"
    RETRAINED = "retrained"
    UNLEARNED = "unlearned"
    RESIDUAL_DECLARED = "residual_declared"
    PENDING = "pending"


class TenantInfluenceLineage(_FrozenContract):
    lineage_id: str = Field(min_length=1)
    tenant_id: UUID
    purpose: str = Field(min_length=1)
    source_artifact_ids: tuple[str, ...] = Field(min_length=1)
    contribution_class: str = Field(min_length=1)
    authority_basis: str = Field(min_length=1)
    permitted_from: datetime
    permitted_until: datetime | None = None
    disposition: TenantInfluenceDisposition = TenantInfluenceDisposition.PENDING
    disposition_evidence_refs: tuple[str, ...] = ()

    @model_validator(mode="after")
    def interval_and_disposition_are_honest(self) -> Self:
        if (
            self.permitted_until is not None
            and self.permitted_until <= self.permitted_from
        ):
            raise ValueError("permitted_until must be after permitted_from")
        if self.disposition is not TenantInfluenceDisposition.PENDING and not (
            self.disposition_evidence_refs
        ):
            raise ValueError("a terminal influence disposition requires evidence")
        return self


class LearnedArtifactIsolationClass(StrEnum):
    NO_TENANT_DERIVED_SHARED = "no_tenant_derived_shared"
    TENANT_ISOLATED = "tenant_isolated"
    GOVERNED_SHARED = "governed_shared"
    UNKNOWN_UNBOUNDED = "unknown_unbounded"


class LearnedArtifactKind(StrEnum):
    MODEL = "model"
    ADAPTER = "adapter"
    EMBEDDING = "embedding"
    PROMPT = "prompt"
    CALIBRATION = "calibration"
    THRESHOLD = "threshold"
    POLICY = "policy"


class LearnedArtifactStatus(StrEnum):
    SHADOW = "shadow"
    ACTIVE = "active"
    FENCED = "fenced"
    REPLACED = "replaced"
    RETIRED = "retired"


class LearnedArtifactManifest(_FrozenContract):
    artifact_id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    kind: LearnedArtifactKind
    isolation_class: LearnedArtifactIsolationClass
    status: LearnedArtifactStatus
    permitted_tenant_ids: frozenset[UUID] = frozenset()
    permitted_purposes: frozenset[str] = Field(min_length=1)
    lineage: tuple[TenantInfluenceLineage, ...] = ()
    training_procedure_ref: str = Field(min_length=1)
    evaluation_refs: tuple[str, ...] = ()
    cross_tenant_policy_id: str | None = None
    leakage_test_refs: tuple[str, ...] = ()
    deletion_contract: str = Field(min_length=1)
    supported_guarantees: frozenset[str] = frozenset()
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("permitted_purposes")
    @classmethod
    def purposes_are_nonempty(cls, value: frozenset[str]) -> frozenset[str]:
        if not all(purpose.strip() for purpose in value):
            raise ValueError("permitted purposes cannot be blank")
        return value

    @model_validator(mode="after")
    def isolation_matches_lineage(self) -> Self:
        lineage_tenants = {item.tenant_id for item in self.lineage}
        if not lineage_tenants.issubset(self.permitted_tenant_ids):
            raise ValueError("tenant influence exceeds permitted tenant scope")
        if self.isolation_class is LearnedArtifactIsolationClass.TENANT_ISOLATED:
            if len(self.permitted_tenant_ids) != 1:
                raise ValueError("tenant-isolated artifacts require exactly one tenant")
        if self.isolation_class is LearnedArtifactIsolationClass.GOVERNED_SHARED:
            if not self.cross_tenant_policy_id:
                raise ValueError("governed shared learning requires a policy")
            if not self.leakage_test_refs:
                raise ValueError("governed shared learning requires leakage evidence")
        if self.isolation_class is LearnedArtifactIsolationClass.UNKNOWN_UNBOUNDED:
            forbidden = {
                "tenant_noninterference",
                "complete_unlearning",
                "cross_tenant_isolation",
            }
            if self.supported_guarantees & forbidden:
                raise ValueError(
                    "unknown learned influence cannot claim noninterference/unlearning"
                )
        return self

    def allows_use(self, *, tenant_id: UUID, purpose: str) -> bool:
        return (
            self.status is LearnedArtifactStatus.ACTIVE
            and tenant_id in self.permitted_tenant_ids
            and purpose in self.permitted_purposes
        )

    @property
    def manifest_ref(self) -> str:
        return f"learned-artifact:{self.artifact_id}:{self.version}"

    @property
    def manifest_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ExperimentAssignmentArm(StrEnum):
    CONTROL = "control"
    TREATMENT = "treatment"


class ExperimentEffectDirection(StrEnum):
    HIGHER_IS_BETTER = "higher_is_better"
    LOWER_IS_BETTER = "lower_is_better"


class ExperimentPlan(_FrozenContract):
    plan_id: UUID
    tenant_id: UUID
    plan_version: int = Field(default=1, ge=1)
    adaptive_family: str = Field(min_length=1)
    hypothesis: str = Field(min_length=1)
    primary_metric_id: str = Field(min_length=1)
    effect_direction: ExperimentEffectDirection
    assignment_unit: str = Field(min_length=1)
    eligibility_rule: str = Field(min_length=1)
    randomization_or_matching_rule: str = Field(min_length=1)
    control_policy_ref: str = Field(min_length=1)
    treatment_policy_ref: str = Field(min_length=1)
    interference_assumptions: tuple[str, ...] = Field(min_length=1)
    authority_and_consent_ref: str = Field(min_length=1)
    minimum_sample_size: int = Field(ge=2)
    stopping_rule: str = Field(min_length=1)
    preregistered_at: datetime
    exposure_window_start: datetime
    exposure_window_end: datetime

    @field_validator("preregistered_at", "exposure_window_start", "exposure_window_end")
    @classmethod
    def experiment_times_are_aware(cls, value: datetime, info) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value

    @model_validator(mode="after")
    def plan_is_pre_exposure_and_comparative(self) -> Self:
        if self.preregistered_at > self.exposure_window_start:
            raise ValueError("ExperimentPlan must be registered before exposure")
        if self.exposure_window_end <= self.exposure_window_start:
            raise ValueError("ExperimentPlan exposure window is empty")
        if self.control_policy_ref == self.treatment_policy_ref:
            raise ValueError("experiment control and treatment must differ")
        return self

    @property
    def plan_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def plan_ref(self) -> str:
        return f"experiment-plan:{self.plan_id}"


class ExperimentAssignment(_FrozenContract):
    assignment_id: UUID
    tenant_id: UUID
    plan_id: UUID
    plan_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    subject_ref: str = Field(min_length=1)
    eligibility_evidence_refs: tuple[str, ...] = Field(min_length=1)
    arm: ExperimentAssignmentArm
    assignment_probability: float = Field(gt=0.0, le=1.0)
    randomization_nonce_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    assigned_at: datetime
    first_exposure_at: datetime
    authority_and_consent_ref: str = Field(min_length=1)
    correction_epoch: int = Field(default=0, ge=0)
    invalidated: bool = False

    @field_validator("assigned_at", "first_exposure_at")
    @classmethod
    def assignment_times_are_aware(cls, value: datetime, info) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value

    @model_validator(mode="after")
    def assignment_precedes_exposure(self) -> Self:
        if self.assigned_at > self.first_exposure_at:
            raise ValueError("ExperimentAssignment must precede first exposure")
        if self.invalidated and self.correction_epoch < 1:
            raise ValueError("invalidated assignment requires a correction epoch")
        return self

    @property
    def assignment_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def assignment_ref(self) -> str:
        return f"experiment-assignment:{self.assignment_id}"


class ControlPolicyState(StrEnum):
    CANDIDATE = "candidate"
    SHADOW = "shadow"
    ELIGIBLE = "eligible"
    AUTHORIZED = "authorized"
    CANARY = "canary"
    ACTIVE = "active"
    FROZEN = "frozen"
    REJECTED = "rejected"
    ROLLED_FORWARD = "rolled_forward"
    ROLLED_BACK = "rolled_back"
    SUPERSEDED = "superseded"

    @property
    def terminal(self) -> bool:
        return self in {
            ControlPolicyState.REJECTED,
            ControlPolicyState.ROLLED_FORWARD,
            ControlPolicyState.ROLLED_BACK,
            ControlPolicyState.SUPERSEDED,
        }


class ControlPolicyCandidate(_FrozenContract):
    policy_id: UUID
    tenant_id: UUID
    policy_family: str = Field(min_length=1)
    policy_version: str = Field(min_length=1)
    base_policy_id: UUID | None = None
    base_policy_aggregate_version: int = Field(default=0, ge=0)
    bootstrap_policy_ref: str = Field(min_length=1)
    learned_artifact_ref: str = Field(min_length=1)
    purpose: str = Field(min_length=1)
    training_lineage_refs: tuple[str, ...] = Field(min_length=1)
    eligible_attribution_refs: tuple[str, ...] = ()
    frozen_control_ref: str = Field(min_length=1)
    scope: dict[str, Any]
    parameters: dict[str, Any]
    risk_cohort: str = Field(min_length=1)
    exploration_cap: float = Field(ge=0.0, le=1.0)
    canary_limit: float = Field(gt=0.0, le=1.0)
    rollback_trigger: str = Field(min_length=1)
    expires_at: datetime
    created_at: datetime

    @field_validator("expires_at", "created_at")
    @classmethod
    def candidate_times_are_aware(cls, value: datetime, info) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value

    @model_validator(mode="after")
    def candidate_is_bounded_and_cas_based(self) -> Self:
        if self.expires_at <= self.created_at:
            raise ValueError("control-policy candidate expiry must follow creation")
        if (self.base_policy_id is None) != (self.base_policy_aggregate_version == 0):
            raise ValueError(
                "base policy identity and aggregate version must appear together"
            )
        if not self.scope or not self.parameters:
            raise ValueError(
                "control-policy candidate requires exact scope and parameters"
            )
        return self

    @property
    def candidate_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def candidate_ref(self) -> str:
        return f"control-policy:{self.policy_id}:{self.policy_version}"


class PolicyEligibilityMeasurement(_FrozenContract):
    measurement_id: UUID
    tenant_id: UUID
    policy_id: UUID
    candidate_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    bootstrap_policy_ref: str = Field(min_length=1)
    experiment_plan_refs: tuple[str, ...] = ()
    experiment_assignment_refs: tuple[str, ...] = ()
    settlement_refs: tuple[str, ...] = Field(min_length=1)
    attribution_refs: tuple[str, ...] = Field(min_length=1)
    independent_evidence_count: int = Field(ge=0)
    required_independent_evidence_count: int = Field(ge=1)
    primary_metric_id: str = Field(min_length=1)
    observed_effect: float
    effect_interval_lower: float
    effect_interval_upper: float
    minimum_effect: float
    harm_event_count: int = Field(ge=0)
    harm_denominator: int = Field(ge=1)
    observed_harm_rate: float = Field(ge=0.0, le=1.0)
    maximum_harm_rate: float = Field(ge=0.0, le=1.0)
    frozen_control_ref: str = Field(min_length=1)
    tail_and_regression_check_refs: tuple[str, ...] = Field(min_length=1)
    correction_state_ref: str = Field(min_length=1)
    measured_at: datetime

    @field_validator("measured_at")
    @classmethod
    def measured_at_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("measured_at must be timezone-aware")
        return value

    @model_validator(mode="after")
    def statistics_are_internally_consistent(self) -> Self:
        if not (
            self.effect_interval_lower
            <= self.observed_effect
            <= self.effect_interval_upper
        ):
            raise ValueError("observed effect must lie inside its interval")
        if self.harm_event_count > self.harm_denominator:
            raise ValueError("harm event count exceeds its denominator")
        calculated_harm_rate = self.harm_event_count / self.harm_denominator
        if abs(calculated_harm_rate - self.observed_harm_rate) > 1e-9:
            raise ValueError("observed harm rate does not match its denominator")
        return self

    @property
    def eligible(self) -> bool:
        return (
            self.independent_evidence_count >= self.required_independent_evidence_count
            and self.effect_interval_lower >= self.minimum_effect
            and self.observed_harm_rate <= self.maximum_harm_rate
        )

    @property
    def measurement_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def measurement_ref(self) -> str:
        return f"policy-eligibility:{self.measurement_id}"


class PolicyPromotionDisposition(StrEnum):
    AUTHORIZED = "authorized"
    REJECTED = "rejected"


class PolicyPromotionDecision(_FrozenContract):
    decision_id: UUID
    tenant_id: UUID
    policy_id: UUID
    candidate_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    eligibility_measurement_id: UUID
    eligibility_measurement_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    disposition: PolicyPromotionDisposition
    governance_principal_ref: str = Field(min_length=1)
    authority: ConsumptionAuthorityContext
    authorized_canary_limit: float = Field(ge=0.0, le=1.0)
    authorized_exploration_cap: float = Field(ge=0.0, le=1.0)
    rollback_trigger: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    decided_at: datetime
    expires_at: datetime

    @field_validator("decided_at", "expires_at")
    @classmethod
    def promotion_times_are_aware(cls, value: datetime, info) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value

    @model_validator(mode="after")
    def decision_is_live_and_tenant_scoped(self) -> Self:
        if self.authority.tenant_id != self.tenant_id:
            raise ValueError("policy promotion authority tenant mismatch")
        if self.authority.principal_or_service_id != self.governance_principal_ref:
            raise ValueError("policy promotion principal does not own its authority")
        if self.expires_at <= self.decided_at:
            raise ValueError("policy promotion decision expiry must follow decision")
        if (
            self.disposition is PolicyPromotionDisposition.AUTHORIZED
            and not self.authority.is_live(self.decided_at)
        ):
            raise ValueError("policy promotion authority was not live")
        return self

    @property
    def decision_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def decision_ref(self) -> str:
        return f"policy-promotion:{self.decision_id}"


class ControlPolicyVersion(_FrozenContract):
    policy_id: UUID
    tenant_id: UUID
    policy_family: str = Field(min_length=1)
    policy_version: str = Field(min_length=1)
    aggregate_version: int = Field(ge=1)
    candidate_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: ControlPolicyState
    bootstrap_policy_ref: str = Field(min_length=1)
    learned_artifact_ref: str = Field(min_length=1)
    eligibility_measurement_ref: str | None = None
    promotion_decision_ref: str | None = None
    source_transition_refs: tuple[str, ...] = Field(min_length=1)
    effective_at: datetime
    expires_at: datetime

    @field_validator("effective_at", "expires_at")
    @classmethod
    def version_times_are_aware(cls, value: datetime, info) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value

    @model_validator(mode="after")
    def active_states_have_governance_evidence(self) -> Self:
        if (
            self.state
            in {
                ControlPolicyState.CANDIDATE,
                ControlPolicyState.SHADOW,
                ControlPolicyState.ELIGIBLE,
                ControlPolicyState.AUTHORIZED,
                ControlPolicyState.CANARY,
                ControlPolicyState.ACTIVE,
            }
            and self.expires_at <= self.effective_at
        ):
            raise ValueError("ControlPolicyVersion expiry must follow effective time")
        if (
            self.state
            in {
                ControlPolicyState.ELIGIBLE,
                ControlPolicyState.AUTHORIZED,
                ControlPolicyState.CANARY,
                ControlPolicyState.ACTIVE,
            }
            and not self.eligibility_measurement_ref
        ):
            raise ValueError(
                "eligible-or-later policy requires eligibility measurement"
            )
        if (
            self.state
            in {
                ControlPolicyState.AUTHORIZED,
                ControlPolicyState.CANARY,
                ControlPolicyState.ACTIVE,
            }
            and not self.promotion_decision_ref
        ):
            raise ValueError("authorized-or-later policy requires promotion decision")
        return self

    @property
    def version_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class LearningUpdate(_FrozenContract):
    update_id: UUID
    tenant_id: UUID
    policy_id: UUID
    candidate_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    settlement_refs: tuple[str, ...] = Field(min_length=1)
    attribution_refs: tuple[str, ...] = Field(min_length=1)
    learned_artifact_ref: str = Field(min_length=1)
    training_procedure_ref: str = Field(min_length=1)
    correction_epoch: int = Field(default=0, ge=0)
    reward_retracted: bool = False
    proposed_parameter_delta: dict[str, Any]
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def learning_time_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        return value

    @model_validator(mode="after")
    def correction_retraction_is_explicit(self) -> Self:
        if self.reward_retracted != (self.correction_epoch > 0):
            raise ValueError("corrected learning reward must be explicitly retracted")
        if not self.proposed_parameter_delta:
            raise ValueError("LearningUpdate requires a proposed parameter delta")
        return self

    @property
    def update_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def update_ref(self) -> str:
        return f"learning-update:{self.update_id}"


class PolicyRegistryObjectKind(StrEnum):
    BOOTSTRAP_POLICY = "bootstrap_policy"
    EXPERIMENT_PLAN = "experiment_plan"
    EXPERIMENT_ASSIGNMENT = "experiment_assignment"
    LEARNED_ARTIFACT = "learned_artifact"
    CONTROL_POLICY_CANDIDATE = "control_policy_candidate"
    ELIGIBILITY_MEASUREMENT = "eligibility_measurement"
    LEARNING_UPDATE = "learning_update"


PolicyRegistryObject = (
    BootstrapPolicy
    | ExperimentPlan
    | ExperimentAssignment
    | LearnedArtifactManifest
    | ControlPolicyCandidate
    | PolicyEligibilityMeasurement
    | LearningUpdate
)


class PolicyRegistryRegistrationCommand(_FrozenContract):
    context: AgencyWriteContext
    object_kind: PolicyRegistryObjectKind
    object: PolicyRegistryObject

    @model_validator(mode="after")
    def registration_uses_the_exact_policy_writer(self) -> Self:
        expected_types = {
            PolicyRegistryObjectKind.BOOTSTRAP_POLICY: BootstrapPolicy,
            PolicyRegistryObjectKind.EXPERIMENT_PLAN: ExperimentPlan,
            PolicyRegistryObjectKind.EXPERIMENT_ASSIGNMENT: ExperimentAssignment,
            PolicyRegistryObjectKind.LEARNED_ARTIFACT: LearnedArtifactManifest,
            PolicyRegistryObjectKind.CONTROL_POLICY_CANDIDATE: ControlPolicyCandidate,
            PolicyRegistryObjectKind.ELIGIBILITY_MEASUREMENT: PolicyEligibilityMeasurement,
            PolicyRegistryObjectKind.LEARNING_UPDATE: LearningUpdate,
        }
        if not isinstance(self.object, expected_types[self.object_kind]):
            raise ValueError("policy registry object kind does not match its payload")
        tenant_id = getattr(self.object, "tenant_id", self.context.tenant_id)
        if tenant_id != self.context.tenant_id:
            raise ValueError("policy registry object tenant mismatch")
        if (
            isinstance(self.object, ExperimentPlan)
            and self.object.preregistered_at != self.context.issued_at
        ):
            raise ValueError("ExperimentPlan preregistration cannot be backdated")
        if (
            isinstance(self.object, ExperimentAssignment)
            and self.object.assigned_at != self.context.issued_at
        ):
            raise ValueError("ExperimentAssignment cannot be backdated")
        if (
            isinstance(self.object, ControlPolicyCandidate)
            and self.object.created_at != self.context.issued_at
        ):
            raise ValueError("ControlPolicyCandidate cannot be backdated")
        if (
            isinstance(self.object, PolicyEligibilityMeasurement)
            and self.object.measured_at != self.context.issued_at
        ):
            raise ValueError("PolicyEligibilityMeasurement cannot be backdated")
        if (
            isinstance(self.object, LearningUpdate)
            and self.object.created_at != self.context.issued_at
        ):
            raise ValueError("LearningUpdate cannot be backdated")
        if (
            isinstance(self.object, LearnedArtifactManifest)
            and self.object.status is not LearnedArtifactStatus.SHADOW
        ):
            raise ValueError("learned artifact must register in shadow state")
        self.context.require_writer(
            owner="PolicyRegistryApplier", responsibility="control_policy"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class LearnedArtifactStateTransitionCommand(_FrozenContract):
    """The only lawful path for changing a learned artifact's usable state."""

    context: AgencyWriteContext
    artifact_id: str = Field(min_length=1)
    artifact_version: str = Field(min_length=1)
    expected_status_version: int = Field(ge=1)
    from_status: LearnedArtifactStatus
    to_status: LearnedArtifactStatus
    promotion_decision_ref: str | None = None
    reason: str = Field(min_length=1)
    transitioned_at: datetime

    @field_validator("transitioned_at")
    @classmethod
    def transition_time_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("transitioned_at must be timezone-aware")
        return value

    @model_validator(mode="after")
    def transition_is_lawful_and_writer_scoped(self) -> Self:
        legal_transitions = {
            LearnedArtifactStatus.SHADOW: {
                LearnedArtifactStatus.ACTIVE,
                LearnedArtifactStatus.FENCED,
                LearnedArtifactStatus.RETIRED,
            },
            LearnedArtifactStatus.ACTIVE: {
                LearnedArtifactStatus.FENCED,
                LearnedArtifactStatus.REPLACED,
                LearnedArtifactStatus.RETIRED,
            },
            LearnedArtifactStatus.FENCED: {
                LearnedArtifactStatus.REPLACED,
                LearnedArtifactStatus.RETIRED,
            },
            LearnedArtifactStatus.REPLACED: set(),
            LearnedArtifactStatus.RETIRED: set(),
        }
        if self.to_status not in legal_transitions[self.from_status]:
            raise ValueError("illegal learned-artifact state transition")
        if self.to_status is LearnedArtifactStatus.ACTIVE and not (
            self.promotion_decision_ref
        ):
            raise ValueError(
                "learned artifact activation requires a promotion decision"
            )
        if self.transitioned_at != self.context.issued_at:
            raise ValueError("learned artifact transition cannot be backdated")
        self.context.require_writer(
            owner="PolicyRegistryApplier", responsibility="control_policy"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class PolicyStateTransitionCommand(_FrozenContract):
    context: AgencyWriteContext
    policy_id: UUID
    expected_aggregate_version: int = Field(ge=1)
    candidate_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    from_state: ControlPolicyState
    to_state: ControlPolicyState
    eligibility_measurement_id: UUID | None = None
    promotion_decision: PolicyPromotionDecision | None = None
    source_transition_refs: tuple[str, ...] = Field(min_length=1)
    transitioned_at: datetime

    @field_validator("transitioned_at")
    @classmethod
    def transition_time_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("transitioned_at must be timezone-aware")
        return value

    @model_validator(mode="after")
    def transition_is_exact_and_writer_scoped(self) -> Self:
        legal_transitions = {
            ControlPolicyState.CANDIDATE: {
                ControlPolicyState.SHADOW,
                ControlPolicyState.FROZEN,
                ControlPolicyState.REJECTED,
            },
            ControlPolicyState.SHADOW: {
                ControlPolicyState.ELIGIBLE,
                ControlPolicyState.FROZEN,
                ControlPolicyState.REJECTED,
            },
            ControlPolicyState.ELIGIBLE: {
                ControlPolicyState.AUTHORIZED,
                ControlPolicyState.FROZEN,
                ControlPolicyState.REJECTED,
            },
            ControlPolicyState.AUTHORIZED: {
                ControlPolicyState.CANARY,
                ControlPolicyState.FROZEN,
                ControlPolicyState.REJECTED,
            },
            ControlPolicyState.CANARY: {
                ControlPolicyState.ACTIVE,
                ControlPolicyState.FROZEN,
                ControlPolicyState.ROLLED_BACK,
            },
            ControlPolicyState.ACTIVE: {
                ControlPolicyState.FROZEN,
                ControlPolicyState.ROLLED_FORWARD,
                ControlPolicyState.ROLLED_BACK,
                ControlPolicyState.SUPERSEDED,
            },
            ControlPolicyState.FROZEN: {
                ControlPolicyState.ROLLED_FORWARD,
                ControlPolicyState.ROLLED_BACK,
                ControlPolicyState.REJECTED,
                ControlPolicyState.SUPERSEDED,
            },
            ControlPolicyState.REJECTED: set(),
            ControlPolicyState.ROLLED_FORWARD: set(),
            ControlPolicyState.ROLLED_BACK: set(),
            ControlPolicyState.SUPERSEDED: set(),
        }
        if self.to_state not in legal_transitions[self.from_state]:
            raise ValueError("illegal control-policy state transition")
        if self.transitioned_at != self.context.issued_at:
            raise ValueError("policy transition cannot be backdated")
        if (
            self.to_state is ControlPolicyState.ELIGIBLE
            and not self.eligibility_measurement_id
        ):
            raise ValueError("eligible transition requires exact measurement")
        if self.to_state is ControlPolicyState.AUTHORIZED:
            if not self.promotion_decision:
                raise ValueError("authorized transition requires promotion decision")
            if self.promotion_decision.tenant_id != self.context.tenant_id:
                raise ValueError("promotion decision tenant mismatch")
            if self.promotion_decision.policy_id != self.policy_id:
                raise ValueError("promotion decision policy mismatch")
            if self.promotion_decision.candidate_digest != self.candidate_digest:
                raise ValueError("promotion decision candidate mismatch")
            if self.promotion_decision.decided_at != self.transitioned_at:
                raise ValueError("promotion decision cannot be backdated")
        self.context.require_writer(
            owner="PolicyRegistryApplier", responsibility="control_policy"
        )
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))
