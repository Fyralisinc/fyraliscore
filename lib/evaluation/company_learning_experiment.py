"""Typed paired evidence for corrective-memory recurrence experiments."""

from __future__ import annotations

from enum import StrEnum
from statistics import fmean
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256


class _ExperimentModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class CorrectiveMemoryArm(StrEnum):
    ADAPTIVE = "adaptive"
    FROZEN = "frozen"


class RecurrenceCaseKind(StrEnum):
    EXACT_ALIAS_POSITIVE = "exact_alias_positive"
    VARIANT_ALIAS_POSITIVE = "variant_alias_positive"
    CONTEXTUAL_PHRASE_NEGATIVE = "contextual_phrase_negative"
    CONFLICTING_SOURCE_HINT = "conflicting_source_hint"
    HOMONYM_LOCAL_ASSOCIATION = "homonym_local_association"
    UNRELATED_NEGATIVE_CONTROL = "unrelated_negative_control"


class ArmTerminalFate(StrEnum):
    CORRECT_RESOLUTION = "correct_resolution"
    WRONG_RESOLUTION = "wrong_resolution"
    SAFE_REVIEW = "safe_review"
    SAFE_ABSTENTION = "safe_abstention"
    SAFE_NO_ADMISSION = "safe_no_admission"
    INCOMPLETE = "incomplete"


class ConsumerTerminalFate(StrEnum):
    RESOLVED_FOR_CONSUMER = "resolved_for_consumer"
    REVIEW = "review"
    ABSTAINED = "abstained"
    REJECTED = "rejected"
    NO_ADMISSION = "no_admission"
    FAILED = "failed"
    INCOMPLETE = "incomplete"


class HardSafetyIncidentClass(StrEnum):
    UNSAFE_AUTONOMOUS_RESOLUTION = "unsafe_autonomous_resolution"
    WRONG_ENTITY_RESOLUTION = "wrong_entity_resolution"
    RESOLVED_ENTITY_MISSING = "resolved_entity_missing"
    UNEXPECTED_RESOLVED_ENTITY = "unexpected_resolved_entity"
    WRONG_MODEL_FROM_REPLAY = "wrong_model_from_replay"
    CONFLICTING_EVIDENCE_IGNORED = "conflicting_evidence_ignored"
    CONTEXTUAL_ALIAS_GLOBALIZED = "contextual_alias_globalized"
    INCOMPLETE_TERMINAL_FATE = "incomplete_terminal_fate"
    SOURCE_OBSERVATION_MUTATED = "source_observation_mutated"
    SELF_AUTHORITATIVE_EVIDENCE = "self_authoritative_evidence"
    MODEL_CARDINALITY_VIOLATION = "model_cardinality_violation"
    UNEXPECTED_TERMINAL_FATE = "unexpected_terminal_fate"


class CanonicalEntityRef(_ExperimentModel):
    type: str = Field(min_length=1)
    id: str = Field(min_length=1)
    version: int = Field(default=1, ge=1)


class SealedArmExpectation(_ExperimentModel):
    tenant_id: UUID
    allowed_consumer_fates: tuple[ConsumerTerminalFate, ...] = Field(
        min_length=1
    )
    expected_entity_ref: CanonicalEntityRef | None = None
    expected_model_count: int = Field(ge=0)
    autonomous_resolution_permitted: bool

    @model_validator(mode="after")
    def coherent_gold(self) -> Self:
        if len(self.allowed_consumer_fates) != len(
            set(self.allowed_consumer_fates)
        ):
            raise ValueError("allowed consumer fates must be unique")
        forbidden = {
            ConsumerTerminalFate.FAILED,
            ConsumerTerminalFate.INCOMPLETE,
        }
        if forbidden.intersection(self.allowed_consumer_fates):
            raise ValueError(
                "failed or incomplete consumer fates cannot be sealed as expected"
            )
        resolved_allowed = (
            ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
            in self.allowed_consumer_fates
        )
        if self.expected_entity_ref is None and resolved_allowed:
            raise ValueError(
                "resolved_for_consumer requires a sealed expected entity"
            )
        if self.expected_model_count and (
            self.expected_entity_ref is None or not resolved_allowed
        ):
            raise ValueError(
                "positive expected model cardinality requires an allowed "
                "resolved entity outcome"
            )
        if self.autonomous_resolution_permitted and (
            self.expected_entity_ref is None or not resolved_allowed
        ):
            raise ValueError(
                "autonomous resolution permission requires an allowed "
                "resolved entity outcome"
            )
        return self


class SealedRecurrenceCase(_ExperimentModel):
    case_id: str = Field(min_length=1)
    case_version: str = Field(min_length=1)
    kind: RecurrenceCaseKind
    alias_surface: str = Field(min_length=1)
    source_text_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    context_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    adaptive_expectation: SealedArmExpectation
    frozen_expectation: SealedArmExpectation
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def matched_arm_gold(self) -> Self:
        adaptive = self.adaptive_expectation
        frozen = self.frozen_expectation
        if adaptive.tenant_id == frozen.tenant_id:
            raise ValueError("paired case arms require distinct tenants")
        adaptive_ref = adaptive.expected_entity_ref
        frozen_ref = frozen.expected_entity_ref
        if (adaptive_ref is None) != (frozen_ref is None):
            raise ValueError(
                "paired arms must both seal an entity or both seal no entity"
            )
        if (
            adaptive_ref is not None
            and frozen_ref is not None
            and (
                adaptive_ref.type != frozen_ref.type
                or adaptive_ref.version != frozen_ref.version
            )
        ):
            raise ValueError(
                "paired arm entity gold must share canonical type and version"
            )
        return self

    def expectation_for(
        self,
        arm: CorrectiveMemoryArm,
    ) -> SealedArmExpectation:
        return (
            self.adaptive_expectation
            if arm is CorrectiveMemoryArm.ADAPTIVE
            else self.frozen_expectation
        )


class ArmLineageRefs(_ExperimentModel):
    training_observation_id: UUID
    recurrence_observation_id: UUID
    clarification_request_id: UUID
    clarification_answer_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    adjudicated_alias_id: UUID
    grounding_trace_id: UUID | None = None
    source_semantic_interpretation_id: UUID | None = None
    source_semantic_admission_id: UUID | None = None
    model_ids: tuple[UUID, ...] = ()
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_model_lineage(self) -> Self:
        if len(self.model_ids) != len(set(self.model_ids)):
            raise ValueError("model lineage IDs must be unique")
        return self


class CorrectiveMemoryArmResult(_ExperimentModel):
    case_id: str = Field(min_length=1)
    arm: CorrectiveMemoryArm
    tenant_id: UUID
    consumer_fate: ConsumerTerminalFate
    resolved_entity_ref: CanonicalEntityRef | None = None
    decision_source: str | None = None
    llm_call_count: int = Field(ge=0)
    latency_ms: float = Field(ge=0.0)
    estimated_cost_usd: float = Field(ge=0.0)
    source_semantic_admitted: bool
    lineage: ArmLineageRefs
    observed_safety_incidents: frozenset[HardSafetyIncidentClass] = frozenset()


class PairedRecurrenceResult(_ExperimentModel):
    case_id: str = Field(min_length=1)
    adaptive: CorrectiveMemoryArmResult
    frozen: CorrectiveMemoryArmResult
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def one_case_two_arms(self) -> Self:
        if self.adaptive.arm is not CorrectiveMemoryArm.ADAPTIVE:
            raise ValueError("adaptive result must use the adaptive arm")
        if self.frozen.arm is not CorrectiveMemoryArm.FROZEN:
            raise ValueError("frozen result must use the frozen arm")
        if {
            self.case_id,
            self.adaptive.case_id,
            self.frozen.case_id,
        } != {self.case_id}:
            raise ValueError("paired result case identities must match")
        return self


class CorrectiveMemoryExperimentSpec(_ExperimentModel):
    schema_version: str = "corrective-memory-experiment-spec-v1"
    experiment_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    system_version: str = Field(min_length=1)
    created_at: str = Field(min_length=1)
    scenario_ids: tuple[str, ...] = Field(min_length=1)
    company_foundation_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    provider_behavior_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    cases: tuple[SealedRecurrenceCase, ...] = Field(min_length=1)
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_population(self) -> Self:
        case_ids = [case.case_id for case in self.cases]
        if len(case_ids) != len(set(case_ids)):
            raise ValueError("sealed recurrence cases must be unique")
        if len(self.scenario_ids) != len(set(self.scenario_ids)):
            raise ValueError("scenario IDs must be unique")
        return self

    @property
    def case_manifest_digest(self) -> str:
        return canonical_sha256(
            [case.model_dump(mode="json") for case in self.cases]
        )

    @property
    def gold_digest(self) -> str:
        return canonical_sha256(
            [
                {
                    "case_id": case.case_id,
                    "adaptive_expectation": (
                        case.adaptive_expectation.model_dump(mode="json")
                    ),
                    "frozen_expectation": (
                        case.frozen_expectation.model_dump(mode="json")
                    ),
                }
                for case in self.cases
            ]
        )

    @property
    def arm_assignment_digest(self) -> str:
        return canonical_sha256(
            [
                {
                    "case_id": case.case_id,
                    CorrectiveMemoryArm.ADAPTIVE.value: str(
                        case.adaptive_expectation.tenant_id
                    ),
                    CorrectiveMemoryArm.FROZEN.value: str(
                        case.frozen_expectation.tenant_id
                    ),
                }
                for case in self.cases
            ]
        )

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class PairedCorrectiveMemoryMetrics(_ExperimentModel):
    pair_count: int = Field(ge=0)
    complete_terminal_fate_count: int = Field(ge=0)
    complete_terminal_fate_rate: float | None
    adaptive_correct_count: int = Field(ge=0)
    frozen_correct_count: int = Field(ge=0)
    adaptive_correctness_rate: float | None
    frozen_correctness_rate: float | None
    adaptive_minus_frozen_correctness: float | None
    adaptive_unsafe_count: int = Field(ge=0)
    frozen_unsafe_count: int = Field(ge=0)
    adaptive_unsafe_rate: float | None
    frozen_unsafe_rate: float | None
    adaptive_review_or_abstention_rate: float | None
    frozen_review_or_abstention_rate: float | None
    adaptive_semantic_admission_rate: float | None
    frozen_semantic_admission_rate: float | None
    adaptive_exactly_one_model_rate: float | None
    frozen_exactly_one_model_rate: float | None
    adaptive_llm_calls: int = Field(ge=0)
    frozen_llm_calls: int = Field(ge=0)
    llm_calls_avoided: int
    adaptive_mean_latency_ms: float | None
    frozen_mean_latency_ms: float | None
    adaptive_minus_frozen_latency_ms: float | None
    adaptive_estimated_cost_usd: float = Field(ge=0.0)
    frozen_estimated_cost_usd: float = Field(ge=0.0)
    estimated_cost_avoided_usd: float
    lineage_coverage_rate: float | None
    both_correct_count: int = Field(ge=0)
    adaptive_only_correct_count: int = Field(ge=0)
    frozen_only_correct_count: int = Field(ge=0)
    neither_correct_count: int = Field(ge=0)
    case_kind_metrics: dict[str, dict[str, int | float | None]]


class CorrectiveMemoryArmAssessment(_ExperimentModel):
    case_id: str = Field(min_length=1)
    arm: CorrectiveMemoryArm
    consumer_fate: ConsumerTerminalFate
    terminal_fate: ArmTerminalFate
    correct: bool
    terminal_fate_allowed: bool
    expected_entity_ref: CanonicalEntityRef | None
    resolved_entity_ref: CanonicalEntityRef | None
    entity_match: bool | None
    expected_model_count: int = Field(ge=0)
    observed_model_count: int = Field(ge=0)
    model_cardinality_valid: bool
    incident_classes: frozenset[HardSafetyIncidentClass]


class HardSafetyIncident(_ExperimentModel):
    incident_id: str = Field(min_length=1)
    incident_class: HardSafetyIncidentClass
    case_id: str = Field(min_length=1)
    arm: CorrectiveMemoryArm
    summary: str = Field(min_length=1)
    artifact_refs: tuple[str, ...] = Field(min_length=1)


class CorrectiveMemoryExperimentReport(_ExperimentModel):
    schema_version: str = "corrective-memory-experiment-report-v1"
    experiment_id: str
    run_id: str
    system_version: str
    created_at: str
    scenario_ids: tuple[str, ...]
    spec_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    case_manifest_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    gold_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    arm_assignment_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    pair_results_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: str
    metrics: PairedCorrectiveMemoryMetrics
    incidents: tuple[HardSafetyIncident, ...]
    pairs: tuple[PairedRecurrenceResult, ...]
    assessments: tuple[CorrectiveMemoryArmAssessment, ...]
    proof_gaps: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


_SAFE_NONMODEL_FATES = {
    ArmTerminalFate.SAFE_REVIEW,
    ArmTerminalFate.SAFE_ABSTENTION,
    ArmTerminalFate.SAFE_NO_ADMISSION,
}


def evaluate_corrective_memory_experiment(
    *,
    spec: CorrectiveMemoryExperimentSpec,
    pairs: tuple[PairedRecurrenceResult, ...],
    artifact_refs: tuple[str, ...],
) -> CorrectiveMemoryExperimentReport:
    """Compile continuous paired metrics while preserving every safety incident."""

    by_case = {case.case_id: case for case in spec.cases}
    pair_ids = [pair.case_id for pair in pairs]
    if len(pair_ids) != len(set(pair_ids)):
        raise ValueError("paired recurrence results must be unique by case")
    if set(pair_ids) != set(by_case):
        raise ValueError("paired results must exactly cover the sealed cases")

    incidents: list[HardSafetyIncident] = []
    assessments: list[CorrectiveMemoryArmAssessment] = []
    for pair in pairs:
        case = by_case[pair.case_id]
        for result in (pair.adaptive, pair.frozen):
            expectation = case.expectation_for(result.arm)
            if result.tenant_id != expectation.tenant_id:
                raise ValueError(
                    f"{result.arm.value} result tenant does not match "
                    f"sealed assignment for case {case.case_id}"
                )
            assessment = _assess_result(
                case=case,
                result=result,
            )
            assessments.append(assessment)
            for incident_class in sorted(
                assessment.incident_classes,
                key=str,
            ):
                incidents.append(
                    HardSafetyIncident(
                        incident_id=(
                            f"{spec.experiment_id}:{pair.case_id}:"
                            f"{result.arm.value}:{incident_class.value}"
                        ),
                        incident_class=incident_class,
                        case_id=pair.case_id,
                        arm=result.arm,
                        summary=(
                            f"Observed {incident_class.value} in "
                            f"{result.arm.value} arm."
                        ),
                        artifact_refs=tuple(
                            dict.fromkeys(
                                (
                                    *artifact_refs,
                                    *pair.artifact_refs,
                                    *result.lineage.artifact_refs,
                                )
                            )
                        ),
                    )
                )

    metrics = _metrics(
        spec=spec,
        pairs=pairs,
        assessments=tuple(assessments),
    )
    status = (
        "contradicted"
        if incidents
        else "not_observed"
        if not pairs
        else "observed"
    )
    return CorrectiveMemoryExperimentReport(
        experiment_id=spec.experiment_id,
        run_id=spec.run_id,
        system_version=spec.system_version,
        created_at=spec.created_at,
        scenario_ids=spec.scenario_ids,
        spec_digest=spec.digest,
        case_manifest_digest=spec.case_manifest_digest,
        gold_digest=spec.gold_digest,
        arm_assignment_digest=spec.arm_assignment_digest,
        pair_results_digest=canonical_sha256(
            [pair.model_dump(mode="json") for pair in pairs]
        ),
        status=status,
        metrics=metrics,
        incidents=tuple(incidents),
        pairs=pairs,
        assessments=tuple(assessments),
        proof_gaps=(
            "This paired harness is synthetic E4 evidence, not open-world E5 validation.",
            "Confidence intervals require a larger held-out recurrence population.",
            "Unseen alias spellings are outside exact-alias replay scope.",
        ),
        artifact_refs=artifact_refs,
    )


def _metrics(
    *,
    spec: CorrectiveMemoryExperimentSpec,
    pairs: tuple[PairedRecurrenceResult, ...],
    assessments: tuple[CorrectiveMemoryArmAssessment, ...],
) -> PairedCorrectiveMemoryMetrics:
    pair_count = len(pairs)
    adaptive = tuple(pair.adaptive for pair in pairs)
    frozen = tuple(pair.frozen for pair in pairs)
    assessment_by_arm = {
        (assessment.case_id, assessment.arm): assessment
        for assessment in assessments
    }
    adaptive_assessments = tuple(
        assessment_by_arm[(pair.case_id, CorrectiveMemoryArm.ADAPTIVE)]
        for pair in pairs
    )
    frozen_assessments = tuple(
        assessment_by_arm[(pair.case_id, CorrectiveMemoryArm.FROZEN)]
        for pair in pairs
    )
    complete = sum(
        assessment.terminal_fate is not ArmTerminalFate.INCOMPLETE
        for assessment in assessments
    )
    expected_terminal_count = pair_count * 2
    adaptive_correct = sum(item.correct for item in adaptive_assessments)
    frozen_correct = sum(item.correct for item in frozen_assessments)
    adaptive_unsafe = sum(
        bool(item.incident_classes) for item in adaptive_assessments
    )
    frozen_unsafe = sum(
        bool(item.incident_classes) for item in frozen_assessments
    )
    adaptive_review = sum(
        item.terminal_fate in _SAFE_NONMODEL_FATES
        for item in adaptive_assessments
    )
    frozen_review = sum(
        item.terminal_fate in _SAFE_NONMODEL_FATES
        for item in frozen_assessments
    )
    adaptive_admitted = sum(result.source_semantic_admitted for result in adaptive)
    frozen_admitted = sum(result.source_semantic_admitted for result in frozen)
    adaptive_one_model = sum(len(result.lineage.model_ids) == 1 for result in adaptive)
    frozen_one_model = sum(len(result.lineage.model_ids) == 1 for result in frozen)
    lineage_complete = sum(
        result.lineage.grounding_trace_id is not None
        and result.lineage.source_semantic_interpretation_id is not None
        and result.lineage.source_semantic_admission_id is not None
        for pair in pairs
        for result in (pair.adaptive, pair.frozen)
    )
    both_correct = sum(
        assessment_by_arm[
            (pair.case_id, CorrectiveMemoryArm.ADAPTIVE)
        ].correct
        and assessment_by_arm[
            (pair.case_id, CorrectiveMemoryArm.FROZEN)
        ].correct
        for pair in pairs
    )
    adaptive_only = sum(
        assessment_by_arm[
            (pair.case_id, CorrectiveMemoryArm.ADAPTIVE)
        ].correct
        and not assessment_by_arm[
            (pair.case_id, CorrectiveMemoryArm.FROZEN)
        ].correct
        for pair in pairs
    )
    frozen_only = sum(
        not assessment_by_arm[
            (pair.case_id, CorrectiveMemoryArm.ADAPTIVE)
        ].correct
        and assessment_by_arm[
            (pair.case_id, CorrectiveMemoryArm.FROZEN)
        ].correct
        for pair in pairs
    )
    case_kind_metrics: dict[str, dict[str, int | float | None]] = {}
    for kind in RecurrenceCaseKind:
        kind_pairs = tuple(
            pair for pair in pairs if _case(spec, pair.case_id).kind is kind
        )
        if not kind_pairs:
            continue
        case_kind_metrics[kind.value] = {
            "pair_count": len(kind_pairs),
            "adaptive_correctness_rate": _rate(
                sum(
                    assessment_by_arm[
                        (pair.case_id, CorrectiveMemoryArm.ADAPTIVE)
                    ].correct
                    for pair in kind_pairs
                ),
                len(kind_pairs),
            ),
            "frozen_correctness_rate": _rate(
                sum(
                    assessment_by_arm[
                        (pair.case_id, CorrectiveMemoryArm.FROZEN)
                    ].correct
                    for pair in kind_pairs
                ),
                len(kind_pairs),
            ),
        }
    adaptive_latency = _mean(result.latency_ms for result in adaptive)
    frozen_latency = _mean(result.latency_ms for result in frozen)
    adaptive_cost = sum(result.estimated_cost_usd for result in adaptive)
    frozen_cost = sum(result.estimated_cost_usd for result in frozen)
    return PairedCorrectiveMemoryMetrics(
        pair_count=pair_count,
        complete_terminal_fate_count=complete,
        complete_terminal_fate_rate=_rate(complete, expected_terminal_count),
        adaptive_correct_count=adaptive_correct,
        frozen_correct_count=frozen_correct,
        adaptive_correctness_rate=_rate(adaptive_correct, pair_count),
        frozen_correctness_rate=_rate(frozen_correct, pair_count),
        adaptive_minus_frozen_correctness=(
            _difference(adaptive_correct, frozen_correct, pair_count)
        ),
        adaptive_unsafe_count=adaptive_unsafe,
        frozen_unsafe_count=frozen_unsafe,
        adaptive_unsafe_rate=_rate(adaptive_unsafe, pair_count),
        frozen_unsafe_rate=_rate(frozen_unsafe, pair_count),
        adaptive_review_or_abstention_rate=_rate(adaptive_review, pair_count),
        frozen_review_or_abstention_rate=_rate(frozen_review, pair_count),
        adaptive_semantic_admission_rate=_rate(adaptive_admitted, pair_count),
        frozen_semantic_admission_rate=_rate(frozen_admitted, pair_count),
        adaptive_exactly_one_model_rate=_rate(adaptive_one_model, pair_count),
        frozen_exactly_one_model_rate=_rate(frozen_one_model, pair_count),
        adaptive_llm_calls=sum(result.llm_call_count for result in adaptive),
        frozen_llm_calls=sum(result.llm_call_count for result in frozen),
        llm_calls_avoided=(
            sum(result.llm_call_count for result in frozen)
            - sum(result.llm_call_count for result in adaptive)
        ),
        adaptive_mean_latency_ms=adaptive_latency,
        frozen_mean_latency_ms=frozen_latency,
        adaptive_minus_frozen_latency_ms=(
            adaptive_latency - frozen_latency
            if adaptive_latency is not None and frozen_latency is not None
            else None
        ),
        adaptive_estimated_cost_usd=adaptive_cost,
        frozen_estimated_cost_usd=frozen_cost,
        estimated_cost_avoided_usd=frozen_cost - adaptive_cost,
        lineage_coverage_rate=_rate(lineage_complete, expected_terminal_count),
        both_correct_count=both_correct,
        adaptive_only_correct_count=adaptive_only,
        frozen_only_correct_count=frozen_only,
        neither_correct_count=pair_count - both_correct - adaptive_only - frozen_only,
        case_kind_metrics=case_kind_metrics,
    )


def _case(
    spec: CorrectiveMemoryExperimentSpec,
    case_id: str,
) -> SealedRecurrenceCase:
    return next(case for case in spec.cases if case.case_id == case_id)


def _assess_result(
    *,
    case: SealedRecurrenceCase,
    result: CorrectiveMemoryArmResult,
) -> CorrectiveMemoryArmAssessment:
    expectation = case.expectation_for(result.arm)
    expected_ref = expectation.expected_entity_ref
    resolved_ref = result.resolved_entity_ref
    entity_match = (
        None
        if expected_ref is None and resolved_ref is None
        else expected_ref == resolved_ref
    )
    terminal_fate = _derived_terminal_fate(
        consumer_fate=result.consumer_fate,
        entity_match=entity_match,
    )
    terminal_fate_allowed = (
        result.consumer_fate in expectation.allowed_consumer_fates
    )
    model_count = len(result.lineage.model_ids)
    model_cardinality_valid = model_count == expectation.expected_model_count
    correct = (
        terminal_fate is ArmTerminalFate.CORRECT_RESOLUTION
        if expected_ref is not None
        else (
            terminal_fate_allowed
            and resolved_ref is None
            and terminal_fate in _SAFE_NONMODEL_FATES
        )
    )
    incidents = set(result.observed_safety_incidents)
    if terminal_fate is ArmTerminalFate.INCOMPLETE:
        incidents.add(HardSafetyIncidentClass.INCOMPLETE_TERMINAL_FATE)
    if not terminal_fate_allowed:
        incidents.add(HardSafetyIncidentClass.UNEXPECTED_TERMINAL_FATE)
    if not model_cardinality_valid:
        incidents.add(HardSafetyIncidentClass.MODEL_CARDINALITY_VIOLATION)
    if result.consumer_fate is ConsumerTerminalFate.RESOLVED_FOR_CONSUMER:
        if resolved_ref is None:
            incidents.add(HardSafetyIncidentClass.RESOLVED_ENTITY_MISSING)
        elif entity_match is not True:
            incidents.add(HardSafetyIncidentClass.WRONG_ENTITY_RESOLUTION)
    elif resolved_ref is not None:
        incidents.add(HardSafetyIncidentClass.UNEXPECTED_RESOLVED_ENTITY)
    unsafe_replay = (
        result.decision_source == "governed_exact_alias_replay"
        and (
            not expectation.autonomous_resolution_permitted
            or terminal_fate is ArmTerminalFate.WRONG_RESOLUTION
        )
        and (
            result.consumer_fate
            is ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
            or resolved_ref is not None
        )
    )
    if unsafe_replay:
        incidents.add(HardSafetyIncidentClass.UNSAFE_AUTONOMOUS_RESOLUTION)
    if (
        result.decision_source == "governed_exact_alias_replay"
        and terminal_fate is ArmTerminalFate.WRONG_RESOLUTION
        and model_count > 0
    ):
        incidents.add(HardSafetyIncidentClass.WRONG_MODEL_FROM_REPLAY)
    return CorrectiveMemoryArmAssessment(
        case_id=case.case_id,
        arm=result.arm,
        consumer_fate=result.consumer_fate,
        terminal_fate=terminal_fate,
        correct=correct,
        terminal_fate_allowed=terminal_fate_allowed,
        expected_entity_ref=expected_ref,
        resolved_entity_ref=resolved_ref,
        entity_match=entity_match,
        expected_model_count=expectation.expected_model_count,
        observed_model_count=model_count,
        model_cardinality_valid=model_cardinality_valid,
        incident_classes=frozenset(incidents),
    )


def _derived_terminal_fate(
    *,
    consumer_fate: ConsumerTerminalFate,
    entity_match: bool | None,
) -> ArmTerminalFate:
    if consumer_fate is ConsumerTerminalFate.RESOLVED_FOR_CONSUMER:
        return (
            ArmTerminalFate.CORRECT_RESOLUTION
            if entity_match is True
            else ArmTerminalFate.WRONG_RESOLUTION
        )
    if consumer_fate is ConsumerTerminalFate.REVIEW:
        return ArmTerminalFate.SAFE_REVIEW
    if consumer_fate in {
        ConsumerTerminalFate.ABSTAINED,
        ConsumerTerminalFate.REJECTED,
    }:
        return ArmTerminalFate.SAFE_ABSTENTION
    if consumer_fate is ConsumerTerminalFate.NO_ADMISSION:
        return ArmTerminalFate.SAFE_NO_ADMISSION
    return ArmTerminalFate.INCOMPLETE


def _rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _difference(left: int, right: int, denominator: int) -> float | None:
    return (left - right) / denominator if denominator else None


def _mean(values) -> float | None:
    materialized = tuple(float(value) for value in values)
    return fmean(materialized) if materialized else None


__all__ = [
    "ArmLineageRefs",
    "ArmTerminalFate",
    "CanonicalEntityRef",
    "ConsumerTerminalFate",
    "CorrectiveMemoryArm",
    "CorrectiveMemoryArmAssessment",
    "CorrectiveMemoryArmResult",
    "CorrectiveMemoryExperimentReport",
    "CorrectiveMemoryExperimentSpec",
    "HardSafetyIncident",
    "HardSafetyIncidentClass",
    "PairedCorrectiveMemoryMetrics",
    "PairedRecurrenceResult",
    "RecurrenceCaseKind",
    "SealedArmExpectation",
    "SealedRecurrenceCase",
    "evaluate_corrective_memory_experiment",
]
