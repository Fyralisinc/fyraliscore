"""Sealed continuous evaluation for governed variant-alias learning."""

from __future__ import annotations

import json
from collections import Counter
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_experiment import (
    CanonicalEntityRef,
    ConsumerTerminalFate,
    CorrectiveMemoryArm,
    CorrectiveMemoryArmAssessment,
    CorrectiveMemoryExperimentReport,
    HardSafetyIncidentClass,
    PairedRecurrenceResult,
    RecurrenceCaseKind,
)
from lib.evaluation.company_learning_population import (
    IntervalEstimate,
    _bootstrap_mean_estimate,
    _wilson_estimate,
)


VARIANT_ALIAS_SCENARIO_ID = (
    "ENTITY-CORRECTIVE-MEMORY-VARIANT-ALIAS-POPULATION"
)


class _VariantPopulationModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class VariantAliasFamily(StrEnum):
    ACRONYM_FROM_LONG_FORM = "acronym_from_long_form"
    PUNCTUATION_COMPACT_FORM = "punctuation_compact_form"
    HYPHEN_SPACING = "hyphen_spacing"
    ANCHORED_SHORT_FORM = "anchored_short_form"
    ORTHOGRAPHIC_OMISSION_SUBSEQUENCE = (
        "orthographic_omission_subsequence"
    )
    POSSESSIVE_OR_PLURAL = "possessive_or_plural"


class HeldOutVariantAliasCase(_VariantPopulationModel):
    case_id: str = Field(min_length=1)
    case_version: str = "v1"
    entity_type: Literal["customer", "project", "team", "system"]
    variant_family: VariantAliasFamily
    candidate_label: str = Field(min_length=1)
    source_channel: Literal["slack:message"] = "slack:message"
    channel: str = Field(min_length=1)
    slack_context: Literal[
        "public_channel",
        "private_channel",
        "cross_thread_recurrence",
    ]
    wording_variant: Literal[
        "status_update",
        "risk_report",
        "commitment",
        "decision",
        "support_escalation",
    ]
    consequence: Literal["low", "medium", "high"]
    recurrence_distance: Literal[
        "same_day",
        "one_week",
        "one_month",
        "one_quarter",
    ]
    training_alias_surface: str = Field(min_length=1)
    recurrence_alias_surface: str = Field(min_length=1)
    training_text: str = Field(min_length=1)
    recurrence_text: str = Field(min_length=1)
    ranking_basis: str = Field(min_length=1)

    @model_validator(mode="after")
    def variant_is_source_anchored_and_mechanically_rankable(self) -> Self:
        if _norm(self.training_alias_surface) == _norm(
            self.recurrence_alias_surface
        ):
            raise ValueError("variant cases cannot repeat the exact alias")
        if self.training_alias_surface not in self.training_text:
            raise ValueError("training text must contain the training alias")
        if self.recurrence_alias_surface not in self.recurrence_text:
            raise ValueError(
                "recurrence text must contain the recurrence alias"
            )
        if self.ranking_basis != _ranking_basis(self.variant_family):
            raise ValueError(
                "variant ranking basis must match the sealed family rule"
            )
        if not _ranking_rule_matches(self):
            raise ValueError(
                "variant is not justified by the current candidate ranker"
            )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class HeldOutVariantAliasPopulation(_VariantPopulationModel):
    schema_version: Literal["company-learning-variant-population-v1"] = (
        "company-learning-variant-population-v1"
    )
    population_definition_version: Literal["variant-alias-slack-v1"] = (
        "variant-alias-slack-v1"
    )
    cases: tuple[HeldOutVariantAliasCase, ...] = Field(
        min_length=24,
        max_length=24,
    )

    @model_validator(mode="after")
    def exact_balanced_registry(self) -> Self:
        case_ids = [case.case_id for case in self.cases]
        if len(case_ids) != len(set(case_ids)):
            raise ValueError("variant-alias case IDs must be unique")
        case_digests = [case.digest for case in self.cases]
        if len(case_digests) != len(set(case_digests)):
            raise ValueError("variant-alias cases must be independently defined")
        entity_counts = Counter(case.entity_type for case in self.cases)
        if entity_counts != {
            "customer": 6,
            "project": 6,
            "team": 6,
            "system": 6,
        }:
            raise ValueError(
                "variant registry requires six cases per entity type"
            )
        family_counts = Counter(case.variant_family for case in self.cases)
        if family_counts != {
            family: 4 for family in VariantAliasFamily
        }:
            raise ValueError(
                "variant registry requires four cases per variant family"
            )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class VariantAliasExecutionObservation(_VariantPopulationModel):
    case_id: str = Field(min_length=1)
    execution_status: Literal["observed", "unsupported"] = "observed"
    unsupported_reason: str | None = None

    @model_validator(mode="after")
    def unsupported_reason_matches_status(self) -> Self:
        if self.execution_status == "observed" and self.unsupported_reason:
            raise ValueError("observed variant cases cannot be unsupported")
        if self.execution_status == "unsupported" and not self.unsupported_reason:
            raise ValueError(
                "unsupported variant cases require an explicit reason"
            )
        return self


class VariantAliasCaseAssignment(_VariantPopulationModel):
    case_id: str = Field(min_length=1)
    logical_entity_type: str = Field(min_length=1)
    runtime_entity_type: str = Field(min_length=1)
    adaptive_tenant_id: UUID
    frozen_tenant_id: UUID
    adaptive_target_id: UUID
    frozen_target_id: UUID
    adaptive_conflicting_id: UUID
    frozen_conflicting_id: UUID


class VariantAliasArmMechanismEvidence(_VariantPopulationModel):
    case_id: str = Field(min_length=1)
    arm: CorrectiveMemoryArm
    tenant_id: UUID
    target_id: UUID
    worker_decision: str = Field(min_length=1)
    candidate_set_id: UUID | None = None
    candidate_set_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    candidate_set_size: int = Field(ge=0)
    authorized_candidate_refs: tuple[CanonicalEntityRef, ...]
    target_candidate_authorized: bool
    target_candidate_evidence_refs: tuple[str, ...]
    closed_set_match: bool
    model_output_ref: CanonicalEntityRef | None = None
    model_output_confidence: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    scripted_high_confidence_target_response_observed: bool
    llm_call_count: int = Field(ge=0)
    source_observation_immutable: bool

    @model_validator(mode="after")
    def mechanism_state_is_coherent(self) -> Self:
        if (self.candidate_set_id is None) != (
            self.candidate_set_hash is None
        ):
            raise ValueError(
                "candidate set identity and hash must be present together"
            )
        refs = [
            (ref.type, ref.id, ref.version)
            for ref in self.authorized_candidate_refs
        ]
        if len(refs) != len(set(refs)):
            raise ValueError(
                "authorized mechanism candidates must be unique"
            )
        target_visible = any(
            ref.id == str(self.target_id)
            for ref in self.authorized_candidate_refs
        )
        if self.target_candidate_authorized != target_visible:
            raise ValueError(
                "target authorization does not match candidate evidence"
            )
        if (
            self.target_candidate_authorized
            and not self.target_candidate_evidence_refs
        ):
            raise ValueError(
                "authorized targets require candidate evidence references"
            )
        if self.closed_set_match and self.model_output_ref not in (
            *self.authorized_candidate_refs,
        ):
            raise ValueError(
                "closed-set matches must select an authorized candidate"
            )
        return self


class VariantAliasPairMechanismEvidence(_VariantPopulationModel):
    case_id: str = Field(min_length=1)
    adaptive: VariantAliasArmMechanismEvidence
    frozen: VariantAliasArmMechanismEvidence

    @model_validator(mode="after")
    def one_case_two_arms(self) -> Self:
        if self.adaptive.arm is not CorrectiveMemoryArm.ADAPTIVE:
            raise ValueError("adaptive mechanism evidence has the wrong arm")
        if self.frozen.arm is not CorrectiveMemoryArm.FROZEN:
            raise ValueError("frozen mechanism evidence has the wrong arm")
        if {
            self.case_id,
            self.adaptive.case_id,
            self.frozen.case_id,
        } != {self.case_id}:
            raise ValueError("mechanism evidence case identities differ")
        return self


class VariantAliasMechanismMetrics(_VariantPopulationModel):
    selected_case_count: int = Field(ge=1)
    observed_pair_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    full_registry_coverage_rate: float = Field(ge=0.0, le=1.0)
    observed_execution_rate: float = Field(ge=0.0, le=1.0)
    adaptive_correctness_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    frozen_correctness_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    adaptive_minus_frozen_correctness: float | None = Field(
        default=None,
        ge=-1.0,
        le=1.0,
    )
    adaptive_target_candidate_authorization_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    frozen_target_candidate_exposure_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    candidate_authorization_gap: float | None = Field(
        default=None,
        ge=-1.0,
        le=1.0,
    )
    adaptive_closed_set_match_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    frozen_closed_set_match_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    both_arms_one_llm_call_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    both_arms_scripted_target_response_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    frozen_safe_review_or_abstention_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    source_immutability_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    candidate_memory_mediated_success_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    adaptive_mean_llm_calls: float | None = Field(default=None, ge=0.0)
    frozen_mean_llm_calls: float | None = Field(default=None, ge=0.0)
    hard_safety_incident_count: int = Field(ge=0)
    control_integrity_violation_count: int = Field(ge=0)
    entity_type_counts: dict[str, int]
    variant_family_counts: dict[str, int]


class VariantAliasStratumReport(_VariantPopulationModel):
    sealed_case_count: int = Field(ge=0)
    observed_case_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    adaptive_correctness: IntervalEstimate | None
    frozen_correctness: IntervalEstimate | None
    adaptive_minus_frozen_correctness: IntervalEstimate | None
    adaptive_unsafe_rate: IntervalEstimate | None
    frozen_unsafe_rate: IntervalEstimate | None

    @model_validator(mode="after")
    def exact_stratum_accounting(self) -> Self:
        if (
            self.observed_case_count + self.unsupported_case_count
            != self.sealed_case_count
        ):
            raise ValueError(
                "variant stratum observations must partition sealed cases"
            )
        estimates = (
            self.adaptive_correctness,
            self.frozen_correctness,
            self.adaptive_minus_frozen_correctness,
            self.adaptive_unsafe_rate,
            self.frozen_unsafe_rate,
        )
        if self.observed_case_count == 0 and any(
            estimate is not None for estimate in estimates
        ):
            raise ValueError(
                "unobserved variant strata cannot contain estimates"
            )
        if self.observed_case_count > 0 and any(
            estimate is None for estimate in estimates
        ):
            raise ValueError(
                "observed variant strata require every estimate"
            )
        return self


class VariantAliasPopulationReport(_VariantPopulationModel):
    schema_version: Literal["company-learning-variant-report-v1"] = (
        "company-learning-variant-report-v1"
    )
    population_definition_version: str = Field(min_length=1)
    population_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    experiment_report_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    observation_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    pair_count: int = Field(ge=0)
    observed_pair_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    complete_population: bool
    strata_counts: dict[str, dict[str, int]]
    observed_strata_counts: dict[str, dict[str, int]]
    unsupported_strata_counts: dict[str, dict[str, int]]
    unsupported_reason_counts: dict[str, int]
    adaptive_correctness: IntervalEstimate
    frozen_correctness: IntervalEstimate
    adaptive_minus_frozen_correctness: IntervalEstimate
    adaptive_unsafe_rate: IntervalEstimate
    frozen_unsafe_rate: IntervalEstimate
    family_reports: dict[str, VariantAliasStratumReport]
    entity_type_reports: dict[str, VariantAliasStratumReport]

    @model_validator(mode="after")
    def exact_population_accounting(self) -> Self:
        if self.pair_count != 24 or not self.complete_population:
            raise ValueError(
                "variant report must retain the complete 24-case registry"
            )
        if (
            self.observed_pair_count + self.unsupported_case_count
            != self.pair_count
        ):
            raise ValueError(
                "observed and unsupported variant cases must partition registry"
            )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class CompanyLearningVariantPopulationEvidence(_VariantPopulationModel):
    schema_version: Literal[
        "company-learning-variant-population-evidence-v1"
    ] = "company-learning-variant-population-evidence-v1"
    created_at: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    system_version: str = Field(min_length=1)
    execution_mode: Literal["smoke", "full"]
    selection_policy: Literal[
        "deterministic_registry_prefix_smoke",
        "full_registry_once_no_selective_reruns",
    ]
    registry_path: str = Field(min_length=1)
    registry_population: HeldOutVariantAliasPopulation
    registry_population_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    selected_case_ids: tuple[str, ...] = Field(min_length=1)
    assignments: tuple[VariantAliasCaseAssignment, ...] = Field(min_length=1)
    observations: tuple[VariantAliasExecutionObservation, ...] = Field(
        min_length=1
    )
    raw_pairs: tuple[PairedRecurrenceResult, ...]
    experiment_report: CorrectiveMemoryExperimentReport
    population_report: VariantAliasPopulationReport | None = None
    mechanism_pairs: tuple[VariantAliasPairMechanismEvidence, ...] = ()
    mechanism_metrics: VariantAliasMechanismMetrics | None = None
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def exact_cross_bound_evidence(self) -> Self:
        registry_ids = tuple(
            case.case_id for case in self.registry_population.cases
        )
        selected_ids = self.selected_case_ids
        if selected_ids != registry_ids[: len(selected_ids)]:
            raise ValueError(
                "variant selection must preserve a deterministic registry prefix"
            )
        if self.registry_population_digest != self.registry_population.digest:
            raise ValueError("variant registry digest does not match registry")
        expected_policy = (
            "full_registry_once_no_selective_reruns"
            if self.execution_mode == "full"
            else "deterministic_registry_prefix_smoke"
        )
        if self.selection_policy != expected_policy:
            raise ValueError(
                "variant selection policy does not match execution mode"
            )
        assignment_ids = tuple(row.case_id for row in self.assignments)
        observation_ids = tuple(row.case_id for row in self.observations)
        if assignment_ids != selected_ids or observation_ids != selected_ids:
            raise ValueError(
                "assignments and observations must preserve selected order"
            )
        runtime_types = {
            "customer": "customer",
            "project": "resource",
            "team": "actor",
            "system": "resource",
        }
        selected_cases = {
            case.case_id: case
            for case in self.registry_population.cases[: len(selected_ids)]
        }
        for assignment in self.assignments:
            case = selected_cases[assignment.case_id]
            if (
                assignment.logical_entity_type != case.entity_type
                or assignment.runtime_entity_type
                != runtime_types[case.entity_type]
            ):
                raise ValueError(
                    "variant assignment entity type does not match registry"
                )
        _validate_assignment_isolation(self.assignments)
        observed_ids = tuple(
            row.case_id
            for row in self.observations
            if row.execution_status == "observed"
        )
        if tuple(pair.case_id for pair in self.raw_pairs) != observed_ids:
            raise ValueError("raw pairs must exactly cover observed cases")
        if self.experiment_report.run_id != self.run_id:
            raise ValueError("variant evidence report run identity mismatch")
        if self.experiment_report.system_version != self.system_version:
            raise ValueError(
                "variant evidence report system version mismatch"
            )
        if self.experiment_report.pairs != self.raw_pairs:
            raise ValueError("experiment report must retain every raw pair")
        _validate_experiment_report(
            self.experiment_report,
            observed_ids=set(observed_ids),
        )
        if self.mechanism_pairs:
            if tuple(
                pair.case_id for pair in self.mechanism_pairs
            ) != observed_ids:
                raise ValueError(
                    "mechanism pairs must exactly cover observed cases"
                )
            _validate_mechanism_pairs(
                assignments=self.assignments,
                raw_pairs=self.raw_pairs,
                mechanisms=self.mechanism_pairs,
            )
        if self.mechanism_metrics is not None:
            _validate_mechanism_metrics(
                metrics=self.mechanism_metrics,
                registry=self.registry_population,
                selected_case_ids=selected_ids,
                observations=self.observations,
                report=self.experiment_report,
                mechanisms=self.mechanism_pairs,
            )
        if self.execution_mode == "full":
            if selected_ids != registry_ids or self.population_report is None:
                raise ValueError(
                    "full execution requires all cases and a population report"
                )
            method = (
                self.population_report.adaptive_minus_frozen_correctness.method
            )
            try:
                bootstrap_samples = int(method.rsplit("_", 1)[1])
            except (IndexError, ValueError) as exc:
                raise ValueError(
                    "variant population bootstrap method is invalid"
                ) from exc
            recomputed = evaluate_variant_alias_population(
                population=self.registry_population,
                experiment_report=self.experiment_report,
                observations=self.observations,
                bootstrap_samples=bootstrap_samples,
            )
            if recomputed != self.population_report:
                raise ValueError(
                    "variant population report does not match recomputation"
                )
        elif self.population_report is not None:
            raise ValueError(
                "smoke execution cannot claim a full population report"
            )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def artifact_payload(self) -> dict[str, Any]:
        return {
            **self.model_dump(mode="json"),
            "evidence_digest": self.digest,
        }


def validate_variant_population_evidence_artifact(
    payload: dict[str, Any],
) -> CompanyLearningVariantPopulationEvidence:
    """Validate one persisted evidence envelope and its self digest."""

    supplied_digest = str(payload.get("evidence_digest") or "")
    evidence = CompanyLearningVariantPopulationEvidence.model_validate(
        {
            key: value
            for key, value in payload.items()
            if key != "evidence_digest"
        }
    )
    if supplied_digest != evidence.digest:
        raise ValueError("variant population evidence digest mismatch")
    return evidence


def _validate_assignment_isolation(
    assignments: tuple[VariantAliasCaseAssignment, ...],
) -> None:
    tenant_ids = [
        tenant_id
        for assignment in assignments
        for tenant_id in (
            assignment.adaptive_tenant_id,
            assignment.frozen_tenant_id,
        )
    ]
    if len(tenant_ids) != len(set(tenant_ids)):
        raise ValueError("every variant arm requires a fresh tenant")
    entity_ids = [
        entity_id
        for assignment in assignments
        for entity_id in (
            assignment.adaptive_target_id,
            assignment.frozen_target_id,
            assignment.adaptive_conflicting_id,
            assignment.frozen_conflicting_id,
        )
    ]
    if len(entity_ids) != len(set(entity_ids)):
        raise ValueError(
            "variant target and conflicting entities must be unique"
        )


def _validate_mechanism_pairs(
    *,
    assignments: tuple[VariantAliasCaseAssignment, ...],
    raw_pairs: tuple[PairedRecurrenceResult, ...],
    mechanisms: tuple[VariantAliasPairMechanismEvidence, ...],
) -> None:
    assignment_by_case = {
        assignment.case_id: assignment for assignment in assignments
    }
    pair_by_case = {pair.case_id: pair for pair in raw_pairs}
    for mechanism in mechanisms:
        assignment = assignment_by_case[mechanism.case_id]
        pair = pair_by_case[mechanism.case_id]
        for arm_mechanism, result, expected_tenant, expected_target in (
            (
                mechanism.adaptive,
                pair.adaptive,
                assignment.adaptive_tenant_id,
                assignment.adaptive_target_id,
            ),
            (
                mechanism.frozen,
                pair.frozen,
                assignment.frozen_tenant_id,
                assignment.frozen_target_id,
            ),
        ):
            if (
                arm_mechanism.tenant_id != expected_tenant
                or result.tenant_id != expected_tenant
                or arm_mechanism.target_id != expected_target
                or arm_mechanism.llm_call_count != result.llm_call_count
            ):
                raise ValueError(
                    "variant mechanism lineage does not match assignments"
                )
            source_mutated = (
                HardSafetyIncidentClass.SOURCE_OBSERVATION_MUTATED
                in result.observed_safety_incidents
            )
            if arm_mechanism.source_observation_immutable == source_mutated:
                raise ValueError(
                    "variant source immutability does not match pair result"
                )
            if (
                arm_mechanism.scripted_high_confidence_target_response_observed
                and (
                    arm_mechanism.model_output_ref is None
                    or arm_mechanism.model_output_ref.id
                    != str(expected_target)
                    or arm_mechanism.model_output_confidence is None
                    or arm_mechanism.model_output_confidence < 0.8
                )
            ):
                raise ValueError(
                    "variant scripted target response evidence is inconsistent"
                )


def _validate_mechanism_metrics(
    *,
    metrics: VariantAliasMechanismMetrics,
    registry: HeldOutVariantAliasPopulation,
    selected_case_ids: tuple[str, ...],
    observations: tuple[VariantAliasExecutionObservation, ...],
    report: CorrectiveMemoryExperimentReport,
    mechanisms: tuple[VariantAliasPairMechanismEvidence, ...],
) -> None:
    selected_cases = tuple(
        case
        for case in registry.cases
        if case.case_id in set(selected_case_ids)
    )
    pair_count = len(report.pairs)
    unsupported_count = sum(
        observation.execution_status == "unsupported"
        for observation in observations
    )
    core_expected: dict[str, Any] = {
        "selected_case_count": len(selected_case_ids),
        "observed_pair_count": pair_count,
        "unsupported_case_count": unsupported_count,
        "full_registry_coverage_rate": (
            len(selected_case_ids) / len(registry.cases)
        ),
        "observed_execution_rate": pair_count / len(selected_case_ids),
        "adaptive_correctness_rate": (
            report.metrics.adaptive_correctness_rate
        ),
        "frozen_correctness_rate": report.metrics.frozen_correctness_rate,
        "adaptive_minus_frozen_correctness": (
            report.metrics.adaptive_minus_frozen_correctness
        ),
        "hard_safety_incident_count": len(report.incidents),
        "entity_type_counts": dict(
            sorted(
                Counter(case.entity_type for case in selected_cases).items()
            )
        ),
        "variant_family_counts": dict(
            sorted(
                Counter(
                    case.variant_family.value for case in selected_cases
                ).items()
            )
        ),
    }
    if any(
        not _metric_value_matches(getattr(metrics, key), value)
        for key, value in core_expected.items()
    ):
        raise ValueError(
            "variant mechanism metrics do not match population evidence"
        )
    if not mechanisms:
        mechanism_fields = (
            metrics.adaptive_target_candidate_authorization_rate,
            metrics.frozen_target_candidate_exposure_rate,
            metrics.candidate_authorization_gap,
            metrics.adaptive_closed_set_match_rate,
            metrics.frozen_closed_set_match_rate,
            metrics.both_arms_one_llm_call_rate,
            metrics.both_arms_scripted_target_response_rate,
            metrics.frozen_safe_review_or_abstention_rate,
            metrics.source_immutability_rate,
            metrics.candidate_memory_mediated_success_rate,
            metrics.adaptive_mean_llm_calls,
            metrics.frozen_mean_llm_calls,
        )
        if any(value is not None for value in mechanism_fields):
            raise ValueError(
                "mechanism metrics require per-pair mechanism evidence"
            )
        if metrics.control_integrity_violation_count:
            raise ValueError(
                "control violations require mechanism evidence"
            )
        return
    assessment = {
        (row.case_id, row.arm): row for row in report.assessments
    }
    pair_by_case = {pair.case_id: pair for pair in report.pairs}
    adaptive_authorized = sum(
        pair.adaptive.target_candidate_authorized for pair in mechanisms
    )
    frozen_exposed = sum(
        pair.frozen.target_candidate_authorized for pair in mechanisms
    )
    adaptive_closed = sum(
        pair.adaptive.closed_set_match for pair in mechanisms
    )
    frozen_closed = sum(
        pair.frozen.closed_set_match for pair in mechanisms
    )
    one_call = sum(
        pair.adaptive.llm_call_count == 1
        and pair.frozen.llm_call_count == 1
        for pair in mechanisms
    )
    scripted = sum(
        pair.adaptive.scripted_high_confidence_target_response_observed
        and pair.frozen.scripted_high_confidence_target_response_observed
        for pair in mechanisms
    )
    immutable = sum(
        pair.adaptive.source_observation_immutable
        and pair.frozen.source_observation_immutable
        for pair in mechanisms
    )
    safe_frozen = sum(
        pair_by_case[row.case_id].frozen.consumer_fate
        in {
            ConsumerTerminalFate.REVIEW,
            ConsumerTerminalFate.ABSTAINED,
            ConsumerTerminalFate.REJECTED,
            ConsumerTerminalFate.NO_ADMISSION,
        }
        and pair_by_case[row.case_id].frozen.resolved_entity_ref is None
        for row in mechanisms
    )
    ideal = sum(
        mechanism.adaptive.target_candidate_authorized
        and not mechanism.frozen.target_candidate_authorized
        and mechanism.adaptive.closed_set_match
        and not mechanism.frozen.closed_set_match
        and mechanism.adaptive.llm_call_count == 1
        and mechanism.frozen.llm_call_count == 1
        and mechanism.adaptive.scripted_high_confidence_target_response_observed
        and mechanism.frozen.scripted_high_confidence_target_response_observed
        and mechanism.adaptive.source_observation_immutable
        and mechanism.frozen.source_observation_immutable
        and assessment[
            (mechanism.case_id, CorrectiveMemoryArm.ADAPTIVE)
        ].correct
        and pair_by_case[mechanism.case_id].frozen.consumer_fate
        in {
            ConsumerTerminalFate.REVIEW,
            ConsumerTerminalFate.ABSTAINED,
            ConsumerTerminalFate.REJECTED,
            ConsumerTerminalFate.NO_ADMISSION,
        }
        and pair_by_case[mechanism.case_id].frozen.resolved_entity_ref is None
        for mechanism in mechanisms
    )
    rate = lambda value: value / pair_count if pair_count else None
    adaptive_auth_rate = rate(adaptive_authorized)
    frozen_exposure_rate = rate(frozen_exposed)
    mechanism_expected = {
        "adaptive_target_candidate_authorization_rate": adaptive_auth_rate,
        "frozen_target_candidate_exposure_rate": frozen_exposure_rate,
        "candidate_authorization_gap": (
            adaptive_auth_rate - frozen_exposure_rate
            if adaptive_auth_rate is not None
            and frozen_exposure_rate is not None
            else None
        ),
        "adaptive_closed_set_match_rate": rate(adaptive_closed),
        "frozen_closed_set_match_rate": rate(frozen_closed),
        "both_arms_one_llm_call_rate": rate(one_call),
        "both_arms_scripted_target_response_rate": rate(scripted),
        "frozen_safe_review_or_abstention_rate": rate(safe_frozen),
        "source_immutability_rate": rate(immutable),
        "candidate_memory_mediated_success_rate": rate(ideal),
        "adaptive_mean_llm_calls": (
            sum(pair.adaptive.llm_call_count for pair in report.pairs)
            / pair_count
            if pair_count
            else None
        ),
        "frozen_mean_llm_calls": (
            sum(pair.frozen.llm_call_count for pair in report.pairs)
            / pair_count
            if pair_count
            else None
        ),
        "control_integrity_violation_count": pair_count - ideal,
    }
    if any(
        not _metric_value_matches(getattr(metrics, key), value)
        for key, value in mechanism_expected.items()
    ):
        raise ValueError(
            "variant mechanism metrics do not match mechanism evidence"
        )


def _metric_value_matches(observed: Any, expected: Any) -> bool:
    if isinstance(observed, float) or isinstance(expected, float):
        if observed is None or expected is None:
            return observed is expected
        return abs(float(observed) - float(expected)) <= 1e-12
    return observed == expected


def build_variant_alias_population() -> HeldOutVariantAliasPopulation:
    """Build the deterministic 24-case variant-alias registry."""

    cases: list[HeldOutVariantAliasCase] = []
    entities = ("customer", "project", "team", "system")
    contexts = (
        "public_channel",
        "private_channel",
        "cross_thread_recurrence",
    )
    distances = ("same_day", "one_week", "one_month", "one_quarter")
    wordings = (
        "status_update",
        "risk_report",
        "commitment",
        "decision",
        "support_escalation",
        "status_update",
    )
    consequences = ("low", "medium", "high")
    surfaces = _variant_surfaces()
    for family_index, family in enumerate(VariantAliasFamily):
        for entity_index, entity_type in enumerate(entities):
            training_alias, recurrence_alias = surfaces[family][entity_type]
            wording = wordings[family_index]
            consequence = consequences[
                (family_index + entity_index) % len(consequences)
            ]
            cases.append(
                HeldOutVariantAliasCase(
                    case_id=(
                        f"heldout-variant-{family_index:02d}-"
                        f"{entity_type}"
                    ),
                    entity_type=entity_type,
                    variant_family=family,
                    candidate_label=(
                        f"Canonical {training_alias} {entity_type}"
                    ),
                    channel=(
                        f"C-VARIANT-{family_index:02d}-"
                        f"{entity_type.upper()}"
                    ),
                    slack_context=contexts[
                        (family_index + entity_index) % len(contexts)
                    ],
                    wording_variant=wording,
                    consequence=consequence,
                    recurrence_distance=distances[
                        (family_index + entity_index) % len(distances)
                    ],
                    training_alias_surface=training_alias,
                    recurrence_alias_surface=recurrence_alias,
                    training_text=(
                        f"{training_alias} is the sealed {entity_type} "
                        f"for {family.value}."
                    ),
                    recurrence_text=_recurrence_text(
                        alias=recurrence_alias,
                        wording=wording,
                        consequence=consequence,
                    ),
                    ranking_basis=_ranking_basis(family),
                )
            )
    return HeldOutVariantAliasPopulation(cases=tuple(cases))


def load_variant_alias_population(
    path: Path | str,
) -> HeldOutVariantAliasPopulation:
    cases = tuple(
        json.loads(line)
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    )
    return HeldOutVariantAliasPopulation(cases=cases)


def evaluate_variant_alias_population(
    *,
    population: HeldOutVariantAliasPopulation,
    experiment_report: CorrectiveMemoryExperimentReport,
    observations: tuple[VariantAliasExecutionObservation, ...],
    bootstrap_samples: int = 2000,
) -> VariantAliasPopulationReport:
    """Evaluate one exact registry without survivor-only reruns."""

    if bootstrap_samples < 200:
        raise ValueError("bootstrap_samples must be at least 200")
    ordered_observations = _ordered_observations(
        population=population,
        observations=observations,
    )
    observed_ids = {
        observation.case_id
        for observation in ordered_observations
        if observation.execution_status == "observed"
    }
    unsupported_ids = {
        observation.case_id
        for observation in ordered_observations
        if observation.execution_status == "unsupported"
    }
    _validate_experiment_report(
        experiment_report,
        observed_ids=observed_ids,
    )
    if not observed_ids:
        raise ValueError(
            "variant-alias population has no runtime-supported cases"
        )
    assessments = {
        (assessment.case_id, assessment.arm): assessment
        for assessment in experiment_report.assessments
    }
    seed = int(
        canonical_sha256(
            {
                "population_digest": population.digest,
                "report_digest": experiment_report.digest,
                "observations": [
                    observation.model_dump(mode="json")
                    for observation in ordered_observations
                ],
            }
        )[:16],
        16,
    )
    overall = _estimate_assessments(
        case_ids=observed_ids,
        assessments=assessments,
        seed=seed,
        bootstrap_samples=bootstrap_samples,
    )
    unsupported_reasons = Counter(
        str(observation.unsupported_reason)
        for observation in ordered_observations
        if observation.execution_status == "unsupported"
    )
    return VariantAliasPopulationReport(
        population_definition_version=population.population_definition_version,
        population_digest=population.digest,
        experiment_report_digest=experiment_report.digest,
        observation_digest=canonical_sha256(
            [
                observation.model_dump(mode="json")
                for observation in ordered_observations
            ]
        ),
        pair_count=len(population.cases),
        observed_pair_count=len(observed_ids),
        unsupported_case_count=len(unsupported_ids),
        complete_population=True,
        strata_counts=_strata_counts(population),
        observed_strata_counts=_strata_counts(
            population,
            case_ids=observed_ids,
        ),
        unsupported_strata_counts=_strata_counts(
            population,
            case_ids=unsupported_ids,
        ),
        unsupported_reason_counts=dict(sorted(unsupported_reasons.items())),
        adaptive_correctness=overall["adaptive_correctness"],
        frozen_correctness=overall["frozen_correctness"],
        adaptive_minus_frozen_correctness=overall[
            "adaptive_minus_frozen_correctness"
        ],
        adaptive_unsafe_rate=overall["adaptive_unsafe_rate"],
        frozen_unsafe_rate=overall["frozen_unsafe_rate"],
        family_reports=_stratum_reports(
            population=population,
            observations=ordered_observations,
            assessments=assessments,
            dimension="variant_family",
            bootstrap_samples=bootstrap_samples,
            seed=seed + 100,
        ),
        entity_type_reports=_stratum_reports(
            population=population,
            observations=ordered_observations,
            assessments=assessments,
            dimension="entity_type",
            bootstrap_samples=bootstrap_samples,
            seed=seed + 200,
        ),
    )


def _ordered_observations(
    *,
    population: HeldOutVariantAliasPopulation,
    observations: tuple[VariantAliasExecutionObservation, ...],
) -> tuple[VariantAliasExecutionObservation, ...]:
    observation_ids = [observation.case_id for observation in observations]
    if len(observation_ids) != len(set(observation_ids)):
        raise ValueError(
            "variant-alias observations must be unique by case"
        )
    expected_ids = {case.case_id for case in population.cases}
    observed_ids = set(observation_ids)
    if observed_ids != expected_ids:
        raise ValueError(
            "variant-alias observations must exactly cover the sealed "
            f"population; missing={sorted(expected_ids - observed_ids)}, "
            f"extra={sorted(observed_ids - expected_ids)}"
        )
    by_case = {
        observation.case_id: observation for observation in observations
    }
    return tuple(by_case[case.case_id] for case in population.cases)


def _validate_experiment_report(
    report: CorrectiveMemoryExperimentReport,
    *,
    observed_ids: set[str],
) -> None:
    if VARIANT_ALIAS_SCENARIO_ID not in report.scenario_ids:
        raise ValueError(
            "variant experiment report is missing the sealed scenario"
        )
    pair_ids = [pair.case_id for pair in report.pairs]
    if len(pair_ids) != len(set(pair_ids)) or set(pair_ids) != observed_ids:
        raise ValueError(
            "variant experiment pairs must exactly cover observed cases"
        )
    if report.pair_results_digest != canonical_sha256(
        [pair.model_dump(mode="json") for pair in report.pairs]
    ):
        raise ValueError("variant experiment pair digest mismatch")
    assessment_keys = [
        (assessment.case_id, assessment.arm)
        for assessment in report.assessments
    ]
    expected_keys = {
        (case_id, arm)
        for case_id in observed_ids
        for arm in CorrectiveMemoryArm
    }
    if (
        len(assessment_keys) != len(set(assessment_keys))
        or set(assessment_keys) != expected_keys
    ):
        raise ValueError(
            "variant experiment assessments must cover both arms exactly"
        )
    pair_by_case = {pair.case_id: pair for pair in report.pairs}
    for assessment in report.assessments:
        pair = pair_by_case[assessment.case_id]
        result = (
            pair.adaptive
            if assessment.arm is CorrectiveMemoryArm.ADAPTIVE
            else pair.frozen
        )
        if (
            assessment.consumer_fate is not result.consumer_fate
            or assessment.resolved_entity_ref != result.resolved_entity_ref
            or assessment.observed_model_count != len(result.lineage.model_ids)
            or not result.observed_safety_incidents.issubset(
                assessment.incident_classes
            )
        ):
            raise ValueError(
                "variant experiment assessments do not match pair results"
            )
    incident_keys = {
        (
            incident.case_id,
            incident.arm,
            incident.incident_class,
        )
        for incident in report.incidents
    }
    assessed_incident_keys = {
        (
            assessment.case_id,
            assessment.arm,
            incident_class,
        )
        for assessment in report.assessments
        for incident_class in assessment.incident_classes
    }
    if incident_keys != assessed_incident_keys:
        raise ValueError(
            "variant experiment incidents do not match assessments"
        )
    if report.metrics.pair_count != len(observed_ids):
        raise ValueError("variant experiment pair count mismatch")
    variant_metrics = report.metrics.case_kind_metrics.get(
        RecurrenceCaseKind.VARIANT_ALIAS_POSITIVE.value
    )
    if (
        variant_metrics is None
        or variant_metrics.get("pair_count") != len(observed_ids)
    ):
        raise ValueError(
            "variant experiment report is not sealed to variant-alias cases"
        )
    adaptive = tuple(
        assessment
        for assessment in report.assessments
        if assessment.arm is CorrectiveMemoryArm.ADAPTIVE
    )
    frozen = tuple(
        assessment
        for assessment in report.assessments
        if assessment.arm is CorrectiveMemoryArm.FROZEN
    )
    expected_metrics = {
        "adaptive_correct_count": sum(item.correct for item in adaptive),
        "frozen_correct_count": sum(item.correct for item in frozen),
        "adaptive_unsafe_count": sum(
            bool(item.incident_classes) for item in adaptive
        ),
        "frozen_unsafe_count": sum(
            bool(item.incident_classes) for item in frozen
        ),
    }
    if any(
        getattr(report.metrics, key) != value
        for key, value in expected_metrics.items()
    ):
        raise ValueError(
            "variant experiment metrics do not match assessments"
        )


def _estimate_assessments(
    *,
    case_ids: set[str],
    assessments: dict[
        tuple[str, CorrectiveMemoryArm],
        CorrectiveMemoryArmAssessment,
    ],
    seed: int,
    bootstrap_samples: int,
) -> dict[str, IntervalEstimate]:
    adaptive = [
        assessments[(case_id, CorrectiveMemoryArm.ADAPTIVE)]
        for case_id in sorted(case_ids)
    ]
    frozen = [
        assessments[(case_id, CorrectiveMemoryArm.FROZEN)]
        for case_id in sorted(case_ids)
    ]
    adaptive_correct = [float(item.correct) for item in adaptive]
    frozen_correct = [float(item.correct) for item in frozen]
    adaptive_unsafe = [
        float(bool(item.incident_classes)) for item in adaptive
    ]
    frozen_unsafe = [
        float(bool(item.incident_classes)) for item in frozen
    ]
    lift = [
        adaptive_value - frozen_value
        for adaptive_value, frozen_value in zip(
            adaptive_correct,
            frozen_correct,
            strict=True,
        )
    ]
    return {
        "adaptive_correctness": _wilson_estimate(adaptive_correct),
        "frozen_correctness": _wilson_estimate(frozen_correct),
        "adaptive_minus_frozen_correctness": _bootstrap_mean_estimate(
            lift,
            seed=seed,
            samples=bootstrap_samples,
        ),
        "adaptive_unsafe_rate": _wilson_estimate(adaptive_unsafe),
        "frozen_unsafe_rate": _wilson_estimate(frozen_unsafe),
    }


def _strata_counts(
    population: HeldOutVariantAliasPopulation,
    *,
    case_ids: set[str] | None = None,
) -> dict[str, dict[str, int]]:
    dimensions = {
        "entity_type": lambda case: case.entity_type,
        "variant_family": lambda case: case.variant_family.value,
        "slack_context": lambda case: case.slack_context,
        "wording_variant": lambda case: case.wording_variant,
        "consequence": lambda case: case.consequence,
        "recurrence_distance": lambda case: case.recurrence_distance,
    }
    result: dict[str, dict[str, int]] = {}
    for dimension, getter in dimensions.items():
        counts = Counter(
            getter(case)
            for case in population.cases
            if case_ids is None or case.case_id in case_ids
        )
        result[dimension] = dict(sorted(counts.items()))
    return result


def _stratum_reports(
    *,
    population: HeldOutVariantAliasPopulation,
    observations: tuple[VariantAliasExecutionObservation, ...],
    assessments: dict[
        tuple[str, CorrectiveMemoryArm],
        CorrectiveMemoryArmAssessment,
    ],
    dimension: Literal["entity_type", "variant_family"],
    bootstrap_samples: int,
    seed: int,
) -> dict[str, VariantAliasStratumReport]:
    observation_by_case = {
        observation.case_id: observation for observation in observations
    }
    values = sorted(
        {
            (
                case.entity_type
                if dimension == "entity_type"
                else case.variant_family.value
            )
            for case in population.cases
        }
    )
    result: dict[str, VariantAliasStratumReport] = {}
    for index, value in enumerate(values):
        sealed_ids = {
            case.case_id
            for case in population.cases
            if (
                case.entity_type
                if dimension == "entity_type"
                else case.variant_family.value
            )
            == value
        }
        observed_ids = {
            case_id
            for case_id in sealed_ids
            if observation_by_case[case_id].execution_status == "observed"
        }
        estimates = (
            _estimate_assessments(
                case_ids=observed_ids,
                assessments=assessments,
                seed=seed + index,
                bootstrap_samples=bootstrap_samples,
            )
            if observed_ids
            else {}
        )
        result[value] = VariantAliasStratumReport(
            sealed_case_count=len(sealed_ids),
            observed_case_count=len(observed_ids),
            unsupported_case_count=len(sealed_ids - observed_ids),
            adaptive_correctness=estimates.get("adaptive_correctness"),
            frozen_correctness=estimates.get("frozen_correctness"),
            adaptive_minus_frozen_correctness=estimates.get(
                "adaptive_minus_frozen_correctness"
            ),
            adaptive_unsafe_rate=estimates.get("adaptive_unsafe_rate"),
            frozen_unsafe_rate=estimates.get("frozen_unsafe_rate"),
        )
    return result


def _ranking_rule_matches(case: HeldOutVariantAliasCase) -> bool:
    training = case.training_alias_surface
    recurrence = case.recurrence_alias_surface
    training_norm = _norm(training)
    recurrence_norm = _norm(recurrence)
    training_compact = _compact(training)
    recurrence_compact = _compact(recurrence)
    family = case.variant_family
    if family is VariantAliasFamily.ACRONYM_FROM_LONG_FORM:
        return recurrence_compact == "".join(
            token[0] for token in _tokens(training)
        )
    if family is VariantAliasFamily.PUNCTUATION_COMPACT_FORM:
        return "." in training and recurrence_compact == training_compact
    if family is VariantAliasFamily.HYPHEN_SPACING:
        return "-" in training and recurrence_compact == training_compact
    if family is VariantAliasFamily.ANCHORED_SHORT_FORM:
        return recurrence_norm in training_norm
    if family is VariantAliasFamily.ORTHOGRAPHIC_OMISSION_SUBSEQUENCE:
        return _is_subsequence(recurrence_compact, training_compact)
    if family is VariantAliasFamily.POSSESSIVE_OR_PLURAL:
        return training_norm in recurrence_norm
    return False


def _variant_surfaces() -> dict[
    VariantAliasFamily,
    dict[str, tuple[str, str]],
]:
    return {
        VariantAliasFamily.ACRONYM_FROM_LONG_FORM: {
            "customer": ("Nimbus Banking Initiative", "NBI"),
            "project": ("Phoenix Launch Program", "PLP"),
            "team": ("Revenue Operations Group", "ROG"),
            "system": ("Order Management Service", "OMS"),
        },
        VariantAliasFamily.PUNCTUATION_COMPACT_FORM: {
            "customer": ("Atlas.Pay", "AtlasPay"),
            "project": ("Project.Nova", "ProjectNova"),
            "team": ("Growth.Ops", "GrowthOps"),
            "system": ("Data.Hub", "DataHub"),
        },
        VariantAliasFamily.HYPHEN_SPACING: {
            "customer": ("North-Star", "North Star"),
            "project": ("Launch-Bridge", "Launch Bridge"),
            "team": ("Core-Platform", "Core Platform"),
            "system": ("Risk-Engine", "Risk Engine"),
        },
        VariantAliasFamily.ANCHORED_SHORT_FORM: {
            "customer": ("Orion Holdings", "Orion"),
            "project": ("Mercury Migration", "Mercury"),
            "team": ("Falcon Support", "Falcon"),
            "system": ("Beacon Gateway", "Beacon"),
        },
        VariantAliasFamily.ORTHOGRAPHIC_OMISSION_SUBSEQUENCE: {
            "customer": ("Nimbus", "Nmbus"),
            "project": ("Phoenix", "Phenix"),
            "team": ("Revenue", "Revnue"),
            "system": ("Telemetry", "Telemtry"),
        },
        VariantAliasFamily.POSSESSIVE_OR_PLURAL: {
            "customer": ("Acme", "Acme's"),
            "project": ("Horizon", "Horizons"),
            "team": ("Platform", "Platform's"),
            "system": ("Sentinel", "Sentinels"),
        },
    }


def _ranking_basis(family: VariantAliasFamily) -> str:
    return {
        VariantAliasFamily.ACRONYM_FROM_LONG_FORM: (
            "recurrence compact form equals the training-token acronym"
        ),
        VariantAliasFamily.PUNCTUATION_COMPACT_FORM: (
            "training and recurrence surfaces have equal alphanumeric compact forms"
        ),
        VariantAliasFamily.HYPHEN_SPACING: (
            "hyphen and whitespace variants have equal compact forms"
        ),
        VariantAliasFamily.ANCHORED_SHORT_FORM: (
            "the source-anchored short form is a normalized substring of the alias"
        ),
        VariantAliasFamily.ORTHOGRAPHIC_OMISSION_SUBSEQUENCE: (
            "the omitted-letter compact form is a subsequence of the alias"
        ),
        VariantAliasFamily.POSSESSIVE_OR_PLURAL: (
            "the normalized training alias is contained in the inflected form"
        ),
    }[family]


def _recurrence_text(
    *,
    alias: str,
    wording: str,
    consequence: str,
) -> str:
    return {
        "status_update": f"{alias} is ready for the next review.",
        "risk_report": f"{alias} now has a {consequence} delivery risk.",
        "commitment": f"We committed the next milestone for {alias}.",
        "decision": f"The decision for {alias} is ready for review.",
        "support_escalation": (
            f"{alias} has a {consequence} support escalation."
        ),
    }[wording]


def _norm(value: str) -> str:
    return " ".join(value.casefold().split())


def _compact(value: str) -> str:
    return "".join(character for character in _norm(value) if character.isalnum())


def _tokens(value: str) -> list[str]:
    return [
        token
        for token in _norm(value).replace("-", " ").split()
        if token and any(character.isalpha() for character in token)
    ]


def _is_subsequence(needle: str, haystack: str) -> bool:
    if not needle:
        return False
    position = 0
    for character in haystack:
        if position < len(needle) and needle[position] == character:
            position += 1
    return position == len(needle)


__all__ = [
    "CompanyLearningVariantPopulationEvidence",
    "HeldOutVariantAliasCase",
    "HeldOutVariantAliasPopulation",
    "VARIANT_ALIAS_SCENARIO_ID",
    "VariantAliasArmMechanismEvidence",
    "VariantAliasCaseAssignment",
    "VariantAliasExecutionObservation",
    "VariantAliasFamily",
    "VariantAliasMechanismMetrics",
    "VariantAliasPairMechanismEvidence",
    "VariantAliasPopulationReport",
    "VariantAliasStratumReport",
    "build_variant_alias_population",
    "evaluate_variant_alias_population",
    "load_variant_alias_population",
    "validate_variant_population_evidence_artifact",
]
