from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_experiment import (
    CanonicalEntityRef,
    ConsumerTerminalFate,
    CorrectiveMemoryArm,
)
from lib.evaluation.company_learning_assurance import (
    ActiveSurfacesAssurance,
    CanonicalReplacementAssurance,
    CompanyLearningAssuranceSummary,
    CorrectionAssurance,
    CustomerLifecycleAssurance,
    NegativeAssurance,
    PopulationAssurance,
    PositiveAssurance,
    RetentionAssurance,
    SourceBindingLifecycleAssurance,
    SlackAssurance,
    VariantCollisionAssurance,
    VariantPopulationAssurance,
    validate_company_learning_assurance_artifact,
    validate_active_surfaces_assurance_component,
    validate_canonical_replacement_assurance_component,
    validate_correction_assurance_component,
    validate_customer_lifecycle_assurance_component,
    validate_retention_assurance_component,
    validate_source_binding_lifecycle_assurance_component,
    validate_variant_collision_assurance_component,
    validate_variant_population_assurance_component,
)
from lib.evaluation.canonical_referent_replacement import (
    CanonicalReplacementDatabaseEvidence,
    CanonicalResourceReplacementEvidence,
    ReplacementProofCell,
    evaluate_canonical_resource_replacement,
)
from lib.evaluation.company_learning_active_surfaces import (
    ActiveLearningSurfacesEvidence,
    SEALED_ACTIVE_SURFACE_CLAIMS,
    SourceSalienceObservation,
    StructuredIdentitySurfaceObservation,
    evaluate_active_learning_surfaces,
)
from lib.evaluation.company_learning_customer_lifecycle import (
    build_customer_lifecycle_population,
    evaluate_customer_lifecycle_population,
)
from lib.evaluation.company_learning_population import IntervalEstimate
from lib.evaluation.company_learning_retention import (
    CompanyLearningRetentionReport,
    RetentionBehavior,
    RetentionCaseSpec,
    RetentionHorizon,
    RetentionObservation,
    RetentionRunSpec,
    evaluate_company_learning_retention,
)
from lib.evaluation.company_learning_variant_population import (
    CompanyLearningVariantPopulationEvidence,
    VariantAliasExecutionObservation,
    VariantAliasMechanismMetrics,
    build_variant_alias_population,
    evaluate_variant_alias_population,
)
from lib.evaluation.company_learning_variant_collisions import (
    HeldOutVariantCollisionPopulation,
    VariantCollisionArmObservation,
    VariantCollisionDecisionBasis,
    VariantCollisionFamily,
    VariantCollisionPairObservation,
    VariantCollisionPopulationReport,
    VariantCollisionTargetRole,
    build_variant_collision_population,
    evaluate_variant_collision_population,
)
from lib.evaluation.correction_assurance import (
    CorrectionAssuranceArtifact,
    CorrectionRuntimeEvidence,
    build_correction_assurance,
)
from lib.evaluation.correction_propagation import (
    CorrectionPropagationAudit,
    CorrectionPropagationScope,
)
from lib.evaluation.proof import EvidenceTier
from lib.evaluation.repository_provenance import (
    capture_repository_provenance,
)
from lib.evaluation.tests.test_company_learning_variant_population import (
    FIXTURE as VARIANT_FIXTURE,
)
from lib.evaluation.tests.test_company_learning_variant_population import (
    _assignments as _variant_assignments,
)
from lib.evaluation.tests.test_company_learning_variant_population import (
    _experiment_report as _variant_experiment_report,
)
from lib.evaluation.tests.test_company_learning_variant_population import (
    _mechanism_metrics as _variant_mechanism_metrics,
)
from lib.evaluation.tests.test_company_learning_variant_population import (
    _mechanisms as _variant_mechanisms,
)
from lib.evaluation.tests.test_company_learning_variant_collisions import (
    _collision_refs,
)
from lib.evaluation.tests.test_company_learning_variant_collisions import (
    _safe_observations as _safe_collision_observations,
)
from lib.evaluation.tests.test_company_learning_customer_lifecycle import (
    _safe_observations as _safe_lifecycle_observations,
)
from lib.evaluation.tests.test_canonical_referent_replacement import (
    _observation as _replacement_observation,
)
from lib.evaluation.tests.test_source_identity_binding_lifecycle import (
    _observation as _binding_lifecycle_observation,
)
from scripts.run_company_learning_customer_lifecycle_db import (
    CompanyLearningCustomerLifecycleEvidence,
    CustomerLifecycleRuntimeAssignment,
)
from lib.evaluation.source_identity_binding_lifecycle import (
    BindingLifecycleProofCell,
    SourceIdentityBindingLifecycleEvidence,
    evaluate_source_identity_binding_lifecycle,
)


_DIGEST = "a" * 64
_ARCHITECTURE_DIGEST = "b" * 64
_IMPLEMENTATION_PLAN_DIGEST = "c" * 64
_SOURCE_ID_GAP = "runtime lacks authenticated SourceIdentityBinding evidence"


@dataclass(frozen=True)
class _CollisionEvidenceFixture:
    created_at: str
    run_id: str
    system_version: str
    registry_path: str
    registry_population: HeldOutVariantCollisionPopulation
    registry_population_digest: str
    assignments: tuple[dict[str, str], ...]
    observations: tuple[VariantCollisionPairObservation, ...]
    report: VariantCollisionPopulationReport
    artifact_refs: tuple[str, ...]

    @property
    def digest(self) -> str:
        return canonical_sha256(self._payload())

    def _payload(self) -> dict[str, object]:
        return {
            "schema_version": ("company-learning-variant-collision-evidence-v1"),
            "created_at": self.created_at,
            "run_id": self.run_id,
            "system_version": self.system_version,
            "registry_path": self.registry_path,
            "registry_population": self.registry_population.model_dump(mode="json"),
            "registry_population_digest": self.registry_population_digest,
            "assignments": list(self.assignments),
            "observations": [row.model_dump(mode="json") for row in self.observations],
            "report": self.report.model_dump(mode="json"),
            "artifact_refs": list(self.artifact_refs),
        }

    def artifact_payload(self) -> dict[str, object]:
        return {**self._payload(), "evidence_digest": self.digest}


def _interval(
    point_estimate: float,
    *,
    sample_size: int = 24,
) -> IntervalEstimate:
    return IntervalEstimate(
        point_estimate=point_estimate,
        lower_95=point_estimate,
        upper_95=point_estimate,
        method="pytest-exact",
        sample_size=sample_size,
    )


def _collision_assurance(
    *,
    path: str = "/tmp/variant-collision-evidence.json",
    status: str = "observed",
    observed_pair_count: int = 16,
    unsupported_case_count: int = 0,
    adaptive_safe_containment_rate: float = 1.0,
    adaptive_unsafe_rate: float = 0.0,
    safety_incident_count: int = 0,
    source_native_authoritative_rate: float = 1.0,
) -> VariantCollisionAssurance:
    def behavior(value: float) -> IntervalEstimate:
        return _interval(value, sample_size=observed_pair_count)

    return VariantCollisionAssurance(
        status=status,
        evidence_tier=EvidenceTier.E4,
        registry_pair_count=16,
        observed_pair_count=observed_pair_count,
        unsupported_case_count=unsupported_case_count,
        runtime_support_rate=_interval(
            observed_pair_count / 16,
            sample_size=16,
        ),
        adaptive_safe_containment_rate=behavior(adaptive_safe_containment_rate),
        frozen_safe_containment_rate=behavior(1.0),
        adaptive_unsafe_rate=behavior(adaptive_unsafe_rate),
        frozen_unsafe_rate=behavior(0.0),
        adaptive_unsafe_resolution_rate=behavior(adaptive_unsafe_rate),
        frozen_unsafe_resolution_rate=behavior(0.0),
        adaptive_authoritative_resolution_rate=behavior(
            (2 / observed_pair_count if unsupported_case_count == 0 else 0.0)
        ),
        frozen_authoritative_resolution_rate=behavior(
            (2 / observed_pair_count if unsupported_case_count == 0 else 0.0)
        ),
        adaptive_candidate_visibility_rate=behavior(1.0),
        frozen_candidate_visibility_rate=behavior(1.0),
        adaptive_none_of_above_availability_rate=behavior(1.0),
        frozen_none_of_above_availability_rate=behavior(1.0),
        adaptive_learned_promotion_rate=behavior(0.0),
        frozen_learned_promotion_rate=behavior(0.0),
        adaptive_wrong_model_rate=behavior(0.0),
        frozen_wrong_model_rate=behavior(0.0),
        adaptive_wrong_model_count=0,
        frozen_wrong_model_count=0,
        adaptive_source_immutability_rate=behavior(1.0),
        frozen_source_immutability_rate=behavior(1.0),
        safety_incident_count=safety_incident_count,
        source_native_observed_case_count=(2 if unsupported_case_count == 0 else 0),
        source_native_unsupported_case_count=unsupported_case_count,
        source_native_adaptive_authoritative_resolution_rate=(
            _interval(source_native_authoritative_rate, sample_size=2)
            if unsupported_case_count == 0
            else None
        ),
        source_native_frozen_authoritative_resolution_rate=(
            _interval(source_native_authoritative_rate, sample_size=2)
            if unsupported_case_count == 0
            else None
        ),
        unsupported_strata_counts={
            "collision_family": {
                (
                    VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER.value
                ): unsupported_case_count,
            },
            "learned_entity_type": (
                {"system": 1, "team": 1} if unsupported_case_count == 2 else {}
            ),
            "entity_type_relation": (
                {"same_type": 2} if unsupported_case_count == 2 else {}
            ),
            "learned_lifecycle": ({"active": 2} if unsupported_case_count == 2 else {}),
        },
        unsupported_reason_counts=(
            {_SOURCE_ID_GAP: unsupported_case_count} if unsupported_case_count else {}
        ),
        artifact_paths={"variant_collision_evidence": path},
        component_digests={
            "evidence": _DIGEST,
            "registry": _DIGEST,
            "report": _DIGEST,
            "observations": _DIGEST,
        },
    )


def _collision_evidence(
    *,
    run_id: str = "pytest-assurance:collision",
    system_version: str = "pytest-system",
) -> _CollisionEvidenceFixture:
    population = build_variant_collision_population()
    observations = list(_safe_collision_observations(population))
    for index, case in enumerate(population.cases):
        if (
            case.collision_family
            is VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER
        ):
            observations[index] = VariantCollisionPairObservation(
                case_id=case.case_id,
                adaptive=_authoritative_collision_arm(
                    case=case,
                    arm=CorrectiveMemoryArm.ADAPTIVE,
                ),
                frozen=_authoritative_collision_arm(
                    case=case,
                    arm=CorrectiveMemoryArm.FROZEN,
                ),
            )
    typed_observations = tuple(observations)
    report = evaluate_variant_collision_population(
        population=population,
        observations=typed_observations,
    )
    return _CollisionEvidenceFixture(
        created_at="2026-07-16T00:00:00+00:00",
        run_id=run_id,
        system_version=system_version,
        registry_path="/tmp/collision-registry.jsonl",
        registry_population=population,
        registry_population_digest=population.digest,
        assignments=tuple(
            {
                "case_id": case.case_id,
                "adaptive_tenant_id": str(uuid4()),
                "frozen_tenant_id": str(uuid4()),
                "adaptive_target_id": str(uuid4()),
                "frozen_target_id": str(uuid4()),
                "adaptive_conflicting_id": str(uuid4()),
                "frozen_conflicting_id": str(uuid4()),
            }
            for case in population.cases
        ),
        observations=typed_observations,
        report=report,
        artifact_refs=("pytest:collision-assurance",),
    )


def _authoritative_collision_arm(
    *,
    case,
    arm: CorrectiveMemoryArm,
) -> VariantCollisionArmObservation:
    learned_ref, conflicting_ref = _collision_refs(case)
    visible_refs = (learned_ref, conflicting_ref)
    assert case.conflicting_source_native_id is not None
    return VariantCollisionArmObservation(
        arm=arm,
        consumer_fate=ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
        resolved_entity_ref=conflicting_ref,
        decision_basis=(
            VariantCollisionDecisionBasis.AUTHENTICATED_SOURCE_NATIVE_IDENTIFIER
        ),
        resolved_target_role=VariantCollisionTargetRole.CONFLICTING,
        decisive_source_native_id=case.conflicting_source_native_id,
        learned_alias_promoted=False,
        candidate_set_digest=canonical_sha256(
            [ref.model_dump(mode="json") for ref in visible_refs]
        ),
        candidate_set_size=2,
        visible_candidate_refs=visible_refs,
        learned_candidate_ref=learned_ref,
        conflicting_candidate_ref=conflicting_ref,
        both_colliding_candidates_visible=True,
        none_of_above_available=True,
        wrong_model_count=0,
        source_observation_immutable=True,
        artifact_refs=(f"pytest:source-native:{case.case_id}:{arm.value}",),
    )


def _collision_assurance_from_evidence(
    evidence: _CollisionEvidenceFixture,
    *,
    path: str,
) -> VariantCollisionAssurance:
    report = evidence.report
    source_native = report.stratum_reports["collision_family"][
        VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER.value
    ]
    return VariantCollisionAssurance(
        status=report.status,
        evidence_tier=EvidenceTier.E4,
        registry_pair_count=report.pair_count,
        observed_pair_count=report.observed_pair_count,
        unsupported_case_count=report.unsupported_case_count,
        runtime_support_rate=report.runtime_support_rate,
        adaptive_safe_containment_rate=(report.adaptive_safe_containment_rate),
        frozen_safe_containment_rate=report.frozen_safe_containment_rate,
        adaptive_unsafe_rate=report.adaptive_unsafe_rate,
        frozen_unsafe_rate=report.frozen_unsafe_rate,
        adaptive_unsafe_resolution_rate=(report.adaptive_unsafe_resolution_rate),
        frozen_unsafe_resolution_rate=(report.frozen_unsafe_resolution_rate),
        adaptive_authoritative_resolution_rate=(
            report.adaptive_authoritative_resolution_rate
        ),
        frozen_authoritative_resolution_rate=(
            report.frozen_authoritative_resolution_rate
        ),
        adaptive_candidate_visibility_rate=(report.adaptive_candidate_visibility_rate),
        frozen_candidate_visibility_rate=(report.frozen_candidate_visibility_rate),
        adaptive_none_of_above_availability_rate=(
            report.adaptive_none_of_above_availability_rate
        ),
        frozen_none_of_above_availability_rate=(
            report.frozen_none_of_above_availability_rate
        ),
        adaptive_learned_promotion_rate=(report.adaptive_learned_promotion_rate),
        frozen_learned_promotion_rate=(report.frozen_learned_promotion_rate),
        adaptive_wrong_model_rate=report.adaptive_wrong_model_rate,
        frozen_wrong_model_rate=report.frozen_wrong_model_rate,
        adaptive_wrong_model_count=report.adaptive_wrong_model_count,
        frozen_wrong_model_count=report.frozen_wrong_model_count,
        adaptive_source_immutability_rate=(report.adaptive_source_immutability_rate),
        frozen_source_immutability_rate=(report.frozen_source_immutability_rate),
        safety_incident_count=report.safety_incident_count,
        source_native_observed_case_count=(source_native.observed_case_count),
        source_native_unsupported_case_count=(source_native.unsupported_case_count),
        source_native_adaptive_authoritative_resolution_rate=(
            source_native.adaptive_authoritative_resolution_rate
        ),
        source_native_frozen_authoritative_resolution_rate=(
            source_native.frozen_authoritative_resolution_rate
        ),
        unsupported_strata_counts=report.unsupported_strata_counts,
        unsupported_reason_counts=report.unsupported_reason_counts,
        artifact_paths={"variant_collision_evidence": path},
        component_digests={
            "evidence": evidence.digest,
            "registry": evidence.registry_population_digest,
            "report": report.digest,
            "observations": canonical_sha256(
                [row.model_dump(mode="json") for row in evidence.observations]
            ),
        },
    )


def _lifecycle_evidence(
    *,
    run_id: str = "pytest-assurance:customer-lifecycle",
    system_version: str = "pytest-system",
) -> CompanyLearningCustomerLifecycleEvidence:
    population = build_customer_lifecycle_population()
    observations = _safe_lifecycle_observations()
    report = evaluate_customer_lifecycle_population(
        population=population,
        observations=observations,
    )
    return CompanyLearningCustomerLifecycleEvidence(
        created_at="2026-07-16T00:00:00+00:00",
        run_id=run_id,
        system_version=system_version,
        registry_path="/tmp/customer-lifecycle-registry.jsonl",
        registry_population=population,
        registry_population_digest=population.digest,
        assignments=tuple(
            CustomerLifecycleRuntimeAssignment(
                case_id=case.case_id,
                tenant_id=uuid4(),
                isolation_tenant_id=uuid4(),
            )
            for case in population.cases
        ),
        observations=observations,
        report=report,
        artifact_refs=("pytest:customer-lifecycle",),
    )


def _lifecycle_assurance_from_evidence(
    evidence: CompanyLearningCustomerLifecycleEvidence,
    *,
    path: str,
) -> CustomerLifecycleAssurance:
    report = evidence.report
    return CustomerLifecycleAssurance(
        status="failed" if report.status == "contradicted" else report.status,
        evidence_tier=EvidenceTier.E4,
        case_count=report.case_count,
        observed_case_count=report.observed_case_count,
        unsupported_case_count=report.unsupported_case_count,
        violating_case_count=report.violating_case_count,
        runtime_support_rate=report.runtime_support_rate,
        rename_continuity_rate=report.rename_continuity_rate,
        valid_time_resolution_accuracy=report.valid_time_resolution_accuracy,
        stale_alias_rejection_rate=report.stale_alias_rejection_rate,
        current_alias_safety_rate=report.current_alias_safety_rate,
        historical_name_reuse_accuracy=(report.historical_name_reuse_accuracy),
        observation_immutability_rate=(report.observation_immutability_rate),
        model_immutability_rate=report.model_immutability_rate,
        archive_alias_rejection_rate=report.archive_alias_rejection_rate,
        archived_mutation_rejection_rate=(report.archived_mutation_rejection_rate),
        alias_interval_non_overlap_rate=(report.alias_interval_non_overlap_rate),
        tenant_isolation_rate=report.tenant_isolation_rate,
        replay_idempotency_rate=report.replay_idempotency_rate,
        unsupported_reason_counts=report.unsupported_reason_counts,
        artifact_paths={"customer_lifecycle_evidence": path},
        component_digests={
            "evidence": evidence.digest,
            "registry": evidence.registry_population_digest,
            "report": report.digest,
            "observations": canonical_sha256(
                [row.model_dump(mode="json") for row in evidence.observations]
            ),
        },
    )


def _lifecycle_assurance(
    *,
    path: str = "/tmp/customer-lifecycle-evidence.json",
    metric_value: float = 1.0,
    unsupported_case_count: int = 0,
    violating_case_count: int = 0,
) -> CustomerLifecycleAssurance:
    observed = 8 - unsupported_case_count
    metric = _interval(metric_value, sample_size=observed)
    support = _interval(observed / 8, sample_size=8)
    blocking = bool(
        unsupported_case_count or violating_case_count or metric_value < 1.0
    )
    return CustomerLifecycleAssurance(
        status=(
            "failed"
            if blocking
            else "observed"
            if observed == 8
            else "observed_with_gaps"
        ),
        evidence_tier=EvidenceTier.E4,
        case_count=8,
        observed_case_count=observed,
        unsupported_case_count=unsupported_case_count,
        violating_case_count=violating_case_count,
        runtime_support_rate=support,
        rename_continuity_rate=metric,
        valid_time_resolution_accuracy=metric,
        stale_alias_rejection_rate=metric,
        current_alias_safety_rate=metric,
        historical_name_reuse_accuracy=metric,
        observation_immutability_rate=metric,
        model_immutability_rate=metric,
        archive_alias_rejection_rate=metric,
        archived_mutation_rejection_rate=metric,
        alias_interval_non_overlap_rate=metric,
        tenant_isolation_rate=metric,
        replay_idempotency_rate=metric,
        unsupported_reason_counts=(
            {"unsupported lifecycle": unsupported_case_count}
            if unsupported_case_count
            else {}
        ),
        artifact_paths={"customer_lifecycle_evidence": path},
        component_digests={
            "evidence": _DIGEST,
            "registry": _DIGEST,
            "report": _DIGEST,
            "observations": _DIGEST,
        },
    )


def _ideal_variant_mechanism_metrics(
    *,
    observed_pair_count: int = 24,
    unsupported_case_count: int = 0,
    adaptive_target_candidate_authorization_rate: float = 1.0,
    frozen_target_candidate_exposure_rate: float = 0.0,
    candidate_memory_mediated_success_rate: float = 1.0,
    hard_safety_incident_count: int = 0,
    control_integrity_violation_count: int = 0,
) -> VariantAliasMechanismMetrics:
    runtime_support_rate = observed_pair_count / 24
    return VariantAliasMechanismMetrics(
        selected_case_count=24,
        observed_pair_count=observed_pair_count,
        unsupported_case_count=unsupported_case_count,
        full_registry_coverage_rate=1.0,
        observed_execution_rate=runtime_support_rate,
        adaptive_correctness_rate=1.0,
        frozen_correctness_rate=0.0,
        adaptive_minus_frozen_correctness=1.0,
        adaptive_target_candidate_authorization_rate=(
            adaptive_target_candidate_authorization_rate
        ),
        frozen_target_candidate_exposure_rate=(frozen_target_candidate_exposure_rate),
        candidate_authorization_gap=(
            adaptive_target_candidate_authorization_rate
            - frozen_target_candidate_exposure_rate
        ),
        adaptive_closed_set_match_rate=1.0,
        frozen_closed_set_match_rate=0.0,
        both_arms_one_llm_call_rate=1.0,
        both_arms_scripted_target_response_rate=1.0,
        frozen_safe_review_or_abstention_rate=1.0,
        source_immutability_rate=1.0,
        candidate_memory_mediated_success_rate=(candidate_memory_mediated_success_rate),
        adaptive_mean_llm_calls=1.0,
        frozen_mean_llm_calls=1.0,
        hard_safety_incident_count=hard_safety_incident_count,
        control_integrity_violation_count=(control_integrity_violation_count),
        entity_type_counts={
            "customer": 6,
            "project": 6,
            "system": 6,
            "team": 6,
        },
        variant_family_counts={
            "acronym_from_long_form": 4,
            "anchored_short_form": 4,
            "hyphen_spacing": 4,
            "orthographic_omission_subsequence": 4,
            "possessive_or_plural": 4,
            "punctuation_compact_form": 4,
        },
    )


def _variant_assurance(
    *,
    path: str = "/tmp/variant-population-evidence.json",
    status: str = "observed",
    observed_pair_count: int = 24,
    unsupported_case_count: int = 0,
    adaptive_unsafe_rate: float = 0.0,
    frozen_unsafe_rate: float = 0.0,
    mechanism_metrics: VariantAliasMechanismMetrics | None = None,
    component_digests: dict[str, str] | None = None,
) -> VariantPopulationAssurance:
    return VariantPopulationAssurance(
        status=status,
        evidence_tier=EvidenceTier.E4,
        registry_pair_count=24,
        observed_pair_count=observed_pair_count,
        unsupported_case_count=unsupported_case_count,
        runtime_support_rate=observed_pair_count / 24,
        adaptive_correctness=_interval(
            1.0,
            sample_size=observed_pair_count,
        ),
        frozen_correctness=_interval(
            0.0,
            sample_size=observed_pair_count,
        ),
        adaptive_minus_frozen_correctness=_interval(
            1.0,
            sample_size=observed_pair_count,
        ),
        adaptive_unsafe_rate=_interval(
            adaptive_unsafe_rate,
            sample_size=observed_pair_count,
        ),
        frozen_unsafe_rate=_interval(
            frozen_unsafe_rate,
            sample_size=observed_pair_count,
        ),
        mechanism_metrics=mechanism_metrics
        or _ideal_variant_mechanism_metrics(
            observed_pair_count=observed_pair_count,
            unsupported_case_count=unsupported_case_count,
        ),
        artifact_paths={"variant_population_evidence": path},
        component_digests=component_digests
        or {
            "evidence": _DIGEST,
            "registry": _DIGEST,
            "report": _DIGEST,
            "experiment_report": _DIGEST,
            "mechanism_metrics": _DIGEST,
        },
    )


def _variant_evidence(
    *,
    run_id: str = "pytest-assurance:variant",
    system_version: str = "pytest-system",
) -> CompanyLearningVariantPopulationEvidence:
    population = build_variant_alias_population()
    report = _variant_experiment_report(population).model_copy(
        update={
            "run_id": run_id,
            "system_version": system_version,
        }
    )
    observations = tuple(
        VariantAliasExecutionObservation(case_id=case.case_id)
        for case in population.cases
    )
    population_report = evaluate_variant_alias_population(
        population=population,
        experiment_report=report,
        observations=observations,
        bootstrap_samples=200,
    )
    assignments = _variant_assignments(population, report)
    return CompanyLearningVariantPopulationEvidence(
        created_at="2026-07-16T00:00:00+00:00",
        run_id=report.run_id,
        system_version=report.system_version,
        execution_mode="full",
        selection_policy="full_registry_once_no_selective_reruns",
        registry_path=str(VARIANT_FIXTURE),
        registry_population=population,
        registry_population_digest=population.digest,
        selected_case_ids=tuple(case.case_id for case in population.cases),
        assignments=assignments,
        observations=observations,
        raw_pairs=report.pairs,
        experiment_report=report,
        population_report=population_report,
        mechanism_pairs=_variant_mechanisms(report, assignments),
        mechanism_metrics=_variant_mechanism_metrics(population),
        artifact_refs=("pytest:variant-assurance-evidence",),
    )


def _variant_assurance_from_evidence(
    evidence: CompanyLearningVariantPopulationEvidence,
    *,
    path: str,
) -> VariantPopulationAssurance:
    report = evidence.population_report
    mechanism_metrics = evidence.mechanism_metrics
    assert report is not None
    assert mechanism_metrics is not None
    return VariantPopulationAssurance(
        status="observed",
        evidence_tier=EvidenceTier.E4,
        registry_pair_count=report.pair_count,
        observed_pair_count=report.observed_pair_count,
        unsupported_case_count=report.unsupported_case_count,
        runtime_support_rate=(report.observed_pair_count / report.pair_count),
        adaptive_correctness=report.adaptive_correctness,
        frozen_correctness=report.frozen_correctness,
        adaptive_minus_frozen_correctness=(report.adaptive_minus_frozen_correctness),
        adaptive_unsafe_rate=report.adaptive_unsafe_rate,
        frozen_unsafe_rate=report.frozen_unsafe_rate,
        mechanism_metrics=mechanism_metrics,
        artifact_paths={"variant_population_evidence": path},
        component_digests={
            "evidence": evidence.digest,
            "registry": evidence.registry_population_digest,
            "report": report.digest,
            "experiment_report": evidence.experiment_report.digest,
            "mechanism_metrics": canonical_sha256(
                mechanism_metrics.model_dump(mode="json")
            ),
        },
    )


def _correction_artifact(
    *,
    with_audit: bool = False,
) -> CorrectionAssuranceArtifact:
    audit = (
        CorrectionPropagationAudit(
            scope=CorrectionPropagationScope(
                tenant_id=uuid4(),
                predecessor_grounding_trace_id=uuid4(),
                run_id="pytest-assurance:correction",
                observed_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
            ),
            correction_grounding_trace_id=uuid4(),
            source_observation_id=uuid4(),
            correction_found=True,
            correction_changes_referent=True,
            discovered_dependency_count=0,
            component_counts={},
            repair_required_dependency_count=0,
            fenced_dependency_count=0,
            repaired_or_superseded_count=0,
            unsafe_readable_count=0,
            repair_pending_count=0,
            residual_repair_debt_count=0,
            convergence_ratio=1.0,
            safe_containment_ratio=1.0,
            source_hash_reference_count=1,
            source_hash_match_count=1,
            source_immutable=True,
            audit_read_only=True,
            cross_tenant_reference_count=0,
            cross_tenant_change_count=0,
            dependencies=(),
            incidents=(),
            uncertainty=(),
            artifact_refs=("pytest:correction-audit",),
        )
        if with_audit
        else None
    )
    return build_correction_assurance(
        run_id="pytest-assurance:correction",
        system_version="pytest-system",
        created_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
        runtime_evidence=CorrectionRuntimeEvidence(
            expected_dependency_refs=("model:old",),
            discovered_dependency_refs=("model:old",),
            expected_immediate_fence_refs=("model:old",),
            immediate_fence_refs=("model:old",),
            source_before_digest="d" * 64,
            source_after_digest="d" * 64,
            artifact_refs=("pytest:correction-runtime",),
        ),
        audit=audit,
        artifact_refs=("pytest:correction-assurance",),
    )


def _correction_summary(
    *,
    path: str = "/tmp/correction-assurance.json",
    artifact_digest: str | None = None,
    evidence_digest: str | None = None,
    artifact: CorrectionAssuranceArtifact | None = None,
    audit_digest: str | None = None,
) -> CorrectionAssurance:
    artifact = artifact or _correction_artifact()
    metrics = artifact.metrics
    component_digests = {
        "artifact": artifact_digest or artifact.digest,
        "evidence": (evidence_digest or artifact.component_digests["evidence"]),
    }
    if artifact.audit is not None:
        component_digests["audit"] = audit_digest or artifact.component_digests["audit"]
    return CorrectionAssurance(
        status=artifact.status,
        evidence_tier=EvidenceTier.E4,
        expected_dependency_count=metrics.expected_dependency_count,
        discovered_dependency_count=metrics.discovered_dependency_count,
        dependency_discovery_rate=metrics.dependency_discovery_rate,
        immediate_fence_rate=metrics.immediate_fence_rate,
        direct_repair_rate=metrics.direct_repair_rate,
        recursive_repair_rate=metrics.recursive_repair_rate,
        relation_retirement_rate=metrics.relation_retirement_rate,
        projection_invalidation_rate=metrics.projection_invalidation_rate,
        projection_rebuild_rate=metrics.projection_rebuild_rate,
        residual_unsafe_debt_count=metrics.residual_unsafe_debt_count,
        convergence_ratio=metrics.convergence_ratio,
        replay_idempotent=metrics.replay_idempotent,
        source_immutable=metrics.source_immutable,
        tenant_isolated=metrics.tenant_isolated,
        converged=metrics.converged,
        incidents=artifact.incidents,
        artifact_paths={"correction_evidence": path},
        component_digests=component_digests,
    )


def _slack_assurance(
    *,
    status: str = "observed",
    scope_complete: bool = True,
    open_world_complete: bool = False,
    blocking_for_active_slice: bool = True,
    evidence_tier: EvidenceTier = EvidenceTier.E4,
    supported_case_count: int = 9,
    correct_case_count: int = 9,
) -> SlackAssurance:
    return SlackAssurance(
        status=status,
        metrics={
            "case_count": 9,
            "supported_case_count": supported_case_count,
            "correct_case_count": correct_case_count,
            "contamination_rate": 0.0,
        },
        evidence_tier=evidence_tier,
        scope_complete=scope_complete,
        open_world_complete=open_world_complete,
        blocking_for_active_slice=blocking_for_active_slice,
        artifact_paths={
            "slack_observations": "/tmp/slack-observations.jsonl",
            "slack_report": "/tmp/slack-report.json",
        },
        component_digests={
            "report": _DIGEST,
            "gold_manifest": _DIGEST,
            "observations": _DIGEST,
        },
    )


def _active_surfaces_evidence(
    *,
    run_id: str = "pytest-assurance:active-surfaces",
    system_version: str = "pytest-system",
) -> ActiveLearningSurfacesEvidence:
    identity = tuple(
        StructuredIdentitySurfaceObservation(
            case_id=case_id,
            expected_claims=SEALED_ACTIVE_SURFACE_CLAIMS[case_id],
            observed_claims=SEALED_ACTIVE_SURFACE_CLAIMS[case_id],
            claim_emitted=True,
            claim_preserved=True,
            preexisting_binding_attached=True,
            handler_created_authority=False,
            ingest_created_authority=False,
            forged_text_resolved=False,
            missing_binding_authoritative=False,
            cross_source_leak=False,
            cross_tenant_leak=False,
            source_observation_immutable=True,
            artifact_refs=(f"pytest:{case_id}",),
        )
        for case_id in (
            "jira_project",
            "linear_issue_bundle",
            "google_drive_file",
            "google_drive_comment",
            "google_drive_revision",
            "gmail_thread",
        )
    )
    salience = tuple(
        SourceSalienceObservation(
            case_id=case_id,
            baseline_salience=baseline,
            learned_salience=learned,
            credit_observed=credit,
            foreign_tenant_learned=False,
            canonical_truth_immutable=True,
            grounding_truth_immutable=True,
            artifact_refs=(f"pytest:{case_id}",),
        )
        for case_id, (baseline, learned, credit) in {
            "settled_useful": (1.0, 2.0, True),
            "corrected": (2.0, 1.0, False),
            "pending": (1.0, 1.0, False),
            "foreign_tenant": (1.0, 1.0, False),
            "profile_load": (1.0, 1.0, False),
        }.items()
    )
    return ActiveLearningSurfacesEvidence(
        run_id=run_id,
        system_version=system_version,
        created_at="2026-07-16T00:00:00+00:00",
        identity_observations=identity,
        salience_observations=salience,
        report=evaluate_active_learning_surfaces(
            identity_observations=identity,
            salience_observations=salience,
        ),
        artifact_refs=("pytest:active-surfaces",),
    )


def _active_surfaces_assurance(
    *,
    path: str = "/tmp/active-surfaces.json",
    evidence: ActiveLearningSurfacesEvidence | None = None,
) -> ActiveSurfacesAssurance:
    evidence = evidence or _active_surfaces_evidence()
    return ActiveSurfacesAssurance(
        status=(
            "observed" if evidence.report.status == "observed" else "failed"
        ),
        evidence_tier=EvidenceTier.E4,
        structured_identity=evidence.report.structured_identity,
        source_salience=evidence.report.source_salience,
        artifact_paths={"active_surfaces_evidence": path},
        component_digests={
            "evidence": evidence.digest,
            "report": evidence.report.digest,
            "structured_identity_report": (
                evidence.report.structured_identity.digest
            ),
            "source_salience_report": evidence.report.source_salience.digest,
            "identity_observations": canonical_sha256(
                [
                    row.model_dump(mode="json")
                    for row in evidence.identity_observations
                ]
            ),
            "salience_observations": canonical_sha256(
                [
                    row.model_dump(mode="json")
                    for row in evidence.salience_observations
                ]
            ),
        },
    )


def _retention_evidence(
    *,
    run_id: str = "pytest-assurance:retention",
    system_version: str = "pytest-system",
) -> tuple[dict[str, object], CompanyLearningRetentionReport]:
    horizons = (
        RetentionHorizon(cycle_count=0, restart_count=0),
        RetentionHorizon(cycle_count=4, restart_count=1),
        RetentionHorizon(cycle_count=16, restart_count=2),
    )
    cases = (
        RetentionCaseSpec(
            case_id="retention-exact",
            behavior=RetentionBehavior.EXACT_ALIAS,
            family="exact_alias_positive",
            expected_ref=CanonicalEntityRef(type="customer", id="exact"),
            horizons=horizons,
            allowed_terminal_fates=(ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,),
        ),
        RetentionCaseSpec(
            case_id="retention-variant",
            behavior=RetentionBehavior.VARIANT_ALIAS,
            family="acronym_from_long_form",
            expected_ref=CanonicalEntityRef(type="customer", id="variant"),
            horizons=horizons,
            allowed_terminal_fates=(ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,),
            candidate_authorization_required=True,
        ),
        RetentionCaseSpec(
            case_id="retention-correction",
            behavior=RetentionBehavior.CORRECTED_ALIAS,
            family="authoritative_exact_correction",
            expected_ref=CanonicalEntityRef(type="customer", id="corrected"),
            horizons=(horizons[-1],),
            allowed_terminal_fates=(ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,),
            correction_authority_required=True,
        ),
        *(
            RetentionCaseSpec(
                case_id=f"retention-negative:{case_id}",
                behavior=RetentionBehavior.NEGATIVE_CONTROL,
                family=family,
                horizons=(horizons[-1],),
                allowed_terminal_fates=(ConsumerTerminalFate.REVIEW,),
            )
            for case_id, family in (
                ("contextual-non-entity", "contextual_phrase_negative"),
                ("unrelated-alias", "unrelated_negative_control"),
                ("same-surface-homonym", "homonym_local_association"),
                ("conflicting-source-hint", "conflicting_source_hint"),
            )
        ),
        *(
            RetentionCaseSpec(
                case_id=f"retention-collision:{case_id}",
                behavior=RetentionBehavior.COLLISION_CONTROL,
                family=family,
                horizons=(horizons[-1],),
                allowed_terminal_fates=(ConsumerTerminalFate.REVIEW,),
            )
            for case_id, family in (
                (
                    "heldout-variant-collision-00",
                    "same_type_acronym_collision",
                ),
                (
                    "heldout-variant-collision-06",
                    "punctuation_unicode_normalization_collision",
                ),
                (
                    "heldout-variant-collision-08",
                    "contextual_channel_local_nickname",
                ),
            )
        ),
    )
    spec = RetentionRunSpec(
        run_id=run_id,
        system_version=system_version,
        created_at="2026-07-16T00:00:00+00:00",
        cases=cases,
        artifact_refs=("pytest:retention-spec",),
    )
    observations = tuple(
        RetentionObservation(
            case_id=case.case_id,
            horizon=horizon,
            intervening_learning_count=horizon.cycle_count,
            consumer_fate=(
                ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
                if case.expected_ref is not None
                else ConsumerTerminalFate.REVIEW
            ),
            observed_ref=case.expected_ref,
            candidate_authorized=(
                True
                if case.behavior is RetentionBehavior.VARIANT_ALIAS
                else None
            ),
            correction_authoritative=(
                True
                if case.behavior is RetentionBehavior.CORRECTED_ALIAS
                else None
            ),
            source_observation_immutable=True,
            models_consistent=True,
            evidence_lineage_consistent=True,
            artifact_refs=(f"pytest:retention:{case.case_id}",),
        )
        for case in cases
        for horizon in case.horizons
    )
    report = evaluate_company_learning_retention(
        spec=spec,
        observations=observations,
        artifact_refs=("pytest:retention-report",),
    )
    payload: dict[str, object] = {
        "spec": spec.model_dump(mode="json"),
        "observations": [
            row.model_dump(mode="json") for row in observations
        ],
        "report": report.model_dump(mode="json"),
        "report_digest": report.digest,
    }
    return payload, report


def _retention_assurance(
    *,
    path: str = "/tmp/retention.json",
    payload: dict[str, object] | None = None,
    report: CompanyLearningRetentionReport | None = None,
) -> RetentionAssurance:
    if payload is None or report is None:
        payload, report = _retention_evidence()
    return RetentionAssurance(
        status="observed",
        evidence_tier=EvidenceTier.E4,
        expected_observation_count=report.expected_observation_count,
        observed_observation_count=report.observed_observation_count,
        exact_retention_rate=report.exact_retention_rate,
        variant_retention_rate=report.variant_retention_rate,
        corrected_retention_rate=report.corrected_retention_rate,
        overall_positive_retention_rate=report.overall_positive_retention_rate,
        overall_forgetting_rate=report.overall_forgetting_rate,
        restart_survival_rate=report.restart_survival_rate,
        correction_authority_rate=report.correction_authority_rate,
        unsafe_globalization_rate=report.unsafe_globalization_rate,
        negative_control_safety_rate=report.negative_control_safety_rate,
        collision_control_safety_rate=report.collision_control_safety_rate,
        source_immutability_rate=report.source_immutability_rate,
        model_consistency_rate=report.model_consistency_rate,
        evidence_lineage_consistency_rate=(
            report.evidence_lineage_consistency_rate
        ),
        hard_safety_incident_rate=report.hard_safety_incident_rate,
        retention_horizon_auc=report.retention_horizon_auc,
        horizon_metrics=report.horizon_metrics,
        family_counts=report.family_counts,
        artifact_paths={"retention_evidence": path},
        component_digests={
            "artifact": canonical_sha256(payload),
            "spec": report.spec_digest,
            "report": report.digest,
            "observations": report.observation_digest,
        },
    )


def _canonical_replacement_evidence(
    *,
    run_id: str = "pytest-assurance:canonical-replacement",
    system_version: str = "pytest-system",
    observation=None,
) -> CanonicalResourceReplacementEvidence:
    observation = observation or _replacement_observation()
    return CanonicalResourceReplacementEvidence(
        run_id=run_id,
        system_version=system_version,
        created_at="2026-07-17T00:00:00+00:00",
        observation=observation,
        database_evidence=CanonicalReplacementDatabaseEvidence(
            query_manifest={"pytest_snapshot": "SELECT 1"},
            snapshots={"pytest_snapshot": {"observed": True}},
            measurement_evidence={
                name: ("pytest_snapshot",)
                for name in observation.measurements
            },
        ),
        report=evaluate_canonical_resource_replacement(observation),
        artifact_refs=("pytest:canonical-replacement",),
    )


def _canonical_replacement_assurance(
    *,
    path: str = "/tmp/canonical-replacement.json",
    evidence: CanonicalResourceReplacementEvidence | None = None,
) -> CanonicalReplacementAssurance:
    evidence = evidence or _canonical_replacement_evidence()
    report = evidence.report
    return CanonicalReplacementAssurance(
        status=(
            "failed"
            if report.violating_measurement_count
            else "observed"
            if report.full_scope_complete
            else "observed_with_gaps"
        ),
        evidence_tier=EvidenceTier.E4,
        report=report,
        artifact_paths={"canonical_replacement_evidence": path},
        component_digests={
            "evidence": evidence.digest,
            "report": report.digest,
            "observation": canonical_sha256(
                evidence.observation.model_dump(mode="json")
            ),
        },
    )


def _source_binding_lifecycle_evidence(
    *,
    run_id: str = "pytest-assurance:source-binding-lifecycle",
    system_version: str = "pytest-system",
    observation=None,
) -> SourceIdentityBindingLifecycleEvidence:
    observation = observation or _binding_lifecycle_observation()
    return SourceIdentityBindingLifecycleEvidence(
        run_id=run_id,
        system_version=system_version,
        created_at="2026-07-17T00:00:00+00:00",
        observation=observation,
        report=evaluate_source_identity_binding_lifecycle(observation),
        artifact_refs=("pytest:source-binding-lifecycle",),
    )


def _source_binding_lifecycle_assurance(
    *,
    path: str = "/tmp/source-binding-lifecycle.json",
    evidence: SourceIdentityBindingLifecycleEvidence | None = None,
) -> SourceBindingLifecycleAssurance:
    evidence = evidence or _source_binding_lifecycle_evidence()
    report = evidence.report
    return SourceBindingLifecycleAssurance(
        status=(
            "failed"
            if report.violating_measurement_count
            else "observed"
            if report.full_scope_complete
            else "observed_with_gaps"
        ),
        evidence_tier=EvidenceTier.E4,
        report=report,
        artifact_paths={"source_binding_lifecycle_evidence": path},
        component_digests={
            "evidence": evidence.digest,
            "report": report.digest,
            "observation": canonical_sha256(
                evidence.observation.model_dump(mode="json")
            ),
        },
    )


def _summary(
    *,
    slack: SlackAssurance | None = None,
    correction: CorrectionAssurance | None = None,
    variant_population: VariantPopulationAssurance | None = None,
    variant_collision: VariantCollisionAssurance | None = None,
    customer_lifecycle: CustomerLifecycleAssurance | None = None,
    active_surfaces: ActiveSurfacesAssurance | None = None,
    retention: RetentionAssurance | None = None,
    canonical_replacement: CanonicalReplacementAssurance | None = None,
    source_binding_lifecycle: SourceBindingLifecycleAssurance | None = None,
    status: str = "working",
    blocking_failures: tuple[str, ...] = (),
    architecture_digest: str = _ARCHITECTURE_DIGEST,
    implementation_plan_digest: str = _IMPLEMENTATION_PLAN_DIGEST,
    excluded_capabilities: tuple[str, ...] = (
        "autonomous_task_planning",
        "autonomous_task_execution",
    ),
) -> CompanyLearningAssuranceSummary:
    positive = PositiveAssurance(
        status="observed",
        pair_count=3,
        adaptive_correctness_rate=1.0,
        frozen_correctness_rate=0.0,
        adaptive_minus_frozen_correctness=1.0,
        hard_failures=(),
        artifact_paths={
            "positive_pair": "/tmp/positive-pair.json",
            "positive_company_learning_evaluation": ("/tmp/positive-evaluation.json"),
            "positive_company_learning_evidence_bundle": ("/tmp/positive-bundle.json"),
        },
        component_digests={
            "report": _DIGEST,
            "company_learning_evaluation": _DIGEST,
            "company_learning_evidence_bundle": _DIGEST,
        },
    )
    negative = NegativeAssurance(
        status="observed",
        pair_count=4,
        safety_incident_count=0,
        adaptive_unsafe_count=0,
        frozen_unsafe_count=0,
        artifact_paths={"negative_evidence": "/tmp/negative.json"},
        component_digests={
            "evidence": _DIGEST,
            "report": _DIGEST,
            "plan": _DIGEST,
        },
    )
    population = PopulationAssurance(
        status="observed",
        registry_pair_count=60,
        observed_pair_count=60,
        unsupported_case_count=0,
        runtime_support_rate=1.0,
        metrics={
            "pair_count": 60,
            "observed_pair_count": 60,
            "unsupported_case_count": 0,
            "complete_population": True,
        },
        unsupported_strata_counts={"entity_type": {}},
        unsupported_reason_counts={},
        artifact_paths={"population_evidence": "/tmp/population.json"},
        component_digests={
            "evidence": _DIGEST,
            "registry": _DIGEST,
            "report": _DIGEST,
        },
    )
    slack = slack or _slack_assurance()
    correction = correction or _correction_summary()
    variant_population = variant_population or _variant_assurance()
    variant_collision = variant_collision or _collision_assurance()
    customer_lifecycle = customer_lifecycle or _lifecycle_assurance()
    active_surfaces = active_surfaces or _active_surfaces_assurance()
    retention = retention or _retention_assurance()
    canonical_replacement = (
        canonical_replacement or _canonical_replacement_assurance()
    )
    source_binding_lifecycle = (
        source_binding_lifecycle or _source_binding_lifecycle_assurance()
    )
    artifact_paths = {
        **positive.artifact_paths,
        **negative.artifact_paths,
        **slack.artifact_paths,
        **correction.artifact_paths,
        **variant_population.artifact_paths,
        **variant_collision.artifact_paths,
        **customer_lifecycle.artifact_paths,
        **active_surfaces.artifact_paths,
        **retention.artifact_paths,
        **canonical_replacement.artifact_paths,
        **source_binding_lifecycle.artifact_paths,
        **population.artifact_paths,
    }
    component_digests = {
        **{
            f"positive_{key}": value
            for key, value in positive.component_digests.items()
        },
        **{
            f"negative_{key}": value
            for key, value in negative.component_digests.items()
        },
        **{f"slack_{key}": value for key, value in slack.component_digests.items()},
        **{
            f"correction_{key}": value
            for key, value in correction.component_digests.items()
        },
        **{
            f"variant_population_{key}": value
            for key, value in variant_population.component_digests.items()
        },
        **{
            f"variant_collision_{key}": value
            for key, value in variant_collision.component_digests.items()
        },
        **{
            f"customer_lifecycle_{key}": value
            for key, value in customer_lifecycle.component_digests.items()
        },
        **{
            f"active_surfaces_{key}": value
            for key, value in active_surfaces.component_digests.items()
        },
        **{
            f"retention_{key}": value
            for key, value in retention.component_digests.items()
        },
        **{
            f"canonical_replacement_{key}": value
            for key, value in canonical_replacement.component_digests.items()
        },
        **{
            f"source_binding_lifecycle_{key}": value
            for key, value in source_binding_lifecycle.component_digests.items()
        },
        **{
            f"population_{key}": value
            for key, value in population.component_digests.items()
        },
    }
    return CompanyLearningAssuranceSummary(
        run_id="pytest-assurance",
        system_version="pytest-system",
        repository_provenance=capture_repository_provenance(
            Path(__file__).resolve().parents[2]
        ),
        architecture_digest=architecture_digest,
        implementation_plan_digest=implementation_plan_digest,
        excluded_capabilities=excluded_capabilities,
        created_at="2026-07-16T00:00:00+00:00",
        status=status,
        positive=positive,
        negative=negative,
        slack=slack,
        correction=correction,
        variant_population=variant_population,
        variant_collision=variant_collision,
        customer_lifecycle=customer_lifecycle,
        active_surfaces=active_surfaces,
        retention=retention,
        canonical_replacement=canonical_replacement,
        source_binding_lifecycle=source_binding_lifecycle,
        population=population,
        proof_gaps=("not open-world or task-autonomy proof",),
        blocking_failures=blocking_failures,
        component_digests=component_digests,
        artifact_paths=artifact_paths,
    )


def test_summary_v7_binds_reviewed_identity_and_active_scope() -> None:
    summary = _summary()

    assert summary.schema_version == "company-learning-assurance-summary-v7"
    assert summary.architecture_digest == _ARCHITECTURE_DIGEST
    assert summary.implementation_plan_digest == _IMPLEMENTATION_PLAN_DIGEST
    assert summary.evaluation_profile == "autonomous-company-learning-v1"
    assert summary.excluded_capabilities == (
        "autonomous_task_planning",
        "autonomous_task_execution",
    )
    assert (
        validate_company_learning_assurance_artifact(summary.artifact_payload())
        == summary
    )

    with pytest.raises(ValidationError, match="architecture_digest"):
        _summary(architecture_digest="not-a-digest")
    with pytest.raises(ValidationError, match="implementation_plan_digest"):
        _summary(implementation_plan_digest="not-a-digest")
    with pytest.raises(ValidationError, match="explicitly exclude"):
        _summary(excluded_capabilities=("autonomous_task_planning",))


def test_summary_v7_rejects_resealed_repository_provenance_mismatch() -> None:
    summary = _summary()
    payload = summary.artifact_payload()
    payload["repository_provenance"]["worktree_digest"] = "0" * 64
    payload["summary_digest"] = canonical_sha256(
        {
            key: value
            for key, value in payload.items()
            if key != "summary_digest"
        }
    )

    with pytest.raises(ValueError, match="worktree digest"):
        validate_company_learning_assurance_artifact(payload)


@pytest.mark.parametrize(
    ("assurance_factory", "component"),
    (
        (_canonical_replacement_assurance, "canonical replacement"),
        (_source_binding_lifecycle_assurance, "source binding lifecycle"),
    ),
)
def test_lifecycle_assurance_rejects_tiers_above_recognized_contract(
    assurance_factory,
    component: str,
) -> None:
    assurance = assurance_factory()
    payload = assurance.model_dump(mode="json")
    payload["evidence_tier"] = EvidenceTier.E5.value

    with pytest.raises(ValidationError, match=f"{component} evidence contract"):
        type(assurance).model_validate(payload)


def test_lifecycle_assurance_rejects_unknown_stronger_contract() -> None:
    assurance = _canonical_replacement_assurance()
    payload = assurance.model_dump(mode="json")
    payload["evidence_tier"] = EvidenceTier.E5.value
    payload["evidence_contract"] = (
        "canonical-resource-replacement-evidence-v2"
    )

    with pytest.raises(ValidationError, match="unrecognized evidence contract"):
        CanonicalReplacementAssurance.model_validate(payload)


def test_replacement_unsupported_and_unsafe_evidence_block_working() -> None:
    unsupported_observation = _replacement_observation().model_copy(
        update={
            "projection_invalidated": ReplacementProofCell(
                status="unsupported",
                unsupported_reason="projection invalidation evidence unavailable",
            )
        }
    )
    unsupported_evidence = _canonical_replacement_evidence(
        observation=unsupported_observation
    )
    unsafe_observation = _replacement_observation().model_copy(
        update={
            "transaction_atomic": ReplacementProofCell(
                status="observed",
                satisfied=False,
                artifact_refs=("pytest:non-atomic-replacement",),
            )
        }
    )
    unsafe_evidence = _canonical_replacement_evidence(
        observation=unsafe_observation
    )

    with pytest.raises(ValidationError, match="working assurance"):
        _summary(
            canonical_replacement=_canonical_replacement_assurance(
                evidence=unsupported_evidence
            )
        )
    with pytest.raises(ValidationError, match="working assurance"):
        _summary(
            canonical_replacement=_canonical_replacement_assurance(
                evidence=unsafe_evidence
            )
        )


def test_binding_lifecycle_unsupported_and_immutability_evidence_block_working() -> None:
    unsupported_observation = _binding_lifecycle_observation().model_copy(
        update={
            "revocation_correct": BindingLifecycleProofCell(
                status="unsupported",
                unsupported_reason="revocation evidence unavailable",
            )
        }
    )
    unsupported_evidence = _source_binding_lifecycle_evidence(
        observation=unsupported_observation
    )
    immutable_failure = _binding_lifecycle_observation().model_copy(
        update={
            "source_immutable": BindingLifecycleProofCell(
                status="observed",
                satisfied=False,
                artifact_refs=("pytest:source-mutated",),
            )
        }
    )
    unsafe_evidence = _source_binding_lifecycle_evidence(
        observation=immutable_failure
    )

    with pytest.raises(ValidationError, match="working assurance"):
        _summary(
            source_binding_lifecycle=_source_binding_lifecycle_assurance(
                evidence=unsupported_evidence
            )
        )
    with pytest.raises(ValidationError, match="working assurance"):
        _summary(
            source_binding_lifecycle=_source_binding_lifecycle_assurance(
                evidence=unsafe_evidence
            )
        )


def test_canonical_replacement_component_reopens_raw_evidence(
    tmp_path: Path,
) -> None:
    evidence = _canonical_replacement_evidence()
    artifact_path = tmp_path / "canonical-replacement.json"
    artifact_path.write_text(
        json.dumps(evidence.artifact_payload()),
        encoding="utf-8",
    )
    assurance = _canonical_replacement_assurance(
        path=str(artifact_path),
        evidence=evidence,
    )

    assert (
        validate_canonical_replacement_assurance_component(
            assurance,
            run_id=evidence.run_id,
            system_version=evidence.system_version,
        )
        == evidence.report
    )
    payload = evidence.artifact_payload()
    payload["observation"]["transaction_atomic"]["satisfied"] = False
    artifact_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="report does not match"):
        validate_canonical_replacement_assurance_component(
            assurance,
            run_id=evidence.run_id,
            system_version=evidence.system_version,
        )


def test_source_binding_lifecycle_component_reopens_raw_evidence(
    tmp_path: Path,
) -> None:
    evidence = _source_binding_lifecycle_evidence()
    artifact_path = tmp_path / "source-binding-lifecycle.json"
    artifact_path.write_text(
        json.dumps(evidence.artifact_payload()),
        encoding="utf-8",
    )
    assurance = _source_binding_lifecycle_assurance(
        path=str(artifact_path),
        evidence=evidence,
    )

    assert (
        validate_source_binding_lifecycle_assurance_component(
            assurance,
            run_id=evidence.run_id,
            system_version=evidence.system_version,
        )
        == evidence.report
    )
    payload = evidence.artifact_payload()
    payload["observation"]["source_immutable"]["satisfied"] = False
    artifact_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="report does not match"):
        validate_source_binding_lifecycle_assurance_component(
            assurance,
            run_id=evidence.run_id,
            system_version=evidence.system_version,
        )


def test_active_surfaces_are_noncompensatory_for_working_status() -> None:
    evidence = _active_surfaces_evidence()
    identity = list(evidence.identity_observations)
    identity[0] = identity[0].model_copy(
        update={"forged_text_resolved": True}
    )
    contradicted = evidence.model_copy(
        update={
            "identity_observations": tuple(identity),
            "report": evaluate_active_learning_surfaces(
                identity_observations=tuple(identity),
                salience_observations=evidence.salience_observations,
            ),
        }
    )
    failed = _active_surfaces_assurance(evidence=contradicted)

    with pytest.raises(ValidationError, match="working assurance"):
        _summary(active_surfaces=failed)


@pytest.mark.parametrize(
    ("updates", "status"),
    (
        (
            {
                "exact_retention_rate": 0.9,
                "overall_positive_retention_rate": 0.95,
                "overall_forgetting_rate": 0.05,
                "retention_horizon_auc": 0.95,
            },
            "observed_with_degradation",
        ),
        ({"source_immutability_rate": 0.9}, "failed"),
        ({"hard_safety_incident_rate": 0.1}, "failed"),
    ),
)
def test_retention_regressions_are_noncompensatory_for_working_status(
    updates: dict[str, float],
    status: str,
) -> None:
    baseline = _retention_assurance()
    degraded = RetentionAssurance.model_validate(
        {
            **baseline.model_dump(mode="json"),
            **updates,
            "status": status,
        }
    )

    with pytest.raises(ValidationError, match="working assurance"):
        _summary(retention=degraded)


def test_active_surface_component_reopens_raw_evidence(
    tmp_path: Path,
) -> None:
    evidence = _active_surfaces_evidence()
    artifact_path = tmp_path / "active-surfaces.json"
    artifact_path.write_text(
        json.dumps(evidence.artifact_payload()),
        encoding="utf-8",
    )
    assurance = _active_surfaces_assurance(
        path=str(artifact_path),
        evidence=evidence,
    )

    assert (
        validate_active_surfaces_assurance_component(
            assurance,
            run_id=evidence.run_id,
            system_version=evidence.system_version,
        )
        == evidence.report
    )
    payload = evidence.artifact_payload()
    payload["identity_observations"][0]["forged_text_resolved"] = True
    artifact_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="report does not match"):
        validate_active_surfaces_assurance_component(
            assurance,
            run_id=evidence.run_id,
            system_version=evidence.system_version,
        )


def test_active_surface_component_binds_sealed_source_contracts(
    tmp_path: Path,
) -> None:
    evidence = _active_surfaces_evidence()
    identity = list(evidence.identity_observations)
    original = identity[0]
    changed_claim = original.expected_claims[0].model_copy(
        update={"source_native_identifier": "jira:wrong:project:10000"}
    )
    identity[0] = original.model_copy(
        update={
            "expected_claims": (changed_claim,),
            "observed_claims": (changed_claim,),
        }
    )
    changed = ActiveLearningSurfacesEvidence(
        run_id=evidence.run_id,
        system_version=evidence.system_version,
        created_at=evidence.created_at,
        identity_observations=tuple(identity),
        salience_observations=evidence.salience_observations,
        report=evaluate_active_learning_surfaces(
            identity_observations=tuple(identity),
            salience_observations=evidence.salience_observations,
        ),
        artifact_refs=evidence.artifact_refs,
    )
    artifact_path = tmp_path / "changed-active-surfaces.json"
    artifact_path.write_text(
        json.dumps(changed.artifact_payload()),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="expected claim contract"):
        validate_active_surfaces_assurance_component(
            _active_surfaces_assurance(
                path=str(artifact_path),
                evidence=changed,
            ),
            run_id=changed.run_id,
            system_version=changed.system_version,
        )


def test_retention_component_recomputes_raw_observations(
    tmp_path: Path,
) -> None:
    payload, report = _retention_evidence()
    artifact_path = tmp_path / "retention.json"
    artifact_path.write_text(json.dumps(payload), encoding="utf-8")
    assurance = _retention_assurance(
        path=str(artifact_path),
        payload=payload,
        report=report,
    )

    assert (
        validate_retention_assurance_component(
            assurance,
            run_id="pytest-assurance:retention",
            system_version="pytest-system",
        )
        == report
    )
    payload["observations"][0]["observed_ref"] = None
    artifact_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="raw recomputation"):
        validate_retention_assurance_component(
            assurance,
            run_id="pytest-assurance:retention",
            system_version="pytest-system",
        )


def test_retention_component_rejects_reduced_green_scope(
    tmp_path: Path,
) -> None:
    payload, _ = _retention_evidence()
    full_spec = RetentionRunSpec.model_validate(payload["spec"])
    selected_ids = {
        "retention-exact",
        "retention-variant",
        "retention-correction",
        "retention-negative:contextual-non-entity",
        "retention-collision:heldout-variant-collision-00",
    }
    reduced_spec = full_spec.model_copy(
        update={
            "cases": tuple(
                case for case in full_spec.cases if case.case_id in selected_ids
            )
        }
    )
    reduced_observations = tuple(
        RetentionObservation.model_validate(row)
        for row in payload["observations"]
        if row["case_id"] in selected_ids
    )
    reduced_report = evaluate_company_learning_retention(
        spec=reduced_spec,
        observations=reduced_observations,
        artifact_refs=("pytest:reduced-retention",),
    )
    reduced_payload = {
        "spec": reduced_spec.model_dump(mode="json"),
        "observations": [
            row.model_dump(mode="json") for row in reduced_observations
        ],
        "report": reduced_report.model_dump(mode="json"),
        "report_digest": reduced_report.digest,
    }
    artifact_path = tmp_path / "reduced-retention.json"
    artifact_path.write_text(json.dumps(reduced_payload), encoding="utf-8")
    baseline = _retention_assurance()
    reduced_assurance = RetentionAssurance.model_validate(
        {
            **baseline.model_dump(mode="json"),
            "status": "observed_with_degradation",
            "expected_observation_count": (
                reduced_report.expected_observation_count
            ),
            "observed_observation_count": (
                reduced_report.observed_observation_count
            ),
            "horizon_metrics": [
                row.model_dump(mode="json")
                for row in reduced_report.horizon_metrics
            ],
            "family_counts": reduced_report.family_counts,
            "artifact_paths": {"retention_evidence": str(artifact_path)},
            "component_digests": {
                "artifact": canonical_sha256(reduced_payload),
                "spec": reduced_spec.digest,
                "report": reduced_report.digest,
                "observations": reduced_report.observation_digest,
            },
        }
    )

    with pytest.raises(ValueError, match="sealed scope"):
        validate_retention_assurance_component(
            reduced_assurance,
            run_id=reduced_spec.run_id,
            system_version=reduced_spec.system_version,
        )


def test_slack_proof_semantics_are_explicit_and_noncompensatory() -> None:
    with pytest.raises(ValidationError, match="at least E5"):
        _slack_assurance(
            open_world_complete=True,
            evidence_tier=EvidenceTier.E4,
        )

    incomplete = _slack_assurance(
        status="observed_with_gaps",
        scope_complete=False,
        supported_case_count=8,
        correct_case_count=8,
    )
    with pytest.raises(ValidationError, match="working assurance"):
        _summary(slack=incomplete)

    failed = _summary(
        slack=incomplete,
        status="failed",
        blocking_failures=("Slack active slice is incomplete.",),
    )
    assert failed.status == "failed"
    assert failed.slack.blocking_for_active_slice is True

    old_payload = {
        "status": "observed",
        "metrics": {},
        "diagnostic_only": True,
        "artifact_paths": {"slack_report": "/tmp/slack.json"},
        "component_digests": {"report": _DIGEST},
    }
    with pytest.raises(ValidationError):
        SlackAssurance.model_validate(old_payload)


@pytest.mark.parametrize(
    "variant_population",
    (
        _variant_assurance(
            status="observed_with_gaps",
            observed_pair_count=23,
            unsupported_case_count=1,
        ),
        _variant_assurance(
            status="failed",
            mechanism_metrics=_ideal_variant_mechanism_metrics(
                candidate_memory_mediated_success_rate=0.99,
                control_integrity_violation_count=1,
            ),
        ),
        _variant_assurance(
            status="failed",
            mechanism_metrics=_ideal_variant_mechanism_metrics(
                adaptive_target_candidate_authorization_rate=0.99,
                control_integrity_violation_count=1,
            ),
        ),
        _variant_assurance(
            status="failed",
            mechanism_metrics=_ideal_variant_mechanism_metrics(
                frozen_target_candidate_exposure_rate=0.01,
                control_integrity_violation_count=1,
            ),
        ),
        _variant_assurance(
            status="failed",
            adaptive_unsafe_rate=1 / 24,
            mechanism_metrics=_ideal_variant_mechanism_metrics(
                hard_safety_incident_count=1,
            ),
        ),
        _variant_assurance(
            status="failed",
            mechanism_metrics=_ideal_variant_mechanism_metrics(
                control_integrity_violation_count=1,
            ),
        ),
    ),
    ids=(
        "incomplete-sealed-coverage",
        "candidate-memory-mediation",
        "adaptive-authorization",
        "frozen-exposure",
        "safety-incident",
        "control-integrity",
    ),
)
def test_variant_population_is_noncompensatory_for_working_status(
    variant_population: VariantPopulationAssurance,
) -> None:
    with pytest.raises(ValidationError, match="working assurance"):
        _summary(variant_population=variant_population)

    failed = _summary(
        variant_population=variant_population,
        status="failed",
        blocking_failures=("variant population requirement failed",),
    )
    assert failed.variant_population == variant_population


def test_variant_component_reopens_full_evidence_and_all_digests(
    tmp_path: Path,
) -> None:
    evidence = _variant_evidence()
    artifact_path = tmp_path / "variant_population_evidence.json"
    artifact_path.write_text(
        json.dumps(evidence.artifact_payload(), sort_keys=True),
        encoding="utf-8",
    )
    assurance = _variant_assurance_from_evidence(
        evidence,
        path=str(artifact_path),
    )

    assert (
        validate_variant_population_assurance_component(
            assurance,
            run_id="pytest-assurance:variant",
            system_version="pytest-system",
        )
        == evidence
    )

    wrong_digest = assurance.model_copy(
        update={
            "component_digests": {
                **assurance.component_digests,
                "report": "f" * 64,
            }
        }
    )
    with pytest.raises(ValueError, match="component digest mismatch"):
        validate_variant_population_assurance_component(
            wrong_digest,
            run_id="pytest-assurance:variant",
            system_version="pytest-system",
        )
    with pytest.raises(ValueError, match="run identity mismatch"):
        validate_variant_population_assurance_component(
            assurance,
            run_id="another-run:variant",
            system_version="pytest-system",
        )


def test_collision_full_source_surface_scope_is_mandatory() -> None:
    summary = _summary()

    assert summary.status == "working"
    assert summary.variant_collision.status == "observed"
    assert summary.variant_collision.supported_scope_satisfied is True
    assert summary.variant_collision.full_scope_complete is True
    assert summary.variant_collision.observed_pair_count == 16
    assert summary.variant_collision.unsupported_case_count == 0

    incomplete = _collision_assurance(
        status="observed_with_gaps",
        observed_pair_count=14,
        unsupported_case_count=2,
    )
    assert incomplete.full_scope_complete is False
    with pytest.raises(ValidationError, match="working assurance"):
        _summary(variant_collision=incomplete)

    review_only_source_scope = _collision_assurance(
        status="failed",
        observed_pair_count=16,
        unsupported_case_count=0,
        source_native_authoritative_rate=0.0,
    )
    assert review_only_source_scope.full_scope_complete is False
    assert review_only_source_scope.source_native_scope_valid is False
    with pytest.raises(ValidationError, match="working assurance"):
        _summary(variant_collision=review_only_source_scope)


@pytest.mark.parametrize(
    "customer_lifecycle",
    (
        _lifecycle_assurance(metric_value=0.875, violating_case_count=1),
        _lifecycle_assurance(unsupported_case_count=1),
    ),
    ids=("continuous-metric-regression", "unsupported-case"),
)
def test_customer_lifecycle_is_noncompensatory_for_working_status(
    customer_lifecycle: CustomerLifecycleAssurance,
) -> None:
    with pytest.raises(ValidationError, match="working assurance"):
        _summary(customer_lifecycle=customer_lifecycle)

    failed = _summary(
        customer_lifecycle=customer_lifecycle,
        status="failed",
        blocking_failures=("customer lifecycle proof failed",),
    )
    assert failed.customer_lifecycle == customer_lifecycle


def test_customer_lifecycle_component_reopens_and_recomputes_evidence(
    tmp_path: Path,
) -> None:
    evidence = _lifecycle_evidence()
    artifact_path = tmp_path / "customer_lifecycle_evidence.json"
    artifact_path.write_text(
        json.dumps(evidence.artifact_payload(), sort_keys=True),
        encoding="utf-8",
    )
    assurance = _lifecycle_assurance_from_evidence(
        evidence,
        path=str(artifact_path),
    )

    assert (
        validate_customer_lifecycle_assurance_component(
            assurance,
            run_id="pytest-assurance:customer-lifecycle",
            system_version="pytest-system",
        )
        == evidence.report
    )

    wrong_digest = assurance.model_copy(
        update={
            "component_digests": {
                **assurance.component_digests,
                "observations": "f" * 64,
            }
        }
    )
    with pytest.raises(ValueError, match="component digest mismatch"):
        validate_customer_lifecycle_assurance_component(
            wrong_digest,
            run_id="pytest-assurance:customer-lifecycle",
            system_version="pytest-system",
        )


def test_collision_unsafe_supported_evidence_is_noncompensatory() -> None:
    unsafe = _collision_assurance(
        status="failed",
        adaptive_safe_containment_rate=13 / 14,
        adaptive_unsafe_rate=1 / 14,
        safety_incident_count=1,
    )

    with pytest.raises(ValidationError, match="working assurance"):
        _summary(variant_collision=unsafe)

    failed = _summary(
        variant_collision=unsafe,
        status="failed",
        blocking_failures=("collision supported scope is unsafe",),
    )
    assert failed.variant_collision == unsafe


def test_collision_component_reopens_and_recomputes_evidence(
    tmp_path: Path,
) -> None:
    evidence = _collision_evidence()
    artifact_path = tmp_path / "variant_collision_evidence.json"
    artifact_path.write_text(
        json.dumps(evidence.artifact_payload(), sort_keys=True),
        encoding="utf-8",
    )
    assurance = _collision_assurance_from_evidence(
        evidence,
        path=str(artifact_path),
    )

    assert (
        validate_variant_collision_assurance_component(
            assurance,
            run_id="pytest-assurance:collision",
            system_version="pytest-system",
        )
        == evidence.report
    )

    wrong_digest = assurance.model_copy(
        update={
            "component_digests": {
                **assurance.component_digests,
                "observations": "f" * 64,
            }
        }
    )
    with pytest.raises(ValueError, match="component digest mismatch"):
        validate_variant_collision_assurance_component(
            wrong_digest,
            run_id="pytest-assurance:collision",
            system_version="pytest-system",
        )


def test_correction_component_reopens_runtime_artifact_and_digest(
    tmp_path: Path,
) -> None:
    artifact = _correction_artifact()
    artifact_path = tmp_path / "correction_assurance.json"
    artifact_path.write_text(
        json.dumps(artifact.artifact_payload(), sort_keys=True),
        encoding="utf-8",
    )
    assurance = _correction_summary(
        path=str(artifact_path),
        artifact_digest=artifact.digest,
    )

    validated = validate_correction_assurance_component(
        assurance,
        run_id="pytest-assurance:correction",
        system_version="pytest-system",
    )
    assert validated == artifact

    payload = artifact.artifact_payload()
    payload["metrics"]["discovered_dependency_count"] = 0
    artifact_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="artifact digest mismatch"):
        validate_correction_assurance_component(
            assurance,
            run_id="pytest-assurance:correction",
            system_version="pytest-system",
        )


def test_correction_component_rejects_summary_digest_or_identity_mismatch(
    tmp_path: Path,
) -> None:
    artifact = _correction_artifact()
    artifact_path = tmp_path / "correction_assurance.json"
    artifact_path.write_text(
        json.dumps(artifact.artifact_payload(), sort_keys=True),
        encoding="utf-8",
    )
    wrong_digest = _correction_summary(
        path=str(artifact_path),
        artifact_digest="e" * 64,
    )

    with pytest.raises(ValueError, match="artifact digest mismatch"):
        validate_correction_assurance_component(
            wrong_digest,
            run_id="pytest-assurance:correction",
            system_version="pytest-system",
        )
    with pytest.raises(ValueError, match="run identity mismatch"):
        validate_correction_assurance_component(
            _correction_summary(path=str(artifact_path)),
            run_id="another-run:correction",
            system_version="pytest-system",
        )


def test_correction_component_validates_optional_audit_digest(
    tmp_path: Path,
) -> None:
    artifact = _correction_artifact(with_audit=True)
    artifact_path = tmp_path / "correction_assurance.json"
    artifact_path.write_text(
        json.dumps(artifact.artifact_payload(), sort_keys=True),
        encoding="utf-8",
    )
    assurance = _correction_summary(
        path=str(artifact_path),
        artifact=artifact,
    )

    assert (
        validate_correction_assurance_component(
            assurance,
            run_id="pytest-assurance:correction",
            system_version="pytest-system",
        )
        == artifact
    )

    with pytest.raises(ValueError, match="audit digest mismatch"):
        validate_correction_assurance_component(
            _correction_summary(
                path=str(artifact_path),
                artifact=artifact,
                audit_digest="f" * 64,
            ),
            run_id="pytest-assurance:correction",
            system_version="pytest-system",
        )
