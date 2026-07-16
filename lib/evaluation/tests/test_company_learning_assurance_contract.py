from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_assurance import (
    CompanyLearningAssuranceSummary,
    CorrectionAssurance,
    NegativeAssurance,
    PopulationAssurance,
    PositiveAssurance,
    SlackAssurance,
    VariantCollisionAssurance,
    VariantPopulationAssurance,
    validate_company_learning_assurance_artifact,
    validate_correction_assurance_component,
    validate_variant_collision_assurance_component,
    validate_variant_population_assurance_component,
)
from lib.evaluation.company_learning_population import IntervalEstimate
from lib.evaluation.company_learning_variant_population import (
    CompanyLearningVariantPopulationEvidence,
    VariantAliasExecutionObservation,
    VariantAliasMechanismMetrics,
    build_variant_alias_population,
    evaluate_variant_alias_population,
)
from lib.evaluation.company_learning_variant_collisions import (
    HeldOutVariantCollisionPopulation,
    VariantCollisionFamily,
    VariantCollisionPairObservation,
    VariantCollisionPopulationReport,
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
    _safe_observations as _safe_collision_observations,
)


_DIGEST = "a" * 64
_ARCHITECTURE_DIGEST = "b" * 64
_IMPLEMENTATION_PLAN_DIGEST = "c" * 64
_SOURCE_ID_GAP = (
    "runtime lacks authenticated SourceIdentityBinding evidence"
)


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
            "schema_version": (
                "company-learning-variant-collision-evidence-v1"
            ),
            "created_at": self.created_at,
            "run_id": self.run_id,
            "system_version": self.system_version,
            "registry_path": self.registry_path,
            "registry_population": self.registry_population.model_dump(
                mode="json"
            ),
            "registry_population_digest": self.registry_population_digest,
            "assignments": list(self.assignments),
            "observations": [
                row.model_dump(mode="json") for row in self.observations
            ],
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
    status: str = "observed_with_gaps",
    observed_pair_count: int = 14,
    unsupported_case_count: int = 2,
    adaptive_safe_containment_rate: float = 1.0,
    adaptive_unsafe_rate: float = 0.0,
    safety_incident_count: int = 0,
    source_native_authoritative_rate: float = 1.0,
) -> VariantCollisionAssurance:
    behavior = lambda value: _interval(
        value,
        sample_size=observed_pair_count,
    )
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
        adaptive_safe_containment_rate=behavior(
            adaptive_safe_containment_rate
        ),
        frozen_safe_containment_rate=behavior(1.0),
        adaptive_unsafe_rate=behavior(adaptive_unsafe_rate),
        frozen_unsafe_rate=behavior(0.0),
        adaptive_unsafe_resolution_rate=behavior(adaptive_unsafe_rate),
        frozen_unsafe_resolution_rate=behavior(0.0),
        adaptive_authoritative_resolution_rate=behavior(
            (
                2 / observed_pair_count
                if unsupported_case_count == 0
                else 0.0
            )
        ),
        frozen_authoritative_resolution_rate=behavior(
            (
                2 / observed_pair_count
                if unsupported_case_count == 0
                else 0.0
            )
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
        source_native_observed_case_count=(
            2 if unsupported_case_count == 0 else 0
        ),
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
                    VariantCollisionFamily
                    .CONFLICTING_SOURCE_NATIVE_IDENTIFIER.value
                ): unsupported_case_count,
            },
            "learned_entity_type": (
                {"system": 1, "team": 1}
                if unsupported_case_count == 2
                else {}
            ),
            "entity_type_relation": (
                {"same_type": 2}
                if unsupported_case_count == 2
                else {}
            ),
            "learned_lifecycle": (
                {"active": 2}
                if unsupported_case_count == 2
                else {}
            ),
        },
        unsupported_reason_counts=(
            {_SOURCE_ID_GAP: unsupported_case_count}
            if unsupported_case_count
            else {}
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
                execution_status="unsupported",
                unsupported_reason=_SOURCE_ID_GAP,
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
        adaptive_safe_containment_rate=(
            report.adaptive_safe_containment_rate
        ),
        frozen_safe_containment_rate=report.frozen_safe_containment_rate,
        adaptive_unsafe_rate=report.adaptive_unsafe_rate,
        frozen_unsafe_rate=report.frozen_unsafe_rate,
        adaptive_unsafe_resolution_rate=(
            report.adaptive_unsafe_resolution_rate
        ),
        frozen_unsafe_resolution_rate=(
            report.frozen_unsafe_resolution_rate
        ),
        adaptive_authoritative_resolution_rate=(
            report.adaptive_authoritative_resolution_rate
        ),
        frozen_authoritative_resolution_rate=(
            report.frozen_authoritative_resolution_rate
        ),
        adaptive_candidate_visibility_rate=(
            report.adaptive_candidate_visibility_rate
        ),
        frozen_candidate_visibility_rate=(
            report.frozen_candidate_visibility_rate
        ),
        adaptive_none_of_above_availability_rate=(
            report.adaptive_none_of_above_availability_rate
        ),
        frozen_none_of_above_availability_rate=(
            report.frozen_none_of_above_availability_rate
        ),
        adaptive_learned_promotion_rate=(
            report.adaptive_learned_promotion_rate
        ),
        frozen_learned_promotion_rate=(
            report.frozen_learned_promotion_rate
        ),
        adaptive_wrong_model_rate=report.adaptive_wrong_model_rate,
        frozen_wrong_model_rate=report.frozen_wrong_model_rate,
        adaptive_wrong_model_count=report.adaptive_wrong_model_count,
        frozen_wrong_model_count=report.frozen_wrong_model_count,
        adaptive_source_immutability_rate=(
            report.adaptive_source_immutability_rate
        ),
        frozen_source_immutability_rate=(
            report.frozen_source_immutability_rate
        ),
        safety_incident_count=report.safety_incident_count,
        source_native_observed_case_count=(
            source_native.observed_case_count
        ),
        source_native_unsupported_case_count=(
            source_native.unsupported_case_count
        ),
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
                [
                    row.model_dump(mode="json")
                    for row in evidence.observations
                ]
            ),
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
        frozen_target_candidate_exposure_rate=(
            frozen_target_candidate_exposure_rate
        ),
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
        candidate_memory_mediated_success_rate=(
            candidate_memory_mediated_success_rate
        ),
        adaptive_mean_llm_calls=1.0,
        frozen_mean_llm_calls=1.0,
        hard_safety_incident_count=hard_safety_incident_count,
        control_integrity_violation_count=(
            control_integrity_violation_count
        ),
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
        runtime_support_rate=(
            report.observed_pair_count / report.pair_count
        ),
        adaptive_correctness=report.adaptive_correctness,
        frozen_correctness=report.frozen_correctness,
        adaptive_minus_frozen_correctness=(
            report.adaptive_minus_frozen_correctness
        ),
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
        "evidence": (
            evidence_digest or artifact.component_digests["evidence"]
        ),
    }
    if artifact.audit is not None:
        component_digests["audit"] = (
            audit_digest or artifact.component_digests["audit"]
        )
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


def _summary(
    *,
    slack: SlackAssurance | None = None,
    correction: CorrectionAssurance | None = None,
    variant_population: VariantPopulationAssurance | None = None,
    variant_collision: VariantCollisionAssurance | None = None,
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
            "positive_company_learning_evaluation": (
                "/tmp/positive-evaluation.json"
            ),
            "positive_company_learning_evidence_bundle": (
                "/tmp/positive-bundle.json"
            ),
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
    artifact_paths = {
        **positive.artifact_paths,
        **negative.artifact_paths,
        **slack.artifact_paths,
        **correction.artifact_paths,
        **variant_population.artifact_paths,
        **variant_collision.artifact_paths,
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
        **{
            f"slack_{key}": value
            for key, value in slack.component_digests.items()
        },
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
            f"population_{key}": value
            for key, value in population.component_digests.items()
        },
    }
    return CompanyLearningAssuranceSummary(
        run_id="pytest-assurance",
        system_version="pytest-system",
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
        population=population,
        proof_gaps=("not open-world or task-autonomy proof",),
        blocking_failures=blocking_failures,
        component_digests=component_digests,
        artifact_paths=artifact_paths,
    )


def test_summary_v4_binds_reviewed_identity_and_active_scope() -> None:
    summary = _summary()

    assert summary.schema_version == "company-learning-assurance-summary-v4"
    assert summary.architecture_digest == _ARCHITECTURE_DIGEST
    assert summary.implementation_plan_digest == _IMPLEMENTATION_PLAN_DIGEST
    assert summary.evaluation_profile == "autonomous-company-learning-v1"
    assert summary.excluded_capabilities == (
        "autonomous_task_planning",
        "autonomous_task_execution",
    )
    assert validate_company_learning_assurance_artifact(
        summary.artifact_payload()
    ) == summary

    with pytest.raises(ValidationError, match="architecture_digest"):
        _summary(architecture_digest="not-a-digest")
    with pytest.raises(ValidationError, match="implementation_plan_digest"):
        _summary(implementation_plan_digest="not-a-digest")
    with pytest.raises(ValidationError, match="explicitly exclude"):
        _summary(excluded_capabilities=("autonomous_task_planning",))


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

    assert validate_variant_population_assurance_component(
        assurance,
        run_id="pytest-assurance:variant",
        system_version="pytest-system",
    ) == evidence

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


def test_collision_supported_scope_is_safe_but_not_full_scope() -> None:
    summary = _summary()

    assert summary.status == "working"
    assert summary.variant_collision.status == "observed_with_gaps"
    assert summary.variant_collision.supported_scope_satisfied is True
    assert summary.variant_collision.full_scope_complete is False
    assert summary.variant_collision.observed_pair_count == 14
    assert summary.variant_collision.unsupported_case_count == 2
    assert summary.variant_collision.unsupported_reason_counts == {
        _SOURCE_ID_GAP: 2
    }

    complete = _collision_assurance(
        status="observed",
        observed_pair_count=16,
        unsupported_case_count=0,
    )
    assert complete.full_scope_complete is True
    assert complete.source_native_scope_valid is True

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

    assert validate_variant_collision_assurance_component(
        assurance,
        run_id="pytest-assurance:collision",
        system_version="pytest-system",
    ) == evidence.report

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

    assert validate_correction_assurance_component(
        assurance,
        run_id="pytest-assurance:correction",
        system_version="pytest-system",
    ) == artifact

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
