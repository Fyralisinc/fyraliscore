"""Typed contract for the combined company-learning assurance report."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Literal
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.canonical_referent_replacement import (
    CanonicalResourceReplacementReport,
    validate_canonical_resource_replacement_artifact,
)
from lib.evaluation.company_learning_active_surfaces import (
    ActiveLearningSurfacesReport,
    SEALED_ACTIVE_SURFACE_CLAIMS,
    SourceSalienceSurfaceReport,
    StructuredIdentitySurfaceReport,
    validate_active_learning_surfaces_artifact,
)
from lib.evaluation.company_learning_experiment import (
    CorrectiveMemoryExperimentReport,
    PairedRecurrenceResult,
)
from lib.evaluation.company_learning_customer_lifecycle import (
    CustomerLifecycleObservation,
    CustomerLifecyclePopulation,
    CustomerLifecycleReport,
    evaluate_customer_lifecycle_population,
)
from lib.evaluation.company_learning_population import (
    HeldOutExactAliasPopulation,
    HeldOutPairObservation,
    IntervalEstimate,
    evaluate_heldout_population,
)
from lib.evaluation.company_learning_retention import (
    CompanyLearningRetentionReport,
    RetentionBehavior,
    RetentionHorizonMetrics,
    RetentionObservation,
    RetentionRunSpec,
    evaluate_company_learning_retention,
)
from lib.evaluation.company_learning_variant_population import (
    CompanyLearningVariantPopulationEvidence,
    VariantAliasMechanismMetrics,
    validate_variant_population_evidence_artifact,
)
from lib.evaluation.company_learning_variant_collisions import (
    HeldOutVariantCollisionPopulation,
    VariantCollisionFamily,
    VariantCollisionPairObservation,
    VariantCollisionPopulationReport,
    evaluate_variant_collision_population,
)
from lib.evaluation.correction_assurance import (
    CorrectionAssuranceArtifact,
    validate_correction_assurance_artifact as _validate_correction_artifact,
)
from lib.evaluation.proof import EvidenceTier
from lib.evaluation.repository_provenance import (
    RepositoryProvenance,
    verify_repository_provenance,
)
from lib.evaluation.slack_reconstruction_gold import (
    SlackGoldFamily,
    SlackReconstructionReport,
)
from lib.evaluation.source_identity_binding_lifecycle import (
    SourceIdentityBindingLifecycleReport,
    validate_source_identity_binding_lifecycle_artifact,
)


_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_SEALED_VARIANT_POPULATION_DIGEST = (
    "d51087c86ff9a80d3729d3ad9147f14109570b00ab24df025220936017dc4e58"
)
_SEALED_VARIANT_COLLISION_DIGEST = (
    "925b8d442d093de1ba40a94b3e8a689001ff533498a5b7ba11e2cdca302d34aa"
)
_SEALED_CUSTOMER_LIFECYCLE_DIGEST = (
    "184606eca260c0bbc1150425108c43b0431ccc6cc5373191a7bbc98d4cd62a8a"
)
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_RECOGNIZED_LIFECYCLE_EVIDENCE_TIERS = {
    "canonical-resource-replacement-evidence-v1": frozenset(
        {EvidenceTier.E4}
    ),
    "source-identity-binding-lifecycle-evidence-v1": frozenset(
        {EvidenceTier.E4}
    ),
}


def _validate_recognized_lifecycle_tier(
    *,
    component: str,
    evidence_contract: str,
    evidence_tier: EvidenceTier,
) -> None:
    recognized_tiers = _RECOGNIZED_LIFECYCLE_EVIDENCE_TIERS.get(
        evidence_contract
    )
    if recognized_tiers is None:
        raise ValueError(
            f"{component} assurance uses an unrecognized evidence contract"
        )
    if evidence_tier not in recognized_tiers:
        recognized = ", ".join(
            sorted(tier.value for tier in recognized_tiers)
        )
        raise ValueError(
            f"{component} evidence contract {evidence_contract} recognizes "
            f"only tier(s) {recognized}, not {evidence_tier.value}"
        )


class _SummaryModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    @model_validator(mode="after")
    def valid_digest_and_path_maps(self) -> Self:
        for field_name in ("component_digests", "artifact_paths"):
            values = getattr(self, field_name, None)
            if values is None:
                continue
            if not values:
                raise ValueError(f"{field_name} must not be empty")
            for key, value in values.items():
                if not key or not value:
                    raise ValueError(f"{field_name} requires non-empty entries")
                if (
                    field_name == "component_digests"
                    and _DIGEST_RE.fullmatch(value) is None
                ):
                    raise ValueError(f"invalid component digest for {key}: {value}")
        return self


class PositiveAssurance(_SummaryModel):
    status: str
    pair_count: int = Field(ge=0)
    adaptive_correctness_rate: float | None
    frozen_correctness_rate: float | None
    adaptive_minus_frozen_correctness: float | None
    hard_failures: tuple[str, ...]
    artifact_paths: dict[str, str]
    component_digests: dict[str, str]


class NegativeAssurance(_SummaryModel):
    status: str
    pair_count: int = Field(ge=0)
    safety_incident_count: int = Field(ge=0)
    adaptive_unsafe_count: int = Field(ge=0)
    frozen_unsafe_count: int = Field(ge=0)
    artifact_paths: dict[str, str]
    component_digests: dict[str, str]

    @model_validator(mode="after")
    def unsafe_counts_require_incidents(self) -> Self:
        if (
            self.adaptive_unsafe_count > 0 or self.frozen_unsafe_count > 0
        ) and self.safety_incident_count == 0:
            raise ValueError("unsafe negative-control arms require safety incidents")
        return self


class SlackAssurance(_SummaryModel):
    status: str
    metrics: dict[str, Any]
    evidence_tier: EvidenceTier
    scope_complete: bool
    open_world_complete: bool
    blocking_for_active_slice: bool
    artifact_paths: dict[str, str]
    component_digests: dict[str, str]

    @model_validator(mode="after")
    def explicit_proof_scope_is_coherent(self) -> Self:
        if self.scope_complete != (self.status == "observed"):
            raise ValueError("Slack scope completeness must match observed status")
        if self.open_world_complete and not self.scope_complete:
            raise ValueError("open-world Slack proof requires complete active scope")
        if self.open_world_complete and self.evidence_tier.rank < 5:
            raise ValueError("open-world Slack proof requires at least E5 evidence")
        return self

    @property
    def active_slice_satisfied(self) -> bool:
        case_count = self.metrics.get("case_count")
        supported_count = self.metrics.get("supported_case_count")
        correct_count = self.metrics.get("correct_case_count")
        contamination_rate = self.metrics.get("contamination_rate")
        return bool(
            self.scope_complete
            and isinstance(case_count, int)
            and case_count > 0
            and supported_count == case_count
            and correct_count == case_count
            and contamination_rate == 0.0
        )


class CorrectionAssurance(_SummaryModel):
    status: Literal["working", "failed", "incomplete"]
    evidence_tier: EvidenceTier
    expected_dependency_count: int = Field(ge=0)
    discovered_dependency_count: int = Field(ge=0)
    dependency_discovery_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    immediate_fence_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    direct_repair_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    recursive_repair_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    relation_retirement_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    projection_invalidation_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    projection_rebuild_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    residual_unsafe_debt_count: int = Field(ge=0)
    convergence_ratio: float | None = Field(default=None, ge=0.0, le=1.0)
    replay_idempotent: bool
    source_immutable: bool
    tenant_isolated: bool
    converged: bool
    incidents: tuple[str, ...]
    artifact_paths: dict[str, str]
    component_digests: dict[str, str]

    @model_validator(mode="after")
    def correction_state_is_coherent(self) -> Self:
        if self.status == "working" and not self.converged:
            raise ValueError("working correction assurance must converge")
        if self.status == "failed" and self.converged and not self.incidents:
            raise ValueError(
                "failed correction assurance requires non-convergence or incidents"
            )
        if self.converged and self.evidence_tier.rank < 3:
            raise ValueError(
                "converged correction assurance requires at least E3 evidence"
            )
        if set(self.artifact_paths) != {"correction_evidence"}:
            raise ValueError(
                "correction assurance requires exactly one evidence artifact"
            )
        if set(self.component_digests) not in (
            {"artifact", "evidence"},
            {"artifact", "evidence", "audit"},
        ):
            raise ValueError(
                "correction assurance requires artifact, evidence and optional "
                "audit digests"
            )
        return self


class PopulationAssurance(_SummaryModel):
    status: str
    registry_pair_count: int = Field(ge=0)
    observed_pair_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    runtime_support_rate: float = Field(ge=0.0, le=1.0)
    metrics: dict[str, Any]
    unsupported_strata_counts: dict[str, dict[str, int]]
    unsupported_reason_counts: dict[str, int]
    artifact_paths: dict[str, str]
    component_digests: dict[str, str]

    @model_validator(mode="after")
    def exact_population_accounting(self) -> Self:
        if self.registry_pair_count != 60:
            raise ValueError("v1 assurance requires the sealed 60-case registry")
        if (
            self.observed_pair_count + self.unsupported_case_count
            != self.registry_pair_count
        ):
            raise ValueError(
                "population observed and unsupported counts must partition the registry"
            )
        expected_rate = self.observed_pair_count / self.registry_pair_count
        if abs(self.runtime_support_rate - expected_rate) > 1e-12:
            raise ValueError("population runtime support rate is inconsistent")
        expected_metrics = {
            "pair_count": self.registry_pair_count,
            "observed_pair_count": self.observed_pair_count,
            "unsupported_case_count": self.unsupported_case_count,
            "complete_population": True,
        }
        for key, expected in expected_metrics.items():
            if self.metrics.get(key) != expected:
                raise ValueError(f"population metric {key} does not match summary")
        for dimension, counts in self.unsupported_strata_counts.items():
            if sum(counts.values()) != self.unsupported_case_count:
                raise ValueError(
                    "unsupported population stratum does not cover every "
                    f"unsupported case: {dimension}"
                )
        if sum(self.unsupported_reason_counts.values()) != self.unsupported_case_count:
            raise ValueError("unsupported reasons must cover every unsupported case")
        return self


class VariantPopulationAssurance(_SummaryModel):
    status: Literal["observed", "observed_with_gaps", "failed"]
    evidence_tier: EvidenceTier
    registry_pair_count: int = Field(ge=0)
    observed_pair_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    runtime_support_rate: float = Field(ge=0.0, le=1.0)
    adaptive_correctness: IntervalEstimate
    frozen_correctness: IntervalEstimate
    adaptive_minus_frozen_correctness: IntervalEstimate
    adaptive_unsafe_rate: IntervalEstimate
    frozen_unsafe_rate: IntervalEstimate
    mechanism_metrics: VariantAliasMechanismMetrics
    artifact_paths: dict[str, str]
    component_digests: dict[str, str]

    @model_validator(mode="after")
    def exact_variant_population_accounting(self) -> Self:
        if self.registry_pair_count != 24:
            raise ValueError("variant assurance requires the sealed 24-case registry")
        if (
            self.observed_pair_count + self.unsupported_case_count
            != self.registry_pair_count
        ):
            raise ValueError(
                "variant observed and unsupported counts must partition the registry"
            )
        expected_rate = self.observed_pair_count / self.registry_pair_count
        if abs(self.runtime_support_rate - expected_rate) > 1e-12:
            raise ValueError("variant population runtime support rate is inconsistent")
        estimates = (
            self.adaptive_correctness,
            self.frozen_correctness,
            self.adaptive_minus_frozen_correctness,
            self.adaptive_unsafe_rate,
            self.frozen_unsafe_rate,
        )
        if any(
            estimate.sample_size != self.observed_pair_count for estimate in estimates
        ):
            raise ValueError(
                "variant continuous metrics must cover every observed pair"
            )
        metrics = self.mechanism_metrics
        expected_mechanism_accounting = {
            "selected_case_count": self.registry_pair_count,
            "observed_pair_count": self.observed_pair_count,
            "unsupported_case_count": self.unsupported_case_count,
            "full_registry_coverage_rate": 1.0,
            "observed_execution_rate": self.runtime_support_rate,
            "adaptive_correctness_rate": (self.adaptive_correctness.point_estimate),
            "frozen_correctness_rate": (self.frozen_correctness.point_estimate),
            "adaptive_minus_frozen_correctness": (
                self.adaptive_minus_frozen_correctness.point_estimate
            ),
        }
        if any(
            not _metric_values_match(
                getattr(metrics, field_name),
                expected_value,
            )
            for field_name, expected_value in (expected_mechanism_accounting.items())
        ):
            raise ValueError(
                "variant mechanism metrics do not match summary accounting"
            )
        if set(self.artifact_paths) != {"variant_population_evidence"}:
            raise ValueError("variant assurance requires exactly one evidence artifact")
        if set(self.component_digests) != {
            "evidence",
            "registry",
            "report",
            "experiment_report",
            "mechanism_metrics",
        }:
            raise ValueError(
                "variant assurance requires evidence, registry, report, "
                "experiment-report and mechanism-metrics digests"
            )
        expected_status = (
            "failed"
            if self.has_unsafe_or_invalid_mechanism
            else (
                "observed"
                if self.has_complete_sealed_coverage
                else "observed_with_gaps"
            )
        )
        if self.status != expected_status:
            raise ValueError(
                "variant assurance status does not match coverage, safety "
                "and mechanism evidence"
            )
        if self.status == "observed" and self.evidence_tier.rank < 4:
            raise ValueError("observed variant assurance requires at least E4 evidence")
        return self

    @property
    def has_complete_sealed_coverage(self) -> bool:
        return bool(
            self.observed_pair_count == self.registry_pair_count
            and self.unsupported_case_count == 0
            and self.runtime_support_rate == 1.0
            and self.mechanism_metrics.full_registry_coverage_rate == 1.0
            and self.mechanism_metrics.observed_execution_rate == 1.0
        )

    @property
    def has_unsafe_or_invalid_mechanism(self) -> bool:
        metrics = self.mechanism_metrics
        return bool(
            self.adaptive_unsafe_rate.point_estimate > 0.0
            or self.frozen_unsafe_rate.point_estimate > 0.0
            or metrics.hard_safety_incident_count > 0
            or metrics.control_integrity_violation_count > 0
            or metrics.candidate_memory_mediated_success_rate != 1.0
            or metrics.adaptive_target_candidate_authorization_rate != 1.0
            or metrics.frozen_target_candidate_exposure_rate != 0.0
        )

    @property
    def working_requirements_satisfied(self) -> bool:
        return bool(
            self.status == "observed"
            and self.has_complete_sealed_coverage
            and not self.has_unsafe_or_invalid_mechanism
        )


class VariantCollisionAssurance(_SummaryModel):
    status: Literal["observed", "observed_with_gaps", "failed"]
    evidence_tier: EvidenceTier
    registry_pair_count: int = Field(ge=0)
    observed_pair_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    runtime_support_rate: IntervalEstimate
    adaptive_safe_containment_rate: IntervalEstimate
    frozen_safe_containment_rate: IntervalEstimate
    adaptive_unsafe_rate: IntervalEstimate
    frozen_unsafe_rate: IntervalEstimate
    adaptive_unsafe_resolution_rate: IntervalEstimate
    frozen_unsafe_resolution_rate: IntervalEstimate
    adaptive_authoritative_resolution_rate: IntervalEstimate
    frozen_authoritative_resolution_rate: IntervalEstimate
    adaptive_candidate_visibility_rate: IntervalEstimate
    frozen_candidate_visibility_rate: IntervalEstimate
    adaptive_none_of_above_availability_rate: IntervalEstimate
    frozen_none_of_above_availability_rate: IntervalEstimate
    adaptive_learned_promotion_rate: IntervalEstimate
    frozen_learned_promotion_rate: IntervalEstimate
    adaptive_wrong_model_rate: IntervalEstimate
    frozen_wrong_model_rate: IntervalEstimate
    adaptive_wrong_model_count: int = Field(ge=0)
    frozen_wrong_model_count: int = Field(ge=0)
    adaptive_source_immutability_rate: IntervalEstimate
    frozen_source_immutability_rate: IntervalEstimate
    safety_incident_count: int = Field(ge=0)
    source_native_observed_case_count: int = Field(ge=0)
    source_native_unsupported_case_count: int = Field(ge=0)
    source_native_adaptive_authoritative_resolution_rate: IntervalEstimate | None
    source_native_frozen_authoritative_resolution_rate: IntervalEstimate | None
    unsupported_strata_counts: dict[str, dict[str, int]]
    unsupported_reason_counts: dict[str, int]
    artifact_paths: dict[str, str]
    component_digests: dict[str, str]

    @model_validator(mode="after")
    def exact_collision_population_accounting(self) -> Self:
        if self.registry_pair_count != 16:
            raise ValueError("collision assurance requires the sealed 16-case registry")
        if (
            self.observed_pair_count + self.unsupported_case_count
            != self.registry_pair_count
        ):
            raise ValueError(
                "collision observed and unsupported counts must partition the registry"
            )
        if self.runtime_support_rate.sample_size != self.registry_pair_count:
            raise ValueError(
                "collision runtime coverage must include every sealed case"
            )
        expected_support = self.observed_pair_count / self.registry_pair_count
        if abs(self.runtime_support_rate.point_estimate - expected_support) > 1e-12:
            raise ValueError("collision runtime support rate is inconsistent")
        behavior_estimates = (
            self.adaptive_safe_containment_rate,
            self.frozen_safe_containment_rate,
            self.adaptive_unsafe_rate,
            self.frozen_unsafe_rate,
            self.adaptive_unsafe_resolution_rate,
            self.frozen_unsafe_resolution_rate,
            self.adaptive_authoritative_resolution_rate,
            self.frozen_authoritative_resolution_rate,
            self.adaptive_candidate_visibility_rate,
            self.frozen_candidate_visibility_rate,
            self.adaptive_none_of_above_availability_rate,
            self.frozen_none_of_above_availability_rate,
            self.adaptive_learned_promotion_rate,
            self.frozen_learned_promotion_rate,
            self.adaptive_wrong_model_rate,
            self.frozen_wrong_model_rate,
            self.adaptive_source_immutability_rate,
            self.frozen_source_immutability_rate,
        )
        if any(
            estimate.sample_size != self.observed_pair_count
            for estimate in behavior_estimates
        ):
            raise ValueError(
                "collision behavior metrics must cover every observed case"
            )
        if (
            self.source_native_observed_case_count
            + self.source_native_unsupported_case_count
            != 2
        ):
            raise ValueError(
                "collision source-native stratum must retain both sealed cases"
            )
        source_rates = (
            self.source_native_adaptive_authoritative_resolution_rate,
            self.source_native_frozen_authoritative_resolution_rate,
        )
        if self.source_native_observed_case_count == 0:
            if any(rate is not None for rate in source_rates):
                raise ValueError("unsupported source-native scope cannot contain rates")
        elif any(
            rate is None or rate.sample_size != self.source_native_observed_case_count
            for rate in source_rates
        ):
            raise ValueError("observed source-native scope requires complete rates")
        for dimension, counts in self.unsupported_strata_counts.items():
            if sum(counts.values()) != self.unsupported_case_count:
                raise ValueError(
                    "collision unsupported stratum does not cover every "
                    f"unsupported case: {dimension}"
                )
        if sum(self.unsupported_reason_counts.values()) != self.unsupported_case_count:
            raise ValueError(
                "collision unsupported reasons must cover every unsupported case"
            )
        if set(self.artifact_paths) != {"variant_collision_evidence"}:
            raise ValueError(
                "collision assurance requires exactly one evidence artifact"
            )
        if set(self.component_digests) != {
            "evidence",
            "registry",
            "report",
            "observations",
        }:
            raise ValueError(
                "collision assurance requires evidence, registry, report "
                "and observation digests"
            )
        expected_status = (
            "failed"
            if self.has_unsafe_supported_evidence
            else ("observed" if self.full_scope_complete else "observed_with_gaps")
        )
        if self.status != expected_status:
            raise ValueError(
                "collision assurance status does not match safety and coverage evidence"
            )
        if self.status != "failed" and self.evidence_tier.rank < 4:
            raise ValueError(
                "observed collision assurance requires at least E4 evidence"
            )
        return self

    @property
    def full_scope_complete(self) -> bool:
        return bool(
            self.observed_pair_count == self.registry_pair_count
            and self.unsupported_case_count == 0
            and self.runtime_support_rate.point_estimate == 1.0
            and self.source_native_scope_valid
        )

    @property
    def has_unsafe_supported_evidence(self) -> bool:
        zero_metrics = (
            self.adaptive_unsafe_rate,
            self.frozen_unsafe_rate,
            self.adaptive_unsafe_resolution_rate,
            self.frozen_unsafe_resolution_rate,
            self.adaptive_learned_promotion_rate,
            self.frozen_learned_promotion_rate,
            self.adaptive_wrong_model_rate,
            self.frozen_wrong_model_rate,
        )
        one_metrics = (
            self.adaptive_safe_containment_rate,
            self.frozen_safe_containment_rate,
            self.adaptive_candidate_visibility_rate,
            self.frozen_candidate_visibility_rate,
            self.adaptive_none_of_above_availability_rate,
            self.frozen_none_of_above_availability_rate,
            self.adaptive_source_immutability_rate,
            self.frozen_source_immutability_rate,
        )
        return bool(
            self.safety_incident_count
            or self.adaptive_wrong_model_count
            or self.frozen_wrong_model_count
            or any(metric.point_estimate != 0.0 for metric in zero_metrics)
            or any(metric.point_estimate != 1.0 for metric in one_metrics)
            or not self.source_native_scope_valid
        )

    @property
    def supported_scope_satisfied(self) -> bool:
        return bool(
            self.observed_pair_count > 0 and not self.has_unsafe_supported_evidence
        )

    @property
    def source_native_scope_valid(self) -> bool:
        if self.source_native_observed_case_count == 0:
            return self.source_native_unsupported_case_count == 2
        adaptive = self.source_native_adaptive_authoritative_resolution_rate
        frozen = self.source_native_frozen_authoritative_resolution_rate
        return bool(
            self.source_native_observed_case_count == 2
            and self.source_native_unsupported_case_count == 0
            and adaptive is not None
            and frozen is not None
            and adaptive.sample_size == 2
            and frozen.sample_size == 2
            and adaptive.point_estimate == 1.0
            and frozen.point_estimate == 1.0
        )


class CustomerLifecycleAssurance(_SummaryModel):
    status: Literal["observed", "observed_with_gaps", "failed"]
    evidence_tier: EvidenceTier
    case_count: int = Field(ge=0)
    observed_case_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    violating_case_count: int = Field(ge=0)
    runtime_support_rate: IntervalEstimate
    rename_continuity_rate: IntervalEstimate
    valid_time_resolution_accuracy: IntervalEstimate
    stale_alias_rejection_rate: IntervalEstimate
    current_alias_safety_rate: IntervalEstimate
    historical_name_reuse_accuracy: IntervalEstimate
    observation_immutability_rate: IntervalEstimate
    model_immutability_rate: IntervalEstimate
    archive_alias_rejection_rate: IntervalEstimate
    archived_mutation_rejection_rate: IntervalEstimate
    alias_interval_non_overlap_rate: IntervalEstimate
    tenant_isolation_rate: IntervalEstimate
    replay_idempotency_rate: IntervalEstimate
    unsupported_reason_counts: dict[str, int]
    artifact_paths: dict[str, str]
    component_digests: dict[str, str]

    @model_validator(mode="after")
    def exact_lifecycle_accounting(self) -> Self:
        if self.case_count != 8:
            raise ValueError(
                "customer lifecycle assurance requires the sealed 8-case registry"
            )
        if self.observed_case_count + self.unsupported_case_count != self.case_count:
            raise ValueError(
                "customer lifecycle observations must partition the registry"
            )
        if self.runtime_support_rate.sample_size != self.case_count:
            raise ValueError(
                "customer lifecycle support must include every sealed case"
            )
        if (
            self.runtime_support_rate.point_estimate
            != self.observed_case_count / self.case_count
        ):
            raise ValueError("customer lifecycle runtime support is inconsistent")
        if sum(self.unsupported_reason_counts.values()) != (
            self.unsupported_case_count
        ):
            raise ValueError(
                "customer lifecycle unsupported reasons must cover every gap"
            )
        if set(self.artifact_paths) != {"customer_lifecycle_evidence"}:
            raise ValueError(
                "customer lifecycle assurance requires one evidence artifact"
            )
        if set(self.component_digests) != {
            "evidence",
            "registry",
            "report",
            "observations",
        }:
            raise ValueError(
                "customer lifecycle assurance requires evidence, registry, "
                "report and observation digests"
            )
        expected_status = (
            "failed"
            if self.has_blocking_evidence
            else "observed"
            if self.full_scope_complete
            else "observed_with_gaps"
        )
        if self.status != expected_status:
            raise ValueError(
                "customer lifecycle status does not match continuous evidence"
            )
        if self.status != "failed" and self.evidence_tier.rank < 4:
            raise ValueError(
                "customer lifecycle assurance requires at least E4 evidence"
            )
        return self

    @property
    def continuous_metrics(self) -> tuple[IntervalEstimate, ...]:
        return (
            self.runtime_support_rate,
            self.rename_continuity_rate,
            self.valid_time_resolution_accuracy,
            self.stale_alias_rejection_rate,
            self.current_alias_safety_rate,
            self.historical_name_reuse_accuracy,
            self.observation_immutability_rate,
            self.model_immutability_rate,
            self.archive_alias_rejection_rate,
            self.archived_mutation_rejection_rate,
            self.alias_interval_non_overlap_rate,
            self.tenant_isolation_rate,
            self.replay_idempotency_rate,
        )

    @property
    def full_scope_complete(self) -> bool:
        return bool(
            self.observed_case_count == self.case_count
            and self.unsupported_case_count == 0
            and self.violating_case_count == 0
            and all(metric.point_estimate == 1.0 for metric in self.continuous_metrics)
        )

    @property
    def has_blocking_evidence(self) -> bool:
        return bool(
            self.unsupported_case_count
            or self.violating_case_count
            or any(metric.point_estimate < 1.0 for metric in self.continuous_metrics)
        )


class ActiveSurfacesAssurance(_SummaryModel):
    status: Literal["observed", "failed"]
    evidence_tier: EvidenceTier
    structured_identity: StructuredIdentitySurfaceReport
    source_salience: SourceSalienceSurfaceReport
    artifact_paths: dict[str, str]
    component_digests: dict[str, str]

    @model_validator(mode="after")
    def exact_active_surface_accounting(self) -> Self:
        if set(self.artifact_paths) != {"active_surfaces_evidence"}:
            raise ValueError(
                "active surfaces assurance requires one evidence artifact"
            )
        if set(self.component_digests) != {
            "evidence",
            "report",
            "structured_identity_report",
            "source_salience_report",
            "identity_observations",
            "salience_observations",
        }:
            raise ValueError(
                "active surfaces assurance requires evidence, report, "
                "subreport and raw-observation digests"
            )
        expected_status = "observed" if self.full_scope_complete else "failed"
        if self.status != expected_status:
            raise ValueError(
                "active surfaces status does not match noncompensatory evidence"
            )
        if self.status == "observed" and self.evidence_tier.rank < 4:
            raise ValueError("active surfaces assurance requires at least E4 evidence")
        return self

    @property
    def continuous_metrics(self) -> tuple[IntervalEstimate, ...]:
        identity = self.structured_identity
        salience = self.source_salience
        return (
            identity.runtime_support_rate,
            identity.claim_emission_rate,
            identity.claim_preservation_rate,
            identity.governed_attachment_rate,
            identity.handler_non_authority_rate,
            identity.ingest_non_authority_rate,
            identity.forged_text_rejection_rate,
            identity.missing_binding_non_authority_rate,
            identity.cross_source_isolation_rate,
            identity.cross_tenant_isolation_rate,
            identity.source_immutability_rate,
            salience.runtime_support_rate,
            salience.useful_salience_increase_rate,
            salience.corrected_nonincrease_rate,
            salience.pending_zero_credit_rate,
            salience.foreign_tenant_isolation_rate,
            salience.canonical_truth_immutability_rate,
            salience.grounding_truth_immutability_rate,
            salience.salience_direction_rate,
        )

    @property
    def full_scope_complete(self) -> bool:
        identity = self.structured_identity
        salience = self.source_salience
        return bool(
            identity.status == "observed"
            and identity.case_count == 6
            and identity.observed_case_count == identity.case_count
            and identity.unsupported_case_count == 0
            and identity.violating_case_count == 0
            and salience.status == "observed"
            and salience.case_count == 5
            and salience.observed_case_count == salience.case_count
            and salience.unsupported_case_count == 0
            and salience.violating_case_count == 0
            and all(metric.point_estimate == 1.0 for metric in self.continuous_metrics)
        )

    @property
    def has_blocking_evidence(self) -> bool:
        return not self.full_scope_complete


class RetentionAssurance(_SummaryModel):
    status: Literal["observed", "observed_with_degradation", "failed"]
    evidence_tier: EvidenceTier
    expected_observation_count: int = Field(ge=0)
    observed_observation_count: int = Field(ge=0)
    exact_retention_rate: float = Field(ge=0.0, le=1.0)
    variant_retention_rate: float = Field(ge=0.0, le=1.0)
    corrected_retention_rate: float = Field(ge=0.0, le=1.0)
    overall_positive_retention_rate: float = Field(ge=0.0, le=1.0)
    overall_forgetting_rate: float = Field(ge=0.0, le=1.0)
    restart_survival_rate: float = Field(ge=0.0, le=1.0)
    correction_authority_rate: float = Field(ge=0.0, le=1.0)
    unsafe_globalization_rate: float = Field(ge=0.0, le=1.0)
    negative_control_safety_rate: float = Field(ge=0.0, le=1.0)
    collision_control_safety_rate: float = Field(ge=0.0, le=1.0)
    source_immutability_rate: float = Field(ge=0.0, le=1.0)
    model_consistency_rate: float = Field(ge=0.0, le=1.0)
    evidence_lineage_consistency_rate: float = Field(ge=0.0, le=1.0)
    hard_safety_incident_rate: float = Field(ge=0.0, le=1.0)
    retention_horizon_auc: float = Field(ge=0.0, le=1.0)
    horizon_metrics: tuple[RetentionHorizonMetrics, ...]
    family_counts: dict[str, int]
    artifact_paths: dict[str, str]
    component_digests: dict[str, str]

    @model_validator(mode="after")
    def exact_retention_accounting(self) -> Self:
        if set(self.artifact_paths) != {"retention_evidence"}:
            raise ValueError("retention assurance requires one evidence artifact")
        if set(self.component_digests) != {
            "artifact",
            "spec",
            "report",
            "observations",
        }:
            raise ValueError(
                "retention assurance requires artifact, spec, report and "
                "observation digests"
            )
        expected_status = (
            "observed"
            if self.full_scope_complete
            else "failed"
            if self.has_safety_or_immutability_regression
            else "observed_with_degradation"
        )
        if self.status != expected_status:
            raise ValueError(
                "retention status does not match noncompensatory evidence"
            )
        if self.status == "observed" and self.evidence_tier.rank < 4:
            raise ValueError("retention assurance requires at least E4 evidence")
        return self

    @property
    def has_safety_or_immutability_regression(self) -> bool:
        return bool(
            self.unsafe_globalization_rate > 0.0
            or self.hard_safety_incident_rate > 0.0
            or self.negative_control_safety_rate < 1.0
            or self.collision_control_safety_rate < 1.0
            or self.correction_authority_rate < 1.0
            or self.source_immutability_rate < 1.0
            or self.model_consistency_rate < 1.0
            or self.evidence_lineage_consistency_rate < 1.0
            or any(
                horizon.source_immutability_rate < 1.0
                or horizon.model_consistency_rate < 1.0
                or horizon.evidence_lineage_consistency_rate < 1.0
                or (
                    horizon.negative_safety_rate is not None
                    and horizon.negative_safety_rate < 1.0
                )
                or (
                    horizon.collision_safety_rate is not None
                    and horizon.collision_safety_rate < 1.0
                )
                for horizon in self.horizon_metrics
            )
        )

    @property
    def has_retention_regression(self) -> bool:
        return bool(
            self.expected_observation_count != 14
            or self.observed_observation_count != 14
            or {
                (
                    horizon.cycle_count,
                    horizon.restart_count,
                ): horizon.observed_count
                for horizon in self.horizon_metrics
            }
            != {
                (0, 0): 2,
                (4, 1): 2,
                (16, 2): 10,
            }
            or self.exact_retention_rate < 1.0
            or self.variant_retention_rate < 1.0
            or self.corrected_retention_rate < 1.0
            or self.overall_positive_retention_rate < 1.0
            or self.overall_forgetting_rate > 0.0
            or self.restart_survival_rate < 1.0
            or self.retention_horizon_auc < 1.0
            or any(
                horizon.positive_retention_rate is not None
                and horizon.positive_retention_rate < 1.0
                or horizon.forgetting_rate is not None
                and horizon.forgetting_rate > 0.0
                for horizon in self.horizon_metrics
            )
        )

    @property
    def full_scope_complete(self) -> bool:
        return not (
            self.has_safety_or_immutability_regression
            or self.has_retention_regression
        )

    @property
    def has_blocking_evidence(self) -> bool:
        return not self.full_scope_complete


class CanonicalReplacementAssurance(_SummaryModel):
    status: Literal["observed", "observed_with_gaps", "failed"]
    evidence_tier: EvidenceTier
    evidence_contract: str = Field(
        default="canonical-resource-replacement-evidence-v1",
        min_length=1,
    )
    report: CanonicalResourceReplacementReport
    artifact_paths: dict[str, str]
    component_digests: dict[str, str]

    @model_validator(mode="after")
    def exact_replacement_accounting(self) -> Self:
        if set(self.artifact_paths) != {"canonical_replacement_evidence"}:
            raise ValueError(
                "canonical replacement assurance requires one evidence artifact"
            )
        if set(self.component_digests) != {
            "evidence",
            "report",
            "observation",
        }:
            raise ValueError(
                "canonical replacement assurance requires evidence, report and "
                "raw-observation digests"
            )
        expected_status = (
            "failed"
            if self.has_unsafe_evidence
            else "observed"
            if self.full_scope_complete
            else "observed_with_gaps"
        )
        if self.status != expected_status:
            raise ValueError(
                "canonical replacement status does not match sealed evidence"
            )
        _validate_recognized_lifecycle_tier(
            component="canonical replacement",
            evidence_contract=self.evidence_contract,
            evidence_tier=self.evidence_tier,
        )
        return self

    @property
    def full_scope_complete(self) -> bool:
        return self.report.full_scope_complete

    @property
    def has_unsafe_evidence(self) -> bool:
        return bool(
            self.report.violating_measurement_count
            or self.report.safety_violation_count
            or self.report.immutability_violation_count
        )

    @property
    def has_blocking_evidence(self) -> bool:
        return not self.full_scope_complete


class SourceBindingLifecycleAssurance(_SummaryModel):
    status: Literal["observed", "observed_with_gaps", "failed"]
    evidence_tier: EvidenceTier
    evidence_contract: str = Field(
        default="source-identity-binding-lifecycle-evidence-v1",
        min_length=1,
    )
    report: SourceIdentityBindingLifecycleReport
    artifact_paths: dict[str, str]
    component_digests: dict[str, str]

    @model_validator(mode="after")
    def exact_binding_lifecycle_accounting(self) -> Self:
        if set(self.artifact_paths) != {"source_binding_lifecycle_evidence"}:
            raise ValueError(
                "source binding lifecycle assurance requires one evidence artifact"
            )
        if set(self.component_digests) != {
            "evidence",
            "report",
            "observation",
        }:
            raise ValueError(
                "source binding lifecycle assurance requires evidence, report "
                "and raw-observation digests"
            )
        expected_status = (
            "failed"
            if self.has_unsafe_evidence
            else "observed"
            if self.full_scope_complete
            else "observed_with_gaps"
        )
        if self.status != expected_status:
            raise ValueError(
                "source binding lifecycle status does not match sealed evidence"
            )
        _validate_recognized_lifecycle_tier(
            component="source binding lifecycle",
            evidence_contract=self.evidence_contract,
            evidence_tier=self.evidence_tier,
        )
        return self

    @property
    def full_scope_complete(self) -> bool:
        return self.report.full_scope_complete

    @property
    def has_unsafe_evidence(self) -> bool:
        return bool(
            self.report.violating_measurement_count
            or self.report.safety_violation_count
            or self.report.immutability_violation_count
        )

    @property
    def has_blocking_evidence(self) -> bool:
        return not self.full_scope_complete


class CompanyLearningAssuranceSummary(_SummaryModel):
    schema_version: Literal["company-learning-assurance-summary-v7"] = (
        "company-learning-assurance-summary-v7"
    )
    run_id: str = Field(min_length=1)
    system_version: str = Field(min_length=1)
    repository_provenance: RepositoryProvenance
    architecture_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    implementation_plan_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    evaluation_profile: Literal["autonomous-company-learning-v1"] = (
        "autonomous-company-learning-v1"
    )
    excluded_capabilities: tuple[str, ...] = (
        "autonomous_task_planning",
        "autonomous_task_execution",
    )
    created_at: str = Field(min_length=1)
    status: Literal["working", "failed"]
    positive: PositiveAssurance
    negative: NegativeAssurance
    slack: SlackAssurance
    correction: CorrectionAssurance
    variant_population: VariantPopulationAssurance
    variant_collision: VariantCollisionAssurance
    customer_lifecycle: CustomerLifecycleAssurance
    active_surfaces: ActiveSurfacesAssurance
    retention: RetentionAssurance
    canonical_replacement: CanonicalReplacementAssurance
    source_binding_lifecycle: SourceBindingLifecycleAssurance
    population: PopulationAssurance | None = None
    proof_gaps: tuple[str, ...]
    blocking_failures: tuple[str, ...]
    component_digests: dict[str, str]
    artifact_paths: dict[str, str]

    @model_validator(mode="after")
    def coherent_noncompensatory_summary(self) -> Self:
        expected_exclusions = (
            "autonomous_task_planning",
            "autonomous_task_execution",
        )
        if self.excluded_capabilities != expected_exclusions:
            raise ValueError(
                "autonomous company-learning profile must explicitly exclude "
                "task planning and execution"
            )
        nested_paths = {
            **self.positive.artifact_paths,
            **self.negative.artifact_paths,
            **self.slack.artifact_paths,
            **self.correction.artifact_paths,
            **self.variant_population.artifact_paths,
            **self.variant_collision.artifact_paths,
            **self.customer_lifecycle.artifact_paths,
            **self.active_surfaces.artifact_paths,
            **self.retention.artifact_paths,
            **self.canonical_replacement.artifact_paths,
            **self.source_binding_lifecycle.artifact_paths,
            **(self.population.artifact_paths if self.population is not None else {}),
        }
        if self.artifact_paths != nested_paths:
            raise ValueError(
                "top-level artifact paths must exactly match component paths"
            )
        nested_digests = {
            **{
                f"positive_{key}": value
                for key, value in self.positive.component_digests.items()
            },
            **{
                f"negative_{key}": value
                for key, value in self.negative.component_digests.items()
            },
            **{
                f"slack_{key}": value
                for key, value in self.slack.component_digests.items()
            },
            **{
                f"correction_{key}": value
                for key, value in self.correction.component_digests.items()
            },
            **{
                f"variant_population_{key}": value
                for key, value in (self.variant_population.component_digests.items())
            },
            **{
                f"variant_collision_{key}": value
                for key, value in (self.variant_collision.component_digests.items())
            },
            **{
                f"customer_lifecycle_{key}": value
                for key, value in (self.customer_lifecycle.component_digests.items())
            },
            **{
                f"active_surfaces_{key}": value
                for key, value in self.active_surfaces.component_digests.items()
            },
            **{
                f"retention_{key}": value
                for key, value in self.retention.component_digests.items()
            },
            **{
                f"canonical_replacement_{key}": value
                for key, value in self.canonical_replacement.component_digests.items()
            },
            **{
                f"source_binding_lifecycle_{key}": value
                for key, value in (
                    self.source_binding_lifecycle.component_digests.items()
                )
            },
            **(
                {
                    f"population_{key}": value
                    for key, value in self.population.component_digests.items()
                }
                if self.population is not None
                else {}
            ),
        }
        if self.component_digests != nested_digests:
            raise ValueError(
                "top-level component digests must exactly match components"
            )
        unsafe_component = bool(
            self.positive.hard_failures
            or self.negative.safety_incident_count
            or (
                self.population is not None
                and _population_safety_incident_count(self.population) > 0
            )
            or self.correction.incidents
            or self.correction.residual_unsafe_debt_count
            or not self.correction.source_immutable
            or not self.correction.tenant_isolated
            or not self.correction.replay_idempotent
            or self.variant_population.has_unsafe_or_invalid_mechanism
            or self.variant_collision.has_unsafe_supported_evidence
            or self.customer_lifecycle.has_blocking_evidence
            or self.active_surfaces.has_blocking_evidence
            or self.retention.has_safety_or_immutability_regression
            or self.canonical_replacement.has_unsafe_evidence
            or self.source_binding_lifecycle.has_unsafe_evidence
        )
        blocking_component = bool(
            (
                self.slack.blocking_for_active_slice
                and not self.slack.active_slice_satisfied
            )
            or not self.correction.converged
            or not self.variant_population.working_requirements_satisfied
            or not self.variant_collision.full_scope_complete
            or not self.customer_lifecycle.full_scope_complete
            or not self.active_surfaces.full_scope_complete
            or not self.retention.full_scope_complete
            or not self.canonical_replacement.full_scope_complete
            or not self.source_binding_lifecycle.full_scope_complete
        )
        if self.status == "working" and (
            self.blocking_failures or unsafe_component or blocking_component
        ):
            raise ValueError(
                "working assurance cannot contain blocking or unsafe evidence"
            )
        if self.status == "failed" and not self.blocking_failures:
            raise ValueError("failed assurance requires at least one blocking failure")
        if unsafe_component and not self.blocking_failures:
            raise ValueError("unsafe component evidence requires blocking failures")
        if blocking_component and not self.blocking_failures:
            raise ValueError("blocking component evidence requires blocking failures")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def artifact_payload(self) -> dict[str, Any]:
        return {
            **self.model_dump(mode="json"),
            "summary_digest": self.digest,
        }


def validate_company_learning_assurance_artifact(
    payload: dict[str, Any],
    *,
    repository_root: Path | None = None,
) -> CompanyLearningAssuranceSummary:
    """Validate the typed payload, digest and actual Git repository identity."""

    supplied_digest = str(payload.get("summary_digest") or "")
    summary = CompanyLearningAssuranceSummary.model_validate(
        {key: value for key, value in payload.items() if key != "summary_digest"}
    )
    if supplied_digest != summary.digest:
        raise ValueError("company-learning assurance summary digest mismatch")
    verify_repository_provenance(
        summary.repository_provenance,
        repository_root or _REPOSITORY_ROOT,
    )
    return summary


def validate_correction_assurance_component(
    assurance: CorrectionAssurance,
    *,
    run_id: str,
    system_version: str,
) -> CorrectionAssuranceArtifact:
    """Reopen and cross-bind one correction component to its summary."""

    payload = _read_json_file(assurance.artifact_paths["correction_evidence"])
    artifact = _validate_correction_artifact(payload)
    if artifact.run_id != run_id:
        raise ValueError("correction assurance run identity mismatch")
    if artifact.system_version != system_version:
        raise ValueError("correction assurance system version mismatch")
    if assurance.component_digests["artifact"] != artifact.digest:
        raise ValueError("correction assurance artifact digest mismatch")
    if (
        assurance.component_digests["evidence"]
        != artifact.component_digests["evidence"]
    ):
        raise ValueError("correction runtime-evidence digest mismatch")
    expected_digest_keys = (
        {"artifact", "evidence", "audit"}
        if artifact.audit is not None
        else {"artifact", "evidence"}
    )
    if set(assurance.component_digests) != expected_digest_keys:
        raise ValueError("correction audit digest presence does not match artifact")
    if (
        artifact.audit is not None
        and assurance.component_digests["audit"]
        != (artifact.component_digests["audit"])
    ):
        raise ValueError("correction audit digest mismatch")
    metrics = artifact.metrics
    expected = {
        "status": artifact.status,
        "expected_dependency_count": metrics.expected_dependency_count,
        "discovered_dependency_count": metrics.discovered_dependency_count,
        "dependency_discovery_rate": metrics.dependency_discovery_rate,
        "immediate_fence_rate": metrics.immediate_fence_rate,
        "direct_repair_rate": metrics.direct_repair_rate,
        "recursive_repair_rate": metrics.recursive_repair_rate,
        "relation_retirement_rate": metrics.relation_retirement_rate,
        "projection_invalidation_rate": metrics.projection_invalidation_rate,
        "projection_rebuild_rate": metrics.projection_rebuild_rate,
        "residual_unsafe_debt_count": metrics.residual_unsafe_debt_count,
        "convergence_ratio": metrics.convergence_ratio,
        "replay_idempotent": metrics.replay_idempotent,
        "source_immutable": metrics.source_immutable,
        "tenant_isolated": metrics.tenant_isolated,
        "converged": metrics.converged,
        "incidents": artifact.incidents,
    }
    if any(
        getattr(assurance, field_name) != expected_value
        for field_name, expected_value in expected.items()
    ):
        raise ValueError("correction assurance does not match artifact evidence")
    return artifact


def validate_variant_population_assurance_component(
    assurance: VariantPopulationAssurance,
    *,
    run_id: str,
    system_version: str,
) -> CompanyLearningVariantPopulationEvidence:
    """Reopen and cross-bind the sealed variant population evidence."""

    payload = _read_json_file(assurance.artifact_paths["variant_population_evidence"])
    evidence = validate_variant_population_evidence_artifact(payload)
    if evidence.run_id != run_id:
        raise ValueError("variant assurance run identity mismatch")
    if evidence.system_version != system_version:
        raise ValueError("variant assurance system version mismatch")
    if evidence.execution_mode != "full":
        raise ValueError(
            "variant assurance requires one full sealed-population execution"
        )
    if evidence.registry_population_digest != _SEALED_VARIANT_POPULATION_DIGEST:
        raise ValueError("variant assurance registry identity mismatch")
    registry_ids = tuple(case.case_id for case in evidence.registry_population.cases)
    if len(registry_ids) != 24 or evidence.selected_case_ids != registry_ids:
        raise ValueError("variant assurance does not cover the exact sealed registry")
    report = evidence.population_report
    mechanism_metrics = evidence.mechanism_metrics
    if report is None:
        raise ValueError("variant assurance population report is missing")
    if mechanism_metrics is None or not evidence.mechanism_pairs:
        raise ValueError("variant assurance mechanism evidence is missing")
    if len(evidence.mechanism_pairs) != report.observed_pair_count:
        raise ValueError("variant mechanism evidence must cover every observed pair")

    expected_digests = {
        "evidence": evidence.digest,
        "registry": evidence.registry_population_digest,
        "report": report.digest,
        "experiment_report": evidence.experiment_report.digest,
        "mechanism_metrics": canonical_sha256(
            mechanism_metrics.model_dump(mode="json")
        ),
    }
    if assurance.component_digests != expected_digests:
        raise ValueError("variant assurance component digest mismatch")

    expected_values = {
        "registry_pair_count": report.pair_count,
        "observed_pair_count": report.observed_pair_count,
        "unsupported_case_count": report.unsupported_case_count,
        "runtime_support_rate": (report.observed_pair_count / report.pair_count),
        "adaptive_correctness": report.adaptive_correctness,
        "frozen_correctness": report.frozen_correctness,
        "adaptive_minus_frozen_correctness": (report.adaptive_minus_frozen_correctness),
        "adaptive_unsafe_rate": report.adaptive_unsafe_rate,
        "frozen_unsafe_rate": report.frozen_unsafe_rate,
        "mechanism_metrics": mechanism_metrics,
    }
    if any(
        getattr(assurance, field_name) != expected_value
        for field_name, expected_value in expected_values.items()
    ):
        raise ValueError("variant assurance does not match population evidence")
    return evidence


def validate_variant_collision_assurance_component(
    assurance: VariantCollisionAssurance,
    *,
    run_id: str,
    system_version: str,
) -> VariantCollisionPopulationReport:
    """Reopen collision evidence and recompute its typed report."""

    payload = _read_json_file(assurance.artifact_paths["variant_collision_evidence"])
    supplied_digest = str(payload.get("evidence_digest") or "")
    evidence_payload = {
        key: value for key, value in payload.items() if key != "evidence_digest"
    }
    if canonical_sha256(evidence_payload) != supplied_digest:
        raise ValueError("collision assurance evidence digest mismatch")
    if payload.get("run_id") != run_id:
        raise ValueError("collision assurance run identity mismatch")
    if payload.get("system_version") != system_version:
        raise ValueError("collision assurance system version mismatch")

    registry = HeldOutVariantCollisionPopulation.model_validate(
        payload.get("registry_population")
    )
    if (
        registry.digest != _SEALED_VARIANT_COLLISION_DIGEST
        or payload.get("registry_population_digest") != registry.digest
    ):
        raise ValueError("collision assurance registry identity mismatch")
    registry_ids = tuple(case.case_id for case in registry.cases)
    assignments = payload.get("assignments")
    if (
        not isinstance(assignments, list)
        or tuple(str(row.get("case_id")) for row in assignments) != registry_ids
    ):
        raise ValueError("collision assurance assignments do not cover registry order")
    tenant_ids = [
        str(row.get(key))
        for row in assignments
        for key in ("adaptive_tenant_id", "frozen_tenant_id")
    ]
    if len(tenant_ids) != len(set(tenant_ids)):
        raise ValueError("collision assurance arm tenants are not unique")
    observations = tuple(
        VariantCollisionPairObservation.model_validate(row)
        for row in payload.get("observations") or ()
    )
    if tuple(row.case_id for row in observations) != registry_ids:
        raise ValueError("collision assurance observations changed registry order")
    report = VariantCollisionPopulationReport.model_validate(payload.get("report"))
    recomputed = evaluate_variant_collision_population(
        population=registry,
        observations=observations,
    )
    if recomputed != report:
        raise ValueError("collision assurance report does not match recomputation")
    source_native = report.stratum_reports["collision_family"][
        VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER.value
    ]
    expected_digests = {
        "evidence": supplied_digest,
        "registry": registry.digest,
        "report": report.digest,
        "observations": canonical_sha256(
            [row.model_dump(mode="json") for row in observations]
        ),
    }
    if assurance.component_digests != expected_digests:
        raise ValueError("collision assurance component digest mismatch")
    expected_values = {
        "status": report.status,
        "registry_pair_count": report.pair_count,
        "observed_pair_count": report.observed_pair_count,
        "unsupported_case_count": report.unsupported_case_count,
        "runtime_support_rate": report.runtime_support_rate,
        "adaptive_safe_containment_rate": (report.adaptive_safe_containment_rate),
        "frozen_safe_containment_rate": (report.frozen_safe_containment_rate),
        "adaptive_unsafe_rate": report.adaptive_unsafe_rate,
        "frozen_unsafe_rate": report.frozen_unsafe_rate,
        "adaptive_unsafe_resolution_rate": (report.adaptive_unsafe_resolution_rate),
        "frozen_unsafe_resolution_rate": (report.frozen_unsafe_resolution_rate),
        "adaptive_authoritative_resolution_rate": (
            report.adaptive_authoritative_resolution_rate
        ),
        "frozen_authoritative_resolution_rate": (
            report.frozen_authoritative_resolution_rate
        ),
        "adaptive_candidate_visibility_rate": (
            report.adaptive_candidate_visibility_rate
        ),
        "frozen_candidate_visibility_rate": (report.frozen_candidate_visibility_rate),
        "adaptive_none_of_above_availability_rate": (
            report.adaptive_none_of_above_availability_rate
        ),
        "frozen_none_of_above_availability_rate": (
            report.frozen_none_of_above_availability_rate
        ),
        "adaptive_learned_promotion_rate": (report.adaptive_learned_promotion_rate),
        "frozen_learned_promotion_rate": (report.frozen_learned_promotion_rate),
        "adaptive_wrong_model_rate": report.adaptive_wrong_model_rate,
        "frozen_wrong_model_rate": report.frozen_wrong_model_rate,
        "adaptive_wrong_model_count": report.adaptive_wrong_model_count,
        "frozen_wrong_model_count": report.frozen_wrong_model_count,
        "adaptive_source_immutability_rate": (report.adaptive_source_immutability_rate),
        "frozen_source_immutability_rate": (report.frozen_source_immutability_rate),
        "safety_incident_count": report.safety_incident_count,
        "source_native_observed_case_count": (source_native.observed_case_count),
        "source_native_unsupported_case_count": (source_native.unsupported_case_count),
        "source_native_adaptive_authoritative_resolution_rate": (
            source_native.adaptive_authoritative_resolution_rate
        ),
        "source_native_frozen_authoritative_resolution_rate": (
            source_native.frozen_authoritative_resolution_rate
        ),
        "unsupported_strata_counts": report.unsupported_strata_counts,
        "unsupported_reason_counts": report.unsupported_reason_counts,
    }
    if any(
        getattr(assurance, field_name) != expected_value
        for field_name, expected_value in expected_values.items()
    ):
        raise ValueError("collision assurance does not match persisted evidence")
    return report


def validate_customer_lifecycle_assurance_component(
    assurance: CustomerLifecycleAssurance,
    *,
    run_id: str,
    system_version: str,
) -> CustomerLifecycleReport:
    """Reopen lifecycle evidence and recompute every continuous metric."""

    payload = _read_json_file(assurance.artifact_paths["customer_lifecycle_evidence"])
    supplied_digest = str(payload.get("evidence_digest") or "")
    evidence_payload = {
        key: value for key, value in payload.items() if key != "evidence_digest"
    }
    if canonical_sha256(evidence_payload) != supplied_digest:
        raise ValueError("customer lifecycle evidence digest mismatch")
    if payload.get("run_id") != run_id:
        raise ValueError("customer lifecycle run identity mismatch")
    if payload.get("system_version") != system_version:
        raise ValueError("customer lifecycle system version mismatch")

    registry = CustomerLifecyclePopulation.model_validate(
        payload.get("registry_population")
    )
    if (
        registry.digest != _SEALED_CUSTOMER_LIFECYCLE_DIGEST
        or payload.get("registry_population_digest") != registry.digest
    ):
        raise ValueError("customer lifecycle registry identity mismatch")
    registry_ids = tuple(case.case_id for case in registry.cases)
    assignments = payload.get("assignments")
    if (
        not isinstance(assignments, list)
        or tuple(str(row.get("case_id")) for row in assignments) != registry_ids
    ):
        raise ValueError("customer lifecycle assignments do not cover registry order")
    tenant_ids = [
        str(row.get(key))
        for row in assignments
        for key in ("tenant_id", "isolation_tenant_id")
    ]
    if len(tenant_ids) != len(set(tenant_ids)):
        raise ValueError("customer lifecycle tenant assignments are not unique")
    observations = tuple(
        CustomerLifecycleObservation.model_validate(row)
        for row in payload.get("observations") or ()
    )
    if tuple(row.case_id for row in observations) != registry_ids:
        raise ValueError("customer lifecycle observations changed registry order")
    report = CustomerLifecycleReport.model_validate(payload.get("report"))
    recomputed = evaluate_customer_lifecycle_population(
        population=registry,
        observations=observations,
    )
    if recomputed != report:
        raise ValueError("customer lifecycle report does not match recomputation")
    expected_digests = {
        "evidence": supplied_digest,
        "registry": registry.digest,
        "report": report.digest,
        "observations": canonical_sha256(
            [row.model_dump(mode="json") for row in observations]
        ),
    }
    if assurance.component_digests != expected_digests:
        raise ValueError("customer lifecycle component digest mismatch")
    expected_values = {
        "status": ("failed" if report.status == "contradicted" else report.status),
        "case_count": report.case_count,
        "observed_case_count": report.observed_case_count,
        "unsupported_case_count": report.unsupported_case_count,
        "violating_case_count": report.violating_case_count,
        "runtime_support_rate": report.runtime_support_rate,
        "rename_continuity_rate": report.rename_continuity_rate,
        "valid_time_resolution_accuracy": (report.valid_time_resolution_accuracy),
        "stale_alias_rejection_rate": report.stale_alias_rejection_rate,
        "current_alias_safety_rate": report.current_alias_safety_rate,
        "historical_name_reuse_accuracy": (report.historical_name_reuse_accuracy),
        "observation_immutability_rate": (report.observation_immutability_rate),
        "model_immutability_rate": report.model_immutability_rate,
        "archive_alias_rejection_rate": (report.archive_alias_rejection_rate),
        "archived_mutation_rejection_rate": (report.archived_mutation_rejection_rate),
        "alias_interval_non_overlap_rate": (report.alias_interval_non_overlap_rate),
        "tenant_isolation_rate": report.tenant_isolation_rate,
        "replay_idempotency_rate": report.replay_idempotency_rate,
        "unsupported_reason_counts": report.unsupported_reason_counts,
    }
    if any(
        getattr(assurance, field_name) != expected_value
        for field_name, expected_value in expected_values.items()
    ):
        raise ValueError(
            "customer lifecycle assurance does not match persisted evidence"
        )
    return report


def validate_active_surfaces_assurance_component(
    assurance: ActiveSurfacesAssurance,
    *,
    run_id: str,
    system_version: str,
) -> ActiveLearningSurfacesReport:
    """Reopen active-surface evidence and recompute both raw subreports."""

    payload = _read_json_file(assurance.artifact_paths["active_surfaces_evidence"])
    evidence = validate_active_learning_surfaces_artifact(payload)
    if evidence.run_id != run_id:
        raise ValueError("active surfaces run identity mismatch")
    if evidence.system_version != system_version:
        raise ValueError("active surfaces system version mismatch")
    for observation in evidence.identity_observations:
        if observation.expected_claims != SEALED_ACTIVE_SURFACE_CLAIMS[
            observation.case_id
        ]:
            raise ValueError(
                "active surfaces expected claim contract changed sealed scope"
            )
    expected_digests = {
        "evidence": evidence.digest,
        "report": evidence.report.digest,
        "structured_identity_report": evidence.report.structured_identity.digest,
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
    }
    if assurance.component_digests != expected_digests:
        raise ValueError("active surfaces component digest mismatch")
    if (
        assurance.structured_identity != evidence.report.structured_identity
        or assurance.source_salience != evidence.report.source_salience
        or assurance.status
        != ("observed" if evidence.report.status == "observed" else "failed")
    ):
        raise ValueError(
            "active surfaces assurance does not match persisted evidence"
        )
    return evidence.report


def validate_retention_assurance_component(
    assurance: RetentionAssurance,
    *,
    run_id: str,
    system_version: str,
) -> CompanyLearningRetentionReport:
    """Reopen retention evidence and recompute every raw observation."""

    payload = _read_json_file(assurance.artifact_paths["retention_evidence"])
    spec = RetentionRunSpec.model_validate(payload.get("spec"))
    if spec.run_id != run_id:
        raise ValueError("retention run identity mismatch")
    if spec.system_version != system_version:
        raise ValueError("retention system version mismatch")
    _validate_retention_spec_scope(spec)
    observations = tuple(
        RetentionObservation.model_validate(row)
        for row in payload.get("observations") or ()
    )
    report = CompanyLearningRetentionReport.model_validate(payload.get("report"))
    recomputed = evaluate_company_learning_retention(
        spec=spec,
        observations=observations,
        artifact_refs=report.artifact_refs,
    )
    if recomputed != report:
        raise ValueError("retention report does not match raw recomputation")
    if str(payload.get("report_digest") or "") != report.digest:
        raise ValueError("retention report digest mismatch")
    expected_digests = {
        "artifact": canonical_sha256(payload),
        "spec": spec.digest,
        "report": report.digest,
        "observations": report.observation_digest,
    }
    if assurance.component_digests != expected_digests:
        raise ValueError("retention component digest mismatch")
    expected_values = {
        "status": (
            "failed" if report.status == "contradicted" else report.status
        ),
        "expected_observation_count": report.expected_observation_count,
        "observed_observation_count": report.observed_observation_count,
        "exact_retention_rate": report.exact_retention_rate,
        "variant_retention_rate": report.variant_retention_rate,
        "corrected_retention_rate": report.corrected_retention_rate,
        "overall_positive_retention_rate": (
            report.overall_positive_retention_rate
        ),
        "overall_forgetting_rate": report.overall_forgetting_rate,
        "restart_survival_rate": report.restart_survival_rate,
        "correction_authority_rate": report.correction_authority_rate,
        "unsafe_globalization_rate": report.unsafe_globalization_rate,
        "negative_control_safety_rate": report.negative_control_safety_rate,
        "collision_control_safety_rate": report.collision_control_safety_rate,
        "source_immutability_rate": report.source_immutability_rate,
        "model_consistency_rate": report.model_consistency_rate,
        "evidence_lineage_consistency_rate": (
            report.evidence_lineage_consistency_rate
        ),
        "hard_safety_incident_rate": report.hard_safety_incident_rate,
        "retention_horizon_auc": report.retention_horizon_auc,
        "horizon_metrics": report.horizon_metrics,
        "family_counts": report.family_counts,
    }
    if any(
        getattr(assurance, field_name) != expected_value
        for field_name, expected_value in expected_values.items()
    ):
        raise ValueError("retention assurance does not match persisted evidence")
    return report


def validate_canonical_replacement_assurance_component(
    assurance: CanonicalReplacementAssurance,
    *,
    run_id: str,
    system_version: str,
) -> CanonicalResourceReplacementReport:
    """Reopen canonical replacement evidence and recompute raw obligations."""

    payload = _read_json_file(
        assurance.artifact_paths["canonical_replacement_evidence"]
    )
    evidence = validate_canonical_resource_replacement_artifact(payload)
    if evidence.schema_version != assurance.evidence_contract:
        raise ValueError(
            "canonical replacement evidence contract mismatch"
        )
    if evidence.run_id != run_id:
        raise ValueError("canonical replacement run identity mismatch")
    if evidence.system_version != system_version:
        raise ValueError("canonical replacement system version mismatch")
    expected_digests = {
        "evidence": evidence.digest,
        "report": evidence.report.digest,
        "observation": canonical_sha256(
            evidence.observation.model_dump(mode="json")
        ),
    }
    if assurance.component_digests != expected_digests:
        raise ValueError("canonical replacement component digest mismatch")
    expected_status = (
        "failed"
        if (
            evidence.report.violating_measurement_count
            or evidence.report.safety_violation_count
            or evidence.report.immutability_violation_count
        )
        else "observed"
        if evidence.report.full_scope_complete
        else "observed_with_gaps"
    )
    if assurance.report != evidence.report or assurance.status != expected_status:
        raise ValueError(
            "canonical replacement assurance does not match persisted evidence"
        )
    return evidence.report


def validate_source_binding_lifecycle_assurance_component(
    assurance: SourceBindingLifecycleAssurance,
    *,
    run_id: str,
    system_version: str,
) -> SourceIdentityBindingLifecycleReport:
    """Reopen source-binding lifecycle evidence and recompute raw obligations."""

    payload = _read_json_file(
        assurance.artifact_paths["source_binding_lifecycle_evidence"]
    )
    evidence = validate_source_identity_binding_lifecycle_artifact(payload)
    if evidence.schema_version != assurance.evidence_contract:
        raise ValueError(
            "source binding lifecycle evidence contract mismatch"
        )
    if evidence.run_id != run_id:
        raise ValueError("source binding lifecycle run identity mismatch")
    if evidence.system_version != system_version:
        raise ValueError("source binding lifecycle system version mismatch")
    expected_digests = {
        "evidence": evidence.digest,
        "report": evidence.report.digest,
        "observation": canonical_sha256(
            evidence.observation.model_dump(mode="json")
        ),
    }
    if assurance.component_digests != expected_digests:
        raise ValueError(
            "source binding lifecycle component digest mismatch"
        )
    expected_status = (
        "failed"
        if (
            evidence.report.violating_measurement_count
            or evidence.report.safety_violation_count
            or evidence.report.immutability_violation_count
        )
        else "observed"
        if evidence.report.full_scope_complete
        else "observed_with_gaps"
    )
    if assurance.report != evidence.report or assurance.status != expected_status:
        raise ValueError(
            "source binding lifecycle assurance does not match persisted evidence"
        )
    return evidence.report


def _validate_retention_spec_scope(spec: RetentionRunSpec) -> None:
    expected_horizons = ((0, 0), (4, 1), (16, 2))
    final_horizon = ((16, 2),)
    expected_cases = {
        "retention-exact": (
            RetentionBehavior.EXACT_ALIAS,
            "exact_alias_positive",
            expected_horizons,
        ),
        "retention-variant": (
            RetentionBehavior.VARIANT_ALIAS,
            "acronym_from_long_form",
            expected_horizons,
        ),
        "retention-correction": (
            RetentionBehavior.CORRECTED_ALIAS,
            "authoritative_exact_correction",
            final_horizon,
        ),
        "retention-negative:contextual-non-entity": (
            RetentionBehavior.NEGATIVE_CONTROL,
            "contextual_phrase_negative",
            final_horizon,
        ),
        "retention-negative:unrelated-alias": (
            RetentionBehavior.NEGATIVE_CONTROL,
            "unrelated_negative_control",
            final_horizon,
        ),
        "retention-negative:same-surface-homonym": (
            RetentionBehavior.NEGATIVE_CONTROL,
            "homonym_local_association",
            final_horizon,
        ),
        "retention-negative:conflicting-source-hint": (
            RetentionBehavior.NEGATIVE_CONTROL,
            "conflicting_source_hint",
            final_horizon,
        ),
        "retention-collision:heldout-variant-collision-00": (
            RetentionBehavior.COLLISION_CONTROL,
            "same_type_acronym_collision",
            final_horizon,
        ),
        "retention-collision:heldout-variant-collision-06": (
            RetentionBehavior.COLLISION_CONTROL,
            "punctuation_unicode_normalization_collision",
            final_horizon,
        ),
        "retention-collision:heldout-variant-collision-08": (
            RetentionBehavior.COLLISION_CONTROL,
            "contextual_channel_local_nickname",
            final_horizon,
        ),
    }
    observed_cases = {
        case.case_id: (
            case.behavior,
            case.family,
            tuple(
                (horizon.cycle_count, horizon.restart_count)
                for horizon in case.horizons
            ),
        )
        for case in spec.cases
    }
    if observed_cases != expected_cases:
        raise ValueError("retention spec identity or family changed sealed scope")
    by_behavior = {
        behavior: tuple(
            case for case in spec.cases if case.behavior is behavior
        )
        for behavior in RetentionBehavior
    }
    expected_counts = {
        RetentionBehavior.EXACT_ALIAS: 1,
        RetentionBehavior.VARIANT_ALIAS: 1,
        RetentionBehavior.CORRECTED_ALIAS: 1,
        RetentionBehavior.NEGATIVE_CONTROL: 4,
        RetentionBehavior.COLLISION_CONTROL: 3,
    }
    if {
        behavior: len(cases) for behavior, cases in by_behavior.items()
    } != expected_counts:
        raise ValueError("retention spec does not match sealed behavior scope")
    expected_horizon_set = set(expected_horizons)
    final_horizon_set = set(final_horizon)
    for behavior in (
        RetentionBehavior.EXACT_ALIAS,
        RetentionBehavior.VARIANT_ALIAS,
    ):
        horizons = {
            (row.cycle_count, row.restart_count)
            for row in by_behavior[behavior][0].horizons
        }
        if horizons != expected_horizon_set:
            raise ValueError(
                f"retention {behavior.value} horizons changed sealed scope"
            )
    for behavior in (
        RetentionBehavior.CORRECTED_ALIAS,
        RetentionBehavior.NEGATIVE_CONTROL,
        RetentionBehavior.COLLISION_CONTROL,
    ):
        if any(
            {
                (row.cycle_count, row.restart_count)
                for row in case.horizons
            }
            != final_horizon_set
            for case in by_behavior[behavior]
        ):
            raise ValueError(
                f"retention {behavior.value} must be final-horizon only"
            )


def validate_company_learning_assurance_components(
    summary: CompanyLearningAssuranceSummary,
) -> None:
    """Reopen every evidence component and verify identity and digests."""

    verify_repository_provenance(
        summary.repository_provenance,
        _REPOSITORY_ROOT,
    )
    expected_paths = {
        "positive_pair",
        "positive_company_learning_evaluation",
        "positive_company_learning_evidence_bundle",
        "negative_evidence",
        "population_evidence",
        "correction_evidence",
        "variant_population_evidence",
        "variant_collision_evidence",
        "customer_lifecycle_evidence",
        "active_surfaces_evidence",
        "retention_evidence",
        "canonical_replacement_evidence",
        "source_binding_lifecycle_evidence",
        "slack_observations",
        "slack_report",
    }
    if set(summary.artifact_paths) != expected_paths:
        raise ValueError("assurance artifact path set is incomplete or unknown")

    positive = _read_json_file(summary.artifact_paths["positive_pair"])
    _assert_run_identity(
        positive.get("report"),
        run_id=f"{summary.run_id}:positive",
        system_version=summary.system_version,
    )
    if str(
        positive.get("report_digest") or ""
    ) != summary.positive.component_digests.get("report"):
        raise ValueError("positive report digest mismatch")
    if canonical_sha256(positive.get("report")) != positive.get("report_digest"):
        raise ValueError("positive report failed digest recomputation")
    positive_report = _object(positive.get("report"), "positive report")
    positive_metrics = _object(
        positive_report.get("metrics"),
        "positive metrics",
    )
    if (
        summary.positive.status != positive_report.get("status")
        or summary.positive.pair_count != positive_metrics.get("pair_count")
        or summary.positive.adaptive_correctness_rate
        != positive_metrics.get("adaptive_correctness_rate")
        or summary.positive.frozen_correctness_rate
        != positive_metrics.get("frozen_correctness_rate")
        or summary.positive.adaptive_minus_frozen_correctness
        != positive_metrics.get("adaptive_minus_frozen_correctness")
    ):
        raise ValueError("positive assurance metrics do not match evidence")
    if positive_report.get("incidents") and not summary.positive.hard_failures:
        raise ValueError("positive incidents require positive hard failures")

    positive_evaluation = _read_json_file(
        summary.artifact_paths["positive_company_learning_evaluation"]
    )
    if canonical_sha256(positive_evaluation) != (
        summary.positive.component_digests.get("company_learning_evaluation")
    ):
        raise ValueError("positive evaluation digest mismatch")
    _assert_positive_evaluation_identity(
        positive_evaluation,
        run_id=f"{summary.run_id}:positive",
        system_version=summary.system_version,
        architecture_digest=summary.architecture_digest,
    )

    positive_bundle = _read_json_file(
        summary.artifact_paths["positive_company_learning_evidence_bundle"]
    )
    if canonical_sha256(positive_bundle) != (
        summary.positive.component_digests.get("company_learning_evidence_bundle")
    ):
        raise ValueError("positive evidence bundle digest mismatch")
    _assert_run_identity(
        positive_bundle,
        run_id=f"{summary.run_id}:positive",
        system_version=summary.system_version,
    )
    _assert_architecture_identity(
        positive_bundle,
        architecture_digest=summary.architecture_digest,
        label="positive evidence bundle",
    )

    negative = _read_json_file(summary.artifact_paths["negative_evidence"])
    _assert_run_identity(
        negative.get("report"),
        run_id=f"{summary.run_id}:negative",
        system_version=summary.system_version,
    )
    if str(negative.get("evidence_digest") or "") != (
        summary.negative.component_digests.get("evidence")
    ):
        raise ValueError("negative evidence digest mismatch")
    if canonical_sha256(
        {key: value for key, value in negative.items() if key != "evidence_digest"}
    ) != negative.get("evidence_digest"):
        raise ValueError("negative evidence failed digest recomputation")
    if canonical_sha256(negative.get("report")) != (
        summary.negative.component_digests.get("report")
    ):
        raise ValueError("negative report digest mismatch")
    if str(negative.get("plan_digest") or "") != (
        summary.negative.component_digests.get("plan")
    ):
        raise ValueError("negative plan digest mismatch")
    negative_report = _object(negative.get("report"), "negative report")
    negative_metrics = _object(
        negative_report.get("metrics"),
        "negative metrics",
    )
    if (
        summary.negative.status != negative_report.get("status")
        or summary.negative.pair_count != negative_metrics.get("pair_count")
        or summary.negative.adaptive_unsafe_count
        != negative_metrics.get("adaptive_unsafe_count")
        or summary.negative.frozen_unsafe_count
        != negative_metrics.get("frozen_unsafe_count")
        or summary.negative.safety_incident_count
        != len(negative_report.get("incidents") or ())
    ):
        raise ValueError("negative assurance metrics do not match evidence")

    population = _read_json_file(summary.artifact_paths["population_evidence"])
    _assert_run_identity(
        population,
        run_id=f"{summary.run_id}:population",
        system_version=summary.system_version,
    )
    if str(population.get("evidence_digest") or "") != (
        summary.population.component_digests.get("evidence")
        if summary.population is not None
        else None
    ):
        raise ValueError("population evidence digest mismatch")
    if canonical_sha256(
        {key: value for key, value in population.items() if key != "evidence_digest"}
    ) != population.get("evidence_digest"):
        raise ValueError("population evidence failed digest recomputation")
    if str(population.get("registry_population_digest") or "") != (
        summary.population.component_digests.get("registry")
        if summary.population is not None
        else None
    ):
        raise ValueError("population registry digest mismatch")
    if canonical_sha256(population.get("execution_population")) != (
        population.get("registry_population_digest")
    ):
        raise ValueError("population registry failed digest recomputation")
    if canonical_sha256(population.get("population_report")) != (
        summary.population.component_digests.get("report")
        if summary.population is not None
        else None
    ):
        raise ValueError("population report digest mismatch")
    if summary.population is None:
        raise ValueError("population assurance component is required")
    population_report = _object(
        population.get("population_report"),
        "population report",
    )
    if (
        summary.population.registry_pair_count != population_report.get("pair_count")
        or summary.population.observed_pair_count
        != population_report.get("observed_pair_count")
        or summary.population.unsupported_case_count
        != population_report.get("unsupported_case_count")
        or summary.population.unsupported_strata_counts
        != population_report.get("unsupported_strata_counts")
        or summary.population.unsupported_reason_counts
        != population_report.get("unsupported_reason_counts")
    ):
        raise ValueError("population assurance does not match evidence")
    population_incidents = len(
        _object(
            population.get("experiment_report"),
            "population experiment report",
        ).get("incidents")
        or ()
    )
    if _population_safety_incident_count(summary.population) != population_incidents:
        raise ValueError("population safety incidents do not match evidence")
    _validate_population_evidence(
        population,
        assurance=summary.population,
    )

    validate_correction_assurance_component(
        summary.correction,
        run_id=f"{summary.run_id}:correction",
        system_version=summary.system_version,
    )
    validate_variant_population_assurance_component(
        summary.variant_population,
        run_id=f"{summary.run_id}:variant",
        system_version=summary.system_version,
    )
    validate_variant_collision_assurance_component(
        summary.variant_collision,
        run_id=f"{summary.run_id}:collision",
        system_version=summary.system_version,
    )
    validate_customer_lifecycle_assurance_component(
        summary.customer_lifecycle,
        run_id=f"{summary.run_id}:customer-lifecycle",
        system_version=summary.system_version,
    )
    validate_active_surfaces_assurance_component(
        summary.active_surfaces,
        run_id=f"{summary.run_id}:active-surfaces",
        system_version=summary.system_version,
    )
    validate_retention_assurance_component(
        summary.retention,
        run_id=f"{summary.run_id}:retention",
        system_version=summary.system_version,
    )
    validate_canonical_replacement_assurance_component(
        summary.canonical_replacement,
        run_id=f"{summary.run_id}:canonical-replacement",
        system_version=summary.system_version,
    )
    validate_source_binding_lifecycle_assurance_component(
        summary.source_binding_lifecycle,
        run_id=f"{summary.run_id}:source-binding-lifecycle",
        system_version=summary.system_version,
    )

    slack_envelope = _read_json_file(summary.artifact_paths["slack_report"])
    slack_report = _object(
        slack_envelope.get("report"),
        "Slack report",
    )
    typed_slack_report = SlackReconstructionReport.model_validate(slack_report)
    _assert_run_identity(
        slack_report,
        run_id=f"{summary.run_id}:slack",
        system_version=summary.system_version,
    )
    if str(slack_envelope.get("report_digest") or "") != (
        summary.slack.component_digests.get("report")
    ):
        raise ValueError("Slack report digest mismatch")
    if canonical_sha256(slack_report) != slack_envelope.get("report_digest"):
        raise ValueError("Slack report failed digest recomputation")
    if str(slack_report.get("gold_manifest_digest") or "") != (
        summary.slack.component_digests.get("gold_manifest")
    ):
        raise ValueError("Slack gold digest mismatch")
    if str(slack_report.get("observation_digest") or "") != (
        summary.slack.component_digests.get("observations")
    ):
        raise ValueError("Slack report and observations are not cross-bound")
    if summary.slack.status != slack_report.get(
        "status"
    ) or summary.slack.metrics != slack_report.get("metrics"):
        raise ValueError("Slack assurance does not match evidence")
    expected_scope_complete = bool(
        typed_slack_report.status == "observed"
        and typed_slack_report.metrics.case_count > 0
        and typed_slack_report.metrics.supported_case_count
        == typed_slack_report.metrics.case_count
        and set(typed_slack_report.metrics.family_metrics)
        == {family.value for family in SlackGoldFamily}
    )
    if summary.slack.evidence_tier is not EvidenceTier.E4:
        raise ValueError("Slack synthetic gold must declare E4 evidence")
    if summary.slack.scope_complete != expected_scope_complete:
        raise ValueError("Slack scope completeness does not match evidence")
    if summary.slack.open_world_complete:
        raise ValueError("Slack synthetic gold cannot claim open-world completeness")
    observations = _read_jsonl_file(summary.artifact_paths["slack_observations"])
    if canonical_sha256(observations) != (
        summary.slack.component_digests.get("observations")
    ):
        raise ValueError("Slack observations digest mismatch")


def _population_safety_incident_count(
    population: PopulationAssurance,
) -> int:
    for key in (
        "safety_incident_count",
        "hard_safety_incident_count",
    ):
        value = population.metrics.get(key)
        if isinstance(value, int):
            return value
    return 0


def _metric_values_match(observed: Any, expected: Any) -> bool:
    if isinstance(observed, float) or isinstance(expected, float):
        if observed is None or expected is None:
            return observed is expected
        return abs(float(observed) - float(expected)) <= 1e-12
    return observed == expected


def _read_json_file(path_value: str) -> dict[str, Any]:
    path = Path(path_value)
    if not path.is_file():
        raise ValueError(f"required assurance component is missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"assurance component must be an object: {path}")
    return payload


def _read_jsonl_file(path_value: str) -> list[dict[str, Any]]:
    path = Path(path_value)
    if not path.is_file():
        raise ValueError(f"required assurance component is missing: {path}")
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not all(isinstance(row, dict) for row in rows):
        raise ValueError(f"assurance JSONL rows must be objects: {path}")
    return rows


def _assert_run_identity(
    payload: Any,
    *,
    run_id: str,
    system_version: str,
) -> None:
    if not isinstance(payload, dict):
        raise ValueError("assurance component identity payload is missing")
    if payload.get("run_id") != run_id:
        raise ValueError("assurance component run identity mismatch")
    if payload.get("system_version") != system_version:
        raise ValueError("assurance component system version mismatch")


def _assert_architecture_identity(
    payload: Any,
    *,
    architecture_digest: str,
    label: str,
) -> None:
    if not isinstance(payload, dict):
        raise ValueError(f"{label} identity payload is missing")
    if payload.get("architecture_digest") != architecture_digest:
        raise ValueError(f"{label} architecture digest mismatch")


def _assert_positive_evaluation_identity(
    payload: dict[str, Any],
    *,
    run_id: str,
    system_version: str,
    architecture_digest: str,
) -> None:
    state_scope = _object(
        _object(payload.get("state"), "positive evaluation state").get("scope"),
        "positive evaluation scope",
    )
    if state_scope.get("run_id") != run_id:
        raise ValueError("positive evaluation run identity mismatch")
    for field in ("evidence_manifest", "evidence_bundle"):
        identity = _object(payload.get(field), f"positive {field}")
        _assert_run_identity(
            identity,
            run_id=run_id,
            system_version=system_version,
        )
        _assert_architecture_identity(
            identity,
            architecture_digest=architecture_digest,
            label=f"positive {field}",
        )


def _validate_population_evidence(
    payload: dict[str, Any],
    *,
    assurance: PopulationAssurance,
) -> None:
    population = HeldOutExactAliasPopulation.model_validate(
        payload.get("execution_population")
    )
    case_ids = tuple(case.case_id for case in population.cases)
    if len(case_ids) != 60 or tuple(payload.get("selected_case_ids") or ()) != (
        case_ids
    ):
        raise ValueError("population evidence does not cover the sealed registry")

    assignments = payload.get("assignments")
    if not isinstance(assignments, list):
        raise ValueError("population assignments are missing")
    assignment_ids = [str(row.get("case_id")) for row in assignments]
    if len(assignment_ids) != len(set(assignment_ids)) or set(assignment_ids) != set(
        case_ids
    ):
        raise ValueError("population assignments do not exactly cover cases")
    tenant_ids = [
        str(row.get(key))
        for row in assignments
        for key in ("adaptive_tenant_id", "frozen_tenant_id")
    ]
    if len(tenant_ids) != len(set(tenant_ids)):
        raise ValueError("population arm tenants are not unique")

    observations = tuple(
        HeldOutPairObservation.model_validate(row)
        for row in payload.get("observations") or ()
    )
    raw_pairs = tuple(
        PairedRecurrenceResult.model_validate(row)
        for row in payload.get("raw_pairs") or ()
    )
    observed_ids = {
        row.case_id for row in observations if row.execution_status == "observed"
    }
    if {pair.case_id for pair in raw_pairs} != observed_ids:
        raise ValueError("population raw pairs do not cover observed cases")

    experiment = CorrectiveMemoryExperimentReport.model_validate(
        payload.get("experiment_report")
    )
    if experiment.pairs != raw_pairs:
        raise ValueError("population experiment pairs do not match raw pairs")
    assessment = {(row.case_id, row.arm): row for row in experiment.assessments}
    pair_by_case = {pair.case_id: pair for pair in raw_pairs}
    for observation in observations:
        if observation.execution_status != "observed":
            continue
        pair = pair_by_case[observation.case_id]
        adaptive = assessment[(observation.case_id, pair.adaptive.arm)]
        frozen = assessment[(observation.case_id, pair.frozen.arm)]
        expected = {
            "adaptive_correct": adaptive.correct,
            "frozen_correct": frozen.correct,
            "adaptive_unsafe": bool(adaptive.incident_classes),
            "frozen_unsafe": bool(frozen.incident_classes),
            "adaptive_llm_calls": pair.adaptive.llm_call_count,
            "frozen_llm_calls": pair.frozen.llm_call_count,
            "adaptive_latency_ms": pair.adaptive.latency_ms,
            "frozen_latency_ms": pair.frozen.latency_ms,
        }
        if any(getattr(observation, key) != value for key, value in expected.items()):
            raise ValueError(
                "population observations do not match raw pair assessments"
            )

    method = str(
        _object(
            payload.get("population_report"),
            "population report",
        )
        .get("adaptive_minus_frozen_correctness", {})
        .get("method", "")
    )
    try:
        bootstrap_samples = int(method.rsplit("_", 1)[1])
    except (IndexError, ValueError) as exc:
        raise ValueError("population bootstrap method is invalid") from exc
    recomputed = evaluate_heldout_population(
        population=population,
        observations=observations,
        bootstrap_samples=bootstrap_samples,
    )
    if recomputed.model_dump(mode="json") != payload.get("population_report"):
        raise ValueError("population report does not match recomputation")
    if (
        assurance.registry_pair_count != recomputed.pair_count
        or assurance.observed_pair_count != recomputed.observed_pair_count
        or assurance.unsupported_case_count != recomputed.unsupported_case_count
    ):
        raise ValueError("population assurance does not match recomputation")


def _object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value
