"""Sealed continuous evaluation for company-learning retention and forgetting."""

from __future__ import annotations

from collections import defaultdict
from enum import StrEnum
from statistics import fmean
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_experiment import (
    CanonicalEntityRef,
    ConsumerTerminalFate,
    HardSafetyIncidentClass,
)


class _RetentionModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class RetentionBehavior(StrEnum):
    EXACT_ALIAS = "exact_alias"
    VARIANT_ALIAS = "variant_alias"
    CORRECTED_ALIAS = "corrected_alias"
    NEGATIVE_CONTROL = "negative_control"
    COLLISION_CONTROL = "collision_control"


class RetentionHorizon(_RetentionModel):
    cycle_count: int = Field(ge=0)
    restart_count: int = Field(ge=0)

    @model_validator(mode="after")
    def restart_is_bounded_by_cycles(self) -> Self:
        if self.restart_count > self.cycle_count + 1:
            raise ValueError("restart count cannot exceed cycle count plus one")
        return self


class RetentionCaseSpec(_RetentionModel):
    case_id: str = Field(min_length=1)
    behavior: RetentionBehavior
    family: str = Field(min_length=1)
    expected_ref: CanonicalEntityRef | None = None
    horizons: tuple[RetentionHorizon, ...] = Field(min_length=1)
    allowed_terminal_fates: tuple[ConsumerTerminalFate, ...] = Field(
        min_length=1
    )
    candidate_authorization_required: bool = False
    correction_authority_required: bool = False

    @model_validator(mode="after")
    def coherent_expectation(self) -> Self:
        if len(self.horizons) != len(
            {(row.cycle_count, row.restart_count) for row in self.horizons}
        ):
            raise ValueError("case horizons must be unique")
        if len(self.allowed_terminal_fates) != len(set(self.allowed_terminal_fates)):
            raise ValueError("allowed terminal fates must be unique")
        positive = self.behavior in {
            RetentionBehavior.EXACT_ALIAS,
            RetentionBehavior.VARIANT_ALIAS,
            RetentionBehavior.CORRECTED_ALIAS,
        }
        if positive and self.expected_ref is None:
            raise ValueError("positive retention cases require an expected ref")
        if not positive and self.expected_ref is not None:
            raise ValueError("negative and collision cases seal no resolved ref")
        if positive and (
            ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
            not in self.allowed_terminal_fates
        ):
            raise ValueError("positive retention cases must allow resolution")
        if (
            self.candidate_authorization_required
            and self.behavior is not RetentionBehavior.VARIANT_ALIAS
        ):
            raise ValueError(
                "candidate authorization is specific to variant retention"
            )
        if (
            self.correction_authority_required
            and self.behavior is not RetentionBehavior.CORRECTED_ALIAS
        ):
            raise ValueError(
                "correction authority is specific to corrected aliases"
            )
        return self


class RetentionRunSpec(_RetentionModel):
    schema_version: Literal["company-learning-retention-spec-v1"] = (
        "company-learning-retention-spec-v1"
    )
    run_id: str = Field(min_length=1)
    system_version: str = Field(min_length=1)
    created_at: str = Field(min_length=1)
    cases: tuple[RetentionCaseSpec, ...] = Field(min_length=1)
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def sealed_case_population(self) -> Self:
        if len(self.cases) != len({case.case_id for case in self.cases}):
            raise ValueError("retention case IDs must be unique")
        by_behavior: dict[RetentionBehavior, list[RetentionCaseSpec]] = defaultdict(list)
        for case in self.cases:
            by_behavior[case.behavior].append(case)
        for behavior in (
            RetentionBehavior.EXACT_ALIAS,
            RetentionBehavior.VARIANT_ALIAS,
            RetentionBehavior.CORRECTED_ALIAS,
            RetentionBehavior.NEGATIVE_CONTROL,
            RetentionBehavior.COLLISION_CONTROL,
        ):
            if behavior not in by_behavior:
                raise ValueError(f"retention spec is missing {behavior.value}")
        for behavior in (
            RetentionBehavior.EXACT_ALIAS,
            RetentionBehavior.VARIANT_ALIAS,
        ):
            if any(len(case.horizons) != 3 for case in by_behavior[behavior]):
                raise ValueError(
                    f"{behavior.value} cases require exactly three horizons"
                )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class RetentionObservation(_RetentionModel):
    case_id: str = Field(min_length=1)
    horizon: RetentionHorizon
    intervening_learning_count: int = Field(ge=0)
    consumer_fate: ConsumerTerminalFate
    observed_ref: CanonicalEntityRef | None = None
    candidate_authorized: bool | None = None
    correction_authoritative: bool | None = None
    unsafe_globalization: bool = False
    source_observation_immutable: bool
    models_consistent: bool
    evidence_lineage_consistent: bool
    observed_safety_incidents: frozenset[HardSafetyIncidentClass] = frozenset()
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def learning_count_matches_horizon(self) -> Self:
        if self.intervening_learning_count != self.horizon.cycle_count:
            raise ValueError(
                "intervening learning count must equal the sealed cycle horizon"
            )
        return self


class RetentionHorizonMetrics(_RetentionModel):
    cycle_count: int = Field(ge=0)
    restart_count: int = Field(ge=0)
    observed_count: int = Field(ge=0)
    positive_retention_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    forgetting_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    negative_safety_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    collision_safety_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    source_immutability_rate: float = Field(ge=0.0, le=1.0)
    model_consistency_rate: float = Field(ge=0.0, le=1.0)
    evidence_lineage_consistency_rate: float = Field(ge=0.0, le=1.0)


class CompanyLearningRetentionReport(_RetentionModel):
    schema_version: Literal["company-learning-retention-report-v1"] = (
        "company-learning-retention-report-v1"
    )
    status: Literal["observed", "observed_with_degradation", "contradicted"]
    spec_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    observation_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
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
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


def evaluate_company_learning_retention(
    *,
    spec: RetentionRunSpec,
    observations: tuple[RetentionObservation, ...],
    artifact_refs: tuple[str, ...],
) -> CompanyLearningRetentionReport:
    """Evaluate every sealed case/horizon exactly once."""

    expected = {
        (case.case_id, horizon.cycle_count, horizon.restart_count): case
        for case in spec.cases
        for horizon in case.horizons
    }
    observed = {
        (
            row.case_id,
            row.horizon.cycle_count,
            row.horizon.restart_count,
        ): row
        for row in observations
    }
    if len(observed) != len(observations):
        raise ValueError("retention observations must be unique by case and horizon")
    if set(observed) != set(expected):
        missing = sorted(set(expected) - set(observed))
        unexpected = sorted(set(observed) - set(expected))
        raise ValueError(
            "retention observations must exactly cover the sealed spec: "
            f"missing={missing}, unexpected={unexpected}"
        )

    assessments = [
        _assess(case=expected[key], observation=observation)
        for key, observation in observed.items()
    ]
    by_behavior: dict[RetentionBehavior, list[dict[str, object]]] = defaultdict(list)
    by_horizon: dict[tuple[int, int], list[dict[str, object]]] = defaultdict(list)
    for assessment in assessments:
        behavior = assessment["case"].behavior
        horizon = assessment["observation"].horizon
        by_behavior[behavior].append(assessment)
        by_horizon[(horizon.cycle_count, horizon.restart_count)].append(assessment)

    positive_behaviors = {
        RetentionBehavior.EXACT_ALIAS,
        RetentionBehavior.VARIANT_ALIAS,
        RetentionBehavior.CORRECTED_ALIAS,
    }
    positive = [
        row for row in assessments if row["case"].behavior in positive_behaviors
    ]
    restart_observations = [
        row for row in positive if row["observation"].horizon.restart_count > 0
    ]
    correction = by_behavior[RetentionBehavior.CORRECTED_ALIAS]
    hard_incident_count = sum(
        bool(row["observation"].observed_safety_incidents) for row in assessments
    )
    unsafe_globalization_count = sum(
        row["observation"].unsafe_globalization for row in assessments
    )
    horizon_metrics = tuple(
        _horizon_metrics(cycle_count=key[0], restart_count=key[1], rows=rows)
        for key, rows in sorted(by_horizon.items())
    )
    retention_auc = fmean(
        row.positive_retention_rate
        for row in horizon_metrics
        if row.positive_retention_rate is not None
    )
    overall_retention = _rate(positive, "correct")
    consistency_failure = any(
        not bool(row[key])
        for row in assessments
        for key in (
            "source_immutable",
            "models_consistent",
            "evidence_lineage_consistent",
        )
    )
    contradicted = bool(
        hard_incident_count
        or unsafe_globalization_count
        or consistency_failure
    )
    degraded = any(not bool(row["correct"]) for row in assessments)
    status: Literal["observed", "observed_with_degradation", "contradicted"] = (
        "contradicted"
        if contradicted
        else "observed_with_degradation"
        if degraded
        else "observed"
    )
    return CompanyLearningRetentionReport(
        status=status,
        spec_digest=spec.digest,
        observation_digest=canonical_sha256(
            [row.model_dump(mode="json") for row in observations]
        ),
        expected_observation_count=len(expected),
        observed_observation_count=len(observations),
        exact_retention_rate=_rate(
            by_behavior[RetentionBehavior.EXACT_ALIAS],
            "correct",
        ),
        variant_retention_rate=_rate(
            by_behavior[RetentionBehavior.VARIANT_ALIAS],
            "correct",
        ),
        corrected_retention_rate=_rate(correction, "correct"),
        overall_positive_retention_rate=overall_retention,
        overall_forgetting_rate=1.0 - overall_retention,
        restart_survival_rate=_rate(restart_observations, "correct"),
        correction_authority_rate=_rate(correction, "correction_authoritative"),
        unsafe_globalization_rate=unsafe_globalization_count / len(assessments),
        negative_control_safety_rate=_rate(
            by_behavior[RetentionBehavior.NEGATIVE_CONTROL],
            "correct",
        ),
        collision_control_safety_rate=_rate(
            by_behavior[RetentionBehavior.COLLISION_CONTROL],
            "correct",
        ),
        source_immutability_rate=_rate(assessments, "source_immutable"),
        model_consistency_rate=_rate(assessments, "models_consistent"),
        evidence_lineage_consistency_rate=_rate(
            assessments,
            "evidence_lineage_consistent",
        ),
        hard_safety_incident_rate=hard_incident_count / len(assessments),
        retention_horizon_auc=retention_auc,
        horizon_metrics=horizon_metrics,
        family_counts=dict(
            sorted(
                (
                    family,
                    sum(case.family == family for case in spec.cases),
                )
                for family in {case.family for case in spec.cases}
            )
        ),
        artifact_refs=artifact_refs,
    )


def _assess(
    *,
    case: RetentionCaseSpec,
    observation: RetentionObservation,
) -> dict[str, object]:
    positive = case.expected_ref is not None
    correct = (
        observation.consumer_fate in case.allowed_terminal_fates
        and observation.observed_ref == case.expected_ref
        and not observation.observed_safety_incidents
        and not observation.unsafe_globalization
    )
    if case.candidate_authorization_required:
        correct = correct and observation.candidate_authorized is True
    if case.correction_authority_required:
        correct = correct and observation.correction_authoritative is True
    if not positive:
        correct = (
            observation.consumer_fate in case.allowed_terminal_fates
            and observation.observed_ref is None
            and not observation.observed_safety_incidents
            and not observation.unsafe_globalization
        )
    return {
        "case": case,
        "observation": observation,
        "correct": bool(correct),
        "correction_authoritative": (
            observation.correction_authoritative is True
        ),
        "source_immutable": observation.source_observation_immutable,
        "models_consistent": observation.models_consistent,
        "evidence_lineage_consistent": observation.evidence_lineage_consistent,
    }


def _horizon_metrics(
    *,
    cycle_count: int,
    restart_count: int,
    rows: list[dict[str, object]],
) -> RetentionHorizonMetrics:
    positive = [row for row in rows if row["case"].expected_ref is not None]
    negative = [
        row
        for row in rows
        if row["case"].behavior is RetentionBehavior.NEGATIVE_CONTROL
    ]
    collision = [
        row
        for row in rows
        if row["case"].behavior is RetentionBehavior.COLLISION_CONTROL
    ]
    positive_rate = _optional_rate(positive, "correct")
    return RetentionHorizonMetrics(
        cycle_count=cycle_count,
        restart_count=restart_count,
        observed_count=len(rows),
        positive_retention_rate=positive_rate,
        forgetting_rate=(
            1.0 - positive_rate if positive_rate is not None else None
        ),
        negative_safety_rate=_optional_rate(negative, "correct"),
        collision_safety_rate=_optional_rate(collision, "correct"),
        source_immutability_rate=_rate(rows, "source_immutable"),
        model_consistency_rate=_rate(rows, "models_consistent"),
        evidence_lineage_consistency_rate=_rate(
            rows,
            "evidence_lineage_consistent",
        ),
    )


def _rate(rows: list[dict[str, object]], key: str) -> float:
    if not rows:
        raise ValueError(f"retention metric {key} has no observations")
    return sum(bool(row[key]) for row in rows) / len(rows)


def _optional_rate(rows: list[dict[str, object]], key: str) -> float | None:
    return _rate(rows, key) if rows else None


__all__ = [
    "CompanyLearningRetentionReport",
    "RetentionBehavior",
    "RetentionCaseSpec",
    "RetentionHorizon",
    "RetentionHorizonMetrics",
    "RetentionObservation",
    "RetentionRunSpec",
    "evaluate_company_learning_retention",
]
