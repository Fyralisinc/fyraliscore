"""Preregistered continuous readiness budgets for company-entity physics.

This evaluator never converts absent exposure into success.  Continuous metric
measurements are reported independently from constitutional blockers so a high
average cannot compensate for unsafe identity or topology behavior.
"""

from __future__ import annotations

from typing import Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.evaluation.entity_extraction_gold import (
    EntityExtractionMetrics,
    GoldEntityExtractionReport,
)
from lib.evaluation.entity_pipeline_gold import GoldEntityPipelineReport


class _Record(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EntityReadinessThresholds(_Record):
    """Versioned defaults; callers may preregister stricter/lower budgets."""

    policy_version: Literal["entity-readiness-budget-v1"] = (
        "entity-readiness-budget-v1"
    )
    min_overall_span_f1: float = Field(default=0.90, ge=0, le=1)
    min_per_type_span_f1: float = Field(default=0.80, ge=0, le=1)
    min_extraction_type_accuracy: float = Field(default=0.95, ge=0, le=1)
    min_pipeline_type_assessment_accuracy: float = Field(default=0.95, ge=0, le=1)
    min_negative_cleanliness: float = Field(default=0.98, ge=0, le=1)
    min_candidate_population_coverage: float = Field(default=0.95, ge=0, le=1)
    min_candidate_recall_at_3: float = Field(default=0.95, ge=0, le=1)
    min_canonical_link_coverage: float = Field(default=0.90, ge=0, le=1)
    min_canonical_link_accuracy: float = Field(default=0.99, ge=0, le=1)
    min_detection_to_terminal_coverage: float = Field(default=0.99, ge=0, le=1)
    min_lineage_integrity: float = Field(default=0.99, ge=0, le=1)
    min_semantic_disposition_accuracy: float = Field(default=0.98, ge=0, le=1)
    min_no_admission_safety: float = Field(default=0.99, ge=0, le=1)
    min_relation_endpoint_accuracy: float = Field(default=0.99, ge=0, le=1)
    min_relation_type_accuracy: float = Field(default=0.99, ge=0, le=1)
    min_relation_direction_accuracy: float = Field(default=0.99, ge=0, le=1)
    min_relation_lineage_coverage: float = Field(default=0.99, ge=0, le=1)
    min_relation_lineage_integrity: float = Field(default=0.99, ge=0, le=1)
    max_harmful_false_link_rate: float = Field(default=0.001, ge=0, le=1)
    max_harmful_semantic_propagation_rate: float = Field(
        default=0.001, ge=0, le=1
    )
    max_harmful_topology_propagation_rate: float = Field(
        default=0.001, ge=0, le=1
    )
    max_unlineaged_active_relation_rate: float = Field(default=0.0, ge=0, le=1)


class ExactRatePopulation(_Record):
    numerator: int = Field(ge=0)
    denominator: int = Field(ge=0)

    @model_validator(mode="after")
    def numerator_within_population(self) -> "ExactRatePopulation":
        if self.numerator > self.denominator:
            raise ValueError("numerator cannot exceed denominator")
        return self

    @property
    def rate(self) -> float | None:
        return self.numerator / self.denominator if self.denominator else None


class EntityReadinessEvidence(_Record):
    """Evaluator-owned facts absent from the two report schemas."""

    per_type_extraction: Mapping[str, EntityExtractionMetrics] = Field(
        default_factory=dict
    )
    negative_cleanliness: ExactRatePopulation | None = None
    exact_rate_populations: Mapping[str, ExactRatePopulation] = Field(
        default_factory=dict
    )
    cross_tenant_identity_incidents: int | None = Field(default=None, ge=0)
    untraceable_canonical_assignments: int | None = Field(default=None, ge=0)
    known_wrong_type_consequential_admissions: int | None = Field(
        default=None, ge=0
    )


class ReadinessMeasurement(_Record):
    name: str
    component: str
    value: float | None = Field(default=None, ge=0, le=1)
    threshold: float
    direction: Literal["minimum", "maximum"]
    status: Literal["meets", "below_budget", "unknown"]
    continuous_score: float | None = Field(default=None, ge=0, le=1)
    numerator: int | None = Field(default=None, ge=0)
    denominator: int | None = Field(default=None, ge=0)
    denominator_source: str | None = None
    exact_denominators: Mapping[str, int] = Field(default_factory=dict)


class ReadinessBlocker(_Record):
    code: str
    observed_count: int | None = Field(default=None, ge=0)
    status: Literal["clear", "triggered", "unknown"]
    source: str


class EntityReadinessReport(_Record):
    schema_version: Literal["entity-readiness-report-v1"] = (
        "entity-readiness-report-v1"
    )
    threshold_policy_version: str
    thresholds: EntityReadinessThresholds
    measurements: tuple[ReadinessMeasurement, ...]
    component_scores: Mapping[str, float | None]
    component_coverage: Mapping[str, float]
    coverage_gaps: tuple[str, ...]
    blockers: tuple[ReadinessBlocker, ...]
    blocker_verdict: Literal["clear", "blocked", "unknown"]
    continuous_overall_score: float | None = Field(default=None, ge=0, le=1)


def evaluate_entity_readiness(
    *,
    extraction: GoldEntityExtractionReport,
    pipeline: GoldEntityPipelineReport,
    evidence: EntityReadinessEvidence | None = None,
    thresholds: EntityReadinessThresholds | None = None,
) -> EntityReadinessReport:
    """Evaluate continuous readiness without allowing scores to clear blockers."""

    evidence = evidence or EntityReadinessEvidence()
    thresholds = thresholds or EntityReadinessThresholds()
    if extraction.schema_version != "gold-entity-extraction-v1":
        raise ValueError("entity readiness requires gold-entity-extraction-v1")
    if pipeline.schema_version != "gold-entity-pipeline-v4":
        raise ValueError("entity readiness requires gold-entity-pipeline-v4")
    metrics = pipeline.overall
    measurements: list[ReadinessMeasurement] = []

    def add(
        name: str,
        component: str,
        value: float | None,
        threshold: float,
        direction: Literal["minimum", "maximum"],
        *,
        numerator: int | None = None,
        denominator: int | None = None,
        denominator_source: str | None = None,
        exact_denominators: Mapping[str, int] | None = None,
        require_population: bool = False,
    ) -> None:
        if require_population and (denominator is None or denominator <= 0):
            value = None
        if value is None:
            status, score = "unknown", None
        else:
            meets = value >= threshold if direction == "minimum" else value <= threshold
            status = "meets" if meets else "below_budget"
            if direction == "minimum":
                score = min(1.0, value / threshold) if threshold else 1.0
            else:
                score = 1.0 if value <= threshold else (
                    max(0.0, 1.0 - value) if threshold == 0 else threshold / value
                )
        measurements.append(ReadinessMeasurement(
            name=name, component=component, value=value, threshold=threshold,
            direction=direction, status=status, continuous_score=score,
            numerator=numerator, denominator=denominator,
            denominator_source=denominator_source,
            exact_denominators=exact_denominators or {},
        ))

    overall = extraction.overall
    add(
        "extraction.overall_span_f1", "extraction", overall.span_f1,
        thresholds.min_overall_span_f1, "minimum",
        numerator=overall.exact_match_count, denominator=overall.gold_count,
        denominator_source="gold_count; prediction_count also reported in extraction",
        exact_denominators={
            "gold_count": overall.gold_count,
            "prediction_count": overall.prediction_count,
        },
        require_population=True,
    )
    add(
        "extraction.type_accuracy", "extraction", overall.type_accuracy,
        thresholds.min_extraction_type_accuracy, "minimum",
        numerator=(
            round(overall.type_accuracy * overall.matched_count)
            if overall.type_accuracy is not None else None
        ),
        denominator=overall.matched_count, denominator_source="matched_count",
        require_population=True,
    )
    negative = evidence.negative_cleanliness
    add(
        "extraction.negative_cleanliness", "extraction",
        negative.rate if negative else None, thresholds.min_negative_cleanliness,
        "minimum", numerator=negative.numerator if negative else None,
        denominator=negative.denominator if negative else None,
        denominator_source="evaluator_owned_negative_population" if negative else None,
        require_population=True,
    )
    for entity_type in sorted(evidence.per_type_extraction):
        item = evidence.per_type_extraction[entity_type]
        add(
            f"extraction.per_type.{entity_type}.span_f1", "extraction_per_type",
            item.span_f1, thresholds.min_per_type_span_f1, "minimum",
            numerator=item.exact_match_count, denominator=item.gold_count,
            denominator_source="per_type_gold_count",
            exact_denominators={
                "gold_count": item.gold_count,
                "prediction_count": item.prediction_count,
            }, require_population=True,
        )
    if not evidence.per_type_extraction:
        add(
            "extraction.per_type.population", "extraction_per_type", None,
            thresholds.min_per_type_span_f1, "minimum", require_population=True,
        )

    def supplemental(name: str) -> ExactRatePopulation | None:
        return evidence.exact_rate_populations.get(name)

    add(
        "pipeline.candidate_population_coverage", "linking",
        metrics.candidate_population_coverage,
        thresholds.min_candidate_population_coverage, "minimum",
        numerator=metrics.candidate_population_count,
        denominator=metrics.detected_case_count,
        denominator_source="detected_case_count", require_population=True,
    )
    add(
        "pipeline.type_assessment_accuracy", "typing",
        metrics.type_assessment_accuracy,
        thresholds.min_pipeline_type_assessment_accuracy, "minimum",
        numerator=(round(metrics.type_assessment_accuracy * metrics.type_assessed_case_count)
                   if metrics.type_assessment_accuracy is not None else None),
        denominator=metrics.type_assessed_case_count,
        denominator_source="type_assessed_case_count", require_population=True,
    )
    candidate = supplemental("pipeline.candidate_recall_at_3")
    add(
        "pipeline.candidate_recall_at_3", "linking",
        metrics.candidate_recall_at_k.get(3), thresholds.min_candidate_recall_at_3,
        "minimum", numerator=candidate.numerator if candidate else None,
        denominator=candidate.denominator if candidate else None,
        denominator_source="evaluator_owned_linkable_gold" if candidate else None,
        require_population=True,
    )
    for name, value, threshold in (
        ("pipeline.canonical_link_coverage", metrics.canonical_link_coverage,
         thresholds.min_canonical_link_coverage),
        ("pipeline.canonical_link_accuracy", metrics.canonical_link_accuracy,
         thresholds.min_canonical_link_accuracy),
    ):
        population = supplemental(name)
        add(name, "linking", value, threshold, "minimum",
            numerator=population.numerator if population else None,
            denominator=population.denominator if population else None,
            denominator_source="evaluator_owned_canonical_gold" if population else None,
            require_population=True)

    add("pipeline.detection_to_terminal_coverage", "lineage",
        metrics.detection_to_terminal_coverage,
        thresholds.min_detection_to_terminal_coverage, "minimum",
        numerator=metrics.terminal_case_count, denominator=metrics.gold_case_count,
        denominator_source="gold_case_count", require_population=True)
    add("pipeline.lineage_integrity", "lineage", metrics.lineage_integrity,
        thresholds.min_lineage_integrity, "minimum",
        numerator=(round(metrics.lineage_integrity * metrics.gold_case_count)
                   if metrics.lineage_integrity is not None else None),
        denominator=metrics.gold_case_count, denominator_source="gold_case_count",
        require_population=True)
    add("pipeline.semantic_disposition_accuracy", "semantic_safety",
        metrics.semantic_disposition_accuracy,
        thresholds.min_semantic_disposition_accuracy, "minimum",
        numerator=(
            round(
                metrics.semantic_disposition_accuracy
                * metrics.semantic_expected_case_count
            )
            if metrics.semantic_disposition_accuracy is not None else None
        ),
        denominator=metrics.semantic_expected_case_count,
        denominator_source="semantic_expected_case_count", require_population=True)
    for name, value, threshold, direction in (
        ("pipeline.no_admission_no_model_safety_rate",
         metrics.no_admission_no_model_safety_rate,
         thresholds.min_no_admission_safety, "minimum"),
        ("pipeline.harmful_semantic_propagation_rate",
         metrics.harmful_semantic_propagation_rate,
         thresholds.max_harmful_semantic_propagation_rate, "maximum"),
    ):
        population = supplemental(name)
        add(name, "semantic_safety", value, threshold, direction,
            numerator=population.numerator if population else None,
            denominator=population.denominator if population else None,
            denominator_source="evaluator_owned_semantic_population" if population else None,
            require_population=True)

    relation_denominator = metrics.expected_relation_admission_count
    for name, value, threshold in (
        ("pipeline.relation_endpoint_accuracy", metrics.relation_endpoint_accuracy,
         thresholds.min_relation_endpoint_accuracy),
        ("pipeline.relation_type_accuracy", metrics.relation_type_accuracy,
         thresholds.min_relation_type_accuracy),
        ("pipeline.relation_direction_accuracy", metrics.relation_direction_accuracy,
         thresholds.min_relation_direction_accuracy),
        ("pipeline.relation_lineage_coverage", metrics.relation_lineage_coverage,
         thresholds.min_relation_lineage_coverage),
    ):
        add(name, "topology", value, threshold, "minimum",
            numerator=(round(value * relation_denominator) if value is not None else None),
            denominator=relation_denominator,
            denominator_source="expected_relation_admission_count",
            require_population=True)
    relation_lineage = supplemental("pipeline.relation_lineage_integrity")
    add(
        "pipeline.relation_lineage_integrity", "topology",
        metrics.relation_lineage_integrity,
        thresholds.min_relation_lineage_integrity, "minimum",
        numerator=relation_lineage.numerator if relation_lineage else None,
        denominator=relation_lineage.denominator if relation_lineage else None,
        denominator_source="evaluator_owned_exact_admitted_relations"
        if relation_lineage else None,
        require_population=True,
    )

    for name, value, threshold, numerator in (
        ("pipeline.harmful_false_link_rate", metrics.harmful_false_link_rate,
         thresholds.max_harmful_false_link_rate, None),
        ("pipeline.harmful_topology_propagation_rate",
         metrics.harmful_topology_propagation_rate,
         thresholds.max_harmful_topology_propagation_rate,
         metrics.harmful_topology_relation_count),
        ("pipeline.unlineaged_active_relation_rate",
         metrics.unlineaged_active_relation_rate,
         thresholds.max_unlineaged_active_relation_rate,
         metrics.unlineaged_active_relation_count),
    ):
        denominator = (
            metrics.detected_case_count if name.endswith("false_link_rate")
            else metrics.observed_active_relation_count
        )
        derived_numerator = numerator
        if derived_numerator is None and value is not None:
            derived_numerator = round(value * denominator)
        add(name, "safety", value, threshold, "maximum",
            numerator=derived_numerator, denominator=denominator,
            denominator_source=("detected_case_count" if name.endswith("false_link_rate")
                                else "observed_active_relation_count"),
            require_population=True)

    blockers = (
        _incident_blocker("cross_tenant_identity", evidence.cross_tenant_identity_incidents,
                          "evaluator_owned_incident_count"),
        _incident_blocker("untraceable_canonical_assignment",
                          evidence.untraceable_canonical_assignments,
                          "evaluator_owned_incident_count"),
        _incident_blocker("known_wrong_type_consequential_admission",
                          evidence.known_wrong_type_consequential_admissions,
                          "evaluator_owned_incident_count"),
        _rate_blocker("harmful_false_link", metrics.harmful_false_link_rate,
                      metrics.detected_case_count),
        _rate_blocker("harmful_topology_propagation",
                      metrics.harmful_topology_propagation_rate,
                      metrics.observed_active_relation_count),
        ReadinessBlocker(
            code="unlineaged_active_edge",
            observed_count=(metrics.unlineaged_active_relation_count
                            if metrics.observed_active_relation_count > 0 else None),
            status=("unknown" if metrics.observed_active_relation_count == 0
                    else "triggered" if metrics.unlineaged_active_relation_count > 0
                    else "clear"),
            source="entity_pipeline_v4.active_relations",
        ),
    )
    blocker_verdict: Literal["clear", "blocked", "unknown"] = (
        "blocked" if any(item.status == "triggered" for item in blockers)
        else "unknown" if any(item.status == "unknown" for item in blockers)
        else "clear"
    )
    components = sorted({item.component for item in measurements})
    component_scores = {
        component: _mean([
            item.continuous_score for item in measurements
            if item.component == component and item.continuous_score is not None
        ]) for component in components
    }
    component_coverage = {
        component: sum(
            item.status != "unknown" for item in measurements
            if item.component == component
        ) / sum(item.component == component for item in measurements)
        for component in components
    }
    coverage_gaps = tuple(item.name for item in measurements if item.status == "unknown")
    known_scores = [item.continuous_score for item in measurements
                    if item.continuous_score is not None]
    return EntityReadinessReport(
        threshold_policy_version=thresholds.policy_version,
        thresholds=thresholds,
        measurements=tuple(measurements), component_scores=component_scores,
        component_coverage=component_coverage, coverage_gaps=coverage_gaps,
        blockers=blockers, blocker_verdict=blocker_verdict,
        continuous_overall_score=_mean(known_scores),
    )


def _incident_blocker(code: str, count: int | None, source: str) -> ReadinessBlocker:
    return ReadinessBlocker(
        code=code, observed_count=count,
        status="unknown" if count is None else "triggered" if count else "clear",
        source=source,
    )


def _rate_blocker(code: str, rate: float | None, denominator: int) -> ReadinessBlocker:
    count = None if rate is None or denominator <= 0 else round(rate * denominator)
    return ReadinessBlocker(
        code=code, observed_count=count,
        status="unknown" if count is None else "triggered" if count else "clear",
        source="entity_pipeline_v4",
    )


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


__all__ = [
    "EntityReadinessEvidence", "EntityReadinessReport",
    "EntityReadinessThresholds", "ExactRatePopulation",
    "ReadinessBlocker", "ReadinessMeasurement", "evaluate_entity_readiness",
]
