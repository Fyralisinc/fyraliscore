"""Focused contracts for continuous, noncompensatory entity readiness."""

from __future__ import annotations

import pytest
from pydantic_core import PydanticUndefined

from lib.evaluation.entity_extraction_gold import (
    EntityExtractionMetrics,
    GoldEntityExtractionReport,
)
from lib.evaluation.entity_pipeline_gold import (
    EntityPipelineMetrics,
    GoldEntityPipelineReport,
)
from lib.evaluation.entity_readiness import (
    EntityReadinessEvidence,
    EntityReadinessThresholds,
    ExactRatePopulation,
    evaluate_entity_readiness,
)


def _extraction(**updates) -> GoldEntityExtractionReport:
    values = dict(
        signal_count=10, batch_count=1, gold_count=10, prediction_count=10,
        exact_match_count=10, matched_count=10, span_precision=1.0,
        span_recall=1.0, span_f1=1.0, mean_boundary_iou=1.0,
        boundary_credit_precision=1.0, boundary_credit_recall=1.0,
        type_accuracy=1.0, canonical_link_accuracy=None,
        canonical_link_coverage=None, duplicate_rate=0.0,
        candidate_fate_coverage=1.0,
    )
    values.update(updates)
    metrics = EntityExtractionMetrics(**values)
    return GoldEntityExtractionReport(
        overall=metrics, by_source={}, by_slack_context={}
    )


def _pipeline(**updates) -> GoldEntityPipelineReport:
    values = {}
    for name, field in EntityPipelineMetrics.model_fields.items():
        if field.default_factory is not None:
            values[name] = field.default_factory()
        elif field.default is not PydanticUndefined:
            values[name] = field.default
        elif name in {"candidate_recall_at_k", "gold_type_present_at_k"}:
            values[name] = {1: None, 3: None, 5: None}
        elif name == "candidate_recall_hits_at_k":
            values[name] = {1: 0, 3: 0, 5: 0}
        else:
            values[name] = 0
    values.update(dict(
        gold_case_count=10, detected_case_count=10,
        candidate_population_count=10, terminal_case_count=10,
        type_assessed_case_count=10, type_assessment_accuracy=1.0,
        candidate_population_coverage=1.0,
        detection_to_terminal_coverage=1.0, lineage_integrity=1.0,
        semantic_expected_case_count=10, semantic_disposition_accuracy=1.0,
        relation_expectation_count=1, expected_relation_admission_count=1,
        observed_active_relation_count=1, relation_endpoint_accuracy=1.0,
        relation_type_accuracy=1.0, relation_direction_accuracy=1.0,
        relation_lineage_coverage=1.0, relation_lineage_integrity=1.0,
        harmful_false_link_rate=0.0,
        harmful_topology_propagation_rate=0.0,
        unlineaged_active_relation_count=0, unlineaged_active_relation_rate=0.0,
    ))
    values.update(updates)
    return GoldEntityPipelineReport(
        overall=EntityPipelineMetrics(**values), by_batch={}
    )


def _complete_evidence(**updates) -> EntityReadinessEvidence:
    values = dict(
        per_type_extraction={"customer": _extraction().overall},
        negative_cleanliness=ExactRatePopulation(numerator=20, denominator=20),
        exact_rate_populations={
            "pipeline.candidate_recall_at_3": ExactRatePopulation(
                numerator=10, denominator=10
            ),
            "pipeline.canonical_link_coverage": ExactRatePopulation(
                numerator=10, denominator=10
            ),
            "pipeline.canonical_link_accuracy": ExactRatePopulation(
                numerator=10, denominator=10
            ),
            "pipeline.no_admission_no_model_safety_rate": ExactRatePopulation(
                numerator=10, denominator=10
            ),
            "pipeline.harmful_semantic_propagation_rate": ExactRatePopulation(
                numerator=0, denominator=10
            ),
            "pipeline.relation_lineage_integrity": ExactRatePopulation(
                numerator=1, denominator=1
            ),
        },
        cross_tenant_identity_incidents=0,
        untraceable_canonical_assignments=0,
        known_wrong_type_consequential_admissions=0,
    )
    values.update(updates)
    return EntityReadinessEvidence(**values)


def _measurement(report, name):
    return next(item for item in report.measurements if item.name == name)


def test_missing_metrics_and_denominators_are_unknown_never_pass() -> None:
    report = evaluate_entity_readiness(
        extraction=_extraction(), pipeline=_pipeline(
            candidate_recall_at_k={1: 1.0, 3: 1.0, 5: 1.0},
            canonical_link_coverage=1.0, canonical_link_accuracy=1.0,
        )
    )

    candidate = _measurement(report, "pipeline.candidate_recall_at_3")
    assert candidate.value is None
    assert candidate.status == "unknown"
    assert candidate.denominator is None
    assert "pipeline.candidate_recall_at_3" in report.coverage_gaps
    assert report.blocker_verdict == "unknown"
    assert report.component_coverage["linking"] < 1.0


def test_high_scores_cannot_compensate_for_wrong_type_consequential_admission() -> None:
    pipeline = _pipeline(
        candidate_recall_at_k={1: 1.0, 3: 1.0, 5: 1.0},
        canonical_link_coverage=1.0, canonical_link_accuracy=1.0,
        no_admission_no_model_safety_rate=1.0,
        harmful_semantic_propagation_rate=0.0,
    )
    evidence = _complete_evidence(
        known_wrong_type_consequential_admissions=1
    )

    report = evaluate_entity_readiness(
        extraction=_extraction(), pipeline=pipeline, evidence=evidence
    )

    assert report.continuous_overall_score == 1.0
    assert report.blocker_verdict == "blocked"
    blocker = next(
        item for item in report.blockers
        if item.code == "known_wrong_type_consequential_admission"
    )
    assert blocker.status == "triggered"
    assert blocker.observed_count == 1


def test_false_link_topology_and_unlineaged_edges_are_independent_blockers() -> None:
    report = evaluate_entity_readiness(
        extraction=_extraction(),
        pipeline=_pipeline(
            harmful_false_link_rate=0.1,
            observed_active_relation_count=10,
            harmful_topology_relation_count=2,
            harmful_topology_propagation_rate=0.2,
            unlineaged_active_relation_count=1,
            unlineaged_active_relation_rate=0.1,
        ),
        evidence=_complete_evidence(),
    )

    triggered = {item.code for item in report.blockers if item.status == "triggered"}
    assert {
        "harmful_false_link", "harmful_topology_propagation",
        "unlineaged_active_edge",
    } <= triggered
    assert report.blocker_verdict == "blocked"
    topology = _measurement(report, "pipeline.harmful_topology_propagation_rate")
    assert (topology.numerator, topology.denominator) == (2, 10)
    assert topology.status == "below_budget"


def test_thresholds_are_versioned_configurable_and_denominators_are_exact() -> None:
    thresholds = EntityReadinessThresholds(
        min_overall_span_f1=0.80, min_per_type_span_f1=0.70,
    )
    extraction = _extraction(
        gold_count=10, prediction_count=10, exact_match_count=8,
        matched_count=10, span_precision=0.8, span_recall=0.8, span_f1=0.8,
    )
    per_type = _extraction(
        gold_count=5, prediction_count=5, exact_match_count=4,
        matched_count=5, span_precision=0.8, span_recall=0.8, span_f1=0.8,
    ).overall
    report = evaluate_entity_readiness(
        extraction=extraction,
        pipeline=_pipeline(),
        evidence=_complete_evidence(per_type_extraction={"workstream": per_type}),
        thresholds=thresholds,
    )

    overall = _measurement(report, "extraction.overall_span_f1")
    per_type_measure = _measurement(
        report, "extraction.per_type.workstream.span_f1"
    )
    assert report.threshold_policy_version == "entity-readiness-budget-v1"
    assert overall.status == "meets"
    assert (overall.numerator, overall.denominator) == (8, 10)
    assert per_type_measure.status == "meets"
    assert (per_type_measure.numerator, per_type_measure.denominator) == (4, 5)
    assert overall.exact_denominators == {
        "gold_count": 10, "prediction_count": 10,
    }
    assert report.thresholds == thresholds


def test_pipeline_contract_must_be_v4() -> None:
    stale = _pipeline().model_copy(update={"schema_version": "gold-entity-pipeline-v3"})

    with pytest.raises(ValueError, match="gold-entity-pipeline-v4"):
        evaluate_entity_readiness(extraction=_extraction(), pipeline=stale)
