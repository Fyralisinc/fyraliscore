"""Fail-closed composition contracts for objective entity evidence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic_core import PydanticUndefined

from lib.evaluation.entity_evidence_composer import (
    canonical_json_bytes,
    compose_objective_entity_evidence,
    load_bound_json,
    sha256_bytes,
    write_atomic_json,
)
from lib.evaluation.entity_extraction_gold import (
    EntityExtractionMetrics,
    GoldEntityExtractionReport,
)
from lib.evaluation.entity_pipeline_gold import (
    EntityPipelineMetrics,
    GoldEntityPipelineReport,
)


def _extraction_metrics(*, gold: int = 10, exact: int = 10) -> EntityExtractionMetrics:
    rate = exact / gold
    return EntityExtractionMetrics(
        signal_count=10, batch_count=1, gold_count=gold, prediction_count=gold,
        exact_match_count=exact, matched_count=gold, span_precision=rate,
        span_recall=rate, span_f1=rate, mean_boundary_iou=rate,
        boundary_credit_precision=rate, boundary_credit_recall=rate,
        type_accuracy=1.0, duplicate_rate=0.0, candidate_fate_coverage=1.0,
    )


def _v3() -> dict:
    metrics = GoldEntityExtractionReport(
        overall=_extraction_metrics(), by_source={}, by_slack_context={},
        uncertainties=("canonical_link_metrics_exclude_gold_without_referents",),
    ).model_dump(mode="json")
    metrics["by_entity_type"] = {
        "customer": _extraction_metrics().model_dump(mode="json")
    }
    return {
        "benchmark": "learned-entity-discovery-quality-v3",
        "evidence_class": "sealed_untouched_holdout_one_shot_completed",
        "frozen_corpus_sha256": "a" * 64,
        "post_verification": {
            "metrics": metrics,
            "negative_cleanliness": {
                "negative_signal_count": 5,
                "clean_negative_signals": 5,
                "rate": 1.0,
            },
        },
    }


def _pipeline() -> GoldEntityPipelineReport:
    values = {}
    for name, field in EntityPipelineMetrics.model_fields.items():
        if field.default_factory is not None:
            values[name] = field.default_factory()
        elif field.default is not PydanticUndefined:
            values[name] = field.default
        elif name in {"candidate_recall_at_k", "gold_type_present_at_k"}:
            values[name] = {1: 1.0, 3: 1.0, 5: 1.0}
        else:
            values[name] = 0
    values.update(dict(
        gold_case_count=10, detected_case_count=10,
        candidate_population_count=10, candidate_population_coverage=1.0,
        type_assessed_case_count=10, type_assessment_accuracy=1.0,
        terminal_case_count=10, detection_to_terminal_coverage=1.0,
        canonical_link_coverage=1.0, canonical_link_accuracy=1.0,
        lineage_integrity=1.0, semantic_expected_case_count=10,
        semantic_disposition_accuracy=1.0,
        no_admission_no_model_safety_rate=None,
        harmful_semantic_propagation_rate=0.0,
        relation_expectation_count=1, expected_relation_admission_count=1,
        observed_active_relation_count=1, relation_endpoint_accuracy=1.0,
        relation_type_accuracy=1.0, relation_direction_accuracy=1.0,
        relation_lineage_coverage=1.0, relation_lineage_integrity=1.0,
        harmful_false_link_rate=0.0, harmful_topology_relation_count=0,
        harmful_topology_propagation_rate=0.0,
        unlineaged_active_relation_count=0, unlineaged_active_relation_rate=0.0,
    ))
    return GoldEntityPipelineReport(
        overall=EntityPipelineMetrics(**values), by_batch={}, uncertainties=()
    )


def _vertical(*, omit_population: str | None = None) -> dict:
    populations = {
        "pipeline.candidate_recall_at_3": {"numerator": 10, "denominator": 10},
        "pipeline.canonical_link_coverage": {"numerator": 10, "denominator": 10},
        "pipeline.canonical_link_accuracy": {"numerator": 10, "denominator": 10},
        "pipeline.no_admission_no_model_safety_rate": {
            "numerator": 0, "denominator": 0,
        },
        "pipeline.harmful_semantic_propagation_rate": {
            "numerator": 0, "denominator": 10,
        },
        "pipeline.relation_lineage_integrity": {"numerator": 1, "denominator": 1},
    }
    if omit_population:
        populations.pop(omit_population)
    result = {
        "schema_version": "sealed-company-physics-objective-v1",
        "entity_pipeline_v4": _pipeline().model_dump(mode="json"),
        "readiness_evidence_v1": {
            "schema_version": "sealed-company-physics-readiness-evidence-v1",
            "exact_rate_populations": populations,
            "incidents": {
                "cross_tenant_identity_incidents": 0,
                "untraceable_canonical_assignments": 0,
                "known_wrong_type_consequential_admissions": 0,
            },
        },
    }
    result["objective_sha256"] = sha256_bytes(canonical_json_bytes(result))
    return result


def test_composes_normalized_reports_readiness_bindings_and_gaps() -> None:
    output = compose_objective_entity_evidence(
        v3=_v3(), vertical=_vertical(),
        v3_artifact_sha256="b" * 64, vertical_artifact_sha256="c" * 64,
    )

    assert output["schema_version"] == "objective-entity-evidence-v1"
    assert output["extraction"]["schema_version"] == "gold-entity-extraction-v1"
    assert "by_entity_type" not in output["extraction"]
    assert output["per_type_extraction"]["customer"]["gold_count"] == 10
    assert output["pipeline"]["schema_version"] == "gold-entity-pipeline-v4"
    assert output["readiness"]["blocker_verdict"] == "clear"
    assert output["artifact_bindings"]["sealed_v3"]["artifact_sha256"] == "b" * 64
    assert "canonical_link_metrics_exclude_gold_without_referents" in output[
        "proof_gaps"
    ]
    assert len(output["composition_sha256"]) == 64


def test_fails_closed_when_vertical_lacks_exact_readiness_population() -> None:
    vertical = _vertical(omit_population="pipeline.canonical_link_accuracy")

    with pytest.raises(ValueError, match="exact readiness populations"):
        compose_objective_entity_evidence(
            v3=_v3(), vertical=vertical,
            v3_artifact_sha256="b" * 64, vertical_artifact_sha256="c" * 64,
        )


def test_fails_closed_when_exact_population_disagrees_with_pipeline() -> None:
    vertical = _vertical()
    vertical["readiness_evidence_v1"]["exact_rate_populations"][
        "pipeline.canonical_link_accuracy"
    ] = {"numerator": 9, "denominator": 10}
    vertical["objective_sha256"] = sha256_bytes(canonical_json_bytes({
        key: value for key, value in vertical.items() if key != "objective_sha256"
    }))

    with pytest.raises(ValueError, match="canonical_link_accuracy disagrees"):
        compose_objective_entity_evidence(
            v3=_v3(), vertical=vertical,
            v3_artifact_sha256="b" * 64, vertical_artifact_sha256="c" * 64,
        )


def test_load_binding_and_atomic_output_fail_closed(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    source.write_text(json.dumps(_v3()) + "\n", encoding="utf-8")
    digest = sha256_bytes(source.read_bytes())

    assert load_bound_json(source, expected_sha256=digest)["benchmark"].endswith("v3")
    with pytest.raises(ValueError, match="artifact SHA mismatch"):
        load_bound_json(source, expected_sha256="0" * 64)

    output = tmp_path / "nested" / "objective.json"
    write_atomic_json(output, {"schema_version": "objective-entity-evidence-v1"})
    assert json.loads(output.read_text())["schema_version"] == (
        "objective-entity-evidence-v1"
    )
    assert not output.with_suffix(".json.tmp").exists()
