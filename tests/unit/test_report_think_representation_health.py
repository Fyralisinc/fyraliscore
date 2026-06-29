from __future__ import annotations

from scripts.report_think_representation_health import (
    _company_question_coverage_report,
    _substrate_readiness_warnings,
)


def test_substrate_readiness_warnings_catch_large_run_failure_shape() -> None:
    warnings = _substrate_readiness_warnings(
        {
            "observations": {
                "observations": 51_709,
                "distinct_source_actor_refs": 667,
                "distinct_source_roots": 25,
            },
            "canonical": {
                "actors": 0,
                "actor_mappings": 0,
                "resources": 0,
                "customer_commitments": 0,
            },
            "candidate_counts": {},
            "source_roots": [
                {"source_root": "github", "n": 10_000},
                {"source_root": "brex", "n": 1_000},
            ],
        }
    )

    codes = {warning["code"] for warning in warnings}
    assert "actor_substrate_too_thin" in codes
    assert "actor_alias_mapping_absent" in codes
    assert "source_system_substrate_too_thin" in codes
    assert "commitment_substrate_absent" in codes
    assert "pattern_substrate_absent" in codes
    assert "customer_substrate_absent" in codes


def test_company_question_coverage_flags_large_run_with_generic_patterns_only() -> None:
    report = _company_question_coverage_report(
        model_counts={
            "active": 68,
            "pattern_models": 66,
            "curiosity_models": 0,
        },
        coverage_roles=[{"tag": "source", "n": 60}, {"tag": "discovered_pattern", "n": 60}],
        retrieval_tags=[{"tag": "source_digest", "n": 60}],
        domain_tags=[],
        model_updates={"contests": 0},
        edges={"active_edges": 0},
        substrate_readiness={
            "observations": {"observations": 51_709},
            "canonical": {
                "actors": 0,
                "resources": 0,
                "customer_commitments": 0,
            },
            "candidate_counts": {},
        },
        model_specificity={
            "active_without_any_scope": 60,
            "max_supporting_events": 19_963,
        },
        truth_seeking={"counter_relations": 0, "active_predictions": 0, "resolved_models": 0},
    )

    assert report["score"] < 0.7
    codes = {warning["code"] for warning in report["warnings"]}
    assert "company_question_coverage_low" in codes
    assert "too_many_models_without_scope" in codes
    assert "truth_seeking_counterevidence_absent" in codes
    assert "model_support_event_runaway" in codes
    assert report["spaces"]["patterns_and_loops"] is True
    assert report["spaces"]["ownership"] is False


def test_company_question_coverage_passes_when_company_surfaces_are_represented() -> None:
    report = _company_question_coverage_report(
        model_counts={
            "active": 500,
            "pattern_models": 80,
            "curiosity_models": 25,
        },
        coverage_roles=[
            {"tag": "entity", "n": 120},
            {"tag": "workstream", "n": 100},
            {"tag": "state", "n": 90},
            {"tag": "relationship", "n": 40},
            {"tag": "temporal", "n": 60},
            {"tag": "intervention", "n": 25},
        ],
        retrieval_tags=[
            {"tag": "progress_signal", "n": 40},
            {"tag": "delivery_risk", "n": 30},
            {"tag": "open_question", "n": 25},
            {"tag": "success_driver", "n": 20},
            {"tag": "source_observability", "n": 20},
        ],
        domain_tags=[{"tag": "source_finance", "n": 10}],
        model_updates={"contests": 7},
        edges={"active_edges": 120},
        substrate_readiness={
            "observations": {"observations": 51_709},
            "canonical": {
                "actors": 40,
                "resources": 12,
                "customer_commitments": 8,
            },
            "candidate_counts": {
                "actor": 120,
                "commitment": 140,
                "customer": 20,
                "system": 25,
                "vendor": 8,
                "pattern": 60,
            },
        },
        model_specificity={
            "active_without_any_scope": 40,
            "max_supporting_events": 120,
        },
        truth_seeking={
            "counter_relations": 11,
            "active_predictions": 15,
            "resolved_models": 4,
        },
    )

    assert report["score"] >= 0.9
    assert report["warnings"] == []
    assert all(report["spaces"].values())
