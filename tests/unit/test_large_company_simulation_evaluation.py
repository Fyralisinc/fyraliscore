from __future__ import annotations

import json
from pathlib import Path

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.large_company_simulation import (
    evaluate_large_company_simulation,
)
from scripts.evaluate_large_company_simulation import main


def _objective_entity_evidence(*, complete: bool = True) -> dict:
    pipeline = {
        "canonical_link_accuracy": 0.96 if complete else None,
        "canonical_link_coverage": 0.90 if complete else None,
        "lineage_integrity": 1.0,
        "semantic_lineage_integrity": 0.98 if complete else None,
        "relation_admission_accuracy": 0.94 if complete else None,
        "relation_endpoint_accuracy": 0.97 if complete else None,
        "relation_type_accuracy": 0.95 if complete else None,
        "relation_direction_accuracy": 0.96 if complete else None,
        "relation_lineage_coverage": 0.92 if complete else None,
        "harmful_false_link_rate": 0.01,
        "harmful_topology_propagation_rate": 0.02 if complete else None,
        "unlineaged_active_relation_rate": 0.0 if complete else None,
    }
    return {
        "schema_version": "objective-entity-evidence-v1",
        "extraction": {
            "schema_version": "gold-entity-extraction-v1",
            "overall": {"span_f1": 0.94, "type_accuracy": 0.98},
            "uncertainties": [],
        },
        "pipeline": {
            "schema_version": "gold-entity-pipeline-v4",
            "overall": pipeline,
            "uncertainties": ([] if complete else [
                "canonical_metrics_exclude_open_world_gold_cases",
                "relation_topology_metrics_exclude_unlabeled_cases",
            ]),
        },
        "proof_gaps": [],
    }


def _objective_company_learning_evidence(*, numeric: bool = True) -> dict:
    names = (
        "retrieval_evolution", "company_model_ablation", "feedback_learning",
        "feedback_quality", "source_equivalence", "correction_homeostasis",
        "joined_runtime", "single_model_synthesis",
    )
    components = {
        name: {
            "status": "observed",
            "continuous_score": (1.0 if numeric else None),
            "verdict": "meets_policy",
        }
        for name in names
    }
    components["retrieval_evolution"].update({
        "historical_verdict": "below_policy",
        "current_bounded_verdict": "meets_preregistered_policy",
    })
    payload = {
        "schema_version": "objective-company-learning-evidence-v1",
        "components": components,
        "exact_populations": {name: {"n": 1} for name in names},
        "evidence_coverage": 1.0 if numeric else 0.0,
        "observed_component_score": 1.0 if numeric else None,
        "coverage_adjusted_score": 1.0 if numeric else None,
        "below_policy_components": [],
        "historical_below_policy_components": ["retrieval_evolution"],
        "noncompensable_blockers": [],
        "proof_gaps": ["bounded current evidence"],
        "proof_boundaries": ["single_model_synthesis:bounded sealed synthesis"],
        "verdict": "meets_bounded_policy",
        "guarantee_boundary": "bounded only",
    }
    payload["composition_sha256"] = canonical_sha256(payload)
    return payload


def _artifacts(*, batches: int = 45, signals: int = 1125) -> tuple[dict, ...]:
    storyline_scores = [
        {
            "storyline_id": f"story-{index}",
            "latent_pattern_score": 0.9,
        }
        for index in range(8)
    ]
    benchmark = {
        "run_id": "large-sim-test",
        "status": "passed",
        "signals": signals,
        "storyline_count": 8,
        "storyline_scores": storyline_scores,
        "required_run_failures": [],
        "latent_pattern_fitness": {
            "average_latent_pattern_score": 0.9,
            "average_best_pattern_coverage": 0.85,
            "storylines_with_concrete_latent_model": 8,
        },
        "thesis_recovery_judge": {
            "n": 8,
            "average_score": 0.9,
            "correct_count": 7,
        },
        "waves": [
            {
                "wave": index + 1,
                "t1_batch": {
                    "member_count": 25,
                    "observation_count": 25,
                    "run": {
                        "status": "success",
                        "retrieval_model_count": index + 1,
                        "retrieval_observation_count": max(1, 25 - index),
                    },
                },
            }
            for index in range(batches)
        ],
        "run_health": {
            "pending_triggers": 0,
            "pending_post_commit_actions": 0,
            "dead_lettered_post_commit_actions": 0,
            "think_runs_success": batches,
            "think_runs_failed": 0,
        },
        "run_amplification": {"validation_error_count": 0},
        "company_intelligence_scorecard": {
            "proof_gaps": [],
            "dimensions": {
                "memory_truth": {"score": 0.9},
                "compression": {"score": 0.9},
                "edge_intelligence": {"score": 0.85},
                "temporal_improvement": {
                    "score": 0.9,
                    "metrics": {
                        "future_validation_events": 12,
                        "future_validation_memory_touch_ops": 10,
                        "future_validation_model_or_graph_context_use_score": 0.9,
                    },
                },
            },
            "product_value_evals": {"proof_gaps": []},
        },
    }
    run_summary = {
        "run_id": "large-sim-test",
        "signal_count": signals,
        "pending_triggers": 0,
        "think_runs_success": batches,
        "think_runs_failed": 0,
        "semantic_memory_before_first_wave": {
            "models": 0,
            "model_edges": 0,
            "pattern_candidates": 0,
            "hypotheses": 0,
        },
        "pre_first_wave_scaffolding": {
            "tenant": 1,
            "sources": 4,
            "actors": 12,
        },
    }
    vitals = {
        "status": "ok",
        "hard_failures": [],
        "proof_gaps": [],
        "vitals": {
            "model_coherence": {"score": 0.9},
            "metabolism_yield": {"score": 0.9},
            "self_improvement": {"score": 0.9},
            "human_loop_closure": {"score": 0.9},
            "control_plane_health": {"score": 1.0},
        },
        "company_physics": {
            "assurance_suite": {"active_surfaces": {"status": "observed"}}
        },
    }
    assurance = {
        "schema_version": "company-learning-assurance-summary-v7",
        "run_id": "large-sim-test",
        "status": "working",
        "blocking_failures": [],
        "positive": {
            "status": "working",
            "adaptive_minus_frozen_correctness": 0.9,
        },
        "correction": {"status": "working"},
        "retention": {"status": "observed"},
        "negative": {"status": "observed"},
    }
    run_config = {
        "mode": "run",
        "target_t1_batches": 45,
        "seed_models": 0,
        "t1_batch_min_size": 2,
        "t1_batch_window_s": 1.0,
    }
    return benchmark, run_summary, vitals, assurance, run_config


def test_full_profile_is_continuous_and_supports_strong_claims() -> None:
    benchmark, run_summary, vitals, assurance, run_config = _artifacts()

    report = evaluate_large_company_simulation(
        benchmark=benchmark,
        run_summary=run_summary,
        vitals=vitals,
        assurance=assurance,
        run_config=run_config,
        profile_name="authoritative-45",
        entity_evidence=_objective_entity_evidence(),
    )

    assert report["status"] == "strong"
    assert report["overall_score"] > 0.85
    assert report["evidence_coverage"] == 1.0
    assert report["hard_failures"] == []
    assert "recovers planted hidden patterns" in report["claims_supported"]
    assert (
        report["dimensions"]["hidden_pattern_recovery"]["metrics"][
            "thesis_accuracy"
        ]
        == 0.875
    )
    hidden = report["dimensions"]["hidden_pattern_recovery"]["metrics"]
    assert hidden["causal_thesis_miss_rate"] == 0.125
    assert hidden["independent_thesis_weight"] == 0.60
    assert hidden["proxy_structure_weight"] == 0.40


def test_status_only_active_surfaces_cannot_manufacture_entity_quality() -> None:
    benchmark, run_summary, vitals, assurance, run_config = _artifacts()

    report = evaluate_large_company_simulation(
        benchmark=benchmark, run_summary=run_summary, vitals=vitals,
        assurance=assurance, run_config=run_config,
        profile_name="authoritative-45",
    )
    entity = report["dimensions"]["entity_model_quality"]
    assert vitals["company_physics"]["assurance_suite"]["active_surfaces"] == {
        "status": "observed"
    }
    assert entity["metrics"]["entity_identity_quality"] is None
    assert entity["metrics"]["entity_identity_evidence_coverage"] == 0.0
    assert entity["coverage"] == 0.8333
    assert any(
        "active-surface status is not objective entity-quality evidence" in gap
        for gap in report["proof_gaps"]
    )


def test_objective_learning_evidence_adds_numeric_metrics_without_rewriting_history():
    benchmark, run_summary, vitals, assurance, run_config = _artifacts()
    report = evaluate_large_company_simulation(
        benchmark=benchmark, run_summary=run_summary, vitals=vitals,
        assurance=assurance, run_config=run_config,
        profile_name="authoritative-45",
        company_learning_evidence=_objective_company_learning_evidence(),
    )

    metrics = report["current_bounded_company_learning"]
    assert metrics["components"] == {
        "retrieval_evolution": 1.0, "company_model_ablation": 1.0,
        "feedback_learning": 1.0, "source_equivalence": 1.0,
        "feedback_quality": 1.0,
        "correction_homeostasis": 1.0,
        "joined_runtime": 1.0,
        "single_model_synthesis": 1.0,
    }
    assert metrics["historical_retrieval_verdict"] == "below_policy"
    assert (
        metrics["current_bounded_retrieval_verdict"]
        == "meets_preregistered_policy"
    )
    assert report["dimensions"]["learning_correction_lift"]["metrics"][
        "objective_company_learning_quality"
    ] == 1.0
    assert any("Immutable historical 45-batch retrieval" in gap
               for gap in report["proof_gaps"])
    assert report["proof_boundaries"] == [
        "single_model_synthesis:bounded sealed synthesis"
    ]
    assert "single_model_synthesis:bounded sealed synthesis" not in report["proof_gaps"]


def test_learning_status_labels_cannot_manufacture_numeric_score():
    benchmark, run_summary, vitals, assurance, run_config = _artifacts()
    report = evaluate_large_company_simulation(
        benchmark=benchmark, run_summary=run_summary, vitals=vitals,
        assurance=assurance, run_config=run_config,
        profile_name="authoritative-45",
        company_learning_evidence=_objective_company_learning_evidence(numeric=False),
    )

    learning = report["dimensions"]["learning_correction_lift"]["metrics"]
    assert learning["objective_company_learning_quality"] is None
    assert report["current_bounded_company_learning"] == {}
    assert any("aggregate score disagrees" in gap for gap in report["proof_gaps"])


def test_feedback_quality_is_mandatory_in_top_evaluator():
    benchmark, run_summary, vitals, assurance, run_config = _artifacts()
    evidence = _objective_company_learning_evidence()
    evidence["components"].pop("feedback_quality")
    evidence["exact_populations"].pop("feedback_quality")
    evidence["evidence_coverage"] = 7 / 8
    evidence.pop("composition_sha256")
    evidence["composition_sha256"] = canonical_sha256(evidence)

    report = evaluate_large_company_simulation(
        benchmark=benchmark, run_summary=run_summary, vitals=vitals,
        assurance=assurance, run_config=run_config,
        profile_name="authoritative-45", company_learning_evidence=evidence,
    )

    assert report["current_bounded_company_learning"]["required_component_count"] == 8
    assert report["current_bounded_company_learning"]["coverage"] == 7 / 8
    assert any("required matched feedback-quality" in failure
               for failure in report["hard_failures"])


def test_single_model_synthesis_is_mandatory_in_top_evaluator():
    benchmark, run_summary, vitals, assurance, run_config = _artifacts()
    evidence = _objective_company_learning_evidence()
    evidence["components"].pop("single_model_synthesis")
    evidence["exact_populations"].pop("single_model_synthesis")
    evidence["evidence_coverage"] = 7 / 8
    evidence.pop("composition_sha256")
    evidence["composition_sha256"] = canonical_sha256(evidence)

    report = evaluate_large_company_simulation(
        benchmark=benchmark, run_summary=run_summary, vitals=vitals,
        assurance=assurance, run_config=run_config,
        profile_name="authoritative-45", company_learning_evidence=evidence,
    )

    assert report["current_bounded_company_learning"]["required_component_count"] == 8
    assert report["current_bounded_company_learning"]["coverage"] == 7 / 8
    assert any("required strict single-Model synthesis" in failure
               for failure in report["hard_failures"])

def test_partial_objective_entity_metrics_remain_continuous_and_gapped() -> None:
    benchmark, run_summary, vitals, assurance, run_config = _artifacts()

    report = evaluate_large_company_simulation(
        benchmark=benchmark, run_summary=run_summary, vitals=vitals,
        assurance=assurance, run_config=run_config,
        profile_name="authoritative-45",
        entity_evidence=_objective_entity_evidence(complete=False),
    )

    entity = report["dimensions"]["entity_model_quality"]["metrics"]
    objective = entity["entity_identity_metrics"]
    assert entity["entity_identity_quality"] == 0.9775
    assert entity["entity_identity_evidence_coverage"] == 4 / 14
    assert objective["observed_component_count"] == 4
    assert objective["required_component_count"] == 14
    assert objective["components"]["canonical_link_accuracy"] is None
    assert objective["components"]["relation_admission_accuracy"] is None
    assert any("canonical-link" in gap for gap in report["proof_gaps"])
    assert any("relation/topology" in gap for gap in report["proof_gaps"])
    assert any(
        "canonical_metrics_exclude_open_world_gold_cases" in gap
        for gap in report["proof_gaps"]
    )


def test_v2_adversarial_components_are_visible_but_weight_capped() -> None:
    benchmark, run_summary, vitals, assurance, run_config = _artifacts()
    evidence = _objective_entity_evidence()
    evidence.update({
        "schema_version": "objective-entity-evidence-v2",
        "readiness": {"component_scores": {
            "adversarial_topology": 1.0,
            "correction_safety": 1.0,
            "consequence_safety": 1.0,
            "open_world_safety": 1.0,
        }},
        "adversarial_company_physics": {
            "population": {"adversarial_relation_attempts": 4}
        },
    })
    report = evaluate_large_company_simulation(
        benchmark=benchmark, run_summary=run_summary, vitals=vitals,
        assurance=assurance, run_config=run_config,
        profile_name="authoritative-45", entity_evidence=evidence,
    )
    objective = report["dimensions"]["entity_model_quality"]["metrics"][
        "entity_identity_metrics"
    ]
    assert objective["components"]["adversarial_topology_safety"] == 1.0
    assert objective["component_weights"]["adversarial_topology_safety"] == 0.25
    assert objective["bounded_adversarial_total_weight"] == 1.0
    assert any(
        "bounded to 4 relation attempts" in gap for gap in report["proof_gaps"]
    )


def test_scale_shortfall_is_precise_instead_of_binary() -> None:
    benchmark, run_summary, vitals, assurance, run_config = _artifacts(
        batches=1,
        signals=50,
    )

    report = evaluate_large_company_simulation(
        benchmark=benchmark,
        run_summary=run_summary,
        vitals=vitals,
        assurance=assurance,
        run_config=run_config,
        profile_name="authoritative-45",
    )

    assert report["status"] == "not_credible"
    scale = report["scale"]
    assert scale["signals"] == {
        "observed": 50,
        "required": 1125,
        "coverage": 0.0444,
    }
    assert scale["successful_t1_batches"]["coverage"] == 0.0222
    assert any("scale is short" in gap for gap in report["proof_gaps"])


def test_safety_and_drain_failures_are_noncompensatory() -> None:
    benchmark, run_summary, vitals, assurance, run_config = _artifacts()
    benchmark["required_run_failures"] = ["trigger queue did not drain"]
    assurance["blocking_failures"] = ["cross-tenant collision"]

    report = evaluate_large_company_simulation(
        benchmark=benchmark,
        run_summary=run_summary,
        vitals=vitals,
        assurance=assurance,
        run_config=run_config,
        profile_name="authoritative-45",
    )

    assert report["status"] == "not_credible"
    assert report["overall_score"] > 0.8
    assert report["claims_supported"] == []
    assert len(report["hard_failures"]) == 2


def test_recovered_think_attempt_is_degradation_not_hard_failure() -> None:
    benchmark, run_summary, vitals, assurance, run_config = _artifacts()
    wave = benchmark["waves"][18]
    wave["t1_batch"]["attempt_history"] = [
        {"attempt": 1, "status": "failed", "error": "provider timeout"},
        {"attempt": 2, "status": "success", "error": None},
    ]
    wave["t1_batch"]["run"]["recovered_after_retry"] = True
    benchmark["run_health"]["think_runs_failed"] = 1
    run_summary["think_runs_failed"] = 1
    vitals["hard_failures"] = ["Think failures present: failed=1"]

    report = evaluate_large_company_simulation(
        benchmark=benchmark,
        run_summary=run_summary,
        vitals=vitals,
        assurance=assurance,
        run_config=run_config,
        profile_name="authoritative-45",
    )

    assert report["hard_failures"] == []
    assert report["status"] == "strong"
    operational = report["dimensions"]["operational_drain"]["metrics"]
    assert operational["think_runs_failed_historical"] == 1
    assert operational["think_failures_recovered"] == 1
    assert operational["think_failures_terminal"] == 0
    assert operational["think_runs_failed"] == 0
    assert any(
        "Recovered Think failure history" in gap for gap in report["proof_gaps"]
    )


def test_terminal_think_failure_remains_noncompensatory() -> None:
    benchmark, run_summary, vitals, assurance, run_config = _artifacts()
    wave = benchmark["waves"][18]
    wave["t1_batch"]["attempt_history"] = [
        {"attempt": 1, "status": "failed", "error": "provider timeout"},
    ]
    wave["t1_batch"]["run"]["status"] = "failed"
    benchmark["run_health"]["think_runs_failed"] = 1
    run_summary["think_runs_failed"] = 1
    vitals["hard_failures"] = ["Think failures present: failed=1"]

    report = evaluate_large_company_simulation(
        benchmark=benchmark,
        run_summary=run_summary,
        vitals=vitals,
        assurance=assurance,
        run_config=run_config,
        profile_name="authoritative-45",
    )

    assert report["status"] == "not_credible"
    assert "vitals: Think failures present: failed=1" in report["hard_failures"]
    operational = report["dimensions"]["operational_drain"]["metrics"]
    assert operational["think_failures_terminal"] == 1
    assert operational["think_runs_failed"] == 1


def test_authoritative_contract_rejects_seeded_or_fake_batches() -> None:
    benchmark, run_summary, vitals, assurance, run_config = _artifacts()
    run_config["seed_models"] = 5
    benchmark["waves"][3]["t1_batch"]["member_count"] = 1
    run_summary["semantic_memory_before_first_wave"]["models"] = 5

    report = evaluate_large_company_simulation(
        benchmark=benchmark,
        run_summary=run_summary,
        vitals=vitals,
        assurance=assurance,
        run_config=run_config,
        profile_name="authoritative-45",
    )

    assert report["status"] == "not_credible"
    checks = report["run_contract"]["checks"]
    assert checks["zero_seeded_models_configured"] is False
    assert checks["every_t1_run_genuinely_batched"] is False
    assert checks["pre_first_wave_semantic_memory_zero"] is False
    assert report["run_contract"]["pre_first_wave_scaffolding"]["tenant"] == 1


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    benchmark, run_summary, vitals, assurance, run_config = _artifacts(
        batches=1,
        signals=50,
    )
    report_dir = tmp_path / "report"
    vitals_dir = report_dir / "vitals"
    vitals_dir.mkdir(parents=True)
    for path, payload in (
        (report_dir / "benchmark_summary.json", benchmark),
        (report_dir / "run_summary.json", run_summary),
        (report_dir / "run_config.json", run_config),
        (vitals_dir / "vitals_scorecard.json", vitals),
        (report_dir / "company_learning_assurance_summary.json", assurance),
        (report_dir / "objective_entity_evidence.json", _objective_entity_evidence()),
        (
            report_dir / "objective_company_learning_evidence.json",
            _objective_company_learning_evidence(),
        ),
    ):
        path.write_text(json.dumps(payload), encoding="utf-8")

    assert main(["--report-dir", str(report_dir)]) == 0

    output = report_dir / "large_simulation_gate"
    payload = json.loads(
        (output / "large_company_simulation_evaluation.json").read_text()
    )
    assert payload["artifact_inputs"]["company_learning_evidence"].endswith(
        "objective_company_learning_evidence.json"
    )
    assert payload["current_bounded_company_learning"]["coverage"] == 1.0
    markdown = (
        output / "large_company_simulation_evaluation.md"
    ).read_text()
    assert payload["profile"] == "authoritative-45"
    assert payload["artifact_inputs"]["vitals"].endswith(
        "vitals/vitals_scorecard.json"
    )
    assert payload["artifact_inputs"]["entity_evidence"].endswith(
        "objective_entity_evidence.json"
    )
    assert "## Hidden Pattern Recovery" in markdown
    assert "## Proof Boundaries" in markdown
    assert "## Claims This Run Does Not Support" in markdown
