from copy import deepcopy

import pytest

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_evidence_composer import (
    BoundArtifact, compose_objective_company_learning_evidence,
)
from lib.evaluation.company_model_ablation import (
    evaluate_company_model_ablation,
    evaluate_single_model_synthesis,
    manifest_digest,
)
from lib.evaluation.feedback_learning_effect import (
    FeedbackLearningEffectEvidence, FeedbackLearningEffectReport, MatchedSalienceEffect,
)
from services.retrieval_evolution_postfix_vertical import run_bounded_retrieval_evolution_postfix
from services.source_equivalence_vertical import run_bounded_source_equivalence


SHA = "a" * 64


def _retrieval():
    checks = {key: True for key in (
        "enough_batches_per_phase", "early_is_observation_heavy", "late_is_model_preferred",
        "late_actual_use_is_model_preferred", "model_reliance_increases",
        "late_selected_context_is_actually_referenced", "late_raw_reopening_is_justified",
    )}
    return {"schema_version": "retrieval-evolution-evaluation-v1", "batch_count": 9,
            "phase_batch_counts": {"early": 3, "middle": 3, "late": 3},
            "checks": checks, "continuous_score": 1.0,
            "verdict": "meets_preregistered_policy"}


def _ablation():
    manifest = {"schema_version": "company-model-hidden-truth-v1", "experiment_id": "x",
                "judge_id": "judge", "hidden_theses": [
                    {"thesis_id": key} for key in ("a", "b", "c")]}
    batches = [{"batch_id": str(i), "signal_ids": [f"{i}a", f"{i}b"]} for i in range(3)]
    learned = {"schema_version": "company-model-ablation-arm-v1", "arm": "learned_memory",
               "producer_id": "runtime", "truth_visible_to_producer": False,
               "hidden_truth_digest": manifest_digest(manifest), "batches": batches,
               "predictions": [{"thesis_id": key, "recovered": True, "confidence": .8,
                                "future_outcomes": [1, 1, 1, 1]} for key in ("a", "b", "c")],
               "safety_incidents": []}
    frozen = {**deepcopy(learned), "arm": "frozen_memory",
              "predictions": [{"thesis_id": key, "recovered": False, "confidence": .2,
                               "future_outcomes": [0, 0, 0, 0]} for key in ("a", "b", "c")]}
    return {"schema_version": "bounded-company-model-ablation-artifact-v1",
            "manifest": manifest, "learned_arm": learned, "frozen_arm": frozen,
            "evaluation": evaluate_company_model_ablation(
                manifest=manifest, learned=learned, frozen=frozen)}


def _active_ablation(*, learned_lift: bool = True, version: str = "v6"):
    payload = deepcopy(_ablation())
    payload["schema_version"] = (
        f"bounded-company-model-holdout-{version}-artifact-v1"
    )
    if not learned_lift:
        payload["learned_arm"]["predictions"] = deepcopy(
            payload["frozen_arm"]["predictions"]
        )
        payload["evaluation"] = evaluate_company_model_ablation(
            manifest=payload["manifest"],
            learned=payload["learned_arm"],
            frozen=payload["frozen_arm"],
        )
    return payload


def _active_failure():
    payload = {
        "schema_version": "bounded-company-model-holdout-v5-failure-artifact-v1",
        "population": {"actual_think_runs": 1, "completed_batches": 0},
        "verdict": "inconclusive_runtime_contract_failure",
        "proof_boundary": "failed before semantic judging",
    }
    payload["objective_sha256"] = canonical_sha256(payload)
    return payload


def _feedback():
    effect = MatchedSalienceEffect(case_id="settled_useful", expected_effect="increase",
        frozen_salience=1.0, adaptive_salience=1.1, adaptive_minus_frozen=.1,
        direction_correct=True, truth_immutable=True)
    report = FeedbackLearningEffectReport(status="observed", matched_pair_count=1,
        useful_pair_count=1, safety_pair_count=0, direction_correct_rate=1,
        truth_immutability_rate=1, useful_adaptive_minus_frozen=.1,
        safety_absolute_effect_mean=0, continuous_score=1, effects=(effect,),
        causal_claim="bounded matched effect", excluded_claims=("generalization",),
        source_evidence_digest="b" * 64)
    return FeedbackLearningEffectEvidence(source_artifact_sha256="c" * 64,
                                          report=report).artifact_payload()


def _feedback_quality():
    checks = {key: True for key in (
        "adaptive_correct_model_remains_active",
        "adaptive_correction_archived_wrong_model",
        "adaptive_correction_lineage_exact",
        "adaptive_later_quality_is_correct",
        "adaptive_reasoning_lineage_corrected_model",
        "adaptive_relation_fenced",
        "all_think_runs_succeed",
        "both_arms_emit_later_models",
        "frozen_preserves_negative_control",
        "frozen_reasoning_lineage_wrong_model",
        "frozen_relation_unchanged",
        "frozen_wrong_model_remains_active",
        "later_models_reference_selected_context",
        "later_quality_improves",
        "matched_later_batches",
        "selected_models_are_tenant_isolated",
        "source_truth_is_immutable_and_matched",
    )}
    payload = {
        "schema_version": "feedback-quality-matched-db-objective-v1",
        "population": {
            "arms": 2, "correction_episodes": 1,
            "later_batches_per_arm": 3, "signals_per_later_batch": 2,
        },
        "checks": checks,
        "continuous_score": 1.0,
        "verdict": "meets_policy",
        "proof_boundary": ["bounded matched synthetic company world"],
    }
    payload["objective_sha256"] = canonical_sha256(payload)
    return payload


def _reseal(payload):
    payload.pop("objective_sha256", None)
    payload["objective_sha256"] = canonical_sha256(payload)


def _source():
    checks = {key: True for key in (
        "all_source_families_covered", "semantic_outcomes_consistent",
        "source_authority_preserved", "source_coordinates_preserved",
        "conversational_boundaries_preserved", "signals_processed_as_batches",
        "learning_outcomes_are_lineaged")}
    return {"schema_version": "normalized-source-equivalence-evaluation-v1",
            "population": {"cases": 2, "source_batches": 8}, "checks": checks,
            "continuous_score": 1.0, "verdict": "meets_policy"}


def _source_db(*, relations_exposed: bool):
    report = _source()
    report["checks"]["relation_outcomes_exposed"] = relations_exposed
    report["continuous_score"] = 1.0 if relations_exposed else 2 / 3
    report["verdict"] = "meets_policy" if relations_exposed else "below_policy"
    payload = {
        "schema_version": "source-equivalence-db-objective-v1",
        "population": {"signal_batches": 1, "signals": 8, "sources": 4},
        "relation_path": {"accepted_edges": 4 if relations_exposed else 0},
        "evaluation": report,
    }
    payload["objective_sha256"] = canonical_sha256(payload)
    return payload


def _correction():
    checks = {key: True for key in (
        "repeated_corrections_exercised", "correction_converges", "unsafe_reads_contained",
        "replay_is_idempotent", "restart_preserves_state",
        "deep_cascade_is_complete_and_cycle_safe", "repair_debt_does_not_grow",
        "terminal_repair_debt_is_cleared", "signals_are_batched")}
    evaluation = {"schema_version": "correction-homeostasis-evaluation-v1",
                  "population": {"correction_episodes": 2, "repair_required": 10,
                                 "residual_debt": 0}, "checks": checks,
                  "continuous_score": 1.0, "verdict": "meets_policy",
                  "proof_boundary": "bounded DB proof"}
    payload = {"schema_version": "correction-homeostasis-db-objective-v1",
               "evaluation": evaluation, "database_evidence": {}}
    payload["objective_sha256"] = canonical_sha256(payload)
    return payload


def _joined_runtime():
    checks = {f"joined_check_{index:02d}": True for index in range(17)}
    payload = {
        "schema_version": "integrated-company-learning-vertical-v2",
        "continuous_score": 1.0, "verdict": "meets_policy",
        "checks": checks, "populations": {"observations": 74, "models": 12},
        "active_populations": {"models": 11, "edges": 0},
        "material_use_ablation": {"with_prior": {}, "without_prior": {}},
        "correction": {"exact_cross_stage_edge": {"status_before": "active", "status_after": "inert"}},
        "negative_controls": {"cross_tenant_selected_models": 0},
        "proof_boundary": ["bounded joined runtime"], "batch_count": 6,
        "signal_count": 74,
    }
    payload["objective_sha256"] = canonical_sha256(payload)
    return payload


def _single_model_synthesis():
    manifest = {"schema_version": "company-model-synthesis-manifest-v1",
        "hidden_patterns": [{"thesis_id": "x", "required_facets": ["a", "b"]}]}
    learned = {"schema_version": "company-model-synthesis-arm-v1",
        "arm": "learned_memory", "prior_model_ids": ["p"],
        "required_lineage_by_thesis": {"x": ["p"]},
        "models": [{"model_id": "m", "thesis_id": "x", "facets": ["a", "b"],
            "evidence_model_ids": ["p"], "persisted": True}]}
    frozen = {"schema_version": "company-model-synthesis-arm-v1",
        "arm": "frozen_memory", "prior_model_ids": [],
        "required_lineage_by_thesis": {"x": []}, "models": []}
    return {"schema_version": "single-model-synthesis-holdout-v1-artifact-v1",
        "manifest": manifest, "learned_arm": learned, "frozen_arm": frozen,
        "evaluation": evaluate_single_model_synthesis(
            manifest=manifest, learned=learned, frozen=frozen)}


def test_composes_all_sha_bound_components_with_exact_populations():
    result = compose_objective_company_learning_evidence(
        retrieval_evolution=BoundArtifact(_retrieval(), SHA),
        retrieval_evolution_postfix=BoundArtifact(
            run_bounded_retrieval_evolution_postfix(), SHA),
        company_model_ablation=BoundArtifact(_active_ablation(), SHA),
        company_model_ablation_legacy=BoundArtifact(_ablation(), SHA),
        company_model_ablation_active_failure=BoundArtifact(_active_failure(), SHA),
        company_model_ablation_active_predecessor=BoundArtifact(
            _active_ablation(learned_lift=False), SHA
        ),
        feedback_learning=BoundArtifact(_feedback(), SHA),
        feedback_quality=BoundArtifact(_feedback_quality(), SHA),
        source_equivalence=BoundArtifact(_source_db(relations_exposed=True), SHA),
        correction_homeostasis=BoundArtifact(_correction(), SHA),
        joined_runtime=BoundArtifact(_joined_runtime(), SHA),
        single_model_synthesis=BoundArtifact(_single_model_synthesis(), SHA),
    )

    assert result["verdict"] == "meets_bounded_policy"
    assert result["evidence_coverage"] == 1.0
    assert result["coverage_adjusted_score"] == 1.0
    assert result["noncompensable_blockers"] == []
    assert result["exact_populations"]["company_model_ablation"]["signals"] == 6
    assert result["exact_populations"]["feedback_learning"]["matched_pairs"] == 1
    assert result["exact_populations"]["feedback_quality"]["arms"] == 2
    assert result["exact_populations"]["single_model_synthesis"]["hidden_patterns"] == 1
    assert len(result["composition_sha256"]) == 64


def test_current_active_ablation_overrides_legacy_development_pass():
    result = compose_objective_company_learning_evidence(
        company_model_ablation=BoundArtifact(
            _active_ablation(learned_lift=False), SHA
        ),
        company_model_ablation_legacy=BoundArtifact(_ablation(), SHA),
        company_model_ablation_active_failure=BoundArtifact(_active_failure(), SHA),
        company_model_ablation_active_predecessor=BoundArtifact(
            _active_ablation(learned_lift=False), SHA
        ),
    )

    component = result["components"]["company_model_ablation"]
    assert component["legacy_v4_development"]["verdict"] == "meets_policy"
    assert component["active_v5_contract_failure"]["verdict"] == (
        "inconclusive_runtime_contract_failure"
    )
    assert component["active_lane_verdict"] == "below_policy"
    assert component["verdict"] == "below_policy"
    assert result["verdict"] == "partial_evidence"
    assert result["artifact_bindings"]["company_model_ablation"][
        "current_active_holdout"
    ]["verdict"] == "below_policy"


def test_future_active_holdout_replaces_v6_without_erasing_history():
    result = compose_objective_company_learning_evidence(
        company_model_ablation=BoundArtifact(
            _active_ablation(version="v7"), SHA
        ),
        company_model_ablation_legacy=BoundArtifact(_ablation(), SHA),
        company_model_ablation_active_failure=BoundArtifact(_active_failure(), SHA),
        company_model_ablation_active_predecessor=BoundArtifact(
            _active_ablation(learned_lift=False), SHA
        ),
    )

    component = result["components"]["company_model_ablation"]
    assert component["governing_era"] == "v7"
    assert component["capability_claim"] == (
        "cross_batch_evidence_accumulation_and_availability"
    )
    assert component["synthesis_claim"] == "not_established"
    assert "v7_collective_facet_union_is_not_single_model_synthesis" in " ".join(
        result["proof_gaps"]
    )
    assert component["active_lane_verdict"] == "meets_policy"
    assert component["legacy_v4_development"]["verdict"] == "meets_policy"
    assert component["active_v5_contract_failure"] is not None
    assert component["superseded_active_holdout"]["verdict"] == "below_policy"


def test_legacy_ablation_without_active_holdout_remains_unknown():
    result = compose_objective_company_learning_evidence(
        company_model_ablation=BoundArtifact(_ablation(), SHA),
    )

    component = result["components"]["company_model_ablation"]
    assert component["status"] == "unknown"
    assert component["legacy_v4_development"]["verdict"] == "meets_policy"
    assert "component_unavailable:company_model_ablation.current_active_holdout" in (
        result["proof_gaps"]
    )


def test_preserves_historical_retrieval_failure_beside_current_postfix_pass():
    historical = _retrieval()
    historical["verdict"] = "below_policy"
    historical["continuous_score"] = 4 / 7
    historical["checks"]["late_is_model_preferred"] = False
    result = compose_objective_company_learning_evidence(
        retrieval_evolution=BoundArtifact(historical, SHA),
        retrieval_evolution_postfix=BoundArtifact(
            run_bounded_retrieval_evolution_postfix(), SHA),
    )

    retrieval = result["components"]["retrieval_evolution"]
    assert retrieval["historical_verdict"] == "below_policy"
    assert retrieval["current_bounded_verdict"] == "meets_preregistered_policy"
    assert retrieval["verdict"] == "current_meets_bounded_policy_historical_below_policy"
    assert result["historical_below_policy_components"] == ["retrieval_evolution"]


def test_missing_components_remain_unknown_and_reduce_coverage():
    result = compose_objective_company_learning_evidence(
        correction_homeostasis=BoundArtifact(_correction(), SHA)
    )

    assert result["verdict"] == "partial_evidence"
    assert result["evidence_coverage"] == 1 / 8
    assert result["components"]["retrieval_evolution"]["status"] == "unknown"
    assert "component_unavailable:retrieval_evolution.current_bounded_postfix" in result[
        "proof_gaps"
    ]


def test_noncompensable_safety_failure_overrides_high_scores():
    source = _source()
    source["checks"]["source_authority_preserved"] = False
    result = compose_objective_company_learning_evidence(
        source_equivalence=BoundArtifact(source, SHA),
        correction_homeostasis=BoundArtifact(_correction(), SHA),
    )

    assert result["verdict"] == "not_credible"
    assert "source_equivalence:source_authority_preserved" in result[
        "noncompensable_blockers"
    ]


def test_salience_feedback_cannot_substitute_for_missing_feedback_quality():
    result = compose_objective_company_learning_evidence(
        feedback_learning=BoundArtifact(_feedback(), SHA),
    )

    assert result["components"]["feedback_learning"]["status"] == "observed"
    assert result["components"]["feedback_quality"]["status"] == "unknown"
    assert "component_unavailable:feedback_quality" in result["proof_gaps"]
    assert result["verdict"] == "partial_evidence"


def test_feedback_quality_requires_exact_population_checks_and_digest():
    malformed = _feedback_quality()
    malformed["population"]["arms"] = 1
    _reseal(malformed)
    with pytest.raises(ValueError, match="matched population mismatch"):
        compose_objective_company_learning_evidence(
            feedback_quality=BoundArtifact(malformed, SHA),
        )

    malformed = _feedback_quality()
    malformed["checks"].pop("later_quality_improves")
    _reseal(malformed)
    with pytest.raises(ValueError, match="check set mismatch"):
        compose_objective_company_learning_evidence(
            feedback_quality=BoundArtifact(malformed, SHA),
        )

    malformed = _feedback_quality()
    malformed["continuous_score"] = 0.5
    with pytest.raises(ValueError, match="objective digest mismatch"):
        compose_objective_company_learning_evidence(
            feedback_quality=BoundArtifact(malformed, SHA),
        )


def test_db_source_equivalence_cannot_hide_missing_production_relations():
    result = compose_objective_company_learning_evidence(
        source_equivalence=BoundArtifact(_source_db(relations_exposed=False), SHA),
    )

    source = result["components"]["source_equivalence"]
    assert source["population"] == {
        "signal_batches": 1, "signals": 8, "sources": 4,
    }
    assert source["continuous_score"] == 2 / 3
    assert result["verdict"] == "not_credible"
    assert "source_equivalence:relation_outcomes_exposed" in result[
        "noncompensable_blockers"
    ]


def test_constructor_only_source_equivalence_is_not_objective_production_proof():
    result = compose_objective_company_learning_evidence(
        source_equivalence=BoundArtifact(run_bounded_source_equivalence(), SHA),
    )

    assert result["components"]["source_equivalence"]["verdict"] == "below_policy"
    assert "source_equivalence:production_relation_path_exercised" in result[
        "noncompensable_blockers"
    ]
