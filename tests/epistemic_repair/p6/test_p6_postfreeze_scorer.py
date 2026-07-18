from __future__ import annotations

import ast
from pathlib import Path

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from lib.evaluation.epistemic_repair.p6_postfreeze_scorer import (
    _score_boundaries,
    score_p6_frozen_execution,
)


def _raw(population, *, evidence=None):
    return {
        "schema_version": "epistemic-repair-p6-production-think-v1",
        "population_digest": population.population_digest,
        "complete": True,
        "waves": [{
            "batch_number": batch,
            "status": "success",
            "execution": {
                "member_count": 25, "observation_count": 25,
                "elapsed_s": 1.0,
            },
            "barrier_receipt": {
                "truth_critical_pending_count": 0,
                "reopened_exactly": True,
            },
            "elapsed_s": 1.0,
        } for batch in range(1, 13)],
        "llm_attempt_receipts": [],
        "expected_llm_configuration": {"provider": "codex", "model": "gpt-5.4"},
        "mixed_llm_attempt_count": 0,
        "run_provenance": {"git_commit": "a" * 40, "worktree_clean": True},
        "postfreeze_evidence": evidence or {},
    }


def test_scorer_is_pure_and_production_runner_does_not_import_it() -> None:
    production = Path("lib/evaluation/epistemic_repair/p6_think_runner.py").read_text()
    imports = {
        alias.name
        for node in ast.walk(ast.parse(production))
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert "p6_postfreeze_scorer" not in imports
    scorer = Path(
        "lib/evaluation/epistemic_repair/p6_postfreeze_scorer.py"
    ).read_text()
    assert "asyncpg" not in scorer
    assert "build_provider" not in scorer
    extractor = Path(
        "lib/evaluation/epistemic_repair/p6_postfreeze_evidence.py"
    ).read_text()
    assert "p6_population" not in extractor
    assert "build_p6_population" not in extractor
    assert "build_provider" not in extractor


def test_current_raw_shape_fails_closed_on_unpreserved_member_evidence() -> None:
    population = build_p6_population()
    report = score_p6_frozen_execution(
        raw_execution=_raw(population), sealed_population=population,
    )
    assert set(report["continuous_metrics"]) == {
        "boundary_b_cubed_f1", "selected_context_contamination",
        "sufficient_context_recall", "exact_mention_f1", "entity_type_accuracy",
        "canonical_link_precision", "canonical_link_recall",
        "atomic_claim_precision", "atomic_claim_recall", "atomic_claim_f1",
        "evidence_lineage_coverage", "scope_precision", "scope_recall",
        "direct_thesis_accuracy", "mean_thesis_facet_completeness",
        "relation_joint_precision", "relation_joint_recall",
        "lifecycle_expected_transition_accuracy",
        "historical_reopening_reason_coverage", "mature_actual_model_use_share",
        "mature_unnecessary_historical_observation_use",
        "resolved_outcome_model_ece", "resolved_outcome_model_brier",
        "selected_context_utilization", "false_model_relation_from_noise",
        "duplicate_causal_credit_fanout", "clean_t1_p95_seconds",
        "clean_max_over_median", "metered_llm_calls_per_signal",
        "question_planning_batch_share", "truth_critical_pending_at_barriers",
        "refresh_key_duplicate_processing_ratio",
    }
    assert "atomic_claim_f1" in report["missing_evidence"]
    assert not report["hard_gates"]["complete_signal_fates"]
    assert not report["phase_exit_ready"]
    assert len(report["input_digests"]["raw_execution"]) == 64
    assert len(report["content_digest"]) == 64


def test_missing_batch_member_count_cannot_infer_exact_300() -> None:
    population = build_p6_population()
    raw = _raw(population)
    del raw["waves"][0]["execution"]["member_count"]
    report = score_p6_frozen_execution(
        raw_execution=raw, sealed_population=population,
    )
    assert not report["hard_gates"]["exact_300_signals_12_batches"]


def test_immutable_population_digest_mismatch_is_a_hard_failure() -> None:
    population = build_p6_population()
    raw = _raw(population)
    raw["population_digest"] = "0" * 64
    report = score_p6_frozen_execution(
        raw_execution=raw, sealed_population=population,
    )
    assert not report["hard_gates"]["immutable_inputs_match"]
    assert not report["phase_exit_ready"]


def test_boundary_b_cubed_uses_all_exact_source_ids() -> None:
    population = build_p6_population()
    boundaries = [{
        "signal_id": item.signal_id,
        "predicted_boundary_id": item.storyline_id or item.signal_id,
    } for item in population.gold]
    score = _score_boundaries(
        _raw(population, evidence={"boundaries": boundaries}), population,
    )["boundary_b_cubed_f1"]
    assert score["value"] == 1.0
    assert score["denominator"] == 1
    assert len(score["source_ids"]) == 300


def test_calibration_under_twenty_is_insufficient_not_passed() -> None:
    population = build_p6_population()
    outcomes = [item for item in population.gold if item.lifecycle_phase == "external_outcome"]
    evidence = {
        "resolved_outcomes": [{
            "outcome_signal_id": item.signal_id,
            "model_id": f"model-{index}",
            "confidence": 0.8,
        } for index, item in enumerate(outcomes[:19])],
    }
    report = score_p6_frozen_execution(
        raw_execution=_raw(population, evidence=evidence),
        sealed_population=population,
    )
    assert report["continuous_metrics"]["resolved_outcome_model_ece"]["status"] == "insufficient_population"
    assert report["continuous_metrics"]["resolved_outcome_model_brier"]["status"] == "insufficient_population"


def test_context_recall_does_not_infer_denominator_from_selected_rows() -> None:
    population = build_p6_population()
    evidence = {"context_items": [{
        "selected": True,
        "source_signal_id": population.signals[0].signal_id,
    }]}
    report = score_p6_frozen_execution(
        raw_execution=_raw(population, evidence=evidence),
        sealed_population=population,
    )
    assert report["continuous_metrics"]["sufficient_context_recall"]["status"] == "unmeasured"
    assert report["continuous_metrics"]["selected_context_contamination"]["status"] == "unmeasured"


def test_postfreeze_evidence_digest_and_receipts_are_a_hard_gate() -> None:
    population = build_p6_population()
    evidence = {"query_receipts": [{
        "query_name": "observations", "row_count": 300, "result_digest": "a" * 64,
    }]}
    evidence["source_digest"] = canonical_sha256(evidence)
    report = score_p6_frozen_execution(
        raw_execution=_raw(population, evidence=evidence),
        sealed_population=population,
    )
    assert report["hard_gates"]["postfreeze_evidence_digest_valid"]

    evidence["query_receipts"][0]["row_count"] = 299
    report = score_p6_frozen_execution(
        raw_execution=_raw(population, evidence=evidence),
        sealed_population=population,
    )
    assert not report["hard_gates"]["postfreeze_evidence_digest_valid"]


def test_estimated_usage_cannot_pass_exact_token_receipt_gate() -> None:
    population = build_p6_population()
    raw = _raw(population)
    receipts = []
    for index, wave in enumerate(raw["waves"], start=1):
        run_id = f"run-{index}"
        wave["execution"]["run"] = {"id": run_id}
        receipts.append({
            "physical_attempt_id": f"attempt-{index}",
            "think_run_id": run_id,
            "provider": "codex",
            "model": "gpt-5.4",
            "usage_exactness": "estimated",
            "input_tokens": 10,
            "output_tokens": 10,
        })
    raw["llm_attempt_receipts"] = receipts
    report = score_p6_frozen_execution(
        raw_execution=raw, sealed_population=population,
    )
    assert report["hard_gates"]["durable_call_receipts"]
    assert not report["hard_gates"]["exact_token_usage_receipts"]
    assert report["continuous_metrics"]["metered_llm_calls_per_signal"]["value"] == 12 / 300
