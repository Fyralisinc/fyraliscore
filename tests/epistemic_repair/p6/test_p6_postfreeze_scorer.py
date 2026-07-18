from __future__ import annotations

import ast
from pathlib import Path

import pytest

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from lib.evaluation.epistemic_repair.p6_postfreeze_scorer import (
    _score_boundaries,
    _score_claims_and_theses,
    _score_context,
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
    production = Path("services/evaluation/epistemic_repair/p6_think_runner.py").read_text()
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


def test_atomic_source_oracle_accepts_exact_ownership_one_ref() -> None:
    population = build_p6_population()
    evidence = {
        "claims": [
            {
                "id": "atlas-owner",
                "natural_text": (
                    "Atlas release certificate has no clearly recorded owner."
                ),
                "proposition": {
                    "kind": "belief",
                    "claim_role": "fact",
                    "subject": "Atlas release certificate",
                    "assertion": "Ownership remains unresolved.",
                },
                "evidence_signal_ids": ["p6-b01-s01"],
                "scope_entities": [
                    {"canonical_ref": "workstream:atlas-release"}
                ],
            }
        ]
    }

    scores = _score_claims_and_theses(
        {"postfreeze_evidence": evidence}, population
    )

    assert scores["atomic_claim_precision"]["value"] == 1.0
    assert scores["atomic_claim_recall"]["numerator"] == 1
    assert scores["atomic_claim_recall"]["denominator"] == 92
    assert scores["atomic_claim_recall"]["value"] > 0
    assert scores["direct_thesis_accuracy"]["value"] == 0


def test_atomic_recall_denominator_is_limited_to_observed_one_batch() -> None:
    population = build_p6_population()
    signal_by_id = {signal.signal_id: signal for signal in population.signals}
    batch_one_gold = [
        item for item in population.gold
        if signal_by_id[item.signal_id].batch_number == 1
        and item.claim_id
        and item.role not in {"noise", "high_similarity_distractor"}
        and not (
            item.lifecycle_phase == "weak_initial"
            and item.claim_id.rsplit(":", 1)[-1] in {"2", "4"}
        )
    ]
    # Batch one has twelve directly assertable atomic coordinates. Represent
    # eleven to exercise the real one-batch smoke's 11/12 recall contract.
    represented = batch_one_gold[:11]
    observed_map = {
        f"observation-{index}": signal.signal_id
        for index, signal in enumerate(population.signals[:25])
    }
    evidence = {
        "observed_source_ids": list(observed_map),
        "observation_signal_map": observed_map,
        "claims": [
            {
                "id": f"model-{item.signal_id}",
                "natural_text": signal_by_id[item.signal_id].text,
                "proposition": {},
                "evidence_signal_ids": [item.signal_id],
                "scope_entities": [{"canonical_ref": item.canonical_ref}],
            }
            for item in represented
        ],
    }
    raw = _raw(population, evidence=evidence)
    raw["waves"] = raw["waves"][:1]

    recall = _score_claims_and_theses(raw, population)["atomic_claim_recall"]

    assert recall["numerator"] == 11
    assert recall["denominator"] == 12
    assert recall["value"] == pytest.approx(11 / 12)
    assert recall["status"] == "pass"


def test_atomic_recall_full_run_denominator_remains_sealed_population() -> None:
    population = build_p6_population()
    evidence = {
        "claims": [{
            "id": "atlas-owner",
            "natural_text": "Atlas release certificate has no clearly recorded owner.",
            "proposition": {},
            "evidence_signal_ids": ["p6-b01-s01"],
            "scope_entities": [{"canonical_ref": "workstream:atlas-release"}],
        }]
    }

    recall = _score_claims_and_theses(
        _raw(population, evidence=evidence), population,
    )["atomic_claim_recall"]

    assert recall["denominator"] == 92


def test_broad_causal_claim_with_all_five_refs_fails_atomic_precision() -> None:
    population = build_p6_population()
    evidence = {
        "claims": [
            {
                "id": "atlas-broad-causal",
                "natural_text": (
                    "Atlas release slips recur because certificate ownership "
                    "changes during handoff."
                ),
                "proposition": {
                    "kind": "belief",
                    "claim_role": "fact",
                    "assertion": (
                        "Certificate ownership handoff causes release delay."
                    ),
                },
                "evidence_signal_ids": [
                    "p6-b01-s01",
                    "p6-b01-s05",
                    "p6-b01-s09",
                    "p6-b01-s13",
                    "p6-b01-s17",
                ],
                "scope_entities": [
                    {"canonical_ref": "workstream:atlas-release"}
                ],
            }
        ]
    }

    scores = _score_claims_and_theses(
        {"postfreeze_evidence": evidence}, population
    )

    assert scores["atomic_claim_precision"]["value"] == 0
    assert scores["atomic_claim_recall"]["value"] == 0
    assert scores["mean_thesis_facet_completeness"]["value"] > 0


def test_provisional_typed_scope_scores_without_claiming_canonical_resolution() -> None:
    population = build_p6_population()
    scopes = (
        ("workstream", "workstream:atlas-release"),
        ("workstream", "workstream:beacon-migration"),
        ("commitment", "commitment:cobalt-renewal"),
        ("workstream", "workstream:delta-handoff"),
    )
    evidence = {
        "claims": [{
            "id": f"scope-{index}",
            "natural_text": "A scoped observation was extracted.",
            "proposition": {"kind": "belief", "claim_role": "fact"},
            "evidence_signal_ids": [],
            "scope_entities": [{
                "canonical_ref": canonical_ref,
                "canonical_ref_status": "provisional",
                "type": entity_type,
            }],
        } for index, (entity_type, canonical_ref) in enumerate(scopes)],
        "extracted_scope_coordinates_complete": True,
        "extracted_scope_coordinates_status": "complete",
        "extracted_scope_coordinate_counts": {
            "total": 4, "typed": 4, "resolved": 0,
            "provisional": 4, "incomplete": 0,
        },
        "scope_coordinates_canonical": False,
    }

    scores = _score_claims_and_theses(
        {"postfreeze_evidence": evidence}, population
    )
    report = score_p6_frozen_execution(
        raw_execution=_raw(population, evidence=evidence),
        sealed_population=population,
    )

    assert scores["scope_precision"]["value"] == 1.0
    assert scores["scope_recall"]["value"] == 1.0
    assert evidence["scope_coordinates_canonical"] is False
    assert report["continuous_metrics"]["canonical_link_precision"]["status"] == "unmeasured"
    assert report["continuous_metrics"]["canonical_link_recall"]["status"] == "unmeasured"


def test_scope_metrics_fail_closed_without_complete_typed_extraction() -> None:
    population = build_p6_population()
    evidence = {
        "claims": [{
            "id": "untyped-scope", "natural_text": "Atlas changed.",
            "proposition": {"kind": "belief", "claim_role": "fact"},
            "evidence_signal_ids": [],
            "scope_entities": [{
                "canonical_ref": "workstream:atlas-release",
                "canonical_ref_status": "provisional",
            }],
        }],
        "extracted_scope_coordinates_complete": False,
        "extracted_scope_coordinates_status": "partial",
        "scope_coordinates_canonical": False,
    }

    scores = _score_claims_and_theses(
        {"postfreeze_evidence": evidence}, population
    )

    assert scores["scope_precision"]["status"] == "unmeasured"
    assert scores["scope_recall"]["status"] == "unmeasured"


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


def test_mixed_batch_context_is_scored_once_per_retrieval_not_cartesian_targets() -> None:
    population = build_p6_population()
    gold = {item.signal_id: item for item in population.gold}
    by_story_batch = {
        (item.storyline_id, signal.batch_number): item.signal_id
        for signal, item in zip(population.signals, population.gold, strict=True)
        if item.storyline_id
    }
    current_atlas = by_story_batch[("atlas", 6)]
    historical_atlas = by_story_batch[("atlas", 2)]
    historical_beacon = by_story_batch[("beacon", 2)]
    noise_id = next(
        item.signal_id for item in population.gold
        if item.role == "noise" and next(
            signal.batch_number for signal in population.signals
            if signal.signal_id == item.signal_id
        ) == 2
    )
    input_ids = [
        signal.signal_id for signal in population.signals if signal.batch_number == 6
    ]

    def row(item_id: str, source_id: str, *, referenced: bool) -> dict[str, object]:
        return {
            "decision_id": item_id, "context_item_id": item_id,
            "think_run_id": "run-6", "selected": True,
            "input_signal_ids": input_ids, "source_signal_ids": [source_id],
            "output_evidence_signal_ids": [current_atlas], "batch_number": 6,
            "context_item_kind": "observation", "referenced": referenced,
            "historical_reopen_reason": "claim_evidence_reopen",
            "necessary_background": referenced,
        }

    rows = [
        row("current-input", current_atlas, referenced=True),
        row("atlas-history", historical_atlas, referenced=True),
        row("beacon-history", historical_beacon, referenced=False),
        row("noise-history", noise_id, referenced=False),
    ]
    scores = _score_context({
        "postfreeze_evidence": {
            "context_items": rows, "context_opportunities_complete": True,
        }
    }, population)
    assert gold[historical_beacon].storyline_id != gold[current_atlas].storyline_id
    assert scores["selected_context_contamination"]["numerator"] == 1
    assert scores["selected_context_contamination"]["denominator"] == 3
    assert scores["sufficient_context_recall"]["value"] == 1.0
    assert scores["sufficient_context_recall"]["denominator"] == 1
    assert scores["selected_context_utilization"]["numerator"] == 1
    assert scores["selected_context_utilization"]["denominator"] == 3


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


@pytest.mark.parametrize("usage_exactness", ("estimated", "unavailable"))
def test_nonreported_usage_cannot_pass_exact_token_receipt_gate(
    usage_exactness: str,
) -> None:
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
            "usage_exactness": usage_exactness,
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


def test_provider_reported_usage_passes_exact_token_receipt_gate() -> None:
    population = build_p6_population()
    raw = _raw(population)
    raw["llm_attempt_receipts"] = []
    for index, wave in enumerate(raw["waves"], start=1):
        run_id = f"run-{index}"
        wave["execution"]["run"] = {"id": run_id}
        raw["llm_attempt_receipts"].append({
            "physical_attempt_id": f"attempt-{index}", "think_run_id": run_id,
            "provider": "codex", "model": "gpt-5.4",
            "usage_exactness": "reported", "input_tokens": 10, "output_tokens": 10,
        })
    report = score_p6_frozen_execution(
        raw_execution=raw, sealed_population=population,
    )
    assert report["hard_gates"]["exact_token_usage_receipts"]


def test_string_proposition_from_live_accepted_view_does_not_crash() -> None:
    population = build_p6_population()
    evidence = {
        "claims": [{
            "id": "live-claim", "proposition": "Project status changed.",
            "natural_text": "Project status changed.",
            "evidence_signal_ids": [population.signals[0].signal_id],
        }],
        "relations": [],
    }
    report = score_p6_frozen_execution(
        raw_execution=_raw(population, evidence=evidence),
        sealed_population=population,
    )
    assert report["hard_gates"]["wrapper_control_models_zero"]
