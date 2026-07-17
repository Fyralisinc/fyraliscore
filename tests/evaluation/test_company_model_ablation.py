from copy import deepcopy

import pytest

from lib.evaluation.company_model_ablation import (
    evaluate_company_model_ablation,
    evaluate_single_model_synthesis,
    manifest_digest,
)


def _evidence() -> tuple[dict, dict, dict]:
    manifest = {
        "schema_version": "company-model-hidden-truth-v1",
        "experiment_id": "sealed-bounded-world-v1",
        "judge_id": "independent-hidden-world-judge-v1",
        "hidden_theses": [
            {"thesis_id": "renewal", "truth": "security evidence blocks renewal"},
            {"thesis_id": "capacity", "truth": "capacity causes onboarding slip"},
            {"thesis_id": "pricing", "truth": "pricing changed off sensor"},
        ],
    }
    batches = [
        {"batch_id": f"batch-{index}", "signal_ids": [f"s{index}-a", f"s{index}-b"]}
        for index in range(1, 4)
    ]
    learned = {
        "schema_version": "company-model-ablation-arm-v1",
        "arm": "learned_memory",
        "producer_id": "company-runtime-v1",
        "truth_visible_to_producer": False,
        "hidden_truth_digest": manifest_digest(manifest),
        "batches": batches,
        "predictions": [
            {"thesis_id": key, "recovered": True, "confidence": 0.8, "future_outcomes": [1, 1, 1, 1]}
            for key in ("renewal", "capacity", "pricing")
        ],
        "safety_incidents": [],
    }
    frozen = {
        **deepcopy(learned),
        "arm": "frozen_memory",
        "predictions": [
            {"thesis_id": key, "recovered": False, "confidence": 0.2, "future_outcomes": [0, 0, 0, 0]}
            for key in ("renewal", "capacity", "pricing")
        ],
    }
    return manifest, learned, frozen


def test_ablation_measures_recovery_lift_and_calibration() -> None:
    manifest, learned, frozen = _evidence()
    report = evaluate_company_model_ablation(
        manifest=manifest, learned=learned, frozen=frozen
    )
    assert report["verdict"] == "meets_policy"
    assert report["effects"]["recovery_rate_lift"] == 1.0
    assert report["effects"]["direction"] == "learned_better"
    assert report["arms"]["learned_memory"]["calibration_n"] == 12
    assert report["batch_count"] == 3


def test_ablation_rejects_different_signal_exposure() -> None:
    manifest, learned, frozen = _evidence()
    frozen["batches"][0]["signal_ids"].append("extra")
    with pytest.raises(ValueError, match="exact same ordered batches"):
        evaluate_company_model_ablation(
            manifest=manifest, learned=learned, frozen=frozen
        )


def test_ablation_rejects_truth_visible_to_producer() -> None:
    manifest, learned, frozen = _evidence()
    learned["truth_visible_to_producer"] = True
    with pytest.raises(ValueError, match="hidden truth was not visible"):
        evaluate_company_model_ablation(
            manifest=manifest, learned=learned, frozen=frozen
        )


def test_ablation_rejects_incomplete_thesis_denominator() -> None:
    manifest, learned, frozen = _evidence()
    learned["predictions"].pop()
    with pytest.raises(ValueError, match="cover every sealed hidden thesis"):
        evaluate_company_model_ablation(
            manifest=manifest, learned=learned, frozen=frozen
        )


def test_ablation_fails_policy_when_learning_has_no_lift() -> None:
    manifest, learned, frozen = _evidence()
    learned["predictions"] = deepcopy(frozen["predictions"])
    report = evaluate_company_model_ablation(
        manifest=manifest, learned=learned, frozen=frozen
    )
    assert report["verdict"] == "below_policy"
    assert report["checks"]["learning_has_causal_direction"] is False


def test_ablation_rejects_non_genuine_duplicate_signal_batches() -> None:
    manifest, learned, frozen = _evidence()
    learned["batches"][1]["signal_ids"][0] = "s1-a"
    frozen["batches"] = deepcopy(learned["batches"])
    with pytest.raises(ValueError, match="exactly one genuine batch"):
        evaluate_company_model_ablation(
            manifest=manifest, learned=learned, frozen=frozen
        )


def test_synthesis_requires_one_complete_persisted_model_with_prior_lineage() -> None:
    manifest = {"schema_version": "company-model-synthesis-manifest-v1",
        "hidden_patterns": [{"thesis_id": "renewal",
            "required_facets": ["usage_drop", "security_hold"]}]}
    learned = {"schema_version": "company-model-synthesis-arm-v1",
        "arm": "learned_memory", "prior_model_ids": ["prior-a", "prior-b"],
        "required_lineage_by_thesis": {"renewal": ["prior-a", "prior-b"]},
        "models": [{"model_id": "synthesis-1", "thesis_id": "renewal",
            "facets": ["usage_drop", "security_hold"],
            "evidence_model_ids": ["prior-a", "prior-b"], "persisted": True}]}
    frozen = {"schema_version": "company-model-synthesis-arm-v1",
        "arm": "frozen_memory", "prior_model_ids": [],
        "required_lineage_by_thesis": {"renewal": []}, "models": []}

    report = evaluate_single_model_synthesis(
        manifest=manifest, learned=learned, frozen=frozen)

    assert report["verdict"] == "meets_policy"
    assert report["synthesis_lift"] == 1.0


def test_collective_facets_across_models_do_not_count_as_synthesis() -> None:
    manifest = {"schema_version": "company-model-synthesis-manifest-v1",
        "hidden_patterns": [{"thesis_id": "renewal",
            "required_facets": ["usage_drop", "security_hold"]}]}
    learned = {"schema_version": "company-model-synthesis-arm-v1",
        "arm": "learned_memory", "prior_model_ids": ["prior-a", "prior-b"],
        "required_lineage_by_thesis": {"renewal": ["prior-a", "prior-b"]},
        "models": [
            {"model_id": "m1", "thesis_id": "renewal", "facets": ["usage_drop"],
             "evidence_model_ids": ["prior-a"], "persisted": True},
            {"model_id": "m2", "thesis_id": "renewal", "facets": ["security_hold"],
             "evidence_model_ids": ["prior-b"], "persisted": True},
        ]}
    frozen = {"schema_version": "company-model-synthesis-arm-v1",
        "arm": "frozen_memory", "prior_model_ids": [],
        "required_lineage_by_thesis": {"renewal": []}, "models": []}

    report = evaluate_single_model_synthesis(
        manifest=manifest, learned=learned, frozen=frozen)

    assert report["verdict"] == "below_policy"
    assert report["arms"]["learned_memory"]["recovered_count"] == 0
