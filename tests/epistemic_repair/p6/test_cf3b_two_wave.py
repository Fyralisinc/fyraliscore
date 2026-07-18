from __future__ import annotations

from copy import deepcopy

from services.evaluation.epistemic_repair.cf3b_two_wave import (
    evaluate_cf3b_two_wave,
)


def _model(model_id: str, version_id: str, *, evidence: bool = True) -> dict:
    proposition = {}
    if evidence:
        proposition = {
            "evidence_event_ids": ["observation-1"],
            "evidence_contract": {
                "evidence_status": "evidence_bound",
                "supporting_event_count": 1,
            },
        }
    return {"id": model_id, "truth_version_id": version_id, "proposition": proposition}


def _wave(number: int, models: list[dict], *, context_use: dict | None = None) -> dict:
    trigger_id = f"trigger-{number}"
    decisions = []
    if number == 2:
        decisions = [{
            "batch_id": trigger_id,
            "context_item_kind": "accepted_model",
            "context_item_id": "model-1",
            "selected": True,
            "referenced": True,
        }]
    return {
        "batch_number": number,
        "status": "success",
        "elapsed_s": 2.5,
        "execution": {
            "trigger_id": trigger_id,
            "run": {
                "status": "success",
                "ops_applied": {"context_use": context_use or {}},
            },
        },
        "snapshot": {
            "accepted_models": models,
            "context_decisions": decisions,
            "pending_work": {"truth_critical": {"total": 0}},
        },
        "barrier_receipt": {
            "barrier_id": f"barrier-{number}",
            "receipt_digest": "a" * 64,
            "reopened_exactly": True,
            "truth_critical_pending_count": 0,
        },
    }


def _artifact() -> dict:
    model = _model("model-1", "version-1")
    context = {
        "selected_model_ids": ["model-1"],
        "referenced_model_ids": ["model-1"],
        "trace_referenced_model_ids": ["model-1"],
        "reasoning_trace_context_decision_used": True,
        "prior_memory_effects": [{
            "source": "prior_memory_effect",
            "effect_scope": "candidate",
            "candidate_id": "candidate-2",
            "relation": "supports",
            "prior_model_id": "model-1",
            "action": "confirm",
            "material": True,
            "reasoning_trace_accounted": True,
        }],
    }
    return {
        "complete": True,
        "completed_batches": 2,
        "elapsed_s": 5.0,
        "provider_mode": "codex_cli",
        "expected_llm_configuration": {
            "provider": "codex",
            "model": "gpt-test-codex",
            "transport": "cli",
        },
        "llm_attempt_receipts": [{
            "provider": "codex",
            "model": "gpt-test-codex",
            "physical_attempt_id": "attempt-1",
            "usage_exactness": "reported",
            "input_tokens": 100,
            "output_tokens": 20,
            "cache_tokens": 10,
        }],
        "waves": [_wave(1, [model]), _wave(2, [model], context_use=context)],
        "founder_identity_bootstrap": {
            "applied_before_enqueue": True,
            "semantic_truth_unchanged": True,
            "manifest_digest": "b" * 64,
            "alias_count": 4,
            "no_behavioral_models_seeded": True,
        },
    }


def test_green_requires_exact_material_wave_one_version_use() -> None:
    report = evaluate_cf3b_two_wave(_artifact())

    assert report["verdict"] == "green"
    assert all(report["gates"].values())
    assert report["measurements"]["b2_materially_used_b1_version_ids"] == [
        "version-1"
    ]
    assert report["measurements"]["b1_evidence_backed_ratio"] == 1.0


def test_lifecycle_only_reference_does_not_earn_material_use_credit() -> None:
    artifact = _artifact()
    context = artifact["waves"][1]["execution"]["run"]["ops_applied"]["context_use"]
    context["trace_referenced_model_ids"] = []
    context["reasoning_trace_context_decision_used"] = False

    report = evaluate_cf3b_two_wave(artifact)

    assert report["measurements"]["b2_referenced_b1_model_count"] == 1
    assert report["measurements"]["b2_materially_used_b1_model_count"] == 0
    assert report["gates"]["b2_materially_uses_exact_b1_model_version"] is False
    assert report["verdict"] == "red"


def test_even_trace_reference_cannot_promote_lifecycle_only_bookkeeping() -> None:
    artifact = _artifact()
    artifact["waves"][1]["execution"]["run"]["ops_applied"][
        "memory_lifecycle_ops"
    ] = [{"model_id": "model-1", "operation": "review"}]
    artifact["waves"][1]["execution"]["run"]["ops_applied"]["context_use"][
        "prior_memory_effects"
    ] = []

    report = evaluate_cf3b_two_wave(artifact)

    assert report["measurements"][
        "b2_lifecycle_only_referenced_b1_model_count"
    ] == 1
    assert report["gates"]["b2_materially_uses_exact_b1_model_version"] is False
    assert report["verdict"] == "red"


def test_generic_or_unchanged_effect_envelope_cannot_earn_material_credit() -> None:
    artifact = _artifact()
    context = artifact["waves"][1]["execution"]["run"]["ops_applied"]["context_use"]
    context["prior_memory_effects"] = [
        {
            "source": "representation_contract",
            "effect_scope": "candidate",
            "candidate_id": "candidate-2",
            "relation": "supports",
            "prior_model_id": "model-1",
            "action": "confirm",
            "material": True,
            "reasoning_trace_accounted": True,
        },
        {
            "source": "prior_memory_effect",
            "effect_scope": "candidate",
            "candidate_id": "candidate-2",
            "relation": "unchanged",
            "prior_model_id": "model-1",
            "action": "unchanged",
            "material": False,
            "reasoning_trace_accounted": True,
        },
    ]

    report = evaluate_cf3b_two_wave(artifact)

    assert report["measurements"]["b2_authorized_prior_memory_effect_count"] == 0
    assert report["gates"]["b2_materially_uses_exact_b1_model_version"] is False
    assert report["verdict"] == "red"


def test_effect_must_be_accounted_for_by_provider_reasoning() -> None:
    artifact = _artifact()
    context = artifact["waves"][1]["execution"]["run"]["ops_applied"]["context_use"]
    context["prior_memory_effects"][0]["reasoning_trace_accounted"] = False

    report = evaluate_cf3b_two_wave(artifact)

    assert report["measurements"]["b2_authorized_prior_memory_effect_count"] == 0
    assert report["gates"]["b2_materially_uses_exact_b1_model_version"] is False


def test_provider_free_artifact_cannot_overclaim_green() -> None:
    artifact = _artifact()
    artifact["provider_mode"] = "provider_free"
    artifact["expected_llm_configuration"] = {
        "provider": "scripted",
        "model": "provider-free-v1",
        "transport": "in_process",
    }

    report = evaluate_cf3b_two_wave(artifact)

    assert report["gates"]["provider_evidence_eligible"] is False
    assert report["verdict"] == "red"


def test_provider_proof_fails_closed_when_configuration_is_missing() -> None:
    artifact = _artifact()
    artifact.pop("expected_llm_configuration")

    report = evaluate_cf3b_two_wave(artifact)

    assert report["gates"]["provider_evidence_eligible"] is False
    assert report["measurements"]["expected_llm_provider"] is None
    assert report["verdict"] == "red"


def test_provider_proof_rejects_mismatch_or_estimated_usage() -> None:
    artifact = _artifact()
    artifact["llm_attempt_receipts"].append({
        "provider": "codex",
        "model": "another-model",
        "physical_attempt_id": "attempt-2",
        "usage_exactness": "estimated",
        "input_tokens": 50,
        "output_tokens": 5,
        "cache_tokens": 0,
    })

    report = evaluate_cf3b_two_wave(artifact)

    assert report["measurements"]["llm_attempt_receipt_count"] == 2
    assert report["measurements"][
        "llm_matching_reported_usage_receipt_count"
    ] == 1
    assert report["measurements"]["llm_matching_reported_usage_ratio"] == 0.5
    assert report["gates"]["provider_evidence_eligible"] is False


def test_each_hard_contract_failure_is_visible_as_its_own_gate() -> None:
    artifact = deepcopy(_artifact())
    artifact["waves"][0]["snapshot"]["accepted_models"] = [
        _model("model-1", "version-1", evidence=False)
    ]
    artifact["waves"][1]["barrier_receipt"]["truth_critical_pending_count"] = 1
    artifact["founder_identity_bootstrap"]["alias_count"] = 0

    report = evaluate_cf3b_two_wave(artifact)

    assert report["gates"]["b1_has_evidence_backed_model"] is False
    assert report["gates"]["both_barriers_complete_pending_zero"] is False
    assert report["gates"]["founder_bootstrap_receipt_valid"] is False
    assert report["verdict"] == "red"


def test_missing_or_extra_wave_is_not_reinterpreted_as_cf3b() -> None:
    artifact = _artifact()
    artifact["waves"].append(deepcopy(artifact["waves"][1]))
    artifact["completed_batches"] = 3

    report = evaluate_cf3b_two_wave(artifact)

    assert report["gates"]["exactly_two_successful_waves"] is False
    assert report["verdict"] == "red"
