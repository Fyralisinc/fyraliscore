from __future__ import annotations

from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid4, uuid5

from services.evaluation.epistemic_repair.stage1_quality_scorer import score_stage1_company_memory


def test_scores_claim_scope_duplicate_prior_use_and_correction() -> None:
    tenant_id = uuid4()
    first = SimpleNamespace(signal_id="s1", batch_number=1, text="Atlas is blocked.")
    correction = SimpleNamespace(signal_id="s2", batch_number=2, text="Atlas is unblocked.")
    first_observation = str(uuid5(NAMESPACE_URL, f"p6-think:{tenant_id}:s1"))
    correction_observation = str(uuid5(NAMESPACE_URL, f"p6-think:{tenant_id}:s2"))
    raw = {
        "tenant_id": str(tenant_id), "completed_batches": 2,
        "population_digest": "digest",
        "frozen_outputs": {"accepted_models": [
            {
                "natural_text": first.text, "truth_version": 1,
                "proposition": {"abstraction_level": "atomic", "scope_ref": "workstream:atlas", "evidence_event_ids": [first_observation]},
            },
            {
                "natural_text": correction.text, "truth_version": 2,
                "proposition": {"abstraction_level": "composite", "lifecycle_phase": "correction", "scope_ref": "workstream:atlas", "evidence_event_ids": [correction_observation]},
            },
        ]},
        "waves": [
            {"batch_number": 1},
            {
                "batch_number": 2,
                "execution": {"run": {"ops_applied": {"context_use": {
                    "model_context_used": True,
                }}}},
            },
        ],
    }
    report = score_stage1_company_memory(
        raw, signals=(first, correction),
        expected_scope_by_signal={"s1": "workstream:atlas", "s2": "workstream:atlas"},
        expected_claim_signal_ids=frozenset({"s1", "s2"}),
        correction_signal_id="s2", expected_correction=correction.text,
    )

    assert report["metrics"]["exact_claim_precision"]["value"] == 1.0
    assert report["metrics"]["canonical_scope_recall"]["value"] == 1.0
    assert report["metrics"]["duplicate_exact_claim_avoidance"]["value"] == 1.0
    assert report["metrics"]["prior_model_use"]["value"] == 1.0
    assert report["metrics"]["correction_in_place_accuracy"]["value"] == 1.0


def test_unresolved_scope_and_duplicate_exact_claims_reduce_scores() -> None:
    tenant_id = uuid4()
    signal = SimpleNamespace(signal_id="s1", batch_number=1, text="Atlas is blocked.")
    observation = str(uuid5(NAMESPACE_URL, f"p6-think:{tenant_id}:s1"))
    model = {
        "natural_text": signal.text, "truth_version": 1,
        "proposition": {"abstraction_level": "atomic", "scope_ref": "mention:one", "evidence_event_ids": [observation]},
    }
    report = score_stage1_company_memory(
        {"tenant_id": str(tenant_id), "completed_batches": 1, "population_digest": "digest", "frozen_outputs": {"accepted_models": [model, model]}, "waves": []},
        signals=(signal,), expected_scope_by_signal={"s1": "workstream:atlas"},
        expected_claim_signal_ids=frozenset({"s1"}),
    )

    assert report["metrics"]["exact_claim_precision"]["value"] == 0.5
    assert report["metrics"]["canonical_scope_recall"]["value"] == 0.0
    assert report["metrics"]["duplicate_exact_claim_avoidance"]["value"] == 0.5
