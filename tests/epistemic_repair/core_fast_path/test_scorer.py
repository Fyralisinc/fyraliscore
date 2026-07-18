from __future__ import annotations

from copy import deepcopy

import pytest

from lib.evaluation.epistemic_repair.core_fast_path_gold import (
    build_core_fast_path_gold,
)
from lib.evaluation.epistemic_repair.core_fast_path_population import (
    build_core_fast_path_population,
)
from services.evaluation.epistemic_repair.core_fast_path_scorer import (
    REQUIRED_RUNTIME_FIELDS,
    score_core_fast_path,
)


TENANT = "00000000-0000-0000-0000-000000000001"


def perfect_receipt() -> dict:
    population = build_core_fast_path_population()
    gold = build_core_fast_path_gold()
    gold_by_id = {item.signal_id: item for item in gold.signals}
    batches = []
    for source_batch in population.batches:
        signal_ids = [item.signal_id for item in source_batch.signals]
        groundings = []
        atomics = []
        for signal_id in signal_ids:
            expected = gold_by_id[signal_id]
            if expected.canonical_ref is None:
                continue
            groundings.append({
                "signal_id": signal_id,
                "canonical_ref": expected.canonical_ref,
                "surface": expected.expected_surface,
                "authority": expected.expected_authority,
            })
            atomics.append({
                "signal_id": signal_id,
                "observation_id": f"observation:{signal_id}",
                "evidence_bound": True,
                "tenant_id": TENANT,
            })
        models = []
        relations = []
        if source_batch.batch_number == 3:
            models.append({
                "model_id": "model:harbor-synthesis",
                "version_id": "version:harbor-synthesis:1",
                "source_signal_id": gold.synthesis_signal_id,
                "proposition": gold.expected_thesis,
                "natural_text": gold.expected_thesis,
                "abstraction_level": "composite",
                "claim_role": "situation",
                "lifecycle": "active",
                "scope_refs": [gold.expected_scope_ref],
                "evidence_signal_ids": [gold.synthesis_signal_id],
                "supporting_model_version_ids": ["atomic:1", "atomic:2"],
                "commit_id": "commit:synthesis-relation",
            })
            relations.append({
                "relation_id": "relation:harbor-dependency",
                "relation_version_id": "relation-version:1",
                "kind": gold.expected_relation_kind,
                "lifecycle": "active",
                "participant_model_version_ids": ["atomic:1", "atomic:2"],
                "commit_id": "commit:synthesis-relation",
            })
        if source_batch.batch_number == 4:
            models.append({
                "model_id": "model:harbor-synthesis",
                "version_id": "version:harbor-synthesis:2",
                "source_signal_id": gold.correction_signal_id,
                "natural_text": gold.expected_corrected_thesis,
                "proposition": gold.expected_corrected_thesis,
                "abstraction_level": "composite",
                "claim_role": "situation",
                "lifecycle": "active",
                "scope_refs": [gold.expected_scope_ref],
                "evidence_signal_ids": [gold.correction_signal_id],
                "supporting_model_version_ids": ["version:harbor-synthesis:1"],
                "prior_version_id": "version:harbor-synthesis:1",
                "supersedes_version_id": "version:harbor-synthesis:1",
                "history_retained": True,
                "commit_id": "commit:correction",
            })
        relation_fates = []
        if source_batch.batch_number == 4:
            relation_fates.append({
                "relation_id": "relation:harbor-dependency",
                "relation_version_id": "relation-version:2",
                "prior_relation_version_id": "relation-version:1",
                "kind": gold.expected_relation_kind,
                "lifecycle": "retired",
                "prior_active_head_absent": True,
            })
        batches.append({
            "batch_number": source_batch.batch_number,
            "input_signal_ids": signal_ids,
            "processed_signal_ids": signal_ids,
            "unbatched_signal_count": 0,
            "groundings": groundings,
            "atomics": atomics,
            "retrieval": {
                "accepted_model_version_ids": (
                    ["atomic:harbor:1", "atomic:northstar:1"]
                    if source_batch.batch_number == 2 else []
                ),
                "observation_ids": [],
            },
            "accepted_models": models,
            "accepted_relations": relations,
            "relation_fates": relation_fates,
            "barrier": {
                "snapshot_validated": True,
                "expected_head_count": 4,
                "matched_head_count": 4,
                "stale_head_count": 0,
                "missing_head_count": 0,
            },
        })
    return {
        "population_digest": population.population_digest,
        "execution_id": "execution:one",
        "tenant_id": TENANT,
        "batches": batches,
        "contamination": {
            "gold_fields_seen": 0,
            "cross_tenant_row_count": 0,
            "oracle_imported": False,
        },
        "replay_digests": ["same-runtime-state", "same-runtime-state"],
    }


def test_perfect_receipts_pass_every_noncompensatory_gate() -> None:
    artifact = score_core_fast_path(
        perfect_receipt(), gold=build_core_fast_path_gold(),
    )

    assert artifact["overall_pass"] is True
    assert all(artifact["gates"].values())
    assert artifact["minimum_dimension_score"] == 1.0
    assert all(item["score"] == 1.0 for item in artifact["metrics"].values())
    assert len(artifact["artifact_digest"]) == 64


def test_continuous_degradation_cannot_be_compensated_by_other_dimensions() -> None:
    receipt = perfect_receipt()
    receipt["batches"][0]["groundings"].pop()
    receipt["batches"][3]["barrier"]["stale_head_count"] = 1
    receipt["contamination"]["gold_fields_seen"] = ["expected_thesis"]

    artifact = score_core_fast_path(
        receipt, gold=build_core_fast_path_gold(),
    )

    assert 0.0 < artifact["metrics"]["grounding"]["score"] < 1.0
    assert artifact["metrics"]["barriers"]["score"] == 0.75
    assert artifact["metrics"]["contamination"]["score"] == pytest.approx(2 / 3)
    assert artifact["gates"]["batch_3_synthesis"] is True
    assert artifact["gates"]["grounding"] is False
    assert artifact["gates"]["barriers"] is False
    assert artifact["gates"]["contamination"] is False
    assert artifact["overall_pass"] is False


def test_synthesis_and_relation_select_composite_not_same_source_atomic() -> None:
    receipt = perfect_receipt()
    gold = build_core_fast_path_gold()
    receipt["batches"][2]["accepted_models"].insert(0, {
        "model_id": "model:harbor-conclusion-atomic",
        "version_id": "version:harbor-conclusion-atomic:1",
        "source_signal_id": gold.synthesis_signal_id,
        "proposition": gold.expected_thesis,
        "abstraction_level": "atomic",
        "claim_role": "fact",
        "lifecycle": "active",
        "scope_refs": [gold.expected_scope_ref],
        "evidence_signal_ids": [gold.synthesis_signal_id],
        "supporting_model_version_ids": [],
        "commit_id": None,
    })

    artifact = score_core_fast_path(receipt, gold=gold)

    assert artifact["metrics"]["batch_3_synthesis"]["score"] == 1.0
    assert artifact["metrics"]["relation_atomicity"]["score"] == 1.0
    assert artifact["gates"]["batch_3_synthesis"] is True
    assert artifact["gates"]["relation_atomicity"] is True


def test_correction_fails_when_canonical_natural_text_is_stale() -> None:
    receipt = perfect_receipt()
    receipt["batches"][3]["accepted_models"][0]["natural_text"] = (
        build_core_fast_path_gold().expected_thesis
    )

    artifact = score_core_fast_path(receipt, gold=build_core_fast_path_gold())

    assert artifact["gates"]["batch_4_lifecycle_correction_history"] is False


def test_correction_fails_without_exact_relation_retirement_lineage() -> None:
    receipt = perfect_receipt()
    receipt["batches"][3]["relation_fates"] = []

    artifact = score_core_fast_path(receipt, gold=build_core_fast_path_gold())

    assert artifact["gates"]["batch_4_relation_retirement"] is False


def test_synthesis_rejects_atomic_with_multiple_derivation_references() -> None:
    receipt = perfect_receipt()
    synthesis = receipt["batches"][2]["accepted_models"][0]
    synthesis["abstraction_level"] = "atomic"
    synthesis["claim_role"] = "fact"

    artifact = score_core_fast_path(
        receipt, gold=build_core_fast_path_gold(),
    )

    assert artifact["gates"]["batch_3_synthesis"] is False
    assert artifact["gates"]["relation_atomicity"] is False


def test_artifact_is_deterministic_and_contract_lists_runner_fields() -> None:
    receipt = perfect_receipt()
    gold = build_core_fast_path_gold()
    first = score_core_fast_path(receipt, gold=gold)
    second = score_core_fast_path(deepcopy(receipt), gold=gold)

    assert first == second
    assert any(
        item.startswith("batches[].barrier:{snapshot_validated")
        for item in REQUIRED_RUNTIME_FIELDS
    )
    assert "replay_digests" in REQUIRED_RUNTIME_FIELDS
