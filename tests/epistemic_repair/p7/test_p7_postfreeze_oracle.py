from __future__ import annotations

from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from lib.evaluation.epistemic_repair.p7_postfreeze_oracle import (
    _score_storyline,
    _world_maps,
    evaluate_frozen_worlds,
)
from lib.shared.errors import InvariantViolation


def test_claim_credit_uses_exact_evidence_ids_not_model_words() -> None:
    population = build_p6_population()
    tenant_id = uuid4()
    maps = _world_maps(population, tenant_id)
    atlas_signal_ids = {
        item.signal_id for item in population.gold
        if item.storyline_id == "atlas"
        and maps["batch_by_signal"][item.signal_id] <= 3
    }
    evidence_ids = [
        str(uuid5(NAMESPACE_URL, f"p6-think:{tenant_id}:{signal_id}"))
        for signal_id in atlas_signal_ids
    ]
    snapshot = {
        "accepted_models": [{
            "id": uuid4(),
            "natural_text": "deliberately contains no benchmark aliases or thesis words",
            "proposition": {"opaque": True},
            "scope_entities": [],
            "evidence_observation_ids": evidence_ids,
        }],
        "accepted_relations": [],
    }
    score = _score_storyline(
        storyline="atlas", stage=3, snapshot=snapshot, maps=maps,
    )
    assert score["atomic_claim_precision"]["value"] == 1
    assert score["atomic_claim_recall"]["value"] == 1
    assert score["direct_thesis_accuracy"]["value"] == 1
    assert not score["external_outcome_calibration_ece"]["measured"]


def test_noise_evidence_is_counted_as_contamination() -> None:
    population = build_p6_population()
    tenant_id = uuid4()
    maps = _world_maps(population, tenant_id)
    atlas = next(item for item in population.gold if item.storyline_id == "atlas")
    noise = next(item for item in population.gold if item.role == "noise")
    ids = [
        str(uuid5(NAMESPACE_URL, f"p6-think:{tenant_id}:{item.signal_id}"))
        for item in (atlas, noise)
    ]
    score = _score_storyline(
        storyline="atlas", stage=12,
        snapshot={"accepted_models": [{
            "id": uuid4(), "evidence_observation_ids": ids, "scope_entities": [],
        }], "accepted_relations": []},
        maps=maps,
    )
    assert score["false_truth_from_noise"] == 1
    assert score["atomic_claim_precision"]["value"] == 0


def test_incomplete_execution_never_receives_oracle_scores() -> None:
    with pytest.raises(InvariantViolation, match="cannot receive scores"):
        evaluate_frozen_worlds(
            execution_artifact={"complete": False, "world_results": []},
            sealed_worlds={},
        )
