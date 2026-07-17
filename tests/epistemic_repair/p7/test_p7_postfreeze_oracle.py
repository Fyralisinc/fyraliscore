from __future__ import annotations

from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from lib.evaluation.epistemic_repair.p7_postfreeze_oracle import (
    _strategic_decision,
    _score_storyline,
    _world_maps,
    evaluate_frozen_worlds,
)
from lib.evaluation.epistemic_repair.p7_population import (
    build_p7_semantic_oracles,
)
from lib.evaluation.epistemic_repair.p7_runner import P7_ARMS
from lib.shared.errors import InvariantViolation


def test_evidence_membership_without_semantic_entailment_gets_no_credit() -> None:
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
        semantic_oracle=build_p7_semantic_oracles(population),
    )
    assert score["atomic_claim_precision"]["value"] == 0
    assert score["atomic_claim_recall"]["value"] == 0
    assert score["semantic_contradiction_or_nonentailment"] == 1


def test_wrong_proposition_with_right_evidence_is_rejected() -> None:
    population = build_p6_population()
    tenant_id = uuid4()
    maps = _world_maps(population, tenant_id)
    evidence_ids = [
        observation_id for observation_id, item in maps["gold_by_observation"].items()
        if item.storyline_id == "atlas"
    ]
    wrong = {
        "id": uuid4(),
        "natural_text": (
            "Atlas release slips do not recur when certificate ownership changes "
            "during handoff."
        ),
        "proposition": {"polarity": "negative"},
        "scope_entities": [],
        "evidence_observation_ids": evidence_ids,
    }
    score = _score_storyline(
        storyline="atlas", stage=12,
        snapshot={"accepted_models": [wrong], "accepted_relations": []},
        maps=maps,
        semantic_oracle=build_p7_semantic_oracles(population),
    )
    assert score["atomic_claim_precision"]["value"] == 0
    assert score["semantic_contradiction_or_nonentailment"] == 1


def test_right_proposition_and_right_evidence_receive_joint_credit() -> None:
    population = build_p6_population()
    tenant_id = uuid4()
    maps = _world_maps(population, tenant_id)
    evidence_ids = [
        observation_id for observation_id, item in maps["gold_by_observation"].items()
        if item.storyline_id == "atlas"
    ]
    score = _score_storyline(
        storyline="atlas", stage=12,
        snapshot={"accepted_models": [{
            "id": uuid4(),
            "natural_text": dict(population.thesis_by_storyline)["atlas"],
            "proposition": {"polarity": "positive"},
            "scope_entities": [],
            "evidence_observation_ids": evidence_ids,
        }], "accepted_relations": []},
        maps=maps,
        semantic_oracle=build_p7_semantic_oracles(population),
    )
    assert score["atomic_claim_precision"]["value"] == 1
    assert score["atomic_claim_recall"]["value"] == 1
    assert score["direct_thesis_accuracy"]["value"] == 1


def test_relation_credit_requires_typed_kind_and_semantic_participants() -> None:
    population = build_p6_population()
    tenant_id = uuid4()
    maps = _world_maps(population, tenant_id)
    evidence_ids = [
        observation_id for observation_id, item in maps["gold_by_observation"].items()
        if item.storyline_id == "atlas"
    ]
    ids = (uuid4(), uuid4())
    models = [{
        "id": model_id,
        "natural_text": dict(population.thesis_by_storyline)["atlas"],
        "proposition": {"polarity": "positive"},
        "scope_entities": [], "evidence_observation_ids": evidence_ids,
    } for model_id in ids]
    relation = {
        "truth_relation_kind": "causal_influence",
        "participants": [
            {"model_id": ids[0], "participant_role": "cause"},
            {"model_id": ids[1], "participant_role": "effect"},
        ],
    }
    score = _score_storyline(
        storyline="atlas", stage=12,
        snapshot={"accepted_models": models, "accepted_relations": [relation]},
        maps=maps,
        semantic_oracle=build_p7_semantic_oracles(population),
    )
    assert score["relation_joint_precision"]["value"] == 1
    assert score["relation_joint_recall"]["value"] == 1
    relation["truth_relation_kind"] = "supports"
    wrong = _score_storyline(
        storyline="atlas", stage=12,
        snapshot={"accepted_models": models, "accepted_relations": [relation]},
        maps=maps,
        semantic_oracle=build_p7_semantic_oracles(population),
    )
    assert wrong["relation_joint_precision"]["value"] == 0


def test_valid_beacon_negation_does_not_invert_causal_predicate() -> None:
    population = build_p6_population()
    oracle = next(
        item for item in build_p7_semantic_oracles(population).claims
        if item.storyline_id == "beacon"
    )
    from lib.evaluation.epistemic_repair.p7_postfreeze_oracle import (
        entails_structured_claim,
    )

    assert entails_structured_claim({
        "natural_text": (
            "Beacon completion depends on access review, not deploy readiness; "
            "deployment remains blocked."
        ),
        "proposition": {"polarity": "positive"},
    }, oracle)


def test_mechanics_pass_but_adaptive_quality_loses_cannot_earn_primary() -> None:
    endpoints = []
    for world in ("w1", "w2", "w3"):
        for arm in P7_ARMS:
            for storyline in ("atlas", "beacon", "cobalt", "delta"):
                value = 0.2 if arm == "adaptive" else 0.8
                endpoints.append({
                    "world_id": world, "arm_id": arm, "storyline_id": storyline,
                    "stage_batch": 12,
                    "direct_thesis_accuracy": {"value": value},
                    "atomic_claim_f1": {"value": value},
                    "boundary_entity_safety": {"value": 1.0},
                    "relation_joint_precision": {"value": 1.0},
                    "external_outcome_calibration_ece": {"value": 0.1},
                })
    intervals = [{
        "comparator_arm": arm, "lower_95": -0.7, "upper_95": -0.5,
    } for arm in P7_ARMS if arm != "adaptive"]
    economics = [{
        "arm_id": arm, "stage_batch": 12, "input_tokens": 100,
        "output_tokens": 10, "wall_time_s": 1.0,
    } for arm in P7_ARMS]
    verdict, decision = _strategic_decision(
        endpoints=endpoints, intervals=intervals,
        correction_by_arm={arm: {"latency": 1.0, "stale": 1.0} for arm in P7_ARMS},
        economics=economics,
        historical_raw={arm: {"selected": 10, "unjustified": 0} for arm in P7_ARMS},
        hard_gates={"mechanics": True}, global_noise_truth=0,
    )
    assert verdict == "not_earned"
    assert not decision["criteria"]["20.7.1_thesis_lift"]


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
        semantic_oracle=build_p7_semantic_oracles(population),
    )
    assert score["false_truth_from_noise"] == 1
    assert score["atomic_claim_precision"]["value"] == 0


def test_incomplete_execution_never_receives_oracle_scores() -> None:
    with pytest.raises(InvariantViolation, match="cannot receive scores"):
        evaluate_frozen_worlds(
            execution_artifact={"complete": False, "world_results": []},
            sealed_worlds={},
        )
