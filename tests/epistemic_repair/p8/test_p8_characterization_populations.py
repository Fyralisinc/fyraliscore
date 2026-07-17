import asyncio
from dataclasses import replace

from lib.evaluation.epistemic_repair.p8_characterization_population import (
    build_all_characterization_populations,
    population_manifest,
)
from lib.evaluation.epistemic_repair.p8_characterization_runner import (
    _predict_boundary,
    _run_boundary,
)


def test_exact_characterization_denominators_and_required_slices() -> None:
    pops = {p.name: p for p in build_all_characterization_populations()}
    assert {name: len(pop.cases) for name, pop in pops.items()} == {
        "boundary_discovery": 1200, "context_selection": 600,
        "entity_grounding": 2400, "retrieval": 600, "feedback": 360,
    }
    boundary = population_manifest(pops["boundary_discovery"])["label_counts"]
    assert pops["boundary_discovery"].version == "2"
    assert boundary["structured"] == 300
    assert boundary["conversational"] == 600
    assert boundary["cross_source"] == 300
    assert boundary["reply_thread_edit"] >= 200
    entity = population_manifest(pops["entity_grounding"])["label_counts"]
    assert entity["explicit"] == 1200 and entity["near_name_collision"] == 200
    retrieval = population_manifest(pops["retrieval"])["label_counts"]
    assert retrieval["cold"] == retrieval["intermediate"] == retrieval["mature"] == 200
    feedback = population_manifest(pops["feedback"])["label_counts"]
    assert feedback["paired_route_policies"] == 360


def test_runtime_payloads_never_contain_evaluator_labels() -> None:
    for population in build_all_characterization_populations():
        assert population.runtime_digest != population.gold_digest
        for case in population.cases:
            payload = case.runtime_payload()
            assert "evaluator_labels" not in payload
            assert "gold" not in payload


def test_boundary_discovery_executes_full_frozen_population_and_registered_slices() -> None:
    population = next(
        item for item in build_all_characterization_populations()
        if item.name == "boundary_discovery"
    )
    result = asyncio.run(_run_boundary(population))

    assert result["metric"] == "boundary_discovery_b_cubed"
    assert result["denominator"] == 1200
    assert result["predictions_frozen_before_gold"] is True
    assert result["production_path"] == (
        "source topology plus generic explicit-topic episode projection"
    )
    assert 0.0 <= result["precision"] <= 1.0
    assert 0.0 <= result["recall"] <= 1.0
    assert 0.0 <= result["f1"] <= 1.0
    assert result["false_merge_clusters"] >= 0
    assert result["worst_example_ids"]
    expected_slices = {
        "structured", "conversational", "cross_source", "reply_thread_edit",
        "discourse_reference", "topic_drift", "split_merge",
        "temporal_distractor", "quote_link", "incomplete_topology",
        "cross_source_object_link",
    }
    assert expected_slices <= result["slices"].keys()
    for name in expected_slices:
        row = result["slices"][name]
        assert row["denominator"] > 0, name
        assert len(row["precision_ci95"]) == 2
        assert len(row["recall_ci95"]) == 2
        assert len(row["f1_ci95"]) == 2


def test_boundary_predictions_are_invariant_to_evaluator_gold_mutation() -> None:
    population = next(
        item for item in build_all_characterization_populations()
        if item.name == "boundary_discovery"
    )
    poisoned = replace(
        population,
        cases=tuple(
            replace(
                case,
                evaluator_labels=tuple(
                    "episode:999" if label.startswith("episode:") else label
                    for label in case.evaluator_labels
                ),
            )
            for case in population.cases
        ),
    )
    assert asyncio.run(_predict_boundary(population)) == asyncio.run(
        _predict_boundary(poisoned)
    )
