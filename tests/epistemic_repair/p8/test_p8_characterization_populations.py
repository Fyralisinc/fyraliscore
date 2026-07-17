from lib.evaluation.epistemic_repair.p8_characterization_population import (
    build_all_characterization_populations,
    population_manifest,
)


def test_exact_characterization_denominators_and_required_slices() -> None:
    pops = {p.name: p for p in build_all_characterization_populations()}
    assert {name: len(pop.cases) for name, pop in pops.items()} == {
        "boundary_discovery": 1200, "context_selection": 600,
        "entity_grounding": 2400, "retrieval": 600, "feedback": 360,
    }
    boundary = population_manifest(pops["boundary_discovery"])["label_counts"]
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
