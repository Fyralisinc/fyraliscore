from lib.evaluation.epistemic_repair.p2_population import build_p2_population


def test_population_has_stable_identity_and_unique_case_ids() -> None:
    first = build_p2_population()
    second = build_p2_population()

    assert first.digest == second.digest
    assert len(first.digest) == 64
    assert len({case.case_id for case in first.cases}) == len(first.cases)
    assert len({race.scenario_id for race in first.races}) == len(first.races)


def test_all_minimum_populations_are_sealed() -> None:
    population = build_p2_population()
    expected = {
        "nonaccepted_admission": 10,
        "accepted_atomic": 10,
        "accepted_synthesis": 5,
        "wrapper_control": 5,
        "entity_type_conflict": 5,
        "representation_divergence": 5,
        "falsification": 5,
        "valid_supersession": 5,
        "invalid_supersession": 5,
        "business_relation": 20,
        "derived_direct_write": 5,
        "retrieval_stability": 1,
        "evidence_idempotence": 1,
        "projection_idempotence": 1,
        "command_idempotence": 1,
    }

    assert {name: len(population.family(name)) for name in expected} == expected


def test_relation_population_spans_every_required_adversarial_shape() -> None:
    relations = build_p2_population().family("business_relation")
    shapes = {case.fact("shape") for case in relations}

    assert shapes >= {
        "valid_direction",
        "reverse_direction",
        "wrong_role",
        "wrong_endpoint",
        "self_negating_rationale",
        "unknown_type",
        "reciprocal_invalidity",
    }
    assert {case.fact("relation_kind") for case in relations if case.expected_disposition == "accept"} == {
        "causal_influence",
        "dependency_constraint",
        "enablement",
        "predictive_indicator",
    }


def test_transaction_and_race_scenarios_are_exactly_sealed() -> None:
    races = build_p2_population().races

    assert len(races) == 5
    assert {race.expected_outcome for race in races} == {
        "wholly_old_state",
        "wholly_fenced_new_state_one_event",
        "exactly_one_cas_winner_no_resurrection",
        "excluded_from_consequential_reads_one_repair_obligation",
        "no_automatic_endpoint_rebinding",
    }
    rollback = next(race for race in races if race.expected_outcome == "wholly_old_state")
    assert rollback.fault_point == "after_projection_fence_3"


def test_population_contract_does_not_import_production_validators() -> None:
    import lib.evaluation.epistemic_repair.p2_population as module

    source_names = set(module.__dict__)
    assert not any(name.endswith("Validator") or name.endswith("Command") for name in source_names)
