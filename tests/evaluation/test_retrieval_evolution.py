from lib.evaluation.retrieval_evolution import evaluate_retrieval_evolution


def _batch(
    models,
    observations,
    *,
    referenced_models=(),
    referenced_observations=(),
    reasons=(),
):
    return {
        "retrieval_model_count": models,
        "retrieval_observation_count": observations,
        "ops_applied": {"context_use": {
            "referenced_model_ids": list(referenced_models),
            "referenced_observation_ids": list(referenced_observations),
            "raw_observation_reopening_reasons": list(reasons),
            "selected_historical_observation_count": observations,
        }},
    }


def test_preregistered_evolution_accepts_observation_to_model_transition():
    early = [_batch(2, 8, referenced_observations=("o",)) for _ in range(3)]
    middle = [_batch(5, 5, referenced_models=("m",)) for _ in range(3)]
    late = [
        _batch(
            8,
            2,
            referenced_models=("m1", "m2", "m3", "m4"),
            referenced_observations=("o",),
            reasons=("contradiction",),
        )
        for _ in range(3)
    ]

    report = evaluate_retrieval_evolution(early + middle + late)

    assert report["verdict"] == "meets_preregistered_policy"
    assert report["measurements"]["early_observation_selection_share"] == 0.8
    assert report["measurements"]["late_model_selection_share"] == 0.8
    assert report["measurements"]["late_model_reference_share"] == 0.8
    assert report["measurements"]["model_selection_share_gain"] == 0.6


def test_selected_models_without_actual_reference_do_not_satisfy_use_gate():
    batches = (
        [_batch(1, 9, referenced_observations=("o",)) for _ in range(3)]
        + [_batch(5, 5, referenced_models=("m",)) for _ in range(3)]
        + [_batch(9, 1) for _ in range(3)]
    )

    report = evaluate_retrieval_evolution(batches)

    assert report["checks"]["late_selected_context_is_actually_referenced"] is False
    assert report["verdict"] == "below_policy"


def test_unjustified_late_raw_reopening_fails_even_when_models_dominate():
    batches = (
        [_batch(1, 9, referenced_observations=("o",)) for _ in range(3)]
        + [_batch(5, 5, referenced_models=("m",)) for _ in range(3)]
        + [
            _batch(
                9,
                1,
                referenced_models=("m",),
                referenced_observations=("o",),
            )
            for _ in range(3)
        ]
    )

    report = evaluate_retrieval_evolution(batches)

    assert report["checks"]["late_raw_reopening_is_justified"] is False
    assert report["verdict"] == "below_policy"


def test_missing_population_is_unknown_and_cannot_pass():
    report = evaluate_retrieval_evolution([])

    assert report["checks"]["early_is_observation_heavy"] is None
    assert report["continuous_score"] == 0.0
    assert report["verdict"] == "below_policy"
