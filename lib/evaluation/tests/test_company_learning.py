from __future__ import annotations

from lib.evaluation.company_learning import assess_company_learning_runtime_state


def test_partial_component_exposure_cannot_substantiate_company_learning() -> None:
    status, health = assess_company_learning_runtime_state(
        incident_count=0,
        context_selection_count=2,
        governed_replay_exposure_count=0,
        source_semantic_exposure_count=0,
        critical_rates=(1.0, 1.0, None, None),
    )

    assert status == "insufficient"
    assert health == "incomplete"


def test_clean_full_runtime_exposure_is_healthy_but_not_yet_substantiated() -> None:
    status, health = assess_company_learning_runtime_state(
        incident_count=0,
        context_selection_count=2,
        governed_replay_exposure_count=1,
        source_semantic_exposure_count=3,
        critical_rates=(1.0,) * 9,
    )

    assert status == "insufficient"
    assert health == "healthy"


def test_confirmed_incident_contradicts_runtime_state() -> None:
    status, health = assess_company_learning_runtime_state(
        incident_count=1,
        context_selection_count=2,
        governed_replay_exposure_count=1,
        source_semantic_exposure_count=3,
        critical_rates=(1.0,) * 9,
    )

    assert status == "contradicted"
    assert health == "contradicted"
