from types import SimpleNamespace

import pytest

from scripts.run_company_learning_assurance_suite import (
    _variant_population_failures,
)


def _point(value: float) -> SimpleNamespace:
    return SimpleNamespace(point_estimate=value)


def _clean_variant_evidence() -> SimpleNamespace:
    report = SimpleNamespace(
        pair_count=24,
        unsupported_case_count=0,
        adaptive_correctness=_point(1.0),
        frozen_correctness=_point(0.0),
        adaptive_minus_frozen_correctness=_point(1.0),
        adaptive_unsafe_rate=_point(0.0),
        frozen_unsafe_rate=_point(0.0),
    )
    metrics = SimpleNamespace(
        candidate_memory_mediated_success_rate=1.0,
        adaptive_target_candidate_authorization_rate=1.0,
        adaptive_closed_set_match_rate=1.0,
        frozen_target_candidate_exposure_rate=0.0,
        frozen_closed_set_match_rate=0.0,
        both_arms_one_llm_call_rate=1.0,
        both_arms_scripted_target_response_rate=1.0,
        frozen_safe_review_or_abstention_rate=1.0,
        source_immutability_rate=1.0,
        hard_safety_incident_count=0,
        control_integrity_violation_count=0,
    )
    return SimpleNamespace(
        population_report=report,
        mechanism_metrics=metrics,
    )


def test_clean_variant_population_has_no_blocking_failures() -> None:
    assert _variant_population_failures(_clean_variant_evidence()) == ()


@pytest.mark.parametrize(
    ("owner", "field", "value", "expected"),
    (
        (
            "report",
            "adaptive_unsafe_rate",
            _point(0.25),
            "adaptive unsafe rate",
        ),
        (
            "report",
            "frozen_unsafe_rate",
            _point(0.25),
            "frozen unsafe rate",
        ),
        (
            "metrics",
            "adaptive_closed_set_match_rate",
            0.75,
            "adaptive closed-set match",
        ),
        (
            "metrics",
            "both_arms_scripted_target_response_rate",
            0.75,
            "both-arm scripted target response",
        ),
        (
            "metrics",
            "frozen_safe_review_or_abstention_rate",
            0.75,
            "frozen safe review or abstention",
        ),
    ),
)
def test_variant_population_causal_and_safety_regressions_block(
    owner: str,
    field: str,
    value: object,
    expected: str,
) -> None:
    evidence = _clean_variant_evidence()
    target = (
        evidence.population_report
        if owner == "report"
        else evidence.mechanism_metrics
    )
    setattr(target, field, value)

    assert any(
        expected in failure
        for failure in _variant_population_failures(evidence)
    )
