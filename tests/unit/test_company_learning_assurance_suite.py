from types import SimpleNamespace

import pytest

from scripts.run_company_learning_assurance_suite import (
    _variant_collision_failures,
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


def _clean_collision_evidence() -> SimpleNamespace:
    source_adaptive = _point(1.0)
    source_adaptive.sample_size = 2
    source_frozen = _point(1.0)
    source_frozen.sample_size = 2
    report = SimpleNamespace(
        pair_count=16,
        observed_pair_count=16,
        unsupported_case_count=0,
        unsupported_reason_counts={},
        adaptive_safe_containment_rate=_point(1.0),
        frozen_safe_containment_rate=_point(1.0),
        adaptive_candidate_visibility_rate=_point(1.0),
        frozen_candidate_visibility_rate=_point(1.0),
        adaptive_none_of_above_availability_rate=_point(1.0),
        frozen_none_of_above_availability_rate=_point(1.0),
        adaptive_source_immutability_rate=_point(1.0),
        frozen_source_immutability_rate=_point(1.0),
        adaptive_unsafe_rate=_point(0.0),
        frozen_unsafe_rate=_point(0.0),
        adaptive_unsafe_resolution_rate=_point(0.0),
        frozen_unsafe_resolution_rate=_point(0.0),
        adaptive_learned_promotion_rate=_point(0.0),
        frozen_learned_promotion_rate=_point(0.0),
        adaptive_wrong_model_rate=_point(0.0),
        frozen_wrong_model_rate=_point(0.0),
        safety_incident_count=0,
        adaptive_wrong_model_count=0,
        frozen_wrong_model_count=0,
        stratum_reports={
            "collision_family": {
                "conflicting_source_native_identifier": SimpleNamespace(
                    observed_case_count=2,
                    unsupported_case_count=0,
                    adaptive_authoritative_resolution_rate=source_adaptive,
                    frozen_authoritative_resolution_rate=source_frozen,
                )
            }
        },
    )
    return SimpleNamespace(report=report)


def test_clean_variant_population_has_no_blocking_failures() -> None:
    assert _variant_population_failures(_clean_variant_evidence()) == ()


def test_safe_supported_collision_scope_has_no_blocking_failures() -> None:
    assert _variant_collision_failures(_clean_collision_evidence()) == ()


def test_collision_safety_regression_blocks_without_hiding_scope() -> None:
    evidence = _clean_collision_evidence()
    evidence.report.adaptive_safe_containment_rate = _point(15 / 16)
    evidence.report.adaptive_unsafe_rate = _point(1 / 16)
    evidence.report.adaptive_unsafe_resolution_rate = _point(1 / 16)
    evidence.report.safety_incident_count = 1

    failures = _variant_collision_failures(evidence)

    assert any("adaptive safe containment" in row for row in failures)
    assert any("adaptive unsafe resolution" in row for row in failures)
    assert any("safety incidents" in row for row in failures)


def test_full_source_native_scope_requires_authoritative_resolution() -> None:
    evidence = _clean_collision_evidence()
    source_native = evidence.report.stratum_reports["collision_family"][
        "conflicting_source_native_identifier"
    ]
    source_native.adaptive_authoritative_resolution_rate = _point(0.0)
    source_native.frozen_authoritative_resolution_rate = _point(0.0)
    source_native.adaptive_authoritative_resolution_rate.sample_size = 2
    source_native.frozen_authoritative_resolution_rate.sample_size = 2

    failures = _variant_collision_failures(evidence)

    assert any(
        "adaptive source-native authoritative resolution" in row
        for row in failures
    )


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
