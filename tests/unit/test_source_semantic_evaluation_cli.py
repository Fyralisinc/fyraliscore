from __future__ import annotations

from dataclasses import dataclass, replace

from scripts.evaluate_source_semantic_state import (
    _observed_core_coverage_is_complete,
)


@dataclass(frozen=True)
class _CoverageState:
    eligible_grounding_interpretation_coverage: float | None = None
    source_coordinate_reconstructability_rate: float | None = None
    interpretation_structural_closure_rate: float | None = None
    grounding_continuity_exactness_rate: float | None = None
    explicit_admission_fate_coverage: float | None = None
    epistemic_consumer_admission_continuity_rate: float | None = None
    applied_decision_model_coverage: float | None = None
    one_model_cardinality_rate: float | None = None
    model_source_provenance_rate: float | None = None
    model_scope_referent_rate: float | None = None
    model_grounding_dependency_rate: float | None = None
    model_dependency_closure_rate: float | None = None
    non_admitted_no_model_safety_rate: float | None = None
    supported_report_admission_precision: float | None = None
    supported_report_admission_recall: float | None = None


def test_complete_fates_gate_requires_every_observed_core_rate() -> None:
    complete = _CoverageState(
        eligible_grounding_interpretation_coverage=1.0,
        explicit_admission_fate_coverage=1.0,
        applied_decision_model_coverage=1.0,
        supported_report_admission_precision=1.0,
        supported_report_admission_recall=1.0,
    )

    assert _observed_core_coverage_is_complete(complete)
    assert not _observed_core_coverage_is_complete(
        replace(complete, explicit_admission_fate_coverage=0.75)
    )
    assert not _observed_core_coverage_is_complete(
        replace(complete, supported_report_admission_precision=0.9)
    )


def test_complete_fates_gate_preserves_zero_exposure_as_unknown() -> None:
    no_exposure = _CoverageState()

    assert _observed_core_coverage_is_complete(no_exposure)
    assert no_exposure.eligible_grounding_interpretation_coverage is None
    assert no_exposure.explicit_admission_fate_coverage is None
