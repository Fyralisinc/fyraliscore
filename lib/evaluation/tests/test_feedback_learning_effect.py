from __future__ import annotations

from lib.evaluation.company_learning_active_surfaces import (
    ActiveLearningSurfacesEvidence,
    SourceSalienceObservation,
    evaluate_active_learning_surfaces,
)
from lib.evaluation.feedback_learning_effect import (
    compose_feedback_learning_effect,
    validate_feedback_learning_effect_artifact,
)

from .test_company_learning_active_surfaces import _safe_identity


def _source(*, useful_learned: float = 1.05) -> ActiveLearningSurfacesEvidence:
    values = {
        "settled_useful": (1.0, useful_learned, True, False),
        "corrected": (1.0, 0.9, False, False),
        "pending": (1.0, 1.0, False, False),
        "foreign_tenant": (1.0, 1.0, False, False),
        "profile_load": (1.0, 1.0, False, False),
    }
    salience = tuple(
        SourceSalienceObservation(
            case_id=case_id,
            baseline_salience=baseline,
            learned_salience=learned,
            credit_observed=credit,
            foreign_tenant_learned=foreign,
            canonical_truth_immutable=True,
            grounding_truth_immutable=True,
            artifact_refs=(f"case:{case_id}",),
        )
        for case_id, (baseline, learned, credit, foreign) in values.items()
    )
    identity = _safe_identity()
    return ActiveLearningSurfacesEvidence(
        run_id="sealed-source-salience",
        system_version="test",
        created_at="2026-07-17T00:00:00+00:00",
        identity_observations=identity,
        salience_observations=salience,
        report=evaluate_active_learning_surfaces(
            identity_observations=identity,
            salience_observations=salience,
        ),
        artifact_refs=("artifact:test",),
    )


def test_composes_explicit_matched_effect_without_terminal_overclaim() -> None:
    source = _source()
    result = compose_feedback_learning_effect(
        source_payload=source.artifact_payload(),
        source_artifact_sha256="a" * 64,
    )
    assert result.report.status == "observed"
    assert result.report.matched_pair_count == 5
    assert result.report.useful_pair_count == 1
    assert result.report.safety_pair_count == 4
    assert result.report.useful_adaptive_minus_frozen > 0.0
    assert result.report.direction_correct_rate == 1.0
    assert result.report.truth_immutability_rate == 1.0
    assert "selected-evidence quality improved" in result.report.excluded_claims
    assert validate_feedback_learning_effect_artifact(
        result.artifact_payload()
    ) == result


def test_contradicts_wrong_direction_useful_effect() -> None:
    # Build a valid source artifact whose own directional report is contradicted;
    # the objective composer must retain that result, not normalize it away.
    source = _source(useful_learned=0.95)
    result = compose_feedback_learning_effect(
        source_payload=source.artifact_payload(),
        source_artifact_sha256="b" * 64,
    )
    assert result.report.status == "contradicted"
    assert result.report.direction_correct_rate == 0.8
    assert result.report.continuous_score == 0.9
