from __future__ import annotations

from services.workers.precipitation.quality_gate import (
    PrecipitationQualityObservation,
    assess_precipitation_quality,
    observation_from_review_payload,
)


def test_precipitation_quality_gate_keeps_smoke_evidence_from_broad_enablement() -> None:
    payload = {
        "pattern_candidate_id": "candidate-1",
        "review_mode": "semantic_required",
        "observed_tendency": {
            "review_features": {
                "feature_axes": ["lexical_recurrence"],
                "review_caution": "Weak statistical cluster.",
                "candidate_counterexample_count": 0,
                "cross_cluster_counterexample_count": 0,
            },
        },
    }
    observations = (
        observation_from_review_payload(
            case_id="tight-cluster-proposes-review",
            expected_candidate=True,
            payload=payload,
            runtime_ms=12.0,
        ),
        PrecipitationQualityObservation(
            case_id="diverse-noise-does-not-propose",
            expected_candidate=False,
            candidate_proposed=False,
            semantic_review_required=True,
            candidate_had_review_features=True,
            candidate_had_counterexample_search=True,
            runtime_ms=7.0,
        ),
        PrecipitationQualityObservation(
            case_id="under-min-cluster-does-not-propose",
            expected_candidate=False,
            candidate_proposed=False,
            semantic_review_required=True,
            candidate_had_review_features=True,
            candidate_had_counterexample_search=True,
            runtime_ms=6.5,
        ),
        PrecipitationQualityObservation(
            case_id="review-rejection-metabolizes-negative-memory",
            expected_candidate=True,
            candidate_proposed=True,
            semantic_review_required=True,
            candidate_had_review_features=True,
            candidate_had_counterexample_search=True,
            runtime_ms=10.0,
        ),
    )

    report = assess_precipitation_quality(observations)

    assert report.verdict == "shadow_ready"
    assert report.broad_enablement_allowed is False
    assert "insufficient_enablement_cases" in report.reasons
    assert report.precision == 1.0
    assert report.recall == 1.0
    assert report.review_gate_rate == 1.0


def test_precipitation_quality_gate_blocks_unsafe_candidate_flow() -> None:
    observations = (
        PrecipitationQualityObservation(
            case_id="false-positive-promoted",
            expected_candidate=False,
            candidate_proposed=True,
            semantic_review_required=False,
            candidate_had_review_features=False,
            candidate_had_counterexample_search=False,
            promoted_without_review=True,
        ),
    )

    report = assess_precipitation_quality(observations)

    assert report.verdict == "weak_evidence_only"
    assert report.broad_enablement_allowed is False
    assert "precision_below_gate" in report.reasons
    assert "semantic_review_not_required_for_all_candidates" in report.reasons
    assert "promotion_without_review_observed" in report.reasons


def test_precipitation_quality_gate_can_pass_representative_shadow_evidence() -> None:
    observations = []
    for index in range(12):
        observations.append(
            PrecipitationQualityObservation(
                case_id=f"true-positive-{index}",
                expected_candidate=True,
                candidate_proposed=True,
                semantic_review_required=True,
                candidate_had_review_features=True,
                candidate_had_counterexample_search=True,
                runtime_ms=25.0 + index,
            )
        )
    for index in range(12):
        observations.append(
            PrecipitationQualityObservation(
                case_id=f"true-negative-{index}",
                expected_candidate=False,
                candidate_proposed=False,
                semantic_review_required=True,
                candidate_had_review_features=True,
                candidate_had_counterexample_search=True,
                runtime_ms=18.0 + index,
            )
        )

    report = assess_precipitation_quality(observations, max_runtime_ms=50.0)

    assert report.verdict == "enablement_candidate"
    assert report.broad_enablement_allowed is True
    assert report.reasons == ("enablement_gate_passed",)
    assert report.case_count == 24
    assert report.false_positive == 0
    assert report.false_negative == 0
