from __future__ import annotations

from uuid import uuid4

from services.reasoning.sage.patterns import (
    assess_promotion_readiness,
    attach_counterexamples,
    build_structural_signature,
    find_pattern_counterexamples,
    scout_global_patterns,
    think_review_notes,
)


def test_structural_signature_ignores_surface_domain_for_shape_hash() -> None:
    sales = _source("sales", "Approval review blocked commitment")
    legal = _source("legal", "Approval review blocked commitment")

    sales_sig = build_structural_signature(sales, source_kind="model")
    legal_sig = build_structural_signature(legal, source_kind="model")

    assert sales_sig.signature_hash == legal_sig.signature_hash
    assert sales_sig.domain_facets == ("sales",)
    assert legal_sig.domain_facets == ("legal",)
    assert sales_sig.surface_key != legal_sig.surface_key
    assert "coordination:approval_loop" in sales_sig.shape_facets
    assert "authority:approval_authority" in sales_sig.shape_facets


def test_global_scout_finds_surface_different_structural_pattern() -> None:
    signatures = [
        build_structural_signature(
            _source("sales", "Sales approval review blocked renewal commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source("security", "Security approval review blocked audit commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source("legal", "Legal approval review blocked contract commitment"),
            source_kind="model",
        ),
    ]

    report = scout_global_patterns(
        signatures,
        min_support=3,
        min_surface_domains=3,
    )

    assert report.signatures_seen == 3
    assert report.buckets_considered > 0
    assert report.all_pairs_avoided_estimate >= 0
    assert report.candidates
    candidate = report.candidates[0]
    assert candidate.support_count == 3
    assert candidate.surface_domain_count == 3
    assert candidate.metadata["canonical_write"] is False
    assert candidate.promotion_readiness_score > 0.7


def test_global_scout_bounds_large_buckets_instead_of_all_pairs() -> None:
    signatures = [
        build_structural_signature(
            _source(f"domain_{idx}", "Approval review blocked commitment"),
            source_kind="model",
        )
        for idx in range(80)
    ]

    report = scout_global_patterns(
        signatures,
        min_support=3,
        min_surface_domains=2,
        max_bucket_size=10,
        max_candidates=5,
    )

    assert report.buckets_pruned > 0
    assert report.candidates
    assert all(candidate.support_count <= 10 for candidate in report.candidates)
    assert report.all_pairs_avoided_estimate > 0


def test_counterexamples_lower_candidate_confidence_and_promote_only_to_think_notes() -> None:
    signatures = [
        build_structural_signature(
            _source("sales", "Approval review blocked renewal commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source("security", "Approval review blocked audit commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source("legal", "Approval review blocked contract commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source(
                "finance",
                "Approval review blocked but resolved without delay commitment",
                observed_outcome="resolved_without_delay",
            ),
            source_kind="model",
        ),
    ]
    report = scout_global_patterns(signatures[:3], min_support=3, min_surface_domains=3)
    candidate = next(
        item
        for item in report.candidates
        if any(facet.startswith("outcome:") for facet in item.shared_facets)
    )

    counterexamples = find_pattern_counterexamples(candidate, signatures)
    adjusted = attach_counterexamples(candidate, counterexamples)
    assessment = assess_promotion_readiness(adjusted, counterexamples=counterexamples)
    notes = think_review_notes(adjusted, assessment)

    assert counterexamples
    assert adjusted.confidence < candidate.confidence
    assert notes["canonical_write"] is False
    assert notes["assessment"]["required_bridge"] == (
        "Think review before Pattern Model promotion"
    )


def test_promotion_assessment_marks_strong_candidate_as_think_review_candidate() -> None:
    signatures = [
        build_structural_signature(
            _source("sales", "Approval review blocked renewal commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source("security", "Approval review blocked audit commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source("legal", "Approval review blocked contract commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source("support", "Approval review blocked enterprise commitment"),
            source_kind="model",
        ),
    ]
    report = scout_global_patterns(signatures, min_support=3, min_surface_domains=3)
    candidate = next(
        item
        for item in report.candidates
        if any(facet.startswith("outcome:") for facet in item.shared_facets)
    )

    assessment = assess_promotion_readiness(candidate)

    assert assessment.ready_for_think_review is True
    assert assessment.status == "promotion_candidate"
    assert "ready_for_think_review" in assessment.reasons


def _source(
    domain: str,
    text: str,
    *,
    observed_outcome: str = "blocked_commitment_delay",
) -> dict:
    return {
        "id": uuid4(),
        "domain_tags": [domain],
        "claim_role": "pattern",
        "abstraction_level": "pattern",
        "time_mode": "recurring",
        "polarity": "negative",
        "proposition": {
            "statement": text,
            "observed_tendency": text,
            "pressure_type": "revenue_risk",
            "trigger_conditions": "approval review blocks commitment",
            "expected_outcome": "blocked_commitment_delay",
            "observed_outcome": observed_outcome,
        },
        "metadata": {
            "expected_outcome": "blocked_commitment_delay",
            "observed_outcome": observed_outcome,
        },
    }
