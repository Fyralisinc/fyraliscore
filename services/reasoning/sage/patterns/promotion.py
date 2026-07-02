"""Promotion assessment for latent SAGE pattern candidates.

The functions here decide whether a latent candidate is worth Think review.
They do not create Models and do not enqueue work on their own.
"""
from __future__ import annotations

from services.reasoning.sage.patterns.types import (
    PatternCounterexample,
    PatternScoutCandidate,
    PromotionAssessment,
)


def assess_promotion_readiness(
    candidate: PatternScoutCandidate,
    *,
    counterexamples: tuple[PatternCounterexample, ...] = (),
    min_support: int = 3,
    min_surface_domains: int = 2,
    min_readiness: float = 0.62,
) -> PromotionAssessment:
    """Score whether a latent candidate deserves Think pattern review."""

    readiness = candidate.promotion_readiness_score
    if counterexamples:
        readiness = max(0.0, readiness - min(0.35, 0.07 * len(counterexamples)))
    rubric = {
        "stable": _stable_score(candidate, min_support=min_support),
        "useful": _useful_score(candidate),
        "explainable": _explainable_score(candidate),
        "falsifiable": _falsifiable_score(candidate, counterexamples),
        "action_shaping": _action_shaping_score(candidate),
        "surface_diverse": _surface_diverse_score(
            candidate,
            min_surface_domains=min_surface_domains,
        ),
    }
    reasons = _assessment_reasons(
        candidate,
        rubric=rubric,
        counterexamples=counterexamples,
        min_support=min_support,
        min_surface_domains=min_surface_domains,
    )
    average = sum(rubric.values()) / max(1, len(rubric))
    effective_score = round((readiness * 0.58) + (average * 0.42), 4)
    if (
        effective_score >= min_readiness
        and candidate.support_count >= min_support
        and candidate.surface_domain_count >= min_surface_domains
        and rubric["explainable"] >= 0.55
        and rubric["falsifiable"] >= 0.40
        and len(counterexamples) <= max(1, candidate.support_count // 2)
    ):
        status = "promotion_candidate"
    elif effective_score >= 0.48 and candidate.support_count >= min_support:
        status = "shadow_ready"
    else:
        status = "not_ready"
    return PromotionAssessment(
        status=status,
        candidate_hash=candidate.candidate_hash,
        readiness_score=effective_score,
        rubric={key: round(value, 4) for key, value in rubric.items()},
        reasons=tuple(reasons),
        counterexamples=counterexamples,
    )


def think_review_notes(candidate: PatternScoutCandidate, assessment: PromotionAssessment) -> dict:
    """Build compact notes for a future T4 Think review payload."""

    return {
        "source": "sage_latent_pattern_promotion",
        "canonical_write": False,
        "candidate": candidate.to_dict(),
        "assessment": assessment.to_notes(),
        "promotion_rule": (
            "SAGE may propose a latent regularity; Think must review evidence "
            "and use normal Pattern Model grammar before canonical promotion."
        ),
    }


def _stable_score(
    candidate: PatternScoutCandidate,
    *,
    min_support: int,
) -> float:
    return min(1.0, candidate.support_count / max(1, min_support + 2))


def _surface_diverse_score(
    candidate: PatternScoutCandidate,
    *,
    min_surface_domains: int,
) -> float:
    return min(1.0, candidate.surface_domain_count / max(1, min_surface_domains + 1))


def _useful_score(candidate: PatternScoutCandidate) -> float:
    return max(
        0.0,
        min(1.0, 0.55 * candidate.utility_score + 0.45 * candidate.confidence),
    )


def _explainable_score(candidate: PatternScoutCandidate) -> float:
    if not candidate.shared_facets:
        return 0.0
    facet_score = min(1.0, len(candidate.shared_facets) / 4.0)
    explanation_score = 1.0 if candidate.explanation.strip() else 0.35
    return max(0.0, min(1.0, 0.72 * facet_score + 0.28 * explanation_score))


def _falsifiable_score(
    candidate: PatternScoutCandidate,
    counterexamples: tuple[PatternCounterexample, ...],
) -> float:
    has_outcome = any(facet.startswith("outcome:") for facet in candidate.shared_facets)
    has_gap = any(facet.startswith("evidence_gap:") for facet in candidate.shared_facets)
    base = 0.45 if has_outcome or has_gap else 0.25
    if counterexamples:
        base += 0.35
    return min(1.0, base + min(0.20, candidate.support_count * 0.03))


def _action_shaping_score(candidate: PatternScoutCandidate) -> float:
    action_prefixes = (
        "authority:",
        "coordination:",
        "pressure:",
        "role:",
    )
    matches = sum(
        1
        for facet in candidate.shared_facets
        if facet.startswith(action_prefixes)
    )
    return min(1.0, 0.25 + 0.22 * matches)


def _assessment_reasons(
    candidate: PatternScoutCandidate,
    *,
    rubric: dict[str, float],
    counterexamples: tuple[PatternCounterexample, ...],
    min_support: int,
    min_surface_domains: int,
) -> list[str]:
    reasons: list[str] = []
    if candidate.support_count < min_support:
        reasons.append("insufficient_support")
    if candidate.surface_domain_count < min_surface_domains:
        reasons.append("insufficient_surface_diversity")
    if rubric["explainable"] < 0.55:
        reasons.append("weak_explanation")
    if rubric["falsifiable"] < 0.40:
        reasons.append("needs_clearer_falsifier")
    if counterexamples:
        reasons.append("counterexamples_require_review")
    if not reasons:
        reasons.append("ready_for_think_review")
    return reasons


__all__ = [
    "assess_promotion_readiness",
    "think_review_notes",
]
