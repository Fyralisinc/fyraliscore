"""Counterexample search for SAGE latent pattern candidates."""
from __future__ import annotations

from typing import Iterable

from services.reasoning.sage.patterns.types import (
    PatternCounterexample,
    PatternScoutCandidate,
    StructuralSignature,
)


def find_pattern_counterexamples(
    candidate: PatternScoutCandidate,
    signatures: Iterable[StructuralSignature],
    *,
    max_counterexamples: int = 12,
) -> tuple[PatternCounterexample, ...]:
    """Find bounded structural counterexamples for a scout candidate."""

    required = set(candidate.shared_facets)
    out: list[PatternCounterexample] = []
    for signature in signatures:
        if not required.issubset(set(signature.shape_facets)):
            continue
        reason = _counterexample_reason(signature)
        if reason is None:
            continue
        out.append(
            PatternCounterexample(
                source_ref=signature.source_ref or signature.signature_hash,
                signature_hash=signature.signature_hash,
                expected_outcome=signature.expected_outcome,
                observed_outcome=signature.observed_outcome,
                reason=reason,
                metadata={
                    "canonical_write": False,
                    "source": "sage_counterexample_search",
                },
            )
        )
        if len(out) >= max(0, int(max_counterexamples)):
            break
    return tuple(out)


def attach_counterexamples(
    candidate: PatternScoutCandidate,
    counterexamples: tuple[PatternCounterexample, ...],
) -> PatternScoutCandidate:
    """Return a candidate copy with updated counterexample count."""

    return PatternScoutCandidate(
        candidate_hash=candidate.candidate_hash,
        scout_kind=candidate.scout_kind,
        shared_facets=candidate.shared_facets,
        support_signature_hashes=candidate.support_signature_hashes,
        support_source_refs=candidate.support_source_refs,
        support_count=candidate.support_count,
        surface_domain_count=candidate.surface_domain_count,
        surface_distance_score=candidate.surface_distance_score,
        outcome_cohesion_score=candidate.outcome_cohesion_score,
        counterexample_count=len(counterexamples),
        utility_score=candidate.utility_score,
        confidence=max(
            0.0,
            round(candidate.confidence - min(0.45, 0.08 * len(counterexamples)), 4),
        ),
        explanation=candidate.explanation,
        metadata={
            **candidate.metadata,
            "counterexample_refs": [
                counterexample.source_ref for counterexample in counterexamples
            ],
        },
    )


def _counterexample_reason(signature: StructuralSignature) -> str | None:
    if bool(signature.metadata.get("counterexample")):
        return "signature is explicitly marked as a counterexample"
    if signature.expected_outcome and signature.observed_outcome:
        if signature.expected_outcome != signature.observed_outcome:
            return "same structural shape produced a different observed outcome"
    return None


__all__ = [
    "attach_counterexamples",
    "find_pattern_counterexamples",
]
