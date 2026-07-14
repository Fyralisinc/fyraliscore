"""Offline scout inputs for SAGE company learning profiles."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from services.reasoning.sage.patterns import (
    GlobalScoutReport,
    PatternScoutCandidate,
    StructuralSignature,
    scout_global_patterns,
)


@dataclass(frozen=True, slots=True)
class LatentPatternProfileInput:
    """Bounded latent-pattern input prepared outside the inquiry hot path."""

    structural_signatures: tuple[StructuralSignature, ...]
    latent_pattern_candidates: tuple[PatternScoutCandidate, ...]
    scout_notes: dict[str, Any]

    @property
    def canonical_write(self) -> bool:
        return False

    def to_profile_kwargs(self) -> dict[str, Any]:
        return {
            "structural_signatures": self.structural_signatures,
            "latent_pattern_candidates": self.latent_pattern_candidates,
        }


def build_latent_pattern_profile_input(
    signatures: Iterable[StructuralSignature],
    *,
    min_support: int = 3,
    min_surface_domains: int = 2,
    max_bucket_size: int = 64,
    max_candidates: int = 12,
    max_structural_signatures: int = 128,
) -> LatentPatternProfileInput:
    """Run the global scout as an explicit offline/background profile step."""

    signature_list = tuple(
        sorted(
            signatures,
            key=lambda signature: (
                -signature.support_weight,
                signature.source_ref or "",
                signature.signature_hash,
            ),
        )[: max(1, int(max_structural_signatures))]
    )
    report = scout_global_patterns(
        signature_list,
        min_support=min_support,
        min_surface_domains=min_surface_domains,
        max_bucket_size=max_bucket_size,
        max_candidates=max_candidates,
    )
    return LatentPatternProfileInput(
        structural_signatures=_supporting_signatures(
            signature_list,
            report=report,
            max_signatures=max_structural_signatures,
        ),
        latent_pattern_candidates=report.candidates,
        scout_notes=_compact_scout_notes(report),
    )


def _supporting_signatures(
    signatures: tuple[StructuralSignature, ...],
    *,
    report: GlobalScoutReport,
    max_signatures: int,
) -> tuple[StructuralSignature, ...]:
    if not report.candidates:
        return signatures[: max(1, int(max_signatures))]
    support_hashes = {
        signature_hash
        for candidate in report.candidates
        for signature_hash in candidate.support_signature_hashes
    }
    selected = [
        signature for signature in signatures if signature.signature_hash in support_hashes
    ]
    return tuple(selected[: max(1, int(max_signatures))])


def _compact_scout_notes(report: GlobalScoutReport) -> dict[str, Any]:
    return {
        "latent_pattern_candidate_count": len(report.candidates),
        "signatures_seen": report.signatures_seen,
        "neighborhoods_built": report.neighborhoods_built,
        "buckets_considered": report.buckets_considered,
        "buckets_pruned": report.buckets_pruned,
        "max_bucket_size": report.max_bucket_size,
        "all_pairs_avoided_estimate": report.all_pairs_avoided_estimate,
        "canonical_write": False,
    }


__all__ = [
    "LatentPatternProfileInput",
    "build_latent_pattern_profile_input",
]
