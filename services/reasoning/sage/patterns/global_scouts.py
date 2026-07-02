"""Bounded global scouts for non-obvious SAGE pattern candidates."""
from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Iterable

from services.reasoning.sage.patterns.signatures import structural_neighborhood_keys
from services.reasoning.sage.patterns.types import (
    PatternScoutCandidate,
    StructuralSignature,
)


@dataclass(frozen=True, slots=True)
class GlobalScoutReport:
    """Result of one bounded global-scout pass."""

    candidates: tuple[PatternScoutCandidate, ...]
    signatures_seen: int
    neighborhoods_built: int
    buckets_considered: int
    buckets_pruned: int
    max_bucket_size: int

    @property
    def all_pairs_avoided_estimate(self) -> int:
        n = self.signatures_seen
        possible_pairs = max(0, (n * (n - 1)) // 2)
        candidate_pairs = sum(
            max(0, (candidate.support_count * (candidate.support_count - 1)) // 2)
            for candidate in self.candidates
        )
        return max(0, possible_pairs - candidate_pairs)

    def notes(self) -> dict[str, object]:
        return {
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "signatures_seen": self.signatures_seen,
            "neighborhoods_built": self.neighborhoods_built,
            "buckets_considered": self.buckets_considered,
            "buckets_pruned": self.buckets_pruned,
            "max_bucket_size": self.max_bucket_size,
            "all_pairs_avoided_estimate": self.all_pairs_avoided_estimate,
            "canonical_write": False,
        }


def scout_global_patterns(
    signatures: Iterable[StructuralSignature],
    *,
    min_support: int = 3,
    min_surface_domains: int = 2,
    max_bucket_size: int = 64,
    max_candidates: int = 20,
) -> GlobalScoutReport:
    """Find far-surface, near-structure latent pattern candidates.

    The scout never compares every signature to every other signature. It builds
    bounded structural-neighborhood buckets and only summarizes buckets that
    already have enough support and surface diversity.
    """

    signature_list = [
        signature
        for signature in signatures
        if len(signature.shape_facets) >= 2 and signature.support_weight > 0
    ]
    index: dict[tuple[str, ...], list[StructuralSignature]] = defaultdict(list)
    neighborhoods_built = 0
    for signature in signature_list:
        for key in structural_neighborhood_keys(signature):
            index[key].append(signature)
            neighborhoods_built += 1

    candidates_by_hash: dict[str, PatternScoutCandidate] = {}
    buckets_pruned = 0
    for key, bucket in index.items():
        if len(bucket) < max(1, int(min_support)):
            continue
        bounded_bucket = _bounded_bucket(bucket, max_bucket_size=max_bucket_size)
        if len(bounded_bucket) < len(bucket):
            buckets_pruned += 1
        candidate = _candidate_from_bucket(
            key,
            bounded_bucket,
            min_surface_domains=max(1, int(min_surface_domains)),
        )
        if candidate is None:
            continue
        existing = candidates_by_hash.get(candidate.candidate_hash)
        if existing is None or candidate.confidence > existing.confidence:
            candidates_by_hash[candidate.candidate_hash] = candidate

    candidates = tuple(
        sorted(
            candidates_by_hash.values(),
            key=lambda item: (
                -item.promotion_readiness_score,
                -item.confidence,
                item.candidate_hash,
            ),
        )[: max(0, int(max_candidates))]
    )
    return GlobalScoutReport(
        candidates=candidates,
        signatures_seen=len(signature_list),
        neighborhoods_built=neighborhoods_built,
        buckets_considered=len(index),
        buckets_pruned=buckets_pruned,
        max_bucket_size=max(1, int(max_bucket_size)),
    )


def _candidate_from_bucket(
    key: tuple[str, ...],
    bucket: list[StructuralSignature],
    *,
    min_surface_domains: int,
) -> PatternScoutCandidate | None:
    domains = _surface_domains(bucket)
    if len(domains) < min_surface_domains:
        return None
    source_refs = tuple(
        dict.fromkeys(signature.source_ref for signature in bucket if signature.source_ref)
    )
    signature_hashes = tuple(
        dict.fromkeys(signature.signature_hash for signature in bucket)
    )
    counterexamples = sum(1 for signature in bucket if _is_counterexample(signature))
    outcome_cohesion = _outcome_cohesion(bucket)
    surface_distance = _surface_distance(bucket, domains=domains)
    avg_weight = sum(signature.support_weight for signature in bucket) / max(
        1,
        len(bucket),
    )
    utility_score = round(
        max(0.0, min(1.0, 0.45 * surface_distance + 0.35 * outcome_cohesion + 0.20 * avg_weight)),
        4,
    )
    confidence = round(
        max(
            0.0,
            min(
                1.0,
                0.30 * min(1.0, len(bucket) / 5.0)
                + 0.30 * min(1.0, len(domains) / 3.0)
                + 0.25 * outcome_cohesion
                + 0.15 * surface_distance
                - min(0.35, counterexamples * 0.08),
            ),
        ),
        4,
    )
    return PatternScoutCandidate(
        candidate_hash=_candidate_hash(key, signature_hashes),
        scout_kind="global_structural_neighborhood",
        shared_facets=key,
        support_signature_hashes=signature_hashes,
        support_source_refs=source_refs,
        support_count=len(bucket),
        surface_domain_count=len(domains),
        surface_distance_score=surface_distance,
        outcome_cohesion_score=outcome_cohesion,
        counterexample_count=counterexamples,
        utility_score=utility_score,
        confidence=confidence,
        explanation=_explanation(key, bucket, domains),
        metadata={
            "canonical_write": False,
            "source": "sage_global_scout",
        },
    )


def _bounded_bucket(
    bucket: list[StructuralSignature],
    *,
    max_bucket_size: int,
) -> list[StructuralSignature]:
    if len(bucket) <= max_bucket_size:
        return list(bucket)
    ordered = sorted(
        bucket,
        key=lambda signature: (
            -signature.support_weight,
            signature.source_ref or "",
            signature.signature_hash,
        ),
    )
    out: list[StructuralSignature] = []
    seen_surface: set[tuple[str, ...]] = set()
    for signature in ordered:
        surface = signature.surface_key
        if surface in seen_surface and len(out) >= max_bucket_size // 2:
            continue
        out.append(signature)
        seen_surface.add(surface)
        if len(out) >= max(1, int(max_bucket_size)):
            break
    return out


def _surface_domains(bucket: list[StructuralSignature]) -> tuple[str, ...]:
    values: list[str] = []
    for signature in bucket:
        values.extend(signature.domain_facets)
        if not signature.domain_facets:
            values.extend(signature.surface_terms[:2])
    return tuple(sorted(dict.fromkeys(value for value in values if value)))


def _surface_distance(
    bucket: list[StructuralSignature],
    *,
    domains: tuple[str, ...],
) -> float:
    if not bucket:
        return 0.0
    unique_surfaces = {
        signature.surface_key or (signature.source_ref or signature.signature_hash,)
        for signature in bucket
    }
    domain_factor = min(1.0, len(domains) / max(2.0, len(bucket) * 0.6))
    surface_factor = min(1.0, len(unique_surfaces) / max(1, len(bucket)))
    return round(0.55 * domain_factor + 0.45 * surface_factor, 4)


def _outcome_cohesion(bucket: list[StructuralSignature]) -> float:
    outcomes = [
        signature.observed_outcome or signature.expected_outcome
        for signature in bucket
        if signature.observed_outcome or signature.expected_outcome
    ]
    if not outcomes:
        shared_outcome_facets = Counter(
            facet for signature in bucket for facet in signature.outcome_facets
        )
        if not shared_outcome_facets:
            return 0.45
        return round(min(1.0, shared_outcome_facets.most_common(1)[0][1] / len(bucket)), 4)
    return round(Counter(outcomes).most_common(1)[0][1] / max(1, len(outcomes)), 4)


def _is_counterexample(signature: StructuralSignature) -> bool:
    if bool(signature.metadata.get("counterexample")):
        return True
    if signature.expected_outcome and signature.observed_outcome:
        return signature.expected_outcome != signature.observed_outcome
    return False


def _candidate_hash(
    key: tuple[str, ...],
    signature_hashes: tuple[str, ...],
) -> str:
    payload = {
        "key": key,
        "support_signature_hashes": signature_hashes,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:24]


def _explanation(
    key: tuple[str, ...],
    bucket: list[StructuralSignature],
    domains: tuple[str, ...],
) -> str:
    facets = ", ".join(key[:5])
    return (
        f"{len(bucket)} surface-different signals share structural facets "
        f"({facets}) across {len(domains)} domain surface(s)."
    )


__all__ = [
    "GlobalScoutReport",
    "scout_global_patterns",
]
