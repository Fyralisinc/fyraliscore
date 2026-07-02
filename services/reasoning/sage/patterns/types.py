"""Non-canonical SAGE pattern-learning types.

These dataclasses describe latent optimization memory, not company truth.
They may help SAGE route retrieval, ask better questions, or propose a Think
review, but they must not directly mutate Models.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


PatternAssessmentStatus = Literal[
    "not_ready",
    "shadow_ready",
    "promotion_candidate",
]


@dataclass(frozen=True, slots=True)
class StructuralSignature:
    """Surface-independent shape extracted from an event/model/residual.

    `signature_hash` is the structural hash. It intentionally excludes
    `domain_facets`, `actor_refs`, `entity_refs`, and `surface_terms` so global
    scouts can find far-apart cases with the same underlying coordination
    shape.
    """

    signature_hash: str
    source_kind: str
    role_facets: tuple[str, ...] = ()
    pressure_facets: tuple[str, ...] = ()
    outcome_facets: tuple[str, ...] = ()
    authority_facets: tuple[str, ...] = ()
    temporal_facets: tuple[str, ...] = ()
    coordination_facets: tuple[str, ...] = ()
    evidence_gap_facets: tuple[str, ...] = ()
    domain_facets: tuple[str, ...] = ()
    actor_refs: tuple[str, ...] = ()
    entity_refs: tuple[str, ...] = ()
    surface_terms: tuple[str, ...] = ()
    expected_outcome: str | None = None
    observed_outcome: str | None = None
    source_ref: str | None = None
    support_weight: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def shape_facets(self) -> tuple[str, ...]:
        facets: list[str] = []
        for prefix, values in (
            ("role", self.role_facets),
            ("pressure", self.pressure_facets),
            ("outcome", self.outcome_facets),
            ("authority", self.authority_facets),
            ("temporal", self.temporal_facets),
            ("coordination", self.coordination_facets),
            ("evidence_gap", self.evidence_gap_facets),
        ):
            facets.extend(f"{prefix}:{value}" for value in values)
        return tuple(sorted(dict.fromkeys(facets)))

    @property
    def surface_key(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                dict.fromkeys(
                    [
                        *(f"domain:{item}" for item in self.domain_facets),
                        *(f"actor:{item}" for item in self.actor_refs),
                        *(f"entity:{item}" for item in self.entity_refs),
                        *(f"term:{item}" for item in self.surface_terms[:8]),
                    ]
                )
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PatternScoutCandidate:
    """Bounded scout output for a latent recurring invariant.

    This is a proposal-shaped optimization signal. It is not a Pattern Model.
    """

    candidate_hash: str
    scout_kind: str
    shared_facets: tuple[str, ...]
    support_signature_hashes: tuple[str, ...]
    support_source_refs: tuple[str, ...]
    support_count: int
    surface_domain_count: int
    surface_distance_score: float
    outcome_cohesion_score: float
    counterexample_count: int = 0
    utility_score: float = 0.0
    confidence: float = 0.0
    explanation: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def promotion_readiness_score(self) -> float:
        counter_penalty = min(0.45, 0.12 * max(0, self.counterexample_count))
        score = (
            0.30 * min(1.0, self.support_count / 5.0)
            + 0.25 * min(1.0, self.surface_domain_count / 3.0)
            + 0.20 * self.surface_distance_score
            + 0.15 * self.outcome_cohesion_score
            + 0.10 * max(0.0, min(1.0, self.utility_score))
            - counter_penalty
        )
        return round(max(0.0, min(1.0, score)), 4)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["promotion_readiness_score"] = self.promotion_readiness_score
        return data


@dataclass(frozen=True, slots=True)
class PatternCounterexample:
    """A structurally related case that weakens a candidate."""

    source_ref: str
    signature_hash: str
    reason: str
    expected_outcome: str | None = None
    observed_outcome: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PromotionAssessment:
    """Think-facing assessment; it performs no canonical write."""

    status: PatternAssessmentStatus
    candidate_hash: str
    readiness_score: float
    rubric: dict[str, float]
    reasons: tuple[str, ...]
    counterexamples: tuple[PatternCounterexample, ...] = ()

    @property
    def ready_for_think_review(self) -> bool:
        return self.status == "promotion_candidate"

    def to_notes(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "candidate_hash": self.candidate_hash,
            "readiness_score": self.readiness_score,
            "rubric": dict(self.rubric),
            "reasons": list(self.reasons),
            "counterexamples": [
                counterexample.to_dict() for counterexample in self.counterexamples
            ],
            "canonical_write": False,
            "required_bridge": "Think review before Pattern Model promotion",
        }


__all__ = [
    "PatternAssessmentStatus",
    "PatternCounterexample",
    "PatternScoutCandidate",
    "PromotionAssessment",
    "StructuralSignature",
]
