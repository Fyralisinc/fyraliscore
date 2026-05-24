"""Shared ranking signals for judgment-worthy organizational work."""
from __future__ import annotations

from dataclasses import dataclass


def clamp_score(v: float) -> float:
    return max(0.0, min(1.0, float(v)))


@dataclass(frozen=True)
class JudgmentScores:
    """Signals used to decide whether something deserves attention.

    `reversibility` is "irreversibility pressure": 1.0 means the
    decision is hard to undo or costly to delay; 0.0 means reversible.
    """

    impact: float = 0.0
    uncertainty: float = 0.0
    urgency: float = 0.0
    reversibility: float = 0.0
    authority_required: float = 0.0
    actionability: float = 0.0
    novelty: float = 0.0
    confidence: float = 0.0

    @property
    def judgment_leverage(self) -> float:
        # Bias toward useful, time-sensitive, human-authority decisions.
        # Confidence matters, but high-confidence low-impact trivia should
        # never outrank an uncertain, material judgment point.
        return clamp_score(
            0.22 * clamp_score(self.impact)
            + 0.16 * clamp_score(self.urgency)
            + 0.15 * clamp_score(self.actionability)
            + 0.14 * clamp_score(self.authority_required)
            + 0.12 * clamp_score(self.uncertainty)
            + 0.10 * clamp_score(self.novelty)
            + 0.07 * clamp_score(self.reversibility)
            + 0.04 * clamp_score(self.confidence)
        )

    def as_dict(self) -> dict[str, float]:
        return {
            "impact": clamp_score(self.impact),
            "uncertainty": clamp_score(self.uncertainty),
            "urgency": clamp_score(self.urgency),
            "reversibility": clamp_score(self.reversibility),
            "authority_required": clamp_score(self.authority_required),
            "actionability": clamp_score(self.actionability),
            "novelty": clamp_score(self.novelty),
            "confidence": clamp_score(self.confidence),
            "judgment_leverage": self.judgment_leverage,
        }
