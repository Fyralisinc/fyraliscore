"""Drift handling for latent SAGE pattern priors."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from services.reasoning.sage.company_profile.types import (
    CompanyLearningProfile,
    LearningPrior,
)


@dataclass(frozen=True, slots=True)
class PatternModelRepairProposal:
    """Think-facing proposal to review an explicit Pattern Model."""

    repair_key: str
    repair_intent: str
    pattern_model_id: str
    prior_key: str
    reason: str
    confidence: float
    seed_natural_text: str
    payload: dict[str, Any]

    @property
    def canonical_write(self) -> bool:
        return False

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["canonical_write"] = False
        return data


def pattern_model_repair_proposals_from_profile(
    profile: CompanyLearningProfile,
    *,
    max_proposals: int = 5,
    min_drift_score: float = 0.35,
    max_decay: float = 0.74,
) -> tuple[PatternModelRepairProposal, ...]:
    """Convert decayed latent pattern priors into bounded repair proposals.

    The output is suitable for a T4 `representation_repair` trigger payload,
    but this helper intentionally does not enqueue or mutate anything.
    """

    proposals: list[PatternModelRepairProposal] = []
    for prior in sorted(
        profile.priors_for_kind("latent_pattern"),
        key=lambda item: (item.decay, -abs(item.effective_score), item.key),
    ):
        if len(proposals) >= max(0, int(max_proposals)):
            break
        pattern_model_id = _pattern_model_id(prior)
        if not pattern_model_id:
            continue
        drift_score = _drift_score(prior)
        counterexamples = _int(prior.metadata.get("counterexample_count"))
        if (
            prior.decay > max_decay
            and drift_score < min_drift_score
            and counterexamples < 2
        ):
            continue
        proposals.append(
            _proposal_from_prior(
                prior,
                pattern_model_id=pattern_model_id,
                drift_score=drift_score,
                counterexamples=counterexamples,
            )
        )
    return tuple(proposals)


def _proposal_from_prior(
    prior: LearningPrior,
    *,
    pattern_model_id: str,
    drift_score: float,
    counterexamples: int,
) -> PatternModelRepairProposal:
    reason = _repair_reason(prior, drift_score=drift_score, counterexamples=counterexamples)
    repair_key = f"pattern_model_drift:{pattern_model_id}:{prior.key}"
    seed_text = (
        "Representation repair needed: review_pattern_model_drift. "
        f"Pattern Model {pattern_model_id} may no longer match recent company "
        f"behavior because {reason}."
    )
    confidence = round(
        max(
            0.0,
            min(
                1.0,
                0.30
                + 0.35 * max(0.0, min(1.0, drift_score))
                + 0.20 * (1.0 - max(0.0, min(1.0, prior.decay)))
                + 0.15 * min(1.0, counterexamples / 3.0),
            ),
        ),
        4,
    )
    payload = {
        "repair_key": repair_key,
        "repair_intent": "review_pattern_model_drift",
        "repair_source": "sage_latent_pattern_drift",
        "pattern_model_id": pattern_model_id,
        "model_ids": [pattern_model_id],
        "latent_pattern_prior_key": prior.key,
        "latent_pattern_decay": round(float(prior.decay), 4),
        "latent_pattern_effective_score": round(float(prior.effective_score), 4),
        "semantic_drift_score": round(float(drift_score), 4),
        "counterexample_count": counterexamples,
        "decay_reasons": list(prior.metadata.get("decay_reasons") or []),
        "success_metric": (
            "Pattern Model is confirmed, revised, archived, or counterevidence "
            "is attached."
        ),
        "seed_natural_text": seed_text,
        "canonical_write": False,
    }
    return PatternModelRepairProposal(
        repair_key=repair_key,
        repair_intent="review_pattern_model_drift",
        pattern_model_id=pattern_model_id,
        prior_key=prior.key,
        reason=reason,
        confidence=confidence,
        seed_natural_text=seed_text,
        payload=payload,
    )


def _repair_reason(
    prior: LearningPrior,
    *,
    drift_score: float,
    counterexamples: int,
) -> str:
    reasons = list(prior.metadata.get("decay_reasons") or [])
    if drift_score > 0 and not any("semantic_drift" in reason for reason in reasons):
        reasons.append(f"semantic_drift:{drift_score:.3f}")
    if counterexamples > 0 and not any("counterexamples" in reason for reason in reasons):
        reasons.append(f"counterexamples:{counterexamples}")
    if not reasons:
        reasons.append(f"latent prior decay={prior.decay:.3f}")
    return ", ".join(str(reason) for reason in reasons[:4])


def _pattern_model_id(prior: LearningPrior) -> str | None:
    for key in ("promoted_pattern_model_id", "pattern_model_id", "model_id"):
        raw = prior.metadata.get(key)
        if raw:
            return str(raw)
    return None


def _drift_score(prior: LearningPrior) -> float:
    return max(
        _float(prior.metadata.get("semantic_drift_score")),
        _float(prior.metadata.get("drift_score")),
        _float(prior.metadata.get("contradiction_score")),
    )


def _float(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value or 0.0)))
    except (TypeError, ValueError):
        return 0.0


def _int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


__all__ = [
    "PatternModelRepairProposal",
    "pattern_model_repair_proposals_from_profile",
]
