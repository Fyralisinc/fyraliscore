"""SAGE experience metabolism contract.

Models remember semantic truth about the world. SAGE remembers how Fyralis'
own retrieval, reasoning, and product choices performed, then turns that
experience into bounded future-behavior policy.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable, Literal


ExperienceStatus = Literal["idle", "sensed", "evaluated", "metabolized"]

_EVALUATION_EVENT_TYPES = frozenset({
    "node_used_in_valid_diff",
    "path_used_in_valid_diff",
    "reader_decision_used_in_valid_diff",
    "reader_decision_low_value",
    "outcome_quality_assessed",
    "validation_failed_due_to_missing_evidence",
    "validation_failed_due_to_bad_reference",
    "user_accepted_node",
    "user_contested_node",
    "model_later_confirmed",
    "model_later_falsified",
    "recommendation_acted_on",
    "recommendation_ignored",
})

_FUTURE_BEHAVIOR_EFFECTS = {
    "affordance_reinforces": "affordance_policy",
    "affordance_decays": "affordance_policy",
    "shortcut_creates_or_bumps": "shortcut_policy",
    "shortcut_decays": "shortcut_policy",
    "negative_memory_inserts": "negative_memory",
    "question_policy_updates": "question_policy",
    "structural_models_written": "structural_features",
    "structural_edges_written": "structural_features",
}


@dataclass(frozen=True, slots=True)
class SageExperienceLoopReport:
    """One auditable report for the SAGE experience loop.

    A loop is closed only when SAGE has all three pieces:
      * sensed experience events;
      * outcome/evaluation signal;
      * a policy effect that can affect future behavior.
    """

    status: ExperienceStatus
    closure_score: float
    outcome_event_count: int
    evaluation_event_count: int
    policy_effect_count: int
    canonical_candidate_count: int
    event_types: tuple[str, ...]
    future_behavior_levers: tuple[str, ...]
    blockers: tuple[str, ...]

    @property
    def closed(self) -> bool:
        return self.status == "metabolized"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def optimizer_metrics(self) -> dict[str, float]:
        return {
            "experience_outcome_events": float(self.outcome_event_count),
            "experience_evaluation_events": float(self.evaluation_event_count),
            "experience_policy_effects": float(self.policy_effect_count),
            "experience_canonical_candidates": float(self.canonical_candidate_count),
            "experience_future_behavior_levers": float(
                len(self.future_behavior_levers)
            ),
            "experience_closure_score": float(self.closure_score),
            "experience_loop_closed": 1.0 if self.closed else 0.0,
        }


def build_experience_loop_report(
    events: Iterable[Any],
    *,
    policy_effects: dict[str, int | float],
    canonical_candidate_count: int = 0,
) -> SageExperienceLoopReport:
    """Summarize whether SAGE turned experience into future policy."""

    event_type_list = [
        event_type for item in events if (event_type := _event_type(item))
    ]
    event_types = tuple(sorted(set(event_type_list)))
    outcome_event_count = len(event_type_list)
    evaluation_event_count = sum(
        1 for event_type in event_types if event_type in _EVALUATION_EVENT_TYPES
    )
    normalized_effects = {
        key: max(0, int(value or 0)) for key, value in policy_effects.items()
    }
    policy_effect_count = sum(normalized_effects.values())
    future_behavior_levers = tuple(
        sorted({
            lever
            for key, lever in _FUTURE_BEHAVIOR_EFFECTS.items()
            if normalized_effects.get(key, 0) > 0
        })
    )
    blockers: list[str] = []
    if outcome_event_count == 0:
        blockers.append("no_outcome_events")
    if evaluation_event_count == 0:
        blockers.append("no_evaluation_events")
    if policy_effect_count == 0:
        blockers.append("no_policy_effects")
    if not future_behavior_levers:
        blockers.append("no_future_behavior_levers")

    status: ExperienceStatus
    if outcome_event_count == 0:
        status = "idle"
    elif evaluation_event_count == 0:
        status = "sensed"
    elif policy_effect_count == 0 or not future_behavior_levers:
        status = "evaluated"
    else:
        status = "metabolized"

    closure_score = _closure_score(
        sensed=outcome_event_count > 0,
        evaluated=evaluation_event_count > 0,
        policy_effects=policy_effect_count > 0,
        future_behavior_levers=bool(future_behavior_levers),
    )
    return SageExperienceLoopReport(
        status=status,
        closure_score=closure_score,
        outcome_event_count=outcome_event_count,
        evaluation_event_count=evaluation_event_count,
        policy_effect_count=policy_effect_count,
        canonical_candidate_count=max(0, int(canonical_candidate_count or 0)),
        event_types=event_types,
        future_behavior_levers=future_behavior_levers,
        blockers=tuple(blockers),
    )


def _event_type(item: Any) -> str | None:
    if isinstance(item, dict):
        raw = item.get("event_type")
    else:
        raw = getattr(item, "event_type", None)
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _closure_score(
    *,
    sensed: bool,
    evaluated: bool,
    policy_effects: bool,
    future_behavior_levers: bool,
) -> float:
    score = 0.0
    if sensed:
        score += 0.20
    if evaluated:
        score += 0.30
    if policy_effects:
        score += 0.30
    if future_behavior_levers:
        score += 0.20
    return round(score, 4)


__all__ = [
    "ExperienceStatus",
    "SageExperienceLoopReport",
    "build_experience_loop_report",
]
