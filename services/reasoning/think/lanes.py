"""Think lane classification.

The lane is an operational routing hint: it decides which worker process should
lease a trigger. It is not a separate mutation path. Every lane still converges
through the shared Think validator, applier, model_events, and post-commit flow.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class ThinkLane(str, Enum):
    REFLEX = "reflex"
    BATCH_MEMORY = "batch_memory"
    RELATIONSHIP = "relationship"
    DEEP_SYNTHESIS = "deep_synthesis"
    REPAIR = "repair"


ALL_THINK_LANES: frozenset[ThinkLane] = frozenset(ThinkLane)


@dataclass(frozen=True, slots=True)
class ThinkLaneDecision:
    lane: ThinkLane
    reason: str


def parse_lane_filter(raw: str | None) -> frozenset[ThinkLane] | None:
    """Parse ``THINK_WORKER_LANES``.

    ``None``, empty strings, and ``all`` mean no filter: the worker can process
    every lane, preserving the legacy single-worker behavior.
    """

    if raw is None or raw.strip() == "":
        return None
    parts = [part.strip().lower() for part in raw.split(",") if part.strip()]
    if not parts or any(part == "all" for part in parts):
        return None
    aliases = {
        "batch": ThinkLane.BATCH_MEMORY,
        "memory": ThinkLane.BATCH_MEMORY,
        "signal_memory": ThinkLane.BATCH_MEMORY,
        "relationships": ThinkLane.RELATIONSHIP,
        "deep": ThinkLane.DEEP_SYNTHESIS,
        "synthesis": ThinkLane.DEEP_SYNTHESIS,
    }
    lanes: set[ThinkLane] = set()
    for part in parts:
        lane = aliases.get(part)
        if lane is None:
            try:
                lane = ThinkLane(part)
            except ValueError as exc:
                allowed = ", ".join(sorted(lane_item.value for lane_item in ThinkLane))
                raise ValueError(
                    f"unknown Think lane {part!r}; expected one of: {allowed}, all"
                ) from exc
        lanes.add(lane)
    return frozenset(lanes)


def classify_trigger_lane(
    trigger_kind: str | None,
    trigger_subkind: str | None,
    payload: dict[str, Any] | None = None,
) -> ThinkLaneDecision:
    """Classify a trigger into one operational Think lane."""

    kind = str(trigger_kind or "")
    subkind = str(trigger_subkind or "")
    payload = payload or {}

    if _is_repair_trigger(kind, subkind, payload):
        return ThinkLaneDecision(ThinkLane.REPAIR, "repair_feedback_or_audit")
    if _is_reflex_trigger(kind, subkind):
        return ThinkLaneDecision(ThinkLane.REFLEX, "authoritative_or_cheap")
    if kind == "T1":
        return ThinkLaneDecision(ThinkLane.BATCH_MEMORY, "signal_memory")
    if _is_relationship_trigger(kind, subkind, payload):
        return ThinkLaneDecision(ThinkLane.RELATIONSHIP, "relationship_candidate")
    return ThinkLaneDecision(ThinkLane.DEEP_SYNTHESIS, "open_ended")


def lane_sql_predicate(
    allowed_lanes: frozenset[ThinkLane] | None,
    *,
    prefix: str = "",
) -> str | None:
    """Return a SQL predicate matching the exclusive classifier above."""

    if allowed_lanes is None or allowed_lanes == ALL_THINK_LANES:
        return None
    selected = [
        _sql_for_lane(lane, prefix=prefix)
        for lane in sorted(allowed_lanes, key=lambda item: item.value)
    ]
    return "(" + " OR ".join(selected) + ")"


def lane_names(allowed_lanes: frozenset[ThinkLane] | None) -> str:
    if allowed_lanes is None:
        return "all"
    return ",".join(
        lane.value for lane in sorted(allowed_lanes, key=lambda item: item.value)
    )


def _is_repair_trigger(
    trigger_kind: str,
    trigger_subkind: str,
    payload: dict[str, Any],
) -> bool:
    return (
        trigger_subkind == "representation_repair"
        or "validation_feedback" in payload
        or "repair_key" in payload
        or "repair_intent" in payload
    )


def _is_reflex_trigger(trigger_kind: str, trigger_subkind: str) -> bool:
    return (
        (trigger_kind == "T1" and trigger_subkind == "state_change")
        or (
            trigger_kind == "T2"
            and trigger_subkind
            in {
                "belief_updated",
                "prediction_overdue",
                "prediction_deadline",
                "hypothesis_approved",
                "hypothesis_corrected",
                "hypothesis_other",
            }
        )
        or (trigger_kind == "T3" and trigger_subkind == "missing_transition")
        or (
            trigger_kind == "T4"
            and trigger_subkind
            in {
                "background_maintenance",
                "entity_resolution_proposal",
                "model_reeval",
            }
        )
    )


def _is_relationship_trigger(
    trigger_kind: str,
    trigger_subkind: str,
    payload: dict[str, Any],
) -> bool:
    return (
        (trigger_kind == "T4" and trigger_subkind == "latent_relationship_candidate")
        or "relationship_candidate_id" in payload
        or "relationship_candidate_ids" in payload
    )


def _sql_for_lane(lane: ThinkLane, *, prefix: str) -> str:
    repair = _repair_sql(prefix)
    reflex = _reflex_sql(prefix)
    relationship = _relationship_sql(prefix)
    batch_memory = _batch_memory_sql(prefix, repair=repair, reflex=reflex)
    if lane is ThinkLane.REPAIR:
        return repair
    if lane is ThinkLane.REFLEX:
        return f"({reflex} AND NOT ({repair}))"
    if lane is ThinkLane.BATCH_MEMORY:
        return batch_memory
    if lane is ThinkLane.RELATIONSHIP:
        return f"({relationship} AND NOT ({repair}))"
    if lane is ThinkLane.DEEP_SYNTHESIS:
        return (
            f"(NOT ({repair}) AND NOT ({reflex}) AND "
            f"NOT ({batch_memory}) AND NOT ({relationship}))"
        )
    raise AssertionError(f"unhandled Think lane: {lane}")


def _col(prefix: str, name: str) -> str:
    return f"{prefix}.{name}" if prefix else name


def _repair_sql(prefix: str) -> str:
    trigger_subkind = _col(prefix, "trigger_subkind")
    payload = _col(prefix, "payload")
    return (
        f"({trigger_subkind} = 'representation_repair' "
        f"OR {payload} ? 'validation_feedback' "
        f"OR {payload} ? 'repair_key' "
        f"OR {payload} ? 'repair_intent')"
    )


def _reflex_sql(prefix: str) -> str:
    trigger_kind = _col(prefix, "trigger_kind")
    trigger_subkind = _col(prefix, "trigger_subkind")
    return (
        f"(({trigger_kind} = 'T1' AND {trigger_subkind} = 'state_change') "
        f"OR ({trigger_kind} = 'T2' AND {trigger_subkind} IN "
        "('belief_updated', 'prediction_overdue', 'prediction_deadline', "
        "'hypothesis_approved', 'hypothesis_corrected', 'hypothesis_other')) "
        f"OR ({trigger_kind} = 'T3' AND {trigger_subkind} = 'missing_transition') "
        f"OR ({trigger_kind} = 'T4' AND {trigger_subkind} IN "
        "('background_maintenance', 'entity_resolution_proposal', "
        "'model_reeval')))"
    )


def _relationship_sql(prefix: str) -> str:
    trigger_kind = _col(prefix, "trigger_kind")
    trigger_subkind = _col(prefix, "trigger_subkind")
    payload = _col(prefix, "payload")
    return (
        f"(({trigger_kind} = 'T4' "
        f"AND {trigger_subkind} = 'latent_relationship_candidate') "
        f"OR {payload} ? 'relationship_candidate_id' "
        f"OR {payload} ? 'relationship_candidate_ids')"
    )


def _batch_memory_sql(prefix: str, *, repair: str, reflex: str) -> str:
    trigger_kind = _col(prefix, "trigger_kind")
    return f"({trigger_kind} = 'T1' AND NOT ({repair}) AND NOT ({reflex}))"
