"""Helpers for keeping model evidence support inspectable.

Large repetitive runs should preserve evidence meaning without letting one
model accumulate tens of thousands of supporting_event_ids. These helpers keep
anchors, a deterministic middle sample, and recent evidence.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any
from uuid import UUID


DEFAULT_SUPPORTING_EVENT_IDS_MAX = 240


@dataclass(frozen=True, slots=True)
class CompactedSupport:
    event_ids: list[UUID]
    total_seen: int
    retained_count: int
    dropped_count: int
    compacted: bool
    policy: str

    def to_metadata(self) -> dict[str, Any]:
        return {
            "total_seen": self.total_seen,
            "retained_count": self.retained_count,
            "dropped_count": self.dropped_count,
            "compacted": self.compacted,
            "policy": self.policy,
        }


def supporting_event_ids_max() -> int:
    raw = os.getenv("THINK_SUPPORTING_EVENT_IDS_MAX")
    if raw is None:
        return DEFAULT_SUPPORTING_EVENT_IDS_MAX
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_SUPPORTING_EVENT_IDS_MAX
    return max(1, value)


def compact_supporting_event_ids(
    *groups: Any,
    max_ids: int | None = None,
    preserve_anchors: int | None = None,
    preserve_recent: int | None = None,
) -> CompactedSupport:
    """Deduplicate and compact supporting event IDs.

    The retention policy keeps:
    - early anchors, which explain why the model was born;
    - a deterministic sample through the middle, which preserves repetitive
      cadence meaning;
    - recent evidence, which preserves the model's current state.
    """

    limit = max(1, int(max_ids or supporting_event_ids_max()))
    ordered = _flatten_uuid_groups(groups)
    total_seen = len(ordered)
    if total_seen <= limit:
        return CompactedSupport(
            event_ids=ordered,
            total_seen=total_seen,
            retained_count=total_seen,
            dropped_count=0,
            compacted=False,
            policy="dedupe_only",
        )

    anchor_limit = min(
        max(1, preserve_anchors if preserve_anchors is not None else limit // 6),
        limit,
    )
    recent_limit = min(
        max(1, preserve_recent if preserve_recent is not None else limit // 2),
        limit - anchor_limit,
    )
    anchors = ordered[:anchor_limit]
    anchor_set = set(anchors)

    recent: list[UUID] = []
    recent_set: set[UUID] = set()
    for event_id in reversed(ordered):
        if event_id in anchor_set or event_id in recent_set:
            continue
        recent.append(event_id)
        recent_set.add(event_id)
        if len(recent) >= recent_limit:
            break
    recent.reverse()

    middle_slots = max(0, limit - len(anchors) - len(recent))
    excluded = anchor_set | recent_set
    middle_pool = [event_id for event_id in ordered if event_id not in excluded]
    middle = _stratified_sample(middle_pool, middle_slots)

    retained = [*anchors, *middle, *recent]
    retained = _dedupe_preserve_order(retained)
    if len(retained) > limit:
        retained = retained[:limit]

    return CompactedSupport(
        event_ids=retained,
        total_seen=total_seen,
        retained_count=len(retained),
        dropped_count=max(0, total_seen - len(retained)),
        compacted=True,
        policy="anchors_stratified_middle_recent",
    )


def _flatten_uuid_groups(groups: tuple[Any, ...]) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for group in groups:
        if group is None:
            continue
        values = group if isinstance(group, (list, tuple, set)) else (group,)
        for value in values:
            uid = _coerce_uuid_or_none(value)
            if uid is None or uid in seen:
                continue
            seen.add(uid)
            out.append(uid)
    return out


def _stratified_sample(values: list[UUID], slots: int) -> list[UUID]:
    if slots <= 0 or not values:
        return []
    if len(values) <= slots:
        return list(values)
    if slots == 1:
        return [values[len(values) // 2]]
    last_index = len(values) - 1
    picked: list[UUID] = []
    seen_indexes: set[int] = set()
    for position in range(slots):
        index = round(position * last_index / (slots - 1))
        if index in seen_indexes:
            continue
        seen_indexes.add(index)
        picked.append(values[index])
    return picked


def _dedupe_preserve_order(values: list[UUID]) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _coerce_uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return value if isinstance(value, UUID) else UUID(str(value))
    except (TypeError, ValueError):
        return None


__all__ = [
    "CompactedSupport",
    "compact_supporting_event_ids",
    "supporting_event_ids_max",
]
