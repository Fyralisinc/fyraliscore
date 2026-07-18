"""Generic, label-blind conversation episode boundary projection."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime


_REFERENCE = re.compile(
    r"\b(?P<name>[a-z][a-z0-9_-]{2,})\s+(?:episode|case|ticket|incident|project)\s*[-:#]?\s*(?P<id>[a-z0-9_-]+)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ConversationBoundaryObservation:
    observation_id: str
    occurred_at: datetime
    content_text: str
    source_container_id: str | None = None
    entity_refs: tuple[str, ...] = ()


def explicit_topic_reference(text: str) -> str | None:
    """Return a source-independent explicit business-object/topic reference.

    This intentionally recognizes only explicit references. Pronouns and weak
    lexical similarity must not silently override authenticated source
    topology; they remain attached to their source container for later
    contextual inquiry.
    """

    match = _REFERENCE.search(text)
    if match is None:
        return None
    return f"{match.group('name').casefold()}:{match.group('id').casefold()}"


def project_conversation_episode_boundaries(
    observations: tuple[ConversationBoundaryObservation, ...],
) -> tuple[tuple[str, ...], ...]:
    """Project deterministic episode hypotheses without evaluator knowledge.

    An explicit topic reference may split a Slack thread or source object and
    may join observations across sources. With no explicit reference, the
    authenticated source container remains the conservative boundary.
    """

    ordered = sorted(
        observations, key=lambda row: (row.occurred_at, row.observation_id)
    )
    parent = {item.observation_id: item.observation_id for item in ordered}

    def find(value: str) -> str:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    first_by_key: dict[str, str] = {}
    for item in ordered:
        topic = explicit_topic_reference(item.content_text)
        entity_keys = {
            f"entity:{value.strip().casefold()}"
            for value in item.entity_refs
            if value and value.strip()
        }
        keys = entity_keys or (
            {f"topic:{topic}"} if topic is not None
            else {f"source:{item.source_container_id}"}
            if item.source_container_id
            else {f"singleton:{item.observation_id}"}
        )
        for key in sorted(keys):
            prior = first_by_key.setdefault(key, item.observation_id)
            union(item.observation_id, prior)

    groups: dict[str, list[str]] = {}
    for item in ordered:
        groups.setdefault(find(item.observation_id), []).append(item.observation_id)
    return tuple(tuple(items) for _, items in sorted(groups.items()))


__all__ = [
    "ConversationBoundaryObservation",
    "explicit_topic_reference",
    "project_conversation_episode_boundaries",
]
