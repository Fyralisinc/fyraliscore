"""Pure Slack source-structure projection over immutable Observations.

Slack message edits remain distinct Observation revisions. This projector
derives the rebuildable conversation revision heads and topology edges needed
by context selection without mutating, replacing, or deleting source evidence.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from lib.contracts.kernel import canonical_sha256
from lib.contracts.perception import (
    ConversationEventKind,
    ConversationEventRevision,
    ConversationTopologyEdge,
    ConversationTopologyKind,
    SourceRetentionFate,
)
from lib.shared.entity_phrases import phrase_requires_context


_PROJECTOR_VERSION = "slack-source-structure-v1"
_CONTEXT_REFERENCE_TOKENS = {
    "again",
    "did",
    "do",
    "does",
    "he",
    "here",
    "it",
    "proceed",
    "she",
    "that",
    "the",
    "them",
    "they",
    "this",
    "those",
    "we",
}


class SlackSourceRevisionFate(StrEnum):
    CURRENT = "current"
    SUPERSEDED = "superseded"
    TOMBSTONE = "tombstone"
    REACTION_EVIDENCE = "reaction_evidence"


@dataclass(frozen=True)
class SlackSourceObservation:
    tenant_id: UUID
    event_revision_id: str
    occurred_at: datetime
    content_text: str
    content: dict[str, Any]


@dataclass(frozen=True)
class SlackSourceStructure:
    revisions: tuple[ConversationEventRevision, ...]
    topology_edges: tuple[ConversationTopologyEdge, ...]
    revision_fates: tuple[tuple[str, SlackSourceRevisionFate], ...]

    def fate_for(self, event_revision_id: str) -> SlackSourceRevisionFate | None:
        return dict(self.revision_fates).get(event_revision_id)

    def incident_edge_ids(self, event_revision_id: str) -> tuple[str, ...]:
        return tuple(
            edge.edge_id
            for edge in self.topology_edges
            if event_revision_id
            in (edge.from_event_revision_id, edge.to_event_or_object_id)
        )

    def connected_revision_ids(
        self,
        event_revision_id: str,
        *,
        max_hops: int,
    ) -> tuple[str, ...]:
        """Return the bounded undirected source neighborhood around one revision."""

        if max_hops < 0:
            raise ValueError("max_hops must be non-negative")
        seen = {event_revision_id}
        frontier = {event_revision_id}
        for _ in range(max_hops):
            next_frontier: set[str] = set()
            for edge in self.topology_edges:
                left = edge.from_event_revision_id
                right = edge.to_event_or_object_id
                if left in frontier and right not in seen:
                    next_frontier.add(right)
                if right in frontier and left not in seen:
                    next_frontier.add(left)
            if not next_frontier:
                break
            seen.update(next_frontier)
            frontier = next_frontier
        seen.discard(event_revision_id)
        return tuple(sorted(seen))


def project_slack_source_structure(
    observations: tuple[SlackSourceObservation, ...],
) -> SlackSourceStructure:
    """Derive immutable revision lineage and bounded Slack topology."""

    if not observations:
        return SlackSourceStructure((), (), ())
    tenant_ids = {item.tenant_id for item in observations}
    if len(tenant_ids) != 1:
        raise ValueError("Slack source structure cannot cross tenant boundaries")
    event_revision_ids = [item.event_revision_id for item in observations]
    if len(event_revision_ids) != len(set(event_revision_ids)):
        raise ValueError("Slack source Observation revisions must be unique")

    grouped: dict[str, list[SlackSourceObservation]] = {}
    for item in observations:
        grouped.setdefault(_source_event_id(item), []).append(item)

    revision_by_id: dict[str, ConversationEventRevision] = {}
    revision_order: list[str] = []
    fates: dict[str, SlackSourceRevisionFate] = {}
    for source_event_id, source_revisions in sorted(grouped.items()):
        ordered = sorted(
            source_revisions,
            key=lambda item: (item.occurred_at, item.event_revision_id),
        )
        predecessor_revision_id: str | None = None
        for revision_number, item in enumerate(ordered, start=1):
            content = item.content
            event_kind = _event_kind(content)
            is_tombstone = event_kind is ConversationEventKind.DELETION
            supersedes_revision_id = (
                predecessor_revision_id
                if event_kind
                in {
                    ConversationEventKind.EDIT,
                    ConversationEventKind.DELETION,
                }
                else None
            )
            revision = ConversationEventRevision(
                tenant_id=item.tenant_id,
                event_id=f"slack-event:{source_event_id}",
                source_system="slack",
                source_event_id=source_event_id,
                revision_id=item.event_revision_id,
                revision_number=revision_number,
                kind=event_kind,
                author_source_id=str(content.get("user") or "slack:unknown"),
                emitted_at=item.occurred_at,
                observed_at=item.occurred_at,
                content_hash=(
                    None
                    if is_tombstone
                    else canonical_sha256(
                        {
                            "content_text": item.content_text,
                            "content": content,
                        }
                    )
                ),
                raw_evidence_ref=(
                    None if is_tombstone else item.event_revision_id
                ),
                retention_fate=(
                    SourceRetentionFate.LEGALLY_REDACTED_TOMBSTONE
                    if is_tombstone
                    else SourceRetentionFate.PAYLOAD_AVAILABLE
                ),
                retention_reason=(
                    str(
                        content.get("retention_reason")
                        or "slack_message_deleted"
                    )
                    if is_tombstone
                    else None
                ),
                supersedes_revision_id=supersedes_revision_id,
                source_thread_id=_source_thread_id(content),
                source_reply_to_id=_source_reply_to_id(content),
                linked_source_object_ids=_linked_source_object_ids(content),
            )
            revision_by_id[item.event_revision_id] = revision
            revision_order.append(item.event_revision_id)
            if supersedes_revision_id is not None:
                fates[predecessor_revision_id] = (
                    SlackSourceRevisionFate.SUPERSEDED
                )
            fates[item.event_revision_id] = (
                SlackSourceRevisionFate.TOMBSTONE
                if is_tombstone
                else SlackSourceRevisionFate.REACTION_EVIDENCE
                if event_kind is ConversationEventKind.REACTION
                else SlackSourceRevisionFate.CURRENT
            )
            predecessor_revision_id = item.event_revision_id

    authority_fingerprint = canonical_sha256(
        {
            "tenant_id": str(next(iter(tenant_ids))),
            "source_system": "slack",
        }
    )
    edges = [
        *_edit_edges(
            grouped=grouped,
            authority_fingerprint=authority_fingerprint,
        ),
        *_thread_edges(
            observations=observations,
            fates=fates,
            authority_fingerprint=authority_fingerprint,
        ),
        *_reaction_edges(
            observations=observations,
            authority_fingerprint=authority_fingerprint,
        ),
    ]
    edge_by_id = {edge.edge_id: edge for edge in edges}
    return SlackSourceStructure(
        revisions=tuple(
            revision_by_id[key] for key in revision_order
        ),
        topology_edges=tuple(edge_by_id[key] for key in sorted(edge_by_id)),
        revision_fates=tuple(sorted(fates.items())),
    )


def slack_context_anchor_terms(phrase: str) -> tuple[str, ...]:
    """Return bounded lexical anchors for deictic cross-channel expansion."""

    if not phrase_requires_context(phrase):
        return ()
    return tuple(
        dict.fromkeys(
            token
            for token in re.findall(r"[a-z0-9]+", phrase.casefold())
            if len(token) >= 3 and token not in _CONTEXT_REFERENCE_TOKENS
        )
    )


def slack_text_matches_context_anchor(
    text: str,
    anchor_terms: tuple[str, ...],
) -> bool:
    tokens = set(re.findall(r"[a-z0-9]+", text.casefold()))
    return bool(tokens.intersection(anchor_terms))


def _edit_edges(
    *,
    grouped: dict[str, list[SlackSourceObservation]],
    authority_fingerprint: str,
) -> tuple[ConversationTopologyEdge, ...]:
    edges: list[ConversationTopologyEdge] = []
    for source_revisions in grouped.values():
        ordered = sorted(
            source_revisions,
            key=lambda item: (item.occurred_at, item.event_revision_id),
        )
        for predecessor, successor in zip(ordered, ordered[1:], strict=False):
            if (
                _event_kind(successor.content)
                is not ConversationEventKind.EDIT
            ):
                continue
            predecessor_ts = _emitted_source_ts(predecessor.content)
            successor_ts = _emitted_source_ts(successor.content)
            if not predecessor_ts or not successor_ts:
                continue
            edges.append(
                ConversationTopologyEdge(
                    edge_id=f"slack-edit:{predecessor_ts}->{successor_ts}",
                    kind=ConversationTopologyKind.EDIT_OF,
                    from_event_revision_id=successor.event_revision_id,
                    to_event_or_object_id=predecessor.event_revision_id,
                    source_basis_refs=(
                        predecessor.event_revision_id,
                        successor.event_revision_id,
                    ),
                    projector_version=_PROJECTOR_VERSION,
                    authority_label_fingerprint=authority_fingerprint,
                )
            )
    return tuple(edges)


def _reaction_edges(
    *,
    observations: tuple[SlackSourceObservation, ...],
    authority_fingerprint: str,
) -> tuple[ConversationTopologyEdge, ...]:
    by_source = {
        (_channel(item.content), _logical_source_ts(item.content)): item
        for item in observations
        if _event_kind(item.content) is not ConversationEventKind.REACTION
    }
    edges: list[ConversationTopologyEdge] = []
    for reaction in observations:
        if _event_kind(reaction.content) is not ConversationEventKind.REACTION:
            continue
        channel = _channel(reaction.content)
        target_ts = reaction.content.get("reaction_item_ts")
        reaction_ts = _emitted_source_ts(reaction.content)
        if (
            not channel
            or not isinstance(target_ts, str)
            or not target_ts
            or not reaction_ts
        ):
            continue
        target = by_source.get((channel, target_ts))
        if target is None:
            continue
        edges.append(
            ConversationTopologyEdge(
                edge_id=f"slack-reaction:{target_ts}->{reaction_ts}",
                kind=ConversationTopologyKind.LINKS,
                from_event_revision_id=reaction.event_revision_id,
                to_event_or_object_id=target.event_revision_id,
                source_basis_refs=(
                    target.event_revision_id,
                    reaction.event_revision_id,
                ),
                projector_version=_PROJECTOR_VERSION,
                authority_label_fingerprint=authority_fingerprint,
            )
        )
    return tuple(edges)


def _thread_edges(
    *,
    observations: tuple[SlackSourceObservation, ...],
    fates: dict[str, SlackSourceRevisionFate],
    authority_fingerprint: str,
) -> tuple[ConversationTopologyEdge, ...]:
    current = [
        item
        for item in observations
        if fates[item.event_revision_id] is SlackSourceRevisionFate.CURRENT
    ]
    by_channel_ts = {
        (_channel(item.content), _logical_source_ts(item.content)): item
        for item in current
        if _channel(item.content) and _logical_source_ts(item.content)
    }
    threads: dict[tuple[str, str], dict[str, SlackSourceObservation]] = {}
    for item in current:
        channel = _channel(item.content)
        thread_ts = item.content.get("thread_ts")
        if not channel or not isinstance(thread_ts, str) or not thread_ts:
            continue
        key = (channel, thread_ts)
        threads.setdefault(key, {})[item.event_revision_id] = item
        root = by_channel_ts.get(key)
        if root is not None:
            threads[key][root.event_revision_id] = root

    edges: list[ConversationTopologyEdge] = []
    for (_, root_ts), members in sorted(threads.items()):
        ordered = sorted(
            members.values(),
            key=lambda item: (
                _slack_ts_order(_logical_source_ts(item.content)),
                item.event_revision_id,
            ),
        )
        for predecessor, successor in zip(ordered, ordered[1:], strict=False):
            predecessor_ts = _logical_source_ts(predecessor.content)
            successor_ts = _logical_source_ts(successor.content)
            if not predecessor_ts or not successor_ts:
                continue
            edges.append(
                ConversationTopologyEdge(
                    edge_id=f"slack-thread:{predecessor_ts}->{successor_ts}",
                    kind=(
                        ConversationTopologyKind.THREAD_ROOT
                        if predecessor_ts == root_ts
                        else ConversationTopologyKind.REPLY_TO
                    ),
                    from_event_revision_id=successor.event_revision_id,
                    to_event_or_object_id=predecessor.event_revision_id,
                    source_basis_refs=(
                        predecessor.event_revision_id,
                        successor.event_revision_id,
                    ),
                    projector_version=_PROJECTOR_VERSION,
                    authority_label_fingerprint=authority_fingerprint,
                )
            )
    return tuple(edges)


def _source_event_id(item: SlackSourceObservation) -> str:
    channel = _channel(item.content)
    source_ts = _logical_source_ts(item.content)
    if not channel or not source_ts:
        return f"unknown:{item.event_revision_id}"
    return f"{channel}:{source_ts}"


def _event_kind(content: dict[str, Any]) -> ConversationEventKind:
    if content.get("subtype") == "message_deleted":
        return ConversationEventKind.DELETION
    if content.get("event_type") in {
        "reaction_added",
        "reaction_removed",
    }:
        return ConversationEventKind.REACTION
    if isinstance(content.get("original_ts"), str):
        return ConversationEventKind.EDIT
    if isinstance(content.get("thread_ts"), str):
        return ConversationEventKind.REPLY
    return ConversationEventKind.MESSAGE


def _channel(content: dict[str, Any]) -> str:
    value = content.get("channel")
    return value if isinstance(value, str) else ""


def _logical_source_ts(content: dict[str, Any]) -> str:
    original = content.get("original_ts")
    if isinstance(original, str) and original:
        return original
    return _emitted_source_ts(content)


def _emitted_source_ts(content: dict[str, Any]) -> str:
    value = content.get("ts")
    return value if isinstance(value, str) else ""


def _source_thread_id(content: dict[str, Any]) -> str | None:
    channel = _channel(content)
    thread_ts = content.get("thread_ts")
    if not channel or not isinstance(thread_ts, str) or not thread_ts:
        return None
    return f"slack-thread:{channel}:{thread_ts}"


def _source_reply_to_id(content: dict[str, Any]) -> str | None:
    channel = _channel(content)
    thread_ts = content.get("thread_ts")
    if not channel or not isinstance(thread_ts, str) or not thread_ts:
        return None
    return f"slack-event:{channel}:{thread_ts}"


def _linked_source_object_ids(content: dict[str, Any]) -> tuple[str, ...]:
    channel = _channel(content)
    target_ts = content.get("reaction_item_ts")
    if not channel or not isinstance(target_ts, str) or not target_ts:
        return ()
    return (f"slack-event:{channel}:{target_ts}",)


def _slack_ts_order(value: str) -> tuple[int, str]:
    try:
        return (int(value.replace(".", "")), value)
    except ValueError:
        return (0, value)


__all__ = [
    "SlackSourceObservation",
    "SlackSourceRevisionFate",
    "SlackSourceStructure",
    "project_slack_source_structure",
    "slack_context_anchor_terms",
    "slack_text_matches_context_anchor",
]
