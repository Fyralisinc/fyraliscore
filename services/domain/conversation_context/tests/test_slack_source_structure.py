from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from lib.contracts.perception import ConversationTopologyKind
from services.domain.conversation_context.slack_source_structure import (
    SlackSourceObservation,
    SlackSourceRevisionFate,
    project_slack_source_structure,
)


TENANT = UUID("55555555-5555-4555-8555-555555555555")


def _source(
    revision_id: str,
    ts: str,
    text: str,
    *,
    thread_ts: str | None = None,
    original_ts: str | None = None,
) -> SlackSourceObservation:
    content = {
        "channel": "C-GOLD",
        "ts": ts,
        "text": text,
        "user": "U-TEST",
        "thread_ts": thread_ts,
    }
    if original_ts is not None:
        content["original_ts"] = original_ts
    return SlackSourceObservation(
        tenant_id=TENANT,
        event_revision_id=revision_id,
        occurred_at=datetime.fromtimestamp(float(ts), tz=timezone.utc),
        content_text=text,
        content=content,
    )


def test_thread_projection_materializes_bounded_adjacent_source_edges() -> None:
    root = _source("observation:root:v1", "1760000000.100001", "Root")
    reply = _source(
        "observation:reply:v1",
        "1760000000.200001",
        "Reply",
        thread_ts="1760000000.100001",
    )
    focal = _source(
        "observation:focal:v1",
        "1760000000.300001",
        "Focal",
        thread_ts="1760000000.100001",
    )

    structure = project_slack_source_structure((root, reply, focal))

    assert tuple(edge.edge_id for edge in structure.topology_edges) == (
        "slack-thread:1760000000.100001->1760000000.200001",
        "slack-thread:1760000000.200001->1760000000.300001",
    )
    assert tuple(edge.kind for edge in structure.topology_edges) == (
        ConversationTopologyKind.THREAD_ROOT,
        ConversationTopologyKind.REPLY_TO,
    )
    assert structure.connected_revision_ids(
        focal.event_revision_id,
        max_hops=2,
    ) == (reply.event_revision_id, root.event_revision_id)


def test_edit_projection_keeps_observations_immutable_and_advances_head() -> None:
    original = _source(
        "observation:original:v1",
        "1760000100.100001",
        "Orion migration is blocked.",
    )
    edit = _source(
        "observation:edit:v1",
        "1760000100.200001",
        "Orion migration is unblocked.",
        original_ts="1760000100.100001",
    )

    structure = project_slack_source_structure((original, edit))

    original_revision, edit_revision = structure.revisions
    assert original_revision.revision_number == 1
    assert edit_revision.revision_number == 2
    assert edit_revision.supersedes_revision_id == original.event_revision_id
    assert structure.fate_for(original.event_revision_id) is (
        SlackSourceRevisionFate.SUPERSEDED
    )
    assert structure.fate_for(edit.event_revision_id) is (
        SlackSourceRevisionFate.CURRENT
    )
    assert tuple(edge.edge_id for edge in structure.topology_edges) == (
        "slack-edit:1760000100.100001->1760000100.200001",
    )
    assert structure.topology_edges[0].kind is (
        ConversationTopologyKind.EDIT_OF
    )
