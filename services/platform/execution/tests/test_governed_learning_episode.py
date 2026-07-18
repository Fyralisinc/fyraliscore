from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from services.platform.execution.governed_learning_episode import (
    build_governed_learning_episodes,
)


def test_episode_identity_is_canonical_ref_first_and_transport_invariant() -> None:
    tenant_id = uuid4()
    first_id, second_id = uuid4(), uuid4()
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    rows = [
        {"id": first_id, "occurred_at": start, "source_channel": "slack:message",
         "content_text": "Atlas ownership is open."},
        {"id": second_id, "occurred_at": start + timedelta(hours=2),
         "source_channel": "jira:issue", "content_text": "Atlas rollout moved."},
    ]
    mentions = {
        first_id: [{"surface": "Atlas release", "canonical_ref":
                    "workstream:atlas-release", "authority": "resolved_for_consumer",
                    "detection_id": str(uuid4())}],
        second_id: [{"surface": "Atlas release", "canonical_ref":
                     "workstream:atlas-release", "authority": "provisional_detection",
                     "detection_id": str(uuid4())}],
    }

    together = build_governed_learning_episodes(
        tenant_id=tenant_id, observations=rows, governed_mentions=mentions,
    )
    reversed_rows = build_governed_learning_episodes(
        tenant_id=tenant_id, observations=reversed(rows), governed_mentions=mentions,
    )
    first_transport = build_governed_learning_episodes(
        tenant_id=tenant_id, observations=rows[:1], governed_mentions=mentions,
    )

    assert len(together) == 1
    assert together[0].episode_id == reversed_rows[0].episode_id
    assert together[0].episode_id == first_transport[0].episode_id
    assert together[0].canonical_ref == "workstream:atlas-release"
    assert together[0].temporal_start == start
    assert together[0].temporal_end == start + timedelta(hours=2)
    assert [item.observation_id for item in together[0].assertions] == [
        first_id, second_id,
    ]
    assert [item.coordinate_authority for item in together[0].assertions] == [
        "resolved", "provisional",
    ]
    assert together[0].uncertainty == ("provisional_entity_coordinate",)


def test_missing_coordinate_remains_explicit_unresolved_episode() -> None:
    tenant_id, observation_id = uuid4(), uuid4()
    occurred_at = datetime(2026, 7, 1, tzinfo=timezone.utc)
    episodes = build_governed_learning_episodes(
        tenant_id=tenant_id,
        observations=[{
            "id": observation_id, "occurred_at": occurred_at,
            "source_channel": "slack:message", "content_text": "It moved again.",
        }],
        governed_mentions={},
    )

    assert len(episodes) == 1
    episode = episodes[0]
    assert episode.canonical_ref is None
    assert episode.uncertainty == ("missing_governed_entity_coordinate",)
    assertion = episode.assertions[0]
    assert assertion.coordinate_authority == "unresolved"
    assert assertion.evidence_address == f"observation:{observation_id}:content_text"
    assert assertion.governed_surface is None
    assert assertion.detection_id is None
