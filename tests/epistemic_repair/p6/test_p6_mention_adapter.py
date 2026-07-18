from __future__ import annotations

from uuid import uuid4

from services.domain.entity_grounding.learned_discovery import PersistedSignalText
from services.evaluation.epistemic_repair.p6_think_runner import (
    _p6_simulation_mention_adapter,
)


def test_p6_adapter_extracts_multiline_envelopes_and_ignores_distractors() -> None:
    atlas_id, cobalt_id, facilities_id, ticket_id = (uuid4() for _ in range(4))
    atlas = "Earlier Slack context\nAtlas release, update 1: blocked."
    candidates = _p6_simulation_mention_adapter((
        PersistedSignalText(atlas_id, "slack:message", atlas),
        PersistedSignalText(cobalt_id, "email:message", "Cobalt renewal, update 1: pending."),
        PersistedSignalText(facilities_id, "slack:message", "Facilities inspection passed."),
        PersistedSignalText(ticket_id, "jira:issue", "Beacon office ticket was closed."),
    ))
    by_surface = {item.surface: item for item in candidates}

    assert set(by_surface) == {"Atlas release", "Cobalt renewal"}
    assert by_surface["Atlas release"].span_start == atlas.index("Atlas release")
    assert by_surface["Atlas release"].entity_type == "workstream"
    assert by_surface["Cobalt renewal"].entity_type == "commitment"
    assert by_surface["Cobalt renewal"].provisional_canonical_ref == (
        "commitment:cobalt-renewal"
    )


def test_p6_adapter_keeps_same_surface_signal_local() -> None:
    left_id, right_id = uuid4(), uuid4()
    candidates = _p6_simulation_mention_adapter((
        PersistedSignalText(left_id, "slack:message", "Atlas release, update 1: blocked."),
        PersistedSignalText(right_id, "email:message", "Atlas release, update 2: clear."),
    ))

    assert {(item.signal_id, item.surface) for item in candidates} == {
        (left_id, "Atlas release"), (right_id, "Atlas release"),
    }


def test_p6_adapter_extracts_distinct_local_work_items_without_storyline_collision() -> None:
    signal_id = uuid4()
    text = "Week 1: Cobalt paint approval is listed in the Beacon office ticket."
    candidates = _p6_simulation_mention_adapter((
        PersistedSignalText(signal_id, "jira:comment", text),
    ))
    by_surface = {item.surface: item for item in candidates}

    assert set(by_surface) == {"Cobalt paint approval", "Beacon office ticket"}
    for surface, candidate in by_surface.items():
        assert text[candidate.span_start:candidate.span_end] == surface
        assert candidate.entity_type == "work_item"
        assert candidate.provisional_canonical_ref == (
            "work_item:" + surface.casefold().replace(" ", "-")
        )
    assert by_surface["Cobalt paint approval"].provisional_canonical_ref != (
        by_surface["Beacon office ticket"].provisional_canonical_ref
    )
