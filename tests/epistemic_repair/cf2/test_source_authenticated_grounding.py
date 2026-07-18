from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from services.evaluation.epistemic_repair.cf2_source_grounding import (
    SourceAuthenticatedSignal,
    build_source_authenticated_grounding_episode,
)


def test_explicit_authenticated_source_resolves_through_production_contract() -> None:
    tenant_id, observation_id = uuid4(), uuid4()
    episode = build_source_authenticated_grounding_episode(
        SourceAuthenticatedSignal(
            tenant_id=tenant_id,
            observation_id=observation_id,
            occurred_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
            source_channel="slack:message",
            source_container_id="slack:release-room",
            content_text=(
                "Atlas release, update 4: The certificate owner is still open."
            ),
        )
    )

    assert episode is not None
    assert episode.current_fate == "resolved_for_consumer"
    assert episode.admitted_canonical_ref == {
        "type": "workstream", "id": "workstream:atlas-release", "version": 1,
    }
    assert episode.mention_detection_command.detection.candidate_surface == (
        "Atlas release"
    )
    assert episode.admission.genuine_source_binding is not None
    assert episode.admission.genuine_source_binding.source_native_identifier == (
        "slack:release-room:Atlas release"
    )
    assert episode.model_output["gold_blind"] is True
    assert episode.model_output["source_authenticated"] is True
    assert episode.model_output["decision_source"] == "deterministic_source_fixture"


def test_fixture_abstains_without_explicit_supported_source_subject() -> None:
    base = dict(
        tenant_id=uuid4(), observation_id=uuid4(),
        occurred_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        source_channel="slack:message", source_container_id="slack:release-room",
    )
    assert build_source_authenticated_grounding_episode(
        SourceAuthenticatedSignal(**base, content_text="It moved again.")
    ) is None
    assert build_source_authenticated_grounding_episode(
        SourceAuthenticatedSignal(
            **{**base, "source_container_id": ""},
            content_text="Atlas release is ready.",
        )
    ) is None


def test_authenticated_named_thread_subject_resolves_without_fuzzy_pronouns() -> None:
    text = "In the Harbor release thread, it is still waiting on the owner handoff."
    episode = build_source_authenticated_grounding_episode(
        SourceAuthenticatedSignal(
            tenant_id=uuid4(), observation_id=uuid4(),
            occurred_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
            source_channel="slack:message",
            source_container_id="slack:harbor-release",
            content_text=text,
        )
    )

    assert episode is not None
    assert episode.current_fate == "resolved_for_consumer"
    assert episode.mention_detection_command.detection.candidate_surface == (
        "Harbor release"
    )
    coordinate = (
        episode.mention_detection_command.detection.mention.primary_anchor.coordinate
    )
    start = coordinate.span_start
    end = coordinate.span_end
    assert text[start:end] == "Harbor release"
