from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from lib.contracts.entity_mentions import EntityMentionDetectionFate
from lib.evaluation.epistemic_repair.core_fast_path_population import (
    build_core_fast_path_population,
)
from services.domain.entity_grounding.episode import prepare_context_selection
from services.domain.entity_grounding.learned_discovery import DISCOVERY_VERSION
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection
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
    assert episode.mention_detection_command.detection.extractor_version == (
        DISCOVERY_VERSION
    )


def test_authenticated_detection_matches_later_learned_batch_identity() -> None:
    tenant_id, observation_id = uuid4(), uuid4()
    occurred_at = datetime(2026, 7, 1, tzinfo=timezone.utc)
    text = "Atlas release, update 4: The certificate owner is still open."
    episode = build_source_authenticated_grounding_episode(
        SourceAuthenticatedSignal(
            tenant_id=tenant_id,
            observation_id=observation_id,
            occurred_at=occurred_at,
            source_channel="slack:message",
            source_container_id="slack:release-room",
            content_text=text,
        )
    )
    assert episode is not None

    learned_at = occurred_at.replace(minute=occurred_at.minute + 1)
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="Atlas release",
        occurred_at=occurred_at,
        source_channel="slack:message",
        source_space="slack:message",
        topology_incomplete=True,
        boundary_hypotheses=(
            {"kind": "slack_batch_boundary", "status": "provisional"},
        ),
        context_observations=(),
        selection_dependency_refs=(),
        now=learned_at,
        focal_content_text=text,
    )
    learned_command = prepare_entity_mention_detection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="Atlas release",
        content_text=text,
        source_channel="slack:message",
        context_command=context_command,
        context_outcome=context_outcome,
        now=learned_at,
        verified_span=(0, len("Atlas release")),
        discovery_fate=EntityMentionDetectionFate.DETECTED,
        discovery_confidence=0.99,
        extractor_version=DISCOVERY_VERSION,
        discovered_entity_type="project",
    )

    assert episode.mention_detection_command.detection_key == (
        learned_command.detection_key
    )


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


def test_complete_batch_grounds_all_named_storylines_and_abstains_elsewhere() -> None:
    batch = build_core_fast_path_population().batches[0]
    expected_refs = {
        "harbor": "workstream:harbor-release",
        "northstar": "workstream:northstar-pilot",
        "access": "workstream:access-review",
        "delta": "workstream:delta-handoff",
    }
    resolved: dict[str, str] = {}
    abstained: list[str] = []
    for signal in batch.signals:
        episode = build_source_authenticated_grounding_episode(
            SourceAuthenticatedSignal(
                tenant_id=uuid4(),
                observation_id=uuid4(),
                occurred_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
                source_channel=signal.source_channel,
                source_container_id=signal.source_space,
                content_text=signal.text,
            )
        )
        storyline = next(
            (
                name for name in expected_refs
                if signal.signal_id.startswith(f"cf2-{name}-")
            ),
            None,
        )
        if storyline is None:
            assert episode is None
            abstained.append(signal.signal_id)
            continue
        assert episode is not None
        assert episode.current_fate == "resolved_for_consumer"
        assert episode.admitted_canonical_ref == {
            "type": "workstream",
            "id": expected_refs[storyline],
            "version": 1,
        }
        resolved[signal.signal_id] = episode.admitted_canonical_ref["id"]

    assert len(resolved) == 20
    assert len(abstained) == 5
    assert all(
        signal_id.startswith(("cf2-noise-", "cf2-distractor-"))
        for signal_id in abstained
    )
