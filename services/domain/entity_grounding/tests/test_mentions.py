from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from lib.contracts.entity_mentions import (
    CommitEntityMentionDetectionCommand,
    EntityMentionDetectionFate,
)
from services.domain.entity_grounding.episode import prepare_context_selection
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection


NOW = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)


def _context(*, phrase: str):
    return prepare_context_selection(
        tenant_id=uuid4(),
        observation_id=uuid4(),
        phrase=phrase,
        occurred_at=NOW,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=(
            {"kind": "source_topology"},
            {"kind": "same_source_space_temporal"},
        ),
        context_observations=(),
        selection_dependency_refs=(),
        now=NOW + timedelta(minutes=1),
    )


def test_exact_explicit_mentions_retain_every_source_coordinate() -> None:
    tenant_id = uuid4()
    observation_id = uuid4()
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="NBI",
        occurred_at=NOW,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=({"kind": "source_topology"},),
        context_observations=(),
        selection_dependency_refs=(),
        now=NOW + timedelta(minutes=1),
    )

    command = prepare_entity_mention_detection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="NBI",
        content_text="NBI blocked the renewal; nbi needs audit proof",
        source_channel="slack:message",
        context_command=context_command,
        context_outcome=context_outcome,
        now=NOW + timedelta(minutes=1),
    )

    detection = command.detection
    assert detection.fate is EntityMentionDetectionFate.DETECTED
    assert detection.mention is not None
    anchors = (
        detection.mention.primary_anchor,
        *detection.mention.alternate_anchors,
    )
    assert [anchor.surface_form for anchor in anchors] == ["NBI", "nbi"]
    assert [
        (anchor.coordinate.span_start, anchor.coordinate.span_end)
        for anchor in anchors
    ] == [(0, 3), (25, 28)]
    assert detection.mention.context_snapshot_id == (
        context_outcome.snapshot.snapshot_id
    )
    assert command.context.processing_authority.fingerprint == (
        context_command.context.processing_authority.fingerprint
    )


def test_provisional_coordinate_is_persisted_in_candidate_plane() -> None:
    tenant_id, observation_id = uuid4(), uuid4()
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id, observation_id=observation_id,
        phrase="Cobalt renewal", occurred_at=NOW,
        source_channel="email:message", source_space="email:message",
        topology_incomplete=False, boundary_hypotheses=(),
        context_observations=(), selection_dependency_refs=(),
        now=NOW + timedelta(minutes=1),
    )
    command = prepare_entity_mention_detection(
        tenant_id=tenant_id, observation_id=observation_id,
        phrase="Cobalt renewal",
        content_text="Cobalt renewal, update 1: pending.",
        source_channel="email:message", context_command=context_command,
        context_outcome=context_outcome, now=NOW + timedelta(minutes=1),
        verified_span=(0, 14), discovered_entity_type="commitment",
        provisional_canonical_ref="commitment:cobalt-renewal",
        normalization_version=1,
    )

    mention = command.detection.mention
    assert mention is not None
    assert mention.primary_anchor.coordinate.span_start == 0
    assert mention.primary_anchor.coordinate.span_end == 14
    assert mention.provisional_entity_type == "commitment"
    assert mention.provisional_canonical_ref == "commitment:cobalt-renewal"
    assert mention.canonical_ref_status == "provisional"
    assert mention.normalization_version == 1
    assert mention.grounding_fate == "extracted_unresolved"


def test_learned_type_is_bound_to_exact_detected_mention() -> None:
    tenant_id = uuid4()
    observation_id = uuid4()
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="Atlas Migration",
        occurred_at=NOW,
        source_channel="jira:issue",
        source_space="jira:ENG",
        topology_incomplete=False,
        boundary_hypotheses=(),
        context_observations=(),
        selection_dependency_refs=(),
        now=NOW + timedelta(minutes=1),
    )

    command = prepare_entity_mention_detection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="Atlas Migration",
        content_text="Atlas Migration is blocked.",
        source_channel="jira:issue",
        context_command=context_command,
        context_outcome=context_outcome,
        now=NOW + timedelta(minutes=1),
        verified_span=(0, 15),
        discovery_fate=EntityMentionDetectionFate.DETECTED,
        discovery_confidence=0.9,
        discovered_entity_type="project",
        extractor_version="learned-test-v1",
    )

    assessment = command.detection.entity_type_assessment
    assert assessment is not None
    assert assessment.mention_or_referent_ref == (
        f"mention:{command.detection.detection_id}:v1"
    )
    assert assessment.type_distribution["project"] == 0.9
    assert round(assessment.type_distribution["unknown"], 6) == 0.1
    assert command.detection.mention is not None
    assert command.detection.mention.extractor_version == "learned-test-v1"


def test_detection_and_type_confidence_persist_independently() -> None:
    tenant_id = uuid4()
    observation_id = uuid4()
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="RUNE-310",
        occurred_at=NOW,
        source_channel="jira:issue",
        source_space="jira:ENG",
        topology_incomplete=False,
        boundary_hypotheses=(),
        context_observations=(),
        selection_dependency_refs=(),
        now=NOW + timedelta(minutes=1),
    )

    command = prepare_entity_mention_detection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="RUNE-310",
        content_text="RUNE-310 blocked delivery.",
        source_channel="jira:issue",
        context_command=context_command,
        context_outcome=context_outcome,
        now=NOW + timedelta(minutes=1),
        verified_span=(0, 8),
        discovery_fate=EntityMentionDetectionFate.DETECTED,
        discovery_confidence=0.92,
        discovery_type_confidence=0.79,
        discovery_reason_codes=(
            "learned_type_hypothesis:goal",
            "learned_type_confidence_capped_ambiguous_identifier",
        ),
        discovered_entity_type="goal",
        extractor_version="learned-test-v2",
    )

    detection = command.detection
    assert detection.mention is not None
    assert detection.mention.detection_confidence == 0.92
    assert detection.entity_type_assessment is not None
    assert detection.entity_type_assessment.type_distribution["goal"] == 0.79
    assert detection.entity_type_assessment.type_distribution["unknown"] == (
        pytest.approx(0.21)
    )
    assert detection.entity_type_assessment.type_distribution["goal"] < 0.80
    assert "learned-type-hypothesis:goal" in (
        detection.entity_type_assessment.evidence_basis_refs
    )


def test_candidate_surface_absent_from_source_has_a_rejected_fate() -> None:
    tenant_id = uuid4()
    observation_id = uuid4()
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="the customer",
        occurred_at=NOW,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=(),
        context_observations=(),
        selection_dependency_refs=(),
        now=NOW + timedelta(minutes=1),
    )

    command = prepare_entity_mention_detection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="the customer",
        content_text="nothing in this signal names one",
        source_channel="slack:message",
        context_command=context_command,
        context_outcome=context_outcome,
        now=NOW + timedelta(minutes=1),
    )

    assert command.detection.fate is EntityMentionDetectionFate.REJECTED_NOT_ANCHORED
    assert command.detection.mention is None
    assert command.detection.reason_codes == (
        "candidate_surface_absent_from_focal_source",
    )


def test_explicit_match_does_not_accept_an_alphanumeric_substring() -> None:
    tenant_id = uuid4()
    observation_id = uuid4()
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="BI",
        occurred_at=NOW,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=(),
        context_observations=(),
        selection_dependency_refs=(),
        now=NOW + timedelta(minutes=1),
    )

    command = prepare_entity_mention_detection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="BI",
        content_text="NBI is not the surface BI was meant to match",
        source_channel="slack:message",
        context_command=context_command,
        context_outcome=context_outcome,
        now=NOW + timedelta(minutes=1),
    )

    assert command.detection.mention is not None
    assert command.detection.mention.primary_anchor.coordinate.span_start == 23


def test_command_rejects_a_source_revision_outside_authority() -> None:
    context_command, context_outcome = _context(phrase="NBI")
    command = prepare_entity_mention_detection(
        tenant_id=context_command.context.tenant_id,
        observation_id=context_command.focal_observation_id,  # type: ignore[arg-type]
        phrase="NBI",
        content_text="NBI is blocked",
        source_channel="slack:message",
        context_command=context_command,
        context_outcome=context_outcome,
        now=NOW + timedelta(minutes=1),
    )
    payload = command.model_dump(mode="python")
    payload["detection"]["source_revision_id"] = "observation:forbidden:v1"

    try:
        CommitEntityMentionDetectionCommand.model_validate(payload)
    except ValueError as exc:
        assert "source revision" in str(exc)
    else:
        raise AssertionError("forbidden source revision was accepted")
