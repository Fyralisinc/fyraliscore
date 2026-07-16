from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

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
