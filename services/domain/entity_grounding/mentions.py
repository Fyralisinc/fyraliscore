"""Prepare exact explicit-mention detections from legacy phrase opportunities."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID

from lib.contracts.semantic_commands import SemanticWriteContext
from lib.contracts.entity_mentions import (
    CommitEntityMentionDetectionCommand,
    EntityMentionDetection,
    EntityMentionDetectionFate,
    EntityMentionHeadExpectation,
)
from lib.contracts.kernel import WriterCutoverState, WriterScopeEpoch, canonical_sha256
from lib.contracts.perception import (
    EntityMention,
    EvidenceCoordinate,
    MentionAnchor,
    MentionAnchorKind,
)
from lib.entity_mention_detection import locate_explicit_surface_spans
from lib.shared.ids import uuid7
from lib.contracts.conversation_context import (
    CommitInterpretationContextCommand,
    ContextSelectionOutcome,
)


_EXTRACTOR_VERSION = "legacy-phrase-opportunity-exact-anchor-v1"


def prepare_entity_mention_detection(
    *,
    tenant_id: UUID,
    observation_id: UUID,
    phrase: str,
    content_text: str,
    source_channel: str,
    context_command: CommitInterpretationContextCommand,
    context_outcome: ContextSelectionOutcome,
    now: datetime,
) -> CommitEntityMentionDetectionCommand:
    """Create one total-fate detection over the exact pre-model context."""

    source_revision_id = context_command.request.focal_event_revision_ids[0]
    spans = locate_explicit_surface_spans(content_text, phrase)
    detection_id = uuid7()
    mention: EntityMention | None = None
    if spans:
        anchors = tuple(
            MentionAnchor(
                anchor_id=f"anchor:{detection_id}:{index}",
                kind=MentionAnchorKind.EXPLICIT,
                coordinate=EvidenceCoordinate(
                    evidence_record_id=f"observation:{observation_id}",
                    source_system=source_channel.split(":", 1)[0],
                    source_object_id=f"observation:{observation_id}",
                    source_revision=source_revision_id,
                    field_path="content_text",
                    span_start=start,
                    span_end=end,
                ),
                surface_form=content_text[start:end],
            )
            for index, (start, end) in enumerate(spans)
        )
        mention = EntityMention(
            mention_id=str(detection_id),
            mention_version=1,
            primary_anchor=anchors[0],
            alternate_anchors=anchors[1:],
            context_snapshot_id=context_outcome.snapshot.snapshot_id,
            source_assertion_and_frame_refs=(
                f"observation:{observation_id}:source-text",
            ),
            detection_confidence=0.6,
            extractor_version=_EXTRACTOR_VERSION,
        )
        fate = EntityMentionDetectionFate.DETECTED
        reasons = ("candidate_surface_exactly_anchored_in_source",)
    else:
        fate = EntityMentionDetectionFate.REJECTED_NOT_ANCHORED
        reasons = ("candidate_surface_absent_from_focal_source",)

    detection = EntityMentionDetection(
        detection_id=detection_id,
        detection_version=1,
        tenant_id=tenant_id,
        source_observation_id=observation_id,
        source_revision_id=source_revision_id,
        candidate_surface=phrase,
        context_snapshot_id=UUID(context_outcome.snapshot.snapshot_id),
        context_snapshot_digest=context_outcome.snapshot.snapshot_content_hash,
        source_content_hash=canonical_sha256(content_text),
        fate=fate,
        mention=mention,
        reason_codes=reasons,
        extractor_version=_EXTRACTOR_VERSION,
        detected_at=now,
    )
    authority = context_command.context.processing_authority
    context = SemanticWriteContext(
        command_id=uuid7(),
        tenant_id=tenant_id,
        processing_authority=authority,
        writer_scope_epoch=WriterScopeEpoch(
            scope_id="legacy-grounding-annotation",
            tenant_id=tenant_id,
            semantic_responsibility="entity_mention_detection",
            source_partition=(
                context_command.context.writer_scope_epoch.source_partition
            ),
            writer_owner="GroundingAnnotationAppender",
            epoch=1,
            state=WriterCutoverState.LEGACY,
        ),
        idempotency_key=f"mention-detection:{detection_id}",
        issued_at=now - timedelta(microseconds=1),
        expires_at=now + timedelta(hours=1),
    )
    return CommitEntityMentionDetectionCommand(
        context=context,
        detection=detection,
        expected=EntityMentionHeadExpectation(expected_detection_version=0),
        invalidation_keys=(
            f"event-revision:{source_revision_id}",
            f"context-snapshot:{context_outcome.snapshot.snapshot_id}",
        ),
        prepared_at=now,
    )


__all__ = ["prepare_entity_mention_detection"]
