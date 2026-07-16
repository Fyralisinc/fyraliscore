"""Sole transactional writer for durable InterpretationContextSnapshot state."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.contracts.conversation_context import CommitInterpretationContextCommand
from lib.contracts.entity_mentions import (
    CommitEntityMentionDetectionCommand,
    EntityMentionDetectionFate,
)
from lib.contracts.kernel import canonical_sha256
from lib.contracts.perception import MentionAnchorKind
from lib.conversation_context_selection import select_context
from lib.entity_mention_detection import locate_explicit_surface_spans
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.agency_protocol import (
    AgencyCommitResult,
    AgencyProtocolIds,
    ensure_live_context,
    insert_protocol_event_and_outbox,
    insert_protocol_result,
    prior_protocol_result,
)


def _dump(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value)


class GroundingAnnotationAppender:
    """Append context selections; never mutate source evidence or identity truth."""

    writer_id = "GroundingAnnotationAppender"

    async def apply_mention_detection(
        self,
        *,
        conn: asyncpg.Connection,
        command: CommitEntityMentionDetectionCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        """Commit one exact detected/rejected mention fate."""

        command = CommitEntityMentionDetectionCommand.model_validate(
            command.model_dump(mode="json")
        )
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await prior_protocol_result(
            conn=conn,
            tenant_id=command.context.tenant_id,
            writer_id=self.writer_id,
            idempotency_key=command.context.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior is not None:
            return prior

        detection = command.detection
        snapshot_row = await conn.fetchrow(
            """
            SELECT snapshot_content_hash
            FROM interpretation_context_snapshots
            WHERE tenant_id=$1 AND id=$2
            FOR KEY SHARE
            """,
            command.context.tenant_id,
            detection.context_snapshot_id,
        )
        if (
            snapshot_row is None
            or snapshot_row["snapshot_content_hash"]
            != detection.context_snapshot_digest
        ):
            raise InvariantViolation(
                "ENTITY_MENTION_CONTEXT_MISMATCH",
                "mention detection does not bind the committed context snapshot",
                context_snapshot_id=str(detection.context_snapshot_id),
            )
        observation_row = await conn.fetchrow(
            """
            SELECT content_text
            FROM observations
            WHERE tenant_id=$1 AND id=$2
            FOR KEY SHARE
            """,
            command.context.tenant_id,
            detection.source_observation_id,
        )
        if observation_row is None:
            raise InvariantViolation(
                "ENTITY_MENTION_SOURCE_MISSING",
                "mention detection source observation does not exist",
                source_observation_id=str(detection.source_observation_id),
            )
        content_text = str(observation_row["content_text"] or "")
        if canonical_sha256(content_text) != detection.source_content_hash:
            raise InvariantViolation(
                "ENTITY_MENTION_SOURCE_HASH_MISMATCH",
                "mention detection source content changed before commit",
                source_observation_id=str(detection.source_observation_id),
            )
        expected_spans = locate_explicit_surface_spans(
            content_text,
            detection.candidate_surface,
        )
        if detection.mention is not None:
            anchors = (
                detection.mention.primary_anchor,
                *detection.mention.alternate_anchors,
            )
            explicit_anchors = tuple(
                anchor
                for anchor in anchors
                if anchor.kind is MentionAnchorKind.EXPLICIT
            )
            observed_spans = tuple(
                (anchor.coordinate.span_start, anchor.coordinate.span_end)
                for anchor in explicit_anchors
            )
            if (
                len(explicit_anchors) == len(anchors)
                and observed_spans != expected_spans
            ) or any(
                anchor.surface_form
                != content_text[
                    anchor.coordinate.span_start : anchor.coordinate.span_end
                ]
                for anchor in explicit_anchors
            ):
                raise InvariantViolation(
                    "ENTITY_MENTION_ANCHOR_MISMATCH",
                    "mention anchors do not exactly reconstruct from source text",
                    expected_spans=expected_spans,
                    observed_spans=observed_spans,
                )
        elif (
            detection.fate is EntityMentionDetectionFate.REJECTED_NOT_ANCHORED
            and expected_spans
        ):
            raise InvariantViolation(
                "ENTITY_MENTION_FALSE_REJECTION",
                "not-anchored fate conflicts with exact source occurrences",
                expected_spans=expected_spans,
            )

        head = await conn.fetchrow(
            """
            SELECT * FROM entity_mention_detection_heads
            WHERE tenant_id=$1 AND detection_key=$2
            FOR UPDATE
            """,
            command.context.tenant_id,
            command.detection_key,
        )
        current_version = int(head["current_detection_version"]) if head else 0
        current_detection_id = head["current_detection_id"] if head else None
        if (
            current_version != command.expected.expected_detection_version
            or current_detection_id != command.expected.expected_detection_id
        ):
            raise InvariantViolation(
                "ENTITY_MENTION_CAS",
                "mention-detection head does not match command expectation",
                detection_key=command.detection_key,
                expected_version=command.expected.expected_detection_version,
                current_version=current_version,
            )
        if head is not None:
            self._validate_mention_head_identity(head=head, command=command)
        next_version = current_version + 1
        if detection.detection_version != next_version:
            raise InvariantViolation(
                "ENTITY_MENTION_VERSION",
                "mention detection version does not follow the current head",
                expected_version=next_version,
                detection_version=detection.detection_version,
            )

        ids = AgencyProtocolIds.new()
        result = {
            "detection_key": command.detection_key,
            "detection_id": str(detection.detection_id),
            "detection_digest": detection.detection_digest,
            "detection_version": next_version,
            "fate": detection.fate.value,
            "mention_id": (
                detection.mention.mention_id if detection.mention is not None else None
            ),
            "context_snapshot_id": str(detection.context_snapshot_id),
            "context_snapshot_digest": detection.context_snapshot_digest,
            "source_content_hash": detection.source_content_hash,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id=self.writer_id,
            command_kind="commit_entity_mention_detection",
            command=command,
            request_digest=command.request_digest,
            object_type="entity_mention_detection",
            object_id=detection.detection_id,
            object_version=next_version,
            result=result,
        )
        await conn.execute(
            """
            INSERT INTO entity_mention_detections (
              id, tenant_id, detection_key, detection_version,
              source_observation_id, source_revision_id, candidate_surface,
              context_snapshot_id, context_snapshot_digest, source_content_hash,
              fate, mention_id, mention, reason_codes, extractor_version,
              detection_digest, supersedes_detection_id, command_result_id,
              detected_at, recorded_at
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
              $12, $13::jsonb, $14, $15, $16, $17, $18, $19, $20
            )
            """,
            detection.detection_id,
            command.context.tenant_id,
            command.detection_key,
            next_version,
            detection.source_observation_id,
            detection.source_revision_id,
            detection.candidate_surface,
            detection.context_snapshot_id,
            detection.context_snapshot_digest,
            detection.source_content_hash,
            detection.fate.value,
            UUID(detection.mention.mention_id) if detection.mention else None,
            _dump(detection.mention) if detection.mention else None,
            list(detection.reason_codes),
            detection.extractor_version,
            detection.detection_digest,
            current_detection_id,
            ids.command_result_id,
            detection.detected_at,
            now,
        )
        if head is None:
            await conn.execute(
                """
                INSERT INTO entity_mention_detection_heads (
                  tenant_id, detection_key, source_observation_id,
                  source_revision_id, candidate_surface, extractor_version,
                  current_detection_version, current_detection_id,
                  current_detection_digest, current_command_result_id,
                  created_at, updated_at
                ) VALUES (
                  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11
                )
                """,
                command.context.tenant_id,
                command.detection_key,
                detection.source_observation_id,
                detection.source_revision_id,
                detection.candidate_surface,
                detection.extractor_version,
                next_version,
                detection.detection_id,
                detection.detection_digest,
                ids.command_result_id,
                now,
            )
        else:
            updated = await conn.execute(
                """
                UPDATE entity_mention_detection_heads
                SET current_detection_version=$3, current_detection_id=$4,
                    current_detection_digest=$5, current_command_result_id=$6,
                    updated_at=$7
                WHERE tenant_id=$1 AND detection_key=$2
                  AND current_detection_version=$8
                  AND current_detection_id=$9
                """,
                command.context.tenant_id,
                command.detection_key,
                next_version,
                detection.detection_id,
                detection.detection_digest,
                ids.command_result_id,
                now,
                current_version,
                current_detection_id,
            )
            if updated != "UPDATE 1":
                raise InvariantViolation(
                    "ENTITY_MENTION_CAS",
                    "mention-detection head changed during commit",
                    detection_key=command.detection_key,
                )

        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id=self.writer_id,
            object_type="entity_mention_detection",
            object_id=detection.detection_id,
            object_version=next_version,
            semantic_transition=(
                "entity_mention_detection_recorded"
                if head is None
                else "entity_mention_detection_superseded"
            ),
            event_payload=result,
            intervention_spec_digest=None,
            destination_operation="grounding.entity_mention.detected",
        )

    async def apply_context(
        self,
        *,
        conn: asyncpg.Connection,
        command: CommitInterpretationContextCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        command = CommitInterpretationContextCommand.model_validate(
            command.model_dump(mode="json")
        )
        now = now or datetime.now(timezone.utc)
        ensure_live_context(command.context, now=now)
        prior = await prior_protocol_result(
            conn=conn,
            tenant_id=command.context.tenant_id,
            writer_id=self.writer_id,
            idempotency_key=command.context.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior is not None:
            return prior

        head = await conn.fetchrow(
            """
            SELECT * FROM interpretation_context_heads
            WHERE tenant_id=$1 AND selection_key=$2
            FOR UPDATE
            """,
            command.context.tenant_id,
            command.selection_key,
        )
        current_version = int(head["current_aggregate_version"]) if head else 0
        current_snapshot_id = head["current_snapshot_id"] if head else None
        if (
            current_version != command.expected.expected_aggregate_version
            or current_snapshot_id != command.expected.expected_snapshot_id
        ):
            raise InvariantViolation(
                "INTERPRETATION_CONTEXT_CAS",
                "interpretation-context head does not match command expectation",
                selection_key=command.selection_key,
                expected_version=command.expected.expected_aggregate_version,
                current_version=current_version,
                expected_snapshot_id=(
                    str(command.expected.expected_snapshot_id)
                    if command.expected.expected_snapshot_id
                    else None
                ),
                current_snapshot_id=(
                    str(current_snapshot_id) if current_snapshot_id else None
                ),
            )
        if head is not None:
            self._validate_head_identity(head=head, command=command)

        next_version = current_version + 1
        snapshot_id = command.proposed_snapshot_id
        dependency_id = command.proposed_dependency_id
        outcome = select_context(
            command,
            aggregate_version=next_version,
            snapshot_id=snapshot_id,
            dependency_id=dependency_id,
            frozen_at=now,
        )
        ids = AgencyProtocolIds.new()
        candidate_hashes = tuple(
            sorted(candidate.candidate_content_hash for candidate in command.candidates)
        )
        probe_manifest = tuple(
            probe.model_dump(mode="json")
            for probe in sorted(command.probes, key=lambda item: str(item.candidate_id))
        )
        candidate_manifest_digest = canonical_sha256(candidate_hashes)
        probe_manifest_digest = canonical_sha256(probe_manifest)
        result = {
            "selection_key": command.selection_key,
            "snapshot_id": str(snapshot_id),
            "snapshot_digest": outcome.snapshot.snapshot_content_hash,
            "dependency_id": str(dependency_id),
            "decision_digest": outcome.decision_digest,
            "disposition": outcome.disposition.value,
            "selected_candidate_ids": [
                str(candidate_id) for candidate_id in outcome.selected_candidate_ids
            ],
            "candidate_manifest_digest": candidate_manifest_digest,
            "probe_manifest_digest": probe_manifest_digest,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id=self.writer_id,
            command_kind="commit_interpretation_context",
            command=command,
            request_digest=command.request_digest,
            object_type="interpretation_context_snapshot",
            object_id=snapshot_id,
            object_version=next_version,
            result=result,
        )

        focal_ids = set(command.request.focal_event_revision_ids)
        focal_item = next(
            item
            for item in outcome.snapshot.selected_items
            if item.event_revision_id in focal_ids
        )
        await conn.execute(
            """
            INSERT INTO interpretation_context_snapshots (
              id, tenant_id, focal_observation_id, phrase, snapshot_version,
              source_channel, source_space, evidence_cutoff,
              processing_authority_fingerprint, snapshot_content_hash, snapshot,
              selection_key, aggregate_version, focal_event_revision_ids,
              interpretation_mode, selection_dependency,
              candidate_manifest_digest, probe_manifest_digest,
              selection_decision_digest, supersedes_snapshot_id,
              command_result_id, contract_version
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb,
              $12, $13, $14, $15, $16::jsonb, $17, $18, $19, $20,
              $21, 'conversation-context-selection-v1'
            )
            """,
            snapshot_id,
            command.context.tenant_id,
            command.focal_observation_id,
            command.selection_subject,
            next_version,
            focal_item.authority_label,
            command.context.writer_scope_epoch.source_partition,
            command.request.evidence_cutoff,
            command.request.processing_authority.fingerprint,
            outcome.snapshot.snapshot_content_hash,
            _dump(outcome.snapshot),
            command.selection_key,
            next_version,
            list(command.request.focal_event_revision_ids),
            command.request.mode.value,
            _dump(outcome.dependency),
            candidate_manifest_digest,
            probe_manifest_digest,
            outcome.decision_digest,
            current_snapshot_id,
            ids.command_result_id,
        )
        if head is None:
            await conn.execute(
                """
                INSERT INTO interpretation_context_heads (
                  tenant_id, selection_key, selection_subject,
                  focal_event_revision_ids, purpose, operation,
                  interpretation_mode, source_partition,
                  current_aggregate_version, current_snapshot_id,
                  current_snapshot_digest, current_decision_digest,
                  current_command_result_id, created_at, updated_at
                ) VALUES (
                  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                  $13, $14, $14
                )
                """,
                command.context.tenant_id,
                command.selection_key,
                command.selection_subject,
                list(command.request.focal_event_revision_ids),
                command.request.processing_authority.purpose,
                command.request.processing_authority.operation,
                command.request.mode.value,
                command.context.writer_scope_epoch.source_partition,
                next_version,
                snapshot_id,
                outcome.snapshot.snapshot_content_hash,
                outcome.decision_digest,
                ids.command_result_id,
                now,
            )
        else:
            updated = await conn.execute(
                """
                UPDATE interpretation_context_heads
                SET current_aggregate_version=$3, current_snapshot_id=$4,
                    current_snapshot_digest=$5, current_decision_digest=$6,
                    current_command_result_id=$7, updated_at=$8
                WHERE tenant_id=$1 AND selection_key=$2
                  AND current_aggregate_version=$9
                  AND current_snapshot_id=$10
                """,
                command.context.tenant_id,
                command.selection_key,
                next_version,
                snapshot_id,
                outcome.snapshot.snapshot_content_hash,
                outcome.decision_digest,
                ids.command_result_id,
                now,
                current_version,
                current_snapshot_id,
            )
            if updated != "UPDATE 1":
                raise InvariantViolation(
                    "INTERPRETATION_CONTEXT_CAS",
                    "interpretation-context head changed during commit",
                    selection_key=command.selection_key,
                )

        selected_ids = set(outcome.selected_candidate_ids)
        eligible_ids = set(outcome.eligible_candidate_ids)
        probes = {probe.candidate_id: probe for probe in command.probes}
        for candidate in command.candidates:
            await conn.execute(
                """
                INSERT INTO conversation_context_candidate_records (
                  id, tenant_id, selection_key, aggregate_version, snapshot_id,
                  candidate_id, candidate_content_hash, selected, eligible,
                  layer_coverage, cost, candidate, probe, command_result_id,
                  recorded_at
                ) VALUES (
                  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                  $11::jsonb, $12::jsonb, $13::jsonb, $14, $15
                )
                """,
                uuid7(),
                command.context.tenant_id,
                command.selection_key,
                next_version,
                snapshot_id,
                candidate.candidate_id,
                candidate.candidate_content_hash,
                candidate.candidate_id in selected_ids,
                candidate.candidate_id in eligible_ids,
                [layer.value for layer in candidate.layer_coverage],
                _dump(candidate.cost),
                _dump(candidate),
                _dump(probes[candidate.candidate_id]),
                ids.command_result_id,
                now,
            )

        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=command.context,
            writer_id=self.writer_id,
            object_type="interpretation_context_snapshot",
            object_id=snapshot_id,
            object_version=next_version,
            semantic_transition=(
                "interpretation_context_selected"
                if head is None
                else "interpretation_context_superseded"
            ),
            event_payload=result,
            intervention_spec_digest=None,
            destination_operation="grounding.interpretation_context.selected",
        )

    @staticmethod
    def _validate_head_identity(
        *,
        head: asyncpg.Record,
        command: CommitInterpretationContextCommand,
    ) -> None:
        exact = (
            head["selection_subject"] == command.selection_subject
            and tuple(head["focal_event_revision_ids"])
            == command.request.focal_event_revision_ids
            and head["purpose"] == command.request.processing_authority.purpose
            and head["operation"] == command.request.processing_authority.operation
            and head["interpretation_mode"] == command.request.mode.value
            and head["source_partition"]
            == command.context.writer_scope_epoch.source_partition
        )
        if not exact:
            raise InvariantViolation(
                "INTERPRETATION_CONTEXT_IDENTITY",
                "selection key resolved to a different immutable context identity",
                selection_key=command.selection_key,
            )

    @staticmethod
    def _validate_mention_head_identity(
        *,
        head: asyncpg.Record,
        command: CommitEntityMentionDetectionCommand,
    ) -> None:
        detection = command.detection
        exact = (
            head["source_observation_id"] == detection.source_observation_id
            and head["source_revision_id"] == detection.source_revision_id
            and head["candidate_surface"] == detection.candidate_surface
            and head["extractor_version"] == detection.extractor_version
        )
        if not exact:
            raise InvariantViolation(
                "ENTITY_MENTION_IDENTITY",
                "detection key resolved to a different immutable mention identity",
                detection_key=command.detection_key,
            )


__all__ = ["GroundingAnnotationAppender"]
