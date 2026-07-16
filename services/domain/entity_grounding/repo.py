"""Persistence adapter for immutable grounding episodes."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.contracts.conversation_context import (
    CommitInterpretationContextCommand,
    ContextSelectionOutcome,
)
from lib.contracts.entity_mentions import (
    CommitEntityMentionDetectionCommand,
    EntityMentionDetectionFate,
)
from lib.contracts.kernel import canonical_sha256
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.conversation_context.repo import GroundingAnnotationAppender
from services.domain.entity_grounding.episode import GroundingEpisode


class EntityGroundingRepo:
    """Atomically append every stage and one explicit downstream fate."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def record_retryable_fate(
        self,
        *,
        tenant_id: UUID,
        source_observation_id: UUID,
        phrase: str,
        failure_class: str,
        failure_reason: str,
        next_attempt_at: datetime,
        conn: asyncpg.Connection | None = None,
    ) -> None:
        """Persist a restart-safe nonterminal fate without inventing a result."""

        async def write(target: asyncpg.Connection) -> None:
            useful_safe_fate = {
                "fate_kind": "retry_scheduled",
                "terminal": False,
                "reason_class": failure_class,
                "reason": failure_reason,
                "next_attempt_at": next_attempt_at.isoformat(),
                "contract_version": "grounding-work-fate-v1",
            }
            await target.execute(
                """
                INSERT INTO entity_grounding_work_items (
                    id, tenant_id, source_observation_id, phrase,
                    processing_generation, status, processing_class,
                    attempt_count, next_attempt_at, last_failure_class,
                    last_failure_reason, useful_safe_fate
                ) VALUES (
                    $1, $2, $3, $4, 1, 'retry_scheduled', 'R2', 1,
                    $5, $6, $7, $8::jsonb
                )
                ON CONFLICT (
                    tenant_id, source_observation_id, phrase,
                    processing_generation
                ) DO UPDATE SET
                    status = 'retry_scheduled',
                    attempt_count = entity_grounding_work_items.attempt_count + 1,
                    next_attempt_at = EXCLUDED.next_attempt_at,
                    last_failure_class = EXCLUDED.last_failure_class,
                    last_failure_reason = EXCLUDED.last_failure_reason,
                    useful_safe_fate = EXCLUDED.useful_safe_fate,
                    updated_at = now()
                WHERE entity_grounding_work_items.status IN (
                    'pending', 'retry_scheduled'
                )
                """,
                uuid7(),
                tenant_id,
                source_observation_id,
                phrase,
                next_attempt_at,
                failure_class,
                failure_reason,
                json.dumps(useful_safe_fate),
            )

        if conn is not None:
            async with conn.transaction():
                await write(conn)
            return
        async with self._pool.acquire() as owned, owned.transaction():
            await write(owned)

    async def append_episode(
        self,
        *,
        episode: GroundingEpisode,
        tenant_id: UUID,
        source_observation_id: UUID,
        phrase: str,
        conn: asyncpg.Connection | None = None,
    ) -> UUID:
        if conn is not None:
            async with conn.transaction():
                return await self._append(
                    conn,
                    episode=episode,
                    tenant_id=tenant_id,
                    source_observation_id=source_observation_id,
                    phrase=phrase,
                )
        async with self._pool.acquire() as owned, owned.transaction():
            return await self._append(
                owned,
                episode=episode,
                tenant_id=tenant_id,
                source_observation_id=source_observation_id,
                phrase=phrase,
            )

    async def append_rejected_mention(
        self,
        *,
        context_command: CommitInterpretationContextCommand,
        context_outcome: ContextSelectionOutcome,
        mention_detection_command: CommitEntityMentionDetectionCommand,
        tenant_id: UUID,
        source_observation_id: UUID,
        phrase: str,
        conn: asyncpg.Connection | None = None,
    ) -> UUID:
        """Commit a rejected mention fate without candidate/model artifacts."""

        if conn is not None:
            async with conn.transaction():
                return await self._append_rejected_mention(
                    conn,
                    context_command=context_command,
                    context_outcome=context_outcome,
                    mention_detection_command=mention_detection_command,
                    tenant_id=tenant_id,
                    source_observation_id=source_observation_id,
                    phrase=phrase,
                )
        async with self._pool.acquire() as owned, owned.transaction():
            return await self._append_rejected_mention(
                owned,
                context_command=context_command,
                context_outcome=context_outcome,
                mention_detection_command=mention_detection_command,
                tenant_id=tenant_id,
                source_observation_id=source_observation_id,
                phrase=phrase,
            )

    async def _append_rejected_mention(
        self,
        conn: asyncpg.Connection,
        *,
        context_command: CommitInterpretationContextCommand,
        context_outcome: ContextSelectionOutcome,
        mention_detection_command: CommitEntityMentionDetectionCommand,
        tenant_id: UUID,
        source_observation_id: UUID,
        phrase: str,
    ) -> UUID:
        detection = mention_detection_command.detection
        if detection.fate is EntityMentionDetectionFate.DETECTED:
            raise InvariantViolation(
                "GROUNDING_REJECTED_MENTION_FATE",
                "detected mentions must continue through candidate processing",
            )
        if detection.mention is not None:
            raise InvariantViolation(
                "GROUNDING_REJECTED_MENTION_PRESENT",
                "rejected mention fate cannot carry an EntityMention",
            )
        await self._commit_context_and_detection(
            conn,
            context_command=context_command,
            context_outcome=context_outcome,
            mention_detection_command=mention_detection_command,
            tenant_id=tenant_id,
            source_observation_id=source_observation_id,
            phrase=phrase,
        )
        useful_safe_fate = {
            "fate_kind": "mention_rejected",
            "terminal": True,
            "mention_detection_id": str(detection.detection_id),
            "mention_detection_fate": detection.fate.value,
            "reason_codes": list(detection.reason_codes),
            "contract_version": "grounding-work-fate-v2",
        }
        await conn.execute(
            """
            INSERT INTO entity_grounding_work_items (
                id, tenant_id, source_observation_id, phrase,
                processing_generation, status, processing_class,
                attempt_count, current_trace_id, useful_safe_fate
            ) VALUES (
                $1, $2, $3, $4, 1, 'unresolved', 'R2', 1, NULL, $5::jsonb
            )
            ON CONFLICT (
                tenant_id, source_observation_id, phrase,
                processing_generation
            ) DO UPDATE SET
                status = 'unresolved',
                attempt_count = entity_grounding_work_items.attempt_count + 1,
                next_attempt_at = NULL,
                last_failure_class = NULL,
                last_failure_reason = NULL,
                current_trace_id = NULL,
                useful_safe_fate = EXCLUDED.useful_safe_fate,
                updated_at = now()
            WHERE entity_grounding_work_items.status IN (
                'pending', 'retry_scheduled'
            )
            """,
            uuid7(),
            tenant_id,
            source_observation_id,
            phrase,
            json.dumps(useful_safe_fate),
        )
        return detection.detection_id

    async def _append(
        self,
        conn: asyncpg.Connection,
        *,
        episode: GroundingEpisode,
        tenant_id: UUID,
        source_observation_id: UUID,
        phrase: str,
    ) -> UUID:
        snapshot = episode.context_snapshot
        candidate_set = episode.candidate_set
        request = candidate_set.request
        assessment = episode.assessment
        admission = episode.admission
        if snapshot != episode.context_selection_outcome.snapshot:
            raise InvariantViolation(
                "GROUNDING_CONTEXT_OUTCOME_MISMATCH",
                "grounding episode snapshot differs from its context-selection outcome",
            )
        if UUID(snapshot.snapshot_id) != episode.context_selection_command.proposed_snapshot_id:
            raise InvariantViolation(
                "GROUNDING_CONTEXT_COMMAND_MISMATCH",
                "grounding episode snapshot differs from its context-selection command",
            )
        candidate_set_payload = candidate_set.model_dump(mode="json")
        assessment_payload = assessment.model_dump(mode="json")
        admission_payload = admission.model_dump(mode="json")

        detection = episode.mention_detection_command.detection
        await self._commit_context_and_detection(
            conn,
            context_command=episode.context_selection_command,
            context_outcome=episode.context_selection_outcome,
            mention_detection_command=episode.mention_detection_command,
            tenant_id=tenant_id,
            source_observation_id=source_observation_id,
            phrase=phrase,
        )
        if detection.mention is None:
            raise InvariantViolation(
                "GROUNDING_MENTION_REQUIRED",
                "candidate processing requires a committed EntityMention",
            )
        mention_id = UUID(detection.mention.mention_id)
        await conn.execute(
            """
            INSERT INTO entity_candidate_generation_requests (
                id, tenant_id, context_snapshot_id, source_observation_id,
                phrase, mention_ref, entity_mention_detection_id,
                entity_mention_id, request_digest,
                processing_authority_fingerprint, required_lanes, request
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb
            )
            """,
            UUID(request.request_id),
            tenant_id,
            UUID(snapshot.snapshot_id),
            source_observation_id,
            phrase,
            request.mention_ref,
            detection.detection_id,
            mention_id,
            request.generation_request_digest,
            request.processing_authority_fingerprint,
            list(request.required_retrieval_lanes),
            json.dumps(request.model_dump(mode="json")),
        )
        await conn.execute(
            """
            INSERT INTO entity_candidate_sets (
                id, tenant_id, request_id, request_digest, candidate_set_version,
                lane_fates, candidates, candidate_set_hash, candidate_set,
                registry_version, expires_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9::jsonb,
                $10, $11
            )
            """,
            UUID(candidate_set.candidate_set_id),
            tenant_id,
            UUID(request.request_id),
            request.generation_request_digest,
            candidate_set.candidate_set_version,
            json.dumps([item.model_dump(mode="json") for item in candidate_set.lane_fates]),
            json.dumps([item.model_dump(mode="json") for item in candidate_set.candidates]),
            canonical_sha256(candidate_set_payload),
            json.dumps(candidate_set_payload),
            candidate_set.registry_version,
            candidate_set.expires_at,
        )
        await conn.execute(
            """
            INSERT INTO resolution_assessments (
                id, tenant_id, candidate_set_id, assessment_version,
                candidate_distribution, selected_candidate_id,
                suggested_canonical_ref, model_output, assessment,
                scorer_and_calibration_version, assessed_at, expires_at
            ) VALUES (
                $1, $2, $3, $4, $5::jsonb, $6, $7::jsonb, $8::jsonb,
                $9::jsonb, $10, $11, $12
            )
            """,
            UUID(assessment.assessment_id),
            tenant_id,
            UUID(candidate_set.candidate_set_id),
            assessment.assessment_version,
            json.dumps(assessment.candidate_distribution),
            episode.selected_candidate_id,
            json.dumps(episode.assessed_canonical_ref),
            json.dumps(episode.model_output),
            json.dumps(assessment_payload),
            assessment.scorer_and_calibration_version,
            assessment.assessed_at,
            assessment.expires_at,
        )
        await conn.execute(
            """
            INSERT INTO grounding_admission_decisions (
                id, tenant_id, assessment_id, decision_version, consumer,
                purpose, operation, risk_tier, disposition, selected_referent,
                reason_codes, consumption_authority_fingerprint, decision,
                decided_at, expires_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb,
                $11, $12, $13::jsonb, $14, $15
            )
            """,
            UUID(admission.decision_id),
            tenant_id,
            UUID(assessment.assessment_id),
            admission.decision_version,
            admission.consumer,
            admission.purpose,
            admission.operation,
            admission.risk_tier,
            admission.disposition.value,
            json.dumps(
                admission.selected_referent.model_dump(mode="json")
                if admission.selected_referent
                else None
            ),
            list(admission.reason_codes),
            admission.consumption_authority.fingerprint,
            json.dumps(admission_payload),
            admission.decided_at,
            admission.expires_at,
        )
        trace_id = uuid7()
        trace: dict[str, Any] = {
            "selection_dependency": episode.selection_dependency.model_dump(mode="json"),
            "context_snapshot": {
                "id": snapshot.snapshot_id,
                "version": snapshot.snapshot_version,
                "hash": snapshot.snapshot_content_hash,
            },
            "mention_detection": {
                "id": str(detection.detection_id),
                "version": detection.detection_version,
                "digest": detection.detection_digest,
                "fate": detection.fate.value,
            },
            "entity_mention": {
                "id": detection.mention.mention_id,
                "version": detection.mention.mention_version,
            },
            "candidate_request": {
                "id": request.request_id,
                "digest": request.generation_request_digest,
            },
            "candidate_set": {
                "id": candidate_set.candidate_set_id,
                "version": candidate_set.candidate_set_version,
            },
            "assessment": {
                "id": assessment.assessment_id,
                "version": assessment.assessment_version,
            },
            "admission": {
                "id": admission.decision_id,
                "version": admission.decision_version,
                "expires_at": admission.expires_at.isoformat(),
            },
            "model_output_is_evidence": False,
            "identity_registry_mutated": False,
            "source_observation_mutated": False,
        }
        await conn.execute(
            """
            INSERT INTO grounding_traces (
                id, tenant_id, source_observation_id, phrase,
                context_snapshot_id, entity_mention_detection_id,
                entity_mention_id, candidate_request_id, candidate_set_id,
                resolution_assessment_id, grounding_admission_id,
                current_fate, selected_referent, identity_registry_mutated,
                source_observation_mutated, trace
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                $13::jsonb, FALSE, FALSE, $14::jsonb
            )
            """,
            trace_id,
            tenant_id,
            source_observation_id,
            phrase,
            UUID(snapshot.snapshot_id),
            detection.detection_id,
            mention_id,
            UUID(request.request_id),
            UUID(candidate_set.candidate_set_id),
            UUID(assessment.assessment_id),
            UUID(admission.decision_id),
            episode.current_fate,
            json.dumps(episode.admitted_canonical_ref),
            json.dumps(trace),
        )
        terminal_fate = {
            "fate_kind": episode.current_fate,
            "terminal": True,
            "trace_id": str(trace_id),
            "reason_codes": list(admission.reason_codes),
            "contract_version": "grounding-work-fate-v1",
        }
        await conn.execute(
            """
            INSERT INTO entity_grounding_work_items (
                id, tenant_id, source_observation_id, phrase,
                processing_generation, status, processing_class,
                attempt_count, current_trace_id, useful_safe_fate
            ) VALUES (
                $1, $2, $3, $4, 1, $5, 'R2', 1, $6, $7::jsonb
            )
            ON CONFLICT (
                tenant_id, source_observation_id, phrase,
                processing_generation
            ) DO UPDATE SET
                status = EXCLUDED.status,
                attempt_count = entity_grounding_work_items.attempt_count + 1,
                next_attempt_at = NULL,
                last_failure_class = NULL,
                last_failure_reason = NULL,
                current_trace_id = EXCLUDED.current_trace_id,
                useful_safe_fate = EXCLUDED.useful_safe_fate,
                updated_at = now()
            WHERE entity_grounding_work_items.status IN (
                'pending', 'retry_scheduled'
            )
            """,
            uuid7(),
            tenant_id,
            source_observation_id,
            phrase,
            episode.current_fate,
            trace_id,
            json.dumps(terminal_fate),
        )
        return trace_id

    async def _commit_context_and_detection(
        self,
        conn: asyncpg.Connection,
        *,
        context_command: CommitInterpretationContextCommand,
        context_outcome: ContextSelectionOutcome,
        mention_detection_command: CommitEntityMentionDetectionCommand,
        tenant_id: UUID,
        source_observation_id: UUID,
        phrase: str,
    ) -> None:
        """Replay and prove the exact pre-model context and mention decisions."""

        snapshot = context_outcome.snapshot
        detection = mention_detection_command.detection
        self._validate_context_and_detection_binding(
            context_command=context_command,
            context_outcome=context_outcome,
            mention_detection_command=mention_detection_command,
            tenant_id=tenant_id,
            source_observation_id=source_observation_id,
            phrase=phrase,
        )
        appender = GroundingAnnotationAppender()
        context_result = await appender.apply_context(
            conn=conn,
            command=context_command,
            now=snapshot.frozen_at,
        )
        exact_context_commit = (
            context_result.object_id == UUID(snapshot.snapshot_id)
            and context_result.result.get("snapshot_digest")
            == snapshot.snapshot_content_hash
            and context_result.result.get("decision_digest")
            == context_outcome.decision_digest
            and context_result.result.get("disposition")
            == context_outcome.disposition.value
        )
        if not exact_context_commit:
            raise InvariantViolation(
                "GROUNDING_CONTEXT_SNAPSHOT_MISMATCH",
                "persisted context selection differs from the pre-model snapshot",
                expected_snapshot_id=snapshot.snapshot_id,
                persisted_snapshot_id=str(context_result.object_id),
                expected_snapshot_digest=snapshot.snapshot_content_hash,
                persisted_snapshot_digest=context_result.result.get(
                    "snapshot_digest"
                ),
                expected_decision_digest=context_outcome.decision_digest,
                persisted_decision_digest=context_result.result.get(
                    "decision_digest"
                ),
            )
        detection_result = await appender.apply_mention_detection(
            conn=conn,
            command=mention_detection_command,
            now=detection.detected_at,
        )
        expected_mention_id = (
            detection.mention.mention_id if detection.mention is not None else None
        )
        exact_detection_commit = (
            detection_result.object_id == detection.detection_id
            and detection_result.object_version == detection.detection_version
            and detection_result.result.get("detection_digest")
            == detection.detection_digest
            and detection_result.result.get("fate") == detection.fate.value
            and detection_result.result.get("mention_id") == expected_mention_id
            and detection_result.result.get("context_snapshot_id")
            == str(detection.context_snapshot_id)
            and detection_result.result.get("context_snapshot_digest")
            == detection.context_snapshot_digest
            and detection_result.result.get("source_content_hash")
            == detection.source_content_hash
        )
        if not exact_detection_commit:
            raise InvariantViolation(
                "GROUNDING_MENTION_DETECTION_MISMATCH",
                "persisted mention detection differs from the prepared detection",
                expected_detection_id=str(detection.detection_id),
                persisted_detection_id=str(detection_result.object_id),
                expected_detection_digest=detection.detection_digest,
                persisted_detection_digest=detection_result.result.get(
                    "detection_digest"
                ),
            )

    @staticmethod
    def _validate_context_and_detection_binding(
        *,
        context_command: CommitInterpretationContextCommand,
        context_outcome: ContextSelectionOutcome,
        mention_detection_command: CommitEntityMentionDetectionCommand,
        tenant_id: UUID,
        source_observation_id: UUID,
        phrase: str,
    ) -> None:
        snapshot = context_outcome.snapshot
        detection = mention_detection_command.detection
        exact = (
            context_command.context.tenant_id == tenant_id
            and detection.tenant_id == tenant_id
            and context_command.focal_observation_id == source_observation_id
            and detection.source_observation_id == source_observation_id
            and detection.candidate_surface == phrase
            and UUID(snapshot.snapshot_id) == context_command.proposed_snapshot_id
            and detection.context_snapshot_id == UUID(snapshot.snapshot_id)
            and detection.context_snapshot_digest == snapshot.snapshot_content_hash
            and detection.source_revision_id
            in context_command.request.focal_event_revision_ids
            and mention_detection_command.context.processing_authority.fingerprint
            == context_command.request.processing_authority.fingerprint
        )
        if not exact:
            raise InvariantViolation(
                "GROUNDING_MENTION_BINDING_MISMATCH",
                "context and mention detection do not bind one grounding opportunity",
                tenant_id=str(tenant_id),
                source_observation_id=str(source_observation_id),
                phrase=phrase,
                detection_id=str(detection.detection_id),
            )


__all__ = ["EntityGroundingRepo"]
