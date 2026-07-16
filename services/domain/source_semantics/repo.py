"""Persistence boundary for grounded source interpretations and admission fates."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID

import asyncpg

from lib.contracts.perception import (
    EntityMention,
    GroundingAdmissionDisposition,
    GroundingAdmissionDecision,
    GroundingContinuity,
)
from lib.contracts.kernel import ConsumptionAuthorityContext, RestrictionSet
from lib.contracts.source_semantics import (
    GroundedBeliefApplyResult,
    GroundedSourceSemanticBundle,
    ProposedBeliefAssertion,
    SourceSemanticAdmissionDisposition,
)
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


@dataclass(frozen=True, slots=True)
class GroundingTraceContext:
    trace_id: UUID
    tenant_id: UUID
    source_observation_id: UUID
    content_text: str
    source_channel: str
    source_author_ref: str
    source_actor_id: UUID | None
    occurred_at: datetime
    context_snapshot_id: UUID
    context_snapshot_version: int
    mention_ref: str
    mention: EntityMention
    resolution_assessment_id: UUID
    resolution_assessment_version: int
    grounding_admission_id: UUID
    grounding_admission_version: int
    grounding_admission: GroundingAdmissionDecision
    current_fate: str
    selected_scope_entity: dict[str, Any] | None

    def continuity(
        self,
        *,
        downstream_object_ref: str,
        admission_id: UUID | None = None,
        admission: GroundingAdmissionDecision | None = None,
    ) -> GroundingContinuity:
        admission_id = admission_id or self.grounding_admission_id
        admission = admission or self.grounding_admission
        return GroundingContinuity(
            downstream_object_ref=downstream_object_ref,
            mention_ref=self.mention_ref,
            mention_version=self.mention.mention_version,
            resolution_assessment_ref=(
                f"resolution-assessment:{self.resolution_assessment_id}"
            ),
            resolution_assessment_version=self.resolution_assessment_version,
            grounding_admission_ref=f"grounding-admission:{admission_id}",
            grounding_admission_version=admission.decision_version,
            selected_referent=admission.selected_referent,
        )


class SourceSemanticWorkStatus(StrEnum):
    AWAITING_EMBEDDING = "awaiting_embedding"
    PENDING = "pending"
    PROCESSING = "processing"
    BELIEF_APPLIED = "belief_applied"
    NO_ADMISSION = "no_admission"
    RETRY_SCHEDULED = "retry_scheduled"
    FAILED_TERMINAL = "failed_terminal"


@dataclass(frozen=True, slots=True)
class SourceSemanticWorkItem:
    id: UUID
    tenant_id: UUID
    grounding_trace_id: UUID
    status: SourceSemanticWorkStatus
    attempt_count: int
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    lease_expires_at: datetime | None
    interpretation_id: UUID | None
    admission_decision_id: UUID | None
    admitted_model_id: UUID | None
    last_failure_class: str | None
    last_failure_reason: str | None


def _work_item(row: asyncpg.Record) -> SourceSemanticWorkItem:
    return SourceSemanticWorkItem(
        id=row["id"],
        tenant_id=row["tenant_id"],
        grounding_trace_id=row["grounding_trace_id"],
        status=SourceSemanticWorkStatus(str(row["status"])),
        attempt_count=int(row["attempt_count"]),
        available_at=row["available_at"],
        claimed_by=row["claimed_by"],
        claim_token=row["claim_token"],
        lease_expires_at=row["lease_expires_at"],
        interpretation_id=row["interpretation_id"],
        admission_decision_id=row["admission_decision_id"],
        admitted_model_id=row["admitted_model_id"],
        last_failure_class=row["last_failure_class"],
        last_failure_reason=row["last_failure_reason"],
    )


class SourceSemanticRepo:
    async def enqueue_work(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        grounding_trace_id: UUID,
        now: datetime,
    ) -> SourceSemanticWorkItem:
        """Idempotently establish one mutable work head for a grounding trace."""

        row = await conn.fetchrow(
            """
            INSERT INTO source_semantic_work_items (
                id, tenant_id, grounding_trace_id, status,
                available_at, created_at, updated_at
            )
            SELECT $1, $2, gt.id,
                   CASE
                     WHEN observation.embedding IS NOT NULL
                          AND observation.embedding_pending=FALSE
                     THEN 'pending'
                     ELSE 'awaiting_embedding'
                   END,
                   $4, $4, $4
            FROM grounding_traces gt
            JOIN observations observation
              ON observation.tenant_id=gt.tenant_id
             AND observation.id=gt.source_observation_id
            WHERE gt.tenant_id=$2 AND gt.id=$3
            ON CONFLICT (tenant_id, grounding_trace_id) DO UPDATE SET
              grounding_trace_id=EXCLUDED.grounding_trace_id
            RETURNING *
            """,
            uuid7(),
            tenant_id,
            grounding_trace_id,
            now,
        )
        if row is None:
            raise InvariantViolation(
                "SOURCE_SEMANTIC_GROUNDING_MISSING",
                "source semantic work requires a tenant-matched grounding trace",
                tenant_id=str(tenant_id),
                grounding_trace_id=str(grounding_trace_id),
            )
        return _work_item(row)

    async def claim_ready_work(
        self,
        conn: asyncpg.Connection,
        *,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
        limit: int,
    ) -> tuple[SourceSemanticWorkItem, ...]:
        """Claim ready work and atomically recover abandoned expired leases.

        Awaiting work becomes claimable only after the trace's source observation
        has a durable embedding. No semantic interpretation happens here.
        """

        if not worker_id.strip():
            raise ValueError("worker_id must be non-empty")
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        rows = await conn.fetch(
            """
            WITH candidates AS (
                SELECT work.id
                FROM source_semantic_work_items work
                JOIN grounding_traces trace
                  ON trace.tenant_id=work.tenant_id
                 AND trace.id=work.grounding_trace_id
                JOIN observations observation
                  ON observation.tenant_id=trace.tenant_id
                 AND observation.id=trace.source_observation_id
                WHERE (
                    (
                      work.status IN ('pending', 'retry_scheduled')
                      AND work.available_at <= $2
                      AND observation.embedding IS NOT NULL
                      AND observation.embedding_pending=FALSE
                    )
                    OR (
                      work.status='awaiting_embedding'
                      AND observation.embedding IS NOT NULL
                      AND observation.embedding_pending=FALSE
                    )
                    OR (
                      work.status='processing'
                      AND work.lease_expires_at <= $2
                    )
                  )
                ORDER BY
                  CASE WHEN work.status='processing' THEN work.lease_expires_at
                       ELSE work.available_at END,
                  work.created_at,
                  work.id
                FOR UPDATE OF work SKIP LOCKED
                LIMIT $4
            )
            UPDATE source_semantic_work_items work
            SET status='processing',
                claimed_by=$1,
                claim_token=gen_random_uuid(),
                lease_expires_at=$2 + $3::interval,
                attempt_count=work.attempt_count + 1,
                updated_at=$2
            FROM candidates
            WHERE work.id=candidates.id
            RETURNING work.*
            """,
            worker_id,
            now,
            lease_duration,
            limit,
        )
        return tuple(_work_item(row) for row in rows)

    async def load_claimed_embedding(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        now: datetime,
    ) -> list[float]:
        """Load the source embedding only for the current live fenced claim."""

        embedding_text = await conn.fetchval(
            """
            SELECT observation.embedding::text
            FROM source_semantic_work_items work
            JOIN grounding_traces trace
              ON trace.tenant_id=work.tenant_id
             AND trace.id=work.grounding_trace_id
            JOIN observations observation
              ON observation.tenant_id=trace.tenant_id
             AND observation.id=trace.source_observation_id
            WHERE work.tenant_id=$1 AND work.id=$2
              AND work.status='processing' AND work.claimed_by=$3
              AND work.claim_token=$4 AND work.lease_expires_at > $5
              AND observation.embedding IS NOT NULL
              AND observation.embedding_pending=FALSE
            """,
            tenant_id,
            work_item_id,
            worker_id,
            claim_token,
            now,
        )
        if embedding_text is None:
            raise InvariantViolation(
                "SOURCE_SEMANTIC_STALE_CLAIM",
                "embedding load requires the current live fence token",
                work_item_id=str(work_item_id),
            )
        embedding = json.loads(str(embedding_text))
        if not isinstance(embedding, list):
            raise InvariantViolation(
                "SOURCE_SEMANTIC_EMBEDDING_INVALID",
                "source observation embedding must decode to a vector",
                work_item_id=str(work_item_id),
            )
        return [float(value) for value in embedding]

    async def schedule_retry(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        now: datetime,
        next_attempt_at: datetime,
        failure_class: str,
        failure_reason: str,
    ) -> SourceSemanticWorkItem:
        """Release a live fenced claim into a durable retry schedule."""

        if next_attempt_at <= now:
            raise ValueError("next_attempt_at must be after now")
        if not failure_class.strip() or not failure_reason.strip():
            raise ValueError("retry failure class and reason must be non-empty")
        row = await conn.fetchrow(
            """
            UPDATE source_semantic_work_items
            SET status='retry_scheduled', available_at=$6,
                claimed_by=NULL, claim_token=NULL, lease_expires_at=NULL,
                last_failure_class=$7, last_failure_reason=$8, updated_at=$5
            WHERE tenant_id=$1 AND id=$2 AND status='processing'
              AND claimed_by=$3 AND claim_token=$4
              AND lease_expires_at > $5
            RETURNING *
            """,
            tenant_id,
            work_item_id,
            worker_id,
            claim_token,
            now,
            next_attempt_at,
            failure_class,
            failure_reason,
        )
        return self._require_claim_transition(row, work_item_id)

    async def terminalize_work(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        disposition: SourceSemanticAdmissionDisposition,
        interpretation_id: UUID,
        admission_decision_id: UUID,
        admitted_model_id: UUID | None,
        now: datetime,
    ) -> SourceSemanticWorkItem:
        """Fence and terminalize a completed semantic interpretation."""

        if (
            disposition is SourceSemanticAdmissionDisposition.BELIEF_APPLIED
            and admitted_model_id is None
        ):
            raise ValueError("belief_applied requires admitted_model_id")
        if (
            disposition is SourceSemanticAdmissionDisposition.NO_ADMISSION
            and admitted_model_id is not None
        ):
            raise ValueError("no_admission cannot name an admitted model")
        row = await conn.fetchrow(
            """
            UPDATE source_semantic_work_items
            SET status=$6, claimed_by=NULL, claim_token=NULL,
                lease_expires_at=NULL, interpretation_id=$7,
                admission_decision_id=$8, admitted_model_id=$9,
                last_failure_class=NULL, last_failure_reason=NULL,
                updated_at=$5
            WHERE tenant_id=$1 AND id=$2 AND status='processing'
              AND claimed_by=$3 AND claim_token=$4
              AND lease_expires_at > $5
              AND EXISTS (
                SELECT 1
                FROM source_semantic_interpretations interpretation
                JOIN source_semantic_admission_decisions admission
                  ON admission.tenant_id=interpretation.tenant_id
                 AND admission.interpretation_id=interpretation.id
                WHERE interpretation.tenant_id=$1
                  AND interpretation.id=$7
                  AND interpretation.grounding_trace_id=(
                    SELECT grounding_trace_id
                    FROM source_semantic_work_items
                    WHERE id=$2 AND tenant_id=$1
                  )
                  AND admission.id=$8
                  AND admission.disposition=$6
                  AND admission.admitted_model_id IS NOT DISTINCT FROM $9
              )
              AND (
                ($6='no_admission' AND $9::uuid IS NULL)
                OR (
                  $6='belief_applied'
                  AND EXISTS (
                    SELECT 1 FROM models model
                    WHERE model.tenant_id=$1 AND model.id=$9
                  )
                )
              )
            RETURNING *
            """,
            tenant_id,
            work_item_id,
            worker_id,
            claim_token,
            now,
            disposition.value,
            interpretation_id,
            admission_decision_id,
            admitted_model_id,
        )
        return self._require_claim_transition(row, work_item_id)

    async def fail_work_terminally(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        now: datetime,
        failure_class: str,
        failure_reason: str,
    ) -> SourceSemanticWorkItem:
        """Fence a non-retryable failure without inventing semantic facts."""

        if not failure_class.strip() or not failure_reason.strip():
            raise ValueError("terminal failure class and reason must be non-empty")
        row = await conn.fetchrow(
            """
            UPDATE source_semantic_work_items
            SET status='failed_terminal', claimed_by=NULL, claim_token=NULL,
                lease_expires_at=NULL, last_failure_class=$6,
                last_failure_reason=$7, updated_at=$5
            WHERE tenant_id=$1 AND id=$2 AND status='processing'
              AND claimed_by=$3 AND claim_token=$4
              AND lease_expires_at > $5
            RETURNING *
            """,
            tenant_id,
            work_item_id,
            worker_id,
            claim_token,
            now,
            failure_class,
            failure_reason,
        )
        return self._require_claim_transition(row, work_item_id)

    @staticmethod
    def _require_claim_transition(
        row: asyncpg.Record | None,
        work_item_id: UUID,
    ) -> SourceSemanticWorkItem:
        if row is None:
            raise InvariantViolation(
                "SOURCE_SEMANTIC_STALE_CLAIM",
                "source semantic work transition requires the current live fence token",
                work_item_id=str(work_item_id),
            )
        return _work_item(row)

    async def ensure_epistemic_admission(
        self,
        conn: asyncpg.Connection,
        *,
        grounding: GroundingTraceContext,
        now: datetime,
    ) -> tuple[UUID, GroundingAdmissionDecision]:
        """Derive a separate live admission for the epistemic consumer."""

        existing = await conn.fetchrow(
            """
            SELECT id, decision
            FROM grounding_admission_decisions
            WHERE tenant_id=$1 AND assessment_id=$2
              AND consumer='epistemic-applier'
              AND purpose='belief-admission'
              AND operation='create-grounded-belief'
              AND decision_version=1
            FOR KEY SHARE
            """,
            grounding.tenant_id,
            grounding.resolution_assessment_id,
        )
        if existing is not None:
            decision = GroundingAdmissionDecision.model_validate(
                _json(existing["decision"])
            )
            if not decision.consumption_authority.is_live(now):
                raise InvariantViolation(
                    "EPISTEMIC_GROUNDING_ADMISSION_EXPIRED",
                    "the existing epistemic grounding admission is no longer live",
                    grounding_admission_id=str(existing["id"]),
                )
            return existing["id"], decision

        source_admission = grounding.grounding_admission
        if (
            grounding.current_fate != "resolved_for_consumer"
            or source_admission.disposition
            is not GroundingAdmissionDisposition.SINGLE_REFERENT
            or source_admission.selected_referent is None
        ):
            raise InvariantViolation(
                "EPISTEMIC_GROUNDING_NOT_ELIGIBLE",
                "epistemic admission requires an admitted source assessment",
                grounding_trace_id=str(grounding.trace_id),
            )
        expires_at = min(
            source_admission.expires_at,
            source_admission.assessment.expires_at,
        )
        if now >= expires_at:
            raise InvariantViolation(
                "EPISTEMIC_GROUNDING_ADMISSION_EXPIRED",
                "the source assessment expired before epistemic admission",
                grounding_trace_id=str(grounding.trace_id),
            )
        authority = ConsumptionAuthorityContext(
            tenant_id=grounding.tenant_id,
            principal_or_service_id="service:epistemic-applier",
            purpose="belief-admission",
            operation="create-grounded-belief",
            object_types=RestrictionSet.only("resolution_assessment"),
            object_ids=RestrictionSet.only(
                str(grounding.resolution_assessment_id),
                str(grounding.source_observation_id),
            ),
            fields=RestrictionSet.unrestricted(),
            source_labels=RestrictionSet.only(grounding.source_channel),
            authority_basis_refs=frozenset(
                {"service-policy:epistemic-grounding-consumer-v1"}
            ),
            policy_version="epistemic-grounding-consumption-v1",
            authority_epoch=1,
            decision_time=now,
            expires_at=expires_at,
        )
        decision_id = uuid7()
        decision = GroundingAdmissionDecision(
            decision_id=str(decision_id),
            decision_version=1,
            assessment=source_admission.assessment,
            consumer="epistemic-applier",
            purpose=authority.purpose,
            operation=authority.operation,
            risk_tier="medium",
            blast_radius="tenant-local-belief-model",
            expected_loss=source_admission.expected_loss,
            consumption_authority=authority,
            consumer_supports_distributions=False,
            disposition=GroundingAdmissionDisposition.SINGLE_REFERENT,
            selected_referent=source_admission.selected_referent,
            reason_codes=("separate_epistemic_consumer_admission",),
            decided_at=now,
            expires_at=expires_at,
        )
        await conn.execute(
            """
            INSERT INTO grounding_admission_decisions (
                id, tenant_id, assessment_id, decision_version, consumer,
                purpose, operation, risk_tier, disposition, selected_referent,
                reason_codes, consumption_authority_fingerprint, decision,
                decided_at, expires_at
            ) VALUES (
                $1, $2, $3, 1, $4, $5, $6, $7, $8, $9::jsonb,
                $10, $11, $12::jsonb, $13, $14
            )
            """,
            decision_id,
            grounding.tenant_id,
            grounding.resolution_assessment_id,
            decision.consumer,
            decision.purpose,
            decision.operation,
            decision.risk_tier,
            decision.disposition.value,
            json.dumps(decision.selected_referent.model_dump(mode="json")),
            list(decision.reason_codes),
            authority.fingerprint,
            json.dumps(decision.model_dump(mode="json")),
            decision.decided_at,
            decision.expires_at,
        )
        return decision_id, decision

    async def load_grounding_trace(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        grounding_trace_id: UUID,
    ) -> GroundingTraceContext:
        row = await conn.fetchrow(
            """
            SELECT gt.id, gt.tenant_id, gt.source_observation_id,
                   gt.context_snapshot_id, gt.resolution_assessment_id,
                   gt.grounding_admission_id, gt.current_fate,
                   gt.selected_referent,
                   o.content_text, o.source_channel, o.source_actor_ref,
                   o.actor_id, o.occurred_at,
                   ics.snapshot_version AS context_snapshot_version,
                   req.mention_ref,
                   emd.mention,
                   ra.assessment_version AS resolution_assessment_version,
                   gad.decision_version AS grounding_admission_version,
                   gad.decision AS grounding_admission
            FROM grounding_traces gt
            JOIN observations o
              ON o.tenant_id=gt.tenant_id AND o.id=gt.source_observation_id
            JOIN interpretation_context_snapshots ics
              ON ics.tenant_id=gt.tenant_id AND ics.id=gt.context_snapshot_id
            JOIN entity_candidate_generation_requests req
              ON req.tenant_id=gt.tenant_id AND req.id=gt.candidate_request_id
            JOIN entity_mention_detections emd
              ON emd.tenant_id=gt.tenant_id
             AND emd.id=gt.entity_mention_detection_id
            JOIN resolution_assessments ra
              ON ra.tenant_id=gt.tenant_id AND ra.id=gt.resolution_assessment_id
            JOIN grounding_admission_decisions gad
              ON gad.tenant_id=gt.tenant_id AND gad.id=gt.grounding_admission_id
            WHERE gt.tenant_id=$1 AND gt.id=$2
            FOR KEY SHARE OF gt, o, ics, req, emd, ra, gad
            """,
            tenant_id,
            grounding_trace_id,
        )
        if row is None:
            raise InvariantViolation(
                "SOURCE_SEMANTIC_GROUNDING_MISSING",
                "source semantics require one completed grounding trace",
                grounding_trace_id=str(grounding_trace_id),
            )
        mention_payload = _json(row["mention"])
        if not isinstance(mention_payload, dict):
            raise InvariantViolation(
                "SOURCE_SEMANTIC_MENTION_MISSING",
                "grounded source semantics require one durable EntityMention",
                grounding_trace_id=str(grounding_trace_id),
            )
        selected_scope_entity = _json(row["selected_referent"])
        source_actor_id = row["actor_id"]
        source_author_ref = row["source_actor_ref"]
        if not source_author_ref and source_actor_id is not None:
            source_author_ref = f"actor:{source_actor_id}"
        if not source_author_ref:
            source_author_ref = f"unresolved-source-author:{row['source_channel']}"
        return GroundingTraceContext(
            trace_id=row["id"],
            tenant_id=row["tenant_id"],
            source_observation_id=row["source_observation_id"],
            content_text=str(row["content_text"] or ""),
            source_channel=str(row["source_channel"]),
            source_author_ref=str(source_author_ref),
            source_actor_id=source_actor_id,
            occurred_at=row["occurred_at"],
            context_snapshot_id=row["context_snapshot_id"],
            context_snapshot_version=int(row["context_snapshot_version"]),
            mention_ref=str(row["mention_ref"]),
            mention=EntityMention.model_validate(mention_payload),
            resolution_assessment_id=row["resolution_assessment_id"],
            resolution_assessment_version=int(
                row["resolution_assessment_version"]
            ),
            grounding_admission_id=row["grounding_admission_id"],
            grounding_admission_version=int(row["grounding_admission_version"]),
            grounding_admission=GroundingAdmissionDecision.model_validate(
                _json(row["grounding_admission"])
            ),
            current_fate=str(row["current_fate"]),
            selected_scope_entity=(
                dict(selected_scope_entity)
                if isinstance(selected_scope_entity, dict)
                else None
            ),
        )

    async def find_result(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        grounding_trace_id: UUID,
        bundle_digest: str,
    ) -> GroundedBeliefApplyResult | None:
        row = await conn.fetchrow(
            """
            SELECT i.id AS interpretation_id, i.bundle_digest,
                   a.id AS admission_decision_id, a.disposition,
                   a.reason_codes, a.admitted_model_id
            FROM source_semantic_interpretations i
            JOIN source_semantic_admission_decisions a
              ON a.tenant_id=i.tenant_id AND a.interpretation_id=i.id
            WHERE i.tenant_id=$1 AND i.grounding_trace_id=$2
            """,
            tenant_id,
            grounding_trace_id,
        )
        if row is None:
            return None
        if row["bundle_digest"] != bundle_digest:
            raise InvariantViolation(
                "SOURCE_SEMANTIC_IDEMPOTENCY_CONFLICT",
                "one grounding trace was reinterpreted with different content",
                grounding_trace_id=str(grounding_trace_id),
            )
        return GroundedBeliefApplyResult(
            interpretation_id=row["interpretation_id"],
            admission_decision_id=row["admission_decision_id"],
            disposition=SourceSemanticAdmissionDisposition(str(row["disposition"])),
            reason_codes=tuple(row["reason_codes"]),
            model_id=row["admitted_model_id"],
            duplicate=True,
        )

    async def append_interpretation(
        self,
        conn: asyncpg.Connection,
        *,
        interpretation_id: UUID,
        grounding: GroundingTraceContext,
        bundle: GroundedSourceSemanticBundle,
        continuity: GroundingContinuity,
        grounding_admission_id: UUID,
        source_content_hash: str,
        recorded_at: datetime,
    ) -> None:
        await conn.execute(
            """
            INSERT INTO source_semantic_interpretations (
                id, tenant_id, grounding_trace_id, source_observation_id,
                context_snapshot_id, entity_mention_id,
                resolution_assessment_id, grounding_admission_id,
                source_content_hash, source_assertion, semantic_frame,
                speech_act, grounding_continuity, bundle_digest,
                extractor_version, recorded_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb,
                $14, $15, $16
            )
            """,
            interpretation_id,
            bundle.tenant_id,
            bundle.grounding_trace_id,
            grounding.source_observation_id,
            grounding.context_snapshot_id,
            UUID(grounding.mention.mention_id),
            grounding.resolution_assessment_id,
            grounding_admission_id,
            source_content_hash,
            json.dumps(bundle.source_assertion.model_dump(mode="json")),
            json.dumps(bundle.semantic_frame.model_dump(mode="json")),
            json.dumps(bundle.speech_act.model_dump(mode="json")),
            json.dumps(continuity.model_dump(mode="json")),
            bundle.bundle_digest,
            bundle.source_assertion.extractor_version,
            recorded_at,
        )

    async def append_admission(
        self,
        conn: asyncpg.Connection,
        *,
        decision_id: UUID,
        tenant_id: UUID,
        interpretation_id: UUID,
        disposition: SourceSemanticAdmissionDisposition,
        reason_codes: tuple[str, ...],
        proposal: ProposedBeliefAssertion | None,
        admitted_model_id: UUID | None,
        decision_digest: str,
        decided_at: datetime,
    ) -> None:
        await conn.execute(
            """
            INSERT INTO source_semantic_admission_decisions (
                id, tenant_id, interpretation_id, disposition, reason_codes,
                proposed_belief_assertion, admitted_model_id,
                decision_digest, decided_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9
            )
            """,
            decision_id,
            tenant_id,
            interpretation_id,
            disposition.value,
            list(reason_codes),
            (
                json.dumps(proposal.model_dump(mode="json"))
                if proposal is not None
                else None
            ),
            admitted_model_id,
            decision_digest,
            decided_at,
        )


__all__ = [
    "GroundingTraceContext",
    "SourceSemanticRepo",
    "SourceSemanticWorkItem",
    "SourceSemanticWorkStatus",
]
