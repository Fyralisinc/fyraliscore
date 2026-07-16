"""Leased scheduling plans for exact registered task Work.

The repository derives runtime delivery work from immutable canonical events.
It never owns Work decisions or lease truth: those remain exclusively with
``WorkLedgerApplier`` and are revalidated before queue terminalization.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import asyncpg

from lib.contracts.agency import (
    AuthorizationDecision,
    AuthorizationDisposition,
    InterventionSpec,
)
from lib.contracts.execution import (
    LeaseState,
    LeaseToken,
    TaskSnapshot,
    WorkDecision,
    WorkObligation,
    WorkObligationState,
    WorkStateTransition,
    WorkflowRunSnapshot,
)
from lib.contracts.kernel import canonical_sha256
from lib.contracts.runtime import ProcessingClass
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7


_POLICY_VERSION = "work-scheduling-policy:v1"
_LEASE_OWNER_REF = "worker:agency-task-executor"
_HEARTBEAT_WINDOW = timedelta(minutes=5)
_LEASE_WINDOW = timedelta(minutes=30)


class WorkSchedulingWorkStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    RETRY_SCHEDULED = "retry_scheduled"
    LEASED = "leased"
    WORK_EXPIRED = "work_expired"
    AUTHORIZATION_EXPIRED = "authorization_expired"
    FAILED_TERMINAL = "failed_terminal"


@dataclass(frozen=True, slots=True)
class WorkSchedulePlan:
    plan_version: int
    tenant_id: UUID
    source_event_id: UUID
    obligation_id: UUID
    obligation_generation: int
    source_obligation_version: int
    task_id: UUID
    task_version: int
    workflow_run_id: UUID
    workflow_version: int
    episode_id: UUID
    authorization_decision_id: UUID
    authorization_decision_version: int
    intervention_spec_id: UUID
    intervention_spec_digest: str
    decision_id: UUID
    lease_token_id: UUID
    selected_processing_class: ProcessingClass
    scheduled_at: datetime
    lease_owner_ref: str
    heartbeat_deadline: datetime
    work_lease_expires_at: datetime
    policy_version_ref: str
    plan_digest: str


@dataclass(frozen=True, slots=True)
class WorkSchedulingWorkItem:
    id: UUID
    plan: WorkSchedulePlan
    status: WorkSchedulingWorkStatus
    attempt_count: int
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    lease_expires_at: datetime | None
    eligible_work_version: int | None
    leased_work_version: int | None
    applied_lease_version: int | None
    applied_lease_fence: int | None
    leased_at: datetime | None
    expired_work_version: int | None
    work_expired_at: datetime | None
    authorization_expired_at: datetime | None
    last_failure_class: str | None
    last_failure_reason: str | None
    created_at: datetime
    updated_at: datetime

    @property
    def tenant_id(self) -> UUID:
        return self.plan.tenant_id

    @property
    def source_event_id(self) -> UUID:
        return self.plan.source_event_id

    @property
    def obligation_id(self) -> UUID:
        return self.plan.obligation_id


@dataclass(frozen=True, slots=True)
class WorkSchedulingWorkContext:
    work_item: WorkSchedulingWorkItem
    plan: WorkSchedulePlan
    obligation: WorkObligation
    task: TaskSnapshot
    workflow: WorkflowRunSnapshot
    authorization: AuthorizationDecision
    intervention_spec: InterventionSpec


@dataclass(frozen=True, slots=True)
class _SchedulingSource:
    event: asyncpg.Record
    obligation: WorkObligation
    task: TaskSnapshot
    task_version: int
    workflow: WorkflowRunSnapshot
    workflow_version: int
    authorization: AuthorizationDecision
    intervention_spec: InterventionSpec


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _target_ref(spec: InterventionSpec) -> str:
    target = spec.target_referent
    return f"referent:{target.referent_id}:v{target.referent_version}"


def _schedule_uuid(
    *,
    tenant_id: UUID,
    obligation_id: UUID,
    generation: int,
    kind: str,
) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        (
            "fyralis:registered-work-scheduling:v1:"
            f"{tenant_id}:{obligation_id}:{generation}:{kind}"
        ),
    )


def _plan_material(plan: WorkSchedulePlan) -> dict[str, Any]:
    return {
        "plan_version": plan.plan_version,
        "tenant_id": str(plan.tenant_id),
        "source_event_id": str(plan.source_event_id),
        "obligation_id": str(plan.obligation_id),
        "obligation_generation": plan.obligation_generation,
        "source_obligation_version": plan.source_obligation_version,
        "task_id": str(plan.task_id),
        "task_version": plan.task_version,
        "workflow_run_id": str(plan.workflow_run_id),
        "workflow_version": plan.workflow_version,
        "episode_id": str(plan.episode_id),
        "authorization_decision_id": str(plan.authorization_decision_id),
        "authorization_decision_version": plan.authorization_decision_version,
        "intervention_spec_id": str(plan.intervention_spec_id),
        "intervention_spec_digest": plan.intervention_spec_digest,
        "decision_id": str(plan.decision_id),
        "lease_token_id": str(plan.lease_token_id),
        "selected_processing_class": plan.selected_processing_class.value,
        "scheduled_at": plan.scheduled_at,
        "lease_owner_ref": plan.lease_owner_ref,
        "heartbeat_deadline": plan.heartbeat_deadline,
        "work_lease_expires_at": plan.work_lease_expires_at,
        "policy_version_ref": plan.policy_version_ref,
    }


def _plan(row: asyncpg.Record) -> WorkSchedulePlan:
    plan = WorkSchedulePlan(
        plan_version=int(row["plan_version"]),
        tenant_id=row["tenant_id"],
        source_event_id=row["source_event_id"],
        obligation_id=row["obligation_id"],
        obligation_generation=int(row["obligation_generation"]),
        source_obligation_version=int(row["source_obligation_version"]),
        task_id=row["task_id"],
        task_version=int(row["task_version"]),
        workflow_run_id=row["workflow_run_id"],
        workflow_version=int(row["workflow_version"]),
        episode_id=row["episode_id"],
        authorization_decision_id=row["authorization_decision_id"],
        authorization_decision_version=int(row["authorization_decision_version"]),
        intervention_spec_id=row["intervention_spec_id"],
        intervention_spec_digest=str(row["intervention_spec_digest"]),
        decision_id=row["decision_id"],
        lease_token_id=row["lease_token_id"],
        selected_processing_class=ProcessingClass(
            str(row["selected_processing_class"])
        ),
        scheduled_at=row["scheduled_at"],
        lease_owner_ref=str(row["lease_owner_ref"]),
        heartbeat_deadline=row["planned_heartbeat_deadline"],
        work_lease_expires_at=row["planned_work_lease_expires_at"],
        policy_version_ref=str(row["policy_version_ref"]),
        plan_digest=str(row["plan_digest"]),
    )
    if canonical_sha256(_plan_material(plan)) != plan.plan_digest:
        raise InvariantViolation(
            "WORK_SCHEDULING_PLAN_DIGEST_DRIFT",
            "stored schedule plan no longer matches its canonical digest",
            work_item_id=str(row["id"]),
        )
    return plan


def _work_item(row: asyncpg.Record) -> WorkSchedulingWorkItem:
    return WorkSchedulingWorkItem(
        id=row["id"],
        plan=_plan(row),
        status=WorkSchedulingWorkStatus(str(row["status"])),
        attempt_count=int(row["attempt_count"]),
        available_at=row["available_at"],
        claimed_by=row["claimed_by"],
        claim_token=row["claim_token"],
        lease_expires_at=row["lease_expires_at"],
        eligible_work_version=(
            int(row["eligible_work_version"])
            if row["eligible_work_version"] is not None
            else None
        ),
        leased_work_version=(
            int(row["leased_work_version"])
            if row["leased_work_version"] is not None
            else None
        ),
        applied_lease_version=(
            int(row["applied_lease_version"])
            if row["applied_lease_version"] is not None
            else None
        ),
        applied_lease_fence=(
            int(row["applied_lease_fence"])
            if row["applied_lease_fence"] is not None
            else None
        ),
        leased_at=row["leased_at"],
        expired_work_version=(
            int(row["expired_work_version"])
            if row["expired_work_version"] is not None
            else None
        ),
        work_expired_at=row["work_expired_at"],
        authorization_expired_at=row["authorization_expired_at"],
        last_failure_class=row["last_failure_class"],
        last_failure_reason=row["last_failure_reason"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class WorkSchedulingRepo:
    async def discover_ready_work(
        self,
        conn: asyncpg.Connection,
        *,
        now: datetime,
        limit: int,
        tenant_id: UUID | None = None,
    ) -> int:
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        rows = await conn.fetch(
            """
            SELECT event.id
            FROM agency_canonical_events event
            JOIN work_obligation_specs spec
              ON spec.tenant_id=event.tenant_id
             AND spec.obligation_id=event.object_id
             AND spec.command_result_id=event.command_result_id
            WHERE event.writer_id='WorkLedgerApplier'
              AND event.object_type='work_obligation'
              AND event.object_version=1
              AND event.semantic_transition='registered'
              AND spec.target_object_type='task'
              AND ($1::uuid IS NULL OR event.tenant_id=$1)
              AND NOT EXISTS (
                SELECT 1
                FROM registered_work_scheduling_items work
                WHERE work.tenant_id=event.tenant_id
                  AND work.source_event_id=event.id
              )
            ORDER BY event.created_at, event.id
            FOR UPDATE OF event SKIP LOCKED
            LIMIT $2
            """,
            tenant_id,
            limit,
        )
        discovered = 0
        for row in rows:
            item = await self.discover_from_event(
                conn,
                source_event_id=row["id"],
                now=now,
            )
            if item is not None:
                discovered += 1
        return discovered

    async def discover_from_event(
        self,
        conn: asyncpg.Connection,
        *,
        source_event_id: UUID,
        now: datetime,
    ) -> WorkSchedulingWorkItem | None:
        source = await self._load_source(
            conn,
            source_event_id=source_event_id,
            require_registered_head=True,
            unsupported_returns_none=True,
        )
        if source is None:
            return None
        work = source.obligation
        scheduled_at = max(now, work.registered_at)
        authorization_end = min(
            source.authorization.expires_at,
            source.authorization.authority.expires_at,
        )
        lease_end = min(
            scheduled_at + _LEASE_WINDOW,
            work.deadline,
            authorization_end,
        )
        heartbeat = min(scheduled_at + _HEARTBEAT_WINDOW, lease_end)
        plan = WorkSchedulePlan(
            plan_version=1,
            tenant_id=work.tenant_id,
            source_event_id=source_event_id,
            obligation_id=work.obligation_id,
            obligation_generation=work.generation,
            source_obligation_version=1,
            task_id=source.task.task_id,
            task_version=source.task_version,
            workflow_run_id=source.workflow.workflow_run_id,
            workflow_version=source.workflow_version,
            episode_id=source.task.episode_id,
            authorization_decision_id=source.authorization.decision_id,
            authorization_decision_version=1,
            intervention_spec_id=source.intervention_spec.spec_id,
            intervention_spec_digest=source.intervention_spec.spec_digest,
            decision_id=_schedule_uuid(
                tenant_id=work.tenant_id,
                obligation_id=work.obligation_id,
                generation=work.generation,
                kind="decision",
            ),
            lease_token_id=_schedule_uuid(
                tenant_id=work.tenant_id,
                obligation_id=work.obligation_id,
                generation=work.generation,
                kind="lease",
            ),
            selected_processing_class=work.minimum_processing_class,
            scheduled_at=scheduled_at,
            lease_owner_ref=_LEASE_OWNER_REF,
            heartbeat_deadline=heartbeat,
            work_lease_expires_at=lease_end,
            policy_version_ref=_POLICY_VERSION,
            plan_digest="",
        )
        plan = replace(
            plan,
            plan_digest=canonical_sha256(_plan_material(plan)),
        )
        inserted = await conn.fetchrow(
            """
            INSERT INTO registered_work_scheduling_items (
              id, tenant_id, source_event_id, plan_version,
              obligation_id, obligation_generation, source_obligation_version,
              task_id, task_version, workflow_run_id, workflow_version,
              episode_id, authorization_decision_id,
              authorization_decision_version, intervention_spec_id,
              intervention_spec_digest, decision_id, lease_token_id,
              selected_processing_class, scheduled_at, lease_owner_ref,
              planned_heartbeat_deadline, planned_work_lease_expires_at,
              policy_version_ref, plan_digest, status,
              available_at, created_at, updated_at
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
              $18,$19,$20,$21,$22,$23,$24,$25,'pending',$26,$26,$26
            )
            ON CONFLICT DO NOTHING
            RETURNING *
            """,
            uuid7(),
            plan.tenant_id,
            plan.source_event_id,
            plan.plan_version,
            plan.obligation_id,
            plan.obligation_generation,
            plan.source_obligation_version,
            plan.task_id,
            plan.task_version,
            plan.workflow_run_id,
            plan.workflow_version,
            plan.episode_id,
            plan.authorization_decision_id,
            plan.authorization_decision_version,
            plan.intervention_spec_id,
            plan.intervention_spec_digest,
            plan.decision_id,
            plan.lease_token_id,
            plan.selected_processing_class.value,
            plan.scheduled_at,
            plan.lease_owner_ref,
            plan.heartbeat_deadline,
            plan.work_lease_expires_at,
            plan.policy_version_ref,
            plan.plan_digest,
            now,
        )
        if inserted is not None:
            return _work_item(inserted)
        existing = await conn.fetchrow(
            """
            SELECT *
            FROM registered_work_scheduling_items
            WHERE tenant_id=$1
              AND (
                source_event_id=$2 OR obligation_id=$3
                OR decision_id=$4 OR lease_token_id=$5
              )
            FOR KEY SHARE
            """,
            plan.tenant_id,
            plan.source_event_id,
            plan.obligation_id,
            plan.decision_id,
            plan.lease_token_id,
        )
        if existing is None:
            raise InvariantViolation(
                "WORK_SCHEDULING_DISCOVERY_RACE",
                "schedule-plan conflict disappeared during discovery",
                source_event_id=str(source_event_id),
            )
        item = _work_item(existing)
        self._assert_plan_matches_source(item.plan, source)
        return item

    async def claim_ready_work(
        self,
        conn: asyncpg.Connection,
        *,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
        limit: int,
    ) -> tuple[WorkSchedulingWorkItem, ...]:
        if not worker_id.strip():
            raise ValueError("worker_id must be non-empty")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        rows = await conn.fetch(
            """
            WITH candidates AS (
              SELECT work.id
              FROM registered_work_scheduling_items work
              WHERE (
                (work.status IN ('pending', 'retry_scheduled')
                  AND work.available_at <= $2)
                OR (work.status='processing' AND work.lease_expires_at <= $2)
              )
              ORDER BY
                CASE WHEN work.status='processing' THEN work.lease_expires_at
                     ELSE work.available_at END,
                work.created_at, work.id
              FOR UPDATE OF work SKIP LOCKED
              LIMIT $4
            )
            UPDATE registered_work_scheduling_items work
            SET status='processing',
                attempt_count=work.attempt_count + 1,
                claimed_by=$1,
                claim_token=gen_random_uuid(),
                lease_expires_at=GREATEST($2, work.scheduled_at) + $3::interval,
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

    async def load_claimed_context(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        now: datetime,
    ) -> WorkSchedulingWorkContext:
        row = await self._load_live_claim(
            conn,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            now=now,
        )
        item = _work_item(row)
        source = await self._load_source(
            conn,
            source_event_id=item.source_event_id,
            require_registered_head=True,
            unsupported_returns_none=False,
        )
        if source is None:
            raise AssertionError("supported scheduling source unexpectedly missing")
        self._assert_plan_matches_source(item.plan, source)
        return WorkSchedulingWorkContext(
            work_item=item,
            plan=item.plan,
            obligation=source.obligation,
            task=source.task,
            workflow=source.workflow,
            authorization=source.authorization,
            intervention_spec=source.intervention_spec,
        )

    async def mark_leased(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        eligible_work_version: int,
        leased_work_version: int,
        lease_version: int,
        lease_fence: int,
        now: datetime,
    ) -> WorkSchedulingWorkItem:
        if (
            eligible_work_version != 2
            or leased_work_version != 3
            or lease_version != 1
            or lease_fence != 1
        ):
            raise ValueError(
                "initial scheduling requires Work v2/v3 and Lease v1/fence1"
            )
        row = await self._load_live_claim(
            conn,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            now=now,
        )
        item = _work_item(row)
        source = await self._load_source(
            conn,
            source_event_id=item.source_event_id,
            require_registered_head=False,
            unsupported_returns_none=False,
        )
        if source is None:
            raise AssertionError("supported scheduling source unexpectedly missing")
        self._assert_plan_matches_source(item.plan, source)
        effective_at = max(now, item.plan.scheduled_at)
        if not self._authorization_is_live(source.authorization, now=effective_at):
            raise InvariantViolation(
                "WORK_SCHEDULING_AUTHORIZATION_EXPIRED",
                "expired authorization cannot be acknowledged as leased",
                work_item_id=str(work_item_id),
            )
        if effective_at >= source.obligation.deadline:
            raise InvariantViolation(
                "WORK_SCHEDULING_WORK_EXPIRED",
                "expired Work cannot be acknowledged as leased",
                work_item_id=str(work_item_id),
            )
        await self._require_exact_eligible_and_lease(
            conn,
            plan=item.plan,
            obligation=source.obligation,
        )
        updated = await conn.fetchrow(
            """
            UPDATE registered_work_scheduling_items
            SET status='leased',
                eligible_work_version=$6,
                leased_work_version=$7,
                applied_lease_version=$8,
                applied_lease_fence=$9,
                leased_at=$10,
                claimed_by=NULL, claim_token=NULL, lease_expires_at=NULL,
                last_failure_class=NULL, last_failure_reason=NULL,
                updated_at=$5
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
            eligible_work_version,
            leased_work_version,
            lease_version,
            lease_fence,
            effective_at,
        )
        return self._require_claim_transition(updated, work_item_id)

    async def mark_work_expired(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        work_version: int,
        now: datetime,
        reason: str,
    ) -> WorkSchedulingWorkItem:
        return await self._mark_expired(
            conn,
            status=WorkSchedulingWorkStatus.WORK_EXPIRED,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            work_version=work_version,
            now=now,
            reason=reason,
        )

    async def mark_authorization_expired(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        work_version: int,
        now: datetime,
        reason: str,
    ) -> WorkSchedulingWorkItem:
        return await self._mark_expired(
            conn,
            status=WorkSchedulingWorkStatus.AUTHORIZATION_EXPIRED,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            work_version=work_version,
            now=now,
            reason=reason,
        )

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
    ) -> WorkSchedulingWorkItem:
        if next_attempt_at <= now:
            raise ValueError("next_attempt_at must be after now")
        self._validate_failure(failure_class, failure_reason)
        updated = await conn.fetchrow(
            """
            UPDATE registered_work_scheduling_items
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
        return self._require_claim_transition(updated, work_item_id)

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
    ) -> WorkSchedulingWorkItem:
        self._validate_failure(failure_class, failure_reason)
        updated = await conn.fetchrow(
            """
            UPDATE registered_work_scheduling_items
            SET status='failed_terminal',
                claimed_by=NULL, claim_token=NULL, lease_expires_at=NULL,
                last_failure_class=$6, last_failure_reason=$7, updated_at=$5
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
        return self._require_claim_transition(updated, work_item_id)

    async def _mark_expired(
        self,
        conn: asyncpg.Connection,
        *,
        status: WorkSchedulingWorkStatus,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        work_version: int,
        now: datetime,
        reason: str,
    ) -> WorkSchedulingWorkItem:
        if work_version != 2:
            raise ValueError("initial scheduling expiry requires exact Work version 2")
        if not reason.strip():
            raise ValueError("expiry reason must be non-empty")
        row = await self._load_live_claim(
            conn,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            now=now,
        )
        item = _work_item(row)
        source = await self._load_source(
            conn,
            source_event_id=item.source_event_id,
            require_registered_head=False,
            unsupported_returns_none=False,
        )
        if source is None:
            raise AssertionError("supported scheduling source unexpectedly missing")
        self._assert_plan_matches_source(item.plan, source)
        effective_at = max(now, item.plan.scheduled_at)
        if status is WorkSchedulingWorkStatus.WORK_EXPIRED:
            if effective_at < source.obligation.deadline:
                raise InvariantViolation(
                    "WORK_SCHEDULING_WORK_STILL_LIVE",
                    "live Work cannot be acknowledged as expired",
                    work_item_id=str(work_item_id),
                )
        elif self._authorization_is_live(source.authorization, now=effective_at):
            raise InvariantViolation(
                "WORK_SCHEDULING_AUTHORIZATION_STILL_LIVE",
                "live authorization cannot be acknowledged as expired",
                work_item_id=str(work_item_id),
            )
        await self._require_exact_expired_work(
            conn,
            plan=item.plan,
            work_version=work_version,
        )
        timestamp_column = (
            "work_expired_at"
            if status is WorkSchedulingWorkStatus.WORK_EXPIRED
            else "authorization_expired_at"
        )
        failure_class = status.value
        updated = await conn.fetchrow(
            f"""
            UPDATE registered_work_scheduling_items
            SET status=$6,
                expired_work_version=$7,
                {timestamp_column}=$8,
                claimed_by=NULL, claim_token=NULL, lease_expires_at=NULL,
                last_failure_class=$9, last_failure_reason=$10,
                updated_at=$5
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
            status.value,
            work_version,
            effective_at,
            failure_class,
            reason,
        )
        return self._require_claim_transition(updated, work_item_id)

    async def _load_source(
        self,
        conn: asyncpg.Connection,
        *,
        source_event_id: UUID,
        require_registered_head: bool,
        unsupported_returns_none: bool,
    ) -> _SchedulingSource | None:
        event = await conn.fetchrow(
            """
            SELECT event.*,
                   result.tenant_id AS result_tenant_id,
                   result.writer_id AS result_writer_id,
                   result.command_kind AS result_command_kind,
                   result.status AS result_status,
                   result.object_type AS result_object_type,
                   result.object_id AS result_object_id,
                   result.object_version AS result_object_version,
                   result.result AS command_result_payload
            FROM agency_canonical_events event
            JOIN agency_command_results result
              ON result.id=event.command_result_id
            WHERE event.id=$1
            """,
            source_event_id,
        )
        if event is None:
            raise InvariantViolation(
                "WORK_SCHEDULING_SOURCE_EVENT_MISSING",
                "scheduling requires an existing canonical event",
                source_event_id=str(source_event_id),
            )
        supported = (
            event["writer_id"] == "WorkLedgerApplier"
            and event["object_type"] == "work_obligation"
            and int(event["object_version"]) == 1
            and event["semantic_transition"] == "registered"
        )
        if not supported:
            if unsupported_returns_none:
                return None
            raise InvariantViolation(
                "WORK_SCHEDULING_SOURCE_EVENT_UNSUPPORTED",
                "schedule work no longer references exact registered Work",
                source_event_id=str(source_event_id),
            )
        event_payload = _json(event["event_payload"])
        command_result_payload = _json(event["command_result_payload"])
        if (
            event["result_tenant_id"] != event["tenant_id"]
            or event["result_writer_id"] != event["writer_id"]
            or event["result_command_kind"] != "register_work_obligation"
            or event["result_status"] != "applied"
            or event["result_object_type"] != event["object_type"]
            or event["result_object_id"] != event["object_id"]
            or int(event["result_object_version"]) != 1
            or event_payload.get("command_result_id")
            != str(event["command_result_id"])
            or event_payload.get("writer_id") != event["writer_id"]
            or event_payload.get("object_type") != event["object_type"]
            or event_payload.get("object_id") != str(event["object_id"])
            or int(event_payload.get("object_version", 0)) != 1
            or event_payload.get("semantic_transition")
            != event["semantic_transition"]
            or any(
                event_payload.get(key) != value
                for key, value in command_result_payload.items()
            )
        ):
            raise InvariantViolation(
                "WORK_SCHEDULING_EVENT_RESULT_DRIFT",
                "registered Work event does not match its exact command result",
                source_event_id=str(source_event_id),
            )
        row = await conn.fetchrow(
            """
            SELECT spec.obligation, spec.obligation_digest,
                   spec.command_result_id AS spec_command_result_id,
                   head.current_version AS work_current_version,
                   head.current_state AS work_current_state,
                   version.state AS registration_state,
                   version.transition_kind AS registration_kind,
                   version.transition_payload AS registration_payload,
                   version.command_result_id AS registration_result_id,
                   task_head.current_version AS task_current_version,
                   task_head.current_state AS task_current_state,
                   task_version.snapshot AS task_snapshot,
                   task_version.snapshot_digest AS task_snapshot_digest,
                   workflow_head.current_version AS workflow_current_version,
                   workflow_head.current_state AS workflow_current_state,
                   workflow_version.snapshot AS workflow_snapshot,
                   workflow_version.snapshot_digest AS workflow_snapshot_digest,
                   auth.decision AS authorization_payload,
                   auth.decision_digest AS stored_decision_digest,
                   auth.disposition AS stored_disposition,
                   intervention.spec AS intervention_spec_payload,
                   intervention.spec_id AS intervention_spec_id,
                   intervention.spec_digest AS stored_spec_digest
            FROM work_obligation_specs spec
            JOIN work_obligation_heads head
              ON head.tenant_id=spec.tenant_id
             AND head.obligation_id=spec.obligation_id
            JOIN work_obligation_versions version
              ON version.tenant_id=spec.tenant_id
             AND version.obligation_id=spec.obligation_id
             AND version.aggregate_version=1
            JOIN agency_task_heads task_head
              ON task_head.tenant_id=spec.tenant_id
             AND task_head.task_id=spec.target_object_id
            JOIN agency_task_versions task_version
              ON task_version.tenant_id=task_head.tenant_id
             AND task_version.task_id=task_head.task_id
             AND task_version.aggregate_version=task_head.current_version
            JOIN agency_workflow_run_heads workflow_head
              ON workflow_head.tenant_id=task_head.tenant_id
             AND workflow_head.workflow_run_id=task_head.workflow_run_id
            JOIN agency_workflow_run_versions workflow_version
              ON workflow_version.tenant_id=workflow_head.tenant_id
             AND workflow_version.workflow_run_id=workflow_head.workflow_run_id
             AND workflow_version.aggregate_version=workflow_head.current_version
            JOIN consequential_authorization_decisions auth
              ON auth.tenant_id=task_head.tenant_id
             AND auth.id=(task_version.snapshot->>'authorization_decision_id')::uuid
            JOIN consequential_intervention_specs intervention
              ON intervention.tenant_id=task_head.tenant_id
             AND intervention.spec_digest=task_head.intervention_spec_digest
            WHERE spec.tenant_id=$1 AND spec.obligation_id=$2
              AND spec.command_result_id=$3
              AND spec.target_object_type='task'
            FOR KEY SHARE OF spec, version, task_head, task_version,
              workflow_head, workflow_version, auth, intervention
            """,
            event["tenant_id"],
            event["object_id"],
            event["command_result_id"],
        )
        if row is None:
            raise InvariantViolation(
                "WORK_SCHEDULING_SOURCE_MISSING",
                "registered Work does not resolve to its exact task agency chain",
                source_event_id=str(source_event_id),
            )
        work = WorkObligation.model_validate(_json(row["obligation"]))
        task = TaskSnapshot.model_validate(_json(row["task_snapshot"]))
        workflow = WorkflowRunSnapshot.model_validate(_json(row["workflow_snapshot"]))
        authorization = AuthorizationDecision.model_validate(
            _json(row["authorization_payload"])
        )
        spec = InterventionSpec.model_validate(
            _json(row["intervention_spec_payload"])
        )
        exact = (
            work.tenant_id == event["tenant_id"]
            and work.obligation_id == event["object_id"]
            and work.target_object_type == "task"
            and work.target_object_id == task.task_id
            and work.obligation_digest == row["obligation_digest"]
            and row["spec_command_result_id"] == event["command_result_id"]
            and row["registration_state"] == WorkObligationState.REGISTERED.value
            and row["registration_kind"] == "register"
            and row["registration_result_id"] == event["command_result_id"]
            and _json(row["registration_payload"]) == work.model_dump(mode="json")
            and event_payload["obligation_id"] == str(work.obligation_id)
            and int(event_payload["generation"]) == work.generation
            and int(event_payload["obligation_version"]) == 1
            and event_payload["state"] == WorkObligationState.REGISTERED.value
            and event_payload["obligation_digest"] == work.obligation_digest
            and int(row["task_current_version"]) >= 1
            and task.snapshot_digest == row["task_snapshot_digest"]
            and task.state.value == row["task_current_state"]
            and task.workflow_run_id == workflow.workflow_run_id
            and task.episode_id == workflow.episode_id == spec.episode_id
            and task.intervention_spec_digest
            == workflow.intervention_spec_digest
            == spec.spec_digest
            and task.authorization_decision_id == authorization.decision_id
            and task.authorization_decision_version == 1
            and workflow.authorization_decision_id == authorization.decision_id
            and workflow.authorization_decision_version == 1
            and authorization.tenant_id == work.tenant_id
            and authorization.disposition is AuthorizationDisposition.AUTHORIZED
            and authorization.intervention_spec_digest == spec.spec_digest
            and authorization.decision_digest == row["stored_decision_digest"]
            and row["stored_disposition"] == AuthorizationDisposition.AUTHORIZED.value
            and spec.spec_id == row["intervention_spec_id"]
            and spec.spec_digest == row["stored_spec_digest"]
            and _target_ref(spec) in task.target_grounding_refs
            and work.causal_parent_ref
            == f"task:{task.task_id}:v{int(row['task_current_version'])}"
        )
        if require_registered_head:
            exact = exact and (
                int(row["work_current_version"]) == 1
                and row["work_current_state"] == WorkObligationState.REGISTERED.value
            )
        if not exact:
            raise InvariantViolation(
                "WORK_SCHEDULING_SOURCE_DRIFT",
                "Work, Task, Workflow, authorization, and spec are not exact",
                source_event_id=str(source_event_id),
            )
        return _SchedulingSource(
            event=event,
            obligation=work,
            task=task,
            task_version=int(row["task_current_version"]),
            workflow=workflow,
            workflow_version=int(row["workflow_current_version"]),
            authorization=authorization,
            intervention_spec=spec,
        )

    def _assert_plan_matches_source(
        self,
        plan: WorkSchedulePlan,
        source: _SchedulingSource,
    ) -> None:
        work = source.obligation
        expected = (
            plan.plan_version == 1
            and plan.tenant_id == work.tenant_id
            and plan.source_event_id == source.event["id"]
            and plan.obligation_id == work.obligation_id
            and plan.obligation_generation == work.generation
            and plan.source_obligation_version == 1
            and plan.task_id == source.task.task_id
            and plan.task_version == source.task_version
            and plan.workflow_run_id == source.workflow.workflow_run_id
            and plan.workflow_version == source.workflow_version
            and plan.episode_id == source.task.episode_id
            and plan.authorization_decision_id
            == source.authorization.decision_id
            and plan.authorization_decision_version == 1
            and plan.intervention_spec_id == source.intervention_spec.spec_id
            and plan.intervention_spec_digest
            == source.intervention_spec.spec_digest
            and plan.decision_id
            == _schedule_uuid(
                tenant_id=work.tenant_id,
                obligation_id=work.obligation_id,
                generation=work.generation,
                kind="decision",
            )
            and plan.lease_token_id
            == _schedule_uuid(
                tenant_id=work.tenant_id,
                obligation_id=work.obligation_id,
                generation=work.generation,
                kind="lease",
            )
            and plan.selected_processing_class == work.minimum_processing_class
            and plan.scheduled_at >= work.registered_at
            and plan.lease_owner_ref == _LEASE_OWNER_REF
            and plan.heartbeat_deadline <= plan.work_lease_expires_at
            and plan.work_lease_expires_at <= work.deadline
            and plan.work_lease_expires_at <= source.authorization.expires_at
            and plan.work_lease_expires_at <= source.authorization.authority.expires_at
            and plan.policy_version_ref == _POLICY_VERSION
            and canonical_sha256(_plan_material(plan)) == plan.plan_digest
        )
        if not expected:
            raise InvariantViolation(
                "WORK_SCHEDULING_PLAN_SOURCE_DRIFT",
                "schedule plan no longer matches its exact registered Work source",
                source_event_id=str(source.event["id"]),
            )

    async def _require_exact_eligible_and_lease(
        self,
        conn: asyncpg.Connection,
        *,
        plan: WorkSchedulePlan,
        obligation: WorkObligation,
    ) -> None:
        decision_row = await conn.fetchrow(
            """
            SELECT decision.*, result.writer_id, result.object_type,
                   result.object_id, result.object_version
            FROM work_decisions decision
            JOIN agency_command_results result
              ON result.id=decision.command_result_id
            WHERE decision.tenant_id=$1 AND decision.decision_id=$2
              AND decision.obligation_id=$3
              AND decision.obligation_version=2
            FOR KEY SHARE OF decision, result
            """,
            plan.tenant_id,
            plan.decision_id,
            plan.obligation_id,
        )
        lease_row = await conn.fetchrow(
            """
            SELECT head.*, version.lease_payload,
                   result.writer_id, result.object_type,
                   result.object_id, result.object_version
            FROM work_lease_token_heads head
            JOIN work_lease_token_versions version
              ON version.tenant_id=head.tenant_id
             AND version.lease_token_id=head.lease_token_id
             AND version.aggregate_version=head.current_version
            JOIN agency_command_results result
              ON result.id=version.command_result_id
            WHERE head.tenant_id=$1 AND head.lease_token_id=$2
              AND head.obligation_id=$3
            FOR KEY SHARE OF head, version, result
            """,
            plan.tenant_id,
            plan.lease_token_id,
            plan.obligation_id,
        )
        head = await conn.fetchrow(
            """
            SELECT *
            FROM work_obligation_heads
            WHERE tenant_id=$1 AND obligation_id=$2
            FOR KEY SHARE
            """,
            plan.tenant_id,
            plan.obligation_id,
        )
        if decision_row is None or lease_row is None or head is None:
            raise InvariantViolation(
                "WORK_SCHEDULING_LEASE_MISSING",
                "exact eligible decision and active lease are required",
                work_item_id=str(plan.obligation_id),
            )
        decision = WorkDecision.model_validate(_json(decision_row["decision"]))
        lease = LeaseToken.model_validate(_json(lease_row["lease_payload"]))
        exact = (
            decision.decision_id == plan.decision_id
            and decision.tenant_id == plan.tenant_id
            and decision.obligation_id == plan.obligation_id
            and decision.obligation_generation == plan.obligation_generation
            and decision.from_state is WorkObligationState.REGISTERED
            and decision.to_state is WorkObligationState.ELIGIBLE
            and decision.selected_processing_class
            == plan.selected_processing_class
            and decision.policy_version_ref == plan.policy_version_ref
            and decision.decided_at == plan.scheduled_at
            and decision_row["writer_id"] == "WorkLedgerApplier"
            and decision_row["object_type"] == "work_obligation"
            and decision_row["object_id"] == plan.obligation_id
            and int(decision_row["object_version"]) == 2
            and lease.lease_token_id == plan.lease_token_id
            and lease.tenant_id == plan.tenant_id
            and lease.obligation_id == plan.obligation_id
            and lease.obligation_generation == plan.obligation_generation
            and lease.fence == 1
            and lease.attempt == 1
            and lease.owner_ref == plan.lease_owner_ref
            and lease.state is LeaseState.ACTIVE
            and lease.heartbeat_deadline == plan.heartbeat_deadline
            and lease.expires_at == plan.work_lease_expires_at
            and lease.effect_possible == obligation.effect_possible
            and lease.granted_at == plan.scheduled_at
            and int(lease_row["current_version"]) == 1
            and lease_row["current_state"] == LeaseState.ACTIVE.value
            and int(lease_row["fence"]) == 1
            and int(lease_row["attempt"]) == 1
            and lease_row["writer_id"] == "WorkLedgerApplier"
            and lease_row["object_type"] == "work_obligation"
            and lease_row["object_id"] == plan.obligation_id
            and int(lease_row["object_version"]) == 3
            and int(head["current_version"]) == 3
            and head["current_state"] == WorkObligationState.LEASED.value
            and head["current_lease_token_id"] == plan.lease_token_id
            and int(head["current_fence"]) == 1
            and int(head["attempt_count"]) == 1
        )
        if not exact:
            raise InvariantViolation(
                "WORK_SCHEDULING_LEASE_DRIFT",
                "Work decision and lease do not implement the exact schedule plan",
                obligation_id=str(plan.obligation_id),
            )

    async def _require_exact_expired_work(
        self,
        conn: asyncpg.Connection,
        *,
        plan: WorkSchedulePlan,
        work_version: int,
    ) -> None:
        row = await conn.fetchrow(
            """
            SELECT head.current_version, head.current_state,
                   version.transition_kind, version.transition_payload,
                   result.writer_id, result.object_type,
                   result.object_id, result.object_version
            FROM work_obligation_heads head
            JOIN work_obligation_versions version
              ON version.tenant_id=head.tenant_id
             AND version.obligation_id=head.obligation_id
             AND version.aggregate_version=head.current_version
            JOIN agency_command_results result
              ON result.id=version.command_result_id
            WHERE head.tenant_id=$1 AND head.obligation_id=$2
              AND head.current_version=$3
            FOR KEY SHARE OF head, version, result
            """,
            plan.tenant_id,
            plan.obligation_id,
            work_version,
        )
        if row is None:
            raise InvariantViolation(
                "WORK_SCHEDULING_EXPIRED_WORK_MISSING",
                "queue expiry requires exact canonical expired Work",
                obligation_id=str(plan.obligation_id),
            )
        transition = WorkStateTransition.model_validate(
            _json(row["transition_payload"])
        )
        exact = (
            row["current_state"] == WorkObligationState.EXPIRED.value
            and row["transition_kind"] == "state_transition"
            and transition.tenant_id == plan.tenant_id
            and transition.obligation_id == plan.obligation_id
            and transition.obligation_generation == plan.obligation_generation
            and transition.from_state is WorkObligationState.REGISTERED
            and transition.to_state is WorkObligationState.EXPIRED
            and row["writer_id"] == "WorkLedgerApplier"
            and row["object_type"] == "work_obligation"
            and row["object_id"] == plan.obligation_id
            and int(row["object_version"]) == work_version
        )
        if not exact:
            raise InvariantViolation(
                "WORK_SCHEDULING_EXPIRED_WORK_DRIFT",
                "canonical Work expiry does not match the schedule plan",
                obligation_id=str(plan.obligation_id),
            )

    async def _load_live_claim(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        now: datetime,
    ) -> asyncpg.Record:
        row = await conn.fetchrow(
            """
            SELECT *
            FROM registered_work_scheduling_items
            WHERE tenant_id=$1 AND id=$2 AND status='processing'
              AND claimed_by=$3 AND claim_token=$4
              AND lease_expires_at > $5
            FOR UPDATE
            """,
            tenant_id,
            work_item_id,
            worker_id,
            claim_token,
            now,
        )
        if row is None:
            self._raise_stale_claim(work_item_id)
        return row

    @staticmethod
    def _authorization_is_live(
        authorization: AuthorizationDecision,
        *,
        now: datetime,
    ) -> bool:
        return (
            authorization.disposition is AuthorizationDisposition.AUTHORIZED
            and now < authorization.expires_at
            and authorization.authority.is_live(now)
        )

    @staticmethod
    def _validate_failure(failure_class: str, failure_reason: str) -> None:
        if not failure_class.strip() or not failure_reason.strip():
            raise ValueError("failure class and reason must be non-empty")

    @staticmethod
    def _raise_stale_claim(work_item_id: UUID) -> None:
        raise InvariantViolation(
            "WORK_SCHEDULING_STALE_CLAIM",
            "schedule transition requires the current live fence token",
            work_item_id=str(work_item_id),
        )

    def _require_claim_transition(
        self,
        row: asyncpg.Record | None,
        work_item_id: UUID,
    ) -> WorkSchedulingWorkItem:
        if row is None:
            self._raise_stale_claim(work_item_id)
        return _work_item(row)


__all__ = [
    "WorkSchedulePlan",
    "WorkSchedulingRepo",
    "WorkSchedulingWorkContext",
    "WorkSchedulingWorkItem",
    "WorkSchedulingWorkStatus",
]
