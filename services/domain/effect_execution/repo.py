"""Leased delivery plans for exact effect-capable Work.

This repository freezes a provider-call identity from canonical leased Work.
ExecutionLedgerApplier remains the sole source of effect attempts and receipts;
queue fates are acknowledged only after exact canonical revalidation.
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
    ActionAdapterCapabilities,
    ExecutionReceipt,
    ExternalEffectAttempt,
    ExternalEffectState,
    LeaseState,
    LeaseToken,
    TaskSnapshot,
    TaskState,
    WorkObligation,
    WorkObligationState,
    WorkflowRunSnapshot,
    WorkflowRunState,
)
from lib.contracts.kernel import canonical_sha256
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7


_DISPATCH_WINDOW = timedelta(minutes=5)
_RESOLUTION_MARGIN = timedelta(seconds=30)
_RECONCILIATION_OWNER = "service:effect-reconciler"


class EffectExecutionWorkStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    RETRY_SCHEDULED = "retry_scheduled"
    DISPATCHED = "dispatched"
    PROVIDER_REJECTED = "provider_rejected"
    PROVIDER_FAILED = "provider_failed"
    UNKNOWN = "unknown"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    FAILED_TERMINAL = "failed_terminal"


@dataclass(frozen=True, slots=True)
class EffectExecutionPlan:
    plan_version: int
    tenant_id: UUID
    source_event_id: UUID
    obligation_id: UUID
    obligation_generation: int
    source_obligation_version: int
    lease_token_id: UUID
    lease_version: int
    lease_fence: int
    task_id: UUID
    task_version: int
    workflow_run_id: UUID
    workflow_version: int
    episode_id: UUID
    authorization_decision_id: UUID
    authorization_decision_version: int
    intervention_spec_id: UUID
    intervention_spec_digest: str
    capability_id: UUID
    capability_version: str
    capability_digest: str
    effect_attempt_id: UUID
    effect_lineage_id: UUID
    operation: str
    canonical_request_hash: str
    provider_idempotency_key: str
    target_grounding_refs: tuple[str, ...]
    reserved_at: datetime
    dispatch_deadline: datetime
    reconciliation_owner_ref: str
    compensation_policy_ref: str
    plan_digest: str


@dataclass(frozen=True, slots=True)
class EffectExecutionWorkItem:
    id: UUID
    plan: EffectExecutionPlan
    status: EffectExecutionWorkStatus
    attempt_count: int
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    lease_expires_at: datetime | None
    applied_effect_version: int | None
    execution_receipt_id: UUID | None
    applied_effect_state: ExternalEffectState | None
    outcome_at: datetime | None
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
class EffectExecutionWorkContext:
    work_item: EffectExecutionWorkItem
    plan: EffectExecutionPlan
    obligation: WorkObligation
    lease: LeaseToken
    task: TaskSnapshot
    workflow: WorkflowRunSnapshot
    authorization: AuthorizationDecision
    intervention_spec: InterventionSpec
    capabilities: ActionAdapterCapabilities


@dataclass(frozen=True, slots=True)
class _ExecutionSource:
    event: asyncpg.Record
    obligation: WorkObligation
    lease: LeaseToken
    task: TaskSnapshot
    task_version: int
    workflow: WorkflowRunSnapshot
    workflow_version: int
    authorization: AuthorizationDecision
    intervention_spec: InterventionSpec
    capabilities: ActionAdapterCapabilities


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _effect_uuid(
    *,
    tenant_id: UUID,
    obligation_id: UUID,
    generation: int,
    kind: str,
) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        (
            "fyralis:leased-work-effect:v1:"
            f"{tenant_id}:{obligation_id}:{generation}:{kind}"
        ),
    )


def _compensation_policy_ref(spec: InterventionSpec) -> str:
    return (
        "compensation-policy:"
        + canonical_sha256(
            {
                "reversible": spec.reversible,
                "declaration": spec.compensation_declaration,
            }
        )
    )


def _request_hash(
    *,
    spec: InterventionSpec,
    task: TaskSnapshot,
    authorization: AuthorizationDecision,
) -> str:
    return canonical_sha256(
        {
            "operation": spec.operation,
            "parameters": spec.parameters,
            "target_grounding_refs": task.target_grounding_refs,
            "task_id": str(task.task_id),
            "authorization_decision_id": str(authorization.decision_id),
            "adapter_version": spec.action_adapter_version,
            "capability_digest": spec.action_adapter_capability_digest,
            "canonicalization": "effect-request-v1",
        }
    )


def _plan_material(plan: EffectExecutionPlan) -> dict[str, Any]:
    return {
        "plan_version": plan.plan_version,
        "tenant_id": str(plan.tenant_id),
        "source_event_id": str(plan.source_event_id),
        "obligation_id": str(plan.obligation_id),
        "obligation_generation": plan.obligation_generation,
        "source_obligation_version": plan.source_obligation_version,
        "lease_token_id": str(plan.lease_token_id),
        "lease_version": plan.lease_version,
        "lease_fence": plan.lease_fence,
        "task_id": str(plan.task_id),
        "task_version": plan.task_version,
        "workflow_run_id": str(plan.workflow_run_id),
        "workflow_version": plan.workflow_version,
        "episode_id": str(plan.episode_id),
        "authorization_decision_id": str(plan.authorization_decision_id),
        "authorization_decision_version": plan.authorization_decision_version,
        "intervention_spec_id": str(plan.intervention_spec_id),
        "intervention_spec_digest": plan.intervention_spec_digest,
        "capability_id": str(plan.capability_id),
        "capability_version": plan.capability_version,
        "capability_digest": plan.capability_digest,
        "effect_attempt_id": str(plan.effect_attempt_id),
        "effect_lineage_id": str(plan.effect_lineage_id),
        "operation": plan.operation,
        "canonical_request_hash": plan.canonical_request_hash,
        "provider_idempotency_key": plan.provider_idempotency_key,
        "target_grounding_refs": plan.target_grounding_refs,
        "reserved_at": plan.reserved_at,
        "dispatch_deadline": plan.dispatch_deadline,
        "reconciliation_owner_ref": plan.reconciliation_owner_ref,
        "compensation_policy_ref": plan.compensation_policy_ref,
    }


def _plan(row: asyncpg.Record) -> EffectExecutionPlan:
    plan = EffectExecutionPlan(
        plan_version=int(row["plan_version"]),
        tenant_id=row["tenant_id"],
        source_event_id=row["source_event_id"],
        obligation_id=row["obligation_id"],
        obligation_generation=int(row["obligation_generation"]),
        source_obligation_version=int(row["source_obligation_version"]),
        lease_token_id=row["lease_token_id"],
        lease_version=int(row["lease_version"]),
        lease_fence=int(row["lease_fence"]),
        task_id=row["task_id"],
        task_version=int(row["task_version"]),
        workflow_run_id=row["workflow_run_id"],
        workflow_version=int(row["workflow_version"]),
        episode_id=row["episode_id"],
        authorization_decision_id=row["authorization_decision_id"],
        authorization_decision_version=int(row["authorization_decision_version"]),
        intervention_spec_id=row["intervention_spec_id"],
        intervention_spec_digest=str(row["intervention_spec_digest"]),
        capability_id=row["capability_id"],
        capability_version=str(row["capability_version"]),
        capability_digest=str(row["capability_digest"]),
        effect_attempt_id=row["effect_attempt_id"],
        effect_lineage_id=row["effect_lineage_id"],
        operation=str(row["operation"]),
        canonical_request_hash=str(row["canonical_request_hash"]),
        provider_idempotency_key=str(row["provider_idempotency_key"]),
        target_grounding_refs=tuple(row["target_grounding_refs"]),
        reserved_at=row["reserved_at"],
        dispatch_deadline=row["dispatch_deadline"],
        reconciliation_owner_ref=str(row["reconciliation_owner_ref"]),
        compensation_policy_ref=str(row["compensation_policy_ref"]),
        plan_digest=str(row["plan_digest"]),
    )
    if canonical_sha256(_plan_material(plan)) != plan.plan_digest:
        raise InvariantViolation(
            "EFFECT_EXECUTION_PLAN_DIGEST_DRIFT",
            "stored effect plan no longer matches its canonical digest",
            work_item_id=str(row["id"]),
        )
    return plan


def _work_item(row: asyncpg.Record) -> EffectExecutionWorkItem:
    return EffectExecutionWorkItem(
        id=row["id"],
        plan=_plan(row),
        status=EffectExecutionWorkStatus(str(row["status"])),
        attempt_count=int(row["attempt_count"]),
        available_at=row["available_at"],
        claimed_by=row["claimed_by"],
        claim_token=row["claim_token"],
        lease_expires_at=row["lease_expires_at"],
        applied_effect_version=(
            int(row["applied_effect_version"])
            if row["applied_effect_version"] is not None
            else None
        ),
        execution_receipt_id=row["execution_receipt_id"],
        applied_effect_state=(
            ExternalEffectState(str(row["applied_effect_state"]))
            if row["applied_effect_state"] is not None
            else None
        ),
        outcome_at=row["outcome_at"],
        last_failure_class=row["last_failure_class"],
        last_failure_reason=row["last_failure_reason"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class EffectExecutionRepo:
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
            WHERE event.writer_id='WorkLedgerApplier'
              AND event.object_type='work_obligation'
              AND event.object_version=3
              AND event.semantic_transition='leased'
              AND spec.target_object_type='task'
              AND spec.effect_possible
              AND ($1::uuid IS NULL OR event.tenant_id=$1)
              AND NOT EXISTS (
                SELECT 1 FROM leased_work_effect_execution_items work
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
    ) -> EffectExecutionWorkItem | None:
        source = await self._load_source(
            conn,
            source_event_id=source_event_id,
            require_live_fence=True,
            unsupported_returns_none=True,
            now=now,
        )
        if source is None:
            return None
        reserved_at = max(now, source.lease.granted_at)
        dispatch_limit = min(
            source.lease.expires_at,
            source.authorization.expires_at,
            source.authorization.authority.expires_at,
            source.capabilities.expires_at,
        ) - _RESOLUTION_MARGIN
        if source.capabilities.idempotency_retention_until is not None:
            dispatch_limit = min(
                dispatch_limit,
                source.capabilities.idempotency_retention_until
                - _RESOLUTION_MARGIN,
            )
        dispatch_deadline = min(reserved_at + _DISPATCH_WINDOW, dispatch_limit)
        if dispatch_deadline <= reserved_at:
            raise InvariantViolation(
                "EFFECT_EXECUTION_WINDOW_CLOSED",
                "leased Work has no live dispatch window",
                source_event_id=str(source_event_id),
            )
        effect_attempt_id = _effect_uuid(
            tenant_id=source.obligation.tenant_id,
            obligation_id=source.obligation.obligation_id,
            generation=source.obligation.generation,
            kind="attempt",
        )
        effect_lineage_id = _effect_uuid(
            tenant_id=source.obligation.tenant_id,
            obligation_id=source.obligation.obligation_id,
            generation=source.obligation.generation,
            kind="lineage",
        )
        plan = EffectExecutionPlan(
            plan_version=1,
            tenant_id=source.obligation.tenant_id,
            source_event_id=source_event_id,
            obligation_id=source.obligation.obligation_id,
            obligation_generation=source.obligation.generation,
            source_obligation_version=3,
            lease_token_id=source.lease.lease_token_id,
            lease_version=1,
            lease_fence=source.lease.fence,
            task_id=source.task.task_id,
            task_version=source.task_version,
            workflow_run_id=source.workflow.workflow_run_id,
            workflow_version=source.workflow_version,
            episode_id=source.task.episode_id,
            authorization_decision_id=source.authorization.decision_id,
            authorization_decision_version=1,
            intervention_spec_id=source.intervention_spec.spec_id,
            intervention_spec_digest=source.intervention_spec.spec_digest,
            capability_id=source.capabilities.capability_id,
            capability_version=source.capabilities.capability_version,
            capability_digest=source.capabilities.capability_digest,
            effect_attempt_id=effect_attempt_id,
            effect_lineage_id=effect_lineage_id,
            operation=source.intervention_spec.operation,
            canonical_request_hash=_request_hash(
                spec=source.intervention_spec,
                task=source.task,
                authorization=source.authorization,
            ),
            provider_idempotency_key=f"fyralis-effect-v1:{effect_attempt_id}",
            target_grounding_refs=source.task.target_grounding_refs,
            reserved_at=reserved_at,
            dispatch_deadline=dispatch_deadline,
            reconciliation_owner_ref=_RECONCILIATION_OWNER,
            compensation_policy_ref=_compensation_policy_ref(
                source.intervention_spec
            ),
            plan_digest="",
        )
        plan = replace(
            plan,
            plan_digest=canonical_sha256(_plan_material(plan)),
        )
        inserted = await conn.fetchrow(
            """
            INSERT INTO leased_work_effect_execution_items (
              id, tenant_id, source_event_id, plan_version,
              obligation_id, obligation_generation, source_obligation_version,
              lease_token_id, lease_version, lease_fence,
              task_id, task_version, workflow_run_id, workflow_version,
              episode_id, authorization_decision_id,
              authorization_decision_version, intervention_spec_id,
              intervention_spec_digest, capability_id, capability_version,
              capability_digest, effect_attempt_id, effect_lineage_id,
              operation, canonical_request_hash, provider_idempotency_key,
              target_grounding_refs, reserved_at, dispatch_deadline,
              reconciliation_owner_ref,
              compensation_policy_ref, plan_digest, status,
              available_at, created_at, updated_at
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
              $18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,
              $33,'pending',$34,$34,$34
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
            plan.lease_token_id,
            plan.lease_version,
            plan.lease_fence,
            plan.task_id,
            plan.task_version,
            plan.workflow_run_id,
            plan.workflow_version,
            plan.episode_id,
            plan.authorization_decision_id,
            plan.authorization_decision_version,
            plan.intervention_spec_id,
            plan.intervention_spec_digest,
            plan.capability_id,
            plan.capability_version,
            plan.capability_digest,
            plan.effect_attempt_id,
            plan.effect_lineage_id,
            plan.operation,
            plan.canonical_request_hash,
            plan.provider_idempotency_key,
            list(plan.target_grounding_refs),
            plan.reserved_at,
            plan.dispatch_deadline,
            plan.reconciliation_owner_ref,
            plan.compensation_policy_ref,
            plan.plan_digest,
            now,
        )
        if inserted is not None:
            return _work_item(inserted)
        existing = await conn.fetchrow(
            """
            SELECT *
            FROM leased_work_effect_execution_items
            WHERE tenant_id=$1
              AND (
                source_event_id=$2 OR obligation_id=$3
                OR effect_attempt_id=$4 OR effect_lineage_id=$5
              )
            FOR KEY SHARE
            """,
            plan.tenant_id,
            plan.source_event_id,
            plan.obligation_id,
            plan.effect_attempt_id,
            plan.effect_lineage_id,
        )
        if existing is None:
            raise InvariantViolation(
                "EFFECT_EXECUTION_DISCOVERY_RACE",
                "effect plan conflict disappeared during discovery",
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
    ) -> tuple[EffectExecutionWorkItem, ...]:
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
              FROM leased_work_effect_execution_items work
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
            UPDATE leased_work_effect_execution_items work
            SET status='processing',
                attempt_count=work.attempt_count + 1,
                claimed_by=$1,
                claim_token=gen_random_uuid(),
                lease_expires_at=GREATEST($2, work.reserved_at) + $3::interval,
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
    ) -> EffectExecutionWorkContext:
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
            require_live_fence=True,
            unsupported_returns_none=False,
            now=now,
        )
        if source is None:
            raise AssertionError("supported effect source unexpectedly missing")
        self._assert_plan_matches_source(item.plan, source)
        return EffectExecutionWorkContext(
            work_item=item,
            plan=item.plan,
            obligation=source.obligation,
            lease=source.lease,
            task=source.task,
            workflow=source.workflow,
            authorization=source.authorization,
            intervention_spec=source.intervention_spec,
            capabilities=source.capabilities,
        )

    async def mark_dispatched(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        effect_version: int,
        receipt_id: UUID,
        now: datetime,
    ) -> EffectExecutionWorkItem:
        return await self._mark_effect_fate(
            conn,
            status=EffectExecutionWorkStatus.DISPATCHED,
            allowed_states={ExternalEffectState.SUCCEEDED},
            require_reconciliation=False,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            effect_version=effect_version,
            receipt_id=receipt_id,
            now=now,
        )

    async def mark_provider_rejected(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        effect_version: int,
        receipt_id: UUID,
        now: datetime,
    ) -> EffectExecutionWorkItem:
        return await self._mark_effect_fate(
            conn,
            status=EffectExecutionWorkStatus.PROVIDER_REJECTED,
            allowed_states={ExternalEffectState.REJECTED},
            require_reconciliation=False,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            effect_version=effect_version,
            receipt_id=receipt_id,
            now=now,
        )

    async def mark_provider_failed(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        effect_version: int,
        receipt_id: UUID,
        now: datetime,
    ) -> EffectExecutionWorkItem:
        return await self._mark_effect_fate(
            conn,
            status=EffectExecutionWorkStatus.PROVIDER_FAILED,
            allowed_states={ExternalEffectState.FAILED},
            require_reconciliation=False,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            effect_version=effect_version,
            receipt_id=receipt_id,
            now=now,
        )

    async def mark_unknown(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        effect_version: int,
        receipt_id: UUID,
        now: datetime,
    ) -> EffectExecutionWorkItem:
        return await self._mark_effect_fate(
            conn,
            status=EffectExecutionWorkStatus.UNKNOWN,
            allowed_states={
                ExternalEffectState.UNKNOWN,
                ExternalEffectState.ACKNOWLEDGED,
            },
            require_reconciliation=True,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            effect_version=effect_version,
            receipt_id=receipt_id,
            now=now,
        )

    async def mark_reconciliation_required(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        effect_version: int,
        receipt_id: UUID,
        now: datetime,
    ) -> EffectExecutionWorkItem:
        return await self._mark_effect_fate(
            conn,
            status=EffectExecutionWorkStatus.RECONCILIATION_REQUIRED,
            allowed_states={ExternalEffectState.RECONCILING},
            require_reconciliation=True,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            effect_version=effect_version,
            receipt_id=receipt_id,
            now=now,
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
    ) -> EffectExecutionWorkItem:
        if next_attempt_at <= now:
            raise ValueError("next_attempt_at must be after now")
        self._validate_failure(failure_class, failure_reason)
        updated = await conn.fetchrow(
            """
            UPDATE leased_work_effect_execution_items
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
    ) -> EffectExecutionWorkItem:
        self._validate_failure(failure_class, failure_reason)
        updated = await conn.fetchrow(
            """
            UPDATE leased_work_effect_execution_items
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

    async def _mark_effect_fate(
        self,
        conn: asyncpg.Connection,
        *,
        status: EffectExecutionWorkStatus,
        allowed_states: set[ExternalEffectState],
        require_reconciliation: bool,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        effect_version: int,
        receipt_id: UUID,
        now: datetime,
    ) -> EffectExecutionWorkItem:
        if effect_version < 2:
            raise ValueError("effect fate requires a post-reservation version")
        row = await self._load_live_claim(
            conn,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            now=now,
        )
        item = _work_item(row)
        effect = await self._load_exact_effect(
            conn,
            plan=item.plan,
            effect_version=effect_version,
            receipt_id=receipt_id,
        )
        if effect["state"] not in allowed_states:
            raise InvariantViolation(
                "EFFECT_EXECUTION_FATE_MISMATCH",
                "canonical effect state does not match the requested queue fate",
                work_item_id=str(work_item_id),
                effect_state=effect["state"].value,
            )
        if require_reconciliation:
            work_state = await conn.fetchval(
                """
                SELECT current_state FROM work_obligation_heads
                WHERE tenant_id=$1 AND obligation_id=$2
                """,
                item.plan.tenant_id,
                item.plan.obligation_id,
            )
            if work_state != WorkObligationState.RECONCILIATION_REQUIRED.value:
                raise InvariantViolation(
                    "EFFECT_EXECUTION_RECONCILIATION_MISSING",
                    "unknown effect fate requires canonical Work reconciliation",
                    work_item_id=str(work_item_id),
                )
        updated = await conn.fetchrow(
            """
            UPDATE leased_work_effect_execution_items
            SET status=$6,
                applied_effect_version=$7,
                execution_receipt_id=$8,
                applied_effect_state=$9,
                outcome_at=$10,
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
            status.value,
            effect_version,
            receipt_id,
            effect["state"].value,
            effect["receipt"].observed_at,
        )
        return self._require_claim_transition(updated, work_item_id)

    async def _load_source(
        self,
        conn: asyncpg.Connection,
        *,
        source_event_id: UUID,
        require_live_fence: bool,
        unsupported_returns_none: bool,
        now: datetime,
    ) -> _ExecutionSource | None:
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
                "EFFECT_EXECUTION_SOURCE_EVENT_MISSING",
                "effect work requires an existing canonical event",
                source_event_id=str(source_event_id),
            )
        supported = (
            event["writer_id"] == "WorkLedgerApplier"
            and event["object_type"] == "work_obligation"
            and int(event["object_version"]) == 3
            and event["semantic_transition"] == "leased"
        )
        if not supported:
            if unsupported_returns_none:
                return None
            raise InvariantViolation(
                "EFFECT_EXECUTION_SOURCE_EVENT_UNSUPPORTED",
                "effect work no longer references exact leased Work",
                source_event_id=str(source_event_id),
            )
        event_payload = _json(event["event_payload"])
        result_payload = _json(event["command_result_payload"])
        if (
            event["result_tenant_id"] != event["tenant_id"]
            or event["result_writer_id"] != event["writer_id"]
            or event["result_command_kind"] != "grant_work_lease"
            or event["result_status"] != "applied"
            or event["result_object_type"] != event["object_type"]
            or event["result_object_id"] != event["object_id"]
            or int(event["result_object_version"]) != 3
            or event_payload.get("command_result_id")
            != str(event["command_result_id"])
            or event_payload.get("writer_id") != event["writer_id"]
            or event_payload.get("object_type") != event["object_type"]
            or event_payload.get("object_id") != str(event["object_id"])
            or int(event_payload.get("object_version", 0)) != 3
            or any(
                event_payload.get(key) != value
                for key, value in result_payload.items()
            )
        ):
            raise InvariantViolation(
                "EFFECT_EXECUTION_EVENT_RESULT_DRIFT",
                "leased Work event does not match its exact command result",
                source_event_id=str(source_event_id),
            )
        row = await conn.fetchrow(
            """
            SELECT spec.obligation, spec.obligation_digest,
                   head.current_version AS work_current_version,
                   head.current_state AS work_current_state,
                   head.current_lease_token_id, head.current_fence,
                   leased_version.transition_kind AS leased_transition_kind,
                   leased_version.transition_payload AS leased_payload,
                   leased_version.command_result_id AS leased_result_id,
                   lease_head.current_version AS lease_current_version,
                   lease_head.current_state AS lease_current_state,
                   lease_version.lease_payload,
                   lease_version.command_result_id AS lease_result_id,
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
                   intervention.spec AS intervention_spec_payload,
                   intervention.spec_id AS intervention_spec_id,
                   intervention.spec_digest AS stored_spec_digest,
                   capability_head.current_version AS capability_current_version,
                   capability_head.capability_id AS stored_capability_id,
                   capability_version.capabilities AS capabilities_payload,
                   capability_version.capability_digest AS stored_capability_digest
            FROM work_obligation_specs spec
            JOIN work_obligation_heads head
              ON head.tenant_id=spec.tenant_id
             AND head.obligation_id=spec.obligation_id
            JOIN work_obligation_versions leased_version
              ON leased_version.tenant_id=spec.tenant_id
             AND leased_version.obligation_id=spec.obligation_id
             AND leased_version.aggregate_version=3
            JOIN work_lease_token_heads lease_head
              ON lease_head.tenant_id=spec.tenant_id
             AND lease_head.lease_token_id=
                 (leased_version.transition_payload->>'lease_token_id')::uuid
            JOIN work_lease_token_versions lease_version
              ON lease_version.tenant_id=lease_head.tenant_id
             AND lease_version.lease_token_id=lease_head.lease_token_id
             AND lease_version.aggregate_version=1
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
            JOIN action_adapter_capability_versions capability_version
              ON capability_version.tenant_id=intervention.tenant_id
             AND capability_version.capability_version=
                 intervention.spec->>'action_adapter_version'
             AND capability_version.capability_digest=
                 intervention.spec->>'action_adapter_capability_digest'
            JOIN action_adapter_capability_heads capability_head
              ON capability_head.tenant_id=capability_version.tenant_id
             AND capability_head.capability_id=capability_version.capability_id
             AND capability_head.current_version=
                 capability_version.aggregate_version
            WHERE spec.tenant_id=$1 AND spec.obligation_id=$2
              AND leased_version.command_result_id=$3
              AND spec.target_object_type='task'
              AND spec.effect_possible
            FOR KEY SHARE OF spec, leased_version, lease_head, lease_version,
              task_head, task_version, workflow_head, workflow_version,
              auth, intervention, capability_head, capability_version
            """,
            event["tenant_id"],
            event["object_id"],
            event["command_result_id"],
        )
        if row is None:
            raise InvariantViolation(
                "EFFECT_EXECUTION_SOURCE_MISSING",
                "leased Work does not resolve to its exact execution chain",
                source_event_id=str(source_event_id),
            )
        work = WorkObligation.model_validate(_json(row["obligation"]))
        lease = LeaseToken.model_validate(_json(row["lease_payload"]))
        task = TaskSnapshot.model_validate(_json(row["task_snapshot"]))
        workflow = WorkflowRunSnapshot.model_validate(_json(row["workflow_snapshot"]))
        authorization = AuthorizationDecision.model_validate(
            _json(row["authorization_payload"])
        )
        spec = InterventionSpec.model_validate(
            _json(row["intervention_spec_payload"])
        )
        capabilities = ActionAdapterCapabilities.model_validate(
            _json(row["capabilities_payload"])
        )
        exact = (
            work.tenant_id == event["tenant_id"]
            and work.obligation_id == event["object_id"]
            and work.target_object_type == "task"
            and work.target_object_id == task.task_id
            and work.effect_possible
            and work.obligation_digest == row["obligation_digest"]
            and row["leased_transition_kind"] == "lease_granted"
            and row["leased_result_id"] == event["command_result_id"]
            and lease.lease_token_id
            == UUID(str(event_payload["lease_token_id"]))
            and lease.obligation_id == work.obligation_id
            and lease.obligation_generation == work.generation
            and lease.fence == 1
            and lease.attempt == 1
            and lease.state is LeaseState.ACTIVE
            and _json(row["leased_payload"]) == lease.model_dump(mode="json")
            and row["lease_result_id"] == event["command_result_id"]
            and int(row["lease_current_version"]) == 1
            and task.snapshot_digest == row["task_snapshot_digest"]
            and task.state is TaskState.IN_PROGRESS
            and row["task_current_state"] == TaskState.IN_PROGRESS.value
            and task.external_effect_required
            and task.workflow_run_id == workflow.workflow_run_id
            and work.causal_parent_ref
            == f"task:{task.task_id}:v{int(row['task_current_version'])}"
            and task.episode_id == workflow.episode_id == spec.episode_id
            and task.intervention_spec_digest
            == workflow.intervention_spec_digest
            == spec.spec_digest
            and task.authorization_decision_id == authorization.decision_id
            and workflow.authorization_decision_id == authorization.decision_id
            and workflow.snapshot_digest == row["workflow_snapshot_digest"]
            and workflow.state is WorkflowRunState.ACTIVE
            and row["workflow_current_state"] == WorkflowRunState.ACTIVE.value
            and authorization.disposition is AuthorizationDisposition.AUTHORIZED
            and authorization.intervention_spec_digest == spec.spec_digest
            and authorization.decision_digest == row["stored_decision_digest"]
            and spec.spec_id == row["intervention_spec_id"]
            and spec.spec_digest == row["stored_spec_digest"]
            and spec.operation in authorization.exact_operations
            and set(task.target_grounding_refs)
            <= set(authorization.exact_target_refs)
            and capabilities.capability_digest == row["stored_capability_digest"]
            and capabilities.capability_version == spec.action_adapter_version
            and capabilities.capability_digest
            == spec.action_adapter_capability_digest
            and capabilities.capability_id == row["stored_capability_id"]
            and spec.operation in capabilities.permitted_operations
            and capabilities.autonomous_repeat_safe
        )
        if require_live_fence:
            live_at = max(now, lease.granted_at)
            exact = exact and (
                int(row["work_current_version"]) == 3
                and row["work_current_state"] == WorkObligationState.LEASED.value
                and row["current_lease_token_id"] == lease.lease_token_id
                and int(row["current_fence"]) == lease.fence
                and row["lease_current_state"] == LeaseState.ACTIVE.value
                and live_at < lease.expires_at
                and self._authorization_is_live(authorization, now=live_at)
                and live_at < capabilities.expires_at
            )
        if not exact:
            raise InvariantViolation(
                "EFFECT_EXECUTION_SOURCE_DRIFT",
                "Work, lease, task, authorization, spec, and capability are not exact",
                source_event_id=str(source_event_id),
            )
        return _ExecutionSource(
            event=event,
            obligation=work,
            lease=lease,
            task=task,
            task_version=int(row["task_current_version"]),
            workflow=workflow,
            workflow_version=int(row["workflow_current_version"]),
            authorization=authorization,
            intervention_spec=spec,
            capabilities=capabilities,
        )

    def _assert_plan_matches_source(
        self,
        plan: EffectExecutionPlan,
        source: _ExecutionSource,
    ) -> None:
        work = source.obligation
        expected_request_hash = _request_hash(
            spec=source.intervention_spec,
            task=source.task,
            authorization=source.authorization,
        )
        expected = (
            plan.plan_version == 1
            and plan.tenant_id == work.tenant_id
            and plan.source_event_id == source.event["id"]
            and plan.obligation_id == work.obligation_id
            and plan.obligation_generation == work.generation
            and plan.source_obligation_version == 3
            and plan.lease_token_id == source.lease.lease_token_id
            and plan.lease_version == 1
            and plan.lease_fence == source.lease.fence == 1
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
            and plan.capability_id == source.capabilities.capability_id
            and plan.capability_version
            == source.capabilities.capability_version
            and plan.capability_digest == source.capabilities.capability_digest
            and plan.effect_attempt_id
            == _effect_uuid(
                tenant_id=work.tenant_id,
                obligation_id=work.obligation_id,
                generation=work.generation,
                kind="attempt",
            )
            and plan.effect_lineage_id
            == _effect_uuid(
                tenant_id=work.tenant_id,
                obligation_id=work.obligation_id,
                generation=work.generation,
                kind="lineage",
            )
            and plan.operation == source.intervention_spec.operation
            and plan.canonical_request_hash == expected_request_hash
            and plan.provider_idempotency_key
            == f"fyralis-effect-v1:{plan.effect_attempt_id}"
            and plan.target_grounding_refs == source.task.target_grounding_refs
            and plan.reserved_at >= source.lease.granted_at
            and plan.dispatch_deadline <= source.lease.expires_at
            and plan.dispatch_deadline <= source.authorization.expires_at
            and plan.dispatch_deadline
            <= source.authorization.authority.expires_at
            and plan.dispatch_deadline <= source.capabilities.expires_at
            and plan.reconciliation_owner_ref == _RECONCILIATION_OWNER
            and plan.compensation_policy_ref
            == _compensation_policy_ref(source.intervention_spec)
            and canonical_sha256(_plan_material(plan)) == plan.plan_digest
        )
        if not expected:
            raise InvariantViolation(
                "EFFECT_EXECUTION_PLAN_SOURCE_DRIFT",
                "effect plan no longer matches exact leased Work",
                source_event_id=str(source.event["id"]),
            )

    async def _load_exact_effect(
        self,
        conn: asyncpg.Connection,
        *,
        plan: EffectExecutionPlan,
        effect_version: int,
        receipt_id: UUID,
    ) -> dict[str, Any]:
        row = await conn.fetchrow(
            """
            SELECT head.*, reserved.attempt_payload AS reserved_payload,
                   current.attempt_payload AS current_payload,
                   receipt.receipt, receipt.effect_version,
                   receipt.effect_state, receipt.observed_at,
                   result.writer_id, result.object_type,
                   result.object_id, result.object_version
            FROM external_effect_attempt_heads head
            JOIN external_effect_attempt_versions reserved
              ON reserved.tenant_id=head.tenant_id
             AND reserved.effect_attempt_id=head.effect_attempt_id
             AND reserved.aggregate_version=1
            JOIN external_effect_attempt_versions current
              ON current.tenant_id=head.tenant_id
             AND current.effect_attempt_id=head.effect_attempt_id
             AND current.aggregate_version=head.current_version
            JOIN execution_receipts receipt
              ON receipt.tenant_id=head.tenant_id
             AND receipt.effect_attempt_id=head.effect_attempt_id
             AND receipt.effect_version=head.current_version
             AND receipt.receipt_id=$4
            JOIN agency_command_results result
              ON result.id=current.command_result_id
            WHERE head.tenant_id=$1 AND head.effect_attempt_id=$2
              AND head.current_version=$3
            FOR KEY SHARE OF head, reserved, current, receipt, result
            """,
            plan.tenant_id,
            plan.effect_attempt_id,
            effect_version,
            receipt_id,
        )
        if row is None:
            raise InvariantViolation(
                "EFFECT_EXECUTION_CANONICAL_FATE_MISSING",
                "queue fate requires exact current effect and receipt",
                effect_attempt_id=str(plan.effect_attempt_id),
            )
        attempt = ExternalEffectAttempt.model_validate(
            _json(row["reserved_payload"])
        )
        receipt = ExecutionReceipt.model_validate(_json(row["receipt"]))
        state = ExternalEffectState(str(row["current_state"]))
        exact = (
            attempt.effect_attempt_id == plan.effect_attempt_id
            and attempt.lineage_id == plan.effect_lineage_id
            and attempt.generation == 1
            and attempt.tenant_id == plan.tenant_id
            and attempt.episode_id == plan.episode_id
            and attempt.task_id == plan.task_id
            and attempt.intervention_spec_digest
            == plan.intervention_spec_digest
            and attempt.authorization_decision_id
            == plan.authorization_decision_id
            and attempt.authorization_decision_version == 1
            and attempt.capability_id == plan.capability_id
            and attempt.capability_version == plan.capability_version
            and attempt.capability_digest == plan.capability_digest
            and attempt.operation == plan.operation
            and attempt.canonical_request_hash == plan.canonical_request_hash
            and attempt.provider_idempotency_key
            == plan.provider_idempotency_key
            and attempt.target_grounding_refs == plan.target_grounding_refs
            and bool(attempt.live_precondition_refs)
            and attempt.work_obligation_id == plan.obligation_id
            and attempt.work_obligation_generation
            == plan.obligation_generation
            and attempt.lease_token_id == plan.lease_token_id
            and attempt.lease_fence == plan.lease_fence
            and attempt.dispatch_deadline == plan.dispatch_deadline
            and attempt.reconciliation_owner_ref
            == plan.reconciliation_owner_ref
            and attempt.compensation_policy_ref
            == plan.compensation_policy_ref
            and attempt.reserved_at == plan.reserved_at
            and receipt.receipt_id == receipt_id
            and receipt.effect_attempt_id == plan.effect_attempt_id
            and receipt.effect_version == effect_version
            and receipt.effect_state is state
            and receipt.canonical_request_hash == plan.canonical_request_hash
            and receipt.provider_idempotency_key
            == plan.provider_idempotency_key
            and row["effect_state"] == state.value
            and row["writer_id"] == "ExecutionLedgerApplier"
            and row["object_type"] == "external_effect_attempt"
            and row["object_id"] == plan.effect_attempt_id
            and int(row["object_version"]) == effect_version
        )
        if not exact:
            raise InvariantViolation(
                "EFFECT_EXECUTION_CANONICAL_FATE_DRIFT",
                "effect head and receipt do not match the frozen execution plan",
                effect_attempt_id=str(plan.effect_attempt_id),
            )
        return {"state": state, "receipt": receipt}

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
            FROM leased_work_effect_execution_items
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
            "EFFECT_EXECUTION_STALE_CLAIM",
            "effect transition requires the current live fence token",
            work_item_id=str(work_item_id),
        )

    def _require_claim_transition(
        self,
        row: asyncpg.Record | None,
        work_item_id: UUID,
    ) -> EffectExecutionWorkItem:
        if row is None:
            self._raise_stale_claim(work_item_id)
        return _work_item(row)


__all__ = [
    "EffectExecutionPlan",
    "EffectExecutionRepo",
    "EffectExecutionWorkContext",
    "EffectExecutionWorkItem",
    "EffectExecutionWorkStatus",
]
