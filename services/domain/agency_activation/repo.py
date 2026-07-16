"""Leased persistence for deterministic authorized-agency activation plans.

This boundary consumes one exact, immutable version-one authorization event.
It freezes the identities and timing required to create a planned WorkflowRun
and Task, but it never interprets a proposal as authority and never widens the
scope of the authorization it was given.
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
    ConsequentialProposal,
    ConsequentialProposalFate,
    InterventionSpec,
)
from lib.contracts.execution import (
    TaskSnapshot,
    TaskState,
    WorkflowRunSnapshot,
    WorkflowRunState,
)
from lib.contracts.kernel import canonical_sha256
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7


class AgencyActivationWorkStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    RETRY_SCHEDULED = "retry_scheduled"
    ACTIVATED = "activated"
    AUTHORIZATION_EXPIRED = "authorization_expired"
    FAILED_TERMINAL = "failed_terminal"


@dataclass(frozen=True, slots=True)
class AgencyActivationPlan:
    plan_version: int
    tenant_id: UUID
    source_event_id: UUID
    authorization_decision_id: UUID
    authorization_decision_version: int
    proposal_id: UUID
    proposal_version: int
    proposal_digest: str
    episode_id: UUID
    intervention_spec_id: UUID
    intervention_spec_digest: str
    workflow_run_id: UUID
    task_id: UUID
    activation_at: datetime
    workflow_spec_version_ref: str
    exact_target_ref: str
    plan_digest: str


@dataclass(frozen=True, slots=True)
class AgencyActivationWorkItem:
    id: UUID
    plan: AgencyActivationPlan
    status: AgencyActivationWorkStatus
    attempt_count: int
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    lease_expires_at: datetime | None
    activated_workflow_version: int | None
    activated_task_version: int | None
    activated_at: datetime | None
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
    def authorization_decision_id(self) -> UUID:
        return self.plan.authorization_decision_id

    @property
    def episode_id(self) -> UUID:
        return self.plan.episode_id

    @property
    def intervention_spec_digest(self) -> str:
        return self.plan.intervention_spec_digest

    @property
    def workflow_run_id(self) -> UUID:
        return self.plan.workflow_run_id

    @property
    def task_id(self) -> UUID:
        return self.plan.task_id


@dataclass(frozen=True, slots=True)
class AgencyActivationWorkContext:
    work_item: AgencyActivationWorkItem
    plan: AgencyActivationPlan
    authorization: AuthorizationDecision
    proposal: ConsequentialProposal
    intervention_spec: InterventionSpec


@dataclass(frozen=True, slots=True)
class _ActivationSource:
    event: asyncpg.Record
    authorization: AuthorizationDecision
    proposal: ConsequentialProposal
    intervention_spec: InterventionSpec


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _target_ref(spec: InterventionSpec) -> str:
    target = spec.target_referent
    return f"referent:{target.referent_id}:v{target.referent_version}"


def _activation_uuid(
    *,
    tenant_id: UUID,
    authorization_decision_id: UUID,
    kind: str,
) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        (
            "fyralis:authorized-agency-activation:v1:"
            f"{tenant_id}:{authorization_decision_id}:{kind}"
        ),
    )


def _plan_material(plan: AgencyActivationPlan) -> dict[str, Any]:
    return {
        "plan_version": plan.plan_version,
        "tenant_id": str(plan.tenant_id),
        "source_event_id": str(plan.source_event_id),
        "authorization_decision_id": str(plan.authorization_decision_id),
        "authorization_decision_version": plan.authorization_decision_version,
        "proposal_id": str(plan.proposal_id),
        "proposal_version": plan.proposal_version,
        "proposal_digest": plan.proposal_digest,
        "episode_id": str(plan.episode_id),
        "intervention_spec_id": str(plan.intervention_spec_id),
        "intervention_spec_digest": plan.intervention_spec_digest,
        "workflow_run_id": str(plan.workflow_run_id),
        "task_id": str(plan.task_id),
        "activation_at": plan.activation_at,
        "workflow_spec_version_ref": plan.workflow_spec_version_ref,
        "exact_target_ref": plan.exact_target_ref,
    }


def _plan(row: asyncpg.Record) -> AgencyActivationPlan:
    plan = AgencyActivationPlan(
        plan_version=int(row["plan_version"]),
        tenant_id=row["tenant_id"],
        source_event_id=row["source_event_id"],
        authorization_decision_id=row["authorization_decision_id"],
        authorization_decision_version=int(row["authorization_decision_version"]),
        proposal_id=row["proposal_id"],
        proposal_version=int(row["proposal_version"]),
        proposal_digest=str(row["proposal_digest"]),
        episode_id=row["episode_id"],
        intervention_spec_id=row["intervention_spec_id"],
        intervention_spec_digest=str(row["intervention_spec_digest"]),
        workflow_run_id=row["workflow_run_id"],
        task_id=row["task_id"],
        activation_at=row["activation_at"],
        workflow_spec_version_ref=str(row["workflow_spec_version_ref"]),
        exact_target_ref=str(row["exact_target_ref"]),
        plan_digest=str(row["plan_digest"]),
    )
    if canonical_sha256(_plan_material(plan)) != plan.plan_digest:
        raise InvariantViolation(
            "AGENCY_ACTIVATION_PLAN_DIGEST_DRIFT",
            "stored activation plan no longer matches its canonical digest",
            work_item_id=str(row["id"]),
        )
    return plan


def _work_item(row: asyncpg.Record) -> AgencyActivationWorkItem:
    return AgencyActivationWorkItem(
        id=row["id"],
        plan=_plan(row),
        status=AgencyActivationWorkStatus(str(row["status"])),
        attempt_count=int(row["attempt_count"]),
        available_at=row["available_at"],
        claimed_by=row["claimed_by"],
        claim_token=row["claim_token"],
        lease_expires_at=row["lease_expires_at"],
        activated_workflow_version=(
            int(row["activated_workflow_version"])
            if row["activated_workflow_version"] is not None
            else None
        ),
        activated_task_version=(
            int(row["activated_task_version"])
            if row["activated_task_version"] is not None
            else None
        ),
        activated_at=row["activated_at"],
        authorization_expired_at=row["authorization_expired_at"],
        last_failure_class=row["last_failure_class"],
        last_failure_reason=row["last_failure_reason"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class AgencyActivationRepo:
    """Discover, lease, revalidate, and terminalize activation work."""

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
            WHERE event.writer_id='AuthorizationApplier'
              AND event.object_type='authorization_decision'
              AND event.object_version=1
              AND event.semantic_transition='authorization_authorized'
              AND ($1::uuid IS NULL OR event.tenant_id=$1)
              AND NOT EXISTS (
                SELECT 1
                FROM authorized_agency_activation_work_items work
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
    ) -> AgencyActivationWorkItem | None:
        """Freeze one deterministic plan from an exact authorization event."""

        source = await self._load_source(
            conn,
            source_event_id=source_event_id,
            unsupported_returns_none=True,
        )
        if source is None:
            return None
        if source.authorization.disposition is AuthorizationDisposition.REJECTED:
            return None
        spec = source.intervention_spec
        workflow_ref = spec.workflow_spec_version_ref
        if workflow_ref is None or not workflow_ref.strip():
            raise InvariantViolation(
                "AGENCY_ACTIVATION_WORKFLOW_SPEC_MISSING",
                "authorized activation requires an exact workflow spec version",
                authorization_decision_id=str(source.authorization.decision_id),
            )
        activation_at = max(now, source.authorization.decided_at)
        plan = AgencyActivationPlan(
            plan_version=1,
            tenant_id=source.authorization.tenant_id,
            source_event_id=source_event_id,
            authorization_decision_id=source.authorization.decision_id,
            authorization_decision_version=1,
            proposal_id=source.proposal.proposal_id,
            proposal_version=source.proposal.proposal_version,
            proposal_digest=source.proposal.proposal_digest,
            episode_id=source.proposal.episode_id,
            intervention_spec_id=spec.spec_id,
            intervention_spec_digest=spec.spec_digest,
            workflow_run_id=_activation_uuid(
                tenant_id=source.authorization.tenant_id,
                authorization_decision_id=source.authorization.decision_id,
                kind="workflow",
            ),
            task_id=_activation_uuid(
                tenant_id=source.authorization.tenant_id,
                authorization_decision_id=source.authorization.decision_id,
                kind="task",
            ),
            activation_at=activation_at,
            workflow_spec_version_ref=workflow_ref,
            exact_target_ref=_target_ref(spec),
            plan_digest="",
        )
        plan = replace(
            plan,
            plan_digest=canonical_sha256(_plan_material(plan)),
        )
        inserted = await conn.fetchrow(
            """
            INSERT INTO authorized_agency_activation_work_items (
              id, tenant_id, source_event_id, plan_version,
              authorization_decision_id, authorization_decision_version,
              proposal_id, proposal_version, proposal_digest, episode_id,
              intervention_spec_id, intervention_spec_digest,
              workflow_run_id, task_id, activation_at,
              workflow_spec_version_ref, exact_target_ref, plan_digest,
              status, available_at, created_at, updated_at
            ) VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
              'pending',$19,$19,$19
            )
            ON CONFLICT DO NOTHING
            RETURNING *
            """,
            uuid7(),
            plan.tenant_id,
            plan.source_event_id,
            plan.plan_version,
            plan.authorization_decision_id,
            plan.authorization_decision_version,
            plan.proposal_id,
            plan.proposal_version,
            plan.proposal_digest,
            plan.episode_id,
            plan.intervention_spec_id,
            plan.intervention_spec_digest,
            plan.workflow_run_id,
            plan.task_id,
            plan.activation_at,
            plan.workflow_spec_version_ref,
            plan.exact_target_ref,
            plan.plan_digest,
            now,
        )
        if inserted is not None:
            return _work_item(inserted)
        existing = await conn.fetchrow(
            """
            SELECT *
            FROM authorized_agency_activation_work_items
            WHERE tenant_id=$1
              AND (
                source_event_id=$2
                OR authorization_decision_id=$3
                OR workflow_run_id=$4
                OR task_id=$5
              )
            FOR KEY SHARE
            """,
            plan.tenant_id,
            plan.source_event_id,
            plan.authorization_decision_id,
            plan.workflow_run_id,
            plan.task_id,
        )
        if existing is None:
            raise InvariantViolation(
                "AGENCY_ACTIVATION_DISCOVERY_RACE",
                "activation work conflict disappeared during idempotent discovery",
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
    ) -> tuple[AgencyActivationWorkItem, ...]:
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
              FROM authorized_agency_activation_work_items work
              WHERE (
                (work.status IN ('pending', 'retry_scheduled')
                  AND work.available_at <= $2)
                OR (work.status='processing' AND work.lease_expires_at <= $2)
              )
              ORDER BY
                CASE WHEN work.status='processing' THEN work.lease_expires_at
                     ELSE work.available_at END,
                work.created_at,
                work.id
              FOR UPDATE OF work SKIP LOCKED
              LIMIT $4
            )
            UPDATE authorized_agency_activation_work_items work
            SET status='processing',
                attempt_count=work.attempt_count + 1,
                claimed_by=$1,
                claim_token=gen_random_uuid(),
                lease_expires_at=GREATEST($2, work.activation_at) + $3::interval,
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
    ) -> AgencyActivationWorkContext:
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
            unsupported_returns_none=False,
        )
        if source is None:
            raise AssertionError("supported activation source unexpectedly missing")
        self._assert_plan_matches_source(item.plan, source)
        return AgencyActivationWorkContext(
            work_item=item,
            plan=item.plan,
            authorization=source.authorization,
            proposal=source.proposal,
            intervention_spec=source.intervention_spec,
        )

    async def mark_activated(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        workflow_version: int,
        task_version: int,
        now: datetime,
    ) -> AgencyActivationWorkItem:
        """Terminalize only after exact planned workflow and task versions exist."""

        if workflow_version != 1 or task_version != 1:
            raise ValueError("activation requires exact initial workflow/task versions")
        context = await self.load_claimed_context(
            conn,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            now=now,
        )
        activation_time = max(now, context.plan.activation_at)
        if not self._authorization_is_live(
            context.authorization,
            now=activation_time,
        ):
            raise InvariantViolation(
                "AGENCY_ACTIVATION_AUTHORIZATION_EXPIRED",
                "expired authorization cannot be marked activated",
                work_item_id=str(work_item_id),
            )
        await self._require_exact_planned_agency(
            conn,
            plan=context.plan,
            intervention_spec=context.intervention_spec,
            workflow_version=workflow_version,
            task_version=task_version,
        )
        updated = await conn.fetchrow(
            """
            UPDATE authorized_agency_activation_work_items
            SET status='activated',
                activated_workflow_version=$6,
                activated_task_version=$7,
                activated_at=$8,
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
            workflow_version,
            task_version,
            activation_time,
        )
        return self._require_claim_transition(updated, work_item_id)

    async def mark_authorization_expired(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        work_item_id: UUID,
        worker_id: str,
        claim_token: UUID,
        now: datetime,
        reason: str,
    ) -> AgencyActivationWorkItem:
        """Record the explicit non-activation fate of an expired authorization."""

        if not reason.strip():
            raise ValueError("authorization expiry reason must be non-empty")
        context = await self.load_claimed_context(
            conn,
            tenant_id=tenant_id,
            work_item_id=work_item_id,
            worker_id=worker_id,
            claim_token=claim_token,
            now=now,
        )
        expiry_check_at = max(now, context.plan.activation_at)
        if self._authorization_is_live(
            context.authorization,
            now=expiry_check_at,
        ):
            raise InvariantViolation(
                "AGENCY_ACTIVATION_AUTHORIZATION_STILL_LIVE",
                "a live authorization cannot be terminalized as expired",
                work_item_id=str(work_item_id),
            )
        existing = await conn.fetchval(
            """
            SELECT EXISTS (
              SELECT 1
              FROM agency_workflow_run_heads
              WHERE tenant_id=$1 AND workflow_run_id=$2
              UNION ALL
              SELECT 1
              FROM agency_task_heads
              WHERE tenant_id=$1 AND task_id=$3
            )
            """,
            tenant_id,
            context.plan.workflow_run_id,
            context.plan.task_id,
        )
        if existing:
            raise InvariantViolation(
                "AGENCY_ACTIVATION_EXPIRED_WITH_AGENCY",
                "expired activation already has a workflow or task and needs reconciliation",
                work_item_id=str(work_item_id),
            )
        updated = await conn.fetchrow(
            """
            UPDATE authorized_agency_activation_work_items
            SET status='authorization_expired',
                authorization_expired_at=$6,
                claimed_by=NULL, claim_token=NULL, lease_expires_at=NULL,
                last_failure_class='authorization_expired',
                last_failure_reason=$7,
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
            expiry_check_at,
            reason,
        )
        return self._require_claim_transition(updated, work_item_id)

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
    ) -> AgencyActivationWorkItem:
        if next_attempt_at <= now:
            raise ValueError("next_attempt_at must be after now")
        self._validate_failure(failure_class, failure_reason)
        updated = await conn.fetchrow(
            """
            UPDATE authorized_agency_activation_work_items
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
    ) -> AgencyActivationWorkItem:
        self._validate_failure(failure_class, failure_reason)
        updated = await conn.fetchrow(
            """
            UPDATE authorized_agency_activation_work_items
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

    async def _load_source(
        self,
        conn: asyncpg.Connection,
        *,
        source_event_id: UUID,
        unsupported_returns_none: bool,
    ) -> _ActivationSource | None:
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
                "AGENCY_ACTIVATION_SOURCE_EVENT_MISSING",
                "activation work requires an existing canonical event",
                source_event_id=str(source_event_id),
            )
        supported = (
            event["writer_id"] == "AuthorizationApplier"
            and event["object_type"] == "authorization_decision"
            and int(event["object_version"]) == 1
        )
        if not supported:
            if unsupported_returns_none:
                return None
            raise InvariantViolation(
                "AGENCY_ACTIVATION_SOURCE_EVENT_UNSUPPORTED",
                "activation work no longer references a version-one authorization event",
                source_event_id=str(source_event_id),
            )
        if (
            event["result_tenant_id"] != event["tenant_id"]
            or event["result_writer_id"] != event["writer_id"]
            or event["result_command_kind"] != "apply_authorization_decision"
            or event["result_status"] != "applied"
            or event["result_object_type"] != event["object_type"]
            or event["result_object_id"] != event["object_id"]
            or int(event["result_object_version"]) != int(event["object_version"])
        ):
            raise InvariantViolation(
                "AGENCY_ACTIVATION_EVENT_RESULT_DRIFT",
                "canonical authorization event does not match its exact command result",
                source_event_id=str(source_event_id),
            )
        row = await conn.fetchrow(
            """
            SELECT auth.*,
                   proposal.proposal AS proposal_payload,
                   proposal.proposal_version AS stored_proposal_version,
                   proposal.proposal_digest AS stored_proposal_digest,
                   proposal.episode_id AS proposal_episode_id,
                   proposal.intervention_spec_id,
                   proposal.intervention_spec_digest AS proposal_spec_digest,
                   proposal.current_fate AS proposal_current_fate,
                   spec.spec AS spec_payload,
                   spec.spec_digest AS stored_spec_digest,
                   spec.episode_id AS spec_episode_id
            FROM consequential_authorization_decisions auth
            JOIN consequential_proposals proposal
              ON proposal.tenant_id=auth.tenant_id
             AND proposal.id=auth.proposal_id
             AND proposal.proposal_version=auth.proposal_version
            JOIN consequential_intervention_specs spec
              ON spec.tenant_id=proposal.tenant_id
             AND spec.spec_id=proposal.intervention_spec_id
            WHERE auth.tenant_id=$1
              AND auth.id=$2
              AND auth.command_result_id=$3
            FOR KEY SHARE OF auth, proposal, spec
            """,
            event["tenant_id"],
            event["object_id"],
            event["command_result_id"],
        )
        if row is None:
            raise InvariantViolation(
                "AGENCY_ACTIVATION_SOURCE_MISSING",
                "authorization event does not resolve to its exact authorization chain",
                source_event_id=str(source_event_id),
            )
        authorization = AuthorizationDecision.model_validate(_json(row["decision"]))
        proposal = ConsequentialProposal.model_validate(_json(row["proposal_payload"]))
        spec = InterventionSpec.model_validate(_json(row["spec_payload"]))
        event_payload = _json(event["event_payload"])
        expected_transition = f"authorization_{authorization.disposition.value}"
        result_payload = {
            "decision_id": str(authorization.decision_id),
            "proposal_id": str(authorization.proposal_id),
            "decision_digest": authorization.decision_digest,
            "intervention_spec_digest": authorization.intervention_spec_digest,
            "disposition": authorization.disposition.value,
        }
        expected_payload = {
            "command_result_id": str(event["command_result_id"]),
            "writer_id": "AuthorizationApplier",
            "object_type": "authorization_decision",
            "object_id": str(authorization.decision_id),
            "object_version": 1,
            "semantic_transition": expected_transition,
            **result_payload,
        }
        expected_fields = {f"parameters.{name}" for name in spec.parameters}
        exact = (
            authorization.decision_id == event["object_id"]
            and authorization.tenant_id == event["tenant_id"]
            and row["decision_digest"] == authorization.decision_digest
            and row["proposal_id"] == authorization.proposal_id
            and int(row["proposal_version"]) == proposal.proposal_version
            and row["proposal_digest"] == proposal.proposal_digest
            and row["intervention_spec_digest"] == spec.spec_digest
            and row["episode_id"] == proposal.episode_id == spec.episode_id
            and int(row["stored_proposal_version"]) == proposal.proposal_version
            and row["stored_proposal_digest"] == proposal.proposal_digest
            and row["proposal_episode_id"] == proposal.episode_id
            and row["proposal_spec_digest"] == spec.spec_digest
            and row["stored_spec_digest"] == spec.spec_digest
            and row["spec_episode_id"] == spec.episode_id
            and row["intervention_spec_id"] == spec.spec_id
            and proposal.intervention_spec.spec_digest == spec.spec_digest
            and proposal.intervention_spec.spec_id == spec.spec_id
            and row["proposal_current_fate"]
            == ConsequentialProposalFate.ACCEPTED_FOR_AUTHORIZATION.value
            and event["intervention_spec_digest"] == spec.spec_digest
            and event["semantic_transition"] == expected_transition
            and event_payload == expected_payload
            and _json(event["command_result_payload"]) == result_payload
            and authorization.intervention_spec_digest == spec.spec_digest
            and spec.operation in authorization.exact_operations
            and _target_ref(spec) in authorization.exact_target_refs
            and expected_fields <= authorization.exact_field_paths
        )
        if not exact:
            raise InvariantViolation(
                "AGENCY_ACTIVATION_SOURCE_DRIFT",
                "authorization, proposal, and InterventionSpec are not exact",
                source_event_id=str(source_event_id),
            )
        return _ActivationSource(
            event=event,
            authorization=authorization,
            proposal=proposal,
            intervention_spec=spec,
        )

    def _assert_plan_matches_source(
        self,
        plan: AgencyActivationPlan,
        source: _ActivationSource,
    ) -> None:
        authorization = source.authorization
        proposal = source.proposal
        spec = source.intervention_spec
        expected = (
            plan.plan_version == 1
            and plan.tenant_id == authorization.tenant_id
            and plan.source_event_id == source.event["id"]
            and plan.authorization_decision_id == authorization.decision_id
            and plan.authorization_decision_version == 1
            and plan.proposal_id == proposal.proposal_id
            and plan.proposal_version == proposal.proposal_version
            and plan.proposal_digest == proposal.proposal_digest
            and plan.episode_id == proposal.episode_id
            and plan.intervention_spec_id == spec.spec_id
            and plan.intervention_spec_digest == spec.spec_digest
            and plan.workflow_run_id
            == _activation_uuid(
                tenant_id=authorization.tenant_id,
                authorization_decision_id=authorization.decision_id,
                kind="workflow",
            )
            and plan.task_id
            == _activation_uuid(
                tenant_id=authorization.tenant_id,
                authorization_decision_id=authorization.decision_id,
                kind="task",
            )
            and plan.activation_at >= authorization.decided_at
            and plan.workflow_spec_version_ref == spec.workflow_spec_version_ref
            and plan.exact_target_ref == _target_ref(spec)
            and canonical_sha256(_plan_material(plan)) == plan.plan_digest
        )
        if not expected:
            raise InvariantViolation(
                "AGENCY_ACTIVATION_PLAN_SOURCE_DRIFT",
                "activation plan no longer matches its exact authorization source",
                source_event_id=str(source.event["id"]),
            )

    async def _require_exact_planned_agency(
        self,
        conn: asyncpg.Connection,
        *,
        plan: AgencyActivationPlan,
        intervention_spec: InterventionSpec,
        workflow_version: int,
        task_version: int,
    ) -> None:
        workflow_row = await conn.fetchrow(
            """
            SELECT head.*, version.snapshot,
                   result.writer_id, result.object_type, result.object_id,
                   result.object_version
            FROM agency_workflow_run_heads head
            JOIN agency_workflow_run_versions version
              ON version.tenant_id=head.tenant_id
             AND version.workflow_run_id=head.workflow_run_id
             AND version.aggregate_version=head.current_version
            JOIN agency_command_results result
              ON result.id=version.command_result_id
            WHERE head.tenant_id=$1 AND head.workflow_run_id=$2
              AND head.current_version=$3
            FOR KEY SHARE OF head, version, result
            """,
            plan.tenant_id,
            plan.workflow_run_id,
            workflow_version,
        )
        task_row = await conn.fetchrow(
            """
            SELECT head.*, version.snapshot,
                   result.writer_id, result.object_type, result.object_id,
                   result.object_version
            FROM agency_task_heads head
            JOIN agency_task_versions version
              ON version.tenant_id=head.tenant_id
             AND version.task_id=head.task_id
             AND version.aggregate_version=head.current_version
            JOIN agency_command_results result
              ON result.id=version.command_result_id
            WHERE head.tenant_id=$1 AND head.task_id=$2
              AND head.current_version=$3
            FOR KEY SHARE OF head, version, result
            """,
            plan.tenant_id,
            plan.task_id,
            task_version,
        )
        if workflow_row is None or task_row is None:
            raise InvariantViolation(
                "AGENCY_ACTIVATION_PLANNED_AGENCY_MISSING",
                "activation cannot complete without exact workflow and task versions",
                workflow_run_id=str(plan.workflow_run_id),
                task_id=str(plan.task_id),
            )
        workflow = WorkflowRunSnapshot.model_validate(_json(workflow_row["snapshot"]))
        task = TaskSnapshot.model_validate(_json(task_row["snapshot"]))
        exact = (
            workflow.state is WorkflowRunState.PLANNED
            and workflow.workflow_run_id == plan.workflow_run_id
            and workflow.tenant_id == plan.tenant_id
            and workflow.episode_id == plan.episode_id
            and workflow.intervention_spec_digest == plan.intervention_spec_digest
            and workflow.workflow_spec_version_ref == plan.workflow_spec_version_ref
            and workflow.authorization_decision_id
            == plan.authorization_decision_id
            and workflow.authorization_decision_version
            == plan.authorization_decision_version
            and workflow.required_task_ids == (plan.task_id,)
            and workflow.created_at == plan.activation_at
            and workflow.updated_at == plan.activation_at
            and workflow_row["episode_id"] == plan.episode_id
            and workflow_row["intervention_spec_digest"]
            == plan.intervention_spec_digest
            and workflow_row["current_state"] == WorkflowRunState.PLANNED.value
            and workflow_row["writer_id"] == "AgencyStateApplier"
            and workflow_row["object_type"] == "workflow_run"
            and workflow_row["object_id"] == plan.workflow_run_id
            and int(workflow_row["object_version"]) == workflow_version
            and task.state is TaskState.PLANNED
            and task.task_id == plan.task_id
            and task.tenant_id == plan.tenant_id
            and task.workflow_run_id == plan.workflow_run_id
            and task.episode_id == plan.episode_id
            and task.intervention_spec_digest == plan.intervention_spec_digest
            and task.authorization_decision_id == plan.authorization_decision_id
            and task.authorization_decision_version
            == plan.authorization_decision_version
            and task.target_grounding_refs == (plan.exact_target_ref,)
            and task.task_kind == f"external_effect:{intervention_spec.operation}"
            and task.external_effect_required
            and task.created_at == plan.activation_at
            and task.updated_at == plan.activation_at
            and task_row["workflow_run_id"] == plan.workflow_run_id
            and task_row["episode_id"] == plan.episode_id
            and task_row["intervention_spec_digest"] == plan.intervention_spec_digest
            and task_row["current_state"] == TaskState.PLANNED.value
            and task_row["writer_id"] == "AgencyStateApplier"
            and task_row["object_type"] == "task"
            and task_row["object_id"] == plan.task_id
            and int(task_row["object_version"]) == task_version
        )
        if not exact:
            raise InvariantViolation(
                "AGENCY_ACTIVATION_PLANNED_AGENCY_DRIFT",
                "workflow and task do not exactly implement the frozen activation plan",
                workflow_run_id=str(plan.workflow_run_id),
                task_id=str(plan.task_id),
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
            FROM authorized_agency_activation_work_items
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
            "AGENCY_ACTIVATION_STALE_CLAIM",
            "activation transition requires the current live fence token",
            work_item_id=str(work_item_id),
        )

    def _require_claim_transition(
        self,
        row: asyncpg.Record | None,
        work_item_id: UUID,
    ) -> AgencyActivationWorkItem:
        if row is None:
            self._raise_stale_claim(work_item_id)
        return _work_item(row)


__all__ = [
    "AgencyActivationPlan",
    "AgencyActivationRepo",
    "AgencyActivationWorkContext",
    "AgencyActivationWorkItem",
    "AgencyActivationWorkStatus",
]
