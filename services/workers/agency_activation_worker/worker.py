"""Leased authorization-to-planned-workflow/task activation worker."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import asyncpg
import structlog

from lib.contracts.agency import AgencyWriteContext
from lib.contracts.execution import (
    TaskCommand,
    TaskSnapshot,
    TaskState,
    WorkflowRunCommand,
    WorkflowRunSnapshot,
    WorkflowRunState,
)
from lib.contracts.kernel import (
    ProcessingAuthorityContext,
    RestrictionSet,
    WriterCutoverState,
    WriterScopeEpoch,
)
from lib.shared.errors import InvariantViolation
from services.domain.agency_activation import (
    AgencyActivationRepo,
    AgencyActivationWorkContext,
    AgencyActivationWorkItem,
)
from services.domain.execution import AgencyStateApplier


@dataclass(slots=True)
class AgencyActivationWorkerStats:
    batches: int = 0
    discovered: int = 0
    claimed: int = 0
    activated: int = 0
    authorization_expired: int = 0
    retries_scheduled: int = 0
    terminal_failures: int = 0
    stale_claims: int = 0


class AgencyActivationWorker:
    """Instantiate internal agency without scheduling or performing an effect."""

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        worker_id: str,
        repo: AgencyActivationRepo | None = None,
        agency_state: AgencyStateApplier | None = None,
        lease_duration: timedelta = timedelta(minutes=2),
        retry_delay: timedelta = timedelta(seconds=30),
        max_attempts: int = 5,
        logger: Any | None = None,
    ) -> None:
        if not worker_id.strip():
            raise ValueError("worker_id must be non-empty")
        if lease_duration <= timedelta(0) or retry_delay <= timedelta(0):
            raise ValueError("lease_duration and retry_delay must be positive")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._pool = pool
        self._worker_id = worker_id
        self._repo = repo or AgencyActivationRepo()
        self._agency_state = agency_state or AgencyStateApplier()
        self._lease_duration = lease_duration
        self._retry_delay = retry_delay
        self._max_attempts = max_attempts
        self._log = logger or structlog.get_logger(__name__)

    async def process_batch(
        self,
        *,
        limit: int = 25,
        stats: AgencyActivationWorkerStats | None = None,
    ) -> int:
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        stats = stats or AgencyActivationWorkerStats()
        claim_time = datetime.now(timezone.utc)
        async with self._pool.acquire() as conn, conn.transaction():
            discovered = await self._repo.discover_ready_work(
                conn,
                now=claim_time,
                limit=limit,
            )
            items = await self._repo.claim_ready_work(
                conn,
                worker_id=self._worker_id,
                now=claim_time,
                lease_duration=self._lease_duration,
                limit=limit,
            )
        stats.batches += 1
        stats.discovered += discovered
        stats.claimed += len(items)
        for item in items:
            await self._process_item(item, stats=stats)
        return len(items)

    async def _process_item(
        self,
        item: AgencyActivationWorkItem,
        *,
        stats: AgencyActivationWorkerStats,
    ) -> None:
        assert item.claim_token is not None
        try:
            async with self._pool.acquire() as conn, conn.transaction():
                actual_now = datetime.now(timezone.utc)
                context = await self._repo.load_claimed_context(
                    conn,
                    tenant_id=item.tenant_id,
                    work_item_id=item.id,
                    worker_id=self._worker_id,
                    claim_token=item.claim_token,
                    now=actual_now,
                )
                effective_now = max(actual_now, context.plan.activation_at)
                authorization = context.authorization
                if (
                    effective_now >= authorization.expires_at
                    or not authorization.authority.is_live(effective_now)
                ):
                    await self._repo.mark_authorization_expired(
                        conn,
                        tenant_id=item.tenant_id,
                        work_item_id=item.id,
                        worker_id=self._worker_id,
                        claim_token=item.claim_token,
                        now=actual_now,
                        reason=(
                            "exact authorization was no longer live before "
                            "planned agency activation"
                        ),
                    )
                    stats.authorization_expired += 1
                    return

                workflow_result, task_result = await self._activate(
                    conn=conn,
                    context=context,
                    effective_now=effective_now,
                )
                await self._repo.mark_activated(
                    conn,
                    tenant_id=item.tenant_id,
                    work_item_id=item.id,
                    worker_id=self._worker_id,
                    claim_token=item.claim_token,
                    workflow_version=workflow_result.object_version,
                    task_version=task_result.object_version,
                    now=actual_now,
                )
            stats.activated += 1
        except Exception as exc:  # noqa: BLE001
            await self._record_failure(item, exc=exc, stats=stats)

    async def _activate(
        self,
        *,
        conn: asyncpg.Connection,
        context: AgencyActivationWorkContext,
        effective_now: datetime,
    ):
        plan = context.plan
        authorization = context.authorization
        spec = context.intervention_spec
        workflow = WorkflowRunSnapshot(
            workflow_run_id=plan.workflow_run_id,
            tenant_id=context.work_item.tenant_id,
            episode_id=context.work_item.episode_id,
            intervention_spec_digest=context.work_item.intervention_spec_digest,
            workflow_spec_version_ref=plan.workflow_spec_version_ref,
            state=WorkflowRunState.PLANNED,
            authorization_decision_id=authorization.decision_id,
            prerequisite_refs=(
                f"authorization:{authorization.decision_id}:v1",
                f"intervention-spec:{spec.spec_id}",
            ),
            required_task_ids=(plan.task_id,),
            completion_predicate=(
                "all required tasks reach evidenced terminal completion"
            ),
            transition_reason=(
                "instantiate the exact authorized intervention workflow"
            ),
            created_at=plan.activation_at,
            updated_at=plan.activation_at,
        )
        workflow_result = await self._agency_state.apply_workflow_run(
            conn=conn,
            command=WorkflowRunCommand(
                context=self._write_context(
                    item=context.work_item,
                    object_type="workflow_run",
                    object_id=plan.workflow_run_id,
                    responsibility="workflow_run",
                    operation="instantiate_authorized_workflow",
                    issued_at=plan.activation_at,
                    expires_at=authorization.expires_at,
                ),
                expected_version=0,
                snapshot=workflow,
            ),
            now=effective_now,
        )
        task = TaskSnapshot(
            task_id=plan.task_id,
            tenant_id=context.work_item.tenant_id,
            workflow_run_id=plan.workflow_run_id,
            episode_id=context.work_item.episode_id,
            intervention_spec_digest=context.work_item.intervention_spec_digest,
            task_kind=f"external_effect:{spec.operation}",
            state=TaskState.PLANNED,
            target_grounding_refs=(plan.exact_target_ref,),
            authorization_decision_id=authorization.decision_id,
            external_effect_required=True,
            transition_reason=(
                "instantiate the exact authorized external-effect task"
            ),
            created_at=plan.activation_at,
            updated_at=plan.activation_at,
        )
        task_result = await self._agency_state.apply_task(
            conn=conn,
            command=TaskCommand(
                context=self._write_context(
                    item=context.work_item,
                    object_type="task",
                    object_id=plan.task_id,
                    responsibility="task",
                    operation="instantiate_authorized_task",
                    issued_at=plan.activation_at,
                    expires_at=authorization.expires_at,
                ),
                expected_version=0,
                snapshot=task,
            ),
            now=effective_now,
        )
        return workflow_result, task_result

    @staticmethod
    def _write_context(
        *,
        item: AgencyActivationWorkItem,
        object_type: str,
        object_id: UUID,
        responsibility: str,
        operation: str,
        issued_at: datetime,
        expires_at: datetime,
    ) -> AgencyWriteContext:
        fields = {
            "workflow_run": RestrictionSet.only(
                "authorization_decision_id",
                "completion_predicate",
                "created_at",
                "episode_id",
                "intervention_spec_digest",
                "prerequisite_refs",
                "required_task_ids",
                "state",
                "transition_reason",
                "updated_at",
                "workflow_spec_version_ref",
            ),
            "task": RestrictionSet.only(
                "authorization_decision_id",
                "created_at",
                "episode_id",
                "external_effect_required",
                "intervention_spec_digest",
                "state",
                "target_grounding_refs",
                "task_kind",
                "transition_reason",
                "updated_at",
                "workflow_run_id",
            ),
        }[object_type]
        command_id = uuid5(
            NAMESPACE_URL,
            (
                "fyralis:agency-activation-command:v1:"
                f"{item.source_event_id}:{object_type}"
            ),
        )
        authority = ProcessingAuthorityContext(
            tenant_id=item.tenant_id,
            principal_or_service_id="service:agency-activation-worker",
            purpose="authorized_internal_agency_activation",
            operation=operation,
            object_types=RestrictionSet.only(object_type),
            object_ids=RestrictionSet.only(str(object_id)),
            fields=fields,
            source_labels=RestrictionSet.only(
                "agency-canonical-event",
                "authorization-decision",
                "intervention-spec",
            ),
            authority_basis_refs=frozenset(
                {
                    f"canonical-event:{item.source_event_id}",
                    f"authorization:{item.authorization_decision_id}",
                }
            ),
            policy_version="agency-activation-v1",
            authority_epoch=1,
            decision_time=issued_at,
            expires_at=expires_at,
        )
        return AgencyWriteContext(
            command_id=command_id,
            tenant_id=item.tenant_id,
            processing_authority=authority,
            writer_scope_epoch=WriterScopeEpoch(
                scope_id=f"{responsibility}:{item.tenant_id}",
                tenant_id=item.tenant_id,
                semantic_responsibility=responsibility,
                source_partition=str(item.tenant_id),
                writer_owner="AgencyStateApplier",
                epoch=1,
                state=WriterCutoverState.NEW_CANONICAL,
            ),
            idempotency_key=(
                f"agency-activation:{item.source_event_id}:{object_type}"
            ),
            issued_at=issued_at,
            expires_at=expires_at,
        )

    async def _record_failure(
        self,
        item: AgencyActivationWorkItem,
        *,
        exc: Exception,
        stats: AgencyActivationWorkerStats,
    ) -> None:
        assert item.claim_token is not None
        now = datetime.now(timezone.utc)
        failure_class = (
            exc.invariant if isinstance(exc, InvariantViolation) else type(exc).__name__
        )
        failure_reason = str(exc)[:1000] or failure_class
        terminal = isinstance(exc, InvariantViolation) or (
            item.attempt_count >= self._max_attempts
        )
        try:
            async with self._pool.acquire() as conn, conn.transaction():
                if terminal:
                    await self._repo.fail_work_terminally(
                        conn,
                        tenant_id=item.tenant_id,
                        work_item_id=item.id,
                        worker_id=self._worker_id,
                        claim_token=item.claim_token,
                        now=now,
                        failure_class=failure_class,
                        failure_reason=failure_reason,
                    )
                    stats.terminal_failures += 1
                else:
                    await self._repo.schedule_retry(
                        conn,
                        tenant_id=item.tenant_id,
                        work_item_id=item.id,
                        worker_id=self._worker_id,
                        claim_token=item.claim_token,
                        now=now,
                        next_attempt_at=now + self._retry_delay,
                        failure_class=failure_class,
                        failure_reason=failure_reason,
                    )
                    stats.retries_scheduled += 1
        except Exception as transition_exc:  # noqa: BLE001
            stats.stale_claims += 1
            self._log.warning(
                "agency_activation.failure_transition_lost_claim",
                work_item_id=str(item.id),
                authorization_decision_id=str(item.authorization_decision_id),
                processing_error=failure_reason,
                transition_error=str(transition_exc),
            )
            return
        self._log.warning(
            "agency_activation.item_failed",
            work_item_id=str(item.id),
            authorization_decision_id=str(item.authorization_decision_id),
            attempt_count=item.attempt_count,
            failure_class=failure_class,
            terminal=terminal,
        )


__all__ = ["AgencyActivationWorker", "AgencyActivationWorkerStats"]
