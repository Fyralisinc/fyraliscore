"""Leased registered-Work scheduling and initial fence issuance."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import asyncpg
import structlog

from lib.contracts.agency import AgencyWriteContext
from lib.contracts.execution import (
    LeaseGrantCommand,
    LeaseToken,
    WorkDecision,
    WorkDecisionCommand,
    WorkObligationState,
    WorkStateTransition,
    WorkStateTransitionCommand,
)
from lib.contracts.kernel import (
    ProcessingAuthorityContext,
    RestrictionSet,
    WriterCutoverState,
    WriterScopeEpoch,
)
from lib.shared.errors import InvariantViolation
from services.domain.execution import WorkLedgerApplier
from services.domain.work_scheduling import (
    WorkSchedulingRepo,
    WorkSchedulingWorkContext,
    WorkSchedulingWorkItem,
)


@dataclass(slots=True)
class WorkSchedulerWorkerStats:
    batches: int = 0
    discovered: int = 0
    claimed: int = 0
    leased: int = 0
    work_expired: int = 0
    authorization_expired: int = 0
    retries_scheduled: int = 0
    terminal_failures: int = 0
    stale_claims: int = 0


class WorkSchedulerWorker:
    """Decide and fence Work without reserving or dispatching any effect."""

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        worker_id: str,
        repo: WorkSchedulingRepo | None = None,
        work_ledger: WorkLedgerApplier | None = None,
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
        self._repo = repo or WorkSchedulingRepo()
        self._work_ledger = work_ledger or WorkLedgerApplier()
        self._lease_duration = lease_duration
        self._retry_delay = retry_delay
        self._max_attempts = max_attempts
        self._log = logger or structlog.get_logger(__name__)

    async def process_batch(
        self,
        *,
        limit: int = 25,
        stats: WorkSchedulerWorkerStats | None = None,
    ) -> int:
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        stats = stats or WorkSchedulerWorkerStats()
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
        item: WorkSchedulingWorkItem,
        *,
        stats: WorkSchedulerWorkerStats,
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
                effective_now = max(actual_now, context.plan.scheduled_at)
                if effective_now >= context.obligation.deadline:
                    version = await self._expire_work(
                        conn=conn,
                        context=context,
                        effective_now=effective_now,
                        reason="registered Work reached its exact deadline",
                        fate="work_expired",
                    )
                    await self._repo.mark_work_expired(
                        conn,
                        tenant_id=item.tenant_id,
                        work_item_id=item.id,
                        worker_id=self._worker_id,
                        claim_token=item.claim_token,
                        work_version=version,
                        now=actual_now,
                        reason="registered Work reached its exact deadline",
                    )
                    stats.work_expired += 1
                    return
                if (
                    effective_now >= context.authorization.expires_at
                    or not context.authorization.authority.is_live(effective_now)
                ):
                    version = await self._expire_work(
                        conn=conn,
                        context=context,
                        effective_now=effective_now,
                        reason="exact authorization expired before Work lease",
                        fate="authorization_expired",
                    )
                    await self._repo.mark_authorization_expired(
                        conn,
                        tenant_id=item.tenant_id,
                        work_item_id=item.id,
                        worker_id=self._worker_id,
                        claim_token=item.claim_token,
                        work_version=version,
                        now=actual_now,
                        reason="exact authorization expired before Work lease",
                    )
                    stats.authorization_expired += 1
                    return
                if effective_now >= context.plan.heartbeat_deadline:
                    raise InvariantViolation(
                        "WORK_SCHEDULING_PLAN_STALE",
                        "frozen initial heartbeat deadline elapsed before lease grant",
                        obligation_id=str(context.plan.obligation_id),
                    )
                eligible, leased = await self._lease(
                    conn=conn,
                    context=context,
                    effective_now=effective_now,
                )
                await self._repo.mark_leased(
                    conn,
                    tenant_id=item.tenant_id,
                    work_item_id=item.id,
                    worker_id=self._worker_id,
                    claim_token=item.claim_token,
                    eligible_work_version=eligible.object_version,
                    leased_work_version=leased.object_version,
                    lease_version=1,
                    lease_fence=1,
                    now=actual_now,
                )
            stats.leased += 1
        except Exception as exc:  # noqa: BLE001
            await self._record_failure(item, exc=exc, stats=stats)

    async def _lease(
        self,
        *,
        conn: asyncpg.Connection,
        context: WorkSchedulingWorkContext,
        effective_now: datetime,
    ):
        plan = context.plan
        decision = WorkDecision(
            decision_id=plan.decision_id,
            tenant_id=plan.tenant_id,
            obligation_id=plan.obligation_id,
            obligation_generation=plan.obligation_generation,
            from_state=WorkObligationState.REGISTERED,
            to_state=WorkObligationState.ELIGIBLE,
            selected_processing_class=plan.selected_processing_class,
            policy_version_ref=plan.policy_version_ref,
            why_no_cheaper_class_is_safe=(
                "the registered Work declares this as its minimum safe "
                "processing class"
            ),
            reason="exact registered task Work is eligible for its initial lease",
            decided_at=plan.scheduled_at,
        )
        eligible = await self._work_ledger.decide(
            conn=conn,
            command=WorkDecisionCommand(
                context=self._write_context(
                    item=context.work_item,
                    operation="decide_registered_work",
                    command_kind="decision",
                    issued_at=plan.scheduled_at,
                    expires_at=plan.work_lease_expires_at,
                ),
                expected_version=1,
                decision=decision,
            ),
            now=effective_now,
        )
        lease = LeaseToken(
            lease_token_id=plan.lease_token_id,
            tenant_id=plan.tenant_id,
            obligation_id=plan.obligation_id,
            obligation_generation=plan.obligation_generation,
            fence=1,
            attempt=1,
            owner_ref=plan.lease_owner_ref,
            heartbeat_deadline=plan.heartbeat_deadline,
            expires_at=plan.work_lease_expires_at,
            effect_possible=context.obligation.effect_possible,
            granted_at=plan.scheduled_at,
        )
        leased = await self._work_ledger.grant_lease(
            conn=conn,
            command=LeaseGrantCommand(
                context=self._write_context(
                    item=context.work_item,
                    operation="grant_initial_work_lease",
                    command_kind="lease",
                    issued_at=plan.scheduled_at,
                    expires_at=plan.work_lease_expires_at,
                ),
                expected_obligation_version=2,
                lease=lease,
            ),
            now=effective_now,
        )
        return eligible, leased

    async def _expire_work(
        self,
        *,
        conn: asyncpg.Connection,
        context: WorkSchedulingWorkContext,
        effective_now: datetime,
        reason: str,
        fate: str,
    ) -> int:
        transition = WorkStateTransition(
            transition_id=uuid5(
                NAMESPACE_URL,
                (
                    "fyralis:work-scheduling-expiry:v1:"
                    f"{context.plan.source_event_id}:{fate}"
                ),
            ),
            tenant_id=context.plan.tenant_id,
            obligation_id=context.plan.obligation_id,
            obligation_generation=context.plan.obligation_generation,
            from_state=WorkObligationState.REGISTERED,
            to_state=WorkObligationState.EXPIRED,
            reason=reason,
            result_evidence_refs=(
                f"authorization:{context.plan.authorization_decision_id}",
                f"work:{context.plan.obligation_id}:v1",
            ),
            transitioned_at=effective_now,
        )
        result = await self._work_ledger.transition(
            conn=conn,
            command=WorkStateTransitionCommand(
                context=self._write_context(
                    item=context.work_item,
                    operation="expire_registered_work",
                    command_kind=fate,
                    issued_at=effective_now,
                    expires_at=effective_now + timedelta(minutes=5),
                ),
                expected_version=1,
                transition=transition,
            ),
            now=effective_now,
        )
        return result.object_version

    @staticmethod
    def _write_context(
        *,
        item: WorkSchedulingWorkItem,
        operation: str,
        command_kind: str,
        issued_at: datetime,
        expires_at: datetime,
    ) -> AgencyWriteContext:
        command_id = uuid5(
            NAMESPACE_URL,
            (
                "fyralis:work-scheduling-command:v1:"
                f"{item.source_event_id}:{command_kind}"
            ),
        )
        authority = ProcessingAuthorityContext(
            tenant_id=item.tenant_id,
            principal_or_service_id="service:work-scheduler-worker",
            purpose="registered_work_scheduling",
            operation=operation,
            object_types=RestrictionSet.only("work_obligation"),
            object_ids=RestrictionSet.only(str(item.obligation_id)),
            fields=RestrictionSet.only(
                "attempt_count",
                "current_fence",
                "current_lease_token_id",
                "current_state",
                "next_eligible_at",
                "updated_at",
                "wake_predicate",
            ),
            source_labels=RestrictionSet.only(
                "agency-canonical-event",
                "registered-work",
                "authorization-decision",
            ),
            authority_basis_refs=frozenset(
                {
                    f"canonical-event:{item.source_event_id}",
                    f"work:{item.obligation_id}:v1",
                }
            ),
            policy_version=item.plan.policy_version_ref,
            authority_epoch=1,
            decision_time=issued_at,
            expires_at=expires_at,
        )
        return AgencyWriteContext(
            command_id=command_id,
            tenant_id=item.tenant_id,
            processing_authority=authority,
            writer_scope_epoch=WriterScopeEpoch(
                scope_id=f"work_obligation:{item.tenant_id}",
                tenant_id=item.tenant_id,
                semantic_responsibility="work_obligation",
                source_partition=str(item.tenant_id),
                writer_owner="WorkLedgerApplier",
                epoch=1,
                state=WriterCutoverState.NEW_CANONICAL,
            ),
            idempotency_key=(
                f"work-scheduling:{item.source_event_id}:{command_kind}"
            ),
            issued_at=issued_at,
            expires_at=expires_at,
        )

    async def _record_failure(
        self,
        item: WorkSchedulingWorkItem,
        *,
        exc: Exception,
        stats: WorkSchedulerWorkerStats,
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
                "work_scheduler.failure_transition_lost_claim",
                work_item_id=str(item.id),
                obligation_id=str(item.obligation_id),
                processing_error=failure_reason,
                transition_error=str(transition_exc),
            )
            return
        self._log.warning(
            "work_scheduler.item_failed",
            work_item_id=str(item.id),
            obligation_id=str(item.obligation_id),
            attempt_count=item.attempt_count,
            failure_class=failure_class,
            terminal=terminal,
        )


__all__ = ["WorkSchedulerWorker", "WorkSchedulerWorkerStats"]
