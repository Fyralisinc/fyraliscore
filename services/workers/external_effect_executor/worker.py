"""Fenced leased-Work executor with explicit provider ambiguity handling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import asyncpg
import structlog

from lib.contracts.agency import AgencyWriteContext
from lib.contracts.execution import (
    EffectObservation,
    EffectReservationCommand,
    EffectTransitionCommand,
    ExternalEffectAttempt,
    ExternalEffectState,
    LeaseResolution,
    LeaseResolutionCommand,
    LeaseState,
    WorkObligationState,
)
from lib.contracts.kernel import (
    ProcessingAuthorityContext,
    RestrictionSet,
    WriterCutoverState,
    WriterScopeEpoch,
)
from lib.shared.errors import InvariantViolation
from services.domain.effect_execution import (
    EffectExecutionRepo,
    EffectExecutionWorkContext,
    EffectExecutionWorkItem,
)
from services.domain.execution import ExecutionLedgerApplier, WorkLedgerApplier
from services.workers.external_effect_executor.adapters import (
    ActionAdapterRegistry,
    ActionAdapterRequest,
    ActionDispatchFate,
    ActionDispatchResult,
    ActionPreflightResult,
)


_POST_EFFECT_CONTEXT_TTL = timedelta(minutes=5)


@dataclass(slots=True)
class ExternalEffectExecutorWorkerStats:
    batches: int = 0
    discovered: int = 0
    claimed: int = 0
    dispatched: int = 0
    provider_rejected: int = 0
    provider_failed: int = 0
    unknown: int = 0
    retries_scheduled: int = 0
    terminal_failures: int = 0
    stale_claims: int = 0


@dataclass(frozen=True, slots=True)
class _EffectHead:
    version: int
    state: ExternalEffectState


class ExternalEffectExecutorWorker:
    """Execute one exact provider call behind a durable dispatch-intent fence.

    The provider call is the only non-transactional step:

    1. fresh read-only preflight;
    2. atomically reserve the exact effect and record dispatch intent;
    3. call the provider outside the database transaction;
    4. atomically record provider fate, resolve Work, and close the queue item.

    A reclaimed dispatch intent is never treated as proof that no call occurred.
    It is re-dispatched only when the registered provider capability guarantees
    idempotency for the exact frozen key; otherwise it becomes UNKNOWN and is
    routed to canonical reconciliation.
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        worker_id: str,
        adapter_registry: ActionAdapterRegistry,
        repo: EffectExecutionRepo | None = None,
        execution_ledger: ExecutionLedgerApplier | None = None,
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
        self._adapter_registry = adapter_registry
        self._repo = repo or EffectExecutionRepo()
        self._execution = execution_ledger or ExecutionLedgerApplier()
        self._work_ledger = work_ledger or WorkLedgerApplier()
        self._lease_duration = lease_duration
        self._retry_delay = retry_delay
        self._max_attempts = max_attempts
        self._log = logger or structlog.get_logger(__name__)

    async def process_batch(
        self,
        *,
        limit: int = 25,
        stats: ExternalEffectExecutorWorkerStats | None = None,
    ) -> int:
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        stats = stats or ExternalEffectExecutorWorkerStats()
        now = datetime.now(timezone.utc)
        async with self._pool.acquire() as conn, conn.transaction():
            discovered = await self._repo.discover_ready_work(
                conn,
                now=now,
                limit=limit,
            )
            items = await self._repo.claim_ready_work(
                conn,
                worker_id=self._worker_id,
                now=now,
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
        item: EffectExecutionWorkItem,
        *,
        stats: ExternalEffectExecutorWorkerStats,
    ) -> None:
        assert item.claim_token is not None
        effect_ambiguous = False
        try:
            context, head = await self._load_context_and_head(item)
            if head is not None and head.state not in {
                ExternalEffectState.RESERVED,
                ExternalEffectState.DISPATCH_INTENT_RECORDED,
            }:
                await self._finish_existing_effect(
                    item=item,
                    context=context,
                    head=head,
                    stats=stats,
                )
                return

            request = self._adapter_request(context)
            adapter = await self._adapter_registry.resolve(request)
            preflight = await adapter.preflight(request)

            head_before_prepare = head
            head = await self._prepare_dispatch(
                item=item,
                preflight=preflight,
            )
            effect_ambiguous = (
                head.state is ExternalEffectState.DISPATCH_INTENT_RECORDED
            )
            reclaimed_dispatch = (
                head_before_prepare is not None
                and head_before_prepare.state
                is ExternalEffectState.DISPATCH_INTENT_RECORDED
            )
            if reclaimed_dispatch and not context.capabilities.idempotency_supported:
                await self._commit_dispatch_result(
                    item=item,
                    context=context,
                    result=ActionDispatchResult(
                        fate=ActionDispatchFate.UNKNOWN,
                        reason=(
                            "reclaimed dispatch intent has no provider idempotency "
                            "guarantee; executor refused blind redispatch"
                        ),
                        provider_observation_refs=(
                            "effect-ledger:reclaimed-dispatch-intent",
                        ),
                    ),
                    expected_head=head,
                    stats=stats,
                )
                return

            result = await adapter.dispatch(request)
            await self._commit_dispatch_result(
                item=item,
                context=context,
                result=result,
                expected_head=head,
                stats=stats,
            )
        except Exception as exc:  # noqa: BLE001
            if effect_ambiguous:
                await self._record_ambiguous_failure(
                    item,
                    exc=exc,
                    stats=stats,
                )
            else:
                await self._record_pre_dispatch_failure(
                    item,
                    exc=exc,
                    stats=stats,
                )

    async def _load_context_and_head(
        self,
        item: EffectExecutionWorkItem,
    ) -> tuple[EffectExecutionWorkContext, _EffectHead | None]:
        assert item.claim_token is not None
        now = datetime.now(timezone.utc)
        async with self._pool.acquire() as conn, conn.transaction():
            context = await self._repo.load_claimed_context(
                conn,
                tenant_id=item.tenant_id,
                work_item_id=item.id,
                worker_id=self._worker_id,
                claim_token=item.claim_token,
                now=now,
            )
            head = await self._effect_head(
                conn,
                tenant_id=item.tenant_id,
                effect_attempt_id=item.plan.effect_attempt_id,
                lock=False,
            )
        return context, head

    @staticmethod
    def _adapter_request(
        context: EffectExecutionWorkContext,
    ) -> ActionAdapterRequest:
        return ActionAdapterRequest(
            tenant_id=context.plan.tenant_id,
            effect_attempt_id=context.plan.effect_attempt_id,
            operation=context.plan.operation,
            parameters=dict(context.intervention_spec.parameters),
            provider_idempotency_key=context.plan.provider_idempotency_key,
            target_grounding_refs=context.plan.target_grounding_refs,
            declared_preconditions=(
                context.intervention_spec.safety_and_preconditions
            ),
            capabilities=context.capabilities,
        )

    async def _prepare_dispatch(
        self,
        *,
        item: EffectExecutionWorkItem,
        preflight: ActionPreflightResult,
    ) -> _EffectHead:
        assert item.claim_token is not None
        actual_now = datetime.now(timezone.utc)
        effective_now = max(actual_now, item.plan.reserved_at)
        async with self._pool.acquire() as conn, conn.transaction():
            context = await self._repo.load_claimed_context(
                conn,
                tenant_id=item.tenant_id,
                work_item_id=item.id,
                worker_id=self._worker_id,
                claim_token=item.claim_token,
                now=effective_now,
            )
            head = await self._effect_head(
                conn,
                tenant_id=item.tenant_id,
                effect_attempt_id=item.plan.effect_attempt_id,
                lock=True,
            )
            if head is None:
                attempt = ExternalEffectAttempt(
                    effect_attempt_id=context.plan.effect_attempt_id,
                    lineage_id=context.plan.effect_lineage_id,
                    tenant_id=context.plan.tenant_id,
                    generation=1,
                    episode_id=context.plan.episode_id,
                    task_id=context.plan.task_id,
                    intervention_spec_digest=(
                        context.plan.intervention_spec_digest
                    ),
                    authorization_decision_id=(
                        context.plan.authorization_decision_id
                    ),
                    authorization_decision_version=(
                        context.plan.authorization_decision_version
                    ),
                    capability_id=context.plan.capability_id,
                    capability_version=context.plan.capability_version,
                    capability_digest=context.plan.capability_digest,
                    operation=context.plan.operation,
                    canonical_request_hash=context.plan.canonical_request_hash,
                    provider_idempotency_key=(
                        context.plan.provider_idempotency_key
                    ),
                    target_grounding_refs=context.plan.target_grounding_refs,
                    live_precondition_refs=preflight.evidence_refs,
                    work_obligation_id=context.plan.obligation_id,
                    work_obligation_generation=(
                        context.plan.obligation_generation
                    ),
                    lease_token_id=context.plan.lease_token_id,
                    lease_fence=context.plan.lease_fence,
                    dispatch_deadline=context.plan.dispatch_deadline,
                    reconciliation_owner_ref=(
                        context.plan.reconciliation_owner_ref
                    ),
                    compensation_policy_ref=(
                        context.plan.compensation_policy_ref
                    ),
                    reserved_at=context.plan.reserved_at,
                )
                await self._execution.reserve(
                    conn=conn,
                    command=EffectReservationCommand(
                        context=self._effect_write_context(
                            item=item,
                            operation="reserve_external_effect",
                            command_kind="reserve",
                            issued_at=context.plan.reserved_at,
                            expires_at=context.plan.dispatch_deadline,
                        ),
                        attempt=attempt,
                    ),
                    now=effective_now,
                )
                head = _EffectHead(
                    version=1,
                    state=ExternalEffectState.RESERVED,
                )
            if head.state is ExternalEffectState.RESERVED:
                observed_at = effective_now
                result = await self._execution.transition(
                    conn=conn,
                    command=EffectTransitionCommand(
                        context=self._effect_write_context(
                            item=item,
                            operation="record_dispatch_intent",
                            command_kind="dispatch_intent",
                            issued_at=observed_at,
                            expires_at=context.plan.dispatch_deadline,
                        ),
                        expected_version=head.version,
                        observation=EffectObservation(
                            receipt_id=self._receipt_id(
                                item,
                                ExternalEffectState.DISPATCH_INTENT_RECORDED,
                            ),
                            tenant_id=item.tenant_id,
                            effect_attempt_id=item.plan.effect_attempt_id,
                            from_state=ExternalEffectState.RESERVED,
                            to_state=(
                                ExternalEffectState.DISPATCH_INTENT_RECORDED
                            ),
                            reason=(
                                "fresh provider preflight passed before the "
                                "effectful call"
                            ),
                            external_state_evidence_refs=preflight.evidence_refs,
                            observed_at=observed_at,
                        ),
                    ),
                    now=effective_now,
                )
                return _EffectHead(
                    version=result.object_version,
                    state=ExternalEffectState.DISPATCH_INTENT_RECORDED,
                )
            if head.state is not ExternalEffectState.DISPATCH_INTENT_RECORDED:
                raise InvariantViolation(
                    "EFFECT_EXECUTOR_PREPARE_STATE",
                    "effect changed to an unsupported state before dispatch",
                    effect_state=head.state.value,
                )
            return head

    async def _commit_dispatch_result(
        self,
        *,
        item: EffectExecutionWorkItem,
        context: EffectExecutionWorkContext,
        result: ActionDispatchResult,
        expected_head: _EffectHead,
        stats: ExternalEffectExecutorWorkerStats,
    ) -> None:
        assert item.claim_token is not None
        now = max(datetime.now(timezone.utc), item.plan.reserved_at)
        async with self._pool.acquire() as conn, conn.transaction():
            head = await self._effect_head(
                conn,
                tenant_id=item.tenant_id,
                effect_attempt_id=item.plan.effect_attempt_id,
                lock=True,
            )
            if head is None:
                raise InvariantViolation(
                    "EFFECT_EXECUTOR_ATTEMPT_MISSING",
                    "dispatch result cannot be recorded without its exact attempt",
                )
            if (
                head.version != expected_head.version
                or head.state is not expected_head.state
            ):
                await self._finalize_existing_in_transaction(
                    conn=conn,
                    item=item,
                    context=context,
                    head=head,
                    now=now,
                    stats=stats,
                )
                return
            if head.state is not ExternalEffectState.DISPATCH_INTENT_RECORDED:
                raise InvariantViolation(
                    "EFFECT_EXECUTOR_DISPATCH_STATE",
                    "provider result requires exact dispatch-intent state",
                    effect_state=head.state.value,
                )
            final_head, receipt_id = await self._apply_provider_result(
                conn=conn,
                item=item,
                head=head,
                result=result,
                now=now,
            )
            await self._resolve_work_for_effect(
                conn=conn,
                item=item,
                context=context,
                effect_state=final_head.state,
                receipt_id=receipt_id,
                now=now,
            )
            await self._mark_queue_fate(
                conn=conn,
                item=item,
                head=final_head,
                receipt_id=receipt_id,
                now=now,
            )
        self._increment_fate(stats, final_head.state)

    async def _apply_provider_result(
        self,
        *,
        conn: asyncpg.Connection,
        item: EffectExecutionWorkItem,
        head: _EffectHead,
        result: ActionDispatchResult,
        now: datetime,
    ) -> tuple[_EffectHead, UUID]:
        if result.fate is ActionDispatchFate.REJECTED:
            return await self._transition_effect(
                conn=conn,
                item=item,
                head=head,
                to_state=ExternalEffectState.REJECTED,
                reason=result.reason,
                provider_refs=result.provider_observation_refs,
                external_refs=result.external_state_evidence_refs,
                observed_at=now,
            )
        if result.fate is ActionDispatchFate.UNKNOWN:
            return await self._transition_effect(
                conn=conn,
                item=item,
                head=head,
                to_state=ExternalEffectState.UNKNOWN,
                reason=result.reason,
                provider_refs=result.provider_observation_refs,
                external_refs=result.external_state_evidence_refs,
                observed_at=now,
            )

        acknowledged, _ack_receipt_id = await self._transition_effect(
            conn=conn,
            item=item,
            head=head,
            to_state=ExternalEffectState.ACKNOWLEDGED,
            reason="provider accepted the exact idempotent request",
            provider_refs=(
                result.provider_observation_refs
                or result.external_state_evidence_refs
            ),
            external_refs=(),
            observed_at=now,
        )
        final_state = (
            ExternalEffectState.SUCCEEDED
            if result.fate is ActionDispatchFate.SUCCEEDED
            else ExternalEffectState.FAILED
        )
        return await self._transition_effect(
            conn=conn,
            item=item,
            head=acknowledged,
            to_state=final_state,
            reason=result.reason,
            provider_refs=result.provider_observation_refs,
            external_refs=result.external_state_evidence_refs,
            observed_at=now,
        )

    async def _transition_effect(
        self,
        *,
        conn: asyncpg.Connection,
        item: EffectExecutionWorkItem,
        head: _EffectHead,
        to_state: ExternalEffectState,
        reason: str,
        provider_refs: tuple[str, ...],
        external_refs: tuple[str, ...],
        observed_at: datetime,
    ) -> tuple[_EffectHead, UUID]:
        receipt_id = self._receipt_id(item, to_state)
        result = await self._execution.transition(
            conn=conn,
            command=EffectTransitionCommand(
                context=self._effect_write_context(
                    item=item,
                    operation=f"record_effect_{to_state.value}",
                    command_kind=to_state.value,
                    issued_at=observed_at,
                    expires_at=observed_at + _POST_EFFECT_CONTEXT_TTL,
                ),
                expected_version=head.version,
                observation=EffectObservation(
                    receipt_id=receipt_id,
                    tenant_id=item.tenant_id,
                    effect_attempt_id=item.plan.effect_attempt_id,
                    from_state=head.state,
                    to_state=to_state,
                    reason=reason,
                    provider_observation_refs=provider_refs,
                    external_state_evidence_refs=external_refs,
                    observed_at=observed_at,
                ),
            ),
            now=observed_at,
        )
        return (
            _EffectHead(version=result.object_version, state=to_state),
            receipt_id,
        )

    async def _resolve_work_for_effect(
        self,
        *,
        conn: asyncpg.Connection,
        item: EffectExecutionWorkItem,
        context: EffectExecutionWorkContext,
        effect_state: ExternalEffectState,
        receipt_id: UUID,
        now: datetime,
    ) -> None:
        work = await conn.fetchrow(
            """
            SELECT current_version, current_state, current_lease_token_id,
                   current_fence
            FROM work_obligation_heads
            WHERE tenant_id=$1 AND obligation_id=$2
            FOR UPDATE
            """,
            item.tenant_id,
            item.plan.obligation_id,
        )
        if work is None:
            raise InvariantViolation(
                "EFFECT_EXECUTOR_WORK_MISSING",
                "effect fate cannot resolve missing Work",
            )
        desired_work_state = self._work_state_for_effect(effect_state)
        if work["current_state"] == desired_work_state.value:
            return
        if (
            int(work["current_version"]) != item.plan.source_obligation_version
            or work["current_state"] != WorkObligationState.LEASED.value
            or work["current_lease_token_id"] != item.plan.lease_token_id
            or int(work["current_fence"]) != item.plan.lease_fence
        ):
            raise InvariantViolation(
                "EFFECT_EXECUTOR_WORK_FENCE_DRIFT",
                "effect result no longer owns the exact leased Work fence",
                work_state=str(work["current_state"]),
            )

        if effect_state is ExternalEffectState.SUCCEEDED:
            lease_state = LeaseState.COMPLETED
            effect_may_have_occurred = True
            next_eligible_at = None
            reason = "exact succeeded provider receipt completed Work"
        elif effect_state in {
            ExternalEffectState.REJECTED,
            ExternalEffectState.FAILED,
        }:
            lease_state = LeaseState.RELEASED
            effect_may_have_occurred = False
            next_eligible_at = now + self._retry_delay
            reason = "exact known-no-effect provider receipt allows governed retry"
        elif effect_state in {
            ExternalEffectState.UNKNOWN,
            ExternalEffectState.RECONCILING,
        }:
            lease_state = LeaseState.RECONCILIATION_REQUIRED
            effect_may_have_occurred = True
            next_eligible_at = None
            reason = "provider effect may have occurred and requires reconciliation"
        else:
            raise InvariantViolation(
                "EFFECT_EXECUTOR_WORK_FATE_UNSUPPORTED",
                "executor cannot resolve Work from this effect state",
                effect_state=effect_state.value,
            )

        await self._work_ledger.resolve_lease(
            conn=conn,
            command=LeaseResolutionCommand(
                context=self._work_write_context(
                    item=item,
                    operation="resolve_effect_work",
                    command_kind=effect_state.value,
                    issued_at=now,
                ),
                expected_obligation_version=(
                    item.plan.source_obligation_version
                ),
                expected_lease_version=item.plan.lease_version,
                resolution=LeaseResolution(
                    lease_token_id=item.plan.lease_token_id,
                    tenant_id=item.tenant_id,
                    obligation_id=item.plan.obligation_id,
                    obligation_generation=item.plan.obligation_generation,
                    fence=item.plan.lease_fence,
                    to_lease_state=lease_state,
                    to_work_state=desired_work_state,
                    effect_may_have_occurred=effect_may_have_occurred,
                    result_evidence_refs=(str(receipt_id),),
                    next_eligible_at=next_eligible_at,
                    reason=reason,
                    resolved_at=now,
                ),
            ),
            now=now,
        )

    @staticmethod
    def _work_state_for_effect(
        effect_state: ExternalEffectState,
    ) -> WorkObligationState:
        if effect_state is ExternalEffectState.SUCCEEDED:
            return WorkObligationState.COMPLETED
        if effect_state in {
            ExternalEffectState.REJECTED,
            ExternalEffectState.FAILED,
        }:
            return WorkObligationState.RETRY_WAIT
        if effect_state in {
            ExternalEffectState.UNKNOWN,
            ExternalEffectState.RECONCILING,
        }:
            return WorkObligationState.RECONCILIATION_REQUIRED
        raise InvariantViolation(
            "EFFECT_EXECUTOR_WORK_FATE_UNSUPPORTED",
            "executor cannot derive Work fate from effect state",
            effect_state=effect_state.value,
        )

    async def _mark_queue_fate(
        self,
        *,
        conn: asyncpg.Connection,
        item: EffectExecutionWorkItem,
        head: _EffectHead,
        receipt_id: UUID,
        now: datetime,
    ) -> None:
        assert item.claim_token is not None
        kwargs = {
            "tenant_id": item.tenant_id,
            "work_item_id": item.id,
            "worker_id": self._worker_id,
            "claim_token": item.claim_token,
            "effect_version": head.version,
            "receipt_id": receipt_id,
            "now": now,
        }
        if head.state is ExternalEffectState.SUCCEEDED:
            await self._repo.mark_dispatched(conn, **kwargs)
        elif head.state is ExternalEffectState.REJECTED:
            await self._repo.mark_provider_rejected(conn, **kwargs)
        elif head.state is ExternalEffectState.FAILED:
            await self._repo.mark_provider_failed(conn, **kwargs)
        elif head.state is ExternalEffectState.UNKNOWN:
            await self._repo.mark_unknown(conn, **kwargs)
        elif head.state is ExternalEffectState.RECONCILING:
            await self._repo.mark_reconciliation_required(conn, **kwargs)
        else:
            raise InvariantViolation(
                "EFFECT_EXECUTOR_QUEUE_FATE_UNSUPPORTED",
                "canonical effect state has no executor queue fate",
                effect_state=head.state.value,
            )

    async def _finish_existing_effect(
        self,
        *,
        item: EffectExecutionWorkItem,
        context: EffectExecutionWorkContext,
        head: _EffectHead,
        stats: ExternalEffectExecutorWorkerStats,
    ) -> None:
        now = max(datetime.now(timezone.utc), item.plan.reserved_at)
        async with self._pool.acquire() as conn, conn.transaction():
            locked_head = await self._effect_head(
                conn,
                tenant_id=item.tenant_id,
                effect_attempt_id=item.plan.effect_attempt_id,
                lock=True,
            )
            if locked_head is None:
                raise InvariantViolation(
                    "EFFECT_EXECUTOR_ATTEMPT_MISSING",
                    "existing effect disappeared before finalization",
                )
            await self._finalize_existing_in_transaction(
                conn=conn,
                item=item,
                context=context,
                head=locked_head,
                now=now,
                stats=stats,
            )

    async def _finalize_existing_in_transaction(
        self,
        *,
        conn: asyncpg.Connection,
        item: EffectExecutionWorkItem,
        context: EffectExecutionWorkContext,
        head: _EffectHead,
        now: datetime,
        stats: ExternalEffectExecutorWorkerStats,
    ) -> None:
        if head.state in {
            ExternalEffectState.DISPATCH_INTENT_RECORDED,
            ExternalEffectState.ACKNOWLEDGED,
        }:
            head, receipt_id = await self._transition_effect(
                conn=conn,
                item=item,
                head=head,
                to_state=ExternalEffectState.UNKNOWN,
                reason=(
                    "executor recovered an incomplete provider observation "
                    "boundary"
                ),
                provider_refs=("effect-ledger:incomplete-provider-observation",),
                external_refs=(),
                observed_at=now,
            )
        elif head.state in {
            ExternalEffectState.SUCCEEDED,
            ExternalEffectState.REJECTED,
            ExternalEffectState.FAILED,
            ExternalEffectState.UNKNOWN,
            ExternalEffectState.RECONCILING,
        }:
            receipt_id = await conn.fetchval(
                """
                SELECT receipt_id
                FROM execution_receipts
                WHERE tenant_id=$1 AND effect_attempt_id=$2
                  AND effect_version=$3 AND effect_state=$4
                """,
                item.tenant_id,
                item.plan.effect_attempt_id,
                head.version,
                head.state.value,
            )
            if receipt_id is None:
                raise InvariantViolation(
                    "EFFECT_EXECUTOR_RECEIPT_MISSING",
                    "canonical effect head has no exact execution receipt",
                    effect_state=head.state.value,
                    effect_version=head.version,
                )
        else:
            raise InvariantViolation(
                "EFFECT_EXECUTOR_RECOVERY_STATE_UNSUPPORTED",
                "executor cannot recover this canonical effect state",
                effect_state=head.state.value,
            )

        await self._resolve_work_for_effect(
            conn=conn,
            item=item,
            context=context,
            effect_state=head.state,
            receipt_id=receipt_id,
            now=now,
        )
        await self._mark_queue_fate(
            conn=conn,
            item=item,
            head=head,
            receipt_id=receipt_id,
            now=now,
        )
        self._increment_fate(stats, head.state)

    async def _record_ambiguous_failure(
        self,
        item: EffectExecutionWorkItem,
        *,
        exc: Exception,
        stats: ExternalEffectExecutorWorkerStats,
    ) -> None:
        assert item.claim_token is not None
        now = max(datetime.now(timezone.utc), item.plan.reserved_at)
        try:
            async with self._pool.acquire() as conn, conn.transaction():
                head = await self._effect_head(
                    conn,
                    tenant_id=item.tenant_id,
                    effect_attempt_id=item.plan.effect_attempt_id,
                    lock=True,
                )
                if head is None:
                    raise InvariantViolation(
                        "EFFECT_EXECUTOR_ATTEMPT_MISSING",
                        "ambiguous provider failure has no exact effect attempt",
                    )
                if head.state in {
                    ExternalEffectState.DISPATCH_INTENT_RECORDED,
                    ExternalEffectState.ACKNOWLEDGED,
                }:
                    head, receipt_id = await self._transition_effect(
                        conn=conn,
                        item=item,
                        head=head,
                        to_state=ExternalEffectState.UNKNOWN,
                        reason=(
                            "executor lost certainty after dispatch boundary: "
                            f"{type(exc).__name__}"
                        ),
                        provider_refs=(
                            f"executor:ambiguous:{type(exc).__name__}",
                        ),
                        external_refs=(),
                        observed_at=now,
                    )
                else:
                    receipt_id = await conn.fetchval(
                        """
                        SELECT receipt_id FROM execution_receipts
                        WHERE tenant_id=$1 AND effect_attempt_id=$2
                          AND effect_version=$3 AND effect_state=$4
                        """,
                        item.tenant_id,
                        item.plan.effect_attempt_id,
                        head.version,
                        head.state.value,
                    )
                    if receipt_id is None:
                        raise InvariantViolation(
                            "EFFECT_EXECUTOR_RECEIPT_MISSING",
                            "ambiguous recovery found no exact receipt",
                        )
                await self._resolve_work_for_effect(
                    conn=conn,
                    item=item,
                    context=await self._context_from_plan(conn, item),
                    effect_state=head.state,
                    receipt_id=receipt_id,
                    now=now,
                )
                await self._mark_queue_fate(
                    conn=conn,
                    item=item,
                    head=head,
                    receipt_id=receipt_id,
                    now=now,
                )
            self._increment_fate(stats, head.state)
        except Exception as transition_exc:  # noqa: BLE001
            stats.stale_claims += 1
            self._log.warning(
                "external_effect_executor.ambiguous_transition_deferred",
                work_item_id=str(item.id),
                effect_attempt_id=str(item.plan.effect_attempt_id),
                processing_error=str(exc),
                transition_error=str(transition_exc),
            )

    async def _context_from_plan(
        self,
        conn: asyncpg.Connection,
        item: EffectExecutionWorkItem,
    ) -> EffectExecutionWorkContext:
        """Load context before Work is resolved during ambiguity recovery."""

        assert item.claim_token is not None
        return await self._repo.load_claimed_context(
            conn,
            tenant_id=item.tenant_id,
            work_item_id=item.id,
            worker_id=self._worker_id,
            claim_token=item.claim_token,
            now=max(datetime.now(timezone.utc), item.plan.reserved_at),
        )

    async def _record_pre_dispatch_failure(
        self,
        item: EffectExecutionWorkItem,
        *,
        exc: Exception,
        stats: ExternalEffectExecutorWorkerStats,
    ) -> None:
        assert item.claim_token is not None
        now = max(datetime.now(timezone.utc), item.plan.reserved_at)
        failure_class = (
            exc.invariant if isinstance(exc, InvariantViolation) else type(exc).__name__
        )
        failure_reason = str(exc)[:1000] or failure_class
        retry_at = now + self._retry_delay
        terminal = (
            isinstance(exc, InvariantViolation)
            or item.attempt_count >= self._max_attempts
            or retry_at >= item.plan.dispatch_deadline
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
                        next_attempt_at=retry_at,
                        failure_class=failure_class,
                        failure_reason=failure_reason,
                    )
                    stats.retries_scheduled += 1
        except Exception as transition_exc:  # noqa: BLE001
            stats.stale_claims += 1
            self._log.warning(
                "external_effect_executor.failure_transition_lost_claim",
                work_item_id=str(item.id),
                effect_attempt_id=str(item.plan.effect_attempt_id),
                processing_error=failure_reason,
                transition_error=str(transition_exc),
            )
            return
        self._log.warning(
            "external_effect_executor.item_failed_before_dispatch",
            work_item_id=str(item.id),
            effect_attempt_id=str(item.plan.effect_attempt_id),
            attempt_count=item.attempt_count,
            failure_class=failure_class,
            terminal=terminal,
        )

    @staticmethod
    async def _effect_head(
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        effect_attempt_id: UUID,
        lock: bool,
    ) -> _EffectHead | None:
        suffix = " FOR UPDATE" if lock else ""
        row = await conn.fetchrow(
            (
                "SELECT current_version, current_state "
                "FROM external_effect_attempt_heads "
                "WHERE tenant_id=$1 AND effect_attempt_id=$2"
                + suffix
            ),
            tenant_id,
            effect_attempt_id,
        )
        if row is None:
            return None
        return _EffectHead(
            version=int(row["current_version"]),
            state=ExternalEffectState(str(row["current_state"])),
        )

    @staticmethod
    def _receipt_id(
        item: EffectExecutionWorkItem,
        state: ExternalEffectState,
    ) -> UUID:
        return uuid5(
            NAMESPACE_URL,
            (
                "fyralis:external-effect-receipt:v1:"
                f"{item.plan.effect_attempt_id}:{state.value}"
            ),
        )

    @staticmethod
    def _effect_write_context(
        *,
        item: EffectExecutionWorkItem,
        operation: str,
        command_kind: str,
        issued_at: datetime,
        expires_at: datetime,
    ) -> AgencyWriteContext:
        authority = ProcessingAuthorityContext(
            tenant_id=item.tenant_id,
            principal_or_service_id="service:external-effect-executor",
            purpose="authorized_external_effect_execution",
            operation=operation,
            object_types=RestrictionSet.only("external_effect_attempt"),
            object_ids=RestrictionSet.only(str(item.plan.effect_attempt_id)),
            fields=RestrictionSet.only(
                "current_state",
                "current_version",
                "provider_observation_refs",
                "external_state_evidence_refs",
                "updated_at",
            ),
            source_labels=RestrictionSet.only(
                "leased-work-effect-plan",
                "provider-preflight",
                "provider-observation",
            ),
            authority_basis_refs=frozenset(
                {
                    f"canonical-event:{item.source_event_id}",
                    (
                        "authorization-decision:"
                        f"{item.plan.authorization_decision_id}"
                    ),
                    f"effect-plan:{item.plan.plan_digest}",
                }
            ),
            policy_version="external-effect-executor-v1",
            authority_epoch=1,
            decision_time=issued_at,
            expires_at=expires_at,
        )
        command_id = uuid5(
            NAMESPACE_URL,
            (
                "fyralis:external-effect-command:v1:"
                f"{item.source_event_id}:{command_kind}"
            ),
        )
        return AgencyWriteContext(
            command_id=command_id,
            tenant_id=item.tenant_id,
            processing_authority=authority,
            writer_scope_epoch=WriterScopeEpoch(
                scope_id=f"external_effect:{item.tenant_id}",
                tenant_id=item.tenant_id,
                semantic_responsibility="external_effect",
                source_partition=str(item.tenant_id),
                writer_owner="ExecutionLedgerApplier",
                epoch=1,
                state=WriterCutoverState.NEW_CANONICAL,
            ),
            idempotency_key=(
                f"external-effect:{item.source_event_id}:{command_kind}"
            ),
            issued_at=issued_at,
            expires_at=expires_at,
        )

    @staticmethod
    def _work_write_context(
        *,
        item: EffectExecutionWorkItem,
        operation: str,
        command_kind: str,
        issued_at: datetime,
    ) -> AgencyWriteContext:
        expires_at = issued_at + _POST_EFFECT_CONTEXT_TTL
        authority = ProcessingAuthorityContext(
            tenant_id=item.tenant_id,
            principal_or_service_id="service:external-effect-executor",
            purpose="external_effect_work_resolution",
            operation=operation,
            object_types=RestrictionSet.only("work_obligation"),
            object_ids=RestrictionSet.only(str(item.plan.obligation_id)),
            fields=RestrictionSet.only(
                "current_lease_token_id",
                "current_state",
                "next_eligible_at",
                "updated_at",
            ),
            source_labels=RestrictionSet.only(
                "execution-receipt",
                "leased-work-effect-plan",
            ),
            authority_basis_refs=frozenset(
                {
                    f"canonical-event:{item.source_event_id}",
                    f"effect-attempt:{item.plan.effect_attempt_id}",
                }
            ),
            policy_version="external-effect-executor-v1",
            authority_epoch=1,
            decision_time=issued_at,
            expires_at=expires_at,
        )
        command_id = uuid5(
            NAMESPACE_URL,
            (
                "fyralis:external-effect-work-command:v1:"
                f"{item.source_event_id}:{command_kind}"
            ),
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
                f"external-effect-work:{item.source_event_id}:{command_kind}"
            ),
            issued_at=issued_at,
            expires_at=expires_at,
        )

    @staticmethod
    def _increment_fate(
        stats: ExternalEffectExecutorWorkerStats,
        state: ExternalEffectState,
    ) -> None:
        if state is ExternalEffectState.SUCCEEDED:
            stats.dispatched += 1
        elif state is ExternalEffectState.REJECTED:
            stats.provider_rejected += 1
        elif state is ExternalEffectState.FAILED:
            stats.provider_failed += 1
        elif state in {
            ExternalEffectState.UNKNOWN,
            ExternalEffectState.RECONCILING,
        }:
            stats.unknown += 1


__all__ = [
    "ExternalEffectExecutorWorker",
    "ExternalEffectExecutorWorkerStats",
]
