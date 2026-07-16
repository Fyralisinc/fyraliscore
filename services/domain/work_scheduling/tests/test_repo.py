from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.contracts.execution import (
    LeaseGrantCommand,
    LeaseToken,
    WorkDecision,
    WorkDecisionCommand,
    WorkObligation,
    WorkObligationRegistrationCommand,
    WorkObligationState,
    WorkStateTransition,
    WorkStateTransitionCommand,
)
from lib.contracts.runtime import ProcessingClass
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.agency_activation import AgencyActivationRepo
from services.domain.agency_activation.tests.test_repo import (
    _authorization_fixture,
    _context,
    _install_planned_agency,
)
from services.domain.execution.repo import WorkLedgerApplier
from services.domain.work_scheduling import (
    WorkSchedulingRepo,
    WorkSchedulingWorkStatus,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _registered_work_fixture(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    start: datetime,
    authorization_expires_at: datetime,
    work_deadline: datetime,
) -> tuple[WorkObligation, UUID]:
    _, proposal, authorization_event_id = await _authorization_fixture(
        conn,
        tenant_id=tenant_id,
        start=start,
        expires_at=authorization_expires_at,
    )
    activation_repo = AgencyActivationRepo()
    activation_discovery_at = start + timedelta(minutes=4)
    activation_item = await activation_repo.discover_from_event(
        conn,
        source_event_id=authorization_event_id,
        now=activation_discovery_at,
    )
    assert activation_item is not None
    (activation_claim,) = await activation_repo.claim_ready_work(
        conn,
        worker_id="work-scheduling-fixture",
        now=activation_discovery_at,
        lease_duration=timedelta(minutes=5),
        limit=1,
    )
    assert activation_claim.claim_token is not None
    activation_context = await activation_repo.load_claimed_context(
        conn,
        tenant_id=tenant_id,
        work_item_id=activation_claim.id,
        worker_id="work-scheduling-fixture",
        claim_token=activation_claim.claim_token,
        now=activation_discovery_at,
    )
    await _install_planned_agency(
        conn,
        tenant_id=tenant_id,
        context=activation_context,
        at=activation_context.plan.activation_at,
    )
    await activation_repo.mark_activated(
        conn,
        tenant_id=tenant_id,
        work_item_id=activation_claim.id,
        worker_id="work-scheduling-fixture",
        claim_token=activation_claim.claim_token,
        workflow_version=1,
        task_version=1,
        now=activation_discovery_at + timedelta(seconds=1),
    )
    registered_at = start + timedelta(minutes=5)
    work = WorkObligation(
        obligation_id=uuid7(),
        lineage_id=uuid7(),
        tenant_id=tenant_id,
        generation=1,
        semantic_dedupe_key=f"schedule-task:{activation_context.plan.task_id}",
        causal_parent_ref=f"task:{activation_context.plan.task_id}:v1",
        reason="the exact authorized task requires scheduled execution",
        target_object_type="task",
        target_object_id=activation_context.plan.task_id,
        owner_writer_id="AgencyStateApplier",
        purpose="execute_governed_task",
        risk_tier="high",
        expected_value=0.8,
        correctness_priority=0.95,
        intent_relevance=1.0,
        uncertainty_reduction_estimate=0.5,
        minimum_processing_class=ProcessingClass.R5_EXTERNAL_AGENCY,
        maximum_processing_class=ProcessingClass.R5_EXTERNAL_AGENCY,
        economic_envelope_ref="economic-envelope:agency-v1",
        maximum_attempts=2,
        deadline=work_deadline,
        generation_depth=0,
        terminal_condition="exact task result or explicit no-effect fate",
        effect_possible=True,
        registered_at=registered_at,
    )
    result = await WorkLedgerApplier().register(
        conn=conn,
        command=WorkObligationRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="register_work",
                at=registered_at,
                key=f"work-scheduling:register:{work.obligation_id}",
            ),
            obligation=work,
        ),
        now=registered_at,
    )
    assert proposal.intervention_spec_digest == activation_context.plan.intervention_spec_digest
    return work, result.event_id


async def _apply_exact_schedule(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    context,
) -> None:
    ledger = WorkLedgerApplier()
    plan = context.plan
    decision = WorkDecision(
        decision_id=plan.decision_id,
        tenant_id=tenant_id,
        obligation_id=plan.obligation_id,
        obligation_generation=plan.obligation_generation,
        from_state=WorkObligationState.REGISTERED,
        to_state=WorkObligationState.ELIGIBLE,
        selected_processing_class=plan.selected_processing_class,
        policy_version_ref=plan.policy_version_ref,
        why_no_cheaper_class_is_safe=(
            "the Work envelope requires this exact minimum processing class"
        ),
        reason="registered exact task Work is eligible",
        decided_at=plan.scheduled_at,
    )
    await ledger.decide(
        conn=conn,
        command=WorkDecisionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="decide_work",
                at=plan.scheduled_at,
                key=f"work-scheduling:decision:{plan.decision_id}",
            ),
            expected_version=1,
            decision=decision,
        ),
        now=plan.scheduled_at,
    )
    lease = LeaseToken(
        lease_token_id=plan.lease_token_id,
        tenant_id=tenant_id,
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
    await ledger.grant_lease(
        conn=conn,
        command=LeaseGrantCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="grant_lease",
                at=plan.scheduled_at,
                key=f"work-scheduling:lease:{plan.lease_token_id}",
            ),
            expected_obligation_version=2,
            lease=lease,
        ),
        now=plan.scheduled_at,
    )


async def _expire_work(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    work: WorkObligation,
    at: datetime,
    reason: str,
) -> None:
    transition = WorkStateTransition(
        transition_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=work.obligation_id,
        obligation_generation=work.generation,
        from_state=WorkObligationState.REGISTERED,
        to_state=WorkObligationState.EXPIRED,
        reason=reason,
        transitioned_at=at,
    )
    await WorkLedgerApplier().transition(
        conn=conn,
        command=WorkStateTransitionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="expire_work",
                at=at,
                key=f"work-scheduling:expire:{work.obligation_id}",
            ),
            expected_version=1,
            transition=transition,
        ),
        now=at,
    )


async def test_registered_work_becomes_one_exact_active_lease(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = WorkSchedulingRepo()
    tenant_id = uuid7()
    start = datetime.now(timezone.utc) - timedelta(minutes=10)
    async with fresh_db.acquire() as conn, conn.transaction():
        work, event_id = await _registered_work_fixture(
            conn,
            tenant_id=tenant_id,
            start=start,
            authorization_expires_at=start + timedelta(hours=2),
            work_deadline=start + timedelta(hours=1),
        )
        discovery_at = start + timedelta(minutes=6)
        assert await repo.discover_ready_work(
            conn,
            now=discovery_at,
            limit=10,
            tenant_id=tenant_id,
        ) == 1
        item = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=discovery_at + timedelta(minutes=1),
        )
        assert item is not None
        assert item.plan.obligation_id == work.obligation_id
        assert item.plan.selected_processing_class is work.minimum_processing_class
        assert item.plan.scheduled_at == discovery_at
        assert item.plan.heartbeat_deadline <= item.plan.work_lease_expires_at
        duplicate = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=discovery_at + timedelta(minutes=2),
        )
        assert duplicate is not None and duplicate.id == item.id

        (claim,) = await repo.claim_ready_work(
            conn,
            worker_id="work-scheduler:test",
            now=discovery_at,
            lease_duration=timedelta(minutes=5),
            limit=1,
        )
        assert claim.claim_token is not None
        context = await repo.load_claimed_context(
            conn,
            tenant_id=tenant_id,
            work_item_id=claim.id,
            worker_id="work-scheduler:test",
            claim_token=claim.claim_token,
            now=discovery_at + timedelta(seconds=1),
        )
        await _apply_exact_schedule(
            conn,
            tenant_id=tenant_id,
            context=context,
        )
        leased = await repo.mark_leased(
            conn,
            tenant_id=tenant_id,
            work_item_id=claim.id,
            worker_id="work-scheduler:test",
            claim_token=claim.claim_token,
            eligible_work_version=2,
            leased_work_version=3,
            lease_version=1,
            lease_fence=1,
            now=discovery_at + timedelta(seconds=2),
        )
        assert leased.status is WorkSchedulingWorkStatus.LEASED
        assert leased.eligible_work_version == 2
        assert leased.leased_work_version == 3
        assert leased.applied_lease_version == 1
        assert leased.applied_lease_fence == 1


async def test_schedule_claim_fencing_retry_and_work_expiry(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = WorkSchedulingRepo()
    tenant_id = uuid7()
    start = datetime.now(timezone.utc) - timedelta(hours=2)
    async with fresh_db.acquire() as conn, conn.transaction():
        work, event_id = await _registered_work_fixture(
            conn,
            tenant_id=tenant_id,
            start=start,
            authorization_expires_at=start + timedelta(hours=1),
            work_deadline=start + timedelta(minutes=10),
        )
        item = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=start + timedelta(minutes=6),
        )
        assert item is not None
        (first,) = await repo.claim_ready_work(
            conn,
            worker_id="work-scheduler:a",
            now=start + timedelta(minutes=6),
            lease_duration=timedelta(seconds=5),
            limit=1,
        )
        assert first.claim_token is not None
        (reclaimed,) = await repo.claim_ready_work(
            conn,
            worker_id="work-scheduler:b",
            now=start + timedelta(minutes=7),
            lease_duration=timedelta(minutes=5),
            limit=1,
        )
        assert reclaimed.claim_token is not None
        with pytest.raises(InvariantViolation, match="current live fence token"):
            await repo.schedule_retry(
                conn,
                tenant_id=tenant_id,
                work_item_id=first.id,
                worker_id="work-scheduler:a",
                claim_token=first.claim_token,
                now=start + timedelta(minutes=7, seconds=1),
                next_attempt_at=start + timedelta(minutes=8),
                failure_class="stale_worker",
                failure_reason="must not overwrite a recovered schedule claim",
            )
        retry = await repo.schedule_retry(
            conn,
            tenant_id=tenant_id,
            work_item_id=reclaimed.id,
            worker_id="work-scheduler:b",
            claim_token=reclaimed.claim_token,
            now=start + timedelta(minutes=7, seconds=1),
            next_attempt_at=start + timedelta(minutes=8),
            failure_class="work_cas",
            failure_reason="retry exact Work scheduling transaction",
        )
        assert retry.status is WorkSchedulingWorkStatus.RETRY_SCHEDULED
        (expiry_claim,) = await repo.claim_ready_work(
            conn,
            worker_id="work-scheduler:expiry",
            now=start + timedelta(minutes=11),
            lease_duration=timedelta(minutes=5),
            limit=1,
        )
        assert expiry_claim.claim_token is not None
        expiry_at = start + timedelta(minutes=11)
        await _expire_work(
            conn,
            tenant_id=tenant_id,
            work=work,
            at=expiry_at,
            reason="the registered Work deadline elapsed before lease",
        )
        expired = await repo.mark_work_expired(
            conn,
            tenant_id=tenant_id,
            work_item_id=expiry_claim.id,
            worker_id="work-scheduler:expiry",
            claim_token=expiry_claim.claim_token,
            work_version=2,
            now=expiry_at,
            reason="the exact canonical Work expired at its deadline",
        )
        assert expired.status is WorkSchedulingWorkStatus.WORK_EXPIRED
        assert expired.expired_work_version == 2
        assert expired.work_expired_at is not None


async def test_authorization_expiry_requires_exact_canonical_work_expiry(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = WorkSchedulingRepo()
    tenant_id = uuid7()
    start = datetime.now(timezone.utc) - timedelta(hours=2)
    async with fresh_db.acquire() as conn, conn.transaction():
        work, event_id = await _registered_work_fixture(
            conn,
            tenant_id=tenant_id,
            start=start,
            authorization_expires_at=start + timedelta(minutes=10),
            work_deadline=start + timedelta(hours=1),
        )
        item = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=start + timedelta(minutes=6),
        )
        assert item is not None
        (claim,) = await repo.claim_ready_work(
            conn,
            worker_id="work-scheduler:authorization-expiry",
            now=start + timedelta(minutes=11),
            lease_duration=timedelta(minutes=5),
            limit=1,
        )
        assert claim.claim_token is not None
        expiry_at = start + timedelta(minutes=11)
        with pytest.raises(
            InvariantViolation,
            match="exact canonical expired Work",
        ):
            await repo.mark_authorization_expired(
                conn,
                tenant_id=tenant_id,
                work_item_id=claim.id,
                worker_id="work-scheduler:authorization-expiry",
                claim_token=claim.claim_token,
                work_version=2,
                now=expiry_at,
                reason="authorization expired before Work could be leased",
            )
        await _expire_work(
            conn,
            tenant_id=tenant_id,
            work=work,
            at=expiry_at,
            reason="authorization expired before Work could be leased",
        )
        expired = await repo.mark_authorization_expired(
            conn,
            tenant_id=tenant_id,
            work_item_id=claim.id,
            worker_id="work-scheduler:authorization-expiry",
            claim_token=claim.claim_token,
            work_version=2,
            now=expiry_at,
            reason="authorization expired before Work could be leased",
        )
        assert (
            expired.status
            is WorkSchedulingWorkStatus.AUTHORIZATION_EXPIRED
        )
        assert expired.expired_work_version == 2
        assert expired.authorization_expired_at is not None


async def test_schedule_terminal_failure_is_explicit_and_fenced(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = WorkSchedulingRepo()
    tenant_id = uuid7()
    start = datetime.now(timezone.utc) - timedelta(minutes=10)
    async with fresh_db.acquire() as conn, conn.transaction():
        _, event_id = await _registered_work_fixture(
            conn,
            tenant_id=tenant_id,
            start=start,
            authorization_expires_at=start + timedelta(hours=2),
            work_deadline=start + timedelta(hours=1),
        )
        item = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=start + timedelta(minutes=6),
        )
        assert item is not None
        (claim,) = await repo.claim_ready_work(
            conn,
            worker_id="work-scheduler:terminal",
            now=start + timedelta(minutes=6),
            lease_duration=timedelta(minutes=5),
            limit=1,
        )
        assert claim.claim_token is not None
        failed = await repo.fail_work_terminally(
            conn,
            tenant_id=tenant_id,
            work_item_id=claim.id,
            worker_id="work-scheduler:terminal",
            claim_token=claim.claim_token,
            now=start + timedelta(minutes=6, seconds=1),
            failure_class="invalid_work_owner",
            failure_reason="the exact Work owner cannot execute this task type",
        )
        assert failed.status is WorkSchedulingWorkStatus.FAILED_TERMINAL
        assert failed.last_failure_class == "invalid_work_owner"
