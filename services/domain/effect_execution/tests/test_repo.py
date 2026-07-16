from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.contracts.agency import (
    AuthorizationDecision,
    AuthorizationDecisionCommand,
    AuthorizationDisposition,
    ConsequentialProposal,
    ConsequentialProposalFate,
    ConsequentialProposalRegistrationCommand,
    ConsequentialProposalReview,
    ConsequentialProposalReviewCommand,
    EpisodeStageFate,
    EpisodeStageLink,
    EpisodeUpdateCommand,
    InterventionEpisode,
    InterventionSpec,
)
from lib.contracts.execution import (
    ActionAdapterCapabilities,
    AdapterCapabilityRegistrationCommand,
    EffectObservation,
    EffectReservationCommand,
    EffectTransitionCommand,
    ExternalEffectAttempt,
    ExternalEffectState,
    LeaseGrantCommand,
    LeaseResolution,
    LeaseResolutionCommand,
    LeaseState,
    LeaseToken,
    TaskCommand,
    TaskSnapshot,
    TaskState,
    WorkDecision,
    WorkDecisionCommand,
    WorkObligation,
    WorkObligationRegistrationCommand,
    WorkObligationState,
    WorkflowRunCommand,
    WorkflowRunSnapshot,
    WorkflowRunState,
)
from lib.contracts.perception import CanonicalReferent, EntityLifecycleStatus
from lib.contracts.runtime import ProcessingClass
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.agency_activation.tests.test_repo import (
    _consumption_authority,
    _context,
    _processing_authority,
)
from services.domain.effect_execution import (
    EffectExecutionRepo,
    EffectExecutionWorkStatus,
)
from services.domain.execution.repo import (
    AgencyStateApplier,
    ExecutionLedgerApplier,
    WorkLedgerApplier,
)
from services.domain.intent.repo import ProposalAppender
from services.domain.outcomes.repo import AuthorizationApplier, EpisodeCoordinator


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _leased_work_fixture(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    start: datetime,
) -> tuple[UUID, ActionAdapterCapabilities]:
    execution = ExecutionLedgerApplier()
    agency = AgencyStateApplier()
    work_ledger = WorkLedgerApplier()
    capabilities = ActionAdapterCapabilities(
        capability_id=uuid7(),
        tenant_id=tenant_id,
        capability_version="slack-adapter-v3",
        adapter_name="slack-message-delivery",
        provider_name="slack",
        permitted_operations=frozenset({"send_message"}),
        request_canonicalization_version="slack-message-request-v2",
        idempotency_supported=True,
        idempotency_scope="workspace/channel/client-message-id",
        idempotency_retention_until=start + timedelta(days=2),
        reconciliation_supported=True,
        reconciliation_consistency_window_seconds=30,
        cancellation_supported=False,
        partial_effect_observable=True,
        compensation_supported=False,
        verified_at=start,
        expires_at=start + timedelta(days=1),
    )
    await execution.register_capabilities(
        conn=conn,
        command=AdapterCapabilityRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ExecutionLedgerApplier",
                responsibility="action_adapter_capability",
                operation="register_adapter_capability",
                at=start,
                key=f"effect-queue:capability:{capabilities.capability_id}",
            ),
            expected_version=0,
            capabilities=capabilities,
        ),
        now=start,
    )
    episode_at = start + timedelta(minutes=1)
    episode_id = uuid7()
    episode = InterventionEpisode(
        episode_id=episode_id,
        tenant_id=tenant_id,
        stage_links=(
            EpisodeStageLink(
                stage="proposal",
                fate=EpisodeStageFate.NOT_EXECUTED,
                reason="proposal has not yet been registered",
            ),
        ),
        created_at=episode_at,
        updated_at=episode_at,
    )
    await EpisodeCoordinator().apply(
        conn=conn,
        command=EpisodeUpdateCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="EpisodeCoordinator",
                responsibility="intervention_episode",
                operation="create_episode",
                at=episode_at,
                key=f"effect-queue:episode:{episode_id}",
            ),
            expected_version=0,
            episode=episode,
        ),
        now=episode_at,
    )
    proposal_at = start + timedelta(minutes=2)
    proposal_authority = _processing_authority(
        tenant_id=tenant_id,
        operation="register_proposal",
        at=proposal_at,
    )
    target = CanonicalReferent(
        tenant_id=tenant_id,
        referent_id="channel:customer-success",
        referent_version=2,
        lifecycle_status=EntityLifecycleStatus.ACTIVE,
        predecessor_referent_refs=(),
        successor_referent_refs=(),
        birth_decision_ref="identity:channel:v2",
        positive_existence_evidence_refs=("slack:channel:C1",),
    )
    spec = InterventionSpec(
        spec_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        target_referent=target,
        target_version="slack-channel-v2",
        operation="send_message",
        parameters={"channel_id": "C1", "text": "Review the Atlas escalation"},
        comparator={"delivery": "no_message"},
        outcome_metric="capable_recipient_acknowledged",
        outcome_window_start=start + timedelta(hours=1),
        outcome_window_end=start + timedelta(days=1),
        workflow_spec_version_ref="workflow:governed-delivery:v1",
        action_adapter_version=capabilities.capability_version,
        action_adapter_capability_digest=capabilities.capability_digest,
        safety_and_preconditions=("channel exists", "recipient may view concern"),
        authority_requirement="capability:send_governed_notification",
        reversible=False,
        compensation_declaration="cannot unsend; post correction if needed",
        grounding_dependency_refs=("grounding:channel:C1:v2",),
        context_dependency_manifest_digest="d" * 64,
    )
    proposal = ConsequentialProposal(
        proposal_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        intervention_spec=spec,
        summary="Send one governed escalation message",
        rationale="A capable owner must review the escalation",
        alternative_refs=("alternative:in-app-only",),
        source_refs=("concern:atlas-escalation",),
        processing_authority=proposal_authority,
        processing_authority_fingerprint=proposal_authority.fingerprint,
        created_at=proposal_at,
        review_due_at=proposal_at + timedelta(hours=1),
    )
    await ProposalAppender().append_consequential(
        conn=conn,
        command=ConsequentialProposalRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ProposalAppender",
                responsibility="consequential_proposal",
                operation="register_proposal",
                at=proposal_at,
                key=f"effect-queue:proposal:{proposal.proposal_id}",
                authority=proposal_authority,
            ),
            proposal=proposal,
        ),
        now=proposal_at,
    )
    review_at = start + timedelta(minutes=3)
    review = ConsequentialProposalReview(
        review_id=uuid7(),
        tenant_id=tenant_id,
        proposal_id=proposal.proposal_id,
        proposal_version=1,
        proposal_digest=proposal.proposal_digest,
        intervention_spec_digest=spec.spec_digest,
        from_fate=ConsequentialProposalFate.OPEN,
        to_fate=ConsequentialProposalFate.ACCEPTED_FOR_AUTHORIZATION,
        principal_or_policy_ref="actor:operations-owner",
        authority=_consumption_authority(
            tenant_id=tenant_id,
            operation="review_proposal",
            at=review_at,
        ),
        reason="the bounded message is appropriate",
        decided_at=review_at,
    )
    await ProposalAppender().review_consequential(
        conn=conn,
        command=ConsequentialProposalReviewCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ProposalAppender",
                responsibility="consequential_proposal",
                operation="review_proposal",
                at=review_at,
                key=f"effect-queue:review:{proposal.proposal_id}",
            ),
            review=review,
        ),
        now=review_at,
    )
    authorization_at = start + timedelta(minutes=4)
    authorization = AuthorizationDecision(
        decision_id=uuid7(),
        tenant_id=tenant_id,
        proposal_id=proposal.proposal_id,
        proposal_digest=proposal.proposal_digest,
        intervention_spec_digest=spec.spec_digest,
        disposition=AuthorizationDisposition.AUTHORIZED,
        principal_or_policy_ref="actor:operations-owner",
        authority=_consumption_authority(
            tenant_id=tenant_id,
            operation="authorize_intervention",
            at=authorization_at,
        ),
        exact_operations=frozenset({spec.operation}),
        exact_target_refs=frozenset({"referent:channel:customer-success:v2"}),
        exact_field_paths=frozenset(
            {"parameters.channel_id", "parameters.text"}
        ),
        constraints={"maximum_messages": 1},
        use_budget=1,
        attempt_budget=1,
        decided_at=authorization_at,
        expires_at=start + timedelta(hours=2),
    )
    await AuthorizationApplier().apply(
        conn=conn,
        command=AuthorizationDecisionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AuthorizationApplier",
                responsibility="authorization",
                operation="authorize_intervention",
                at=authorization_at,
                key=f"effect-queue:authorization:{authorization.decision_id}",
            ),
            decision=authorization,
        ),
        now=authorization_at,
    )
    workflow_id = uuid7()
    task_id = uuid7()
    workflow_at = start + timedelta(minutes=5)
    workflow = WorkflowRunSnapshot(
        workflow_run_id=workflow_id,
        tenant_id=tenant_id,
        episode_id=episode_id,
        intervention_spec_digest=spec.spec_digest,
        workflow_spec_version_ref=spec.workflow_spec_version_ref,
        state=WorkflowRunState.PLANNED,
        authorization_decision_id=authorization.decision_id,
        prerequisite_refs=("authorization:live",),
        required_task_ids=(task_id,),
        completion_predicate="required task has a succeeded receipt",
        transition_reason="instantiate accepted workflow",
        created_at=workflow_at,
        updated_at=workflow_at,
    )
    await agency.apply_workflow_run(
        conn=conn,
        command=WorkflowRunCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="workflow_run",
                operation="create_workflow",
                at=workflow_at,
                key=f"effect-queue:workflow:{workflow_id}:planned",
            ),
            expected_version=0,
            snapshot=workflow,
        ),
        now=workflow_at,
    )
    active_at = start + timedelta(minutes=6)
    workflow = workflow.model_copy(
        update={
            "state": WorkflowRunState.ACTIVE,
            "transition_reason": "authorization and prerequisites are live",
            "updated_at": active_at,
        }
    )
    await agency.apply_workflow_run(
        conn=conn,
        command=WorkflowRunCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="workflow_run",
                operation="activate_workflow",
                at=active_at,
                key=f"effect-queue:workflow:{workflow_id}:active",
            ),
            expected_version=1,
            snapshot=workflow,
        ),
        now=active_at,
    )
    task_at = start + timedelta(minutes=7)
    task = TaskSnapshot(
        task_id=task_id,
        tenant_id=tenant_id,
        workflow_run_id=workflow_id,
        episode_id=episode_id,
        intervention_spec_digest=spec.spec_digest,
        task_kind=f"external_effect:{spec.operation}",
        state=TaskState.PLANNED,
        target_grounding_refs=("referent:channel:customer-success:v2",),
        authorization_decision_id=authorization.decision_id,
        external_effect_required=True,
        transition_reason="create exact governed effect task",
        created_at=task_at,
        updated_at=task_at,
    )
    for expected_version, state, offset in (
        (0, TaskState.PLANNED, 0),
        (1, TaskState.READY, 1),
        (2, TaskState.IN_PROGRESS, 2),
    ):
        at = task_at + timedelta(minutes=offset)
        task = task.model_copy(
            update={
                "state": state,
                "transition_reason": f"task enters {state.value}",
                "updated_at": at,
            }
        )
        await agency.apply_task(
            conn=conn,
            command=TaskCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="AgencyStateApplier",
                    responsibility="task",
                    operation=f"task_{state.value}",
                    at=at,
                    key=f"effect-queue:task:{task_id}:{state.value}",
                ),
                expected_version=expected_version,
                snapshot=task,
            ),
            now=at,
        )
    registered_at = start + timedelta(minutes=10)
    work = WorkObligation(
        obligation_id=uuid7(),
        lineage_id=uuid7(),
        tenant_id=tenant_id,
        generation=1,
        semantic_dedupe_key=f"effect-task:{task_id}",
        causal_parent_ref=f"task:{task_id}:v3",
        reason="authorized in-progress task requires provider execution",
        target_object_type="task",
        target_object_id=task_id,
        owner_writer_id="AgencyStateApplier",
        purpose="execute_governed_effect",
        risk_tier="high",
        expected_value=0.8,
        correctness_priority=0.95,
        intent_relevance=1.0,
        uncertainty_reduction_estimate=0.5,
        minimum_processing_class=ProcessingClass.R5_EXTERNAL_AGENCY,
        maximum_processing_class=ProcessingClass.R5_EXTERNAL_AGENCY,
        economic_envelope_ref="economic-envelope:effect-v1",
        maximum_attempts=1,
        deadline=start + timedelta(hours=1),
        generation_depth=0,
        terminal_condition="succeeded receipt or explicit no-effect fate",
        effect_possible=True,
        registered_at=registered_at,
    )
    await work_ledger.register(
        conn=conn,
        command=WorkObligationRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="register_work",
                at=registered_at,
                key=f"effect-queue:work:{work.obligation_id}:registered",
            ),
            obligation=work,
        ),
        now=registered_at,
    )
    scheduled_at = start + timedelta(minutes=11)
    decision = WorkDecision(
        decision_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=work.obligation_id,
        obligation_generation=1,
        from_state=WorkObligationState.REGISTERED,
        to_state=WorkObligationState.ELIGIBLE,
        selected_processing_class=ProcessingClass.R5_EXTERNAL_AGENCY,
        policy_version_ref="work-scheduling-policy:v1",
        why_no_cheaper_class_is_safe="the effect can change external state",
        reason="exact task Work is eligible",
        decided_at=scheduled_at,
    )
    await work_ledger.decide(
        conn=conn,
        command=WorkDecisionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="decide_work",
                at=scheduled_at,
                key=f"effect-queue:work:{work.obligation_id}:eligible",
            ),
            expected_version=1,
            decision=decision,
        ),
        now=scheduled_at,
    )
    lease = LeaseToken(
        lease_token_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=work.obligation_id,
        obligation_generation=1,
        fence=1,
        attempt=1,
        owner_ref="worker:agency-task-executor",
        heartbeat_deadline=scheduled_at + timedelta(minutes=10),
        expires_at=scheduled_at + timedelta(minutes=30),
        effect_possible=True,
        granted_at=scheduled_at,
    )
    lease_result = await work_ledger.grant_lease(
        conn=conn,
        command=LeaseGrantCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="grant_lease",
                at=scheduled_at,
                key=f"effect-queue:work:{work.obligation_id}:lease",
            ),
            expected_obligation_version=2,
            lease=lease,
        ),
        now=scheduled_at,
    )
    return lease_result.event_id, capabilities


async def _reserve_effect(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    context,
) -> ExternalEffectAttempt:
    plan = context.plan
    attempt = ExternalEffectAttempt(
        effect_attempt_id=plan.effect_attempt_id,
        lineage_id=plan.effect_lineage_id,
        tenant_id=tenant_id,
        generation=1,
        episode_id=plan.episode_id,
        task_id=plan.task_id,
        intervention_spec_digest=plan.intervention_spec_digest,
        authorization_decision_id=plan.authorization_decision_id,
        authorization_decision_version=1,
        capability_id=plan.capability_id,
        capability_version=plan.capability_version,
        capability_digest=plan.capability_digest,
        operation=plan.operation,
        canonical_request_hash=plan.canonical_request_hash,
        provider_idempotency_key=plan.provider_idempotency_key,
        target_grounding_refs=plan.target_grounding_refs,
        live_precondition_refs=(
            "preflight:slack-channel:C1:exists",
            "preflight:recipient-policy:permitted",
        ),
        work_obligation_id=plan.obligation_id,
        work_obligation_generation=plan.obligation_generation,
        lease_token_id=plan.lease_token_id,
        lease_fence=plan.lease_fence,
        dispatch_deadline=plan.dispatch_deadline,
        reconciliation_owner_ref=plan.reconciliation_owner_ref,
        compensation_policy_ref=plan.compensation_policy_ref,
        reserved_at=plan.reserved_at,
    )
    await ExecutionLedgerApplier().reserve(
        conn=conn,
        command=EffectReservationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ExecutionLedgerApplier",
                responsibility="external_effect",
                operation="reserve_effect",
                at=plan.reserved_at,
                key=f"effect-queue:reserve:{plan.effect_attempt_id}",
            ),
            attempt=attempt,
        ),
        now=plan.reserved_at,
    )
    return attempt


async def _transition_effect(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    attempt: ExternalEffectAttempt,
    expected_version: int,
    from_state: ExternalEffectState,
    to_state: ExternalEffectState,
    at: datetime,
    provider_refs: tuple[str, ...] = (),
    external_refs: tuple[str, ...] = (),
) -> UUID:
    receipt_id = uuid7()
    observation = EffectObservation(
        receipt_id=receipt_id,
        tenant_id=tenant_id,
        effect_attempt_id=attempt.effect_attempt_id,
        from_state=from_state,
        to_state=to_state,
        reason=f"simulated provider transition to {to_state.value}",
        provider_observation_refs=provider_refs,
        external_state_evidence_refs=external_refs,
        observed_at=at,
    )
    await ExecutionLedgerApplier().transition(
        conn=conn,
        command=EffectTransitionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ExecutionLedgerApplier",
                responsibility="external_effect",
                operation=f"effect_{to_state.value}",
                at=at,
                key=f"effect-queue:{attempt.effect_attempt_id}:{to_state.value}",
            ),
            expected_version=expected_version,
            observation=observation,
        ),
        now=at,
    )
    return receipt_id


async def test_leased_work_executes_to_exact_success_receipt(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = EffectExecutionRepo()
    tenant_id = uuid7()
    start = datetime.now(timezone.utc) - timedelta(minutes=15)
    async with fresh_db.acquire() as conn, conn.transaction():
        event_id, capabilities = await _leased_work_fixture(
            conn,
            tenant_id=tenant_id,
            start=start,
        )
        discovery_at = start + timedelta(minutes=12)
        assert await repo.discover_ready_work(
            conn,
            now=discovery_at,
            limit=10,
            tenant_id=tenant_id,
        ) == 1
        item = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=discovery_at + timedelta(seconds=1),
        )
        assert item is not None
        assert item.plan.capability_id == capabilities.capability_id
        assert item.plan.reserved_at == discovery_at
        duplicate = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=discovery_at + timedelta(minutes=1),
        )
        assert duplicate is not None and duplicate.id == item.id
        (claim,) = await repo.claim_ready_work(
            conn,
            worker_id="effect-executor:success",
            now=discovery_at,
            lease_duration=timedelta(minutes=10),
            limit=1,
        )
        assert claim.claim_token is not None
        context = await repo.load_claimed_context(
            conn,
            tenant_id=tenant_id,
            work_item_id=claim.id,
            worker_id="effect-executor:success",
            claim_token=claim.claim_token,
            now=discovery_at + timedelta(seconds=1),
        )
        attempt = await _reserve_effect(
            conn,
            tenant_id=tenant_id,
            context=context,
        )
        await _transition_effect(
            conn,
            tenant_id=tenant_id,
            attempt=attempt,
            expected_version=1,
            from_state=ExternalEffectState.RESERVED,
            to_state=ExternalEffectState.DISPATCH_INTENT_RECORDED,
            at=context.plan.reserved_at + timedelta(minutes=1),
        )
        await _transition_effect(
            conn,
            tenant_id=tenant_id,
            attempt=attempt,
            expected_version=2,
            from_state=ExternalEffectState.DISPATCH_INTENT_RECORDED,
            to_state=ExternalEffectState.ACKNOWLEDGED,
            at=context.plan.reserved_at + timedelta(minutes=2),
            provider_refs=("simulated-provider:accepted",),
        )
        success_receipt = await _transition_effect(
            conn,
            tenant_id=tenant_id,
            attempt=attempt,
            expected_version=3,
            from_state=ExternalEffectState.ACKNOWLEDGED,
            to_state=ExternalEffectState.SUCCEEDED,
            at=context.plan.reserved_at + timedelta(minutes=3),
            provider_refs=("simulated-provider:ok",),
            external_refs=("simulated-message:1717.001",),
        )
        dispatched = await repo.mark_dispatched(
            conn,
            tenant_id=tenant_id,
            work_item_id=claim.id,
            worker_id="effect-executor:success",
            claim_token=claim.claim_token,
            effect_version=4,
            receipt_id=success_receipt,
            now=context.plan.reserved_at + timedelta(minutes=3, seconds=1),
        )
        assert dispatched.status is EffectExecutionWorkStatus.DISPATCHED
        assert dispatched.applied_effect_state is ExternalEffectState.SUCCEEDED
        assert dispatched.execution_receipt_id == success_receipt


async def test_unknown_effect_requires_canonical_work_reconciliation(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = EffectExecutionRepo()
    tenant_id = uuid7()
    start = datetime.now(timezone.utc) - timedelta(minutes=15)
    async with fresh_db.acquire() as conn, conn.transaction():
        event_id, _ = await _leased_work_fixture(
            conn,
            tenant_id=tenant_id,
            start=start,
        )
        discovery_at = start + timedelta(minutes=12)
        item = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=discovery_at,
        )
        assert item is not None
        (abandoned,) = await repo.claim_ready_work(
            conn,
            worker_id="effect-executor:abandoned",
            now=discovery_at,
            lease_duration=timedelta(seconds=5),
            limit=1,
        )
        assert abandoned.claim_token is not None
        (claim,) = await repo.claim_ready_work(
            conn,
            worker_id="effect-executor:unknown",
            now=discovery_at + timedelta(seconds=10),
            lease_duration=timedelta(minutes=10),
            limit=1,
        )
        assert claim.claim_token is not None
        with pytest.raises(InvariantViolation, match="current live fence token"):
            await repo.schedule_retry(
                conn,
                tenant_id=tenant_id,
                work_item_id=abandoned.id,
                worker_id="effect-executor:abandoned",
                claim_token=abandoned.claim_token,
                now=discovery_at + timedelta(seconds=11),
                next_attempt_at=discovery_at + timedelta(minutes=1),
                failure_class="stale_worker",
                failure_reason="must not overwrite the recovered effect claim",
            )
        context = await repo.load_claimed_context(
            conn,
            tenant_id=tenant_id,
            work_item_id=claim.id,
            worker_id="effect-executor:unknown",
            claim_token=claim.claim_token,
            now=discovery_at + timedelta(seconds=11),
        )
        attempt = await _reserve_effect(
            conn,
            tenant_id=tenant_id,
            context=context,
        )
        await _transition_effect(
            conn,
            tenant_id=tenant_id,
            attempt=attempt,
            expected_version=1,
            from_state=ExternalEffectState.RESERVED,
            to_state=ExternalEffectState.DISPATCH_INTENT_RECORDED,
            at=context.plan.reserved_at + timedelta(minutes=1),
        )
        unknown_receipt = await _transition_effect(
            conn,
            tenant_id=tenant_id,
            attempt=attempt,
            expected_version=2,
            from_state=ExternalEffectState.DISPATCH_INTENT_RECORDED,
            to_state=ExternalEffectState.UNKNOWN,
            at=context.plan.reserved_at + timedelta(minutes=2),
        )
        with pytest.raises(
            InvariantViolation,
            match="canonical Work reconciliation",
        ):
            await repo.mark_unknown(
                conn,
                tenant_id=tenant_id,
                work_item_id=claim.id,
                worker_id="effect-executor:unknown",
                claim_token=claim.claim_token,
                effect_version=3,
                receipt_id=unknown_receipt,
                now=context.plan.reserved_at + timedelta(minutes=2, seconds=1),
            )
        resolution_at = context.plan.reserved_at + timedelta(minutes=2, seconds=2)
        await WorkLedgerApplier().resolve_lease(
            conn=conn,
            command=LeaseResolutionCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="WorkLedgerApplier",
                    responsibility="work_obligation",
                    operation="require_reconciliation",
                    at=resolution_at,
                    key=f"effect-queue:reconcile:{context.plan.obligation_id}",
                ),
                expected_obligation_version=3,
                expected_lease_version=1,
                resolution=LeaseResolution(
                    lease_token_id=context.plan.lease_token_id,
                    tenant_id=tenant_id,
                    obligation_id=context.plan.obligation_id,
                    obligation_generation=context.plan.obligation_generation,
                    fence=1,
                    to_lease_state=LeaseState.RECONCILIATION_REQUIRED,
                    to_work_state=WorkObligationState.RECONCILIATION_REQUIRED,
                    effect_may_have_occurred=True,
                    result_evidence_refs=(str(unknown_receipt),),
                    reason="provider outcome is unknown and must be reconciled",
                    resolved_at=resolution_at,
                ),
            ),
            now=resolution_at,
        )
        unknown = await repo.mark_unknown(
            conn,
            tenant_id=tenant_id,
            work_item_id=claim.id,
            worker_id="effect-executor:unknown",
            claim_token=claim.claim_token,
            effect_version=3,
            receipt_id=unknown_receipt,
            now=resolution_at + timedelta(seconds=1),
        )
        assert unknown.status is EffectExecutionWorkStatus.UNKNOWN
        assert unknown.applied_effect_state is ExternalEffectState.UNKNOWN


async def test_discovery_rejects_dispatch_window_without_resolution_margin(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = EffectExecutionRepo()
    tenant_id = uuid7()
    start = datetime.now(timezone.utc) - timedelta(minutes=15)
    async with fresh_db.acquire() as conn, conn.transaction():
        event_id, _ = await _leased_work_fixture(
            conn,
            tenant_id=tenant_id,
            start=start,
        )
        lease_expires_at = start + timedelta(minutes=41)
        with pytest.raises(InvariantViolation, match="no live dispatch window"):
            await repo.discover_from_event(
                conn,
                source_event_id=event_id,
                now=lease_expires_at - timedelta(seconds=20),
            )
