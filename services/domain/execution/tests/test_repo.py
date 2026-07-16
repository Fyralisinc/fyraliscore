from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.contracts import (
    ActionAdapterCapabilities,
    AdapterCapabilityRegistrationCommand,
    AgencyWriteContext,
    AuthorizationDecision,
    AuthorizationDecisionCommand,
    AuthorizationDisposition,
    CanonicalReferent,
    ConsequentialProposal,
    ConsequentialProposalFate,
    ConsequentialProposalRegistrationCommand,
    ConsequentialProposalReview,
    ConsequentialProposalReviewCommand,
    ConsumptionAuthorityContext,
    EffectObservation,
    EffectReservationCommand,
    EffectTransitionCommand,
    EntityLifecycleStatus,
    EpisodeStageFate,
    EpisodeStageLink,
    EpisodeUpdateCommand,
    ExternalEffectAttempt,
    ExternalEffectState,
    FailureClassification,
    FailureRecord,
    FailureRecordCommand,
    FailureState,
    InterventionEpisode,
    InterventionSpec,
    LeaseGrantCommand,
    LeaseHeartbeat,
    LeaseHeartbeatCommand,
    LeaseResolution,
    LeaseResolutionCommand,
    LeaseState,
    LeaseTakeover,
    LeaseTakeoverCommand,
    LeaseToken,
    ProcessingAuthorityContext,
    ProcessingClass,
    RestrictionSet,
    EffectUncertainty,
    OwnerTerminalizationRequest,
    OwnerTerminalizationRequestCommand,
    OwnerTerminalizationResolution,
    OwnerTerminalizationResolutionCommand,
    TaskCommand,
    TaskSnapshot,
    TaskState,
    WorkflowRunCommand,
    WorkflowRunSnapshot,
    WorkflowRunState,
    WorkDecision,
    WorkDecisionCommand,
    WorkObligation,
    WorkObligationRegistrationCommand,
    WorkObligationState,
    WorkStateTransition,
    WorkStateTransitionCommand,
    WriterCutoverState,
    WriterScopeEpoch,
)
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from lib.evaluation.execution import (
    ExecutionEvaluationScope,
    evaluate_execution_state,
)
from services.domain.execution.repo import (
    AgencyStateApplier,
    ExecutionLedgerApplier,
    WorkLedgerApplier,
)
from services.domain.execution.failure_repo import WorkFailureLedgerApplier
from services.domain.intent.repo import ProposalAppender
from services.domain.outcomes.repo import (
    AuthorizationApplier,
    EpisodeCoordinator,
)


START = datetime(2026, 7, 16, 9, 0, tzinfo=timezone.utc)


def _processing_authority(*, tenant_id: UUID, operation: str, at: datetime):
    return ProcessingAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id=f"service:{operation}",
        purpose="consequential_execution",
        operation=operation,
        object_types=RestrictionSet.unrestricted(),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("simulated-provider"),
        authority_basis_refs=frozenset({f"processing:{operation}"}),
        policy_version="execution-processing-v1",
        authority_epoch=1,
        decision_time=at - timedelta(minutes=1),
        expires_at=at + timedelta(days=2),
    )


def _consumption_authority(*, tenant_id: UUID, operation: str, at: datetime):
    return ConsumptionAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id="principal:operations-owner",
        purpose="consequential_execution",
        operation=operation,
        object_types=RestrictionSet.unrestricted(),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("governed-proposal"),
        authority_basis_refs=frozenset({"role:operations-owner"}),
        policy_version="execution-consumption-v1",
        authority_epoch=1,
        decision_time=at - timedelta(minutes=1),
        expires_at=at + timedelta(days=2),
    )


def _context(
    *,
    tenant_id: UUID,
    owner: str,
    responsibility: str,
    operation: str,
    at: datetime,
    key: str,
    authority: ProcessingAuthorityContext | None = None,
) -> AgencyWriteContext:
    return AgencyWriteContext(
        command_id=uuid7(),
        tenant_id=tenant_id,
        processing_authority=authority
        or _processing_authority(
            tenant_id=tenant_id,
            operation=operation,
            at=at,
        ),
        writer_scope_epoch=WriterScopeEpoch(
            scope_id=f"{responsibility}:{tenant_id}",
            tenant_id=tenant_id,
            semantic_responsibility=responsibility,
            source_partition=str(tenant_id),
            writer_owner=owner,
            epoch=1,
            state=WriterCutoverState.NEW_CANONICAL,
        ),
        idempotency_key=key,
        issued_at=at,
        expires_at=at + timedelta(hours=2),
    )


def _referent(tenant_id: UUID) -> CanonicalReferent:
    return CanonicalReferent(
        tenant_id=tenant_id,
        referent_id="channel:customer-success",
        referent_version=2,
        lifecycle_status=EntityLifecycleStatus.ACTIVE,
        predecessor_referent_refs=(),
        successor_referent_refs=(),
        birth_decision_ref="identity:channel:v2",
        positive_existence_evidence_refs=("slack:channel:C1",),
    )


async def _apply(pool, applier, method: str, *, command, now):
    async with pool.acquire() as conn, conn.transaction():
        return await getattr(applier, method)(conn=conn, command=command, now=now)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_effect_unknown_is_fenced_until_reconciled_and_receipted(fresh_db):
    tenant_id = uuid4()
    execution = ExecutionLedgerApplier()
    agency = AgencyStateApplier()
    work_ledger = WorkLedgerApplier()

    capability_at = START
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
        idempotency_retention_until=START + timedelta(days=3),
        reconciliation_supported=True,
        reconciliation_consistency_window_seconds=30,
        cancellation_supported=False,
        partial_effect_observable=True,
        compensation_supported=False,
        verified_at=capability_at,
        expires_at=START + timedelta(days=2),
    )
    capability_command = AdapterCapabilityRegistrationCommand(
        context=_context(
            tenant_id=tenant_id,
            owner="ExecutionLedgerApplier",
            responsibility="action_adapter_capability",
            operation="register_adapter_capability",
            at=capability_at,
            key="capability:slack-v3",
        ),
        expected_version=0,
        capabilities=capabilities,
    )
    await _apply(
        fresh_db,
        execution,
        "register_capabilities",
        command=capability_command,
        now=capability_at,
    )

    episode_at = START + timedelta(minutes=1)
    episode_id = uuid7()
    episode = InterventionEpisode(
        episode_id=episode_id,
        tenant_id=tenant_id,
        stage_links=(
            EpisodeStageLink(
                stage="proposal",
                fate=EpisodeStageFate.NOT_EXECUTED,
                reason="not registered yet",
            ),
        ),
        created_at=episode_at,
        updated_at=episode_at,
    )
    await _apply(
        fresh_db,
        EpisodeCoordinator(),
        "apply",
        command=EpisodeUpdateCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="EpisodeCoordinator",
                responsibility="intervention_episode",
                operation="create_episode",
                at=episode_at,
                key="episode:create",
            ),
            expected_version=0,
            episode=episode,
        ),
        now=episode_at,
    )

    proposal_at = START + timedelta(minutes=2)
    proposal_authority = _processing_authority(
        tenant_id=tenant_id,
        operation="register_proposal",
        at=proposal_at,
    )
    spec = InterventionSpec(
        spec_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        target_referent=_referent(tenant_id),
        target_version="slack-channel-v2",
        operation="send_message",
        parameters={"channel_id": "C1", "text": "Review the Atlas escalation"},
        comparator={"delivery": "no_message"},
        outcome_metric="capable_recipient_acknowledged",
        outcome_window_start=START + timedelta(hours=1),
        outcome_window_end=START + timedelta(days=1),
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
        rationale="A capable owner must review the customer escalation",
        alternative_refs=("alternative:in-app-only", "alternative:no-message"),
        source_refs=("concern:atlas-escalation",),
        processing_authority=proposal_authority,
        processing_authority_fingerprint=proposal_authority.fingerprint,
        created_at=proposal_at,
        review_due_at=proposal_at + timedelta(hours=1),
    )
    await _apply(
        fresh_db,
        ProposalAppender(),
        "append_consequential",
        command=ConsequentialProposalRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ProposalAppender",
                responsibility="consequential_proposal",
                operation="register_proposal",
                at=proposal_at,
                key="proposal:delivery",
                authority=proposal_authority,
            ),
            proposal=proposal,
        ),
        now=proposal_at,
    )

    review_at = START + timedelta(minutes=3)
    review = ConsequentialProposalReview(
        review_id=uuid7(),
        tenant_id=tenant_id,
        proposal_id=proposal.proposal_id,
        proposal_version=1,
        proposal_digest=proposal.proposal_digest,
        intervention_spec_digest=spec.spec_digest,
        from_fate=ConsequentialProposalFate.OPEN,
        to_fate=ConsequentialProposalFate.ACCEPTED_FOR_AUTHORIZATION,
        principal_or_policy_ref="principal:operations-owner",
        authority=_consumption_authority(
            tenant_id=tenant_id,
            operation="review_proposal",
            at=review_at,
        ),
        reason="bounded message is appropriate",
        decided_at=review_at,
    )
    await _apply(
        fresh_db,
        ProposalAppender(),
        "review_consequential",
        command=ConsequentialProposalReviewCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ProposalAppender",
                responsibility="consequential_proposal",
                operation="review_proposal",
                at=review_at,
                key="proposal:delivery:accept",
            ),
            review=review,
        ),
        now=review_at,
    )

    authorization_at = START + timedelta(minutes=4)
    decision = AuthorizationDecision(
        decision_id=uuid7(),
        tenant_id=tenant_id,
        proposal_id=proposal.proposal_id,
        proposal_digest=proposal.proposal_digest,
        intervention_spec_digest=spec.spec_digest,
        disposition=AuthorizationDisposition.AUTHORIZED,
        principal_or_policy_ref="principal:operations-owner",
        authority=_consumption_authority(
            tenant_id=tenant_id,
            operation="authorize_intervention",
            at=authorization_at,
        ),
        exact_operations=frozenset({"send_message"}),
        exact_target_refs=frozenset({"referent:channel:customer-success:v2"}),
        exact_field_paths=frozenset({"parameters.channel_id", "parameters.text"}),
        constraints={"maximum_messages": 1},
        use_budget=1,
        attempt_budget=2,
        decided_at=authorization_at,
        expires_at=authorization_at + timedelta(minutes=15),
    )
    await _apply(
        fresh_db,
        AuthorizationApplier(),
        "apply",
        command=AuthorizationDecisionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AuthorizationApplier",
                responsibility="authorization",
                operation="authorize_intervention",
                at=authorization_at,
                key="authorization:delivery",
            ),
            decision=decision,
        ),
        now=authorization_at,
    )

    workflow_id = uuid7()
    task_id = uuid7()
    workflow_at = START + timedelta(minutes=5)
    workflow = WorkflowRunSnapshot(
        workflow_run_id=workflow_id,
        tenant_id=tenant_id,
        episode_id=episode_id,
        intervention_spec_digest=spec.spec_digest,
        workflow_spec_version_ref="workflow:governed-delivery:v1",
        state=WorkflowRunState.PLANNED,
        authorization_decision_id=decision.decision_id,
        prerequisite_refs=("authorization:live",),
        required_task_ids=(task_id,),
        completion_predicate="required delivery task has succeeded receipt",
        transition_reason="instantiate accepted workflow",
        created_at=workflow_at,
        updated_at=workflow_at,
    )
    await _apply(
        fresh_db,
        agency,
        "apply_workflow_run",
        command=WorkflowRunCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="workflow_run",
                operation="create_workflow",
                at=workflow_at,
                key="workflow:create",
            ),
            expected_version=0,
            snapshot=workflow,
        ),
        now=workflow_at,
    )
    workflow_active_at = START + timedelta(minutes=6)
    workflow = workflow.model_copy(
        update={
            "state": WorkflowRunState.ACTIVE,
            "transition_reason": "authorization and prerequisites are live",
            "updated_at": workflow_active_at,
        }
    )
    await _apply(
        fresh_db,
        agency,
        "apply_workflow_run",
        command=WorkflowRunCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="workflow_run",
                operation="activate_workflow",
                at=workflow_active_at,
                key="workflow:activate",
            ),
            expected_version=1,
            snapshot=workflow,
        ),
        now=workflow_active_at,
    )

    task_at = START + timedelta(minutes=7)
    task = TaskSnapshot(
        task_id=task_id,
        tenant_id=tenant_id,
        workflow_run_id=workflow_id,
        episode_id=episode_id,
        intervention_spec_digest=spec.spec_digest,
        task_kind="external_effect",
        state=TaskState.PLANNED,
        target_grounding_refs=("referent:channel:customer-success:v2",),
        authorization_decision_id=decision.decision_id,
        external_effect_required=True,
        transition_reason="create delivery task",
        created_at=task_at,
        updated_at=task_at,
    )
    for version, state, minute in (
        (0, TaskState.PLANNED, 7),
        (1, TaskState.READY, 8),
        (2, TaskState.IN_PROGRESS, 9),
    ):
        at = START + timedelta(minutes=minute)
        task = task.model_copy(
            update={
                "state": state,
                "transition_reason": f"task enters {state}",
                "updated_at": at,
            }
        )
        await _apply(
            fresh_db,
            agency,
            "apply_task",
            command=TaskCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="AgencyStateApplier",
                    responsibility="task",
                    operation=f"task_{state}",
                    at=at,
                    key=f"task:{state}",
                ),
                expected_version=version,
                snapshot=task,
            ),
            now=at,
        )

    work_at = START + timedelta(minutes=10)
    obligation = WorkObligation(
        obligation_id=uuid7(),
        lineage_id=uuid7(),
        tenant_id=tenant_id,
        generation=1,
        semantic_dedupe_key=f"delivery-task:{task_id}",
        causal_parent_ref=f"task:{task_id}:v3",
        reason="authorized in-progress delivery task",
        target_object_type="task",
        target_object_id=task_id,
        owner_writer_id="AgencyStateApplier",
        purpose="execute_governed_delivery",
        risk_tier="high",
        expected_value=0.8,
        correctness_priority=0.95,
        intent_relevance=1.0,
        uncertainty_reduction_estimate=0.5,
        minimum_processing_class=ProcessingClass.R5_EXTERNAL_AGENCY,
        maximum_processing_class=ProcessingClass.R5_EXTERNAL_AGENCY,
        economic_envelope_ref="economic-envelope:delivery-v1",
        maximum_attempts=2,
        deadline=START + timedelta(hours=1),
        generation_depth=0,
        terminal_condition="succeeded effect receipt or explicit no-effect fate",
        effect_possible=True,
        registered_at=work_at,
    )
    await _apply(
        fresh_db,
        work_ledger,
        "register",
        command=WorkObligationRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="register_work",
                at=work_at,
                key="work:register",
            ),
            obligation=obligation,
        ),
        now=work_at,
    )
    decision_at = START + timedelta(minutes=11)
    work_decision = WorkDecision(
        decision_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=obligation.obligation_id,
        obligation_generation=1,
        from_state=WorkObligationState.REGISTERED,
        to_state=WorkObligationState.ELIGIBLE,
        selected_processing_class=ProcessingClass.R5_EXTERNAL_AGENCY,
        policy_version_ref="work-policy:v1",
        why_no_cheaper_class_is_safe="this path can cause an external message",
        reason="authorized task is due",
        decided_at=decision_at,
    )
    await _apply(
        fresh_db,
        work_ledger,
        "decide",
        command=WorkDecisionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="decide_work",
                at=decision_at,
                key="work:eligible",
            ),
            expected_version=1,
            decision=work_decision,
        ),
        now=decision_at,
    )
    lease_at = START + timedelta(minutes=12)
    lease = LeaseToken(
        lease_token_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=obligation.obligation_id,
        obligation_generation=1,
        fence=1,
        attempt=1,
        owner_ref="worker:delivery-1",
        heartbeat_deadline=lease_at + timedelta(minutes=10),
        expires_at=lease_at + timedelta(minutes=30),
        effect_possible=True,
        granted_at=lease_at,
    )
    await _apply(
        fresh_db,
        work_ledger,
        "grant_lease",
        command=LeaseGrantCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="grant_lease",
                at=lease_at,
                key="work:lease:1",
            ),
            expected_obligation_version=2,
            lease=lease,
        ),
        now=lease_at,
    )

    effect_at = START + timedelta(minutes=13)
    attempt = ExternalEffectAttempt(
        effect_attempt_id=uuid7(),
        lineage_id=uuid7(),
        tenant_id=tenant_id,
        generation=1,
        episode_id=episode_id,
        task_id=task_id,
        intervention_spec_digest=spec.spec_digest,
        authorization_decision_id=decision.decision_id,
        capability_id=capabilities.capability_id,
        capability_version=capabilities.capability_version,
        capability_digest=capabilities.capability_digest,
        operation="send_message",
        canonical_request_hash="e" * 64,
        provider_idempotency_key="client-message:atlas-escalation",
        target_grounding_refs=("referent:channel:customer-success:v2",),
        live_precondition_refs=("slack-channel:C1:exists",),
        work_obligation_id=obligation.obligation_id,
        work_obligation_generation=1,
        lease_token_id=lease.lease_token_id,
        lease_fence=1,
        dispatch_deadline=effect_at + timedelta(minutes=5),
        reconciliation_owner_ref="service:slack-reconciler",
        compensation_policy_ref="compensation:post-correction-only",
        reserved_at=effect_at,
    )
    await _apply(
        fresh_db,
        execution,
        "reserve",
        command=EffectReservationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ExecutionLedgerApplier",
                responsibility="external_effect",
                operation="reserve_effect",
                at=effect_at,
                key="effect:reserve:1",
            ),
            attempt=attempt,
        ),
        now=effect_at,
    )

    effect_version = 1
    effect_state = ExternalEffectState.RESERVED
    receipts = []
    for minute, target, reason, provider_refs, external_refs in (
        (14, ExternalEffectState.DISPATCH_INTENT_RECORDED, "commit before call", (), ()),
        (15, ExternalEffectState.UNKNOWN, "provider timeout", (), ()),
        (16, ExternalEffectState.RECONCILING, "query provider by key", (), ()),
        (
            17,
            ExternalEffectState.RECONCILED_NO_EFFECT,
            "provider lookup proved the timed-out call caused no effect",
            ("slack-reconciliation:not-found",),
            ("slack-message-key:atlas-escalation:absent",),
        ),
    ):
        at = START + timedelta(minutes=minute)
        observation = EffectObservation(
            receipt_id=uuid7(),
            tenant_id=tenant_id,
            effect_attempt_id=attempt.effect_attempt_id,
            from_state=effect_state,
            to_state=target,
            reason=reason,
            provider_observation_refs=provider_refs,
            external_state_evidence_refs=external_refs,
            observed_at=at,
        )
        transition_command = EffectTransitionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ExecutionLedgerApplier",
                responsibility="external_effect",
                operation=f"effect_{target}",
                at=at,
                key=f"effect:{target}",
            ),
            expected_version=effect_version,
            observation=observation,
        )
        first = await _apply(
            fresh_db,
            execution,
            "transition",
            command=transition_command,
            now=at,
        )
        if target is ExternalEffectState.DISPATCH_INTENT_RECORDED:
            duplicate = await _apply(
                fresh_db,
                execution,
                "transition",
                command=transition_command,
                now=at,
            )
            assert duplicate.duplicate
            assert duplicate.object_version == first.object_version
        receipts.append(observation.receipt_id)
        effect_version += 1
        effect_state = target

        if target is ExternalEffectState.UNKNOWN:
            retry = attempt.model_copy(
                update={
                    "effect_attempt_id": uuid7(),
                    "generation": 2,
                    "prior_attempt_id": attempt.effect_attempt_id,
                    "reserved_at": at,
                    "dispatch_deadline": at + timedelta(minutes=5),
                }
            )
            retry_command = EffectReservationCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="ExecutionLedgerApplier",
                    responsibility="external_effect",
                    operation="unsafe_retry",
                    at=at,
                    key="effect:unsafe-retry",
                ),
                attempt=retry,
            )
            with pytest.raises(InvariantViolation, match="terminal known-no-effect"):
                async with fresh_db.acquire() as conn, conn.transaction():
                    await execution.reserve(conn=conn, command=retry_command, now=at)

            premature_task = task.model_copy(
                update={
                    "state": TaskState.COMPLETED,
                    "effect_attempt_id": attempt.effect_attempt_id,
                    "execution_receipt_id": observation.receipt_id,
                    "completion_evidence_refs": (str(observation.receipt_id),),
                    "transition_reason": "timeout is not completion",
                    "updated_at": at,
                }
            )
            with pytest.raises(InvariantViolation, match="succeeded receipt"):
                async with fresh_db.acquire() as conn, conn.transaction():
                    await agency.apply_task(
                        conn=conn,
                        command=TaskCommand(
                            context=_context(
                                tenant_id=tenant_id,
                                owner="AgencyStateApplier",
                                responsibility="task",
                                operation="premature_complete",
                                at=at,
                                key="task:premature-complete",
                            ),
                            expected_version=3,
                            snapshot=premature_task,
                        ),
                        now=at,
                    )

    retry_at = START + timedelta(minutes=17, seconds=10)
    retry_attempt = attempt.model_copy(
        update={
            "effect_attempt_id": uuid7(),
            "generation": 2,
            "prior_attempt_id": attempt.effect_attempt_id,
            "reserved_at": retry_at,
            "dispatch_deadline": retry_at + timedelta(minutes=5),
        }
    )
    await _apply(
        fresh_db,
        execution,
        "reserve",
        command=EffectReservationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ExecutionLedgerApplier",
                responsibility="external_effect",
                operation="reserve_safe_retry",
                at=retry_at,
                key="effect:safe-retry:reserve",
            ),
            attempt=retry_attempt,
        ),
        now=retry_at,
    )

    retry_version = 1
    retry_state = ExternalEffectState.RESERVED
    for second, target, reason, provider_refs, external_refs in (
        (
            20,
            ExternalEffectState.DISPATCH_INTENT_RECORDED,
            "commit successor dispatch intent before provider call",
            (),
            (),
        ),
        (
            30,
            ExternalEffectState.ACKNOWLEDGED,
            "provider acknowledged generation two",
            ("slack-response:accepted",),
            (),
        ),
        (
            40,
            ExternalEffectState.SUCCEEDED,
            "provider message observed for generation two",
            ("slack-response:ok",),
            ("slack-message:1717.002",),
        ),
    ):
        observed_at = START + timedelta(minutes=17, seconds=second)
        observation = EffectObservation(
            receipt_id=uuid7(),
            tenant_id=tenant_id,
            effect_attempt_id=retry_attempt.effect_attempt_id,
            from_state=retry_state,
            to_state=target,
            reason=reason,
            provider_observation_refs=provider_refs,
            external_state_evidence_refs=external_refs,
            observed_at=observed_at,
        )
        await _apply(
            fresh_db,
            execution,
            "transition",
            command=EffectTransitionCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="ExecutionLedgerApplier",
                    responsibility="external_effect",
                    operation=f"safe_retry_{target}",
                    at=observed_at,
                    key=f"effect:safe-retry:{target}",
                ),
                expected_version=retry_version,
                observation=observation,
            ),
            now=observed_at,
        )
        receipts.append(observation.receipt_id)
        retry_version += 1
        retry_state = target

    succeeded_receipt_id = receipts[-1]
    work_complete_at = START + timedelta(minutes=18)
    await _apply(
        fresh_db,
        work_ledger,
        "resolve_lease",
        command=LeaseResolutionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="complete_work",
                at=work_complete_at,
                key="work:complete",
            ),
            expected_obligation_version=3,
            expected_lease_version=1,
            resolution=LeaseResolution(
                lease_token_id=lease.lease_token_id,
                tenant_id=tenant_id,
                obligation_id=obligation.obligation_id,
                obligation_generation=1,
                fence=1,
                to_lease_state=LeaseState.COMPLETED,
                to_work_state=WorkObligationState.COMPLETED,
                effect_may_have_occurred=True,
                result_evidence_refs=(str(succeeded_receipt_id),),
                reason="succeeded effect receipt observed",
                resolved_at=work_complete_at,
            ),
        ),
        now=work_complete_at,
    )

    task_complete_at = START + timedelta(minutes=19)
    task = task.model_copy(
        update={
            "state": TaskState.COMPLETED,
            "effect_attempt_id": retry_attempt.effect_attempt_id,
            "execution_receipt_id": succeeded_receipt_id,
            "completion_evidence_refs": (str(succeeded_receipt_id),),
            "transition_reason": "exact succeeded execution receipt",
            "updated_at": task_complete_at,
        }
    )
    task_completion_result = await _apply(
        fresh_db,
        agency,
        "apply_task",
        command=TaskCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="task",
                operation="complete_task",
                at=task_complete_at,
                key="task:complete",
            ),
            expected_version=3,
            snapshot=task,
        ),
        now=task_complete_at,
    )
    workflow_complete_at = START + timedelta(minutes=20)
    workflow = workflow.model_copy(
        update={
            "state": WorkflowRunState.COMPLETED,
            "completion_evidence_refs": (str(succeeded_receipt_id),),
            "transition_reason": "all required tasks completed",
            "updated_at": workflow_complete_at,
        }
    )
    await _apply(
        fresh_db,
        agency,
        "apply_workflow_run",
        command=WorkflowRunCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="workflow_run",
                operation="complete_workflow",
                at=workflow_complete_at,
                key="workflow:complete",
            ),
            expected_version=2,
            snapshot=workflow,
        ),
        now=workflow_complete_at,
    )

    async with fresh_db.acquire() as conn:
        states = {
            "workflow": await conn.fetchval(
                "SELECT current_state FROM agency_workflow_run_heads WHERE tenant_id=$1",
                tenant_id,
            ),
            "task": await conn.fetchval(
                "SELECT current_state FROM agency_task_heads WHERE tenant_id=$1",
                tenant_id,
            ),
            "work": await conn.fetchval(
                "SELECT current_state FROM work_obligation_heads WHERE tenant_id=$1",
                tenant_id,
            ),
            "effect": await conn.fetchval(
                """
                SELECT h.current_state
                FROM external_effect_attempt_lineage_heads l
                JOIN external_effect_attempt_heads h
                  ON h.tenant_id=l.tenant_id
                 AND h.effect_attempt_id=l.current_effect_attempt_id
                WHERE l.tenant_id=$1 AND l.lineage_id=$2
                """,
                tenant_id,
                attempt.lineage_id,
            ),
        }
        command_count = await conn.fetchval(
            """
            SELECT count(*) FROM agency_command_results
            WHERE tenant_id=$1 AND writer_id IN (
              'AgencyStateApplier','WorkLedgerApplier','ExecutionLedgerApplier'
            )
            """,
            tenant_id,
        )
        event_count = await conn.fetchval(
            """
            SELECT count(*) FROM agency_canonical_events
            WHERE tenant_id=$1 AND writer_id IN (
              'AgencyStateApplier','WorkLedgerApplier','ExecutionLedgerApplier'
            )
            """,
            tenant_id,
        )
        outbox_count = await conn.fetchval(
            """
            SELECT count(*) FROM agency_outbox_records o
            JOIN agency_canonical_events e ON e.id=o.event_id
            WHERE o.tenant_id=$1 AND e.writer_id IN (
              'AgencyStateApplier','WorkLedgerApplier','ExecutionLedgerApplier'
            )
            """,
            tenant_id,
        )
        receipt_count = await conn.fetchval(
            "SELECT count(*) FROM execution_receipts WHERE tenant_id=$1",
            tenant_id,
        )
    assert states == {
        "workflow": "completed",
        "task": "completed",
        "work": "completed",
        "effect": "succeeded",
    }
    assert command_count == event_count == outbox_count == 21
    assert receipt_count == 7

    with pytest.raises(asyncpg.PostgresError, match="append-only"):
        async with fresh_db.acquire() as conn, conn.transaction():
            await conn.execute(
                """
                UPDATE execution_receipts SET effect_state='failed'
                WHERE tenant_id=$1 AND receipt_id=$2
                """,
                tenant_id,
                succeeded_receipt_id,
            )

    # A second, pure-computation obligation exercises the exact cross-owner
    # failure handshake. WorkLedger cannot manufacture the task's terminal
    # state; it consumes AgencyStateApplier's already committed task result.
    failure_ledger = WorkFailureLedgerApplier()
    recovery_work_at = START + timedelta(minutes=21)
    recovery_work = WorkObligation(
        obligation_id=uuid7(),
        lineage_id=uuid7(),
        tenant_id=tenant_id,
        generation=1,
        semantic_dedupe_key=f"receipt-index:{task_id}",
        causal_parent_ref=f"task:{task_id}:v4",
        reason="index the task's terminal receipt for a downstream consumer",
        target_object_type="task",
        target_object_id=task_id,
        owner_writer_id="AgencyStateApplier",
        purpose="post_execution_indexing",
        risk_tier="medium",
        expected_value=0.4,
        correctness_priority=0.8,
        intent_relevance=0.5,
        uncertainty_reduction_estimate=0.2,
        minimum_processing_class=ProcessingClass.R2_PROVISIONAL_GROUNDING,
        maximum_processing_class=ProcessingClass.R3_DURABLE_UNDERSTANDING,
        economic_envelope_ref="economic-envelope:indexing-v1",
        maximum_attempts=2,
        deadline=START + timedelta(hours=1),
        generation_depth=0,
        terminal_condition="owner task fate is acknowledged or explicitly escalated",
        effect_possible=False,
        registered_at=recovery_work_at,
    )
    await _apply(
        fresh_db,
        work_ledger,
        "register",
        command=WorkObligationRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="register_recovery_work",
                at=recovery_work_at,
                key="recovery-work:register",
            ),
            obligation=recovery_work,
        ),
        now=recovery_work_at,
    )
    recovery_decision_at = START + timedelta(minutes=22)
    await _apply(
        fresh_db,
        work_ledger,
        "decide",
        command=WorkDecisionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="decide_recovery_work",
                at=recovery_decision_at,
                key="recovery-work:eligible",
            ),
            expected_version=1,
            decision=WorkDecision(
                decision_id=uuid7(),
                tenant_id=tenant_id,
                obligation_id=recovery_work.obligation_id,
                obligation_generation=1,
                from_state=WorkObligationState.REGISTERED,
                to_state=WorkObligationState.ELIGIBLE,
                selected_processing_class=ProcessingClass.R2_PROVISIONAL_GROUNDING,
                policy_version_ref="work-policy:v1",
                why_no_cheaper_class_is_safe="durable receipt indexing needs grounding",
                reason="terminal task receipt is available",
                decided_at=recovery_decision_at,
            ),
        ),
        now=recovery_decision_at,
    )
    recovery_lease_at = START + timedelta(minutes=23)
    recovery_lease = LeaseToken(
        lease_token_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=recovery_work.obligation_id,
        obligation_generation=1,
        fence=1,
        attempt=1,
        owner_ref="worker:indexer-1",
        heartbeat_deadline=recovery_lease_at + timedelta(minutes=1),
        expires_at=recovery_lease_at + timedelta(minutes=15),
        effect_possible=False,
        granted_at=recovery_lease_at,
    )
    await _apply(
        fresh_db,
        work_ledger,
        "grant_lease",
        command=LeaseGrantCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="lease_recovery_work",
                at=recovery_lease_at,
                key="recovery-work:lease",
            ),
            expected_obligation_version=2,
            lease=recovery_lease,
        ),
        now=recovery_lease_at,
    )
    heartbeat_at = recovery_lease_at + timedelta(seconds=30)
    extended_heartbeat_deadline = START + timedelta(minutes=25)
    await _apply(
        fresh_db,
        work_ledger,
        "heartbeat_lease",
        command=LeaseHeartbeatCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="heartbeat_recovery_work",
                at=heartbeat_at,
                key="recovery-work:heartbeat",
            ),
            expected_lease_version=1,
            heartbeat=LeaseHeartbeat(
                heartbeat_id=uuid7(),
                tenant_id=tenant_id,
                lease_token_id=recovery_lease.lease_token_id,
                obligation_id=recovery_work.obligation_id,
                obligation_generation=1,
                fence=1,
                owner_ref=recovery_lease.owner_ref,
                expected_heartbeat_deadline=recovery_lease.heartbeat_deadline,
                extended_heartbeat_deadline=extended_heartbeat_deadline,
                lease_expires_at=recovery_lease.expires_at,
                heartbeat_at=heartbeat_at,
            ),
        ),
        now=heartbeat_at,
    )
    takeover_at = extended_heartbeat_deadline
    successor_lease = LeaseToken(
        lease_token_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=recovery_work.obligation_id,
        obligation_generation=1,
        fence=2,
        attempt=2,
        owner_ref="worker:indexer-2",
        heartbeat_deadline=takeover_at + timedelta(minutes=2),
        expires_at=takeover_at + timedelta(minutes=10),
        effect_possible=False,
        granted_at=takeover_at,
    )
    await _apply(
        fresh_db,
        work_ledger,
        "take_over_lease",
        command=LeaseTakeoverCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="take_over_recovery_work",
                at=takeover_at,
                key="recovery-work:takeover",
            ),
            expected_obligation_version=3,
            expected_predecessor_lease_version=2,
            takeover=LeaseTakeover(
                takeover_id=uuid7(),
                tenant_id=tenant_id,
                obligation_id=recovery_work.obligation_id,
                obligation_generation=1,
                predecessor_lease_token_id=recovery_lease.lease_token_id,
                predecessor_fence=1,
                predecessor_attempt=1,
                predecessor_owner_ref=recovery_lease.owner_ref,
                predecessor_heartbeat_deadline=extended_heartbeat_deadline,
                successor=successor_lease,
                reason="first indexer missed its extended heartbeat",
                taken_over_at=takeover_at,
            ),
        ),
        now=takeover_at,
    )
    quarantine_at = START + timedelta(minutes=26)
    await _apply(
        fresh_db,
        work_ledger,
        "resolve_lease",
        command=LeaseResolutionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="quarantine_recovery_work",
                at=quarantine_at,
                key="recovery-work:quarantine",
            ),
            expected_obligation_version=4,
            expected_lease_version=1,
            resolution=LeaseResolution(
                lease_token_id=successor_lease.lease_token_id,
                tenant_id=tenant_id,
                obligation_id=recovery_work.obligation_id,
                obligation_generation=1,
                fence=2,
                to_lease_state=LeaseState.TERMINAL,
                to_work_state=WorkObligationState.QUARANTINED,
                effect_may_have_occurred=False,
                reason="index payload repeatedly violates the consumer schema",
                resolved_at=quarantine_at,
            ),
        ),
        now=quarantine_at,
    )

    failure_at = START + timedelta(minutes=27)
    failure = FailureRecord(
        failure_id=uuid7(),
        lineage_id=uuid7(),
        tenant_id=tenant_id,
        generation=1,
        work_obligation_id=recovery_work.obligation_id,
        work_obligation_generation=1,
        causal_operation="index_execution_receipt",
        classification=FailureClassification.POISON_INPUT,
        owner_writer_id="WorkLedgerApplier",
        semantic_owner_writer_id="AgencyStateApplier",
        target_object_type="task",
        target_object_id=task_id,
        original_semantic_idempotency_key="recovery-work:takeover",
        attempt=2,
        maximum_attempts=2,
        deadline=recovery_work.deadline,
        next_action="classify poison input without declaring task failure",
        effect_uncertainty=EffectUncertainty.NONE,
        state=FailureState.DETECTED,
        reason="consumer schema rejected the receipt projection",
        created_at=failure_at,
        updated_at=failure_at,
    )
    await _apply(
        fresh_db,
        failure_ledger,
        "apply_failure",
        command=FailureRecordCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="failure_record",
                operation="detect_failure",
                at=failure_at,
                key="failure:detect",
            ),
            expected_version=0,
            record=failure,
        ),
        now=failure_at,
    )
    for expected, state, minute in (
        (1, FailureState.CLASSIFIED, 28),
        (2, FailureState.QUARANTINED, 29),
    ):
        at = START + timedelta(minutes=minute)
        failure = failure.model_copy(
            update={
                "state": state,
                "next_action": (
                    "quarantine and request semantic-owner terminal result"
                    if state is FailureState.QUARANTINED
                    else "classification confirms poison input"
                ),
                "reason": f"failure enters {state}",
                "updated_at": at,
            }
        )
        await _apply(
            fresh_db,
            failure_ledger,
            "apply_failure",
            command=FailureRecordCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="WorkLedgerApplier",
                    responsibility="failure_record",
                    operation=f"failure_{state}",
                    at=at,
                    key=f"failure:{state}",
                ),
                expected_version=expected,
                record=failure,
            ),
            now=at,
        )

    request_at = START + timedelta(minutes=30)
    terminalization_request = OwnerTerminalizationRequest(
        request_id=uuid7(),
        tenant_id=tenant_id,
        failure_id=failure.failure_id,
        failure_generation=1,
        from_failure_state=FailureState.QUARANTINED,
        work_obligation_id=recovery_work.obligation_id,
        work_obligation_generation=1,
        from_work_state=WorkObligationState.QUARANTINED,
        semantic_owner_writer_id="AgencyStateApplier",
        target_object_type="task",
        target_object_id=task_id,
        acceptable_owner_terminal_states=frozenset({"completed"}),
        terminal_reason="work cannot declare the Agency-owned task fate",
        requested_at=request_at,
    )
    await _apply(
        fresh_db,
        failure_ledger,
        "request_owner_terminalization",
        command=OwnerTerminalizationRequestCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="failure_record",
                operation="request_owner_terminalization",
                at=request_at,
                key="failure:owner-request",
            ),
            expected_failure_version=3,
            expected_work_version=5,
            request=terminalization_request,
        ),
        now=request_at,
    )
    resolution_at = START + timedelta(minutes=31)
    terminalization_resolution = OwnerTerminalizationResolution(
        resolution_id=uuid7(),
        tenant_id=tenant_id,
        request_id=terminalization_request.request_id,
        failure_id=failure.failure_id,
        failure_generation=1,
        work_obligation_id=recovery_work.obligation_id,
        work_obligation_generation=1,
        owner_command_result_id=task_completion_result.command_result_id,
        observed_owner_writer_id="AgencyStateApplier",
        observed_owner_object_type="task",
        observed_owner_object_id=task_id,
        observed_owner_object_version=task_completion_result.object_version,
        observed_owner_terminal_state="completed",
        to_failure_state=FailureState.RESOLVED,
        to_work_state=WorkObligationState.COMPLETED,
        reason="exact owner result proves the target task already completed",
        resolved_at=resolution_at,
    )
    await _apply(
        fresh_db,
        failure_ledger,
        "resolve_owner_terminalization",
        command=OwnerTerminalizationResolutionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="failure_record",
                operation="resolve_owner_terminalization",
                at=resolution_at,
                key="failure:owner-resolution",
            ),
            expected_failure_version=4,
            expected_work_version=6,
            resolution=terminalization_resolution,
        ),
        now=resolution_at,
    )

    async with fresh_db.acquire() as conn:
        evaluation = await evaluate_execution_state(
            conn,
            scope=ExecutionEvaluationScope(
                tenant_id=tenant_id,
                start=START - timedelta(days=1),
                end=START + timedelta(days=1),
                run_id="execution-component-replay",
            ),
            artifact_refs=("pytest://execution-component-replay",),
        )
    assert evaluation.incident_counts == {}
    assert evaluation.workflow_history_integrity_rate == 1.0
    assert evaluation.task_history_integrity_rate == 1.0
    assert evaluation.work_history_integrity_rate == 1.0
    assert evaluation.lease_integrity_rate == 1.0
    assert evaluation.lease_heartbeat_count == 1
    assert evaluation.missed_heartbeat_takeover_count == 1
    assert evaluation.safe_missed_heartbeat_takeover_count == 1
    assert evaluation.takeover_safety_rate == 1.0
    assert evaluation.effect_history_integrity_rate == 1.0
    assert evaluation.effect_continuity_rate == 1.0
    assert evaluation.external_task_receipt_rate == 1.0
    assert evaluation.receipt_closure_rate == 1.0
    assert evaluation.retry_attempt_count == 1
    assert evaluation.safe_retry_attempt_count == 1
    assert evaluation.retry_safety_rate == 1.0
    assert evaluation.command_reconstructability_rate == 1.0
    assert evaluation.command_event_coverage == 1.0
    assert evaluation.command_outbox_coverage == 1.0
    assert evaluation.failure_history_integrity_rate == 1.0
    assert evaluation.owner_terminalization_closure_rate == 1.0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_work_redrive_requires_authorized_parent_and_advances_lineage(fresh_db):
    tenant_id = uuid4()
    ledger = WorkLedgerApplier()
    failure_ledger = WorkFailureLedgerApplier()
    lineage_id = uuid7()
    target_id = uuid7()
    first_at = START + timedelta(hours=2)
    first = WorkObligation(
        obligation_id=uuid7(),
        lineage_id=lineage_id,
        tenant_id=tenant_id,
        generation=1,
        semantic_dedupe_key=f"redrive-probe:{target_id}",
        causal_parent_ref=f"probe:{target_id}",
        reason="exercise a poison-input quarantine and governed redrive",
        target_object_type="redrive_probe",
        target_object_id=target_id,
        owner_writer_id="WorkLedgerApplier",
        purpose="redrive_protocol_probe",
        risk_tier="low",
        expected_value=0.3,
        correctness_priority=0.9,
        intent_relevance=0.2,
        uncertainty_reduction_estimate=0.6,
        minimum_processing_class=ProcessingClass.R1_MINIMAL_INTERPRETATION,
        maximum_processing_class=ProcessingClass.R2_PROVISIONAL_GROUNDING,
        economic_envelope_ref="economic-envelope:redrive-probe-v1",
        maximum_attempts=1,
        deadline=START + timedelta(hours=3),
        generation_depth=0,
        terminal_condition="probe is no-op or terminally classified",
        effect_possible=False,
        registered_at=first_at,
    )
    await _apply(
        fresh_db,
        ledger,
        "register",
        command=WorkObligationRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="register_redrive_probe",
                at=first_at,
                key="redrive:first:register",
            ),
            obligation=first,
        ),
        now=first_at,
    )
    eligible_at = first_at + timedelta(minutes=1)
    await _apply(
        fresh_db,
        ledger,
        "decide",
        command=WorkDecisionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="make_redrive_probe_eligible",
                at=eligible_at,
                key="redrive:first:eligible",
            ),
            expected_version=1,
            decision=WorkDecision(
                decision_id=uuid7(),
                tenant_id=tenant_id,
                obligation_id=first.obligation_id,
                obligation_generation=1,
                from_state=WorkObligationState.REGISTERED,
                to_state=WorkObligationState.ELIGIBLE,
                selected_processing_class=ProcessingClass.R1_MINIMAL_INTERPRETATION,
                policy_version_ref="work-policy:redrive-probe-v1",
                why_no_cheaper_class_is_safe="the probe requires one durable reducer pass",
                reason="probe is due",
                decided_at=eligible_at,
            ),
        ),
        now=eligible_at,
    )
    lease_at = first_at + timedelta(minutes=2)
    first_lease = LeaseToken(
        lease_token_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=first.obligation_id,
        obligation_generation=1,
        fence=1,
        attempt=1,
        owner_ref="worker:redrive-probe-1",
        heartbeat_deadline=lease_at + timedelta(minutes=2),
        expires_at=lease_at + timedelta(minutes=5),
        effect_possible=False,
        granted_at=lease_at,
    )
    await _apply(
        fresh_db,
        ledger,
        "grant_lease",
        command=LeaseGrantCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="lease_redrive_probe",
                at=lease_at,
                key="redrive:first:lease",
            ),
            expected_obligation_version=2,
            lease=first_lease,
        ),
        now=lease_at,
    )
    quarantine_at = first_at + timedelta(minutes=3)
    await _apply(
        fresh_db,
        ledger,
        "resolve_lease",
        command=LeaseResolutionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="quarantine_redrive_probe",
                at=quarantine_at,
                key="redrive:first:quarantine",
            ),
            expected_obligation_version=3,
            expected_lease_version=1,
            resolution=LeaseResolution(
                lease_token_id=first_lease.lease_token_id,
                tenant_id=tenant_id,
                obligation_id=first.obligation_id,
                obligation_generation=1,
                fence=1,
                to_lease_state=LeaseState.TERMINAL,
                to_work_state=WorkObligationState.QUARANTINED,
                effect_may_have_occurred=False,
                result_evidence_refs=("failure:poison-input:v1",),
                reason="the first generation used a poisoned input snapshot",
                resolved_at=quarantine_at,
            ),
        ),
        now=quarantine_at,
    )

    failure_at = quarantine_at + timedelta(seconds=10)
    failure = FailureRecord(
        failure_id=uuid7(),
        lineage_id=uuid7(),
        tenant_id=tenant_id,
        generation=1,
        work_obligation_id=first.obligation_id,
        work_obligation_generation=1,
        causal_operation="redrive_protocol_probe",
        classification=FailureClassification.UNCLASSIFIED,
        owner_writer_id="WorkLedgerApplier",
        semantic_owner_writer_id="WorkLedgerApplier",
        target_object_type=first.target_object_type,
        target_object_id=first.target_object_id,
        original_semantic_idempotency_key="redrive:first:lease",
        attempt=1,
        maximum_attempts=1,
        deadline=first.deadline,
        next_action="classify the poisoned-input failure",
        effect_uncertainty=EffectUncertainty.NONE,
        state=FailureState.DETECTED,
        reason="generation one encountered a poisoned input snapshot",
        created_at=failure_at,
        updated_at=failure_at,
    )
    await _apply(
        fresh_db,
        failure_ledger,
        "apply_failure",
        command=FailureRecordCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="failure_record",
                operation="detect_redrive_failure",
                at=failure_at,
                key="redrive:failure:first:detected",
            ),
            expected_version=0,
            record=failure,
        ),
        now=failure_at,
    )
    for version, seconds, target_state, next_action, reason, evidence in (
        (
            1,
            20,
            FailureState.CLASSIFIED,
            "quarantine until corrected input is authorized",
            "the input snapshot is deterministically poisoned",
            ("classifier:poison-input:v1",),
        ),
        (
            2,
            30,
            FailureState.QUARANTINED,
            "await explicit redrive authorization",
            "automatic retry would repeat the poisoned snapshot",
            ("quarantine:poison-input:v1",),
        ),
    ):
        transition_at = quarantine_at + timedelta(seconds=seconds)
        failure = failure.model_copy(
            update={
                "classification": FailureClassification.POISON_INPUT,
                "state": target_state,
                "next_action": next_action,
                "reason": reason,
                "remediation_evidence_refs": evidence,
                "updated_at": transition_at,
            }
        )
        await _apply(
            fresh_db,
            failure_ledger,
            "apply_failure",
            command=FailureRecordCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="WorkLedgerApplier",
                    responsibility="failure_record",
                    operation=f"redrive_failure_{target_state}",
                    at=transition_at,
                    key=f"redrive:failure:first:{target_state}",
                ),
                expected_version=version,
                record=failure,
            ),
            now=transition_at,
        )

    successor_at = first_at + timedelta(minutes=5)
    successor = first.model_copy(
        update={
            "obligation_id": uuid7(),
            "generation": 2,
            "parent_obligation_id": first.obligation_id,
            "causal_parent_ref": "redrive-authorization:probe:v2",
            "reason": "redrive after replacing the poisoned input snapshot",
            "generation_depth": 1,
            "registered_at": successor_at,
        }
    )
    premature_command = WorkObligationRegistrationCommand(
        context=_context(
            tenant_id=tenant_id,
            owner="WorkLedgerApplier",
            responsibility="work_obligation",
            operation="premature_redrive_probe",
            at=successor_at,
            key="redrive:premature-successor",
        ),
        obligation=successor,
    )
    with pytest.raises(InvariantViolation, match="redrive-authorized parent"):
        async with fresh_db.acquire() as conn, conn.transaction():
            await ledger.register(conn=conn, command=premature_command, now=successor_at)

    authorize_at = first_at + timedelta(minutes=4)
    await _apply(
        fresh_db,
        ledger,
        "transition",
        command=WorkStateTransitionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="authorize_redrive_probe",
                at=authorize_at,
                key="redrive:first:authorize",
            ),
            expected_version=4,
            transition=WorkStateTransition(
                transition_id=uuid7(),
                tenant_id=tenant_id,
                obligation_id=first.obligation_id,
                obligation_generation=1,
                from_state=WorkObligationState.QUARANTINED,
                to_state=WorkObligationState.REDRIVE_AUTHORIZED,
                reason="operator approved a new generation with corrected input",
                result_evidence_refs=("operator-decision:redrive-probe:v2",),
                transitioned_at=authorize_at,
            ),
        ),
        now=authorize_at,
    )
    failure_authorize_at = authorize_at + timedelta(seconds=10)
    failure = failure.model_copy(
        update={
            "state": FailureState.REDRIVE_AUTHORIZED,
            "next_action": "admit the exact corrected Work successor",
            "reason": "operator authorized one identity-preserving redrive",
            "remediation_evidence_refs": tuple(
                sorted(
                    {
                        *failure.remediation_evidence_refs,
                        "operator-decision:redrive-probe:v2",
                    }
                )
            ),
            "updated_at": failure_authorize_at,
        }
    )
    await _apply(
        fresh_db,
        failure_ledger,
        "apply_failure",
        command=FailureRecordCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="failure_record",
                operation="authorize_failure_redrive",
                at=failure_authorize_at,
                key="redrive:failure:first:authorized",
            ),
            expected_version=3,
            record=failure,
        ),
        now=failure_authorize_at,
    )
    drifted_successor = successor.model_copy(
        update={
            "obligation_id": uuid7(),
            "target_object_id": uuid7(),
            "registered_at": successor_at - timedelta(seconds=1),
        }
    )
    with pytest.raises(InvariantViolation, match="semantic work identity"):
        async with fresh_db.acquire() as conn, conn.transaction():
            await ledger.register(
                conn=conn,
                command=WorkObligationRegistrationCommand(
                    context=_context(
                        tenant_id=tenant_id,
                        owner="WorkLedgerApplier",
                        responsibility="work_obligation",
                        operation="drifted_redrive_successor",
                        at=drifted_successor.registered_at,
                        key="redrive:drifted-successor",
                    ),
                    obligation=drifted_successor,
                ),
                now=drifted_successor.registered_at,
            )
    successor_result = await _apply(
        fresh_db,
        ledger,
        "register",
        command=WorkObligationRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="register_redrive_successor",
                at=successor_at,
                key="redrive:successor:register",
            ),
            obligation=successor,
        ),
        now=successor_at,
    )
    assert successor_result.object_version == 1

    stale_successor = successor.model_copy(
        update={
            "obligation_id": uuid7(),
            "registered_at": successor_at + timedelta(seconds=1),
        }
    )
    with pytest.raises(InvariantViolation, match="exact current lineage head"):
        async with fresh_db.acquire() as conn, conn.transaction():
            await ledger.register(
                conn=conn,
                command=WorkObligationRegistrationCommand(
                    context=_context(
                        tenant_id=tenant_id,
                        owner="WorkLedgerApplier",
                        responsibility="work_obligation",
                        operation="stale_redrive_successor",
                        at=stale_successor.registered_at,
                        key="redrive:stale-successor",
                    ),
                    obligation=stale_successor,
                ),
                now=stale_successor.registered_at,
            )

    successor_eligible_at = first_at + timedelta(minutes=6)
    await _apply(
        fresh_db,
        ledger,
        "decide",
        command=WorkDecisionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="make_redrive_successor_eligible",
                at=successor_eligible_at,
                key="redrive:successor:eligible",
            ),
            expected_version=1,
            decision=WorkDecision(
                decision_id=uuid7(),
                tenant_id=tenant_id,
                obligation_id=successor.obligation_id,
                obligation_generation=2,
                from_state=WorkObligationState.REGISTERED,
                to_state=WorkObligationState.ELIGIBLE,
                selected_processing_class=ProcessingClass.R1_MINIMAL_INTERPRETATION,
                policy_version_ref="work-policy:redrive-probe-v1",
                why_no_cheaper_class_is_safe="the corrected input still requires one durable pass",
                reason="authorized successor is due",
                decided_at=successor_eligible_at,
            ),
        ),
        now=successor_eligible_at,
    )
    successor_lease_at = first_at + timedelta(minutes=7)
    successor_lease = LeaseToken(
        lease_token_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=successor.obligation_id,
        obligation_generation=2,
        fence=1,
        attempt=1,
        owner_ref="worker:redrive-probe-2",
        heartbeat_deadline=successor_lease_at + timedelta(minutes=2),
        expires_at=successor_lease_at + timedelta(minutes=5),
        effect_possible=False,
        granted_at=successor_lease_at,
    )
    await _apply(
        fresh_db,
        ledger,
        "grant_lease",
        command=LeaseGrantCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="lease_redrive_successor",
                at=successor_lease_at,
                key="redrive:successor:lease",
            ),
            expected_obligation_version=2,
            lease=successor_lease,
        ),
        now=successor_lease_at,
    )
    successor_quarantine_at = first_at + timedelta(minutes=8)
    await _apply(
        fresh_db,
        ledger,
        "resolve_lease",
        command=LeaseResolutionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="quarantine_redrive_successor",
                at=successor_quarantine_at,
                key="redrive:successor:quarantine",
            ),
            expected_obligation_version=3,
            expected_lease_version=1,
            resolution=LeaseResolution(
                lease_token_id=successor_lease.lease_token_id,
                tenant_id=tenant_id,
                obligation_id=successor.obligation_id,
                obligation_generation=2,
                fence=1,
                to_lease_state=LeaseState.TERMINAL,
                to_work_state=WorkObligationState.QUARANTINED,
                effect_may_have_occurred=False,
                result_evidence_refs=("failure:successor-poison-input:v2",),
                reason="generation two exposed a distinct terminal input defect",
                resolved_at=successor_quarantine_at,
            ),
        ),
        now=successor_quarantine_at,
    )

    child_failure_at = successor_quarantine_at + timedelta(seconds=10)
    child_failure = failure.model_copy(
        update={
            "failure_id": uuid7(),
            "generation": 2,
            "parent_failure_id": failure.failure_id,
            "work_obligation_id": successor.obligation_id,
            "work_obligation_generation": 2,
            "classification": FailureClassification.UNCLASSIFIED,
            "original_semantic_idempotency_key": "redrive:successor:lease",
            "state": FailureState.DETECTED,
            "next_action": "classify the generation-two defect",
            "remediation_evidence_refs": (),
            "reason": "the authorized successor encountered a new input defect",
            "created_at": child_failure_at,
            "updated_at": child_failure_at,
        }
    )
    reused_key_child = child_failure.model_copy(
        update={
            "failure_id": uuid7(),
            "original_semantic_idempotency_key": failure.original_semantic_idempotency_key,
            "created_at": child_failure_at - timedelta(seconds=1),
            "updated_at": child_failure_at - timedelta(seconds=1),
        }
    )
    with pytest.raises(InvariantViolation, match="new semantic key"):
        async with fresh_db.acquire() as conn, conn.transaction():
            await failure_ledger.apply_failure(
                conn=conn,
                command=FailureRecordCommand(
                    context=_context(
                        tenant_id=tenant_id,
                        owner="WorkLedgerApplier",
                        responsibility="failure_record",
                        operation="reuse_failure_redrive_key",
                        at=reused_key_child.created_at,
                        key="redrive:failure:reused-key",
                    ),
                    expected_version=0,
                    record=reused_key_child,
                ),
                now=reused_key_child.created_at,
            )
    await _apply(
        fresh_db,
        failure_ledger,
        "apply_failure",
        command=FailureRecordCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="failure_record",
                operation="detect_failure_redrive_successor",
                at=child_failure_at,
                key="redrive:failure:successor:detected",
            ),
            expected_version=0,
            record=child_failure,
        ),
        now=child_failure_at,
    )
    for version, seconds, target_state, next_action, reason in (
        (
            1,
            20,
            FailureState.CLASSIFIED,
            "reject the invalid successor input",
            "generation two input violates its declared schema",
        ),
        (
            2,
            30,
            FailureState.TERMINAL_REJECTED,
            "preserve the terminal rejection",
            "the redrive completed with a typed terminal rejection",
        ),
    ):
        transition_at = successor_quarantine_at + timedelta(seconds=seconds)
        child_failure = child_failure.model_copy(
            update={
                "classification": FailureClassification.INVALID_INPUT,
                "state": target_state,
                "next_action": next_action,
                "reason": reason,
                "remediation_evidence_refs": (
                    "schema-validation:redrive-successor:v2",
                ),
                "updated_at": transition_at,
            }
        )
        await _apply(
            fresh_db,
            failure_ledger,
            "apply_failure",
            command=FailureRecordCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="WorkLedgerApplier",
                    responsibility="failure_record",
                    operation=f"failure_redrive_successor_{target_state}",
                    at=transition_at,
                    key=f"redrive:failure:successor:{target_state}",
                ),
                expected_version=version,
                record=child_failure,
            ),
            now=transition_at,
        )

    exhaust_at = successor_quarantine_at + timedelta(seconds=40)
    await _apply(
        fresh_db,
        ledger,
        "transition",
        command=WorkStateTransitionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="exhaust_redrive_successor",
                at=exhaust_at,
                key="redrive:successor:exhausted",
            ),
            expected_version=4,
            transition=WorkStateTransition(
                transition_id=uuid7(),
                tenant_id=tenant_id,
                obligation_id=successor.obligation_id,
                obligation_generation=2,
                from_state=WorkObligationState.QUARANTINED,
                to_state=WorkObligationState.EXHAUSTED,
                reason="typed child Failure exhausted this redrive generation",
                result_evidence_refs=(f"failure:{child_failure.failure_id}",),
                transitioned_at=exhaust_at,
            ),
        ),
        now=exhaust_at,
    )
    parent_resolved_at = successor_quarantine_at + timedelta(seconds=50)
    failure = failure.model_copy(
        update={
            "state": FailureState.RESOLVED,
            "next_action": "preserve the terminal child result",
            "reason": "the authorized redrive reached a typed terminal child fate",
            "remediation_evidence_refs": tuple(
                sorted(
                    {
                        *failure.remediation_evidence_refs,
                        f"failure:{child_failure.failure_id}",
                        f"work:{successor.obligation_id}",
                    }
                )
            ),
            "updated_at": parent_resolved_at,
        }
    )
    await _apply(
        fresh_db,
        failure_ledger,
        "apply_failure",
        command=FailureRecordCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="failure_record",
                operation="resolve_failure_redrive_parent",
                at=parent_resolved_at,
                key="redrive:failure:first:resolved",
            ),
            expected_version=5,
            record=failure,
        ),
        now=parent_resolved_at,
    )

    async with fresh_db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT obligation_id, current_state
            FROM work_obligation_heads
            WHERE tenant_id=$1 AND lineage_id=$2
            ORDER BY generation
            """,
            tenant_id,
            lineage_id,
        )
        lineage = await conn.fetchrow(
            """
            SELECT current_obligation_id, current_generation
            FROM work_obligation_lineage_heads
            WHERE tenant_id=$1 AND lineage_id=$2
            """,
            tenant_id,
            lineage_id,
        )
        failure_rows = await conn.fetch(
            """
            SELECT failure_id, current_state
            FROM failure_record_heads
            WHERE tenant_id=$1 AND lineage_id=$2
            ORDER BY generation
            """,
            tenant_id,
            failure.lineage_id,
        )
        failure_lineage = await conn.fetchrow(
            """
            SELECT current_failure_id, current_generation
            FROM failure_record_lineage_heads
            WHERE tenant_id=$1 AND lineage_id=$2
            """,
            tenant_id,
            failure.lineage_id,
        )
        evaluation = await evaluate_execution_state(
            conn,
            scope=ExecutionEvaluationScope(
                tenant_id=tenant_id,
                start=START,
                end=START + timedelta(days=1),
                run_id="work-redrive-component-replay",
            ),
            artifact_refs=("pytest://work-redrive-component-replay",),
        )
    assert [(row["obligation_id"], row["current_state"]) for row in rows] == [
        (first.obligation_id, "superseded_by_new_generation"),
        (successor.obligation_id, "exhausted"),
    ]
    assert lineage["current_obligation_id"] == successor.obligation_id
    assert lineage["current_generation"] == 2
    assert [(row["failure_id"], row["current_state"]) for row in failure_rows] == [
        (failure.failure_id, "resolved"),
        (child_failure.failure_id, "terminal_rejected"),
    ]
    assert failure_lineage["current_failure_id"] == child_failure.failure_id
    assert failure_lineage["current_generation"] == 2
    assert evaluation.incident_counts == {}
    assert evaluation.work_obligation_count == 2
    assert evaluation.work_history_integrity_rate == 1.0
    assert evaluation.work_lineage_integrity_rate == 1.0
    assert evaluation.work_redrive_generation_count == 1
    assert evaluation.authorized_work_redrive_generation_count == 1
    assert evaluation.work_redrive_authorization_rate == 1.0
    assert evaluation.work_decision_envelope_rate == 1.0
    assert evaluation.lease_integrity_rate == 1.0
    assert evaluation.failure_record_count == 2
    assert evaluation.failure_history_integrity_rate == 1.0
    assert evaluation.failure_redrive_generation_count == 1
    assert evaluation.authorized_failure_redrive_generation_count == 1
    assert evaluation.failure_redrive_authorization_rate == 1.0
    assert evaluation.closed_failure_redrive_generation_count == 1
    assert evaluation.failure_redrive_closure_rate == 1.0
    assert evaluation.command_reconstructability_rate == 1.0
    assert evaluation.command_event_coverage == 1.0
    assert evaluation.command_outbox_coverage == 1.0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_effect_capable_takeover_requires_exact_no_attempt_ledger_proof(fresh_db):
    base = START + timedelta(hours=5)
    tenant_id = uuid4()
    ledger = WorkLedgerApplier()
    work = WorkObligation(
        obligation_id=uuid7(),
        lineage_id=uuid7(),
        tenant_id=tenant_id,
        generation=1,
        semantic_dedupe_key="effect-capable-takeover:probe",
        causal_parent_ref="intervention:effect-capable-takeover",
        reason="prove takeover before any external effect attempt exists",
        target_object_type="intervention_execution",
        target_object_id=uuid7(),
        owner_writer_id="ExecutionLedgerApplier",
        purpose="execute_external_effect",
        risk_tier="high",
        expected_value=0.6,
        correctness_priority=1.0,
        intent_relevance=0.8,
        uncertainty_reduction_estimate=0.2,
        minimum_processing_class=ProcessingClass.R2_PROVISIONAL_GROUNDING,
        maximum_processing_class=ProcessingClass.R3_DURABLE_UNDERSTANDING,
        economic_envelope_ref="economic-envelope:effect-takeover-v1",
        maximum_attempts=2,
        deadline=base + timedelta(hours=1),
        generation_depth=0,
        terminal_condition="effect is receipted or work is cancelled before dispatch",
        effect_possible=True,
        registered_at=base,
    )
    await _apply(
        fresh_db,
        ledger,
        "register",
        command=WorkObligationRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="register_effect_takeover_probe",
                at=base,
                key="effect-takeover:work:register",
            ),
            obligation=work,
        ),
        now=base,
    )
    eligible_at = base + timedelta(minutes=1)
    await _apply(
        fresh_db,
        ledger,
        "decide",
        command=WorkDecisionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="schedule_effect_takeover_probe",
                at=eligible_at,
                key="effect-takeover:work:eligible",
            ),
            expected_version=1,
            decision=WorkDecision(
                decision_id=uuid7(),
                tenant_id=tenant_id,
                obligation_id=work.obligation_id,
                obligation_generation=1,
                from_state=WorkObligationState.REGISTERED,
                to_state=WorkObligationState.ELIGIBLE,
                selected_processing_class=ProcessingClass.R2_PROVISIONAL_GROUNDING,
                policy_version_ref="work-policy:effect-takeover-v1",
                why_no_cheaper_class_is_safe="external effects require durable grounding",
                reason="effect-capable probe is due",
                decided_at=eligible_at,
            ),
        ),
        now=eligible_at,
    )
    lease_at = base + timedelta(minutes=2)
    predecessor = LeaseToken(
        lease_token_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=work.obligation_id,
        obligation_generation=1,
        fence=1,
        attempt=1,
        owner_ref="worker:effect-takeover-1",
        heartbeat_deadline=lease_at + timedelta(minutes=1),
        expires_at=lease_at + timedelta(minutes=10),
        effect_possible=True,
        granted_at=lease_at,
    )
    await _apply(
        fresh_db,
        ledger,
        "grant_lease",
        command=LeaseGrantCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="lease_effect_takeover_probe",
                at=lease_at,
                key="effect-takeover:lease:first",
            ),
            expected_obligation_version=2,
            lease=predecessor,
        ),
        now=lease_at,
    )
    takeover_at = predecessor.heartbeat_deadline
    successor = LeaseToken(
        lease_token_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=work.obligation_id,
        obligation_generation=1,
        fence=2,
        attempt=2,
        owner_ref="worker:effect-takeover-2",
        heartbeat_deadline=takeover_at + timedelta(minutes=2),
        expires_at=takeover_at + timedelta(minutes=8),
        effect_possible=True,
        granted_at=takeover_at,
    )
    exact_predecessor_ref = (
        f"effect-ledger:no-attempt:{predecessor.lease_token_id}:fence:1"
    )
    wrong_takeover = LeaseTakeover(
        takeover_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=work.obligation_id,
        obligation_generation=1,
        predecessor_lease_token_id=predecessor.lease_token_id,
        predecessor_fence=1,
        predecessor_attempt=1,
        predecessor_owner_ref=predecessor.owner_ref,
        predecessor_heartbeat_deadline=predecessor.heartbeat_deadline,
        successor=successor,
        no_effect_evidence_refs=("effect-ledger:no-dispatch",),
        reason="probe a free-form no-effect claim",
        taken_over_at=takeover_at,
    )
    with pytest.raises(InvariantViolation, match="exact predecessor ledger evidence"):
        await _apply(
            fresh_db,
            ledger,
            "take_over_lease",
            command=LeaseTakeoverCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="WorkLedgerApplier",
                    responsibility="work_obligation",
                    operation="reject_unbound_effect_takeover",
                    at=takeover_at,
                    key="effect-takeover:wrong-proof",
                ),
                expected_obligation_version=3,
                expected_predecessor_lease_version=1,
                takeover=wrong_takeover,
            ),
            now=takeover_at,
        )

    takeover = wrong_takeover.model_copy(
        update={
            "takeover_id": uuid7(),
            "no_effect_evidence_refs": (exact_predecessor_ref,),
            "reason": "exact ledger proves no attempt existed under the old fence",
        }
    )
    await _apply(
        fresh_db,
        ledger,
        "take_over_lease",
        command=LeaseTakeoverCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="take_over_effect_capable_work",
                at=takeover_at,
                key="effect-takeover:exact-proof",
            ),
            expected_obligation_version=3,
            expected_predecessor_lease_version=1,
            takeover=takeover,
        ),
        now=takeover_at,
    )

    successor_no_attempt_ref = (
        f"effect-ledger:no-attempt:{successor.lease_token_id}:fence:2"
    )
    completed_at = takeover_at + timedelta(minutes=1)
    with pytest.raises(InvariantViolation, match="cannot complete without a succeeded"):
        await _apply(
            fresh_db,
            ledger,
            "resolve_lease",
            command=LeaseResolutionCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="WorkLedgerApplier",
                    responsibility="work_obligation",
                    operation="reject_effect_completion_without_attempt",
                    at=completed_at,
                    key="effect-takeover:completion-bypass",
                ),
                expected_obligation_version=4,
                expected_lease_version=1,
                resolution=LeaseResolution(
                    lease_token_id=successor.lease_token_id,
                    tenant_id=tenant_id,
                    obligation_id=work.obligation_id,
                    obligation_generation=1,
                    fence=2,
                    to_lease_state=LeaseState.COMPLETED,
                    to_work_state=WorkObligationState.COMPLETED,
                    effect_may_have_occurred=False,
                    result_evidence_refs=(successor_no_attempt_ref,),
                    reason="no attempt is not proof of successful execution",
                    resolved_at=completed_at,
                ),
            ),
            now=completed_at,
        )

    cancelled_at = completed_at + timedelta(seconds=1)
    await _apply(
        fresh_db,
        ledger,
        "resolve_lease",
        command=LeaseResolutionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="cancel_effect_work_before_dispatch",
                at=cancelled_at,
                key="effect-takeover:cancelled",
            ),
            expected_obligation_version=4,
            expected_lease_version=1,
            resolution=LeaseResolution(
                lease_token_id=successor.lease_token_id,
                tenant_id=tenant_id,
                obligation_id=work.obligation_id,
                obligation_generation=1,
                fence=2,
                to_lease_state=LeaseState.RELEASED,
                to_work_state=WorkObligationState.CANCELLED,
                effect_may_have_occurred=False,
                result_evidence_refs=(successor_no_attempt_ref,),
                reason="work was cancelled before any governed dispatch intent",
                resolved_at=cancelled_at,
            ),
        ),
        now=cancelled_at,
    )

    async with fresh_db.acquire() as conn:
        evaluation = await evaluate_execution_state(
            conn,
            scope=ExecutionEvaluationScope(
                tenant_id=tenant_id,
                start=base - timedelta(minutes=1),
                end=base + timedelta(hours=1),
                run_id="effect-capable-takeover-component-replay",
            ),
            artifact_refs=("pytest://effect-capable-takeover-component-replay",),
        )
    assert evaluation.incident_counts == {}
    assert evaluation.missed_heartbeat_takeover_count == 1
    assert evaluation.safe_missed_heartbeat_takeover_count == 1
    assert evaluation.takeover_safety_rate == 1.0
    assert evaluation.work_fate_counts == {"cancelled": 1}
    assert evaluation.work_history_integrity_rate == 1.0
    assert evaluation.lease_integrity_rate == 1.0
