from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest

import services.domain.execution.repo as execution_repo

from lib.contracts import (
    ActionAdapterCapabilities,
    AdapterCapabilityRegistrationCommand,
    AuthorizationDecision,
    AuthorizationDecisionCommand,
    AuthorizationDisposition,
    ConsequentialProposal,
    ConsequentialProposalFate,
    ConsequentialProposalRegistrationCommand,
    ConsequentialProposalReview,
    ConsequentialProposalReviewCommand,
    EffectObservation,
    EffectReservationCommand,
    EffectTransitionCommand,
    EffectUncertainty,
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
    LeaseResolution,
    LeaseResolutionCommand,
    LeaseState,
    LeaseTakeover,
    LeaseTakeoverCommand,
    LeaseToken,
    OwnerTerminalizationRequest,
    OwnerTerminalizationRequestCommand,
    OwnerTerminalizationResolution,
    OwnerTerminalizationResolutionCommand,
    ProcessingClass,
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
)
from lib.evaluation.execution import ExecutionEvaluationScope, evaluate_execution_state
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.execution.repo import (
    AgencyStateApplier,
    ExecutionLedgerApplier,
    WorkLedgerApplier,
)
from services.domain.execution.failure_repo import WorkFailureLedgerApplier
from services.domain.execution.tests.test_repo import (
    START,
    _apply,
    _consumption_authority,
    _context,
    _processing_authority,
    _referent,
)
from services.domain.intent.repo import ProposalAppender
from services.domain.outcomes.repo import AuthorizationApplier, EpisodeCoordinator


async def _register_action_proposal(
    pool,
    *,
    tenant_id,
    capabilities,
    base,
    label,
    operation,
    parameters,
    grounding_refs,
    reversible,
    compensation_declaration,
):
    episode_id = uuid7()
    episode = InterventionEpisode(
        episode_id=episode_id,
        tenant_id=tenant_id,
        stage_links=(
            EpisodeStageLink(
                stage="proposal",
                fate=EpisodeStageFate.NOT_EXECUTED,
                reason="proposal is being registered",
            ),
        ),
        created_at=base,
        updated_at=base,
    )
    await _apply(
        pool,
        EpisodeCoordinator(),
        "apply",
        command=EpisodeUpdateCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="EpisodeCoordinator",
                responsibility="intervention_episode",
                operation=f"create_{label}_episode",
                at=base,
                key=f"{label}:episode:create",
            ),
            expected_version=0,
            episode=episode,
        ),
        now=base,
    )
    proposal_at = base + timedelta(seconds=1)
    authority = _processing_authority(
        tenant_id=tenant_id,
        operation=f"register_{label}_proposal",
        at=proposal_at,
    )
    spec = InterventionSpec(
        spec_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        target_referent=_referent(tenant_id),
        target_version="slack-channel-v2",
        operation=operation,
        parameters=parameters,
        comparator={"delivery": "leave_partial_effect_unchanged"},
        outcome_metric=f"{label}_externally_observed",
        outcome_window_start=base + timedelta(hours=1),
        outcome_window_end=base + timedelta(days=1),
        workflow_spec_version_ref=f"workflow:{label}:v1",
        action_adapter_version=capabilities.capability_version,
        action_adapter_capability_digest=capabilities.capability_digest,
        safety_and_preconditions=("target still exists", "authority is live"),
        authority_requirement=f"capability:{operation}",
        reversible=reversible,
        compensation_declaration=compensation_declaration,
        grounding_dependency_refs=grounding_refs,
        context_dependency_manifest_digest=("a" if label == "original" else "b")
        * 64,
    )
    proposal = ConsequentialProposal(
        proposal_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        intervention_spec=spec,
        summary=f"Execute {label} action",
        rationale=f"The {label} action has a separately governed purpose",
        alternative_refs=(f"alternative:{label}:none",),
        source_refs=(f"source:{label}:need",),
        processing_authority=authority,
        processing_authority_fingerprint=authority.fingerprint,
        created_at=proposal_at,
        review_due_at=proposal_at + timedelta(hours=1),
    )
    await _apply(
        pool,
        ProposalAppender(),
        "append_consequential",
        command=ConsequentialProposalRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ProposalAppender",
                responsibility="consequential_proposal",
                operation=f"register_{label}_proposal",
                at=proposal_at,
                key=f"{label}:proposal:register",
                authority=authority,
            ),
            proposal=proposal,
        ),
        now=proposal_at,
    )
    return episode, proposal, spec


async def _authorize_action(
    pool, *, tenant_id, base, label, proposal, spec, attempt_budget=1
):
    review_at = base
    review = ConsequentialProposalReview(
        review_id=uuid7(),
        tenant_id=tenant_id,
        proposal_id=proposal.proposal_id,
        proposal_version=proposal.proposal_version,
        proposal_digest=proposal.proposal_digest,
        intervention_spec_digest=spec.spec_digest,
        from_fate=ConsequentialProposalFate.OPEN,
        to_fate=ConsequentialProposalFate.ACCEPTED_FOR_AUTHORIZATION,
        principal_or_policy_ref="principal:operations-owner",
        authority=_consumption_authority(
            tenant_id=tenant_id,
            operation=f"review_{label}_proposal",
            at=review_at,
        ),
        reason=f"capable principal accepts the exact {label} proposal",
        decided_at=review_at,
    )
    await _apply(
        pool,
        ProposalAppender(),
        "review_consequential",
        command=ConsequentialProposalReviewCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ProposalAppender",
                responsibility="consequential_proposal",
                operation=f"review_{label}_proposal",
                at=review_at,
                key=f"{label}:proposal:accept",
            ),
            review=review,
        ),
        now=review_at,
    )
    decision_at = review_at + timedelta(seconds=1)
    target = spec.target_referent
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
            operation=f"authorize_{label}_action",
            at=decision_at,
        ),
        exact_operations=frozenset({spec.operation}),
        exact_target_refs=frozenset(
            {f"referent:{target.referent_id}:v{target.referent_version}"}
        ),
        exact_field_paths=frozenset(
            {f"parameters.{name}" for name in spec.parameters}
        ),
        constraints={"maximum_uses": 1},
        use_budget=1,
        attempt_budget=attempt_budget,
        decided_at=decision_at,
        expires_at=decision_at + timedelta(hours=2),
    )
    await _apply(
        pool,
        AuthorizationApplier(),
        "apply",
        command=AuthorizationDecisionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AuthorizationApplier",
                responsibility="authorization",
                operation=f"authorize_{label}_action",
                at=decision_at,
                key=f"{label}:authorization",
            ),
            decision=decision,
        ),
        now=decision_at,
    )
    return decision


async def _instantiate_effect(
    pool,
    *,
    tenant_id,
    capabilities,
    base,
    label,
    spec,
    decision,
    compensates_effect_attempt_id=None,
    maximum_attempts=1,
):
    agency = AgencyStateApplier()
    work_ledger = WorkLedgerApplier()
    execution = ExecutionLedgerApplier()
    workflow_id = uuid7()
    task_id = uuid7()
    workflow = WorkflowRunSnapshot(
        workflow_run_id=workflow_id,
        tenant_id=tenant_id,
        episode_id=spec.episode_id,
        intervention_spec_digest=spec.spec_digest,
        workflow_spec_version_ref=f"workflow:{label}:v1",
        state=WorkflowRunState.PLANNED,
        authorization_decision_id=decision.decision_id,
        prerequisite_refs=("authorization:live",),
        required_task_ids=(task_id,),
        completion_predicate="the effect task reaches an explicit receipted fate",
        transition_reason=f"instantiate {label} workflow",
        created_at=base,
        updated_at=base,
    )
    await _apply(
        pool,
        agency,
        "apply_workflow_run",
        command=WorkflowRunCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="workflow_run",
                operation=f"create_{label}_workflow",
                at=base,
                key=f"{label}:workflow:planned",
            ),
            expected_version=0,
            snapshot=workflow,
        ),
        now=base,
    )
    active_at = base + timedelta(seconds=1)
    workflow = workflow.model_copy(
        update={
            "state": WorkflowRunState.ACTIVE,
            "transition_reason": f"{label} workflow is active",
            "updated_at": active_at,
        }
    )
    await _apply(
        pool,
        agency,
        "apply_workflow_run",
        command=WorkflowRunCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="workflow_run",
                operation=f"activate_{label}_workflow",
                at=active_at,
                key=f"{label}:workflow:active",
            ),
            expected_version=1,
            snapshot=workflow,
        ),
        now=active_at,
    )
    task_at = base + timedelta(seconds=2)
    target = spec.target_referent
    target_ref = f"referent:{target.referent_id}:v{target.referent_version}"
    task = TaskSnapshot(
        task_id=task_id,
        tenant_id=tenant_id,
        workflow_run_id=workflow_id,
        episode_id=spec.episode_id,
        intervention_spec_digest=spec.spec_digest,
        task_kind=f"{label}_external_effect",
        state=TaskState.PLANNED,
        target_grounding_refs=(target_ref,),
        authorization_decision_id=decision.decision_id,
        external_effect_required=True,
        transition_reason=f"plan {label} task",
        created_at=task_at,
        updated_at=task_at,
    )
    await _apply(
        pool,
        agency,
        "apply_task",
        command=TaskCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="task",
                operation=f"plan_{label}_task",
                at=task_at,
                key=f"{label}:task:planned",
            ),
            expected_version=0,
            snapshot=task,
        ),
        now=task_at,
    )
    for version, seconds, state in (
        (1, 3, TaskState.READY),
        (2, 4, TaskState.IN_PROGRESS),
    ):
        at = base + timedelta(seconds=seconds)
        task = task.model_copy(
            update={
                "state": state,
                "transition_reason": f"{label} task enters {state}",
                "updated_at": at,
            }
        )
        await _apply(
            pool,
            agency,
            "apply_task",
            command=TaskCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="AgencyStateApplier",
                    responsibility="task",
                    operation=f"{label}_task_{state}",
                    at=at,
                    key=f"{label}:task:{state}",
                ),
                expected_version=version,
                snapshot=task,
            ),
            now=at,
        )
    work_at = base + timedelta(seconds=5)
    work = WorkObligation(
        obligation_id=uuid7(),
        lineage_id=uuid7(),
        tenant_id=tenant_id,
        generation=1,
        semantic_dedupe_key=f"{label}:effect:{task_id}",
        causal_parent_ref=f"task:{task_id}:v3",
        reason=f"execute the exact {label} task",
        target_object_type="task",
        target_object_id=task_id,
        owner_writer_id="AgencyStateApplier",
        purpose=f"execute_{label}_effect",
        risk_tier="high",
        expected_value=0.7,
        correctness_priority=1.0,
        intent_relevance=0.9,
        uncertainty_reduction_estimate=0.1,
        minimum_processing_class=ProcessingClass.R2_PROVISIONAL_GROUNDING,
        maximum_processing_class=ProcessingClass.R3_DURABLE_UNDERSTANDING,
        economic_envelope_ref=f"economic-envelope:{label}:v1",
        maximum_attempts=maximum_attempts,
        deadline=base + timedelta(minutes=30),
        generation_depth=0,
        terminal_condition="external effect has an explicit receipted fate",
        effect_possible=True,
        registered_at=work_at,
    )
    await _apply(
        pool,
        work_ledger,
        "register",
        command=WorkObligationRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation=f"register_{label}_work",
                at=work_at,
                key=f"{label}:work:register",
            ),
            obligation=work,
        ),
        now=work_at,
    )
    eligible_at = base + timedelta(seconds=6)
    await _apply(
        pool,
        work_ledger,
        "decide",
        command=WorkDecisionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation=f"schedule_{label}_work",
                at=eligible_at,
                key=f"{label}:work:eligible",
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
                policy_version_ref="work-policy:consequential-effect-v1",
                why_no_cheaper_class_is_safe="external effects require durable grounding",
                reason=f"{label} work is due",
                decided_at=eligible_at,
            ),
        ),
        now=eligible_at,
    )
    lease_at = base + timedelta(seconds=7)
    lease = LeaseToken(
        lease_token_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=work.obligation_id,
        obligation_generation=1,
        fence=1,
        attempt=1,
        owner_ref=f"worker:{label}:1",
        heartbeat_deadline=lease_at + timedelta(minutes=5),
        expires_at=lease_at + timedelta(minutes=20),
        effect_possible=True,
        granted_at=lease_at,
    )
    await _apply(
        pool,
        work_ledger,
        "grant_lease",
        command=LeaseGrantCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation=f"lease_{label}_work",
                at=lease_at,
                key=f"{label}:work:lease",
            ),
            expected_obligation_version=2,
            lease=lease,
        ),
        now=lease_at,
    )
    reserve_at = base + timedelta(seconds=8)
    attempt = ExternalEffectAttempt(
        effect_attempt_id=uuid7(),
        lineage_id=uuid7(),
        tenant_id=tenant_id,
        generation=1,
        compensates_effect_attempt_id=compensates_effect_attempt_id,
        episode_id=spec.episode_id,
        task_id=task_id,
        intervention_spec_digest=spec.spec_digest,
        authorization_decision_id=decision.decision_id,
        capability_id=capabilities.capability_id,
        capability_version=capabilities.capability_version,
        capability_digest=capabilities.capability_digest,
        operation=spec.operation,
        canonical_request_hash=("c" if label == "original" else "d") * 64,
        provider_idempotency_key=f"provider:{label}:{task_id}",
        target_grounding_refs=(target_ref,),
        live_precondition_refs=(f"precondition:{label}:target-live",),
        work_obligation_id=work.obligation_id,
        work_obligation_generation=1,
        lease_token_id=lease.lease_token_id,
        lease_fence=1,
        dispatch_deadline=base + timedelta(minutes=15),
        reconciliation_owner_ref=f"reconciler:{label}",
        compensation_policy_ref="policy:separate-authorized-compensation:v1",
        reserved_at=reserve_at,
    )
    await _apply(
        pool,
        execution,
        "reserve",
        command=EffectReservationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ExecutionLedgerApplier",
                responsibility="external_effect",
                operation=f"reserve_{label}_effect",
                at=reserve_at,
                key=f"{label}:effect:reserve",
            ),
            attempt=attempt,
        ),
        now=reserve_at,
    )
    return {
        "workflow": workflow,
        "task": task,
        "work": work,
        "lease": lease,
        "attempt": attempt,
    }


async def _transition_effect(
    pool,
    *,
    tenant_id,
    label,
    attempt_id,
    expected_version,
    from_state,
    to_state,
    at,
    provider_refs=(),
    external_refs=(),
    **updates,
):
    receipt_id = uuid7()
    await _apply(
        pool,
        ExecutionLedgerApplier(),
        "transition",
        command=EffectTransitionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ExecutionLedgerApplier",
                responsibility="external_effect",
                operation=f"{label}_{to_state}",
                at=at,
                key=f"{label}:effect:{expected_version}:{to_state}",
            ),
            expected_version=expected_version,
            observation=EffectObservation(
                receipt_id=receipt_id,
                tenant_id=tenant_id,
                effect_attempt_id=attempt_id,
                from_state=from_state,
                to_state=to_state,
                reason=f"{label} effect enters {to_state}",
                provider_observation_refs=provider_refs,
                external_state_evidence_refs=external_refs,
                observed_at=at,
                **updates,
            ),
        ),
        now=at,
    )
    return receipt_id


async def _setup_governed_effect(
    pool,
    *,
    base,
    label,
    compensation_supported=False,
    reversible=False,
    compensation_declaration="not supported for this operation",
):
    tenant_id = uuid4()
    capabilities = ActionAdapterCapabilities(
        capability_id=uuid7(),
        tenant_id=tenant_id,
        capability_version="slack-takeover-adapter-v1",
        adapter_name="slack-fenced-message-delivery",
        provider_name="slack",
        permitted_operations=frozenset({"send_message"}),
        request_canonicalization_version="slack-fenced-message-v1",
        idempotency_supported=True,
        idempotency_scope="workspace/channel/client-operation-id",
        idempotency_retention_until=base + timedelta(days=3),
        reconciliation_supported=True,
        reconciliation_consistency_window_seconds=30,
        cancellation_supported=True,
        partial_effect_observable=True,
        compensation_supported=compensation_supported,
        verified_at=base,
        expires_at=base + timedelta(days=2),
    )
    await _apply(
        pool,
        ExecutionLedgerApplier(),
        "register_capabilities",
        command=AdapterCapabilityRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ExecutionLedgerApplier",
                responsibility="action_adapter_capability",
                operation=f"register_{label}_takeover_capability",
                at=base,
                key=f"{label}:takeover:capability",
            ),
            expected_version=0,
            capabilities=capabilities,
        ),
        now=base,
    )
    _, proposal, spec = await _register_action_proposal(
        pool,
        tenant_id=tenant_id,
        capabilities=capabilities,
        base=base + timedelta(seconds=1),
        label=label,
        operation="send_message",
        parameters={"channel": "C1", "text": "governed update"},
        grounding_refs=(f"source:{label}:authorized-update",),
        reversible=reversible,
        compensation_declaration=compensation_declaration,
    )
    decision = await _authorize_action(
        pool,
        tenant_id=tenant_id,
        base=base + timedelta(seconds=3),
        label=label,
        proposal=proposal,
        spec=spec,
        attempt_budget=2,
    )
    objects = await _instantiate_effect(
        pool,
        tenant_id=tenant_id,
        capabilities=capabilities,
        base=base + timedelta(seconds=5),
        label=label,
        spec=spec,
        decision=decision,
        maximum_attempts=2,
    )
    return tenant_id, objects, capabilities


async def _exercise_foreign_owner_terminalization(
    pool,
    *,
    tenant_id,
    base,
    label,
    semantic_owner_writer_id,
    target_object_type,
    target_object_id,
    terminal_state,
    owner_result,
):
    work_ledger = WorkLedgerApplier()
    failure_ledger = WorkFailureLedgerApplier()
    work = WorkObligation(
        obligation_id=uuid7(),
        lineage_id=uuid7(),
        tenant_id=tenant_id,
        generation=1,
        semantic_dedupe_key=f"{label}:observe-owner-fate",
        causal_parent_ref=f"{target_object_type}:{target_object_id}",
        reason="downstream processing must acknowledge the semantic owner's fate",
        target_object_type=target_object_type,
        target_object_id=target_object_id,
        owner_writer_id=semantic_owner_writer_id,
        purpose="consume_semantic_owner_terminal_fate",
        risk_tier="medium",
        expected_value=0.4,
        correctness_priority=1.0,
        intent_relevance=0.5,
        uncertainty_reduction_estimate=0.2,
        minimum_processing_class=ProcessingClass.R2_PROVISIONAL_GROUNDING,
        maximum_processing_class=ProcessingClass.R3_DURABLE_UNDERSTANDING,
        economic_envelope_ref=f"economic-envelope:{label}:owner-fate",
        maximum_attempts=1,
        deadline=base + timedelta(hours=1),
        generation_depth=0,
        terminal_condition="exact semantic-owner CommandResult is consumed",
        effect_possible=False,
        registered_at=base,
    )
    await _apply(
        pool,
        work_ledger,
        "register",
        command=WorkObligationRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation=f"register_{label}_owner_fate_work",
                at=base,
                key=f"{label}:owner-fate:work:register",
            ),
            obligation=work,
        ),
        now=base,
    )
    eligible_at = base + timedelta(seconds=1)
    await _apply(
        pool,
        work_ledger,
        "decide",
        command=WorkDecisionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation=f"schedule_{label}_owner_fate_work",
                at=eligible_at,
                key=f"{label}:owner-fate:work:eligible",
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
                policy_version_ref="work-policy:owner-fate-v1",
                why_no_cheaper_class_is_safe="foreign semantic fate needs a durable handshake",
                reason="semantic owner already committed a terminal fate",
                decided_at=eligible_at,
            ),
        ),
        now=eligible_at,
    )
    lease_at = base + timedelta(seconds=2)
    lease = LeaseToken(
        lease_token_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=work.obligation_id,
        obligation_generation=1,
        fence=1,
        attempt=1,
        owner_ref=f"worker:{label}:owner-fate",
        heartbeat_deadline=lease_at + timedelta(minutes=2),
        expires_at=lease_at + timedelta(minutes=10),
        effect_possible=False,
        granted_at=lease_at,
    )
    await _apply(
        pool,
        work_ledger,
        "grant_lease",
        command=LeaseGrantCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation=f"lease_{label}_owner_fate_work",
                at=lease_at,
                key=f"{label}:owner-fate:work:lease",
            ),
            expected_obligation_version=2,
            lease=lease,
        ),
        now=lease_at,
    )
    quarantine_at = base + timedelta(seconds=3)
    await _apply(
        pool,
        work_ledger,
        "resolve_lease",
        command=LeaseResolutionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation=f"quarantine_{label}_owner_fate_work",
                at=quarantine_at,
                key=f"{label}:owner-fate:work:quarantine",
            ),
            expected_obligation_version=3,
            expected_lease_version=1,
            resolution=LeaseResolution(
                lease_token_id=lease.lease_token_id,
                tenant_id=tenant_id,
                obligation_id=work.obligation_id,
                obligation_generation=1,
                fence=1,
                to_lease_state=LeaseState.TERMINAL,
                to_work_state=WorkObligationState.QUARANTINED,
                effect_may_have_occurred=False,
                reason="consumer cannot declare the foreign semantic fate locally",
                resolved_at=quarantine_at,
            ),
        ),
        now=quarantine_at,
    )
    failure_at = base + timedelta(seconds=4)
    failure = FailureRecord(
        failure_id=uuid7(),
        lineage_id=uuid7(),
        tenant_id=tenant_id,
        generation=1,
        work_obligation_id=work.obligation_id,
        work_obligation_generation=1,
        causal_operation="consume_semantic_owner_terminal_fate",
        classification=FailureClassification.OWNER_REJECTED,
        owner_writer_id="WorkLedgerApplier",
        semantic_owner_writer_id=semantic_owner_writer_id,
        target_object_type=target_object_type,
        target_object_id=target_object_id,
        original_semantic_idempotency_key=f"{label}:owner-fate:work:lease",
        attempt=1,
        maximum_attempts=1,
        deadline=work.deadline,
        next_action="request exact semantic-owner result",
        effect_uncertainty=EffectUncertainty.NONE,
        state=FailureState.DETECTED,
        reason="foreign semantic fate cannot be inferred by WorkLedger",
        created_at=failure_at,
        updated_at=failure_at,
    )
    await _apply(
        pool,
        failure_ledger,
        "apply_failure",
        command=FailureRecordCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="failure_record",
                operation=f"detect_{label}_owner_fate_failure",
                at=failure_at,
                key=f"{label}:owner-fate:failure:detected",
            ),
            expected_version=0,
            record=failure,
        ),
        now=failure_at,
    )
    for expected_version, state, seconds in (
        (1, FailureState.CLASSIFIED, 5),
        (2, FailureState.QUARANTINED, 6),
    ):
        at = base + timedelta(seconds=seconds)
        failure = failure.model_copy(
            update={
                "state": state,
                "next_action": "request exact semantic-owner result",
                "reason": f"owner-fate failure enters {state}",
                "updated_at": at,
            }
        )
        await _apply(
            pool,
            failure_ledger,
            "apply_failure",
            command=FailureRecordCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="WorkLedgerApplier",
                    responsibility="failure_record",
                    operation=f"{label}_owner_fate_failure_{state}",
                    at=at,
                    key=f"{label}:owner-fate:failure:{state}",
                ),
                expected_version=expected_version,
                record=failure,
            ),
            now=at,
        )
    request_at = base + timedelta(seconds=7)
    request = OwnerTerminalizationRequest(
        request_id=uuid7(),
        tenant_id=tenant_id,
        failure_id=failure.failure_id,
        failure_generation=1,
        from_failure_state=FailureState.QUARANTINED,
        work_obligation_id=work.obligation_id,
        work_obligation_generation=1,
        from_work_state=WorkObligationState.QUARANTINED,
        semantic_owner_writer_id=semantic_owner_writer_id,
        target_object_type=target_object_type,
        target_object_id=target_object_id,
        acceptable_owner_terminal_states=frozenset({terminal_state}),
        terminal_reason="consume the exact foreign semantic-owner result",
        requested_at=request_at,
    )
    await _apply(
        pool,
        failure_ledger,
        "request_owner_terminalization",
        command=OwnerTerminalizationRequestCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="failure_record",
                operation=f"request_{label}_owner_terminalization",
                at=request_at,
                key=f"{label}:owner-fate:request",
            ),
            expected_failure_version=3,
            expected_work_version=4,
            request=request,
        ),
        now=request_at,
    )
    resolution_at = base + timedelta(seconds=8)
    resolution = OwnerTerminalizationResolution(
        resolution_id=uuid7(),
        tenant_id=tenant_id,
        request_id=request.request_id,
        failure_id=failure.failure_id,
        failure_generation=1,
        work_obligation_id=work.obligation_id,
        work_obligation_generation=1,
        owner_command_result_id=owner_result.command_result_id,
        observed_owner_writer_id=semantic_owner_writer_id,
        observed_owner_object_type=target_object_type,
        observed_owner_object_id=target_object_id,
        observed_owner_object_version=owner_result.object_version,
        observed_owner_terminal_state=terminal_state,
        to_failure_state=FailureState.RESOLVED,
        to_work_state=WorkObligationState.COMPLETED,
        reason="exact semantic-owner result closes failure and work",
        resolved_at=resolution_at,
    )
    await _apply(
        pool,
        failure_ledger,
        "resolve_owner_terminalization",
        command=OwnerTerminalizationResolutionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="failure_record",
                operation=f"resolve_{label}_owner_terminalization",
                at=resolution_at,
                key=f"{label}:owner-fate:resolution",
            ),
            expected_failure_version=4,
            expected_work_version=5,
            resolution=resolution,
        ),
        now=resolution_at,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_effect_takeover_binds_reserved_state_at_takeover_transaction(fresh_db):
    base = START + timedelta(hours=7)
    tenant_id, objects, _ = await _setup_governed_effect(
        fresh_db, base=base, label="reserved_takeover"
    )
    predecessor = objects["lease"]
    attempt = objects["attempt"]
    takeover_at = predecessor.heartbeat_deadline
    successor = LeaseToken(
        lease_token_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=objects["work"].obligation_id,
        obligation_generation=1,
        fence=2,
        attempt=2,
        owner_ref="worker:takeover:2",
        heartbeat_deadline=takeover_at + timedelta(minutes=2),
        expires_at=takeover_at + timedelta(minutes=10),
        effect_possible=True,
        granted_at=takeover_at,
    )
    exact_reserved_ref = (
        f"external-effect-attempt:{attempt.effect_attempt_id}:"
        "state:reserved:version:1"
    )
    takeover = LeaseTakeover(
        takeover_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=objects["work"].obligation_id,
        obligation_generation=1,
        predecessor_lease_token_id=predecessor.lease_token_id,
        predecessor_fence=1,
        predecessor_attempt=1,
        predecessor_owner_ref=predecessor.owner_ref,
        predecessor_heartbeat_deadline=predecessor.heartbeat_deadline,
        successor=successor,
        no_effect_evidence_refs=("execution-receipt:not-the-reserved-attempt",),
        reason="probe an unrelated no-effect claim",
        taken_over_at=takeover_at,
    )
    with pytest.raises(InvariantViolation, match="exact predecessor ledger evidence"):
        await _apply(
            fresh_db,
            WorkLedgerApplier(),
            "take_over_lease",
            command=LeaseTakeoverCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="WorkLedgerApplier",
                    responsibility="work_obligation",
                    operation="reject_wrong_reserved_takeover_evidence",
                    at=takeover_at,
                    key="takeover:wrong-reserved-proof",
                ),
                expected_obligation_version=3,
                expected_predecessor_lease_version=1,
                takeover=takeover,
            ),
            now=takeover_at,
        )

    takeover = takeover.model_copy(
        update={
            "takeover_id": uuid7(),
            "no_effect_evidence_refs": (exact_reserved_ref,),
            "reason": "the exact reserved version proves dispatch never began",
        }
    )
    await _apply(
        fresh_db,
        WorkLedgerApplier(),
        "take_over_lease",
        command=LeaseTakeoverCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="take_over_reserved_effect_work",
                at=takeover_at,
                key="takeover:exact-reserved-proof",
            ),
            expected_obligation_version=3,
            expected_predecessor_lease_version=1,
            takeover=takeover,
        ),
        now=takeover_at,
    )

    stale_dispatch_at = takeover_at + timedelta(seconds=1)
    with pytest.raises(InvariantViolation, match="exact live work lease fence"):
        await _transition_effect(
            fresh_db,
            tenant_id=tenant_id,
            label="stale_takeover",
            attempt_id=attempt.effect_attempt_id,
            expected_version=1,
            from_state=ExternalEffectState.RESERVED,
            to_state=ExternalEffectState.DISPATCH_INTENT_RECORDED,
            at=stale_dispatch_at,
        )

    cancel_attempt_at = stale_dispatch_at + timedelta(seconds=1)
    cancellation_receipt_id = await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="cancel_stale_takeover",
        attempt_id=attempt.effect_attempt_id,
        expected_version=1,
        from_state=ExternalEffectState.RESERVED,
        to_state=ExternalEffectState.CANCELLED,
        at=cancel_attempt_at,
    )
    cancel_work_at = cancel_attempt_at + timedelta(seconds=1)
    successor_no_attempt_ref = (
        f"effect-ledger:no-attempt:{successor.lease_token_id}:fence:2"
    )
    await _apply(
        fresh_db,
        WorkLedgerApplier(),
        "resolve_lease",
        command=LeaseResolutionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="cancel_taken_over_work",
                at=cancel_work_at,
                key="takeover:work:cancelled",
            ),
            expected_obligation_version=4,
            expected_lease_version=1,
            resolution=LeaseResolution(
                lease_token_id=successor.lease_token_id,
                tenant_id=tenant_id,
                obligation_id=objects["work"].obligation_id,
                obligation_generation=1,
                fence=2,
                to_lease_state=LeaseState.RELEASED,
                to_work_state=WorkObligationState.CANCELLED,
                effect_may_have_occurred=False,
                result_evidence_refs=(successor_no_attempt_ref,),
                reason="stale reservation was cancelled and no successor attempt exists",
                resolved_at=cancel_work_at,
            ),
        ),
        now=cancel_work_at,
    )
    task_cancel_at = cancel_work_at + timedelta(seconds=1)
    task = objects["task"].model_copy(
        update={
            "state": TaskState.CANCELLED,
            "completion_evidence_refs": (
                f"execution-receipt:{cancellation_receipt_id}",
            ),
            "transition_reason": "work takeover safely cancelled the undispatched effect",
            "updated_at": task_cancel_at,
        }
    )
    await _apply(
        fresh_db,
        AgencyStateApplier(),
        "apply_task",
        command=TaskCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="task",
                operation="cancel_takeover_task",
                at=task_cancel_at,
                key="takeover:task:cancelled",
            ),
            expected_version=3,
            snapshot=task,
        ),
        now=task_cancel_at,
    )
    workflow_cancel_at = task_cancel_at + timedelta(seconds=1)
    workflow = objects["workflow"].model_copy(
        update={
            "state": WorkflowRunState.CANCELLED,
            "completion_evidence_refs": (
                f"execution-receipt:{cancellation_receipt_id}",
            ),
            "transition_reason": "the only task was safely cancelled before dispatch",
            "updated_at": workflow_cancel_at,
        }
    )
    await _apply(
        fresh_db,
        AgencyStateApplier(),
        "apply_workflow_run",
        command=WorkflowRunCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="workflow_run",
                operation="cancel_takeover_workflow",
                at=workflow_cancel_at,
                key="takeover:workflow:cancelled",
            ),
            expected_version=2,
            snapshot=workflow,
        ),
        now=workflow_cancel_at,
    )

    async with fresh_db.acquire() as conn:
        evaluation = await evaluate_execution_state(
            conn,
            scope=ExecutionEvaluationScope(
                tenant_id=tenant_id,
                start=base - timedelta(minutes=1),
                end=base + timedelta(hours=1),
                run_id="reserved-effect-takeover-component-replay",
            ),
            artifact_refs=("pytest://reserved-effect-takeover-component-replay",),
        )
    assert evaluation.incident_counts == {}
    assert evaluation.missed_heartbeat_takeover_count == 1
    assert evaluation.safe_missed_heartbeat_takeover_count == 1
    assert evaluation.takeover_safety_rate == 1.0
    assert evaluation.effect_fate_counts == {"cancelled": 1}
    assert evaluation.work_fate_counts == {"cancelled": 1}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_effect_takeover_requires_exact_terminal_no_effect_receipt(fresh_db):
    base = START + timedelta(hours=7, minutes=30)
    tenant_id, objects, _ = await _setup_governed_effect(
        fresh_db, base=base, label="terminal_takeover"
    )
    predecessor = objects["lease"]
    attempt = objects["attempt"]
    state = ExternalEffectState.RESERVED
    receipts = []
    for seconds, target, provider_refs, external_refs in (
        (20, ExternalEffectState.DISPATCH_INTENT_RECORDED, (), ()),
        (21, ExternalEffectState.UNKNOWN, (), ()),
        (22, ExternalEffectState.RECONCILING, (), ()),
        (
            23,
            ExternalEffectState.RECONCILED_NO_EFFECT,
            ("provider-query:not-found",),
            ("provider-key:absent",),
        ),
    ):
        receipt_id = await _transition_effect(
            fresh_db,
            tenant_id=tenant_id,
            label="terminal_takeover",
            attempt_id=attempt.effect_attempt_id,
            expected_version=len(receipts) + 1,
            from_state=state,
            to_state=target,
            at=base + timedelta(seconds=seconds),
            provider_refs=provider_refs,
            external_refs=external_refs,
        )
        receipts.append(receipt_id)
        state = target

    takeover_at = predecessor.heartbeat_deadline
    successor = LeaseToken(
        lease_token_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=objects["work"].obligation_id,
        obligation_generation=1,
        fence=2,
        attempt=2,
        owner_ref="worker:terminal-takeover:2",
        heartbeat_deadline=takeover_at + timedelta(minutes=2),
        expires_at=takeover_at + timedelta(minutes=10),
        effect_possible=True,
        granted_at=takeover_at,
    )
    takeover = LeaseTakeover(
        takeover_id=uuid7(),
        tenant_id=tenant_id,
        obligation_id=objects["work"].obligation_id,
        obligation_generation=1,
        predecessor_lease_token_id=predecessor.lease_token_id,
        predecessor_fence=1,
        predecessor_attempt=1,
        predecessor_owner_ref=predecessor.owner_ref,
        predecessor_heartbeat_deadline=predecessor.heartbeat_deadline,
        successor=successor,
        no_effect_evidence_refs=(f"execution-receipt:{receipts[-2]}",),
        reason="probe a stale nonterminal reconciliation receipt",
        taken_over_at=takeover_at,
    )
    with pytest.raises(InvariantViolation, match="exact predecessor ledger evidence"):
        await _apply(
            fresh_db,
            WorkLedgerApplier(),
            "take_over_lease",
            command=LeaseTakeoverCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="WorkLedgerApplier",
                    responsibility="work_obligation",
                    operation="reject_stale_terminal_takeover_receipt",
                    at=takeover_at,
                    key="terminal-takeover:stale-receipt",
                ),
                expected_obligation_version=3,
                expected_predecessor_lease_version=1,
                takeover=takeover,
            ),
            now=takeover_at,
        )

    takeover = takeover.model_copy(
        update={
            "takeover_id": uuid7(),
            "no_effect_evidence_refs": (f"execution-receipt:{receipts[-1]}",),
            "reason": "the exact current receipt proves terminal known no effect",
        }
    )
    await _apply(
        fresh_db,
        WorkLedgerApplier(),
        "take_over_lease",
        command=LeaseTakeoverCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="take_over_terminal_no_effect_work",
                at=takeover_at,
                key="terminal-takeover:exact-receipt",
            ),
            expected_obligation_version=3,
            expected_predecessor_lease_version=1,
            takeover=takeover,
        ),
        now=takeover_at,
    )

    retry_at = takeover_at + timedelta(seconds=1)
    retry_attempt = attempt.model_copy(
        update={
            "effect_attempt_id": uuid7(),
            "generation": 2,
            "prior_attempt_id": attempt.effect_attempt_id,
            "lease_token_id": successor.lease_token_id,
            "lease_fence": 2,
            "reserved_at": retry_at,
            "dispatch_deadline": retry_at + timedelta(minutes=5),
        }
    )
    await _apply(
        fresh_db,
        ExecutionLedgerApplier(),
        "reserve",
        command=EffectReservationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ExecutionLedgerApplier",
                responsibility="external_effect",
                operation="reserve_after_terminal_takeover",
                at=retry_at,
                key="terminal-takeover:retry:reserve",
            ),
            attempt=retry_attempt,
        ),
        now=retry_at,
    )
    retry_cancel_at = retry_at + timedelta(seconds=1)
    retry_cancellation_receipt = await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="terminal_takeover_retry",
        attempt_id=retry_attempt.effect_attempt_id,
        expected_version=1,
        from_state=ExternalEffectState.RESERVED,
        to_state=ExternalEffectState.CANCELLED,
        at=retry_cancel_at,
    )
    work_cancel_at = retry_cancel_at + timedelta(seconds=1)
    await _apply(
        fresh_db,
        WorkLedgerApplier(),
        "resolve_lease",
        command=LeaseResolutionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="cancel_terminal_takeover_retry",
                at=work_cancel_at,
                key="terminal-takeover:work:cancelled",
            ),
            expected_obligation_version=4,
            expected_lease_version=1,
            resolution=LeaseResolution(
                lease_token_id=successor.lease_token_id,
                tenant_id=tenant_id,
                obligation_id=objects["work"].obligation_id,
                obligation_generation=1,
                fence=2,
                to_lease_state=LeaseState.RELEASED,
                to_work_state=WorkObligationState.CANCELLED,
                effect_may_have_occurred=False,
                result_evidence_refs=(str(retry_cancellation_receipt),),
                reason="the successor reservation was cancelled before dispatch",
                resolved_at=work_cancel_at,
            ),
        ),
        now=work_cancel_at,
    )
    task_cancel_at = work_cancel_at + timedelta(seconds=1)
    task = objects["task"].model_copy(
        update={
            "state": TaskState.CANCELLED,
            "completion_evidence_refs": (str(retry_cancellation_receipt),),
            "transition_reason": "terminal takeover retry was safely cancelled",
            "updated_at": task_cancel_at,
        }
    )
    await _apply(
        fresh_db,
        AgencyStateApplier(),
        "apply_task",
        command=TaskCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="task",
                operation="cancel_terminal_takeover_task",
                at=task_cancel_at,
                key="terminal-takeover:task:cancelled",
            ),
            expected_version=3,
            snapshot=task,
        ),
        now=task_cancel_at,
    )
    workflow_cancel_at = task_cancel_at + timedelta(seconds=1)
    workflow = objects["workflow"].model_copy(
        update={
            "state": WorkflowRunState.CANCELLED,
            "completion_evidence_refs": (str(retry_cancellation_receipt),),
            "transition_reason": "the terminal takeover task was cancelled",
            "updated_at": workflow_cancel_at,
        }
    )
    await _apply(
        fresh_db,
        AgencyStateApplier(),
        "apply_workflow_run",
        command=WorkflowRunCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="workflow_run",
                operation="cancel_terminal_takeover_workflow",
                at=workflow_cancel_at,
                key="terminal-takeover:workflow:cancelled",
            ),
            expected_version=2,
            snapshot=workflow,
        ),
        now=workflow_cancel_at,
    )

    async with fresh_db.acquire() as conn:
        evaluation = await evaluate_execution_state(
            conn,
            scope=ExecutionEvaluationScope(
                tenant_id=tenant_id,
                start=base - timedelta(minutes=1),
                end=base + timedelta(hours=1),
                run_id="terminal-effect-takeover-component-replay",
            ),
            artifact_refs=("pytest://terminal-effect-takeover-component-replay",),
        )
    assert evaluation.incident_counts == {}
    assert evaluation.missed_heartbeat_takeover_count == 1
    assert evaluation.safe_missed_heartbeat_takeover_count == 1
    assert evaluation.takeover_safety_rate == 1.0
    assert evaluation.retry_attempt_count == 1
    assert evaluation.safe_retry_attempt_count == 1
    assert evaluation.retry_safety_rate == 1.0
    assert evaluation.effect_fate_counts == {
        "cancelled": 1,
        "reconciled_no_effect": 1,
    }


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("proposal_fate", "effect_fate"),
    [
        (
            ConsequentialProposalFate.REJECTED,
            ExternalEffectState.COMPENSATION_REJECTED,
        ),
        (
            ConsequentialProposalFate.EXPIRED,
            ExternalEffectState.COMPENSATION_EXPIRED,
        ),
    ],
)
async def test_compensation_proposal_terminal_fate_binds_exact_review(
    fresh_db,
    proposal_fate,
    effect_fate,
):
    base = START + timedelta(hours=9)
    label = f"{proposal_fate.value}_compensation"
    tenant_id, original, capabilities = await _setup_governed_effect(
        fresh_db,
        base=base,
        label=f"{label}_original",
        compensation_supported=True,
        reversible=True,
        compensation_declaration="post a separately reviewed correction",
    )
    original_attempt = original["attempt"]
    state = ExternalEffectState.RESERVED
    for seconds, target, provider_refs, external_refs in (
        (20, ExternalEffectState.DISPATCH_INTENT_RECORDED, (), ()),
        (21, ExternalEffectState.ACKNOWLEDGED, ("provider:accepted",), ()),
        (
            22,
            ExternalEffectState.PARTIALLY_EXECUTED,
            (),
            ("provider-read:partial",),
        ),
    ):
        await _transition_effect(
            fresh_db,
            tenant_id=tenant_id,
            label=label,
            attempt_id=original_attempt.effect_attempt_id,
            expected_version=(seconds - 19),
            from_state=state,
            to_state=target,
            at=base + timedelta(seconds=seconds),
            provider_refs=provider_refs,
            external_refs=external_refs,
        )
        state = target

    _, compensation_proposal, compensation_spec = await _register_action_proposal(
        fresh_db,
        tenant_id=tenant_id,
        capabilities=capabilities,
        base=base + timedelta(minutes=1),
        label=label,
        operation="send_message",
        parameters={"channel": "C1", "text": "reviewed correction"},
        grounding_refs=(
            f"external-effect-attempt:{original_attempt.effect_attempt_id}",
        ),
        reversible=False,
        compensation_declaration="no automatic nested compensation",
    )
    proposed_at = base + timedelta(minutes=1, seconds=2)
    await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label=label,
        attempt_id=original_attempt.effect_attempt_id,
        expected_version=4,
        from_state=ExternalEffectState.PARTIALLY_EXECUTED,
        to_state=ExternalEffectState.COMPENSATION_PROPOSED,
        at=proposed_at,
        compensation_intervention_spec_digest=compensation_spec.spec_digest,
    )
    with pytest.raises(InvariantViolation, match="exact current proposal review"):
        await _transition_effect(
            fresh_db,
            tenant_id=tenant_id,
            label=f"{label}_unreviewed",
            attempt_id=original_attempt.effect_attempt_id,
            expected_version=5,
            from_state=ExternalEffectState.COMPENSATION_PROPOSED,
            to_state=effect_fate,
            at=proposed_at + timedelta(seconds=1),
            external_refs=(f"agency-command-result:{uuid7()}",),
        )

    review_at = (
        compensation_proposal.review_due_at
        if proposal_fate is ConsequentialProposalFate.EXPIRED
        else proposed_at + timedelta(seconds=2)
    )
    review = ConsequentialProposalReview(
        review_id=uuid7(),
        tenant_id=tenant_id,
        proposal_id=compensation_proposal.proposal_id,
        proposal_version=compensation_proposal.proposal_version,
        proposal_digest=compensation_proposal.proposal_digest,
        intervention_spec_digest=compensation_spec.spec_digest,
        from_fate=ConsequentialProposalFate.OPEN,
        to_fate=proposal_fate,
        principal_or_policy_ref="principal:operations-owner",
        authority=_consumption_authority(
            tenant_id=tenant_id,
            operation=f"review_{label}",
            at=review_at,
        ),
        reason=f"the compensation proposal is {proposal_fate.value}",
        decided_at=review_at,
    )
    if proposal_fate is ConsequentialProposalFate.EXPIRED:
        early_at = proposed_at + timedelta(seconds=2)
        early_review = review.model_copy(
            update={
                "review_id": uuid7(),
                "authority": _consumption_authority(
                    tenant_id=tenant_id,
                    operation=f"prematurely_expire_{label}",
                    at=early_at,
                ),
                "reason": "probe premature expiration",
                "decided_at": early_at,
            }
        )
        with pytest.raises(InvariantViolation, match="cannot expire before"):
            await _apply(
                fresh_db,
                ProposalAppender(),
                "review_consequential",
                command=ConsequentialProposalReviewCommand(
                    context=_context(
                        tenant_id=tenant_id,
                        owner="ProposalAppender",
                        responsibility="consequential_proposal",
                        operation=f"prematurely_expire_{label}",
                        at=early_at,
                        key=f"{label}:proposal:premature-expiry",
                    ),
                    review=early_review,
                ),
                now=early_at,
            )
    review_result = await _apply(
        fresh_db,
        ProposalAppender(),
        "review_consequential",
        command=ConsequentialProposalReviewCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ProposalAppender",
                responsibility="consequential_proposal",
                operation=f"review_{label}",
                at=review_at,
                key=f"{label}:proposal:{proposal_fate.value}",
            ),
            review=review,
        ),
        now=review_at,
    )
    terminal_at = review_at + timedelta(seconds=1)
    terminal_receipt_id = await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label=label,
        attempt_id=original_attempt.effect_attempt_id,
        expected_version=5,
        from_state=ExternalEffectState.COMPENSATION_PROPOSED,
        to_state=effect_fate,
        at=terminal_at,
        external_refs=(
            f"agency-command-result:{review_result.command_result_id}",
        ),
    )
    close_at = terminal_at + timedelta(seconds=1)
    await _apply(
        fresh_db,
        WorkLedgerApplier(),
        "resolve_lease",
        command=LeaseResolutionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation=f"close_{label}_work",
                at=close_at,
                key=f"{label}:work:cancelled",
            ),
            expected_obligation_version=3,
            expected_lease_version=1,
            resolution=LeaseResolution(
                lease_token_id=original["lease"].lease_token_id,
                tenant_id=tenant_id,
                obligation_id=original["work"].obligation_id,
                obligation_generation=1,
                fence=1,
                to_lease_state=LeaseState.TERMINAL,
                to_work_state=WorkObligationState.CANCELLED,
                effect_may_have_occurred=True,
                result_evidence_refs=(str(terminal_receipt_id),),
                reason=f"compensation proposal {proposal_fate.value}",
                resolved_at=close_at,
            ),
        ),
        now=close_at,
    )
    task_at = close_at + timedelta(seconds=1)
    task = original["task"].model_copy(
        update={
            "state": TaskState.CANCELLED,
            "effect_attempt_id": original_attempt.effect_attempt_id,
            "execution_receipt_id": terminal_receipt_id,
            "transition_reason": f"compensation {proposal_fate.value}",
            "updated_at": task_at,
        }
    )
    await _apply(
        fresh_db,
        AgencyStateApplier(),
        "apply_task",
        command=TaskCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="task",
                operation=f"cancel_{label}_task",
                at=task_at,
                key=f"{label}:task:cancelled",
            ),
            expected_version=3,
            snapshot=task,
        ),
        now=task_at,
    )
    workflow_at = task_at + timedelta(seconds=1)
    workflow = original["workflow"].model_copy(
        update={
            "state": WorkflowRunState.CANCELLED,
            "transition_reason": f"compensation {proposal_fate.value}",
            "updated_at": workflow_at,
        }
    )
    await _apply(
        fresh_db,
        AgencyStateApplier(),
        "apply_workflow_run",
        command=WorkflowRunCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="workflow_run",
                operation=f"cancel_{label}_workflow",
                at=workflow_at,
                key=f"{label}:workflow:cancelled",
            ),
            expected_version=2,
            snapshot=workflow,
        ),
        now=workflow_at,
    )

    await _exercise_foreign_owner_terminalization(
        fresh_db,
        tenant_id=tenant_id,
        base=workflow_at + timedelta(seconds=1),
        label=label,
        semantic_owner_writer_id="ProposalAppender",
        target_object_type="consequential_proposal",
        target_object_id=compensation_proposal.proposal_id,
        terminal_state=proposal_fate.value,
        owner_result=review_result,
    )

    async with fresh_db.acquire() as conn:
        evaluation = await evaluate_execution_state(
            conn,
            scope=ExecutionEvaluationScope(
                tenant_id=tenant_id,
                start=base - timedelta(minutes=1),
                end=base + timedelta(hours=2),
                run_id=f"{label}-component-replay",
            ),
            artifact_refs=(f"pytest://{label}-component-replay",),
        )
    assert evaluation.incident_counts == {}
    assert evaluation.compensation_episode_count == 1
    assert evaluation.compensation_integrity_rate == 1.0
    assert evaluation.terminal_compensation_episode_count == 1
    assert evaluation.compensation_closure_rate == 1.0
    assert evaluation.effect_fate_counts == {effect_fate.value: 1}
    assert evaluation.owner_terminalization_request_count == 1
    assert evaluation.valid_owner_terminalization_count == 1
    assert evaluation.owner_terminalization_closure_rate == 1.0
    assert evaluation.owner_terminalization_writer_counts == {
        "ProposalAppender": 1
    }
    assert evaluation.resolved_owner_terminalization_writer_counts == {
        "ProposalAppender": 1
    }
    assert evaluation.owner_terminalization_writer_closure_rates == {
        "ProposalAppender": 1.0
    }


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "reconciliation_result_state",
        "linked_terminal_state",
        "original_terminal_state",
        "nested_compensation_probe",
    ),
    [
        (
            ExternalEffectState.SUCCEEDED,
            ExternalEffectState.SUCCEEDED,
            ExternalEffectState.COMPENSATED,
            False,
        ),
        (
            ExternalEffectState.FAILED,
            ExternalEffectState.FAILED,
            ExternalEffectState.COMPENSATION_FAILED,
            False,
        ),
        (
            ExternalEffectState.PARTIALLY_EXECUTED,
            ExternalEffectState.TERMINAL_PARTIAL,
            ExternalEffectState.COMPENSATION_FAILED,
            True,
        ),
    ],
)
async def test_partial_effect_compensation_is_a_separately_authorized_attempt(
    fresh_db,
    monkeypatch,
    reconciliation_result_state,
    linked_terminal_state,
    original_terminal_state,
    nested_compensation_probe,
):
    compensation_succeeded = linked_terminal_state is ExternalEffectState.SUCCEEDED
    base = START + timedelta(hours=8)
    tenant_id = uuid4()
    capabilities = ActionAdapterCapabilities(
        capability_id=uuid7(),
        tenant_id=tenant_id,
        capability_version="slack-compensation-adapter-v1",
        adapter_name="slack-governed-message-lifecycle",
        provider_name="slack",
        permitted_operations=frozenset({"send_message", "post_correction"}),
        request_canonicalization_version="slack-governed-message-v1",
        idempotency_supported=True,
        idempotency_scope="workspace/channel/client-operation-id",
        idempotency_retention_until=base + timedelta(days=3),
        reconciliation_supported=True,
        reconciliation_consistency_window_seconds=30,
        cancellation_supported=False,
        partial_effect_observable=True,
        compensation_supported=True,
        verified_at=base,
        expires_at=base + timedelta(days=2),
    )
    await _apply(
        fresh_db,
        ExecutionLedgerApplier(),
        "register_capabilities",
        command=AdapterCapabilityRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ExecutionLedgerApplier",
                responsibility="action_adapter_capability",
                operation="register_compensation_capability",
                at=base,
                key="compensation:capability:v1",
            ),
            expected_version=0,
            capabilities=capabilities,
        ),
        now=base,
    )

    _, original_proposal, original_spec = await _register_action_proposal(
        fresh_db,
        tenant_id=tenant_id,
        capabilities=capabilities,
        base=base + timedelta(minutes=1),
        label="original",
        operation="send_message",
        parameters={"channel_id": "C1", "text": "Partial rollout notice"},
        grounding_refs=("grounding:channel:C1:v2",),
        reversible=True,
        compensation_declaration="post one correction linked to the original effect",
    )
    original_decision = await _authorize_action(
        fresh_db,
        tenant_id=tenant_id,
        base=base + timedelta(minutes=1, seconds=10),
        label="original",
        proposal=original_proposal,
        spec=original_spec,
    )
    original = await _instantiate_effect(
        fresh_db,
        tenant_id=tenant_id,
        capabilities=capabilities,
        base=base + timedelta(minutes=2),
        label="original",
        spec=original_spec,
        decision=original_decision,
    )
    original_attempt = original["attempt"]
    dispatch_at = base + timedelta(minutes=3)
    await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="original",
        attempt_id=original_attempt.effect_attempt_id,
        expected_version=1,
        from_state=ExternalEffectState.RESERVED,
        to_state=ExternalEffectState.DISPATCH_INTENT_RECORDED,
        at=dispatch_at,
    )
    await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="original",
        attempt_id=original_attempt.effect_attempt_id,
        expected_version=2,
        from_state=ExternalEffectState.DISPATCH_INTENT_RECORDED,
        to_state=ExternalEffectState.ACKNOWLEDGED,
        at=dispatch_at + timedelta(seconds=1),
        provider_refs=("provider:original:accepted",),
    )
    await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="original",
        attempt_id=original_attempt.effect_attempt_id,
        expected_version=3,
        from_state=ExternalEffectState.ACKNOWLEDGED,
        to_state=ExternalEffectState.PARTIALLY_EXECUTED,
        at=dispatch_at + timedelta(seconds=2),
        external_refs=("provider-read:original:partial-effect",),
    )

    _, compensation_proposal, compensation_spec = await _register_action_proposal(
        fresh_db,
        tenant_id=tenant_id,
        capabilities=capabilities,
        base=base + timedelta(minutes=4),
        label="compensation",
        operation="post_correction",
        parameters={"channel_id": "C1", "text": "Correction to partial notice"},
        grounding_refs=(
            f"external-effect-attempt:{original_attempt.effect_attempt_id}",
        ),
        reversible=False,
        compensation_declaration="no automatic nested compensation",
    )
    await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="original",
        attempt_id=original_attempt.effect_attempt_id,
        expected_version=4,
        from_state=ExternalEffectState.PARTIALLY_EXECUTED,
        to_state=ExternalEffectState.COMPENSATION_PROPOSED,
        at=base + timedelta(minutes=4, seconds=2),
        compensation_intervention_spec_digest=compensation_spec.spec_digest,
    )
    compensation_decision = await _authorize_action(
        fresh_db,
        tenant_id=tenant_id,
        base=base + timedelta(minutes=4, seconds=3),
        label="compensation",
        proposal=compensation_proposal,
        spec=compensation_spec,
    )
    wrong_decision_id = uuid7()
    with pytest.raises(InvariantViolation, match="unknown authorization decision"):
        await _transition_effect(
            fresh_db,
            tenant_id=tenant_id,
            label="original",
            attempt_id=original_attempt.effect_attempt_id,
            expected_version=5,
            from_state=ExternalEffectState.COMPENSATION_PROPOSED,
            to_state=ExternalEffectState.COMPENSATION_AUTHORIZED,
            at=base + timedelta(minutes=4, seconds=5),
            compensation_intervention_spec_digest=compensation_spec.spec_digest,
            compensation_authorization_decision_id=wrong_decision_id,
            compensation_authorization_ref=(
                f"authorization-decision:{wrong_decision_id}"
            ),
        )
    await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="original",
        attempt_id=original_attempt.effect_attempt_id,
        expected_version=5,
        from_state=ExternalEffectState.COMPENSATION_PROPOSED,
        to_state=ExternalEffectState.COMPENSATION_AUTHORIZED,
        at=base + timedelta(minutes=4, seconds=5),
        compensation_intervention_spec_digest=compensation_spec.spec_digest,
        compensation_authorization_decision_id=compensation_decision.decision_id,
        compensation_authorization_ref=(
            f"authorization-decision:{compensation_decision.decision_id}"
        ),
    )

    compensation = await _instantiate_effect(
        fresh_db,
        tenant_id=tenant_id,
        capabilities=capabilities,
        base=base + timedelta(minutes=5),
        label="compensation",
        spec=compensation_spec,
        decision=compensation_decision,
        compensates_effect_attempt_id=original_attempt.effect_attempt_id,
    )
    compensation_attempt = compensation["attempt"]
    link_at = base + timedelta(minutes=5, seconds=10)

    async def crash_before_event_and_outbox(**_kwargs):
        raise RuntimeError("simulated crash before event and outbox commit")

    with monkeypatch.context() as crash:
        crash.setattr(
            execution_repo,
            "insert_protocol_event_and_outbox",
            crash_before_event_and_outbox,
        )
        with pytest.raises(RuntimeError, match="simulated crash"):
            await _transition_effect(
                fresh_db,
                tenant_id=tenant_id,
                label="original_link_crash",
                attempt_id=original_attempt.effect_attempt_id,
                expected_version=6,
                from_state=ExternalEffectState.COMPENSATION_AUTHORIZED,
                to_state=ExternalEffectState.COMPENSATION_ATTEMPT_LINKED,
                at=link_at,
                compensation_attempt_id=compensation_attempt.effect_attempt_id,
            )
    async with fresh_db.acquire() as conn:
        rolled_back_head = await conn.fetchrow(
            """
            SELECT current_version, current_state, current_compensation_attempt_id
            FROM external_effect_attempt_heads
            WHERE tenant_id=$1 AND effect_attempt_id=$2
            """,
            tenant_id,
            original_attempt.effect_attempt_id,
        )
        rolled_back_version_count = await conn.fetchval(
            """
            SELECT count(*) FROM external_effect_attempt_versions
            WHERE tenant_id=$1 AND effect_attempt_id=$2 AND aggregate_version=7
            """,
            tenant_id,
            original_attempt.effect_attempt_id,
        )
        rolled_back_receipt_count = await conn.fetchval(
            """
            SELECT count(*) FROM execution_receipts
            WHERE tenant_id=$1 AND effect_attempt_id=$2 AND effect_version=7
            """,
            tenant_id,
            original_attempt.effect_attempt_id,
        )
    assert dict(rolled_back_head) == {
        "current_version": 6,
        "current_state": "compensation_authorized",
        "current_compensation_attempt_id": None,
    }
    assert rolled_back_version_count == 0
    assert rolled_back_receipt_count == 0

    await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="original",
        attempt_id=original_attempt.effect_attempt_id,
        expected_version=6,
        from_state=ExternalEffectState.COMPENSATION_AUTHORIZED,
        to_state=ExternalEffectState.COMPENSATION_ATTEMPT_LINKED,
        at=link_at,
        compensation_attempt_id=compensation_attempt.effect_attempt_id,
    )
    compensation_dispatch_at = base + timedelta(minutes=6)
    await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="compensation",
        attempt_id=compensation_attempt.effect_attempt_id,
        expected_version=1,
        from_state=ExternalEffectState.RESERVED,
        to_state=ExternalEffectState.DISPATCH_INTENT_RECORDED,
        at=compensation_dispatch_at,
    )
    await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="compensation",
        attempt_id=compensation_attempt.effect_attempt_id,
        expected_version=2,
        from_state=ExternalEffectState.DISPATCH_INTENT_RECORDED,
        to_state=ExternalEffectState.ACKNOWLEDGED,
        at=compensation_dispatch_at + timedelta(seconds=1),
        provider_refs=("provider:compensation:accepted",),
    )
    compensation_unknown_receipt_id = await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="compensation",
        attempt_id=compensation_attempt.effect_attempt_id,
        expected_version=3,
        from_state=ExternalEffectState.ACKNOWLEDGED,
        to_state=ExternalEffectState.UNKNOWN,
        at=compensation_dispatch_at + timedelta(seconds=2),
    )
    await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="original",
        attempt_id=original_attempt.effect_attempt_id,
        expected_version=7,
        from_state=ExternalEffectState.COMPENSATION_ATTEMPT_LINKED,
        to_state=ExternalEffectState.COMPENSATION_UNKNOWN,
        at=compensation_dispatch_at + timedelta(seconds=3),
        external_refs=(
            f"execution-receipt:{compensation_unknown_receipt_id}",
        ),
    )
    await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="original",
        attempt_id=original_attempt.effect_attempt_id,
        expected_version=8,
        from_state=ExternalEffectState.COMPENSATION_UNKNOWN,
        to_state=ExternalEffectState.COMPENSATION_RECONCILING,
        at=compensation_dispatch_at + timedelta(seconds=4),
    )
    await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="compensation",
        attempt_id=compensation_attempt.effect_attempt_id,
        expected_version=4,
        from_state=ExternalEffectState.UNKNOWN,
        to_state=ExternalEffectState.RECONCILING,
        at=compensation_dispatch_at + timedelta(seconds=5),
    )
    compensation_terminal_receipt_id = await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="compensation",
        attempt_id=compensation_attempt.effect_attempt_id,
        expected_version=5,
        from_state=ExternalEffectState.RECONCILING,
        to_state=reconciliation_result_state,
        at=compensation_dispatch_at + timedelta(seconds=6),
        external_refs=(
            "provider-read:correction-visible"
            if compensation_succeeded
            else "provider-read:compensation-failed",
        ),
    )
    if nested_compensation_probe:
        _, _, nested_spec = await _register_action_proposal(
            fresh_db,
            tenant_id=tenant_id,
            capabilities=capabilities,
            base=compensation_dispatch_at + timedelta(seconds=7),
            label="nested_compensation",
            operation="post_correction",
            parameters={"channel_id": "C1", "text": "Nested correction"},
            grounding_refs=(
                f"external-effect-attempt:{compensation_attempt.effect_attempt_id}",
            ),
            reversible=False,
            compensation_declaration="nested reversal is not authorized",
        )
        with pytest.raises(InvariantViolation, match="supported reversal"):
            await _transition_effect(
                fresh_db,
                tenant_id=tenant_id,
                label="nested_compensation",
                attempt_id=compensation_attempt.effect_attempt_id,
                expected_version=6,
                from_state=ExternalEffectState.PARTIALLY_EXECUTED,
                to_state=ExternalEffectState.COMPENSATION_PROPOSED,
                at=compensation_dispatch_at + timedelta(seconds=9),
                compensation_intervention_spec_digest=nested_spec.spec_digest,
            )
        compensation_terminal_receipt_id = await _transition_effect(
            fresh_db,
            tenant_id=tenant_id,
            label="compensation_terminal_partial",
            attempt_id=compensation_attempt.effect_attempt_id,
            expected_version=6,
            from_state=ExternalEffectState.PARTIALLY_EXECUTED,
            to_state=ExternalEffectState.TERMINAL_PARTIAL,
            at=compensation_dispatch_at + timedelta(seconds=9),
            external_refs=("provider-read:compensation-terminal-partial",),
        )
    original_terminal_at = compensation_dispatch_at + timedelta(
        seconds=10 if nested_compensation_probe else 7
    )
    with pytest.raises(InvariantViolation, match="exact linked attempt receipt"):
        await _transition_effect(
            fresh_db,
            tenant_id=tenant_id,
            label="original",
            attempt_id=original_attempt.effect_attempt_id,
            expected_version=9,
            from_state=ExternalEffectState.COMPENSATION_RECONCILING,
            to_state=original_terminal_state,
            at=original_terminal_at,
            external_refs=(f"execution-receipt:{uuid7()}",),
        )
    original_terminal_receipt_id = await _transition_effect(
        fresh_db,
        tenant_id=tenant_id,
        label="original",
        attempt_id=original_attempt.effect_attempt_id,
        expected_version=9,
        from_state=ExternalEffectState.COMPENSATION_RECONCILING,
        to_state=original_terminal_state,
        at=original_terminal_at,
        external_refs=(
            f"execution-receipt:{compensation_terminal_receipt_id}",
        ),
    )

    close_at = base + timedelta(minutes=7)
    work_ledger = WorkLedgerApplier()
    agency = AgencyStateApplier()
    await _apply(
        fresh_db,
        work_ledger,
        "resolve_lease",
        command=LeaseResolutionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation=(
                    "complete_compensation_work"
                    if compensation_succeeded
                    else "close_failed_compensation_work"
                ),
                at=close_at,
                key=(
                    "compensation:work:complete"
                    if compensation_succeeded
                    else "compensation:work:failed"
                ),
            ),
            expected_obligation_version=3,
            expected_lease_version=1,
            resolution=LeaseResolution(
                lease_token_id=compensation["lease"].lease_token_id,
                tenant_id=tenant_id,
                obligation_id=compensation["work"].obligation_id,
                obligation_generation=1,
                fence=1,
                to_lease_state=(
                    LeaseState.COMPLETED
                    if compensation_succeeded
                    else LeaseState.TERMINAL
                ),
                to_work_state=(
                    WorkObligationState.COMPLETED
                    if compensation_succeeded
                    else WorkObligationState.CANCELLED
                ),
                effect_may_have_occurred=True,
                result_evidence_refs=(str(compensation_terminal_receipt_id),),
                reason=(
                    "separate compensation effect succeeded"
                    if compensation_succeeded
                    else "separate compensation effect failed with an exact receipt"
                ),
                resolved_at=close_at,
            ),
        ),
        now=close_at,
    )
    await _apply(
        fresh_db,
        work_ledger,
        "resolve_lease",
        command=LeaseResolutionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="WorkLedgerApplier",
                responsibility="work_obligation",
                operation="close_original_after_compensation_fate",
                at=close_at + timedelta(seconds=1),
                key="original:work:compensation-terminal",
            ),
            expected_obligation_version=3,
            expected_lease_version=1,
            resolution=LeaseResolution(
                lease_token_id=original["lease"].lease_token_id,
                tenant_id=tenant_id,
                obligation_id=original["work"].obligation_id,
                obligation_generation=1,
                fence=1,
                to_lease_state=LeaseState.TERMINAL,
                to_work_state=WorkObligationState.CANCELLED,
                effect_may_have_occurred=True,
                result_evidence_refs=(str(original_terminal_receipt_id),),
                reason=(
                    "original partial effect was separately compensated"
                    if compensation_succeeded
                    else "compensation failed and the original residual remains explicit"
                ),
                resolved_at=close_at + timedelta(seconds=1),
            ),
        ),
        now=close_at + timedelta(seconds=1),
    )
    compensation_task = compensation["task"].model_copy(
        update={
            "state": (
                TaskState.COMPLETED
                if compensation_succeeded
                else TaskState.CANCELLED
            ),
            "effect_attempt_id": compensation_attempt.effect_attempt_id,
            "execution_receipt_id": compensation_terminal_receipt_id,
            "completion_evidence_refs": (
                f"execution-receipt:{compensation_terminal_receipt_id}",
            ),
            "transition_reason": (
                "separate compensation action succeeded"
                if compensation_succeeded
                else "separate compensation action failed explicitly"
            ),
            "updated_at": close_at + timedelta(seconds=2),
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
                operation="terminalize_compensation_task",
                at=compensation_task.updated_at,
                key="compensation:task:terminal",
            ),
            expected_version=3,
            snapshot=compensation_task,
        ),
        now=compensation_task.updated_at,
    )
    original_task = original["task"].model_copy(
        update={
            "state": TaskState.CANCELLED,
            "effect_attempt_id": original_attempt.effect_attempt_id,
            "execution_receipt_id": original_terminal_receipt_id,
            "transition_reason": (
                "original partial effect was compensated"
                if compensation_succeeded
                else "compensation failed and residual closure is explicit"
            ),
            "updated_at": close_at + timedelta(seconds=3),
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
                operation="cancel_compensated_original_task",
                at=original_task.updated_at,
                key="original:task:compensated",
            ),
            expected_version=3,
            snapshot=original_task,
        ),
        now=original_task.updated_at,
    )
    compensation_workflow = compensation["workflow"].model_copy(
        update={
            "state": (
                WorkflowRunState.COMPLETED
                if compensation_succeeded
                else WorkflowRunState.CANCELLED
            ),
            "completion_evidence_refs": (
                f"task:{compensation_task.task_id}:completed",
            ),
            "transition_reason": (
                "compensation task completed"
                if compensation_succeeded
                else "compensation task failed explicitly"
            ),
            "updated_at": close_at + timedelta(seconds=4),
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
                operation="terminalize_compensation_workflow",
                at=compensation_workflow.updated_at,
                key="compensation:workflow:terminal",
            ),
            expected_version=2,
            snapshot=compensation_workflow,
        ),
        now=compensation_workflow.updated_at,
    )
    original_workflow = original["workflow"].model_copy(
        update={
            "state": WorkflowRunState.CANCELLED,
            "transition_reason": "original action closed through compensation",
            "updated_at": close_at + timedelta(seconds=5),
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
                operation="cancel_compensated_original_workflow",
                at=original_workflow.updated_at,
                key="original:workflow:compensated",
            ),
            expected_version=2,
            snapshot=original_workflow,
        ),
        now=original_workflow.updated_at,
    )

    async with fresh_db.acquire() as conn:
        states = await conn.fetch(
            """
            SELECT effect_attempt_id, current_state,
                   current_compensation_spec_digest,
                   current_compensation_authorization_decision_id,
                   current_compensation_attempt_id
            FROM external_effect_attempt_heads
            WHERE tenant_id=$1
            ORDER BY reserved_at
            """,
            tenant_id,
        )
        evaluation = await evaluate_execution_state(
            conn,
            scope=ExecutionEvaluationScope(
                tenant_id=tenant_id,
                start=base - timedelta(minutes=1),
                end=base + timedelta(hours=1),
                run_id="separate-compensation-component-replay",
            ),
            artifact_refs=("pytest://separate-compensation-component-replay",),
        )
    assert [row["current_state"] for row in states] == [
        original_terminal_state.value,
        linked_terminal_state.value,
    ]
    assert states[0]["current_compensation_spec_digest"] == compensation_spec.spec_digest
    assert (
        states[0]["current_compensation_authorization_decision_id"]
        == compensation_decision.decision_id
    )
    assert (
        states[0]["current_compensation_attempt_id"]
        == compensation_attempt.effect_attempt_id
    )
    assert evaluation.incident_counts == {}
    assert evaluation.effect_attempt_count == 2
    assert evaluation.effect_history_integrity_rate == 1.0
    assert evaluation.effect_continuity_rate == 1.0
    assert evaluation.receipt_closure_rate == 1.0
    assert evaluation.compensation_episode_count == 1
    assert evaluation.valid_compensation_episode_count == 1
    assert evaluation.compensation_integrity_rate == 1.0
    assert evaluation.terminal_compensation_episode_count == 1
    assert evaluation.closed_compensation_episode_count == 1
    assert evaluation.compensation_closure_rate == 1.0
    assert evaluation.unresolved_effect_count == 0
    expected_fates = (
        {"cancelled": 1, "completed": 1}
        if compensation_succeeded
        else {"cancelled": 2}
    )
    assert evaluation.work_fate_counts == expected_fates
    assert evaluation.task_state_counts == expected_fates
    assert evaluation.workflow_state_counts == expected_fates
