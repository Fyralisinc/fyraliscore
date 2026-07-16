"""Disposable deterministic harness for the first joined revised-system loop.

This module is test-only.  It composes production domain writers against a
disposable database and simulates provider/oracle observations; it owns no
runtime or production authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg

from lib.contracts.agency import (
    AgencyWriteContext,
    AttentionGovernanceBinding,
    AttentionSourceKind,
    Attribution,
    AttributionCommand,
    AuthorityBasisSurvivalMode,
    AuthorityBasisSurvivalPolicy,
    AuthorizationDecision,
    AuthorizationDecisionCommand,
    AuthorizationDisposition,
    ConsequentialProposal,
    ConsequentialProposalFate,
    ConsequentialProposalRegistrationCommand,
    ConsequentialProposalReview,
    ConsequentialProposalReviewCommand,
    ConstitutiveIntentAuthorityBasis,
    ConstitutiveIntentAuthorityBasisKind,
    ConcernCriterionState,
    ConcernEvaluationCommand,
    ConcernIdentity,
    CriterionImpact,
    CriterionWorkEligibility,
    EpisodeStageFate,
    EpisodeStageLink,
    EpisodeUpdateCommand,
    ExactProposalAcceptance,
    IntentMutation,
    IntentObjectKind,
    IntentOperation,
    InterpretedIntentProposal,
    InterventionEpisode,
    InterventionSpec,
    Outcome,
    OutcomeRecordingCommand,
    Prediction,
    PredictionKind,
    PredictionRegistrationCommand,
    ResidualClass,
    Settlement,
    SettlementCommand,
    SettlementDisposition,
    TypedConstitutiveIntentCommand,
    derive_concern_id,
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
from lib.contracts.kernel import (
    ConsumptionAuthorityContext,
    ProcessingAuthorityContext,
    RestrictionSet,
    WriterCutoverState,
    WriterScopeEpoch,
    canonical_sha256,
)
from lib.contracts.perception import CanonicalReferent, EntityLifecycleStatus
from lib.contracts.runtime import ProcessingClass
from lib.shared.ids import uuid7
from services.domain.acts.goals import create as create_goal
from services.domain.concerns import (
    AttentionGovernanceBindingRegistry,
    ConcernApplier,
)
from services.domain.execution import (
    AgencyStateApplier,
    ExecutionLedgerApplier,
    WorkLedgerApplier,
)
from services.domain.intent import (
    AppliedIntentMutation,
    IntentApplier,
    ProposalAppender,
)
from services.domain.outcomes import (
    AttributionApplier,
    AuthorizationApplier,
    EpisodeCoordinator,
    OutcomeRecorder,
    PredictionWriter,
    SettlementApplier,
)


@dataclass(frozen=True, slots=True)
class ClosedLoopArtifacts:
    tenant_id: UUID
    source_observation_id: UUID
    model_id: UUID
    intent_proposal_id: UUID
    intent_command_result_id: UUID
    goal_id: UUID
    attention_binding_ref: str
    concern_id: UUID
    concern_version: int
    episode_id: UUID
    proposal_id: UUID
    prediction_id: UUID
    authorization_id: UUID
    workflow_run_id: UUID
    task_id: UUID
    work_obligation_id: UUID
    effect_attempt_id: UUID
    execution_receipt_id: UUID
    outcome_id: UUID
    settlement_id: UUID
    attribution_id: UUID
    intervention_spec_digest: str
    completed_at: datetime


async def _apply(
    pool: asyncpg.Pool,
    applier,
    method: str,
    *,
    command,
    now: datetime,
):
    async with pool.acquire() as conn, conn.transaction():
        return await getattr(applier, method)(
            conn=conn,
            command=command,
            now=now,
        )


def _processing_authority(
    *,
    tenant_id: UUID,
    operation: str,
    at: datetime,
    purpose: str,
) -> ProcessingAuthorityContext:
    return ProcessingAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id=f"service:{operation}",
        purpose=purpose,
        operation=operation,
        object_types=RestrictionSet.unrestricted(),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only(
            "slack",
            "governed-proposal",
            "simulated-independent-source",
        ),
        authority_basis_refs=frozenset({f"processing-grant:{operation}"}),
        policy_version="closed-loop-processing-v1",
        authority_epoch=1,
        decision_time=at - timedelta(minutes=1),
        expires_at=at + timedelta(days=30),
    )


def _consumption_authority(
    *,
    tenant_id: UUID,
    operation: str,
    at: datetime,
    purpose: str,
) -> ConsumptionAuthorityContext:
    return ConsumptionAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id="actor:operations-owner",
        purpose=purpose,
        operation=operation,
        object_types=RestrictionSet.unrestricted(),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only(
            "slack",
            "governed-proposal",
            "simulated-independent-source",
        ),
        authority_basis_refs=frozenset({f"capability:{operation}"}),
        policy_version="closed-loop-consumption-v1",
        authority_epoch=1,
        decision_time=at - timedelta(minutes=1),
        expires_at=at + timedelta(days=30),
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
    processing = authority or _processing_authority(
        tenant_id=tenant_id,
        operation=operation,
        at=at,
        purpose="closed_loop_intervention",
    )
    return AgencyWriteContext(
        command_id=uuid7(),
        tenant_id=tenant_id,
        processing_authority=processing,
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


def _intent_authority(
    *,
    tenant_id: UUID,
    processing: bool,
    at: datetime,
) -> ProcessingAuthorityContext | ConsumptionAuthorityContext:
    cls = ProcessingAuthorityContext if processing else ConsumptionAuthorityContext
    return cls(
        tenant_id=tenant_id,
        principal_or_service_id=(
            "service:intent-interpreter" if processing else "actor:operations-owner"
        ),
        purpose="intent_mutation",
        operation="create",
        object_types=RestrictionSet.only("goal"),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("slack", "source-semantic"),
        authority_basis_refs=frozenset({"grant:operations-owner"}),
        policy_version="closed-loop-intent-authority-v1",
        authority_epoch=1,
        decision_time=at - timedelta(minutes=1),
        expires_at=at + timedelta(days=1),
    )


def _concern_command(
    *,
    tenant_id: UUID,
    concern_id: UUID,
    identity: ConcernIdentity,
    criterion: ConcernCriterionState,
    expected_version: int,
    state_estimate: dict,
    at: datetime,
    key: str,
) -> ConcernEvaluationCommand:
    return ConcernEvaluationCommand(
        command_id=uuid7(),
        tenant_id=tenant_id,
        concern_id=concern_id,
        expected_version=expected_version,
        identity=identity,
        criteria=(criterion,),
        current_state_estimate=state_estimate,
        materiality=0.9,
        uncertainty=0.2,
        consequence=0.9,
        urgency=0.8,
        actionability=0.9,
        evidence_cutoff=at,
        validity_deadline=at + timedelta(days=30),
        next_review_at=at + timedelta(days=1),
        transition_cause=key,
        originating_attention_source_ref=criterion.attention_source_ref,
        processing_authority=_processing_authority(
            tenant_id=tenant_id,
            operation="evaluate_concern",
            at=at,
            purpose="concern_evaluation",
        ),
        consumption_authority=_consumption_authority(
            tenant_id=tenant_id,
            operation="commit_concern",
            at=at,
            purpose="concern_evaluation",
        ),
        writer_scope_epoch=WriterScopeEpoch(
            scope_id=f"concern:{tenant_id}",
            tenant_id=tenant_id,
            semantic_responsibility="concern",
            source_partition=str(tenant_id),
            writer_owner="ConcernApplier",
            epoch=1,
            state=WriterCutoverState.NEW_CANONICAL,
        ),
        idempotency_key=key,
        issued_at=at,
        expires_at=at + timedelta(minutes=30),
    )


async def run_closed_loop_vertical(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    source_observation_id: UUID,
    model_id: UUID,
    source_assertion_ref: str,
    semantic_frame_ref: str,
    started_at: datetime,
    activation_worker: Any | None = None,
    work_scheduler: Any | None = None,
    effect_executor: Any | None = None,
    finalize_episode_manifest: bool = True,
) -> ClosedLoopArtifacts:
    """Run one deterministic canonical loop over a simulated external world."""

    if started_at.tzinfo is None or started_at.utcoffset() is None:
        raise ValueError("started_at must be timezone-aware")

    # Expressed model-derived direction remains a proposal until exact acceptance.
    intent_at = started_at + timedelta(minutes=1)
    mutation = IntentMutation(
        object_kind=IntentObjectKind.GOAL,
        operation=IntentOperation.CREATE,
        payload={
            "title": "Resolve the Nimbus Bank blocker",
            "description": "Remove the verified customer blocker without hiding risk.",
            "altitude": "operational",
            "success_criteria": {
                "metric": "nimbus_blocker_cleared",
                "target": True,
            },
        },
        schema_version="intent-mutation-v1",
        effective_at=intent_at,
    )
    intent_processing = _intent_authority(
        tenant_id=tenant_id,
        processing=True,
        at=intent_at,
    )
    assert isinstance(intent_processing, ProcessingAuthorityContext)
    intent_proposal = InterpretedIntentProposal(
        proposal_id=uuid7(),
        tenant_id=tenant_id,
        proposal_version=1,
        normalized_mutation=mutation,
        normalized_payload_digest=mutation.payload_digest,
        source_assertion_refs=(source_assertion_ref,),
        semantic_frame_refs=(semantic_frame_ref,),
        uncertainty_reasons=("model_output_is_not_constitutive_intent",),
        processing_authority=intent_processing,
        processing_authority_fingerprint=intent_processing.fingerprint,
        created_at=intent_at,
        review_due_at=intent_at + timedelta(hours=2),
    )
    acceptance_at = intent_at + timedelta(minutes=1)
    intent_consumption = _intent_authority(
        tenant_id=tenant_id,
        processing=False,
        at=acceptance_at,
    )
    assert isinstance(intent_consumption, ConsumptionAuthorityContext)
    acceptance = ExactProposalAcceptance(
        acceptance_id=uuid7(),
        tenant_id=tenant_id,
        proposal_id=intent_proposal.proposal_id,
        proposal_version=1,
        proposal_digest=canonical_sha256(intent_proposal.model_dump(mode="json")),
        normalized_payload_digest=intent_proposal.normalized_payload_digest,
        principal_id="actor:operations-owner",
        capability_ref="grant:operations-owner",
        authority=intent_consumption,
        accepted_at=acceptance_at,
        expires_at=acceptance_at + timedelta(hours=1),
    )
    basis = ConstitutiveIntentAuthorityBasis(
        kind=ConstitutiveIntentAuthorityBasisKind.EXPLICIT_PRINCIPAL,
        basis_id=f"acceptance:{acceptance.acceptance_id}",
        principal_or_actor_id=acceptance.principal_id,
        capability_or_grant_ref=acceptance.capability_ref,
        acknowledged_payload_digest=intent_proposal.normalized_payload_digest,
        valid_from=acceptance_at - timedelta(minutes=1),
        valid_until=acceptance.expires_at,
    )
    intent_command = TypedConstitutiveIntentCommand(
        command_id=uuid7(),
        tenant_id=tenant_id,
        mutation=mutation,
        declared_payload_digest=mutation.payload_digest,
        authority_basis=basis,
        survival_policy=AuthorityBasisSurvivalPolicy(
            policy_version="intent-survival-v1",
            mode=AuthorityBasisSurvivalMode.REVIEW_REQUIRED,
            maximum_mode_permitted_by_operation=(
                AuthorityBasisSurvivalMode.REVIEW_REQUIRED
            ),
            maximum_mode_permitted_by_basis=(
                AuthorityBasisSurvivalMode.POINT_IN_TIME_CONSTITUTIVE
            ),
        ),
        processing_authority=_intent_authority(
            tenant_id=tenant_id,
            processing=True,
            at=acceptance_at,
        ),
        consumption_authority=intent_consumption,
        writer_scope_epoch=WriterScopeEpoch(
            scope_id=f"intent:{tenant_id}",
            tenant_id=tenant_id,
            semantic_responsibility="intent",
            source_partition=str(tenant_id),
            writer_owner="IntentApplier",
            epoch=1,
            state=WriterCutoverState.NEW_CANONICAL,
        ),
        idempotency_key=f"acceptance:{acceptance.acceptance_id}",
        exact_input_anchors=(f"proposal:{intent_proposal.proposal_id}:v1",),
        issued_at=acceptance_at,
        expires_at=acceptance_at + timedelta(minutes=30),
        proposal_acceptance_ref=str(acceptance.acceptance_id),
    )

    async def _apply_goal(
        conn: asyncpg.Connection,
        accepted_mutation: IntentMutation,
    ) -> AppliedIntentMutation:
        goal = await create_goal(
            title=str(accepted_mutation.payload["title"]),
            description=str(accepted_mutation.payload["description"]),
            altitude=accepted_mutation.payload["altitude"],
            success_criteria=dict(accepted_mutation.payload["success_criteria"]),
            created_by_event_id=source_observation_id,
            tenant_id=tenant_id,
            conn=conn,
        )
        return AppliedIntentMutation(
            aggregate_id=goal.id,
            result_kind="create_goal",
            result_payload={"goal_id": str(goal.id)},
        )

    async with pool.acquire() as conn, conn.transaction():
        await ProposalAppender().append(
            conn=conn,
            proposal=intent_proposal,
            semantic_idempotency_key=f"closed-loop:intent:{model_id}",
            actor_or_service_ref="ClosedLoopHarness",
        )
        await ProposalAppender().accept_exact(conn=conn, acceptance=acceptance)
        intent_result = await IntentApplier().apply(
            conn=conn,
            command=intent_command,
            mutation_applier=_apply_goal,
            now=acceptance_at,
        )
        intent_duplicate = await IntentApplier().apply(
            conn=conn,
            command=intent_command,
            mutation_applier=_apply_goal,
            now=acceptance_at,
        )
    assert intent_duplicate.duplicate
    goal_id = intent_result.aggregate_id
    intent_ref = f"intent:{goal_id}"

    # The accepted Goal governs one scoped gap; the belief supplies actual state.
    binding_at = acceptance_at + timedelta(minutes=1)
    binding = AttentionGovernanceBinding(
        binding_id=uuid7(),
        binding_version=1,
        attention_source_ref=f"goal:{goal_id}",
        attention_source_kind=AttentionSourceKind.GOAL,
        work_budget_units=10,
        interruption_budget_count=1,
        interruption_budget_minutes=5,
        maximum_duration_seconds=3600,
        satisfaction_rule="Nimbus Bank blocker is independently observed cleared",
        expiry_rule="goal expires when superseded or after 30 days",
        review_rule="review daily while material",
        stop_rule="stop when outcome proves satisfied or value turns nonpositive",
        permitted_priority_modifier_fields=frozenset({"order"}),
        nonwaivable_fields=frozenset({"source_identity", "outcome_independence"}),
        valid_from=binding_at - timedelta(minutes=1),
        valid_until=binding_at + timedelta(days=30),
    )
    identity = ConcernIdentity(
        tenant_id=tenant_id,
        affected_object_or_scope="customer:nimbus-bank",
        state_dimension_or_missing_proposition="delivery_blocker",
        valid_time_window="current-quarter",
        gap_identity_policy_version="gap-v1",
    )
    concern_id = derive_concern_id(identity)
    criterion_ref = f"criterion:goal:{goal_id}:nimbus-blocker"
    candidate_criterion = ConcernCriterionState(
        criterion_ref=criterion_ref,
        attention_source_ref=binding.attention_source_ref,
        attention_binding_ref=binding.binding_ref,
        applicable=True,
        impact=CriterionImpact.UNKNOWN,
        work_eligibility=CriterionWorkEligibility.ACTIONABLE,
    )
    concern_applier = ConcernApplier()
    async with pool.acquire() as conn, conn.transaction():
        await AttentionGovernanceBindingRegistry().register(
            conn=conn,
            tenant_id=tenant_id,
            binding=binding,
            registered_by_ref=(
                f"intent-command-result:{intent_result.command_result_id}"
            ),
        )
        await concern_applier.apply_evaluation(
            conn=conn,
            command=_concern_command(
                tenant_id=tenant_id,
                concern_id=concern_id,
                identity=identity,
                criterion=candidate_criterion,
                expected_version=0,
                state_estimate={
                    "belief_model_refs": (f"belief:{model_id}",),
                    "intent_refs": (intent_ref,),
                    "status": "needs_evaluation",
                },
                at=binding_at,
                key="closed-loop:concern:candidate",
            ),
            now=binding_at,
        )
    open_at = binding_at + timedelta(minutes=1)
    open_criterion = candidate_criterion.model_copy(
        update={"impact": CriterionImpact.MATERIAL_GAP}
    )
    await _apply(
        pool,
        concern_applier,
        "apply_evaluation",
        command=_concern_command(
            tenant_id=tenant_id,
            concern_id=concern_id,
            identity=identity,
            criterion=open_criterion,
            expected_version=1,
            state_estimate={
                "belief_model_refs": (f"belief:{model_id}",),
                "intent_refs": (intent_ref,),
                "status": "blocked",
                "assertion": "NBI is blocked",
            },
            at=open_at,
            key="closed-loop:concern:open",
        ),
        now=open_at,
    )

    # Register the simulated provider's exact guarantees before proposing action.
    capability_at = open_at + timedelta(minutes=1)
    capabilities = ActionAdapterCapabilities(
        capability_id=uuid7(),
        tenant_id=tenant_id,
        capability_version="simulated-slack-adapter-v1",
        adapter_name="simulated-slack-message-delivery",
        provider_name="simulated-slack",
        permitted_operations=frozenset({"send_message"}),
        request_canonicalization_version="simulated-slack-request-v1",
        idempotency_supported=True,
        idempotency_scope="tenant/channel/client-message-id",
        idempotency_retention_until=capability_at + timedelta(days=7),
        reconciliation_supported=True,
        reconciliation_consistency_window_seconds=5,
        cancellation_supported=False,
        partial_effect_observable=True,
        compensation_supported=False,
        verified_at=capability_at,
        expires_at=capability_at + timedelta(days=30),
    )
    execution = ExecutionLedgerApplier()
    await _apply(
        pool,
        execution,
        "register_capabilities",
        command=AdapterCapabilityRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="ExecutionLedgerApplier",
                responsibility="action_adapter_capability",
                operation="register_adapter_capability",
                at=capability_at,
                key="closed-loop:capability",
            ),
            expected_version=0,
            capabilities=capabilities,
        ),
        now=capability_at,
    )

    episode_at = capability_at + timedelta(minutes=1)
    episode_id = uuid7()
    initial_episode = InterventionEpisode(
        episode_id=episode_id,
        tenant_id=tenant_id,
        stage_links=(
            EpisodeStageLink(
                stage="belief",
                fate=EpisodeStageFate.PRESENT,
                object_ref=f"belief:{model_id}",
                writer_id="EpistemicApplier",
            ),
            EpisodeStageLink(
                stage="intent",
                fate=EpisodeStageFate.PRESENT,
                object_ref=intent_ref,
                writer_id="IntentApplier",
            ),
            EpisodeStageLink(
                stage="concern",
                fate=EpisodeStageFate.PRESENT,
                object_ref=f"concern:{concern_id}",
                writer_id="ConcernApplier",
            ),
            EpisodeStageLink(
                stage="proposal",
                fate=EpisodeStageFate.NOT_EXECUTED,
                reason="proposal has not yet been registered",
            ),
        ),
        created_at=episode_at,
        updated_at=episode_at,
    )
    initial_episode_command = EpisodeUpdateCommand(
        context=_context(
            tenant_id=tenant_id,
            owner="EpisodeCoordinator",
            responsibility="intervention_episode",
            operation="create_episode",
            at=episode_at,
            key="closed-loop:episode:create",
        ),
        expected_version=0,
        episode=initial_episode,
    )
    await _apply(
        pool,
        EpisodeCoordinator(),
        "apply",
        command=initial_episode_command,
        now=episode_at,
    )

    proposal_at = episode_at + timedelta(minutes=1)
    forecast_start = proposal_at + timedelta(hours=1)
    forecast_end = forecast_start + timedelta(days=1)
    action_target = CanonicalReferent(
        tenant_id=tenant_id,
        referent_id="channel:customer-success",
        referent_version=1,
        lifecycle_status=EntityLifecycleStatus.ACTIVE,
        predecessor_referent_refs=(),
        successor_referent_refs=(),
        birth_decision_ref="source-binding:slack:C-CUSTOMER-SUCCESS:v1",
        positive_existence_evidence_refs=("slack-channel:C-CUSTOMER-SUCCESS",),
    )
    spec = InterventionSpec(
        spec_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        target_referent=action_target,
        target_version="slack-channel-v1",
        operation="send_message",
        parameters={
            "channel_id": "C-CUSTOMER-SUCCESS",
            "text": "Nimbus Bank is blocked; owner review is required.",
        },
        comparator={"delivery": "no_message"},
        outcome_metric="nimbus_blocker_cleared",
        outcome_window_start=forecast_start,
        outcome_window_end=forecast_end,
        workflow_spec_version_ref="workflow:governed-escalation:v1",
        action_adapter_version=capabilities.capability_version,
        action_adapter_capability_digest=capabilities.capability_digest,
        safety_and_preconditions=(
            "channel exists",
            "recipient is authorized to view the customer concern",
        ),
        authority_requirement="capability:send_governed_notification",
        reversible=False,
        compensation_declaration="cannot unsend; issue a correction if necessary",
        grounding_dependency_refs=(f"grounded-model:{model_id}",),
        context_dependency_manifest_digest=canonical_sha256(
            {
                "model_id": str(model_id),
                "goal_id": str(goal_id),
                "concern_id": str(concern_id),
            }
        ),
    )
    proposal_processing = _processing_authority(
        tenant_id=tenant_id,
        operation="register_proposal",
        at=proposal_at,
        purpose="closed_loop_intervention",
    )
    proposal = ConsequentialProposal(
        proposal_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        intervention_spec=spec,
        summary="Send one governed escalation to a capable owner",
        rationale="A verified customer blocker conflicts with an accepted goal",
        alternative_refs=("alternative:in-app-only", "alternative:no-action"),
        source_refs=(
            f"belief:{model_id}",
            intent_ref,
            f"concern:{concern_id}",
        ),
        processing_authority=proposal_processing,
        processing_authority_fingerprint=proposal_processing.fingerprint,
        created_at=proposal_at,
        review_due_at=proposal_at + timedelta(hours=1),
    )
    proposal_command = ConsequentialProposalRegistrationCommand(
        context=_context(
            tenant_id=tenant_id,
            owner="ProposalAppender",
            responsibility="consequential_proposal",
            operation="register_proposal",
            at=proposal_at,
            key="closed-loop:proposal",
            authority=proposal_processing,
        ),
        proposal=proposal,
    )
    await _apply(
        pool,
        ProposalAppender(),
        "append_consequential",
        command=proposal_command,
        now=proposal_at,
    )
    proposal_duplicate = await _apply(
        pool,
        ProposalAppender(),
        "append_consequential",
        command=proposal_command,
        now=proposal_at,
    )
    assert proposal_duplicate.duplicate

    review_at = proposal_at + timedelta(minutes=1)
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
            purpose="closed_loop_intervention",
        ),
        reason="one bounded escalation is worthwhile and authorized",
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
                operation="review_proposal",
                at=review_at,
                key="closed-loop:proposal:accept",
            ),
            review=review,
        ),
        now=review_at,
    )

    prediction_at = review_at + timedelta(minutes=1)
    prediction = Prediction(
        prediction_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        kind=PredictionKind.INTERVENTION_EFFECT,
        target={"customer": "nimbus-bank", "outcome": "blocker_cleared"},
        probability_distribution={"cleared": 0.7, "not_cleared": 0.3},
        metric_definition=spec.outcome_metric,
        evidence_cutoff=prediction_at - timedelta(seconds=1),
        forecast_window_start=forecast_start,
        forecast_window_end=forecast_end,
        assumptions=("the owner can remove the identified blocker",),
        censoring_rule="measurement unavailable 24h after the outcome window",
        intervention_spec_digest=spec.spec_digest,
        comparator=spec.comparator,
        baseline={"clearance_probability": 0.35},
        preregistered_at=prediction_at,
    )
    await _apply(
        pool,
        PredictionWriter(),
        "register",
        command=PredictionRegistrationCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="PredictionWriter",
                responsibility="prediction",
                operation="register_prediction",
                at=prediction_at,
                key="closed-loop:prediction",
            ),
            prediction=prediction,
        ),
        now=prediction_at,
    )

    authorization_at = prediction_at + timedelta(minutes=1)
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
            purpose="closed_loop_intervention",
        ),
        exact_operations=frozenset({spec.operation}),
        exact_target_refs=frozenset(
            {
                f"referent:{action_target.referent_id}:"
                f"v{action_target.referent_version}"
            }
        ),
        exact_field_paths=frozenset(
            f"parameters.{key}" for key in spec.parameters
        ),
        constraints={"maximum_messages": 1},
        use_budget=1,
        attempt_budget=1,
        decided_at=authorization_at,
        expires_at=authorization_at + timedelta(hours=1),
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
                operation="authorize_intervention",
                at=authorization_at,
                key="closed-loop:authorization",
            ),
            decision=authorization,
        ),
        now=authorization_at,
    )

    agency = AgencyStateApplier()
    work_ledger = WorkLedgerApplier()
    if activation_worker is None:
        workflow_id = uuid7()
        task_id = uuid7()
        workflow_at = authorization_at + timedelta(minutes=1)
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
            completion_predicate=(
                "the required delivery task has a succeeded receipt"
            ),
            transition_reason="instantiate the authorized workflow",
            created_at=workflow_at,
            updated_at=workflow_at,
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
                    operation="create_workflow",
                    at=workflow_at,
                    key="closed-loop:workflow:create",
                ),
                expected_version=0,
                snapshot=workflow,
            ),
            now=workflow_at,
        )
        task_at = workflow_at
        task = TaskSnapshot(
            task_id=task_id,
            tenant_id=tenant_id,
            workflow_run_id=workflow_id,
            episode_id=episode_id,
            intervention_spec_digest=spec.spec_digest,
            task_kind="external_effect",
            state=TaskState.PLANNED,
            target_grounding_refs=(
                f"referent:{action_target.referent_id}:"
                f"v{action_target.referent_version}",
            ),
            authorization_decision_id=authorization.decision_id,
            external_effect_required=True,
            transition_reason="create the governed delivery task",
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
                    operation="task_planned",
                    at=task_at,
                    key="closed-loop:task:planned",
                ),
                expected_version=0,
                snapshot=task,
            ),
            now=task_at,
        )
    else:
        assert await activation_worker.process_batch(limit=10) == 1
        async with pool.acquire() as conn:
            activation = await conn.fetchrow(
                """
                SELECT workflow_run_id, task_id
                FROM authorized_agency_activation_work_items
                WHERE tenant_id=$1 AND authorization_decision_id=$2
                  AND status='activated'
                """,
                tenant_id,
                authorization.decision_id,
            )
            assert activation is not None
            workflow_id = activation["workflow_run_id"]
            task_id = activation["task_id"]
            workflow_payload = await conn.fetchval(
                """
                SELECT snapshot
                FROM agency_workflow_run_versions
                WHERE tenant_id=$1 AND workflow_run_id=$2
                  AND aggregate_version=1
                """,
                tenant_id,
                workflow_id,
            )
            task_payload = await conn.fetchval(
                """
                SELECT snapshot
                FROM agency_task_versions
                WHERE tenant_id=$1 AND task_id=$2 AND aggregate_version=1
                """,
                tenant_id,
                task_id,
            )
        workflow = WorkflowRunSnapshot.model_validate(workflow_payload)
        task = TaskSnapshot.model_validate(task_payload)
        assert workflow.state is WorkflowRunState.PLANNED
        assert task.state is TaskState.PLANNED
        workflow_at = workflow.created_at
        task_at = task.created_at

    if effect_executor is None:
        workflow_active_at = max(
            workflow_at + timedelta(minutes=1),
            authorization_at + timedelta(minutes=1),
        )
    else:
        workflow_active_at = max(
            workflow_at,
            authorization_at,
            datetime.now(started_at.tzinfo),
        )
    workflow = workflow.model_copy(
        update={
            "state": WorkflowRunState.ACTIVE,
            "transition_reason": "authorization and preconditions remain live",
            "updated_at": workflow_active_at,
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
                operation="activate_workflow",
                at=workflow_active_at,
                key="closed-loop:workflow:active",
            ),
            expected_version=1,
            snapshot=workflow,
        ),
        now=workflow_active_at,
    )
    task_transition_at = max(
        task_at + timedelta(minutes=1),
        workflow_active_at + timedelta(minutes=1),
    )
    for expected_version, state, offset in (
        (1, TaskState.READY, 0),
        (2, TaskState.IN_PROGRESS, 1),
    ):
        at = (
            task_transition_at + timedelta(minutes=offset)
            if effect_executor is None
            else max(task.updated_at, datetime.now(started_at.tzinfo))
        )
        task = task.model_copy(
            update={
                "state": state,
                "transition_reason": f"task enters {state.value}",
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
                    operation=f"task_{state.value}",
                    at=at,
                    key=f"closed-loop:task:{state.value}",
                ),
                expected_version=expected_version,
                snapshot=task,
            ),
            now=at,
        )

    work_at = (
        task_transition_at + timedelta(minutes=2)
        if effect_executor is None
        else max(task.updated_at, datetime.now(started_at.tzinfo))
    )
    obligation = WorkObligation(
        obligation_id=uuid7(),
        lineage_id=uuid7(),
        tenant_id=tenant_id,
        generation=1,
        semantic_dedupe_key=f"closed-loop-task:{task_id}",
        causal_parent_ref=f"task:{task_id}:v3",
        reason="the authorized in-progress task requires one external effect",
        target_object_type="task",
        target_object_id=task_id,
        owner_writer_id="AgencyStateApplier",
        purpose="execute_governed_escalation",
        risk_tier="high",
        expected_value=0.8,
        correctness_priority=0.95,
        intent_relevance=1.0,
        uncertainty_reduction_estimate=0.4,
        minimum_processing_class=ProcessingClass.R5_EXTERNAL_AGENCY,
        maximum_processing_class=ProcessingClass.R5_EXTERNAL_AGENCY,
        economic_envelope_ref="economic-envelope:closed-loop-v1",
        maximum_attempts=1,
        deadline=work_at + timedelta(hours=1),
        generation_depth=0,
        terminal_condition="one succeeded effect receipt or explicit no-effect fate",
        effect_possible=True,
        governing_criterion_refs=(criterion_ref,),
        attention_governance_binding_refs=(binding.binding_ref,),
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
                operation="register_work",
                at=work_at,
                key="closed-loop:work:register",
            ),
            obligation=obligation,
        ),
        now=work_at,
    )
    if work_scheduler is None:
        decision_at = work_at + timedelta(minutes=1)
        work_decision = WorkDecision(
            decision_id=uuid7(),
            tenant_id=tenant_id,
            obligation_id=obligation.obligation_id,
            obligation_generation=1,
            from_state=WorkObligationState.REGISTERED,
            to_state=WorkObligationState.ELIGIBLE,
            selected_processing_class=ProcessingClass.R5_EXTERNAL_AGENCY,
            policy_version_ref="work-policy:closed-loop-v1",
            why_no_cheaper_class_is_safe="the task may send an external message",
            reason="the accepted and authorized intervention is due",
            decided_at=decision_at,
        )
        await _apply(
            pool,
            work_ledger,
            "decide",
            command=WorkDecisionCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="WorkLedgerApplier",
                    responsibility="work_obligation",
                    operation="decide_work",
                    at=decision_at,
                    key="closed-loop:work:eligible",
                ),
                expected_version=1,
                decision=work_decision,
            ),
            now=decision_at,
        )
        lease_at = decision_at + timedelta(minutes=1)
        lease = LeaseToken(
            lease_token_id=uuid7(),
            tenant_id=tenant_id,
            obligation_id=obligation.obligation_id,
            obligation_generation=1,
            fence=1,
            attempt=1,
            owner_ref="worker:closed-loop-simulator",
            heartbeat_deadline=lease_at + timedelta(minutes=10),
            expires_at=lease_at + timedelta(minutes=30),
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
                    operation="grant_lease",
                    at=lease_at,
                    key="closed-loop:work:lease",
                ),
                expected_obligation_version=2,
                lease=lease,
            ),
            now=lease_at,
        )
    else:
        assert await work_scheduler.process_batch(limit=10) == 1
        async with pool.acquire() as conn:
            scheduling = await conn.fetchrow(
                """
                SELECT decision_id, lease_token_id
                FROM registered_work_scheduling_items
                WHERE tenant_id=$1 AND obligation_id=$2 AND status='leased'
                """,
                tenant_id,
                obligation.obligation_id,
            )
            assert scheduling is not None
            lease_payload = await conn.fetchval(
                """
                SELECT lease_payload
                FROM work_lease_token_versions
                WHERE tenant_id=$1 AND lease_token_id=$2
                  AND aggregate_version=1
                """,
                tenant_id,
                scheduling["lease_token_id"],
            )
        lease = LeaseToken.model_validate(lease_payload)
        lease_at = lease.granted_at

    if effect_executor is None:
        effect_at = lease_at + timedelta(minutes=1)
        attempt = ExternalEffectAttempt(
            effect_attempt_id=uuid7(),
            lineage_id=uuid7(),
            tenant_id=tenant_id,
            generation=1,
            episode_id=episode_id,
            task_id=task_id,
            intervention_spec_digest=spec.spec_digest,
            authorization_decision_id=authorization.decision_id,
            capability_id=capabilities.capability_id,
            capability_version=capabilities.capability_version,
            capability_digest=capabilities.capability_digest,
            operation=spec.operation,
            canonical_request_hash=canonical_sha256(spec.parameters),
            provider_idempotency_key=f"closed-loop:{episode_id}",
            target_grounding_refs=task.target_grounding_refs,
            live_precondition_refs=("simulated-slack-channel:exists",),
            work_obligation_id=obligation.obligation_id,
            work_obligation_generation=1,
            lease_token_id=lease.lease_token_id,
            lease_fence=1,
            dispatch_deadline=effect_at + timedelta(minutes=5),
            reconciliation_owner_ref="service:simulated-slack-reconciler",
            compensation_policy_ref="compensation:correction-only",
            reserved_at=effect_at,
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
                    operation="reserve_effect",
                    at=effect_at,
                    key="closed-loop:effect:reserve",
                ),
                attempt=attempt,
            ),
            now=effect_at,
        )
        effect_state = ExternalEffectState.RESERVED
        effect_version = 1
        succeeded_receipt_id: UUID | None = None
        for offset, target, provider_refs, external_refs in (
            (1, ExternalEffectState.DISPATCH_INTENT_RECORDED, (), ()),
            (
                2,
                ExternalEffectState.ACKNOWLEDGED,
                ("simulated-slack:accepted",),
                (),
            ),
            (
                3,
                ExternalEffectState.SUCCEEDED,
                ("simulated-slack:ok",),
                ("simulated-slack-message:1717.001",),
            ),
        ):
            observed_at = effect_at + timedelta(minutes=offset)
            observation = EffectObservation(
                receipt_id=uuid7(),
                tenant_id=tenant_id,
                effect_attempt_id=attempt.effect_attempt_id,
                from_state=effect_state,
                to_state=target,
                reason=f"simulated provider transition to {target.value}",
                provider_observation_refs=provider_refs,
                external_state_evidence_refs=external_refs,
                observed_at=observed_at,
            )
            transition_command = EffectTransitionCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="ExecutionLedgerApplier",
                    responsibility="external_effect",
                    operation=f"effect_{target.value}",
                    at=observed_at,
                    key=f"closed-loop:effect:{target.value}",
                ),
                expected_version=effect_version,
                observation=observation,
            )
            result = await _apply(
                pool,
                execution,
                "transition",
                command=transition_command,
                now=observed_at,
            )
            if target is ExternalEffectState.DISPATCH_INTENT_RECORDED:
                duplicate = await _apply(
                    pool,
                    execution,
                    "transition",
                    command=transition_command,
                    now=observed_at,
                )
                assert duplicate.duplicate
                assert duplicate.object_version == result.object_version
            if target is ExternalEffectState.SUCCEEDED:
                succeeded_receipt_id = observation.receipt_id
            effect_state = target
            effect_version += 1
        assert succeeded_receipt_id is not None
    else:
        assert await effect_executor.process_batch(limit=10) == 1
        async with pool.acquire() as conn:
            execution_work = await conn.fetchrow(
                """
                SELECT effect_attempt_id, status, applied_effect_version,
                       execution_receipt_id, applied_effect_state, outcome_at
                FROM leased_work_effect_execution_items
                WHERE tenant_id=$1 AND obligation_id=$2
                """,
                tenant_id,
                obligation.obligation_id,
            )
            assert execution_work is not None
            attempt_payload = await conn.fetchval(
                """
                SELECT attempt_payload
                FROM external_effect_attempt_versions
                WHERE tenant_id=$1 AND effect_attempt_id=$2
                  AND aggregate_version=1
                """,
                tenant_id,
                execution_work["effect_attempt_id"],
            )
        assert execution_work["status"] == "dispatched"
        assert execution_work["applied_effect_state"] == "succeeded"
        assert execution_work["execution_receipt_id"] is not None
        assert execution_work["applied_effect_version"] is not None
        assert execution_work["outcome_at"] is not None
        attempt = ExternalEffectAttempt.model_validate(attempt_payload)
        effect_at = attempt.reserved_at
        succeeded_receipt_id = execution_work["execution_receipt_id"]

    if effect_executor is None:
        work_complete_at = effect_at + timedelta(minutes=4)
        await _apply(
            pool,
            work_ledger,
            "resolve_lease",
            command=LeaseResolutionCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="WorkLedgerApplier",
                    responsibility="work_obligation",
                    operation="complete_work",
                    at=work_complete_at,
                    key="closed-loop:work:complete",
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
                    reason="the exact succeeded effect receipt is present",
                    resolved_at=work_complete_at,
                ),
            ),
            now=work_complete_at,
        )
    else:
        work_complete_at = execution_work["outcome_at"]
    task_complete_at = work_complete_at + timedelta(minutes=1)
    task = task.model_copy(
        update={
            "state": TaskState.COMPLETED,
            "effect_attempt_id": attempt.effect_attempt_id,
            "execution_receipt_id": succeeded_receipt_id,
            "completion_evidence_refs": (str(succeeded_receipt_id),),
            "transition_reason": "exact succeeded execution receipt",
            "updated_at": task_complete_at,
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
                operation="complete_task",
                at=task_complete_at,
                key="closed-loop:task:complete",
            ),
            expected_version=3,
            snapshot=task,
        ),
        now=task_complete_at,
    )
    workflow_complete_at = task_complete_at + timedelta(minutes=1)
    workflow = workflow.model_copy(
        update={
            "state": WorkflowRunState.COMPLETED,
            "completion_evidence_refs": (str(succeeded_receipt_id),),
            "transition_reason": "all required tasks have succeeded receipts",
            "updated_at": workflow_complete_at,
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
                operation="complete_workflow",
                at=workflow_complete_at,
                key="closed-loop:workflow:complete",
            ),
            expected_version=2,
            snapshot=workflow,
        ),
        now=workflow_complete_at,
    )

    # Task completion is not the outcome.  A distinct oracle observes reality.
    outcome_valid_at = forecast_start + timedelta(hours=1)
    outcome_observed_at = outcome_valid_at + timedelta(minutes=5)
    outcome = Outcome(
        outcome_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        metric_definition=prediction.metric_definition,
        observed_value={"cleared": True},
        observed_at=outcome_observed_at,
        valid_time=outcome_valid_at,
        source_evidence_refs=("simulation-oracle:jira:NBI-42:resolved",),
        independent_of_execution_claim=True,
        measurement_quality=0.95,
    )
    await _apply(
        pool,
        OutcomeRecorder(),
        "record",
        command=OutcomeRecordingCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="OutcomeRecorder",
                responsibility="outcome",
                operation="record_outcome",
                at=outcome_observed_at,
                key="closed-loop:outcome",
            ),
            outcome=outcome,
        ),
        now=outcome_observed_at,
    )
    settlement_at = outcome_observed_at + timedelta(minutes=1)
    settlement = Settlement(
        settlement_id=uuid7(),
        prediction_id=prediction.prediction_id,
        outcome_id=outcome.outcome_id,
        disposition=SettlementDisposition.SETTLED,
        settled_at=settlement_at,
        comparison_result={
            "predicted_clearance_probability": 0.7,
            "observed_cleared": True,
            "brier_loss": (1.0 - 0.7) ** 2,
        },
        reason_codes=("metric_comparable", "independent_jira_oracle"),
        residual_distribution={
            ResidualClass.MODEL: 0.25,
            ResidualClass.EXECUTION: 0.1,
            ResidualClass.CONFOUNDING: 0.65,
        },
    )
    await _apply(
        pool,
        SettlementApplier(),
        "apply",
        command=SettlementCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="SettlementApplier",
                responsibility="settlement",
                operation="settle_prediction",
                at=settlement_at,
                key="closed-loop:settlement",
            ),
            settlement=settlement,
        ),
        now=settlement_at,
    )
    attribution_at = settlement_at + timedelta(minutes=1)
    attribution = Attribution(
        attribution_id=uuid7(),
        episode_id=episode_id,
        subject_ref=f"intervention:{spec.spec_id}",
        attributed_effect_distribution={"unknown": 1.0},
        causal_confidence=0.2,
        method="single observational episode with substantial confounding",
        evidence_refs=(str(settlement.settlement_id),),
        withheld_credit=True,
        withholding_reason=(
            "one confounded episode may inform review but cannot train a policy"
        ),
    )
    await _apply(
        pool,
        AttributionApplier(),
        "apply",
        command=AttributionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AttributionApplier",
                responsibility="attribution",
                operation="apply_attribution",
                at=attribution_at,
                key="closed-loop:attribution",
            ),
            settlement_id=settlement.settlement_id,
            attribution=attribution,
        ),
        now=attribution_at,
    )

    resolved_at = attribution_at + timedelta(minutes=1)
    resolved_criterion = open_criterion.model_copy(
        update={"impact": CriterionImpact.SATISFIED}
    )
    await _apply(
        pool,
        concern_applier,
        "apply_evaluation",
        command=_concern_command(
            tenant_id=tenant_id,
            concern_id=concern_id,
            identity=identity,
            criterion=resolved_criterion,
            expected_version=2,
            state_estimate={
                "belief_model_refs": (f"belief:{model_id}",),
                "intent_refs": (intent_ref,),
                "outcome_refs": (f"outcome:{outcome.outcome_id}",),
                "status": "cleared",
            },
            at=resolved_at,
            key="closed-loop:concern:resolved",
        ),
        now=resolved_at,
    )

    final_at = resolved_at + timedelta(minutes=1)
    final_episode = initial_episode.model_copy(
        update={
            "intervention_spec_digest": spec.spec_digest,
            "stage_links": (
                *initial_episode.stage_links[:3],
                EpisodeStageLink(
                    stage="proposal",
                    fate=EpisodeStageFate.PRESENT,
                    object_ref=f"proposal:{proposal.proposal_id}",
                    writer_id="ProposalAppender",
                ),
                EpisodeStageLink(
                    stage="prediction",
                    fate=EpisodeStageFate.PRESENT,
                    object_ref=f"prediction:{prediction.prediction_id}",
                    writer_id="PredictionWriter",
                ),
                EpisodeStageLink(
                    stage="authorization",
                    fate=EpisodeStageFate.PRESENT,
                    object_ref=f"authorization:{authorization.decision_id}",
                    writer_id="AuthorizationApplier",
                ),
                EpisodeStageLink(
                    stage="workflow",
                    fate=EpisodeStageFate.PRESENT,
                    object_ref=f"workflow:{workflow_id}",
                    writer_id="AgencyStateApplier",
                ),
                EpisodeStageLink(
                    stage="task",
                    fate=EpisodeStageFate.PRESENT,
                    object_ref=f"task:{task_id}",
                    writer_id="AgencyStateApplier",
                ),
                EpisodeStageLink(
                    stage="work",
                    fate=EpisodeStageFate.PRESENT,
                    object_ref=f"work:{obligation.obligation_id}",
                    writer_id="WorkLedgerApplier",
                ),
                EpisodeStageLink(
                    stage="effect",
                    fate=EpisodeStageFate.PRESENT,
                    object_ref=f"effect:{attempt.effect_attempt_id}",
                    writer_id="ExecutionLedgerApplier",
                ),
                EpisodeStageLink(
                    stage="outcome",
                    fate=EpisodeStageFate.PRESENT,
                    object_ref=f"outcome:{outcome.outcome_id}",
                    writer_id="OutcomeRecorder",
                ),
                EpisodeStageLink(
                    stage="settlement",
                    fate=EpisodeStageFate.PRESENT,
                    object_ref=f"settlement:{settlement.settlement_id}",
                    writer_id="SettlementApplier",
                ),
                EpisodeStageLink(
                    stage="attribution",
                    fate=EpisodeStageFate.PRESENT,
                    object_ref=f"attribution:{attribution.attribution_id}",
                    writer_id="AttributionApplier",
                ),
            ),
            "updated_at": final_at,
        }
    )
    if finalize_episode_manifest:
        final_command = EpisodeUpdateCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="EpisodeCoordinator",
                responsibility="intervention_episode",
                operation="complete_episode",
                at=final_at,
                key="closed-loop:episode:complete",
            ),
            expected_version=1,
            episode=final_episode,
        )
        await _apply(
            pool,
            EpisodeCoordinator(),
            "apply",
            command=final_command,
            now=final_at,
        )
        duplicate_episode = await _apply(
            pool,
            EpisodeCoordinator(),
            "apply",
            command=final_command,
            now=final_at,
        )
        assert duplicate_episode.duplicate

    return ClosedLoopArtifacts(
        tenant_id=tenant_id,
        source_observation_id=source_observation_id,
        model_id=model_id,
        intent_proposal_id=intent_proposal.proposal_id,
        intent_command_result_id=intent_result.command_result_id,
        goal_id=goal_id,
        attention_binding_ref=binding.binding_ref,
        concern_id=concern_id,
        concern_version=3,
        episode_id=episode_id,
        proposal_id=proposal.proposal_id,
        prediction_id=prediction.prediction_id,
        authorization_id=authorization.decision_id,
        workflow_run_id=workflow_id,
        task_id=task_id,
        work_obligation_id=obligation.obligation_id,
        effect_attempt_id=attempt.effect_attempt_id,
        execution_receipt_id=succeeded_receipt_id,
        outcome_id=outcome.outcome_id,
        settlement_id=settlement.settlement_id,
        attribution_id=attribution.attribution_id,
        intervention_spec_digest=spec.spec_digest,
        completed_at=final_at,
    )


__all__ = ["ClosedLoopArtifacts", "run_closed_loop_vertical"]
