from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.contracts.agency import (
    AgencyWriteContext,
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
    TaskCommand,
    TaskSnapshot,
    TaskState,
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
)
from lib.contracts.perception import CanonicalReferent, EntityLifecycleStatus
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.agency_activation import (
    AgencyActivationRepo,
    AgencyActivationWorkStatus,
)
from services.domain.execution.repo import AgencyStateApplier
from services.domain.intent.repo import ProposalAppender
from services.domain.outcomes.repo import AuthorizationApplier, EpisodeCoordinator


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _processing_authority(*, tenant_id: UUID, operation: str, at: datetime):
    return ProcessingAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id=f"service:{operation}",
        purpose="agency_activation_test",
        operation=operation,
        object_types=RestrictionSet.unrestricted(),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("governed-proposal"),
        authority_basis_refs=frozenset({f"processing-grant:{operation}"}),
        policy_version="agency-activation-test-v1",
        authority_epoch=1,
        decision_time=at - timedelta(hours=1),
        expires_at=at + timedelta(days=2),
    )


def _consumption_authority(*, tenant_id: UUID, operation: str, at: datetime):
    return ConsumptionAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id="actor:operations-owner",
        purpose="agency_activation_test",
        operation=operation,
        object_types=RestrictionSet.unrestricted(),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("governed-proposal"),
        authority_basis_refs=frozenset({f"capability:{operation}"}),
        policy_version="agency-activation-test-v1",
        authority_epoch=1,
        decision_time=at - timedelta(hours=1),
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
    processing = authority or _processing_authority(
        tenant_id=tenant_id,
        operation=operation,
        at=at,
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


async def _authorization_fixture(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    start: datetime,
    expires_at: datetime,
    disposition: AuthorizationDisposition = AuthorizationDisposition.AUTHORIZED,
) -> tuple[AuthorizationDecision, ConsequentialProposal, UUID]:
    episode_id = uuid7()
    episode = InterventionEpisode(
        episode_id=episode_id,
        tenant_id=tenant_id,
        stage_links=(
            EpisodeStageLink(
                stage="authorization",
                fate=EpisodeStageFate.NOT_EXECUTED,
                reason="authorization has not yet been applied",
            ),
        ),
        created_at=start,
        updated_at=start,
    )
    await EpisodeCoordinator().apply(
        conn=conn,
        command=EpisodeUpdateCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="EpisodeCoordinator",
                responsibility="intervention_episode",
                operation="create_episode",
                at=start,
                key=f"activation-test:episode:{episode_id}",
            ),
            expected_version=0,
            episode=episode,
        ),
        now=start,
    )
    proposal_at = start + timedelta(minutes=1)
    proposal_authority = _processing_authority(
        tenant_id=tenant_id,
        operation="register_proposal",
        at=proposal_at,
    )
    target = CanonicalReferent(
        tenant_id=tenant_id,
        referent_id="customer:atlas",
        referent_version=4,
        lifecycle_status=EntityLifecycleStatus.ACTIVE,
        predecessor_referent_refs=(),
        successor_referent_refs=(),
        birth_decision_ref="identity:atlas:v4",
        positive_existence_evidence_refs=("crm:atlas",),
    )
    spec = InterventionSpec(
        spec_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        target_referent=target,
        target_version="crm-customer-v4",
        operation="offer_retention_package",
        parameters={"discount_percent": 8},
        comparator={"policy": "no_special_offer"},
        outcome_metric="customer_retained_at_renewal",
        outcome_window_start=start + timedelta(days=1),
        outcome_window_end=start + timedelta(days=14),
        workflow_spec_version_ref="workflow:retention:v3",
        action_adapter_version="crm-adapter-v2",
        action_adapter_capability_digest="a" * 64,
        safety_and_preconditions=("account owner confirms fit",),
        authority_requirement="capability:authorize_retention_offer",
        reversible=True,
        compensation_declaration="withdraw unaccepted draft offer",
        grounding_dependency_refs=("grounding:atlas:v4",),
        context_dependency_manifest_digest="b" * 64,
    )
    proposal = ConsequentialProposal(
        proposal_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        intervention_spec=spec,
        summary="Offer Atlas a bounded renewal package",
        rationale="Evidence indicates preventable renewal risk",
        alternative_refs=("alternative:no-action",),
        source_refs=("concern:atlas-renewal",),
        processing_authority=proposal_authority,
        processing_authority_fingerprint=proposal_authority.fingerprint,
        created_at=proposal_at,
        review_due_at=proposal_at + timedelta(days=1),
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
                key=f"activation-test:proposal:{proposal.proposal_id}",
                authority=proposal_authority,
            ),
            proposal=proposal,
        ),
        now=proposal_at,
    )
    review_at = start + timedelta(minutes=2)
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
        reason="bounded proposal is accepted for exact authorization",
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
                key=f"activation-test:review:{proposal.proposal_id}",
            ),
            review=review,
        ),
        now=review_at,
    )
    decision_at = start + timedelta(minutes=3)
    authorization = AuthorizationDecision(
        decision_id=uuid7(),
        tenant_id=tenant_id,
        proposal_id=proposal.proposal_id,
        proposal_digest=proposal.proposal_digest,
        intervention_spec_digest=spec.spec_digest,
        disposition=disposition,
        principal_or_policy_ref="actor:operations-owner",
        authority=_consumption_authority(
            tenant_id=tenant_id,
            operation="authorize_intervention",
            at=decision_at,
        ),
        exact_operations=frozenset({spec.operation}),
        exact_target_refs=frozenset({"referent:customer:atlas:v4"}),
        exact_field_paths=frozenset({"parameters.discount_percent"}),
        constraints={"maximum_discount_percent": 8},
        use_budget=1 if disposition is AuthorizationDisposition.AUTHORIZED else 0,
        attempt_budget=1 if disposition is AuthorizationDisposition.AUTHORIZED else 0,
        decided_at=decision_at,
        expires_at=expires_at,
    )
    result = await AuthorizationApplier().apply(
        conn=conn,
        command=AuthorizationDecisionCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AuthorizationApplier",
                responsibility="authorization",
                operation="authorize_intervention",
                at=decision_at,
                key=f"activation-test:authorization:{authorization.decision_id}",
            ),
            decision=authorization,
        ),
        now=decision_at,
    )
    return authorization, proposal, result.event_id


async def _install_planned_agency(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    context,
    at: datetime,
) -> None:
    agency = AgencyStateApplier()
    workflow = WorkflowRunSnapshot(
        workflow_run_id=context.plan.workflow_run_id,
        tenant_id=tenant_id,
        episode_id=context.plan.episode_id,
        intervention_spec_digest=context.plan.intervention_spec_digest,
        workflow_spec_version_ref=context.plan.workflow_spec_version_ref,
        state=WorkflowRunState.PLANNED,
        authorization_decision_id=context.plan.authorization_decision_id,
        authorization_decision_version=1,
        prerequisite_refs=("authorization:live",),
        required_task_ids=(context.plan.task_id,),
        completion_predicate="the required task reaches a terminal success fate",
        transition_reason="instantiate the exact authorized activation plan",
        created_at=at,
        updated_at=at,
    )
    await agency.apply_workflow_run(
        conn=conn,
        command=WorkflowRunCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="workflow_run",
                operation="create_workflow",
                at=at,
                key=f"activation-test:workflow:{context.plan.workflow_run_id}",
            ),
            expected_version=0,
            snapshot=workflow,
        ),
        now=at,
    )
    task = TaskSnapshot(
        task_id=context.plan.task_id,
        tenant_id=tenant_id,
        workflow_run_id=context.plan.workflow_run_id,
        episode_id=context.plan.episode_id,
        intervention_spec_digest=context.plan.intervention_spec_digest,
        task_kind=f"external_effect:{context.intervention_spec.operation}",
        state=TaskState.PLANNED,
        target_grounding_refs=(context.plan.exact_target_ref,),
        authorization_decision_id=context.plan.authorization_decision_id,
        authorization_decision_version=1,
        external_effect_required=True,
        transition_reason="instantiate the exact authorized task",
        created_at=at,
        updated_at=at,
    )
    await agency.apply_task(
        conn=conn,
        command=TaskCommand(
            context=_context(
                tenant_id=tenant_id,
                owner="AgencyStateApplier",
                responsibility="task",
                operation="create_task",
                at=at,
                key=f"activation-test:task:{context.plan.task_id}",
            ),
            expected_version=0,
            snapshot=task,
        ),
        now=at,
    )


async def test_authorized_event_becomes_one_exact_activated_plan(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = AgencyActivationRepo()
    tenant_id = uuid7()
    start = datetime.now(timezone.utc) - timedelta(minutes=10)
    async with fresh_db.acquire() as conn, conn.transaction():
        authorization, proposal, event_id = await _authorization_fixture(
            conn,
            tenant_id=tenant_id,
            start=start,
            expires_at=start + timedelta(hours=2),
        )
        discovery_at = start + timedelta(minutes=4)
        assert await repo.discover_ready_work(
            conn,
            now=discovery_at,
            limit=10,
            tenant_id=tenant_id,
        ) == 1
        item = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=discovery_at,
        )
        duplicate = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=discovery_at + timedelta(minutes=1),
        )
        assert item is not None and duplicate is not None
        assert duplicate.id == item.id
        assert item.plan.authorization_decision_id == authorization.decision_id
        assert item.plan.proposal_id == proposal.proposal_id
        assert item.plan.intervention_spec_digest == proposal.intervention_spec_digest
        assert item.plan.activation_at == discovery_at
        assert item.plan.exact_target_ref == "referent:customer:atlas:v4"
        assert item.plan.workflow_run_id != item.plan.task_id

        (claim,) = await repo.claim_ready_work(
            conn,
            worker_id="activation:test",
            now=discovery_at,
            lease_duration=timedelta(minutes=5),
            limit=1,
        )
        assert claim.claim_token is not None
        context = await repo.load_claimed_context(
            conn,
            tenant_id=tenant_id,
            work_item_id=claim.id,
            worker_id="activation:test",
            claim_token=claim.claim_token,
            now=discovery_at + timedelta(seconds=1),
        )
        await _install_planned_agency(
            conn,
            tenant_id=tenant_id,
            context=context,
            at=context.plan.activation_at,
        )
        activated = await repo.mark_activated(
            conn,
            tenant_id=tenant_id,
            work_item_id=claim.id,
            worker_id="activation:test",
            claim_token=claim.claim_token,
            workflow_version=1,
            task_version=1,
            now=discovery_at + timedelta(seconds=3),
        )
        assert activated.status is AgencyActivationWorkStatus.ACTIVATED
        assert activated.activated_workflow_version == 1
        assert activated.activated_task_version == 1
        assert await repo.claim_ready_work(
            conn,
            worker_id="activation:test",
            now=discovery_at + timedelta(minutes=20),
            lease_duration=timedelta(minutes=5),
            limit=1,
        ) == ()


async def test_activation_leases_fence_retries_and_expiry_fate(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = AgencyActivationRepo()
    tenant_id = uuid7()
    start = datetime.now(timezone.utc) - timedelta(hours=2)
    async with fresh_db.acquire() as conn, conn.transaction():
        _, _, event_id = await _authorization_fixture(
            conn,
            tenant_id=tenant_id,
            start=start,
            expires_at=start + timedelta(minutes=10),
        )
        discovered = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=start + timedelta(minutes=4),
        )
        assert discovered is not None
        (first,) = await repo.claim_ready_work(
            conn,
            worker_id="activation:a",
            now=start + timedelta(minutes=4),
            lease_duration=timedelta(seconds=5),
            limit=1,
        )
        assert first.claim_token is not None
        (reclaimed,) = await repo.claim_ready_work(
            conn,
            worker_id="activation:b",
            now=start + timedelta(minutes=5),
            lease_duration=timedelta(minutes=5),
            limit=1,
        )
        assert reclaimed.claim_token is not None
        assert reclaimed.claim_token != first.claim_token
        with pytest.raises(InvariantViolation, match="current live fence token"):
            await repo.schedule_retry(
                conn,
                tenant_id=tenant_id,
                work_item_id=first.id,
                worker_id="activation:a",
                claim_token=first.claim_token,
                now=start + timedelta(minutes=5, seconds=1),
                next_attempt_at=start + timedelta(minutes=6),
                failure_class="stale_worker",
                failure_reason="must not overwrite the recovered claim",
            )
        retry = await repo.schedule_retry(
            conn,
            tenant_id=tenant_id,
            work_item_id=reclaimed.id,
            worker_id="activation:b",
            claim_token=reclaimed.claim_token,
            now=start + timedelta(minutes=5, seconds=1),
            next_attempt_at=start + timedelta(minutes=6),
            failure_class="agency_cas",
            failure_reason="retry exact activation transaction",
        )
        assert retry.status is AgencyActivationWorkStatus.RETRY_SCHEDULED
        (final_claim,) = await repo.claim_ready_work(
            conn,
            worker_id="activation:c",
            now=start + timedelta(minutes=20),
            lease_duration=timedelta(minutes=5),
            limit=1,
        )
        assert final_claim.claim_token is not None
        expired = await repo.mark_authorization_expired(
            conn,
            tenant_id=tenant_id,
            work_item_id=final_claim.id,
            worker_id="activation:c",
            claim_token=final_claim.claim_token,
            now=start + timedelta(minutes=20, seconds=1),
            reason="the exact authorization expired before activation",
        )
        assert expired.status is AgencyActivationWorkStatus.AUTHORIZATION_EXPIRED
        assert expired.authorization_expired_at is not None

        terminal_start = start + timedelta(minutes=30)
        _, _, terminal_event_id = await _authorization_fixture(
            conn,
            tenant_id=tenant_id,
            start=terminal_start,
            expires_at=terminal_start + timedelta(hours=1),
        )
        terminal_item = await repo.discover_from_event(
            conn,
            source_event_id=terminal_event_id,
            now=terminal_start + timedelta(minutes=4),
        )
        assert terminal_item is not None
        (terminal_claim,) = await repo.claim_ready_work(
            conn,
            worker_id="activation:terminal",
            now=terminal_start + timedelta(minutes=4),
            lease_duration=timedelta(minutes=5),
            limit=1,
        )
        assert terminal_claim.claim_token is not None
        failed = await repo.fail_work_terminally(
            conn,
            tenant_id=tenant_id,
            work_item_id=terminal_claim.id,
            worker_id="activation:terminal",
            claim_token=terminal_claim.claim_token,
            now=terminal_start + timedelta(minutes=4, seconds=1),
            failure_class="invalid_workflow_template",
            failure_reason="template cannot implement the exact authorized spec",
        )
        assert failed.status is AgencyActivationWorkStatus.FAILED_TERMINAL
        assert failed.last_failure_class == "invalid_workflow_template"
