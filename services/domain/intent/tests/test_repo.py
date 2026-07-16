from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from lib.contracts.agency import (
    AuthorityBasisSurvivalMode,
    AuthorityBasisSurvivalPolicy,
    ConstitutiveIntentAuthorityBasis,
    ConstitutiveIntentAuthorityBasisKind,
    ExactProposalAcceptance,
    IntentMutation,
    IntentObjectKind,
    IntentOperation,
    InterpretedIntentProposal,
    TypedConstitutiveIntentCommand,
)
from lib.contracts.kernel import (
    ConsumptionAuthorityContext,
    ProcessingAuthorityContext,
    RestrictionSet,
    WriterCutoverState,
    WriterScopeEpoch,
    canonical_sha256,
)
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.intent.repo import (
    AppliedIntentMutation,
    IntentApplier,
    ProposalAppender,
)


NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)


def _authority(*, tenant_id: UUID, processing: bool):
    cls = ProcessingAuthorityContext if processing else ConsumptionAuthorityContext
    return cls(
        tenant_id=tenant_id,
        principal_or_service_id="actor:ceo",
        purpose="intent_mutation",
        operation="create",
        object_types=RestrictionSet.only("goal"),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("think", "product:recommendation"),
        authority_basis_refs=frozenset({"grant:leadership"}),
        policy_version="intent-authority-v1",
        authority_epoch=1,
        decision_time=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
    )


def _mutation() -> IntentMutation:
    return IntentMutation(
        object_kind=IntentObjectKind.GOAL,
        operation=IntentOperation.CREATE,
        payload={"title": "Reduce onboarding time", "altitude": "operational"},
        schema_version="intent-mutation-v1",
        effective_at=NOW,
    )


def _proposal(*, tenant_id: UUID, proposal_id: UUID | None = None):
    mutation = _mutation()
    processing_authority = _authority(tenant_id=tenant_id, processing=True)
    return InterpretedIntentProposal(
        proposal_id=proposal_id or uuid7(),
        tenant_id=tenant_id,
        proposal_version=1,
        normalized_mutation=mutation,
        normalized_payload_digest=mutation.payload_digest,
        source_assertion_refs=("observation:1",),
        semantic_frame_refs=("think-act-op:1",),
        uncertainty_reasons=("model_output_is_not_constitutive_intent",),
        processing_authority=processing_authority,
        processing_authority_fingerprint=processing_authority.fingerprint,
        created_at=NOW,
        review_due_at=NOW + timedelta(days=1),
    )


def _acceptance(*, tenant_id: UUID, proposal: InterpretedIntentProposal):
    return ExactProposalAcceptance(
        acceptance_id=uuid7(),
        tenant_id=tenant_id,
        proposal_id=proposal.proposal_id,
        proposal_version=proposal.proposal_version,
        proposal_digest=canonical_sha256(proposal.model_dump(mode="json")),
        normalized_payload_digest=proposal.normalized_payload_digest,
        principal_id="actor:ceo",
        capability_ref="grant:leadership",
        authority=_authority(tenant_id=tenant_id, processing=False),
        accepted_at=NOW,
        expires_at=NOW + timedelta(minutes=30),
    )


def _command(
    *,
    tenant_id: UUID,
    proposal: InterpretedIntentProposal,
    acceptance: ExactProposalAcceptance,
):
    basis = ConstitutiveIntentAuthorityBasis(
        kind=ConstitutiveIntentAuthorityBasisKind.EXPLICIT_PRINCIPAL,
        basis_id=f"acceptance:{acceptance.acceptance_id}",
        principal_or_actor_id=acceptance.principal_id,
        capability_or_grant_ref=acceptance.capability_ref,
        acknowledged_payload_digest=proposal.normalized_payload_digest,
        valid_from=NOW - timedelta(minutes=1),
        valid_until=NOW + timedelta(minutes=30),
    )
    return TypedConstitutiveIntentCommand(
        command_id=uuid7(),
        tenant_id=tenant_id,
        mutation=proposal.normalized_mutation,
        declared_payload_digest=proposal.normalized_payload_digest,
        authority_basis=basis,
        survival_policy=AuthorityBasisSurvivalPolicy(
            policy_version="intent-survival-v1",
            mode=AuthorityBasisSurvivalMode.REVIEW_REQUIRED,
            maximum_mode_permitted_by_operation=AuthorityBasisSurvivalMode.REVIEW_REQUIRED,
            maximum_mode_permitted_by_basis=AuthorityBasisSurvivalMode.POINT_IN_TIME_CONSTITUTIVE,
        ),
        processing_authority=_authority(tenant_id=tenant_id, processing=True),
        consumption_authority=_authority(tenant_id=tenant_id, processing=False),
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
        exact_input_anchors=(f"proposal:{proposal.proposal_id}:v1",),
        issued_at=NOW,
        expires_at=NOW + timedelta(minutes=10),
        proposal_acceptance_ref=str(acceptance.acceptance_id),
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_proposal_append_is_exactly_idempotent_and_has_initial_fate(
    fresh_db,
):
    tenant = uuid4()
    proposal = _proposal(tenant_id=tenant)
    appender = ProposalAppender()
    async with fresh_db.acquire() as conn, conn.transaction():
        first, inserted = await appender.append(
            conn=conn,
            proposal=proposal,
            semantic_idempotency_key="think:run-1:act-0",
            actor_or_service_ref="Think",
        )
        second, inserted_again = await appender.append(
            conn=conn,
            proposal=proposal,
            semantic_idempotency_key="think:run-1:act-0",
            actor_or_service_ref="Think",
        )
        fate_count = await conn.fetchval(
            "SELECT count(*) FROM intent_proposal_fate_events WHERE proposal_id = $1",
            proposal.proposal_id,
        )
    assert inserted is True
    assert inserted_again is False
    assert first == second == proposal
    assert fate_count == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_proposal_idempotency_key_rejects_different_content(
    fresh_db,
):
    tenant = uuid4()
    appender = ProposalAppender()
    first = _proposal(tenant_id=tenant)
    changed_mutation = first.normalized_mutation.model_copy(
        update={"payload": {"title": "Different goal"}}
    )
    changed = _proposal(tenant_id=tenant).model_copy(
        update={
            "normalized_mutation": changed_mutation,
            "normalized_payload_digest": changed_mutation.payload_digest,
        }
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        await appender.append(
            conn=conn,
            proposal=first,
            semantic_idempotency_key="same-key",
            actor_or_service_ref="Think",
        )
        with pytest.raises(InvariantViolation, match="different content"):
            await appender.append(
                conn=conn,
                proposal=changed,
                semantic_idempotency_key="same-key",
                actor_or_service_ref="Think",
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_exact_acceptance_and_intent_apply_commit_full_protocol_atomically(
    fresh_db,
):
    tenant = uuid4()
    proposal = _proposal(tenant_id=tenant)
    acceptance = _acceptance(tenant_id=tenant, proposal=proposal)
    command = _command(tenant_id=tenant, proposal=proposal, acceptance=acceptance)
    calls = 0

    async def apply_goal(conn, mutation):
        nonlocal calls
        calls += 1
        goal_id = uuid7()
        await conn.execute(
            """
            INSERT INTO goals (
                id, tenant_id, title, state, altitude, created_by_event_id
            ) VALUES ($1, $2, $3, 'active', $4, $5)
            """,
            goal_id,
            tenant,
            mutation.payload["title"],
            mutation.payload["altitude"],
            uuid4(),
        )
        return AppliedIntentMutation(
            aggregate_id=goal_id,
            result_kind="create_goal",
            result_payload={"goal_id": str(goal_id)},
        )

    async with fresh_db.acquire() as conn, conn.transaction():
        await ProposalAppender().append(
            conn=conn,
            proposal=proposal,
            semantic_idempotency_key="think:run-2:act-0",
            actor_or_service_ref="Think",
        )
        await ProposalAppender().accept_exact(conn=conn, acceptance=acceptance)
        result = await IntentApplier().apply(
            conn=conn,
            command=command,
            mutation_applier=apply_goal,
            now=NOW,
        )
        duplicate = await IntentApplier().apply(
            conn=conn,
            command=command,
            mutation_applier=apply_goal,
            now=NOW,
        )
        counts = {
            table: await conn.fetchval(f"SELECT count(*) FROM {table} WHERE tenant_id = $1", tenant)
            for table in (
                "goals",
                "intent_exact_acceptances",
                "intent_command_results",
                "intent_versions",
                "intent_canonical_events",
                "intent_outbox_records",
            )
        }
        proposal_fate = await conn.fetchval(
            "SELECT fate FROM intent_proposals WHERE tenant_id = $1 AND id = $2",
            tenant,
            proposal.proposal_id,
        )
    assert result.aggregate_version == 1
    assert duplicate.duplicate is True
    assert duplicate.command_result_id == result.command_result_id
    assert calls == 1
    assert counts == {
        "goals": 1,
        "intent_exact_acceptances": 1,
        "intent_command_results": 1,
        "intent_versions": 1,
        "intent_canonical_events": 1,
        "intent_outbox_records": 1,
    }
    assert proposal_fate == "accepted_for_authorization"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_apply_rejects_changed_idempotent_request_before_mutation(
    fresh_db,
):
    tenant = uuid4()
    proposal = _proposal(tenant_id=tenant)
    acceptance = _acceptance(tenant_id=tenant, proposal=proposal)
    command = _command(tenant_id=tenant, proposal=proposal, acceptance=acceptance)

    async def apply_goal(conn, mutation):
        return AppliedIntentMutation(
            aggregate_id=uuid7(), result_kind="create_goal", result_payload={}
        )

    async with fresh_db.acquire() as conn, conn.transaction():
        await ProposalAppender().append(
            conn=conn,
            proposal=proposal,
            semantic_idempotency_key="think:run-3:act-0",
            actor_or_service_ref="Think",
        )
        await ProposalAppender().accept_exact(conn=conn, acceptance=acceptance)
        await IntentApplier().apply(
            conn=conn, command=command, mutation_applier=apply_goal, now=NOW
        )
        changed = command.model_copy(update={"command_id": uuid7()})
        with pytest.raises(InvariantViolation, match="different request"):
            await IntentApplier().apply(
                conn=conn,
                command=changed,
                mutation_applier=apply_goal,
                now=NOW,
            )
