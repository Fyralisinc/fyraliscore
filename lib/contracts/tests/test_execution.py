from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from lib.contracts import (
    ActionAdapterCapabilities,
    AdapterCapabilityRegistrationCommand,
    AgencyWriteContext,
    EffectObservation,
    EffectReservationCommand,
    ExternalEffectAttempt,
    ExternalEffectState,
    LeaseHeartbeat,
    LeaseResolution,
    LeaseState,
    LeaseTakeover,
    LeaseToken,
    ProcessingAuthorityContext,
    ProcessingClass,
    RestrictionSet,
    TaskSnapshot,
    TaskState,
    WorkflowRunCommand,
    WorkflowRunSnapshot,
    WorkflowRunState,
    WorkDecision,
    WorkObligation,
    WorkObligationState,
    WriterCutoverState,
    WriterScopeEpoch,
)


TENANT = UUID("00000000-0000-4000-8000-000000000211")
NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)


def _context(*, owner: str, responsibility: str, at: datetime = NOW):
    return AgencyWriteContext(
        command_id=uuid4(),
        tenant_id=TENANT,
        processing_authority=ProcessingAuthorityContext(
            tenant_id=TENANT,
            principal_or_service_id=f"service:{owner}",
            purpose="consequential_execution",
            operation="apply",
            object_types=RestrictionSet.unrestricted(),
            object_ids=RestrictionSet.unrestricted(),
            fields=RestrictionSet.unrestricted(),
            source_labels=RestrictionSet.unrestricted(),
            authority_basis_refs=frozenset({f"grant:{owner}"}),
            policy_version="execution-processing-v1",
            authority_epoch=1,
            decision_time=at - timedelta(minutes=1),
            expires_at=at + timedelta(hours=2),
        ),
        writer_scope_epoch=WriterScopeEpoch(
            scope_id=f"{responsibility}:tenant",
            tenant_id=TENANT,
            semantic_responsibility=responsibility,
            source_partition=str(TENANT),
            writer_owner=owner,
            epoch=1,
            state=WriterCutoverState.NEW_CANONICAL,
        ),
        idempotency_key=f"{responsibility}:{uuid4()}",
        issued_at=at,
        expires_at=at + timedelta(hours=1),
    )


def _workflow(*, state: WorkflowRunState = WorkflowRunState.PLANNED):
    return WorkflowRunSnapshot(
        workflow_run_id=uuid4(),
        tenant_id=TENANT,
        episode_id=uuid4(),
        intervention_spec_digest="a" * 64,
        workflow_spec_version_ref="workflow-spec:v1",
        state=state,
        authorization_decision_id=uuid4(),
        prerequisite_refs=("precondition:1",),
        completion_predicate="all required tasks completed from valid evidence",
        completion_evidence_refs=("task-set:complete",) if state is WorkflowRunState.COMPLETED else (),
        transition_reason="register workflow" if state is WorkflowRunState.PLANNED else "complete",
        created_at=NOW,
        updated_at=NOW,
    )


def _work(
    *,
    generation: int = 1,
    parent=None,
    minimum: ProcessingClass = ProcessingClass.R4_CONSEQUENTIAL_DECISION_SUPPORT,
    maximum: ProcessingClass = ProcessingClass.R5_EXTERNAL_AGENCY,
):
    return WorkObligation(
        obligation_id=uuid4(),
        lineage_id=uuid4(),
        tenant_id=TENANT,
        generation=generation,
        parent_obligation_id=parent,
        semantic_dedupe_key=f"effect:{uuid4()}",
        causal_parent_ref="task:1",
        reason="authorized external effect is due",
        target_object_type="task",
        target_object_id=uuid4(),
        owner_writer_id="AgencyStateApplier",
        purpose="execute_intervention",
        risk_tier="high",
        expected_value=0.7,
        correctness_priority=0.95,
        intent_relevance=1.0,
        uncertainty_reduction_estimate=0.4,
        minimum_processing_class=minimum,
        maximum_processing_class=maximum,
        economic_envelope_ref="economic-envelope:v1",
        maximum_attempts=3,
        deadline=NOW + timedelta(hours=1),
        generation_depth=generation - 1,
        terminal_condition="task has valid execution receipt or explicit failure fate",
        effect_possible=True,
        registered_at=NOW,
    )


def _capabilities(**updates):
    values = dict(
        capability_id=uuid4(),
        tenant_id=TENANT,
        capability_version="provider-v1",
        adapter_name="slack-delivery",
        provider_name="slack",
        permitted_operations=frozenset({"send_message"}),
        request_canonicalization_version="slack-message-v1",
        idempotency_supported=True,
        idempotency_scope="workspace/channel/provider-key",
        idempotency_retention_until=NOW + timedelta(days=2),
        reconciliation_supported=True,
        reconciliation_consistency_window_seconds=60,
        cancellation_supported=False,
        partial_effect_observable=True,
        compensation_supported=False,
        verified_at=NOW,
        expires_at=NOW + timedelta(days=1),
    )
    values.update(updates)
    return ActionAdapterCapabilities(**values)


def _attempt(*, generation: int = 1, prior=None):
    return ExternalEffectAttempt(
        effect_attempt_id=uuid4(),
        lineage_id=uuid4(),
        tenant_id=TENANT,
        generation=generation,
        prior_attempt_id=prior,
        episode_id=uuid4(),
        task_id=uuid4(),
        intervention_spec_digest="a" * 64,
        authorization_decision_id=uuid4(),
        capability_id=uuid4(),
        capability_version="provider-v1",
        capability_digest="b" * 64,
        operation="send_message",
        canonical_request_hash="c" * 64,
        provider_idempotency_key="provider-key:1",
        target_grounding_refs=("channel:C1",),
        live_precondition_refs=("channel:exists",),
        work_obligation_id=uuid4(),
        work_obligation_generation=1,
        lease_token_id=uuid4(),
        lease_fence=1,
        dispatch_deadline=NOW + timedelta(minutes=10),
        reconciliation_owner_ref="service:slack-reconciler",
        compensation_policy_ref="compensation:none",
        reserved_at=NOW,
    )


def test_workflow_creation_is_planned_and_writer_scoped() -> None:
    snapshot = _workflow()
    command = WorkflowRunCommand(
        context=_context(
            owner="AgencyStateApplier", responsibility="workflow_run"
        ),
        expected_version=0,
        snapshot=snapshot,
    )
    assert len(command.request_digest) == 64

    with pytest.raises(ValidationError, match="new workflow runs begin planned"):
        WorkflowRunCommand(
            context=command.context,
            expected_version=0,
            snapshot=_workflow(state=WorkflowRunState.COMPLETED),
        )


def test_external_effect_task_cannot_complete_without_receipt() -> None:
    with pytest.raises(ValidationError, match="requires attempt and receipt"):
        TaskSnapshot(
            task_id=uuid4(),
            tenant_id=TENANT,
            workflow_run_id=uuid4(),
            episode_id=uuid4(),
            intervention_spec_digest="a" * 64,
            task_kind="external_effect",
            state=TaskState.COMPLETED,
            target_grounding_refs=("channel:C1",),
            authorization_decision_id=uuid4(),
            external_effect_required=True,
            completion_evidence_refs=("worker:claimed-complete",),
            transition_reason="worker says done",
            created_at=NOW,
            updated_at=NOW,
        )


def test_work_generation_and_processing_envelope_are_explicit() -> None:
    assert _work().obligation_digest
    with pytest.raises(ValidationError, match="requires its exact parent"):
        _work(generation=2)
    with pytest.raises(ValidationError, match="range is inverted"):
        _work(
            minimum=ProcessingClass.R5_EXTERNAL_AGENCY,
            maximum=ProcessingClass.R4_CONSEQUENTIAL_DECISION_SUPPORT,
        )


def test_work_decision_requires_wake_and_terminal_fate_evidence() -> None:
    with pytest.raises(ValidationError, match="requires a wake"):
        WorkDecision(
            decision_id=uuid4(),
            tenant_id=TENANT,
            obligation_id=uuid4(),
            obligation_generation=1,
            from_state=WorkObligationState.REGISTERED,
            to_state=WorkObligationState.DEFERRED,
            selected_processing_class=ProcessingClass.R4_CONSEQUENTIAL_DECISION_SUPPORT,
            policy_version_ref="work-policy:v1",
            why_no_cheaper_class_is_safe="authorization-sensitive work",
            reason="capacity unavailable",
            decided_at=NOW,
        )
    with pytest.raises(ValidationError, match="requires UsefulSafeFate"):
        WorkDecision(
            decision_id=uuid4(),
            tenant_id=TENANT,
            obligation_id=uuid4(),
            obligation_generation=1,
            from_state=WorkObligationState.REGISTERED,
            to_state=WorkObligationState.SUPPRESSED,
            selected_processing_class=ProcessingClass.R4_CONSEQUENTIAL_DECISION_SUPPORT,
            policy_version_ref="work-policy:v1",
            why_no_cheaper_class_is_safe="risk cannot be lowered",
            reason="suppressed",
            decided_at=NOW,
        )


def test_possible_effect_never_returns_to_retry_without_reconciliation() -> None:
    values = dict(
        lease_token_id=uuid4(),
        tenant_id=TENANT,
        obligation_id=uuid4(),
        obligation_generation=1,
        fence=4,
        to_lease_state=LeaseState.EXPIRED,
        to_work_state=WorkObligationState.RETRY_WAIT,
        effect_may_have_occurred=True,
        next_eligible_at=NOW + timedelta(minutes=1),
        reason="lease expired after dispatch",
        resolved_at=NOW,
    )
    with pytest.raises(ValidationError, match="requires reconciliation"):
        LeaseResolution(**values)
    resolution = LeaseResolution(
        **{
            **values,
            "to_lease_state": LeaseState.RECONCILIATION_REQUIRED,
            "to_work_state": WorkObligationState.RECONCILIATION_REQUIRED,
            "next_eligible_at": None,
        }
    )
    assert resolution.to_work_state is WorkObligationState.RECONCILIATION_REQUIRED


def test_lease_heartbeat_and_takeover_are_strictly_fenced() -> None:
    lease_id = uuid4()
    obligation_id = uuid4()
    heartbeat_deadline = NOW + timedelta(minutes=2)
    expires_at = NOW + timedelta(minutes=10)
    heartbeat = LeaseHeartbeat(
        heartbeat_id=uuid4(),
        tenant_id=TENANT,
        lease_token_id=lease_id,
        obligation_id=obligation_id,
        obligation_generation=1,
        fence=4,
        owner_ref="worker:old",
        expected_heartbeat_deadline=heartbeat_deadline,
        extended_heartbeat_deadline=NOW + timedelta(minutes=4),
        lease_expires_at=expires_at,
        heartbeat_at=NOW + timedelta(minutes=1),
    )
    assert heartbeat.extended_heartbeat_deadline > heartbeat.expected_heartbeat_deadline
    with pytest.raises(ValidationError, match="before its current deadline"):
        LeaseHeartbeat.model_validate(
            {
                **heartbeat.model_dump(mode="json"),
                "heartbeat_at": heartbeat_deadline,
            }
        )

    takeover_at = heartbeat_deadline + timedelta(seconds=1)
    successor = LeaseToken(
        lease_token_id=uuid4(),
        tenant_id=TENANT,
        obligation_id=obligation_id,
        obligation_generation=1,
        fence=5,
        attempt=2,
        owner_ref="worker:new",
        heartbeat_deadline=takeover_at + timedelta(minutes=2),
        expires_at=takeover_at + timedelta(minutes=5),
        effect_possible=True,
        granted_at=takeover_at,
    )
    takeover = LeaseTakeover(
        takeover_id=uuid4(),
        tenant_id=TENANT,
        obligation_id=obligation_id,
        obligation_generation=1,
        predecessor_lease_token_id=lease_id,
        predecessor_fence=4,
        predecessor_attempt=1,
        predecessor_owner_ref="worker:old",
        predecessor_heartbeat_deadline=heartbeat_deadline,
        successor=successor,
        no_effect_evidence_refs=("effect-ledger:no-dispatch",),
        reason="old owner missed heartbeat",
        taken_over_at=takeover_at,
    )
    assert takeover.successor.fence == takeover.predecessor_fence + 1
    with pytest.raises(ValidationError, match="no-effect evidence"):
        LeaseTakeover.model_validate(
            {**takeover.model_dump(mode="json"), "no_effect_evidence_refs": []}
        )


def test_adapter_capabilities_cannot_claim_undefined_provider_guarantees() -> None:
    capabilities = _capabilities()
    assert capabilities.autonomous_repeat_safe
    command = AdapterCapabilityRegistrationCommand(
        context=_context(
            owner="ExecutionLedgerApplier",
            responsibility="action_adapter_capability",
        ),
        expected_version=0,
        capabilities=capabilities,
    )
    assert command.request_digest

    with pytest.raises(ValidationError, match="idempotency support requires"):
        _capabilities(idempotency_scope=None)
    with pytest.raises(ValidationError, match="reconciliation support requires"):
        _capabilities(reconciliation_consistency_window_seconds=None)


def test_effect_generations_and_writer_scope_are_fenced() -> None:
    attempt = _attempt()
    command = EffectReservationCommand(
        context=_context(
            owner="ExecutionLedgerApplier", responsibility="external_effect"
        ),
        attempt=attempt,
    )
    assert command.request_digest
    with pytest.raises(ValidationError, match="requires exact prior attempt"):
        _attempt(generation=2)


def test_effect_attempt_cannot_compensate_itself() -> None:
    attempt = _attempt()
    with pytest.raises(ValidationError, match="cannot compensate itself"):
        ExternalEffectAttempt(
            **{
                **attempt.model_dump(),
                "compensates_effect_attempt_id": attempt.effect_attempt_id,
            }
        )


def test_effect_material_state_requires_provider_or_external_evidence() -> None:
    with pytest.raises(ValidationError, match="requires provider or external evidence"):
        EffectObservation(
            receipt_id=uuid4(),
            tenant_id=TENANT,
            effect_attempt_id=uuid4(),
            from_state=ExternalEffectState.ACKNOWLEDGED,
            to_state=ExternalEffectState.SUCCEEDED,
            reason="claimed success",
            observed_at=NOW,
        )
    observation = EffectObservation(
        receipt_id=uuid4(),
        tenant_id=TENANT,
        effect_attempt_id=uuid4(),
        from_state=ExternalEffectState.ACKNOWLEDGED,
        to_state=ExternalEffectState.SUCCEEDED,
        reason="provider object observed",
        external_state_evidence_refs=("provider-object:123",),
        observed_at=NOW,
    )
    assert observation.to_state is ExternalEffectState.SUCCEEDED


def test_compensation_proposal_requires_exact_specification() -> None:
    with pytest.raises(
        ValidationError, match="compensation proposal requires exact intervention spec"
    ):
        EffectObservation(
            receipt_id=uuid4(),
            tenant_id=TENANT,
            effect_attempt_id=uuid4(),
            from_state=ExternalEffectState.PARTIALLY_EXECUTED,
            to_state=ExternalEffectState.COMPENSATION_PROPOSED,
            reason="compensation requires review",
            observed_at=NOW,
        )


@pytest.mark.parametrize(
    "terminal_fate",
    [
        ExternalEffectState.COMPENSATION_REJECTED,
        ExternalEffectState.COMPENSATION_EXPIRED,
    ],
)
def test_compensation_proposal_terminal_fate_requires_review_evidence(
    terminal_fate: ExternalEffectState,
) -> None:
    with pytest.raises(
        ValidationError,
        match="compensation proposal terminal fate requires exact review evidence",
    ):
        EffectObservation(
            receipt_id=uuid4(),
            tenant_id=TENANT,
            effect_attempt_id=uuid4(),
            from_state=ExternalEffectState.COMPENSATION_PROPOSED,
            to_state=terminal_fate,
            reason="proposal reached a terminal review fate",
            observed_at=NOW,
        )


@pytest.mark.parametrize(
    ("authorization_ref", "authorization_decision_id"),
    [
        (None, None),
        ("authorization-decision:missing-id", None),
        (None, uuid4()),
        ("authorization-decision:wrong", uuid4()),
    ],
)
def test_compensation_authorization_requires_exact_decision_identity(
    authorization_ref: str | None,
    authorization_decision_id: UUID | None,
) -> None:
    with pytest.raises(
        ValidationError, match="compensation authorization requires exact spec and decision"
    ):
        EffectObservation(
            receipt_id=uuid4(),
            tenant_id=TENANT,
            effect_attempt_id=uuid4(),
            from_state=ExternalEffectState.COMPENSATION_PROPOSED,
            to_state=ExternalEffectState.COMPENSATION_AUTHORIZED,
            reason="authorization recorded",
            compensation_intervention_spec_digest="d" * 64,
            compensation_authorization_ref=authorization_ref,
            compensation_authorization_decision_id=authorization_decision_id,
            observed_at=NOW,
        )
