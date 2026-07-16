from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from lib.contracts import (
    AgencyWriteContext,
    EffectUncertainty,
    FailureClassification,
    FailureRecord,
    FailureRecordCommand,
    FailureState,
    OwnerTerminalizationRequest,
    ProcessingAuthorityContext,
    RestrictionSet,
    WorkObligationState,
    WriterCutoverState,
    WriterScopeEpoch,
    failure_transition_allowed,
)


TENANT = UUID("00000000-0000-4000-8000-000000000219")
NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)


def _context():
    return AgencyWriteContext(
        command_id=uuid4(),
        tenant_id=TENANT,
        processing_authority=ProcessingAuthorityContext(
            tenant_id=TENANT,
            principal_or_service_id="service:work-ledger",
            purpose="failure_governance",
            operation="apply_failure",
            object_types=RestrictionSet.unrestricted(),
            object_ids=RestrictionSet.unrestricted(),
            fields=RestrictionSet.unrestricted(),
            source_labels=RestrictionSet.unrestricted(),
            authority_basis_refs=frozenset({"platform:failure-governance"}),
            policy_version="failure-processing-v1",
            authority_epoch=1,
            decision_time=NOW - timedelta(minutes=1),
            expires_at=NOW + timedelta(hours=2),
        ),
        writer_scope_epoch=WriterScopeEpoch(
            scope_id=f"failure_record:{TENANT}",
            tenant_id=TENANT,
            semantic_responsibility="failure_record",
            source_partition=str(TENANT),
            writer_owner="WorkLedgerApplier",
            epoch=1,
            state=WriterCutoverState.NEW_CANONICAL,
        ),
        idempotency_key=f"failure:{uuid4()}",
        issued_at=NOW,
        expires_at=NOW + timedelta(hours=1),
    )


def _record(**updates):
    values = dict(
        failure_id=uuid4(),
        lineage_id=uuid4(),
        tenant_id=TENANT,
        generation=1,
        work_obligation_id=uuid4(),
        work_obligation_generation=1,
        causal_operation="compile_entity_grounding",
        classification=FailureClassification.POISON_INPUT,
        owner_writer_id="WorkLedgerApplier",
        semantic_owner_writer_id="GroundingAnnotationAppender",
        target_object_type="entity_candidate_request",
        target_object_id=uuid4(),
        original_semantic_idempotency_key="grounding-request:1",
        attempt=1,
        maximum_attempts=3,
        deadline=NOW + timedelta(days=1),
        next_action="classify and quarantine poison input",
        effect_uncertainty=EffectUncertainty.NONE,
        state=FailureState.DETECTED,
        reason="worker rejected malformed normalized arguments",
        created_at=NOW,
        updated_at=NOW,
    )
    values.update(updates)
    return FailureRecord(**values)


def test_failure_record_begins_detected_and_is_bound_to_work_writer():
    record = _record()
    command = FailureRecordCommand(
        context=_context(),
        expected_version=0,
        record=record,
    )

    assert len(command.request_digest) == 64
    assert failure_transition_allowed(None, FailureState.DETECTED)
    assert failure_transition_allowed(
        FailureState.DETECTED, FailureState.CLASSIFIED
    )
    assert not failure_transition_allowed(
        FailureState.EXHAUSTED, FailureState.CLASSIFIED
    )


def test_reconciliation_and_retry_require_explicit_evidence_or_wake_time():
    with pytest.raises(ValidationError, match="reconciliation requires"):
        _record(state=FailureState.RECONCILIATION_REQUIRED)

    with pytest.raises(ValidationError, match="next eligible"):
        _record(state=FailureState.RETRY_SCHEDULED)


def test_owner_terminalization_requires_a_legal_nonterminal_work_edge():
    request_values = dict(
        request_id=uuid4(),
        tenant_id=TENANT,
        failure_id=uuid4(),
        failure_generation=1,
        from_failure_state=FailureState.QUARANTINED,
        work_obligation_id=uuid4(),
        work_obligation_generation=1,
        from_work_state=WorkObligationState.QUARANTINED,
        semantic_owner_writer_id="GroundingAnnotationAppender",
        target_object_type="entity_candidate_request",
        target_object_id=uuid4(),
        acceptable_owner_terminal_states=frozenset({"terminal_rejected"}),
        terminal_reason="attempt budget exhausted on poison input",
        requested_at=NOW,
    )
    request = OwnerTerminalizationRequest(**request_values)
    assert len(request.request_digest) == 64

    with pytest.raises(ValidationError, match="cannot enter"):
        OwnerTerminalizationRequest(
            **{
                **request_values,
                "from_work_state": WorkObligationState.ELIGIBLE,
            }
        )
