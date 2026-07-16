from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from lib.contracts.agency import (
    AgencyWriteContext,
    ConsequentialProposal,
    ConsequentialProposalRegistrationCommand,
    InterventionSpec,
    Outcome,
    OutcomeRecordingCommand,
    Prediction,
    PredictionKind,
    PredictionRegistrationCommand,
)
from lib.contracts.kernel import (
    ProcessingAuthorityContext,
    RestrictionSet,
    WriterCutoverState,
    WriterScopeEpoch,
)
from lib.contracts.perception import CanonicalReferent, EntityLifecycleStatus


NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)


def _authority(tenant_id: UUID, *, operation: str) -> ProcessingAuthorityContext:
    return ProcessingAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id="service:agency-test",
        purpose="consequential_agency",
        operation=operation,
        object_types=RestrictionSet.unrestricted(),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("test"),
        authority_basis_refs=frozenset({"grant:test"}),
        policy_version="test-v1",
        authority_epoch=1,
        decision_time=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(days=10),
    )


def _context(
    tenant_id: UUID,
    *,
    owner: str,
    responsibility: str,
    operation: str,
    authority: ProcessingAuthorityContext | None = None,
) -> AgencyWriteContext:
    return AgencyWriteContext(
        command_id=uuid4(),
        tenant_id=tenant_id,
        processing_authority=authority or _authority(tenant_id, operation=operation),
        writer_scope_epoch=WriterScopeEpoch(
            scope_id=f"{responsibility}:{tenant_id}",
            tenant_id=tenant_id,
            semantic_responsibility=responsibility,
            source_partition=str(tenant_id),
            writer_owner=owner,
            epoch=1,
            state=WriterCutoverState.NEW_CANONICAL,
        ),
        idempotency_key=f"{operation}:1",
        issued_at=NOW,
        expires_at=NOW + timedelta(minutes=10),
    )


def _spec(tenant_id: UUID, episode_id: UUID) -> InterventionSpec:
    return InterventionSpec(
        spec_id=uuid4(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        target_referent=CanonicalReferent(
            tenant_id=tenant_id,
            referent_id="customer:atlas",
            referent_version=1,
            lifecycle_status=EntityLifecycleStatus.ACTIVE,
            predecessor_referent_refs=(),
            successor_referent_refs=(),
            birth_decision_ref="identity:atlas",
            positive_existence_evidence_refs=("crm:atlas",),
        ),
        target_version="v1",
        operation="send_offer",
        parameters={"discount": 5},
        comparator={"action": "none"},
        outcome_metric="retained",
        outcome_window_start=NOW + timedelta(days=1),
        outcome_window_end=NOW + timedelta(days=10),
        action_adapter_version="crm-v1",
        action_adapter_capability_digest="a" * 64,
        safety_and_preconditions=("owner approval",),
        authority_requirement="retention-owner",
        reversible=True,
        compensation_declaration="withdraw draft",
        grounding_dependency_refs=("grounding:atlas",),
        context_dependency_manifest_digest="b" * 64,
    )


def test_prediction_rejects_post_cutoff_and_backdated_registration() -> None:
    tenant_id = uuid4()
    base = {
        "prediction_id": uuid4(),
        "tenant_id": tenant_id,
        "episode_id": uuid4(),
        "kind": PredictionKind.STATE_FORECAST,
        "target": {"metric": "retained"},
        "probability_distribution": {"yes": 0.6, "no": 0.4},
        "metric_definition": "retained",
        "evidence_cutoff": NOW + timedelta(minutes=1),
        "forecast_window_start": NOW + timedelta(days=1),
        "forecast_window_end": NOW + timedelta(days=2),
        "censoring_rule": "censor after day two",
        "preregistered_at": NOW,
    }
    with pytest.raises(ValidationError, match="cutoff"):
        Prediction(**base)
    prediction = Prediction(**(base | {"evidence_cutoff": NOW}))
    with pytest.raises(ValidationError, match="backdated"):
        PredictionRegistrationCommand(
            context=_context(
                tenant_id,
                owner="PredictionWriter",
                responsibility="prediction",
                operation="register_prediction",
            ).model_copy(update={"issued_at": NOW + timedelta(seconds=1)}),
            prediction=prediction,
        )


def test_proposal_command_requires_same_processing_authority_and_writer() -> None:
    tenant_id = uuid4()
    episode_id = uuid4()
    proposal_authority = _authority(tenant_id, operation="register_proposal")
    proposal = ConsequentialProposal(
        proposal_id=uuid4(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        intervention_spec=_spec(tenant_id, episode_id),
        summary="Send a bounded offer",
        rationale="Retention risk is material",
        alternative_refs=("no-action",),
        source_refs=("concern:1",),
        processing_authority=proposal_authority,
        processing_authority_fingerprint=proposal_authority.fingerprint,
        created_at=NOW,
        review_due_at=NOW + timedelta(days=1),
    )
    with pytest.raises(ValidationError, match="processing authority mismatch"):
        ConsequentialProposalRegistrationCommand(
            context=_context(
                tenant_id,
                owner="ProposalAppender",
                responsibility="consequential_proposal",
                operation="different_operation",
            ),
            proposal=proposal,
        )
    command = ConsequentialProposalRegistrationCommand(
        context=_context(
            tenant_id,
            owner="ProposalAppender",
            responsibility="consequential_proposal",
            operation="register_proposal",
            authority=proposal_authority,
        ),
        proposal=proposal,
    )
    assert len(command.request_digest) == 64


def test_canonical_outcome_command_rejects_task_completion_as_measurement() -> None:
    tenant_id = uuid4()
    outcome = Outcome(
        outcome_id=uuid4(),
        tenant_id=tenant_id,
        episode_id=uuid4(),
        metric_definition="customer retained",
        observed_value=True,
        observed_at=NOW,
        valid_time=NOW - timedelta(minutes=1),
        source_evidence_refs=("workflow:task-complete",),
        independent_of_execution_claim=False,
        measurement_quality=0.1,
    )
    with pytest.raises(ValidationError, match="independent measurement"):
        OutcomeRecordingCommand(
            context=_context(
                tenant_id,
                owner="OutcomeRecorder",
                responsibility="outcome",
                operation="record_outcome",
            ),
            outcome=outcome,
        )
