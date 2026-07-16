from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from lib.contracts import (
    AgencyWriteContext,
    ConsumptionAuthorityContext,
    ControlPolicyState,
    ExperimentAssignment,
    ExperimentAssignmentArm,
    ExperimentEffectDirection,
    ExperimentPlan,
    LearnedArtifactStateTransitionCommand,
    LearnedArtifactStatus,
    LearningUpdate,
    PolicyPromotionDecision,
    PolicyPromotionDisposition,
    PolicyRegistryObjectKind,
    PolicyRegistryRegistrationCommand,
    PolicyStateTransitionCommand,
    ProcessingAuthorityContext,
    RestrictionSet,
    WriterCutoverState,
    WriterScopeEpoch,
)


TENANT = UUID("00000000-0000-4000-8000-000000000111")
NOW = datetime(2026, 7, 16, 9, 0, tzinfo=timezone.utc)


def _context(*, at: datetime = NOW) -> AgencyWriteContext:
    return AgencyWriteContext(
        command_id=uuid4(),
        tenant_id=TENANT,
        processing_authority=ProcessingAuthorityContext(
            tenant_id=TENANT,
            principal_or_service_id="service:policy-registry",
            purpose="governed_learning",
            operation="apply_control_policy",
            object_types=RestrictionSet.unrestricted(),
            object_ids=RestrictionSet.unrestricted(),
            fields=RestrictionSet.unrestricted(),
            source_labels=RestrictionSet.unrestricted(),
            authority_basis_refs=frozenset({"grant:policy-registry"}),
            policy_version="processing-v1",
            authority_epoch=1,
            decision_time=at - timedelta(minutes=5),
            expires_at=at + timedelta(hours=2),
        ),
        writer_scope_epoch=WriterScopeEpoch(
            scope_id="control-policy:tenant",
            tenant_id=TENANT,
            semantic_responsibility="control_policy",
            source_partition=str(TENANT),
            writer_owner="PolicyRegistryApplier",
            epoch=1,
            state=WriterCutoverState.NEW_CANONICAL,
        ),
        idempotency_key=f"policy:{uuid4()}",
        issued_at=at,
        expires_at=at + timedelta(hours=1),
    )


def _plan(*, preregistered_at: datetime = NOW) -> ExperimentPlan:
    return ExperimentPlan(
        plan_id=uuid4(),
        tenant_id=TENANT,
        adaptive_family="entity_grounding",
        hypothesis="candidate lowers entity-resolution loss",
        primary_metric_id="entity_resolution.brier_loss",
        effect_direction=ExperimentEffectDirection.LOWER_IS_BETTER,
        assignment_unit="conversation_episode",
        eligibility_rule="eligible Slack episode with independent labels",
        randomization_or_matching_rule="seeded tenant-local block randomization",
        control_policy_ref="control-policy:entity:v3",
        treatment_policy_ref="control-policy:entity:v4-candidate",
        interference_assumptions=("episodes do not share referent state",),
        authority_and_consent_ref="experiment-authority:v1",
        minimum_sample_size=40,
        stopping_rule="fixed horizon; no optional stopping",
        preregistered_at=preregistered_at,
        exposure_window_start=NOW + timedelta(days=1),
        exposure_window_end=NOW + timedelta(days=8),
    )


def _promotion(*, principal: str = "principal:risk-owner") -> PolicyPromotionDecision:
    authority = ConsumptionAuthorityContext(
        tenant_id=TENANT,
        principal_or_service_id="principal:risk-owner",
        purpose="governed_learning",
        operation="authorize_policy_promotion",
        object_types=RestrictionSet.only("control_policy"),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("independent-outcome"),
        authority_basis_refs=frozenset({"role:risk-owner"}),
        policy_version="promotion-v1",
        authority_epoch=2,
        decision_time=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(days=2),
    )
    return PolicyPromotionDecision(
        decision_id=uuid4(),
        tenant_id=TENANT,
        policy_id=uuid4(),
        candidate_digest="a" * 64,
        eligibility_measurement_id=uuid4(),
        eligibility_measurement_digest="b" * 64,
        disposition=PolicyPromotionDisposition.AUTHORIZED,
        governance_principal_ref=principal,
        authority=authority,
        authorized_canary_limit=0.05,
        authorized_exploration_cap=0.10,
        rollback_trigger="harm rate above 1 percent",
        reason="independent evidence meets the preregistered threshold",
        decided_at=NOW,
        expires_at=NOW + timedelta(days=1),
    )


def test_experiment_registration_time_cannot_be_backdated() -> None:
    with pytest.raises(ValidationError, match="preregistration cannot be backdated"):
        PolicyRegistryRegistrationCommand(
            context=_context(at=NOW + timedelta(minutes=1)),
            object_kind=PolicyRegistryObjectKind.EXPERIMENT_PLAN,
            object=_plan(preregistered_at=NOW),
        )


def test_assignment_must_precede_first_exposure() -> None:
    plan = _plan()
    with pytest.raises(ValidationError, match="precede first exposure"):
        ExperimentAssignment(
            assignment_id=uuid4(),
            tenant_id=TENANT,
            plan_id=plan.plan_id,
            plan_digest=plan.plan_digest,
            subject_ref="episode:slack-42",
            eligibility_evidence_refs=("eligibility:42",),
            arm=ExperimentAssignmentArm.TREATMENT,
            assignment_probability=0.5,
            randomization_nonce_digest="c" * 64,
            assigned_at=NOW + timedelta(days=2, seconds=1),
            first_exposure_at=NOW + timedelta(days=2),
            authority_and_consent_ref="experiment-authority:v1",
        )


def test_promotion_decision_cannot_borrow_another_principals_authority() -> None:
    with pytest.raises(ValidationError, match="does not own its authority"):
        _promotion(principal="service:learning-worker")


def test_learned_artifact_activation_requires_governance_and_legal_lifecycle() -> None:
    with pytest.raises(ValidationError, match="requires a promotion decision"):
        LearnedArtifactStateTransitionCommand(
            context=_context(),
            artifact_id="entity-calibrator",
            artifact_version="v4",
            expected_status_version=1,
            from_status=LearnedArtifactStatus.SHADOW,
            to_status=LearnedArtifactStatus.ACTIVE,
            reason="activate",
            transitioned_at=NOW,
        )
    with pytest.raises(ValidationError, match="illegal learned-artifact"):
        LearnedArtifactStateTransitionCommand(
            context=_context(),
            artifact_id="entity-calibrator",
            artifact_version="v4",
            expected_status_version=1,
            from_status=LearnedArtifactStatus.SHADOW,
            to_status=LearnedArtifactStatus.REPLACED,
            reason="skip lifecycle",
            transitioned_at=NOW,
        )


def test_authorized_policy_transition_binds_exact_decision() -> None:
    decision = _promotion()
    command = PolicyStateTransitionCommand(
        context=_context(),
        policy_id=decision.policy_id,
        expected_aggregate_version=3,
        candidate_digest=decision.candidate_digest,
        from_state=ControlPolicyState.ELIGIBLE,
        to_state=ControlPolicyState.AUTHORIZED,
        eligibility_measurement_id=decision.eligibility_measurement_id,
        promotion_decision=decision,
        source_transition_refs=("eligibility:measurement",),
        transitioned_at=NOW,
    )
    assert command.promotion_decision == decision


def test_policy_candidate_cannot_skip_shadow_and_evidence_lifecycle() -> None:
    with pytest.raises(ValidationError, match="illegal control-policy"):
        PolicyStateTransitionCommand(
            context=_context(),
            policy_id=uuid4(),
            expected_aggregate_version=1,
            candidate_digest="a" * 64,
            from_state=ControlPolicyState.CANDIDATE,
            to_state=ControlPolicyState.ACTIVE,
            source_transition_refs=("learner:self-promote",),
            transitioned_at=NOW,
        )


def test_corrected_learning_update_cannot_keep_old_reward() -> None:
    values = dict(
        update_id=uuid4(),
        tenant_id=TENANT,
        policy_id=uuid4(),
        candidate_digest="d" * 64,
        settlement_refs=("settlement:1",),
        attribution_refs=("attribution:1",),
        learned_artifact_ref="learned-artifact:entity:v4",
        training_procedure_ref="procedure:entity:v4",
        proposed_parameter_delta={"resolution_threshold": -0.02},
        created_at=NOW,
    )
    with pytest.raises(ValidationError, match="explicitly retracted"):
        LearningUpdate(**values, correction_epoch=1, reward_retracted=False)
    with pytest.raises(ValidationError, match="explicitly retracted"):
        LearningUpdate(**values, correction_epoch=0, reward_retracted=True)
