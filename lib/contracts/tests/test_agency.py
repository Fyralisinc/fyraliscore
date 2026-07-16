from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from lib.contracts.agency import (
    AttentionGovernanceBinding,
    AttentionSourceKind,
    Attribution,
    AuthorityBasisChange,
    AuthorityBasisChangeKind,
    AuthorityBasisSurvivalMode,
    AuthorityBasisSurvivalPolicy,
    AuthorizationDecision,
    AuthorizationDisposition,
    ConcernCriterionState,
    ConcernDisposition,
    ConcernEvaluationCommand,
    ConcernIdentity,
    ConcernSnapshot,
    ConcernState,
    ConcernTransition,
    ConstitutiveIntentAuthorityBasis,
    ConstitutiveIntentAuthorityBasisKind,
    CriterionImpact,
    CriterionWorkEligibility,
    EpisodeStageFate,
    EpisodeStageLink,
    ExactProposalAcceptance,
    IntentDependentFate,
    IntentMutation,
    IntentObjectKind,
    IntentOperation,
    IntentProposalFate,
    InterpretedIntentProposal,
    InterventionEpisode,
    InterventionSpec,
    Outcome,
    Prediction,
    PredictionKind,
    ResidualClass,
    Settlement,
    SettlementDisposition,
    TypedConstitutiveIntentCommand,
    compose_attention_governance_bindings,
    derive_concern_id,
    reduce_concern_state,
    reduce_intent_basis_change,
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


NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)


def _authority(*, tenant_id, processing: bool):
    cls = ProcessingAuthorityContext if processing else ConsumptionAuthorityContext
    return cls(
        tenant_id=tenant_id,
        principal_or_service_id="principal:ceo",
        purpose="intent_mutation",
        operation="create",
        object_types=RestrictionSet.only("goal", "commitment", "decision"),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("product:recommendation"),
        authority_basis_refs=frozenset({"grant:ceo"}),
        policy_version="authority-v1",
        authority_epoch=3,
        decision_time=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
    )


def _mutation(*, kind: IntentObjectKind = IntentObjectKind.GOAL) -> IntentMutation:
    return IntentMutation(
        object_kind=kind,
        operation=IntentOperation.CREATE,
        payload={"title": "Reduce onboarding time", "target_days": 3},
        schema_version="intent-mutation-v1",
        effective_at=NOW,
    )


def _survival(
    mode: AuthorityBasisSurvivalMode = AuthorityBasisSurvivalMode.REVIEW_REQUIRED,
) -> AuthorityBasisSurvivalPolicy:
    return AuthorityBasisSurvivalPolicy(
        policy_version="survival-v1",
        mode=mode,
        maximum_mode_permitted_by_operation=AuthorityBasisSurvivalMode.REVIEW_REQUIRED,
        maximum_mode_permitted_by_basis=AuthorityBasisSurvivalMode.POINT_IN_TIME_CONSTITUTIVE,
    )


def _referent(*, tenant_id) -> CanonicalReferent:
    return CanonicalReferent(
        tenant_id=tenant_id,
        referent_id="entity:atlas",
        referent_version=2,
        lifecycle_status=EntityLifecycleStatus.ACTIVE,
        predecessor_referent_refs=(),
        successor_referent_refs=(),
        birth_decision_ref="identity-decision:1",
        positive_existence_evidence_refs=("source:github/repo/atlas",),
    )


def _proposal(*, tenant_id, mutation: IntentMutation | None = None):
    mutation = mutation or _mutation()
    processing_authority = _authority(tenant_id=tenant_id, processing=True)
    partial = dict(
        proposal_id=uuid4(),
        tenant_id=tenant_id,
        proposal_version=1,
        normalized_mutation=mutation,
        normalized_payload_digest=mutation.payload_digest,
        source_assertion_refs=("assertion:1",),
        semantic_frame_refs=("frame:1",),
        uncertainty_reasons=("free_text_is_not_constitutive",),
        processing_authority=processing_authority,
        processing_authority_fingerprint=processing_authority.fingerprint,
        created_at=NOW,
        review_due_at=NOW + timedelta(days=2),
    )
    return InterpretedIntentProposal(**partial)


def test_intent_mutation_digest_is_stable_and_create_has_no_target():
    left = _mutation()
    right = _mutation()
    assert left.payload_digest == right.payload_digest
    with pytest.raises(ValidationError, match="create intent"):
        IntentMutation(
            object_kind=IntentObjectKind.GOAL,
            operation=IntentOperation.CREATE,
            target_aggregate_id=uuid4(),
            payload={"title": "x"},
            schema_version="v1",
            effective_at=NOW,
        )


def test_authority_basis_paths_are_mutually_exclusive():
    mutation = _mutation()
    explicit = ConstitutiveIntentAuthorityBasis(
        kind=ConstitutiveIntentAuthorityBasisKind.EXPLICIT_PRINCIPAL,
        basis_id="basis:1",
        principal_or_actor_id="actor:1",
        capability_or_grant_ref="grant:1",
        acknowledged_payload_digest=mutation.payload_digest,
        valid_from=NOW - timedelta(minutes=1),
        valid_until=NOW + timedelta(hours=1),
    )
    assert explicit.is_live(NOW)
    with pytest.raises(ValidationError, match="borrow"):
        explicit.model_copy(update={"source_contract_ref": "contract:1"})
        ConstitutiveIntentAuthorityBasis.model_validate(
            explicit.model_dump() | {"source_contract_ref": "contract:1"}
        )


def test_survival_policy_cannot_be_more_permissive_than_basis_or_operation():
    with pytest.raises(ValidationError, match="more permissive"):
        AuthorityBasisSurvivalPolicy(
            policy_version="v1",
            mode=AuthorityBasisSurvivalMode.POINT_IN_TIME_CONSTITUTIVE,
            maximum_mode_permitted_by_operation=AuthorityBasisSurvivalMode.REVIEW_REQUIRED,
            maximum_mode_permitted_by_basis=AuthorityBasisSurvivalMode.POINT_IN_TIME_CONSTITUTIVE,
        )


def test_typed_command_requires_exact_acknowledged_digest_and_live_authority():
    tenant_id = uuid4()
    mutation = _mutation()
    basis = ConstitutiveIntentAuthorityBasis(
        kind=ConstitutiveIntentAuthorityBasisKind.EXPLICIT_PRINCIPAL,
        basis_id="basis:1",
        principal_or_actor_id="actor:1",
        capability_or_grant_ref="grant:1",
        acknowledged_payload_digest=mutation.payload_digest,
        valid_from=NOW - timedelta(minutes=1),
        valid_until=NOW + timedelta(hours=1),
    )
    command = TypedConstitutiveIntentCommand(
        command_id=uuid4(),
        tenant_id=tenant_id,
        mutation=mutation,
        declared_payload_digest=mutation.payload_digest,
        authority_basis=basis,
        survival_policy=_survival(),
        processing_authority=_authority(tenant_id=tenant_id, processing=True),
        consumption_authority=_authority(tenant_id=tenant_id, processing=False),
        writer_scope_epoch=WriterScopeEpoch(
            scope_id="intent:tenant",
            tenant_id=tenant_id,
            semantic_responsibility="intent",
            source_partition="tenant",
            writer_owner="IntentApplier",
            epoch=1,
            state=WriterCutoverState.NEW_CANONICAL,
        ),
        idempotency_key="acceptance:1",
        exact_input_anchors=("ui:recommendation:1",),
        issued_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )
    assert command.request_digest == canonical_sha256(command.model_dump(mode="json"))
    with pytest.raises(ValidationError, match="declared intent digest"):
        TypedConstitutiveIntentCommand.model_validate(
            command.model_dump() | {"declared_payload_digest": "0" * 64}
        )


def test_free_text_proposal_requires_uncertainty_and_exact_acceptance():
    tenant_id = uuid4()
    proposal = _proposal(tenant_id=tenant_id)
    acceptance = ExactProposalAcceptance(
        acceptance_id=uuid4(),
        tenant_id=tenant_id,
        proposal_id=proposal.proposal_id,
        proposal_version=proposal.proposal_version,
        proposal_digest=canonical_sha256(proposal.model_dump(mode="json")),
        normalized_payload_digest=proposal.normalized_payload_digest,
        principal_id="actor:1",
        capability_ref="grant:ceo",
        authority=_authority(tenant_id=tenant_id, processing=False),
        accepted_at=NOW,
        expires_at=NOW + timedelta(minutes=10),
    )
    assert acceptance.accepts(proposal)
    edited = proposal.model_copy(
        update={
            "normalized_mutation": IntentMutation(
                object_kind=IntentObjectKind.GOAL,
                operation=IntentOperation.CREATE,
                payload={"title": "Different goal"},
                schema_version="intent-mutation-v1",
                effective_at=NOW,
            )
        }
    )
    assert not acceptance.accepts(edited)
    assert not proposal.fate.terminal
    assert IntentProposalFate.ACCEPTED_FOR_AUTHORIZATION.terminal


@pytest.mark.parametrize(
    ("kind", "mode", "expected"),
    [
        (
            AuthorityBasisChangeKind.RETROSPECTIVE_INVALIDITY,
            AuthorityBasisSurvivalMode.POINT_IN_TIME_CONSTITUTIVE,
            IntentDependentFate.RETROSPECTIVELY_CONTAMINATED,
        ),
        (
            AuthorityBasisChangeKind.PROSPECTIVE_REVOCATION,
            AuthorityBasisSurvivalMode.BASIS_CONTINGENT,
            IntentDependentFate.SUSPENDED_BASIS_ENDED,
        ),
        (
            AuthorityBasisChangeKind.PROSPECTIVE_EXPIRY,
            AuthorityBasisSurvivalMode.REVIEW_REQUIRED,
            IntentDependentFate.DISPUTED_PENDING_REVIEW,
        ),
    ],
)
def test_basis_change_reducer_is_total(kind, mode, expected):
    maximum = (
        AuthorityBasisSurvivalMode.POINT_IN_TIME_CONSTITUTIVE
        if mode is AuthorityBasisSurvivalMode.POINT_IN_TIME_CONSTITUTIVE
        else AuthorityBasisSurvivalMode.REVIEW_REQUIRED
    )
    policy = AuthorityBasisSurvivalPolicy(
        policy_version="v1",
        mode=mode,
        maximum_mode_permitted_by_operation=maximum,
        maximum_mode_permitted_by_basis=AuthorityBasisSurvivalMode.POINT_IN_TIME_CONSTITUTIVE,
    )
    change = AuthorityBasisChange(change_id=uuid4(), kind=kind, changed_at=NOW)
    assert reduce_intent_basis_change(policy=policy, change=change) is expected


def test_grounding_expiry_can_retain_only_same_revalidated_referent():
    policy = _survival()
    change = AuthorityBasisChange(
        change_id=uuid4(),
        kind=AuthorityBasisChangeKind.GROUNDING_EXPIRED_SAME_REFERENT,
        changed_at=NOW,
        replacement_basis_ref="admission:2",
        revalidated_same_referent=True,
    )
    assert reduce_intent_basis_change(
        policy=policy, change=change
    ) is IntentDependentFate.RETAINED_WITH_REVALIDATED_GROUNDING


def test_platform_obligation_binding_requires_nonwaivable_fields():
    with pytest.raises(ValidationError, match="nonwaivable"):
        AttentionGovernanceBinding(
            binding_id=uuid4(),
            binding_version=1,
            attention_source_ref="platform:safety",
            attention_source_kind=AttentionSourceKind.PLATFORM_OBLIGATION,
            work_budget_units=5,
            interruption_budget_count=0,
            interruption_budget_minutes=0,
            maximum_duration_seconds=3600,
            satisfaction_rule="all repair obligations terminal",
            expiry_rule="never while material",
            review_rule="daily",
            stop_rule="quiescent",
            valid_from=NOW,
            valid_until=NOW + timedelta(days=1),
        )


def _criterion(
    ref: str,
    *,
    impact: CriterionImpact,
    eligibility: CriterionWorkEligibility = CriterionWorkEligibility.ACTIONABLE,
    disposition: ConcernDisposition | None = None,
):
    return ConcernCriterionState(
        criterion_ref=ref,
        attention_source_ref=f"source:{ref}",
        attention_binding_ref=f"binding:{ref}",
        applicable=True,
        impact=impact,
        disposition=disposition,
        disposition_capability_ref="capability:1" if disposition else None,
        disposition_expires_at=NOW + timedelta(hours=1) if disposition else None,
        work_eligibility=eligibility,
    )


def test_concern_reducer_preserves_unknown_and_plural_material_gap():
    unknown = _criterion("a", impact=CriterionImpact.UNKNOWN)
    assert reduce_concern_state(
        criteria=(unknown,),
        at=NOW,
        gap_identity_valid=True,
        validity_deadline=None,
    ) is ConcernState.CANDIDATE

    accepted = _criterion(
        "a",
        impact=CriterionImpact.MATERIAL_GAP,
        disposition=ConcernDisposition.ACCEPTED_RISK,
    )
    untreated = _criterion("b", impact=CriterionImpact.MATERIAL_GAP)
    assert reduce_concern_state(
        criteria=(accepted, untreated),
        at=NOW,
        gap_identity_valid=True,
        validity_deadline=None,
    ) is ConcernState.OPEN


def test_concern_snapshot_dedupe_excludes_criterion_arrival_and_urgency():
    tenant_id = uuid4()
    identity = ConcernIdentity(
        tenant_id=tenant_id,
        affected_object_or_scope="customer:atlas",
        state_dimension_or_missing_proposition="renewal_risk",
        valid_time_window="2026-Q3",
        gap_identity_policy_version="gap-v1",
    )
    criterion = _criterion("goal:1", impact=CriterionImpact.MATERIAL_GAP)
    snapshot = ConcernSnapshot(
        concern_id=derive_concern_id(identity),
        aggregate_version=1,
        identity=identity,
        declared_dedupe_key=identity.dedupe_key,
        originating_attention_source_ref="source:goal:1",
        contributing_attention_source_refs=frozenset({"source:goal:1"}),
        criteria=(criterion,),
        current_state_estimate={"risk": 0.8},
        materiality=0.9,
        uncertainty=0.3,
        consequence=0.9,
        urgency=0.7,
        actionability=0.8,
        evidence_cutoff=NOW,
        state=ConcernState.OPEN,
        transition_cause="material gap detected",
    )
    assert snapshot.declared_dedupe_key == identity.dedupe_key
    assert snapshot.originating_attention_source_ref in snapshot.contributing_attention_source_refs


def _binding(
    *,
    source_ref: str,
    work_budget: float,
    interruption_count: int,
    priority_fields: frozenset[str],
) -> AttentionGovernanceBinding:
    return AttentionGovernanceBinding(
        binding_id=uuid4(),
        binding_version=1,
        attention_source_ref=source_ref,
        attention_source_kind=AttentionSourceKind.GOAL,
        work_budget_units=work_budget,
        interruption_budget_count=interruption_count,
        interruption_budget_minutes=float(interruption_count * 5),
        maximum_duration_seconds=3600,
        satisfaction_rule=f"{source_ref}:satisfied",
        expiry_rule=f"{source_ref}:expiry",
        review_rule=f"{source_ref}:review",
        stop_rule=f"{source_ref}:stop",
        permitted_priority_modifier_fields=priority_fields,
        disposition_capability_refs={
            ConcernDisposition.ACCEPTED_RISK: f"capability:{source_ref}"
        },
        nonwaivable_fields=frozenset({f"protected:{source_ref}"}),
        valid_from=NOW - timedelta(hours=1),
        valid_until=NOW + timedelta(hours=1),
    )


def test_attention_binding_composition_is_a_monotone_meet():
    left = _binding(
        source_ref="goal:1",
        work_budget=10,
        interruption_count=3,
        priority_fields=frozenset({"urgency", "order"}),
    )
    right = _binding(
        source_ref="commitment:1",
        work_budget=4,
        interruption_count=1,
        priority_fields=frozenset({"order"}),
    )
    envelope = compose_attention_governance_bindings((left, right), at=NOW)
    assert envelope.work_budget_units == 4
    assert envelope.interruption_budget_count == 1
    assert envelope.permitted_priority_modifier_fields == frozenset({"order"})
    assert envelope.nonwaivable_fields == frozenset(
        {"protected:goal:1", "protected:commitment:1"}
    )


def test_concern_command_requires_derived_id_and_candidate_first_version():
    tenant_id = uuid4()
    identity = ConcernIdentity(
        tenant_id=tenant_id,
        affected_object_or_scope="customer:atlas",
        state_dimension_or_missing_proposition="renewal_risk",
        valid_time_window="2026-Q3",
        gap_identity_policy_version="gap-v1",
    )
    authority = _authority(tenant_id=tenant_id, processing=True).model_copy(
        update={
            "purpose": "concern_evaluation",
            "operation": "evaluate",
            "object_types": RestrictionSet.only("concern"),
        }
    )
    consumption = _authority(tenant_id=tenant_id, processing=False).model_copy(
        update={
            "purpose": "concern_evaluation",
            "operation": "evaluate",
            "object_types": RestrictionSet.only("concern"),
        }
    )
    command = ConcernEvaluationCommand(
        command_id=uuid4(),
        tenant_id=tenant_id,
        concern_id=derive_concern_id(identity),
        expected_version=0,
        identity=identity,
        criteria=(_criterion("goal:1", impact=CriterionImpact.UNKNOWN),),
        current_state_estimate={"risk": "unknown"},
        materiality=0.8,
        uncertainty=0.9,
        consequence=0.8,
        urgency=0.5,
        actionability=0.3,
        evidence_cutoff=NOW,
        transition_cause="candidate detected",
        processing_authority=authority,
        consumption_authority=consumption,
        writer_scope_epoch=WriterScopeEpoch(
            scope_id=f"concern:{tenant_id}",
            tenant_id=tenant_id,
            semantic_responsibility="concern",
            source_partition=str(tenant_id),
            writer_owner="ConcernApplier",
            epoch=1,
            state=WriterCutoverState.NEW_CANONICAL,
        ),
        idempotency_key="candidate:atlas-renewal",
        issued_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )
    assert command.request_digest == canonical_sha256(command.model_dump(mode="json"))
    with pytest.raises(ValidationError, match="unevaluated candidate"):
        ConcernEvaluationCommand.model_validate(
            command.model_dump(mode="python")
            | {"criteria": (_criterion("goal:1", impact=CriterionImpact.MATERIAL_GAP),)}
        )


def test_invalidated_concern_is_absorbing():
    transition = ConcernTransition(
        concern_id=uuid4(),
        from_version=1,
        to_version=2,
        from_state=ConcernState.INVALIDATED,
        to_state=ConcernState.INVALIDATED,
        cause="duplicate replay",
        transitioned_at=NOW,
    )
    assert transition.to_state is ConcernState.INVALIDATED
    with pytest.raises(ValidationError, match="illegal"):
        ConcernTransition(
            concern_id=transition.concern_id,
            from_version=2,
            to_version=3,
            from_state=ConcernState.INVALIDATED,
            to_state=ConcernState.OPEN,
            cause="illegal reopen",
            transitioned_at=NOW,
        )


def test_prediction_kind_requires_causal_fields_only_when_causal():
    base = dict(
        prediction_id=uuid4(),
        tenant_id=uuid4(),
        episode_id=uuid4(),
        target={"metric": "cycle_time"},
        probability_distribution={"improves": 0.7, "not_improves": 0.3},
        metric_definition="median cycle time in days",
        evidence_cutoff=NOW,
        forecast_window_start=NOW + timedelta(hours=1),
        forecast_window_end=NOW + timedelta(days=7),
        censoring_rule="censor after window if source unavailable",
        preregistered_at=NOW,
    )
    with pytest.raises(ValidationError, match="spec, comparator, and baseline"):
        Prediction(kind=PredictionKind.INTERVENTION_EFFECT, **base)
    forecast = Prediction(kind=PredictionKind.STATE_FORECAST, **base)
    assert forecast.comparator is None


def test_intervention_spec_digest_changes_for_material_parameter():
    tenant_id = uuid4()
    base = dict(
        spec_id=uuid4(),
        tenant_id=tenant_id,
        episode_id=uuid4(),
        target_referent=_referent(tenant_id=tenant_id),
        target_version="v2",
        operation="change_oncall_schedule",
        parameters={"rotation_days": 7},
        comparator={"rotation_days": 14},
        outcome_metric="incident response minutes",
        outcome_window_start=NOW + timedelta(days=1),
        outcome_window_end=NOW + timedelta(days=30),
        action_adapter_version="pagerduty-v1",
        action_adapter_capability_digest="b" * 64,
        safety_and_preconditions=("no active sev0",),
        authority_requirement="ops-admin",
        reversible=True,
        compensation_declaration="restore prior schedule",
        grounding_dependency_refs=("grounding:1",),
        context_dependency_manifest_digest="c" * 64,
    )
    left = InterventionSpec(**base)
    right = InterventionSpec(**(base | {"parameters": {"rotation_days": 5}}))
    assert left.spec_digest != right.spec_digest


def test_authorization_binds_exact_spec_and_nonzero_attempt_budget():
    tenant_id = uuid4()
    with pytest.raises(ValidationError, match="nonzero"):
        AuthorizationDecision(
            decision_id=uuid4(),
            tenant_id=tenant_id,
            proposal_id=uuid4(),
            proposal_digest="a" * 64,
            intervention_spec_digest="b" * 64,
            disposition=AuthorizationDisposition.AUTHORIZED,
            principal_or_policy_ref="actor:1",
            authority=_authority(tenant_id=tenant_id, processing=False),
            exact_operations=frozenset({"send"}),
            exact_target_refs=frozenset({"channel:1"}),
            exact_field_paths=frozenset({"message"}),
            constraints={},
            use_budget=1,
            attempt_budget=0,
            decided_at=NOW,
            expires_at=NOW + timedelta(minutes=10),
        )


def test_outcome_settlement_and_attribution_preserve_uncertainty():
    outcome = Outcome(
        outcome_id=uuid4(),
        tenant_id=uuid4(),
        episode_id=uuid4(),
        metric_definition="retained customers",
        observed_value=8,
        observed_at=NOW,
        valid_time=NOW - timedelta(hours=1),
        source_evidence_refs=("crm:snapshot:1",),
        independent_of_execution_claim=True,
        measurement_quality=0.9,
    )
    settlement = Settlement(
        settlement_id=uuid4(),
        prediction_id=uuid4(),
        outcome_id=outcome.outcome_id,
        disposition=SettlementDisposition.SETTLED,
        settled_at=NOW,
        comparison_result={"predicted": 7, "observed": 8},
        reason_codes=("metric_comparable",),
        residual_distribution={
            ResidualClass.MODEL: 0.3,
            ResidualClass.EXTERNAL_SHOCK: 0.7,
        },
    )
    attribution = Attribution(
        attribution_id=uuid4(),
        episode_id=outcome.episode_id,
        subject_ref="intervention:1",
        attributed_effect_distribution={"positive": 0.4, "unknown": 0.6},
        causal_confidence=0.2,
        method="observational comparison",
        evidence_refs=(str(settlement.settlement_id),),
        withheld_credit=True,
        withholding_reason="confounding not identifiable",
    )
    assert attribution.withheld_credit


def test_episode_requires_typed_absence_not_silent_missing_stage():
    episode = InterventionEpisode(
        episode_id=uuid4(),
        tenant_id=uuid4(),
        stage_links=(
            EpisodeStageLink(
                stage="proposal",
                fate=EpisodeStageFate.PRESENT,
                object_ref="proposal:1",
                writer_id="ProposalAppender",
            ),
            EpisodeStageLink(
                stage="execution",
                fate=EpisodeStageFate.NOT_EXECUTED,
                reason="proposal rejected",
            ),
        ),
        created_at=NOW,
        updated_at=NOW,
    )
    assert len(episode.stage_links) == 2
