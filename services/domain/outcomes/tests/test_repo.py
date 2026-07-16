from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.contracts.agency import (
    AgencyWriteContext,
    Attribution,
    AttributionCommand,
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
    Outcome,
    OutcomeRecordingCommand,
    Prediction,
    PredictionKind,
    PredictionRegistrationCommand,
    ResidualClass,
    Settlement,
    SettlementCommand,
    SettlementDisposition,
)
from lib.contracts.kernel import (
    ConsumptionAuthorityContext,
    ProcessingAuthorityContext,
    RestrictionSet,
    WriterCutoverState,
    WriterScopeEpoch,
)
from lib.contracts.perception import CanonicalReferent, EntityLifecycleStatus
from lib.evaluation.agency import AgencyEvaluationScope, evaluate_agency_state
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.intent.repo import ProposalAppender
from services.domain.outcomes.repo import (
    AttributionApplier,
    AuthorizationApplier,
    EpisodeCoordinator,
    OutcomeRecorder,
    PredictionWriter,
    SettlementApplier,
)


START = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)
WINDOW_START = START + timedelta(days=1)
WINDOW_END = START + timedelta(days=14)
OUTCOME_VALID = WINDOW_START + timedelta(days=2)
OUTCOME_OBSERVED = OUTCOME_VALID + timedelta(hours=1)


def _processing_authority(*, tenant_id: UUID, operation: str, at: datetime):
    return ProcessingAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id=f"service:{operation}",
        purpose="consequential_agency",
        operation=operation,
        object_types=RestrictionSet.unrestricted(),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("simulated-independent-source"),
        authority_basis_refs=frozenset({f"processing-grant:{operation}"}),
        policy_version="agency-processing-v1",
        authority_epoch=1,
        decision_time=at - timedelta(hours=1),
        expires_at=at + timedelta(days=60),
    )


def _consumption_authority(*, tenant_id: UUID, operation: str, at: datetime):
    return ConsumptionAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id="actor:operations-owner",
        purpose="consequential_agency",
        operation=operation,
        object_types=RestrictionSet.unrestricted(),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("governed-proposal"),
        authority_basis_refs=frozenset({f"capability:{operation}"}),
        policy_version="agency-consumption-v1",
        authority_epoch=3,
        decision_time=at - timedelta(hours=1),
        expires_at=at + timedelta(days=60),
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
        tenant_id=tenant_id, operation=operation, at=at
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


def _referent(tenant_id: UUID) -> CanonicalReferent:
    return CanonicalReferent(
        tenant_id=tenant_id,
        referent_id="customer:atlas",
        referent_version=4,
        lifecycle_status=EntityLifecycleStatus.ACTIVE,
        predecessor_referent_refs=(),
        successor_referent_refs=(),
        birth_decision_ref="identity:atlas:v4",
        positive_existence_evidence_refs=("crm:atlas",),
    )


def _episode_command(
    *,
    tenant_id: UUID,
    episode: InterventionEpisode,
    expected_version: int,
    at: datetime,
    key: str,
) -> EpisodeUpdateCommand:
    return EpisodeUpdateCommand(
        context=_context(
            tenant_id=tenant_id,
            owner="EpisodeCoordinator",
            responsibility="intervention_episode",
            operation="update_episode",
            at=at,
            key=key,
        ),
        expected_version=expected_version,
        episode=episode,
    )


def _proposal_and_command(
    *, tenant_id: UUID, episode_id: UUID
) -> tuple[ConsequentialProposal, ConsequentialProposalRegistrationCommand]:
    created_at = START + timedelta(minutes=1)
    proposal_authority = _processing_authority(
        tenant_id=tenant_id,
        operation="register_proposal",
        at=created_at,
    )
    spec = InterventionSpec(
        spec_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        target_referent=_referent(tenant_id),
        target_version="crm-customer-v4",
        operation="offer_retention_package",
        parameters={"discount_percent": 8, "term_months": 12},
        comparator={"policy": "no_special_offer"},
        outcome_metric="customer_retained_at_renewal",
        outcome_window_start=WINDOW_START,
        outcome_window_end=WINDOW_END,
        workflow_spec_version_ref="workflow:retention:v3",
        action_adapter_version="crm-adapter-v2",
        action_adapter_capability_digest="a" * 64,
        safety_and_preconditions=("account owner confirms commercial fit",),
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
        rationale="Paired evidence indicates preventable renewal risk",
        alternative_refs=("alternative:no-action", "alternative:executive-call"),
        source_refs=("concern:atlas-renewal", "evidence:crm-risk"),
        processing_authority=proposal_authority,
        processing_authority_fingerprint=proposal_authority.fingerprint,
        created_at=created_at,
        review_due_at=created_at + timedelta(days=1),
    )
    command = ConsequentialProposalRegistrationCommand(
        context=_context(
            tenant_id=tenant_id,
            owner="ProposalAppender",
            responsibility="consequential_proposal",
            operation="register_proposal",
            at=created_at,
            key="proposal:atlas-retention",
            authority=proposal_authority,
        ),
        proposal=proposal,
    )
    return proposal, command


@pytest.mark.integration
@pytest.mark.asyncio
async def test_consequential_episode_is_exact_immutable_and_reconstructable(fresh_db):
    tenant_id = uuid4()
    episode_id = uuid7()
    initial_episode = InterventionEpisode(
        episode_id=episode_id,
        tenant_id=tenant_id,
        stage_links=(
            EpisodeStageLink(
                stage="proposal",
                fate=EpisodeStageFate.NOT_EXECUTED,
                reason="proposal has not yet been registered",
            ),
        ),
        created_at=START,
        updated_at=START,
    )
    initial_episode_command = _episode_command(
        tenant_id=tenant_id,
        episode=initial_episode,
        expected_version=0,
        at=START,
        key="episode:atlas:create",
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        episode_create = await EpisodeCoordinator().apply(
            conn=conn, command=initial_episode_command, now=START
        )

    proposal, proposal_command = _proposal_and_command(
        tenant_id=tenant_id, episode_id=episode_id
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        proposal_result = await ProposalAppender().append_consequential(
            conn=conn,
            command=proposal_command,
            now=proposal.created_at,
        )
    async with fresh_db.acquire() as conn, conn.transaction():
        duplicate_proposal = await ProposalAppender().append_consequential(
            conn=conn,
            command=proposal_command,
            now=proposal.created_at,
        )

    review_at = START + timedelta(minutes=2)
    review = ConsequentialProposalReview(
        review_id=uuid7(),
        tenant_id=tenant_id,
        proposal_id=proposal.proposal_id,
        proposal_version=proposal.proposal_version,
        proposal_digest=proposal.proposal_digest,
        intervention_spec_digest=proposal.intervention_spec_digest,
        from_fate=ConsequentialProposalFate.OPEN,
        to_fate=ConsequentialProposalFate.ACCEPTED_FOR_AUTHORIZATION,
        principal_or_policy_ref="actor:operations-owner",
        authority=_consumption_authority(
            tenant_id=tenant_id, operation="review_proposal", at=review_at
        ),
        reason="bounded offer is worthwhile and within authority",
        decided_at=review_at,
    )
    review_command = ConsequentialProposalReviewCommand(
        context=_context(
            tenant_id=tenant_id,
            owner="ProposalAppender",
            responsibility="consequential_proposal",
            operation="review_proposal",
            at=review_at,
            key="proposal:atlas-retention:accept",
        ),
        review=review,
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        await ProposalAppender().review_consequential(
            conn=conn, command=review_command, now=review_at
        )

    prediction_at = START + timedelta(minutes=3)
    prediction = Prediction(
        prediction_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        kind=PredictionKind.INTERVENTION_EFFECT,
        target={"referent": "customer:atlas", "outcome": "retained"},
        probability_distribution={"retained": 0.72, "not_retained": 0.28},
        metric_definition=proposal.intervention_spec.outcome_metric,
        evidence_cutoff=prediction_at - timedelta(minutes=1),
        forecast_window_start=WINDOW_START,
        forecast_window_end=WINDOW_END,
        assumptions=("no acquisition of Atlas before renewal",),
        censoring_rule="measurement unavailable 24h after renewal close",
        intervention_spec_digest=proposal.intervention_spec_digest,
        comparator=proposal.intervention_spec.comparator,
        baseline={"retention_probability": 0.45},
        preregistered_at=prediction_at,
    )
    prediction_command = PredictionRegistrationCommand(
        context=_context(
            tenant_id=tenant_id,
            owner="PredictionWriter",
            responsibility="prediction",
            operation="register_prediction",
            at=prediction_at,
            key="prediction:atlas-retention",
        ),
        prediction=prediction,
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        await PredictionWriter().register(
            conn=conn, command=prediction_command, now=prediction_at
        )

    authorization_at = START + timedelta(minutes=4)
    decision = AuthorizationDecision(
        decision_id=uuid7(),
        tenant_id=tenant_id,
        proposal_id=proposal.proposal_id,
        proposal_digest=proposal.proposal_digest,
        intervention_spec_digest=proposal.intervention_spec_digest,
        disposition=AuthorizationDisposition.AUTHORIZED,
        principal_or_policy_ref="actor:operations-owner",
        authority=_consumption_authority(
            tenant_id=tenant_id,
            operation="authorize_intervention",
            at=authorization_at,
        ),
        exact_operations=frozenset({proposal.intervention_spec.operation}),
        exact_target_refs=frozenset({"referent:customer:atlas:v4"}),
        exact_field_paths=frozenset(
            {"parameters.discount_percent", "parameters.term_months"}
        ),
        constraints={"maximum_discount_percent": 8},
        use_budget=1,
        attempt_budget=1,
        decided_at=authorization_at,
        expires_at=authorization_at + timedelta(days=7),
    )
    authorization_command = AuthorizationDecisionCommand(
        context=_context(
            tenant_id=tenant_id,
            owner="AuthorizationApplier",
            responsibility="authorization",
            operation="authorize_intervention",
            at=authorization_at,
            key="authorization:atlas-retention",
        ),
        decision=decision,
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        await AuthorizationApplier().apply(
            conn=conn, command=authorization_command, now=authorization_at
        )

    outcome = Outcome(
        outcome_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        metric_definition=prediction.metric_definition,
        observed_value={"retained": True},
        observed_at=OUTCOME_OBSERVED,
        valid_time=OUTCOME_VALID,
        source_evidence_refs=("crm:renewal-contract:atlas",),
        independent_of_execution_claim=True,
        measurement_quality=0.97,
    )
    outcome_command = OutcomeRecordingCommand(
        context=_context(
            tenant_id=tenant_id,
            owner="OutcomeRecorder",
            responsibility="outcome",
            operation="record_outcome",
            at=OUTCOME_OBSERVED,
            key="outcome:atlas-renewal",
        ),
        outcome=outcome,
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        await OutcomeRecorder().record(
            conn=conn, command=outcome_command, now=OUTCOME_OBSERVED
        )

    settlement_at = OUTCOME_OBSERVED + timedelta(minutes=1)
    settlement = Settlement(
        settlement_id=uuid7(),
        prediction_id=prediction.prediction_id,
        outcome_id=outcome.outcome_id,
        disposition=SettlementDisposition.SETTLED,
        settled_at=settlement_at,
        comparison_result={
            "predicted_retention_probability": 0.72,
            "observed_retained": True,
            "brier_loss": (1.0 - 0.72) ** 2,
        },
        reason_codes=("metric_comparable", "independent_crm_measurement"),
        residual_distribution={
            ResidualClass.MODEL: 0.45,
            ResidualClass.EXTERNAL_SHOCK: 0.15,
            ResidualClass.CONFOUNDING: 0.4,
        },
    )
    settlement_command = SettlementCommand(
        context=_context(
            tenant_id=tenant_id,
            owner="SettlementApplier",
            responsibility="settlement",
            operation="settle_prediction",
            at=settlement_at,
            key="settlement:atlas-retention",
        ),
        settlement=settlement,
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        await SettlementApplier().apply(
            conn=conn, command=settlement_command, now=settlement_at
        )

    attribution_at = settlement_at + timedelta(minutes=1)
    attribution = Attribution(
        attribution_id=uuid7(),
        episode_id=episode_id,
        subject_ref=f"intervention:{proposal.intervention_spec.spec_id}",
        attributed_effect_distribution={"positive": 0.55, "unknown": 0.45},
        causal_confidence=0.42,
        method="observational comparator with residual uncertainty",
        evidence_refs=(str(settlement.settlement_id),),
        withheld_credit=False,
    )
    attribution_command = AttributionCommand(
        context=_context(
            tenant_id=tenant_id,
            owner="AttributionApplier",
            responsibility="attribution",
            operation="apply_attribution",
            at=attribution_at,
            key="attribution:atlas-retention",
        ),
        settlement_id=settlement.settlement_id,
        attribution=attribution,
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        await AttributionApplier().apply(
            conn=conn, command=attribution_command, now=attribution_at
        )

    episode_final_at = attribution_at + timedelta(minutes=1)
    final_episode = initial_episode.model_copy(
        update={
            "intervention_spec_digest": proposal.intervention_spec_digest,
            "stage_links": (
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
                    object_ref=f"authorization:{decision.decision_id}",
                    writer_id="AuthorizationApplier",
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
            "updated_at": episode_final_at,
        }
    )
    final_episode_command = _episode_command(
        tenant_id=tenant_id,
        episode=final_episode,
        expected_version=1,
        at=episode_final_at,
        key="episode:atlas:complete-manifest",
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        episode_final = await EpisodeCoordinator().apply(
            conn=conn, command=final_episode_command, now=episode_final_at
        )
        counts = {
            table: await conn.fetchval(
                f"SELECT count(*) FROM {table} WHERE tenant_id = $1", tenant_id
            )
            for table in (
                "agency_command_results",
                "agency_canonical_events",
                "agency_outbox_records",
                "intervention_episode_versions",
                "consequential_intervention_specs",
                "consequential_proposals",
                "consequential_proposal_reviews",
                "consequential_predictions",
                "consequential_authorization_decisions",
                "consequential_outcomes",
                "consequential_settlements",
                "consequential_attributions",
            )
        }
        evaluation = await evaluate_agency_state(
            conn,
            scope=AgencyEvaluationScope(
                tenant_id=tenant_id,
                start=START - timedelta(days=1),
                end=WINDOW_END + timedelta(days=1),
                run_id="consequential-agency-component",
            ),
            artifact_refs=("pytest:consequential-agency-component",),
        )
    async with fresh_db.acquire() as conn:
        with pytest.raises(
            asyncpg.IntegrityConstraintViolationError, match="append-only"
        ):
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE consequential_predictions
                    SET metric_definition = 'postdicted_metric'
                    WHERE tenant_id = $1 AND id = $2
                    """,
                    tenant_id,
                    prediction.prediction_id,
                )

    assert episode_create.object_version == 1
    assert episode_final.object_version == 2
    assert duplicate_proposal.duplicate is True
    assert duplicate_proposal.command_result_id == proposal_result.command_result_id
    assert evaluation.incident_counts == {}
    assert evaluation.proposal_spec_atomicity_rate == 1.0
    assert evaluation.prediction_preregistration_rate == 1.0
    assert evaluation.authorization_exactness_rate == 1.0
    assert evaluation.outcome_independence_rate == 1.0
    assert evaluation.settlement_comparability_rate == 1.0
    assert evaluation.conservative_attribution_rate == 1.0
    assert evaluation.episode_manifest_integrity_rate == 1.0
    assert evaluation.spec_continuity_rate == 1.0
    assert evaluation.command_reconstructability_rate == 1.0
    assert evaluation.immutable_storage_guard_rate == 1.0
    assert counts == {
        "agency_command_results": 9,
        "agency_canonical_events": 9,
        "agency_outbox_records": 9,
        "intervention_episode_versions": 2,
        "consequential_intervention_specs": 1,
        "consequential_proposals": 1,
        "consequential_proposal_reviews": 1,
        "consequential_predictions": 1,
        "consequential_authorization_decisions": 1,
        "consequential_outcomes": 1,
        "consequential_settlements": 1,
        "consequential_attributions": 1,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_agency_writers_reject_postdiction_and_scope_substitution(
    fresh_db,
):
    tenant_id = uuid4()
    episode_id = uuid7()
    episode = InterventionEpisode(
        episode_id=episode_id,
        tenant_id=tenant_id,
        stage_links=(
            EpisodeStageLink(
                stage="proposal",
                fate=EpisodeStageFate.NOT_EXECUTED,
                reason="not proposed",
            ),
        ),
        created_at=START,
        updated_at=START,
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        await EpisodeCoordinator().apply(
            conn=conn,
            command=_episode_command(
                tenant_id=tenant_id,
                episode=episode,
                expected_version=0,
                at=START,
                key="attack:episode:create",
            ),
            now=START,
        )
    proposal, proposal_command = _proposal_and_command(
        tenant_id=tenant_id, episode_id=episode_id
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        await ProposalAppender().append_consequential(
            conn=conn, command=proposal_command, now=proposal.created_at
        )
    review_at = START + timedelta(minutes=2)
    review = ConsequentialProposalReview(
        review_id=uuid7(),
        tenant_id=tenant_id,
        proposal_id=proposal.proposal_id,
        proposal_version=1,
        proposal_digest=proposal.proposal_digest,
        intervention_spec_digest=proposal.intervention_spec_digest,
        from_fate=ConsequentialProposalFate.OPEN,
        to_fate=ConsequentialProposalFate.ACCEPTED_FOR_AUTHORIZATION,
        principal_or_policy_ref="actor:operations-owner",
        authority=_consumption_authority(
            tenant_id=tenant_id, operation="review_proposal", at=review_at
        ),
        reason="accepted for test",
        decided_at=review_at,
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        await ProposalAppender().review_consequential(
            conn=conn,
            command=ConsequentialProposalReviewCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="ProposalAppender",
                    responsibility="consequential_proposal",
                    operation="review_proposal",
                    at=review_at,
                    key="attack:proposal:review",
                ),
                review=review,
            ),
            now=review_at,
        )

    authorization_at = START + timedelta(minutes=3)
    bad_scope_decision = AuthorizationDecision(
        decision_id=uuid7(),
        tenant_id=tenant_id,
        proposal_id=proposal.proposal_id,
        proposal_digest=proposal.proposal_digest,
        intervention_spec_digest=proposal.intervention_spec_digest,
        disposition=AuthorizationDisposition.AUTHORIZED,
        principal_or_policy_ref="actor:operations-owner",
        authority=_consumption_authority(
            tenant_id=tenant_id,
            operation="authorize_intervention",
            at=authorization_at,
        ),
        exact_operations=frozenset({proposal.intervention_spec.operation}),
        exact_target_refs=frozenset({"referent:customer:atlas:v4"}),
        exact_field_paths=frozenset({"parameters.discount_percent"}),
        constraints={},
        use_budget=1,
        attempt_budget=1,
        decided_at=authorization_at,
        expires_at=authorization_at + timedelta(days=1),
    )
    bad_scope_command = AuthorizationDecisionCommand(
        context=_context(
            tenant_id=tenant_id,
            owner="AuthorizationApplier",
            responsibility="authorization",
            operation="authorize_intervention",
            at=authorization_at,
            key="attack:authorization:substitution",
        ),
        decision=bad_scope_decision,
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        with pytest.raises(InvariantViolation, match="omits the exact"):
            await AuthorizationApplier().apply(
                conn=conn, command=bad_scope_command, now=authorization_at
            )

    preexisting_outcome = Outcome(
        outcome_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        metric_definition=proposal.intervention_spec.outcome_metric,
        observed_value={"retained": True},
        observed_at=START + timedelta(minutes=3),
        valid_time=START + timedelta(minutes=2),
        source_evidence_refs=("crm:already-known",),
        independent_of_execution_claim=True,
        measurement_quality=1.0,
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        await OutcomeRecorder().record(
            conn=conn,
            command=OutcomeRecordingCommand(
                context=_context(
                    tenant_id=tenant_id,
                    owner="OutcomeRecorder",
                    responsibility="outcome",
                    operation="record_outcome",
                    at=preexisting_outcome.observed_at,
                    key="attack:outcome:known",
                ),
                outcome=preexisting_outcome,
            ),
            now=preexisting_outcome.observed_at,
        )
    prediction_at = START + timedelta(minutes=4)
    late_prediction = Prediction(
        prediction_id=uuid7(),
        tenant_id=tenant_id,
        episode_id=episode_id,
        kind=PredictionKind.INTERVENTION_EFFECT,
        target={"referent": "customer:atlas"},
        probability_distribution={"retained": 0.99, "not_retained": 0.01},
        metric_definition=proposal.intervention_spec.outcome_metric,
        evidence_cutoff=prediction_at,
        forecast_window_start=WINDOW_START,
        forecast_window_end=WINDOW_END,
        censoring_rule="censor after renewal",
        intervention_spec_digest=proposal.intervention_spec_digest,
        comparator=proposal.intervention_spec.comparator,
        baseline={"retention_probability": 0.45},
        preregistered_at=prediction_at,
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        with pytest.raises(
            InvariantViolation, match="after canonical outcome visibility"
        ):
            await PredictionWriter().register(
                conn=conn,
                command=PredictionRegistrationCommand(
                    context=_context(
                        tenant_id=tenant_id,
                        owner="PredictionWriter",
                        responsibility="prediction",
                        operation="register_prediction",
                        at=prediction_at,
                        key="attack:prediction:postdicted",
                    ),
                    prediction=late_prediction,
                ),
                now=prediction_at,
            )

    changed_spec = proposal.intervention_spec.model_copy(
        update={"parameters": {"discount_percent": 12, "term_months": 12}}
    )
    assert changed_spec.spec_digest != proposal.intervention_spec_digest
