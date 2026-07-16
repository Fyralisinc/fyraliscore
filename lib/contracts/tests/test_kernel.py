from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from lib.contracts import (
    AggregateVersionRef,
    BitemporalInterval,
    CommandEnvelope,
    CommandResult,
    CommandResultStatus,
    CommitAuthorityBinding,
    CommittedAggregateVersion,
    CompatibilityMaturity,
    ComputationalBundle,
    ConsumptionAuthorityContext,
    ContentDomain,
    ContractCompatibilityManifest,
    EpistemicStatus,
    EventPosition,
    IdempotencyReplayDecision,
    IdempotencyReplayDisposition,
    IsolationRequirement,
    LifecycleStatus,
    MultiAggregateMutationPlan,
    ProcessingAuthorityContext,
    ProposalValidationApply,
    ProvenanceAndConfidence,
    RelationSemantics,
    RestrictionSet,
    SemanticAxes,
    SemanticComputationalCommitBoundary,
    SemanticDecisionRecord,
    SemanticPlane,
    ValidationDisposition,
    WatermarkVector,
    WriterCutoverState,
    WriterScopeEpoch,
    validate_command_writer_scope,
)


NOW = datetime(2026, 7, 16, tzinfo=timezone.utc)
TENANT = uuid4()
REQUEST_HASH = "a" * 64


def _updated(model, **updates):
    payload = model.model_dump(mode="python")
    payload.update(updates)
    return type(model).model_validate(payload)


def _processing_authority(
    *,
    object_types: RestrictionSet | None = None,
    fields: RestrictionSet | None = None,
    source_labels: RestrictionSet | None = None,
    purpose: str = "entity_grounding",
    decision_time: datetime = NOW,
    expires_at: datetime = NOW + timedelta(hours=1),
) -> ProcessingAuthorityContext:
    return ProcessingAuthorityContext(
        tenant_id=TENANT,
        principal_or_service_id="entity-resolver",
        purpose=purpose,
        operation="derive_candidates",
        object_types=object_types or RestrictionSet.unrestricted(),
        object_ids=RestrictionSet.unrestricted(),
        fields=fields or RestrictionSet.unrestricted(),
        source_labels=source_labels or RestrictionSet.unrestricted(),
        authority_basis_refs=frozenset({"grant:1"}),
        policy_version="authority-v1",
        authority_epoch=3,
        decision_time=decision_time,
        expires_at=expires_at,
    )


def _position(offset: int, *, partition: str = "p0") -> EventPosition:
    return EventPosition(
        log_id="canonical-events",
        partition_epoch=1,
        partition_id=partition,
        offset=offset,
    )


def _watermark(*positions: EventPosition) -> WatermarkVector:
    return WatermarkVector(
        positions=positions,
        database_snapshot_token="snapshot:1",
        captured_at=NOW,
    )


def _aggregate(expected_version: int = 2) -> AggregateVersionRef:
    return AggregateVersionRef(
        semantic_responsibility="belief_assertion",
        aggregate_id="belief:1",
        expected_version=expected_version,
    )


def _command(*, epoch: int = 4) -> CommandEnvelope:
    return CommandEnvelope(
        tenant_id=TENANT,
        command_id="cmd:1",
        semantic_operation="revise_belief",
        target_semantic_key="belief:1",
        writer_scope_id="scope:belief",
        writer_epoch=epoch,
        expected_aggregate=_aggregate(),
        semantic_idempotency_scope="belief-revision",
        idempotency_key="observation:1:belief:1",
        canonical_request_hash=REQUEST_HASH,
        issuing_principal="think-worker",
        authority_decision_ref="authority-decision:1",
        deadline=NOW + timedelta(minutes=1),
        schema_version="belief-command-v1",
        trace_id="trace:1",
        correlation_id="correlation:1",
    )


def _applied_result() -> CommandResult:
    return CommandResult(
        result_id="result:1",
        command_id="cmd:1",
        canonical_request_hash=REQUEST_HASH,
        writer_scope_id="scope:belief",
        writer_epoch=4,
        status=CommandResultStatus.APPLIED,
        committed_aggregate_versions=(
            CommittedAggregateVersion(
                semantic_responsibility="belief_assertion",
                aggregate_id="belief:1",
                committed_version=3,
            ),
        ),
        event_ids=("event:1",),
    )


def test_semantic_axes_keep_relation_epistemic_and_lifecycle_meanings_independent() -> None:
    axes = SemanticAxes(
        content_domain=ContentDomain.DESCRIPTIVE_WORLD,
        epistemic_status=EpistemicStatus.INFERRED_BELIEF,
        lifecycle_status=LifecycleStatus.ACTIVE,
        relation_semantics=RelationSemantics.CAUSAL,
        plane=SemanticPlane.BRAIN,
        provenance_and_confidence=ProvenanceAndConfidence(
            derived_from_refs=("evidence:1",),
            confidence_distribution={"true": 0.7, "false": 0.3},
        ),
    )
    assert axes.relation_semantics is RelationSemantics.CAUSAL
    assert axes.epistemic_status is EpistemicStatus.INFERRED_BELIEF


@pytest.mark.parametrize(
    ("plane", "epistemic"),
    [
        (SemanticPlane.EVIDENCE, EpistemicStatus.INFERRED_BELIEF),
        (SemanticPlane.DERIVED, EpistemicStatus.AUTHORITATIVE_RECORD),
        (SemanticPlane.BRAIN, EpistemicStatus.SOURCE_EMITTED),
        (SemanticPlane.INTENT, EpistemicStatus.INFERRED_BELIEF),
        (SemanticPlane.PHYSICAL_STATE, EpistemicStatus.HYPOTHESIS),
    ],
)
def test_semantic_axes_reject_plane_authority_collapse(plane, epistemic) -> None:
    with pytest.raises(ValidationError):
        SemanticAxes(
            content_domain=ContentDomain.DESCRIPTIVE_WORLD,
            epistemic_status=epistemic,
            lifecycle_status=LifecycleStatus.ACTIVE,
            plane=plane,
            provenance_and_confidence=ProvenanceAndConfidence(),
        )


def test_bitemporal_interval_uses_valid_and_transaction_time_independently() -> None:
    interval = BitemporalInterval(
        valid_from=NOW,
        valid_to=NOW + timedelta(days=2),
        transaction_from=NOW + timedelta(hours=1),
    )
    assert not interval.visible_at(valid_at=NOW, known_at=NOW)
    assert interval.visible_at(
        valid_at=NOW + timedelta(days=1),
        known_at=NOW + timedelta(hours=2),
    )
    assert not interval.visible_at(
        valid_at=NOW + timedelta(days=2),
        known_at=NOW + timedelta(hours=2),
    )


def test_bitemporal_interval_rejects_naive_or_backward_times() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        BitemporalInterval(
            valid_from=datetime(2026, 1, 1),
            transaction_from=NOW,
        )
    with pytest.raises(ValidationError, match="valid_to"):
        BitemporalInterval(
            valid_from=NOW,
            valid_to=NOW,
            transaction_from=NOW,
        )


def test_authority_restriction_composition_is_monotone() -> None:
    broad = _processing_authority()
    narrow = _processing_authority(
        object_types=RestrictionSet.only("person"),
        fields=RestrictionSet.only("display_name"),
        source_labels=RestrictionSet.only("slack:public"),
    )
    composed = broad.restrict_with(narrow)

    assert composed.is_no_broader_than(broad)
    assert composed.is_no_broader_than(narrow)
    assert composed.object_types.permits("person")
    assert not composed.object_types.permits("customer")


def test_authority_fingerprint_is_stable_for_unordered_sets() -> None:
    first = _processing_authority(
        fields=RestrictionSet.only("name", "role"),
        source_labels=RestrictionSet.only("jira", "slack"),
    )
    second = _processing_authority(
        fields=RestrictionSet(values=frozenset(("role", "name"))),
        source_labels=RestrictionSet(values=frozenset(("slack", "jira"))),
    )
    assert first.fingerprint == second.fingerprint


def test_authority_composition_rejects_kind_purpose_and_nonoverlapping_time() -> None:
    processing = _processing_authority()
    consuming = ConsumptionAuthorityContext(
        **processing.model_dump(exclude={"kind"}),
    )
    with pytest.raises(ValueError, match="different kind"):
        processing.restrict_with(consuming)
    with pytest.raises(ValueError, match="different purpose"):
        processing.restrict_with(_processing_authority(purpose="answer_user"))
    later = _processing_authority(
        decision_time=NOW + timedelta(hours=2),
        expires_at=NOW + timedelta(hours=3),
    )
    with pytest.raises(ValidationError, match="expiry"):
        processing.restrict_with(later)


def test_event_positions_are_ordered_only_within_one_partition_epoch() -> None:
    assert _position(9).covers(_position(8))
    with pytest.raises(ValueError, match="incomparable"):
        _position(9).covers(_position(8, partition="p1"))


def test_watermark_vector_covers_every_registered_partition() -> None:
    older = _watermark(_position(4), _position(2, partition="p1"))
    newer = _watermark(_position(5), _position(2, partition="p1"))
    incomplete = _watermark(_position(9))
    assert newer.covers(older)
    assert not incomplete.covers(older)
    with pytest.raises(ValidationError, match="one offset"):
        _watermark(_position(1), _position(2))


def test_writer_scope_fences_stale_wrong_and_retired_writers() -> None:
    scope = WriterScopeEpoch(
        scope_id="scope:belief",
        tenant_id=TENANT,
        semantic_responsibility="belief_assertion",
        source_partition="tenant-wide",
        writer_owner="EpistemicApplier",
        epoch=4,
        state=WriterCutoverState.NEW_CANONICAL,
    )
    validate_command_writer_scope(
        _command(),
        scope,
        writer_owner="EpistemicApplier",
        semantic_responsibility="belief_assertion",
        source_partition="tenant-wide",
    )
    with pytest.raises(ValueError, match="does not permit"):
        validate_command_writer_scope(
            _command(epoch=3),
            scope,
            writer_owner="EpistemicApplier",
            semantic_responsibility="belief_assertion",
            source_partition="tenant-wide",
        )
    retired = _updated(scope, state=WriterCutoverState.RETIRED)
    with pytest.raises(ValueError, match="does not permit"):
        validate_command_writer_scope(
            _command(),
            retired,
            writer_owner="EpistemicApplier",
            semantic_responsibility="belief_assertion",
            source_partition="tenant-wide",
        )


def test_multi_aggregate_plan_requires_bounded_complete_sorted_write_set() -> None:
    members = (
        AggregateVersionRef(
            semantic_responsibility="concern",
            aggregate_id="a",
            expected_version=1,
        ),
        AggregateVersionRef(
            semantic_responsibility="concern",
            aggregate_id="b",
            expected_version=2,
        ),
    )
    plan = MultiAggregateMutationPlan(
        plan_id="plan:1",
        tenant_id=TENANT,
        aggregate_versions=members,
        shared_invariant="replace corrected concern gap atomically",
        canonical_request_hash=REQUEST_HASH,
        maximum_size=2,
        dependency_edge_refs=("dependency:a:b",),
        isolation_requirement=IsolationRequirement.SERIALIZABLE,
    )
    assert len(plan.aggregate_versions) == 2
    with pytest.raises(ValidationError, match="lock order"):
        _updated(plan, aggregate_versions=tuple(reversed(members)))


def test_command_has_exactly_one_aggregate_boundary() -> None:
    command = _command()
    assert command.expected_aggregate is not None
    with pytest.raises(ValidationError, match="exactly one"):
        _updated(command, mutation_plan_id="plan:1")
    with pytest.raises(ValidationError, match="exactly one"):
        _updated(command, expected_aggregate=None)


def test_command_result_shapes_cannot_hide_missing_commit_or_retry_fate() -> None:
    assert _applied_result().status is CommandResultStatus.APPLIED
    with pytest.raises(ValidationError, match="committed versions"):
        _updated(_applied_result(), event_ids=())
    with pytest.raises(ValidationError, match="prior_result"):
        _updated(
            _applied_result(),
            status=CommandResultStatus.DUPLICATE,
            committed_aggregate_versions=(),
            event_ids=(),
        )
    with pytest.raises(ValidationError, match="retry_after"):
        _updated(
            _applied_result(),
            status=CommandResultStatus.REJECTED_RETRYABLE,
            committed_aggregate_versions=(),
            event_ids=(),
            rejection_code="lease_busy",
        )


def test_idempotency_replay_distinguishes_duplicate_from_conflict() -> None:
    duplicate = IdempotencyReplayDecision(
        semantic_idempotency_scope="belief-revision",
        idempotency_key="key:1",
        incoming_request_hash=REQUEST_HASH,
        stored_request_hash=REQUEST_HASH,
        prior_result_ref="result:1",
    )
    conflict = _updated(duplicate, incoming_request_hash="b" * 64)
    assert duplicate.disposition is IdempotencyReplayDisposition.DUPLICATE
    assert conflict.disposition is IdempotencyReplayDisposition.CONFLICT


def test_joint_compute_keeps_semantic_decisions_and_commit_authority_distinct() -> None:
    decisions = (
        SemanticDecisionRecord(
            decision_id="decision:frame",
            decision_type="semantic_frame",
            proposition_or_state_space="commitment or report",
            distribution={"commitment": 0.6, "report": 0.4},
            evidence_ref_ids=("evidence:1",),
            policy_or_model_version="model:v1",
        ),
        SemanticDecisionRecord(
            decision_id="decision:entity",
            decision_type="entity_resolution",
            proposition_or_state_space="person:1 or unknown",
            distribution={"person:1": 0.8, "unknown": 0.2},
            evidence_ref_ids=("evidence:1",),
            policy_or_model_version="model:v1",
        ),
    )
    boundary = SemanticComputationalCommitBoundary(
        semantic_decisions=decisions,
        computational_bundle=ComputationalBundle(
            bundle_id="bundle:1",
            topology="joint",
            semantic_decision_ids=("decision:frame", "decision:entity"),
            model_call_refs=("call:1",),
        ),
        commit_bindings=(
            CommitAuthorityBinding(
                semantic_decision_id="decision:entity",
                command_id="command:entity-admission",
                writer_scope_id="scope:grounding",
                writer_epoch=1,
                applier_id="GroundingAdmissionApplier",
            ),
        ),
    )
    assert len(boundary.semantic_decisions) == 2
    assert len(boundary.commit_bindings) == 1
    with pytest.raises(ValidationError, match="every and only"):
        _updated(
            boundary,
            computational_bundle=_updated(
                boundary.computational_bundle,
                semantic_decision_ids=("decision:frame",),
            ),
        )


def test_compatibility_manifest_enforces_readers_first_and_proven_decoder_removal() -> None:
    base = ContractCompatibilityManifest(
        contract_id="BeliefAssertion",
        maturity=CompatibilityMaturity.STABLE,
        producer_version="v2",
        supported_reader_range=">=v1,<v3",
        additive_default_behavior="new field defaults to unknown",
        semantic_migration="v1 unknown maps to v2 explicit_unknown",
        dual_decode_until=NOW + timedelta(days=30),
        minimum_consumer_version="v1.4",
        activation_gate="all consumer receipts report v1.4+",
    )
    with pytest.raises(ValidationError, match="required readers"):
        _updated(base, new_producer_enabled=True)
    with pytest.raises(ValidationError, match="watermark"):
        _updated(
            base,
            all_required_readers_verified=True,
            new_producer_enabled=True,
            old_decoder_removed=True,
        )
    complete = _updated(
        base,
        all_required_readers_verified=True,
        new_producer_enabled=True,
        removal_watermark=_watermark(_position(10)),
        replay_fixtures_passed=True,
        rollback_requires_old_decoder=False,
        old_decoder_removed=True,
    )
    assert complete.old_decoder_removed


def test_only_validated_accepted_proposal_can_have_apply_result() -> None:
    accepted = ProposalValidationApply(
        proposal_id="proposal:1",
        proposal_version="v1",
        canonical_proposal_hash=REQUEST_HASH,
        validator_id="epistemic-validator",
        validator_version="v1",
        disposition=ValidationDisposition.ACCEPT,
        validation_reason_codes=("evidence_sufficient",),
        applier_id="EpistemicApplier",
        command_result=_applied_result(),
    )
    assert accepted.command_result is not None
    with pytest.raises(ValidationError, match="cannot carry"):
        _updated(accepted, disposition=ValidationDisposition.REJECT)
