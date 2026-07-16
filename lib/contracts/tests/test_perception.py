from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from lib.contracts import (
    BitemporalInterval,
    CandidateGenerationBudget,
    CandidateGenerationFateKind,
    CandidateLaneFate,
    CandidateLaneFateKind,
    CandidateLaneReasonClass,
    CaptureAttempt,
    CaptureAttemptTransition,
    CommandResult,
    CommandResultStatus,
    CommittedAggregateVersion,
    ConsumptionAuthorityContext,
    ContextBudget,
    ContextRiskTier,
    ConversationEpisodeHypothesis,
    ConversationEventKind,
    ConversationEventRevision,
    DiscourseReferent,
    DiscourseReferentKind,
    EntityCandidate,
    EntityCandidateGenerationFate,
    EntityCandidateGenerationRequest,
    EntityCandidateKind,
    EntityCandidateSet,
    EntityMention,
    EntityTypeAssessment,
    EvidenceCoordinate,
    GroundingAdmissionDecision,
    GroundingAdmissionDisposition,
    IngestionReceipt,
    InterpretationContextRequest,
    InterpretationContextSnapshot,
    InterpretationMode,
    MentionAnchor,
    MentionAnchorKind,
    Modality,
    OperationalSufficiencyVerdict,
    ProcessingAuthorityContext,
    ProcessingGeneration,
    ProcessingGenerationState,
    ProcessingGenerationTransition,
    RawDurabilityState,
    ReferentVersionRef,
    ResolutionAssessment,
    RestrictionSet,
    SelectedContextItem,
    SemanticArgument,
    SemanticFrameCandidate,
    SourceAssertion,
    SourceAssertionKind,
    SourceIdentityBinding,
    SourceRetentionFate,
    SpeechActCandidate,
    SpeechActKind,
    SufficiencyDisposition,
)


NOW = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
TENANT = uuid4()


def _updated(model, **updates):
    payload = model.model_dump(mode="python")
    payload.update(updates)
    return type(model).model_validate(payload)


def _processing_authority() -> ProcessingAuthorityContext:
    return ProcessingAuthorityContext(
        tenant_id=TENANT,
        principal_or_service_id="perception-worker",
        purpose="company-physics",
        operation="interpret_signal",
        object_types=RestrictionSet.unrestricted(),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("slack:public"),
        authority_basis_refs=frozenset({"connector-grant:1"}),
        policy_version="processing-authority-v1",
        authority_epoch=2,
        decision_time=NOW - timedelta(hours=1),
        expires_at=NOW + timedelta(hours=2),
    )


def _consumption_authority() -> ConsumptionAuthorityContext:
    return ConsumptionAuthorityContext(
        tenant_id=TENANT,
        principal_or_service_id="ask-consumer",
        purpose="answer-user",
        operation="consume-grounding",
        object_types=RestrictionSet.unrestricted(),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("slack:public"),
        authority_basis_refs=frozenset({"user-grant:1"}),
        policy_version="consumption-authority-v1",
        authority_epoch=4,
        decision_time=NOW - timedelta(minutes=5),
        expires_at=NOW + timedelta(hours=1),
    )


def _coordinate(*, start: int = 0, end: int = 3) -> EvidenceCoordinate:
    return EvidenceCoordinate(
        evidence_record_id="evidence:message:1",
        source_system="slack",
        source_object_id="channel:1:ts:1",
        source_revision="revision:1",
        field_path="text",
        span_start=start,
        span_end=end,
    )


def _context_request(*, self_contained: bool = False) -> InterpretationContextRequest:
    return InterpretationContextRequest(
        request_id="context-request:1",
        tenant_id=TENANT,
        focal_event_revision_ids=("revision:1",),
        mode=InterpretationMode.AS_KNOWN_AT_CUTOFF,
        effective_query_time=NOW,
        evidence_cutoff=NOW,
        knowledge_cutoff=NOW,
        source_topology_version="slack-topology-v1",
        processing_authority=_processing_authority(),
        allowed_source_spaces=RestrictionSet.only("slack:channel:public"),
        risk_tier=ContextRiskTier.HIGH,
        required_probe_surfaces=("coreference", "entity_roles"),
        budget=ContextBudget(
            max_events=20,
            max_topology_hops=4,
            max_source_reads=4,
            max_model_calls=2,
            max_tokens=4000,
            max_latency_ms=3000,
        ),
        policy_versions=("context-policy-v1",),
        self_contained_source=self_contained,
    )


def _selected_item(
    event_id: str = "revision:1",
    *,
    emitted_at: datetime = NOW - timedelta(minutes=2),
    source_space: str = "slack:channel:public",
    authority_label: str = "slack:public",
) -> SelectedContextItem:
    return SelectedContextItem(
        event_revision_id=event_id,
        source_space=source_space,
        emitted_at=emitted_at,
        layer="focal" if event_id == "revision:1" else "source_topology",
        inclusion_reasons=("focal signal" if event_id == "revision:1" else "reply ancestor",),
        source_version=event_id,
        authority_label=authority_label,
        relation_to_focal="self" if event_id == "revision:1" else "reply ancestor",
    )


def _episode(*event_ids: str) -> ConversationEpisodeHypothesis:
    return ConversationEpisodeHypothesis.build(
        membership_weights={event_id: 1.0 for event_id in event_ids},
        boundary_alternatives=("thread plus prior unthreaded Acme message",),
        topic_state="resumed role-map rollout issue",
        continuity_evidence_refs=("topology:reply", "jira:role-map"),
        split_merge_evidence_refs=(),
        boundary_confidence=0.72,
        generator_version="episode-generator-v1",
        configuration_version="context-policy-v1",
    )


def _verdict() -> OperationalSufficiencyVerdict:
    return OperationalSufficiencyVerdict(
        verdict_id="verdict:1",
        probe_refs=("probe:1",),
        risk_tier=ContextRiskTier.HIGH,
        perturbation_policy_version="perturb-v1",
        budget=_context_request().budget,
        disposition=SufficiencyDisposition.OPERATIONALLY_SUFFICIENT,
        omissions=(),
        unresolved_references=(),
        stop_reason="references stable under registered perturbations",
    )


def _snapshot() -> InterpretationContextSnapshot:
    items = (
        _selected_item(),
        _selected_item("revision:root", emitted_at=NOW - timedelta(minutes=10)),
    )
    return InterpretationContextSnapshot.build(
        snapshot_id="context-snapshot:1",
        snapshot_version=1,
        request=_context_request(),
        focal_event_revision_ids=("revision:1",),
        selected_items=items,
        topology_edge_ids=("topology:reply",),
        embedded_episode_hypotheses=(_episode(*(item.event_revision_id for item in items)),),
        discourse_referents=(),
        sufficiency_verdict=_verdict(),
        inherited_processing_authority=_processing_authority(),
        frozen_at=NOW,
        model_and_policy_versions=("context-model-v1", "context-policy-v1"),
    )


def _candidate_request() -> EntityCandidateGenerationRequest:
    return EntityCandidateGenerationRequest.build(
        request_id="candidate-request:1",
        tenant_id=TENANT,
        mention_ref="mention:sam",
        mention_version=1,
        entity_type_assessment_refs=("type:sam:v1",),
        local_role_binding_refs=("role:speaker:v1",),
        context_snapshot_ref="context-snapshot:1:v1",
        registry_as_of_cutoff=NOW,
        processing_authority_fingerprint=_processing_authority().fingerprint,
        permitted_candidate_sources=RestrictionSet.only("source-id", "tenant-alias"),
        permitted_candidate_types=RestrictionSet.only("person"),
        required_retrieval_lanes=("source-id", "tenant-alias"),
        generator_version="candidate-generator-v1",
        index_versions=("source-id-index-v1", "alias-index-v2"),
        model_versions=("resolver-v3",),
        configuration_version="candidate-policy-v2",
        budget=CandidateGenerationBudget(
            max_candidates=20,
            max_source_reads=2,
            max_index_queries=4,
            max_model_calls=1,
            max_latency_ms=1200,
        ),
    )


def _candidate_result(request: EntityCandidateGenerationRequest) -> CommandResult:
    return CommandResult(
        result_id="candidate-result:1",
        command_id=request.request_id,
        canonical_request_hash=request.generation_request_digest,
        writer_scope_id="scope:grounding",
        writer_epoch=1,
        status=CommandResultStatus.APPLIED,
        committed_aggregate_versions=(
            CommittedAggregateVersion(
                semantic_responsibility="entity_candidate_set",
                aggregate_id="candidate-set:1",
                committed_version=1,
            ),
        ),
        event_ids=("event:candidate-set:1",),
    )


def _candidates() -> tuple[EntityCandidate, ...]:
    return (
        EntityCandidate(
            candidate_id="candidate:sam-k",
            kind=EntityCandidateKind.CANONICAL_REFERENT,
            canonical_referent_id="referent:sam-k",
            canonical_referent_version=2,
            candidate_source="tenant-alias",
            candidate_type="person",
            authorized_positive_evidence_refs=("alias:sam",),
            authorized_negative_evidence_refs=(),
        ),
        EntityCandidate(
            candidate_id="candidate:none",
            kind=EntityCandidateKind.NONE_OF_THE_ABOVE,
            authorized_positive_evidence_refs=(),
            authorized_negative_evidence_refs=(),
        ),
        EntityCandidate(
            candidate_id="candidate:novel",
            kind=EntityCandidateKind.NOVEL_REFERENT,
            authorized_positive_evidence_refs=("mention:sam",),
            authorized_negative_evidence_refs=(),
        ),
        EntityCandidate(
            candidate_id="candidate:unknown",
            kind=EntityCandidateKind.UNKNOWN,
            authorized_positive_evidence_refs=(),
            authorized_negative_evidence_refs=(),
        ),
    )


def _candidate_set() -> EntityCandidateSet:
    request = _candidate_request()
    return EntityCandidateSet(
        candidate_set_id="candidate-set:1",
        candidate_set_version=1,
        request=request,
        command_result=_candidate_result(request),
        lane_fates=(
            CandidateLaneFate(
                lane_id="source-id",
                fate=CandidateLaneFateKind.COMPLETE,
                reason_class=CandidateLaneReasonClass.COMPLETED,
                artifact_refs=("query:source-id",),
            ),
            CandidateLaneFate(
                lane_id="tenant-alias",
                fate=CandidateLaneFateKind.COMPLETE,
                reason_class=CandidateLaneReasonClass.COMPLETED,
                artifact_refs=("query:alias",),
            ),
        ),
        candidates=_candidates(),
        registry_version="entity-registry-v9",
        expires_at=NOW + timedelta(hours=4),
    )


def _assessment() -> ResolutionAssessment:
    return ResolutionAssessment(
        assessment_id="resolution:sam:1",
        assessment_version=1,
        candidate_set=_candidate_set(),
        candidate_distribution={
            "candidate:sam-k": 0.62,
            "candidate:none": 0.08,
            "candidate:novel": 0.10,
            "candidate:unknown": 0.20,
        },
        identity_evidence_refs=("alias:sam", "participant-at-cutoff:sam"),
        evidence_dependence_groups={
            "alias:sam": "source-lineage:slack-profile",
            "participant-at-cutoff:sam": "source-lineage:slack-channel",
        },
        decisive_evidence_refs=(),
        missing_discriminators=("which Sam authored the quoted approval",),
        temporal_compatibility_refs=("role-valid-at:message-time",),
        calibration_cohort="slack-person-ambiguous-name",
        scorer_and_calibration_version="resolver-v3/calibration-v2",
        assessed_at=NOW,
        expires_at=NOW + timedelta(hours=2),
    )


def test_conversation_revision_preserves_payload_or_typed_absence() -> None:
    available = ConversationEventRevision(
        tenant_id=TENANT,
        event_id="event:1",
        source_system="slack",
        source_event_id="channel:1:ts:1",
        revision_id="revision:1",
        revision_number=1,
        kind=ConversationEventKind.MESSAGE,
        author_source_id="user:1",
        emitted_at=NOW - timedelta(minutes=2),
        observed_at=NOW - timedelta(minutes=1),
        content_hash="a" * 64,
        raw_evidence_ref="raw:1",
        retention_fate=SourceRetentionFate.PAYLOAD_AVAILABLE,
    )
    assert available.raw_evidence_ref == "raw:1"
    redacted = _updated(
        available,
        content_hash=None,
        raw_evidence_ref=None,
        retention_fate=SourceRetentionFate.LEGALLY_REDACTED_TOMBSTONE,
        retention_reason="subject deletion request",
    )
    assert redacted.retention_reason
    with pytest.raises(ValidationError, match="typed reason"):
        _updated(redacted, retention_reason=None)


def test_context_snapshot_embeds_boundary_hypothesis_and_authorized_cutoff() -> None:
    snapshot = _snapshot()
    assert snapshot.focal_event_revision_ids == ("revision:1",)
    assert len(snapshot.embedded_episode_hypotheses) == 1
    assert snapshot.inherited_processing_authority.is_no_broader_than(
        snapshot.request.processing_authority
    )
    with pytest.raises(ValidationError, match="content_hash"):
        _updated(
            snapshot.embedded_episode_hypotheses[0],
            topic_state="silently changed topic",
        )
    with pytest.raises(ValidationError, match="snapshot_content_hash"):
        _updated(snapshot, model_and_policy_versions=("different-policy",))


@pytest.mark.parametrize(
    ("item", "message"),
    [
        (_selected_item(emitted_at=NOW + timedelta(seconds=1)), "after its cutoff"),
        (_selected_item(source_space="slack:private"), "disallowed source space"),
        (_selected_item(authority_label="slack:restricted"), "impermissible authority label"),
    ],
)
def test_context_rejects_future_or_unauthorized_items(item, message) -> None:
    snapshot = _snapshot()
    with pytest.raises(ValidationError, match=message):
        _updated(snapshot, selected_items=(item,))


def test_self_contained_source_may_freeze_without_episode_but_slack_may_not() -> None:
    snapshot = _snapshot()
    with pytest.raises(ValidationError, match="boundary hypothesis"):
        _updated(snapshot, embedded_episode_hypotheses=())
    request = _context_request(self_contained=True)
    one_item = _selected_item()
    payload = snapshot.model_dump(mode="python", exclude={"snapshot_content_hash"})
    self_contained = InterpretationContextSnapshot.build(
        **{
            **payload,
            "request": request,
            "selected_items": (one_item,),
            "embedded_episode_hypotheses": (),
        },
    )
    assert self_contained.request.self_contained_source


def test_local_discourse_referent_can_remain_multi_candidate_or_unresolved() -> None:
    referent = DiscourseReferent(
        referent_id="discourse:sam",
        kind=DiscourseReferentKind.NOMINAL,
        anchor=_coordinate(),
        candidate_antecedent_refs=("source-user:sam-k", "source-user:sam-r"),
        supporting_event_revision_ids=("revision:root",),
        confidence_distribution={"source-user:sam-k": 0.55, "source-user:sam-r": 0.45},
    )
    assert len(referent.candidate_antecedent_refs) == 2
    with pytest.raises(ValidationError, match="must remain unresolved"):
        _updated(referent, candidate_antecedent_refs=(), unresolved=False)


def test_source_semantics_preserve_attribution_negation_modality_roles_and_act() -> None:
    assertion = SourceAssertion(
        assertion_id="assertion:1",
        assertion_version=1,
        context_snapshot_id="context-snapshot:1",
        coordinates=(_coordinate(start=0, end=42),),
        current_speaker_or_author="user:transmitter",
        attributed_speaker_or_author="user:sam",
        kind=SourceAssertionKind.PROMISED,
        expressed_content="Sam said do not ship until the role map lands",
        source_status="ordinary Slack utterance",
        extractor_version="source-semantics-v1",
        uncertainty=0.18,
    )
    frame = SemanticFrameCandidate(
        frame_id="frame:ship",
        frame_version=1,
        source_assertion_id=assertion.assertion_id,
        predicate_or_event_type="ship_product",
        arguments=(
            SemanticArgument(
                argument_id="arg:actor",
                role="speaker_of_quoted_instruction",
                local_value="Sam",
                confidence=0.74,
            ),
            SemanticArgument(
                argument_id="arg:object",
                role="object",
                implicit=True,
                confidence=0.52,
            ),
        ),
        negated=True,
        modality=Modality.REQUIRED,
        conditional_expression="until role_map_lands",
        confidence=0.68,
        extractor_version="semantic-frame-v1",
    )
    act = SpeechActCandidate(
        speech_act_id="speech-act:1",
        source_assertion_id=assertion.assertion_id,
        distribution={SpeechActKind.PROMISE: 0.55, SpeechActKind.REPORT: 0.45},
        authority_cue_refs=("attributed speaker uncertain",),
        extractor_version="speech-act-v1",
    )
    assert frame.negated and frame.conditional_expression
    assert act.distribution[SpeechActKind.PROMISE] == 0.55


def test_explicit_and_implicit_mentions_cannot_impersonate_each_other() -> None:
    explicit = MentionAnchor(
        anchor_id="anchor:sam",
        kind=MentionAnchorKind.EXPLICIT,
        coordinate=_coordinate(),
        surface_form="Sam",
    )
    implicit = MentionAnchor(
        anchor_id="anchor:ship-object",
        kind=MentionAnchorKind.IMPLICIT_REFERENT,
        coordinate=EvidenceCoordinate(
            evidence_record_id="evidence:message:1",
            source_system="slack",
            source_object_id="channel:1:ts:1",
            source_revision="revision:1",
            field_path="text",
        ),
        triggering_frame_id="frame:ship",
        omitted_role="object",
        supporting_context_refs=("revision:root",),
        inference_basis="elided object of ship",
    )
    assert explicit.surface_form == "Sam"
    assert implicit.surface_form is None
    with pytest.raises(ValidationError, match="cannot fabricate"):
        _updated(implicit, surface_form="it")


def test_entity_mention_has_no_identity_or_type_authority() -> None:
    mention = EntityMention(
        mention_id="mention:sam",
        mention_version=1,
        primary_anchor=MentionAnchor(
            anchor_id="anchor:sam",
            kind=MentionAnchorKind.EXPLICIT,
            coordinate=_coordinate(),
            surface_form="Sam",
        ),
        source_assertion_and_frame_refs=("assertion:1", "frame:ship"),
        detection_confidence=0.99,
        extractor_version="mention-extractor-v1",
    )
    assert "canonical_referent_id" not in type(mention).model_fields
    with pytest.raises(ValidationError, match="Extra inputs"):
        EntityMention.model_validate(
            {**mention.model_dump(mode="python"), "canonical_referent_id": "referent:sam"}
        )


def test_entity_type_assessment_is_open_world_and_separate() -> None:
    assessment = EntityTypeAssessment(
        assessment_id="type:sam",
        assessment_version=1,
        mention_or_referent_ref="mention:sam:v1",
        type_distribution={"person": 0.8, "shared_account": 0.1, "unknown": 0.1},
        evidence_basis_refs=("source-schema:slack-user",),
        temporal_scope=BitemporalInterval(
            valid_from=NOW - timedelta(days=1),
            transaction_from=NOW,
        ),
        model_and_calibration_version="type-model-v2",
    )
    assert assessment.type_distribution["unknown"] == 0.1
    with pytest.raises(ValidationError, match="unknown option"):
        _updated(assessment, type_distribution={"person": 1.0})


def test_candidate_request_digest_is_self_contained_and_tamper_evident() -> None:
    request = _candidate_request()
    same = _candidate_request()
    assert request.generation_request_digest == same.generation_request_digest
    renamed = EntityCandidateGenerationRequest.build(
        **{
            **request.model_dump(
                mode="python",
                exclude={"generation_request_digest", "request_id"},
            ),
            "request_id": "candidate-request:renamed",
        }
    )
    assert renamed.generation_request_digest == request.generation_request_digest
    with pytest.raises(ValidationError, match="does not match"):
        _updated(request, configuration_version="candidate-policy-v3")
    with pytest.raises(ValidationError, match="sorted and unique"):
        _updated(
            request,
            required_retrieval_lanes=("tenant-alias", "source-id"),
            generation_request_digest="0" * 64,
        )


def test_hidden_impermissible_population_cannot_change_request_digest_or_metadata() -> None:
    request = _candidate_request()
    hidden_world_a = {"impermissible_candidates": 0}
    hidden_world_b = {"impermissible_candidates": 5000}
    assert hidden_world_a != hidden_world_b
    assert request.generation_request_digest == _candidate_request().generation_request_digest
    assert "impermissible" not in request.model_dump_json()
    assert not hasattr(CandidateLaneFate, "hidden_match_count")


def test_candidate_set_has_every_lane_fate_open_set_options_and_authorized_candidates() -> None:
    candidate_set = _candidate_set()
    assert {item.kind for item in candidate_set.candidates} >= {
        EntityCandidateKind.NONE_OF_THE_ABOVE,
        EntityCandidateKind.NOVEL_REFERENT,
        EntityCandidateKind.UNKNOWN,
    }
    with pytest.raises(ValidationError, match="exactly one fate"):
        _updated(candidate_set, lane_fates=candidate_set.lane_fates[:1])
    forbidden = _updated(
        candidate_set.candidates[0],
        candidate_source="cross-tenant-index",
    )
    with pytest.raises(ValidationError, match="outside processing authority"):
        _updated(candidate_set, candidates=(forbidden, *candidate_set.candidates[1:]))
    with pytest.raises(ValidationError, match="none, novel and unknown"):
        _updated(
            candidate_set,
            candidates=tuple(
                item for item in candidate_set.candidates if item.kind is not EntityCandidateKind.UNKNOWN
            ),
        )


def test_candidate_request_has_one_set_or_one_terminal_no_set_fate() -> None:
    candidate_set = _candidate_set()
    committed = EntityCandidateGenerationFate(
        request=candidate_set.request,
        kind=CandidateGenerationFateKind.SET_COMMITTED,
        candidate_set=candidate_set,
    )
    assert committed.candidate_set is not None
    with pytest.raises(ValidationError, match="exactly one candidate set"):
        _updated(committed, candidate_set=None)


def test_resolution_distribution_covers_every_candidate_without_consumer_policy() -> None:
    assessment = _assessment()
    assert set(assessment.candidate_distribution) == {
        item.candidate_id for item in assessment.candidate_set.candidates
    }
    assert "risk_tier" not in type(assessment).model_fields
    with pytest.raises(ValidationError, match="every and only"):
        _updated(assessment, candidate_distribution={"candidate:sam-k": 1.0})


def test_grounding_admission_can_select_distribute_or_abstain_without_mutating_assessment() -> None:
    assessment = _assessment()
    decision = GroundingAdmissionDecision(
        decision_id="admission:1",
        decision_version=1,
        assessment=assessment,
        consumer="Ask",
        purpose="answer-user",
        operation="consume-grounding",
        risk_tier="medium",
        blast_radius="one answer",
        expected_loss=0.2,
        consumption_authority=_consumption_authority(),
        consumer_supports_distributions=True,
        disposition=GroundingAdmissionDisposition.SINGLE_REFERENT,
        selected_referent=ReferentVersionRef(
            referent_id="referent:sam-k",
            referent_version=2,
        ),
        reason_codes=("sufficient_for_exploratory_answer",),
        decided_at=NOW,
        expires_at=NOW + timedelta(minutes=30),
    )
    assert decision.selected_referent is not None
    with pytest.raises(ValidationError, match="purpose must match"):
        _updated(decision, purpose="different-purpose")
    with pytest.raises(ValidationError, match="no top-one"):
        _updated(
            decision,
            disposition=GroundingAdmissionDisposition.CANDIDATE_DISTRIBUTION,
            permitted_distribution={"candidate:sam-k": 0.62, "candidate:unknown": 0.38},
        )
    abstained = _updated(
        decision,
        disposition=GroundingAdmissionDisposition.ABSTENTION,
        selected_referent=None,
    )
    assert abstained.selected_referent is None


def test_source_binding_is_optional_and_must_be_genuine_and_matching() -> None:
    assessment = _assessment()
    binding = SourceIdentityBinding(
        binding_id="binding:slack:sam",
        binding_version=1,
        tenant_id=TENANT,
        source_system="slack",
        source_native_identifier="U-SAM",
        source_identity_authority_ref="slack-user-id-contract-v1",
        canonical_referent_id="referent:sam-k",
        canonical_referent_version=2,
        temporal_scope=BitemporalInterval(
            valid_from=NOW - timedelta(days=30),
            transaction_from=NOW - timedelta(days=30),
        ),
        evidence_refs=("source-profile:U-SAM",),
    )
    decision = GroundingAdmissionDecision(
        decision_id="admission:source-bound",
        decision_version=1,
        assessment=assessment,
        consumer="PhysicalStateApplier",
        purpose="answer-user",
        operation="consume-grounding",
        risk_tier="high",
        blast_radius="tenant identity registry",
        expected_loss=2.0,
        consumption_authority=_consumption_authority(),
        consumer_supports_distributions=False,
        disposition=GroundingAdmissionDisposition.SINGLE_REFERENT,
        selected_referent=ReferentVersionRef(
            referent_id="referent:sam-k",
            referent_version=2,
        ),
        genuine_source_binding=binding,
        reason_codes=("authenticated_source_native_mapping",),
        decided_at=NOW,
        expires_at=NOW + timedelta(minutes=30),
    )
    assert decision.genuine_source_binding is binding
    with pytest.raises(ValidationError, match="must match"):
        _updated(
            decision,
            genuine_source_binding=_updated(
                binding,
                canonical_referent_id="referent:other-sam",
            ),
        )


def test_ingestion_ack_requires_raw_durability_and_one_current_processing_head() -> None:
    capture = CaptureAttempt(
        attempt_id="capture:1",
        attempt_generation=1,
        adapter_version="slack-adapter-v1",
        storage_version="raw-store-v1",
        authority_fingerprint=_processing_authority().fingerprint,
        state=RawDurabilityState.RAW_DURABLE,
        occurred_at=NOW,
    )
    first = ProcessingGeneration(
        generation_id="generation:1",
        generation_number=1,
        raw_reference="raw:1",
        mapping_version="slack-map-v1",
        schema_version="observation-v2",
        configuration_version="ingest-v2",
        processing_authority_fingerprint=_processing_authority().fingerprint,
        generation_digest="3" * 64,
        state=ProcessingGenerationState.SUPERSEDED_BY_NEW_GENERATION,
        occurred_at=NOW,
    )
    second = ProcessingGeneration(
        generation_id="generation:2",
        generation_number=2,
        parent_generation_id="generation:1",
        raw_reference="raw:1",
        mapping_version="slack-map-v2",
        schema_version="observation-v2",
        configuration_version="ingest-v3",
        processing_authority_fingerprint=_processing_authority().fingerprint,
        generation_digest="4" * 64,
        state=ProcessingGenerationState.PENDING,
        occurred_at=NOW + timedelta(seconds=1),
    )
    receipt = IngestionReceipt(
        receipt_id="receipt:1",
        tenant_id=TENANT,
        source_system="slack",
        authenticated_delivery_id="delivery:1",
        source_cursor_or_offset="cursor:1",
        payload_hash="5" * 64,
        raw_durability_state=RawDurabilityState.RAW_DURABLE,
        capture_attempts=(capture,),
        raw_reference="raw:1",
        external_acknowledged_at=NOW + timedelta(seconds=1),
        processing_generations=(first, second),
        current_processing_generation_id="generation:2",
    )
    assert receipt.current_processing_generation_id == "generation:2"
    with pytest.raises(ValidationError, match="cannot precede raw durability"):
        _updated(
            receipt,
            raw_durability_state=RawDurabilityState.RECEIVED,
            raw_reference=None,
        )
    with pytest.raises(ValidationError, match="exactly one current head"):
        _updated(
            receipt,
            processing_generations=(
                _updated(first, state=ProcessingGenerationState.OBSERVATION_COMMITTED),
                _updated(second, parent_generation_id=None),
            ),
        )


def test_capture_and_processing_reducers_reject_illegal_or_terminal_transitions() -> None:
    received = CaptureAttempt(
        attempt_id="capture:1",
        attempt_generation=1,
        adapter_version="v1",
        storage_version="v1",
        authority_fingerprint="6" * 64,
        state=RawDurabilityState.RECEIVED,
        occurred_at=NOW,
    )
    CaptureAttemptTransition(
        before=received,
        after=_updated(
            received,
            state=RawDurabilityState.RAW_DURABLE,
            occurred_at=NOW + timedelta(seconds=1),
        ),
    )
    with pytest.raises(ValidationError, match="illegal capture"):
        CaptureAttemptTransition(
            before=received,
            after=_updated(received, state=RawDurabilityState.CAPTURE_EXHAUSTED),
        )

    pending = ProcessingGeneration(
        generation_id="generation:1",
        generation_number=1,
        raw_reference="raw:1",
        mapping_version="v1",
        schema_version="v1",
        configuration_version="v1",
        processing_authority_fingerprint="7" * 64,
        generation_digest="8" * 64,
        state=ProcessingGenerationState.PENDING,
        occurred_at=NOW,
    )
    ProcessingGenerationTransition(
        before=pending,
        after=_updated(pending, state=ProcessingGenerationState.NORMALIZING),
    )
    committed = _updated(pending, state=ProcessingGenerationState.OBSERVATION_COMMITTED)
    with pytest.raises(ValidationError, match="terminal processing"):
        ProcessingGenerationTransition(
            before=committed,
            after=_updated(committed, state=ProcessingGenerationState.RETRYABLE),
        )
