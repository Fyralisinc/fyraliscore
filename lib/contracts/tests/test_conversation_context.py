from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from lib.contracts import (
    AgencyWriteContext,
    CandidateContextLayer,
    CommitInterpretationContextCommand,
    ContextBudget,
    ContextCandidateCost,
    ContextProbeEnvelope,
    ContextProbeResult,
    ContextRiskTier,
    ContextSelectionPolicy,
    ConversationContextCandidate,
    ConversationEpisodeHypothesis,
    InterpretationContextHeadExpectation,
    InterpretationContextRequest,
    InterpretationMode,
    ProcessingAuthorityContext,
    RestrictionSet,
    SelectedContextItem,
    SufficiencyDisposition,
    WriterCutoverState,
    WriterScopeEpoch,
    canonical_sha256,
)
from lib.conversation_context_selection import select_context


NOW = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)


def _request(*, allowed_ids: tuple[str, ...]) -> InterpretationContextRequest:
    tenant_id = UUID("11111111-1111-4111-8111-111111111111")
    authority = ProcessingAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id="service:context-probe",
        purpose="entity_grounding",
        operation="select_interpretation_context",
        object_types=RestrictionSet.only("conversation_event_revision"),
        object_ids=RestrictionSet.only(*allowed_ids),
        fields=RestrictionSet.only("content", "author", "source_topology"),
        source_labels=RestrictionSet.only("slack:message"),
        authority_basis_refs=frozenset({"policy:context-selection-v1"}),
        policy_version="context-processing-v1",
        authority_epoch=7,
        decision_time=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=1),
    )
    return InterpretationContextRequest(
        request_id="request:slack:focal",
        tenant_id=tenant_id,
        focal_event_revision_ids=("slack:C-finance:102.1:v1",),
        mode=InterpretationMode.AS_KNOWN_AT_CUTOFF,
        effective_query_time=NOW,
        evidence_cutoff=NOW,
        knowledge_cutoff=NOW,
        source_topology_version="slack-topology-v4",
        processing_authority=authority,
        allowed_source_spaces=RestrictionSet.only("C-finance"),
        risk_tier=ContextRiskTier.MEDIUM,
        required_probe_surfaces=(
            "source_topology",
            "boundary_sensitivity",
        ),
        budget=ContextBudget(
            max_events=12,
            max_topology_hops=3,
            max_source_reads=4,
            max_model_calls=2,
            max_tokens=2_048,
            max_latency_ms=5_000,
        ),
        policy_versions=("context-candidates-v1",),
        self_contained_source=False,
    )


def _item(
    event_id: str,
    *,
    emitted_at: datetime,
    layer: CandidateContextLayer,
) -> SelectedContextItem:
    return SelectedContextItem(
        event_revision_id=event_id,
        source_space="C-finance",
        emitted_at=emitted_at,
        layer=layer,
        inclusion_reasons=("test candidate",),
        source_version=event_id,
        authority_label="slack:message",
        relation_to_focal=layer.value,
    )


def _candidate(
    request: InterpretationContextRequest,
    *,
    candidate_id: UUID,
    extra_event: str | None = None,
    token_count: int = 100,
) -> ConversationContextCandidate:
    items = [
        _item(
            request.focal_event_revision_ids[0],
            emitted_at=NOW,
            layer=CandidateContextLayer.FOCAL,
        )
    ]
    layers = [CandidateContextLayer.FOCAL]
    if extra_event:
        items.append(
            _item(
                extra_event,
                emitted_at=NOW - timedelta(minutes=2),
                layer=CandidateContextLayer.SOURCE_TOPOLOGY,
            )
        )
        layers.append(CandidateContextLayer.SOURCE_TOPOLOGY)
    hypothesis = ConversationEpisodeHypothesis.build(
        membership_weights={item.event_revision_id: 1.0 for item in items},
        boundary_alternatives=("thread", "temporal burst"),
        topic_state="payment incident",
        continuity_evidence_refs=tuple(item.event_revision_id for item in items),
        split_merge_evidence_refs=(),
        boundary_confidence=0.7,
        generator_version="episode-probe-v1",
        configuration_version="context-candidates-v1",
    )
    return ConversationContextCandidate.build(
        candidate_id=candidate_id,
        request_id=request.request_id,
        selected_items=tuple(items),
        topology_edge_ids=(("reply:101->102",) if extra_event else ()),
        embedded_episode_hypotheses=(hypothesis,),
        discourse_referents=(),
        layer_coverage=tuple(layers),
        omitted_lane_reasons={},
        cost=ContextCandidateCost(
            event_count=len(items),
            token_count=token_count,
            source_reads=1,
            model_calls=1,
            latency_ms=50,
        ),
        generator_version="context-candidates-v1",
        configuration_version="context-candidates-config-v1",
    )


def _probe(
    candidate: ConversationContextCandidate,
    *,
    semantic: str,
    completed: tuple[str, ...] = (
        "source_topology",
        "boundary_sensitivity",
    ),
    unresolved: tuple[str, ...] = (),
    perturbation: float = 0.02,
    incident_refs: tuple[str, ...] = (),
) -> ContextProbeEnvelope:
    return ContextProbeEnvelope(
        candidate_id=candidate.candidate_id,
        probe=ContextProbeResult(
            probe_id=f"probe:{candidate.candidate_id}",
            probe_version="context-light-parser-v1",
            tested_context_hash=candidate.candidate_content_hash,
            unresolved_dependency_refs=unresolved,
            alternative_interpretation_refs=(),
            perturbation_results={"adjacent_context": perturbation},
            future_or_authority_incident_refs=incident_refs,
            expected_value_of_expansion=0.4,
            cost_of_expansion=0.1,
        ),
        completed_probe_surfaces=completed,
        failed_probe_surfaces={},
        semantic_output_digest=canonical_sha256(semantic),
        contamination_score=0.01,
    )


def _command(
    *,
    request: InterpretationContextRequest,
    candidates: tuple[ConversationContextCandidate, ...],
    probes: tuple[ContextProbeEnvelope, ...],
    search_exhausted: bool = False,
) -> CommitInterpretationContextCommand:
    return CommitInterpretationContextCommand(
        context=AgencyWriteContext(
            command_id=uuid4(),
            tenant_id=request.tenant_id,
            processing_authority=request.processing_authority,
            writer_scope_epoch=WriterScopeEpoch(
                scope_id="legacy-grounding-annotation",
                tenant_id=request.tenant_id,
                semantic_responsibility="interpretation_context",
                source_partition="C-finance",
                writer_owner="GroundingAnnotationAppender",
                epoch=1,
                state=WriterCutoverState.LEGACY,
            ),
            idempotency_key="context:slack:focal:v1",
            issued_at=NOW - timedelta(seconds=1),
            expires_at=NOW + timedelta(minutes=30),
        ),
        proposed_snapshot_id=uuid4(),
        proposed_dependency_id=uuid4(),
        selection_subject="entity-mention:NBI",
        focal_observation_id=None,
        request=request,
        candidates=candidates,
        probes=probes,
        policy=ContextSelectionPolicy(
            policy_version="context-selection-v1",
            max_semantic_perturbation=0.1,
            max_contamination_score=0.1,
        ),
        expected=InterpretationContextHeadExpectation(
            expected_aggregate_version=0
        ),
        invalidation_keys=("slack:C-finance:102.1",),
        search_exhausted=search_exhausted,
        prepared_at=NOW,
    )


def test_selector_chooses_the_cheapest_probe_supported_context() -> None:
    extra = "slack:C-finance:100.1:v1"
    request = _request(allowed_ids=("slack:C-finance:102.1:v1", extra))
    focal = _candidate(request, candidate_id=uuid4())
    expanded = _candidate(request, candidate_id=uuid4(), extra_event=extra)
    command = _command(
        request=request,
        candidates=(expanded, focal),
        probes=(
            _probe(expanded, semantic="same interpretation"),
            _probe(focal, semantic="same interpretation"),
        ),
    )

    result = select_context(
        command,
        aggregate_version=1,
        snapshot_id=uuid4(),
        dependency_id=uuid4(),
        frozen_at=NOW,
    )

    assert result.disposition is SufficiencyDisposition.OPERATIONALLY_SUFFICIENT
    assert result.selected_candidate_ids == (focal.candidate_id,)
    assert len(result.snapshot.selected_items) == 1
    assert result.dependency.selected_event_revision_ids == (
        "slack:C-finance:102.1:v1",
    )


def test_close_but_semantically_distinct_contexts_preserve_alternatives() -> None:
    extra = "slack:C-finance:100.1:v1"
    request = _request(allowed_ids=("slack:C-finance:102.1:v1", extra))
    first = _candidate(request, candidate_id=uuid4(), token_count=100)
    second = _candidate(
        request,
        candidate_id=uuid4(),
        extra_event=extra,
        token_count=100,
    )
    policy_command = _command(
        request=request,
        candidates=(first, second),
        probes=(
            _probe(first, semantic="customer A"),
            _probe(second, semantic="customer B"),
        ),
    )
    payload = policy_command.model_dump(mode="python")
    payload["policy"] = {
        **policy_command.policy.model_dump(mode="python"),
        "multi_context_cost_tolerance": 1.0,
    }
    command = CommitInterpretationContextCommand.model_validate(payload)

    result = select_context(
        command,
        aggregate_version=1,
        snapshot_id=uuid4(),
        dependency_id=uuid4(),
        frozen_at=NOW,
    )

    assert result.disposition is SufficiencyDisposition.MULTI_CONTEXT
    assert set(result.selected_candidate_ids) == {first.candidate_id, second.candidate_id}
    assert {item.event_revision_id for item in result.snapshot.selected_items} == {
        "slack:C-finance:102.1:v1",
        extra,
    }


def test_missing_probe_surface_never_becomes_sufficient() -> None:
    request = _request(allowed_ids=("slack:C-finance:102.1:v1",))
    candidate = _candidate(request, candidate_id=uuid4())
    command = _command(
        request=request,
        candidates=(candidate,),
        probes=(
            _probe(
                candidate,
                semantic="not fully probed",
                completed=("source_topology",),
                unresolved=("referent:it",),
            ),
        ),
    )

    result = select_context(
        command,
        aggregate_version=1,
        snapshot_id=uuid4(),
        dependency_id=uuid4(),
        frozen_at=NOW,
    )

    assert result.disposition is SufficiencyDisposition.NEEDS_EXPANSION
    assert "referent:it" in result.snapshot.sufficiency_verdict.unresolved_references


def test_partial_selection_keeps_the_most_informative_safe_candidate() -> None:
    extra = "slack:C-finance:100.1:v1"
    request = _request(allowed_ids=("slack:C-finance:102.1:v1", extra))
    focal = _candidate(request, candidate_id=uuid4())
    expanded = _candidate(request, candidate_id=uuid4(), extra_event=extra)
    command = _command(
        request=request,
        candidates=(focal, expanded),
        probes=(
            _probe(
                focal,
                semantic="unresolved focal",
                completed=("source_topology",),
                unresolved=("referent:it",),
            ),
            _probe(
                expanded,
                semantic="unresolved with topology",
                completed=("source_topology",),
                unresolved=("referent:it",),
            ),
        ),
    )

    result = select_context(
        command,
        aggregate_version=1,
        snapshot_id=uuid4(),
        dependency_id=uuid4(),
        frozen_at=NOW,
    )

    assert result.disposition is SufficiencyDisposition.NEEDS_EXPANSION
    assert result.selected_candidate_ids == (expanded.candidate_id,)
    assert result.dependency.selected_event_revision_ids == (
        "slack:C-finance:102.1:v1",
        extra,
    )


def test_search_exhaustion_is_an_explicit_partial_fate() -> None:
    request = _request(allowed_ids=("slack:C-finance:102.1:v1",))
    candidate = _candidate(request, candidate_id=uuid4())
    command = _command(
        request=request,
        candidates=(candidate,),
        probes=(
            _probe(
                candidate,
                semantic="not identifiable",
                completed=(),
                unresolved=("referent:it",),
            ),
        ),
        search_exhausted=True,
    )

    result = select_context(
        command,
        aggregate_version=1,
        snapshot_id=uuid4(),
        dependency_id=uuid4(),
        frozen_at=NOW,
    )

    assert result.disposition is SufficiencyDisposition.BUDGET_EXHAUSTED
    assert result.snapshot.sufficiency_verdict.unresolved_references


def test_future_or_unauthorized_event_is_rejected_before_selection() -> None:
    request = _request(allowed_ids=("slack:C-finance:102.1:v1",))
    focal = _candidate(request, candidate_id=uuid4())
    payload = focal.model_dump(mode="python", exclude={"candidate_content_hash"})
    payload["selected_items"] = (
        *payload["selected_items"],
        _item(
            "slack:C-finance:future:v1",
            emitted_at=NOW + timedelta(seconds=1),
            layer=CandidateContextLayer.TEMPORAL,
        ),
    )
    payload["cost"] = {**focal.cost.model_dump(), "event_count": 2}
    future = ConversationContextCandidate.build(**payload)

    with pytest.raises(ValidationError, match="future evidence"):
        _command(
            request=request,
            candidates=(future,),
            probes=(_probe(future, semantic="leaked"),),
        )


def test_probe_authority_incident_cannot_be_selected() -> None:
    request = _request(allowed_ids=("slack:C-finance:102.1:v1",))
    candidate = _candidate(request, candidate_id=uuid4())
    command = _command(
        request=request,
        candidates=(candidate,),
        probes=(
            _probe(
                candidate,
                semantic="forbidden influence",
                incident_refs=("authority:private-channel-feature",),
            ),
        ),
    )

    with pytest.raises(ValueError, match="no context candidate is safe"):
        select_context(
            command,
            aggregate_version=1,
            snapshot_id=uuid4(),
            dependency_id=uuid4(),
            frozen_at=NOW,
        )
