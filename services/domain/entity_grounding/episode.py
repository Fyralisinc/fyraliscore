"""Construct the immutable context -> assessment -> admission episode.

This module is deliberately pure.  It converts source-structured resolver
inputs into the shared C0b contracts, enforces a closed candidate population,
and makes the consumer fate explicit.  It has no authority to mutate an alias,
referent, Observation, or any other company-physics aggregate.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from lib.contracts.semantic_commands import SemanticWriteContext
from lib.contracts.conversation_context import (
    CommitInterpretationContextCommand,
    ContextCandidateCost,
    ContextProbeEnvelope,
    ContextSelectionOutcome,
    ContextSelectionPolicy,
    ConversationContextCandidate,
    InterpretationContextHeadExpectation,
)
from lib.contracts.entity_mentions import (
    CommitEntityMentionDetectionCommand,
    EntityMentionDetectionFate,
)
from lib.contracts.kernel import (
    CommandResult,
    CommandResultStatus,
    CommittedAggregateVersion,
    ConsumptionAuthorityContext,
    ProcessingAuthorityContext,
    RestrictionSet,
    WriterCutoverState,
    WriterScopeEpoch,
    canonical_sha256,
)
from lib.contracts.perception import (
    CandidateContextLayer,
    CandidateGenerationBudget,
    CandidateLaneFate,
    CandidateLaneFateKind,
    CandidateLaneReasonClass,
    ContextBudget,
    ContextProbeResult,
    ContextRiskTier,
    ConversationEpisodeHypothesis,
    EntityCandidate,
    EntityCandidateGenerationRequest,
    EntityCandidateKind,
    EntityCandidateSet,
    GroundingAdmissionDecision,
    GroundingAdmissionDisposition,
    InterpretationContextRequest,
    InterpretationContextSnapshot,
    InterpretationMode,
    ReferentVersionRef,
    ResolutionAssessment,
    SelectedContextItem,
    SelectionDependency,
    SufficiencyDisposition,
)
from lib.conversation_context_selection import select_context
from lib.shared.ids import uuid7


_CONTEXT_POLICY_VERSION = "resolver-context-policy-v2"
_CANDIDATE_POLICY_VERSION = "bounded-candidate-generation-v1"
_SCORER_VERSION = "closed-set-llm-assessment-uncalibrated-v1"
_ADMISSION_POLICY_VERSION = "observation-grounding-admission-v1"
_SPECIAL_NONE = "candidate:none-of-the-above"
_SPECIAL_NOVEL = "candidate:novel-referent"
_SPECIAL_UNKNOWN = "candidate:unknown"


@dataclass(frozen=True)
class ContextObservationInput:
    observation_id: UUID
    occurred_at: datetime
    source_channel: str
    source_space: str
    inclusion_layer: str
    inclusion_reasons: tuple[str, ...]


@dataclass(frozen=True)
class GroundingCandidateInput:
    canonical_ref: dict[str, Any]
    candidate_source: str
    positive_evidence_refs: tuple[str, ...]
    negative_evidence_refs: tuple[str, ...] = ()
    independent_identity_evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class GroundingEpisode:
    context_selection_command: CommitInterpretationContextCommand
    context_selection_outcome: ContextSelectionOutcome
    mention_detection_command: CommitEntityMentionDetectionCommand
    context_snapshot: InterpretationContextSnapshot
    selection_dependency: SelectionDependency
    candidate_set: EntityCandidateSet
    assessment: ResolutionAssessment
    admission: GroundingAdmissionDecision
    current_fate: str
    selected_candidate_id: str | None
    assessed_canonical_ref: dict[str, Any] | None
    admitted_canonical_ref: dict[str, Any] | None
    model_output: dict[str, Any]


def candidate_id_for_ref(canonical_ref: dict[str, Any]) -> str:
    """Stable closed-set identifier for a tenant-local canonical ref."""

    return f"candidate:canonical:{canonical_sha256(canonical_ref)}"


def build_grounding_episode(
    *,
    tenant_id: UUID,
    observation_id: UUID,
    phrase: str,
    occurred_at: datetime,
    source_channel: str,
    source_space: str,
    topology_incomplete: bool,
    boundary_hypotheses: tuple[dict[str, Any], ...],
    context_observations: tuple[ContextObservationInput, ...],
    selection_dependency_refs: tuple[str, ...],
    candidates: tuple[GroundingCandidateInput, ...],
    model_candidate_id: str | None,
    model_canonical_ref: dict[str, Any] | None,
    model_confidence: float,
    model_reasoning: str,
    high_confidence: float,
    review_min: float,
    prepared_context_command: CommitInterpretationContextCommand,
    prepared_context_outcome: ContextSelectionOutcome,
    prepared_mention_detection_command: CommitEntityMentionDetectionCommand,
    now: datetime | None = None,
) -> GroundingEpisode:
    """Build one fully linked, evidence-relative grounding episode.

    The model may select only an item in ``candidates``.  A bare or invented
    canonical ref is retained in the model-output audit field but maps to an
    abstaining fate and can never escape into selected identity state.
    """

    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if occurred_at.tzinfo is None or occurred_at.utcoffset() is None:
        raise ValueError("occurred_at must be timezone-aware")
    if not 0.0 <= model_confidence <= 1.0:
        raise ValueError("model_confidence must lie in [0, 1]")

    snapshot = prepared_context_outcome.snapshot
    dependency = prepared_context_outcome.dependency
    _validate_detected_mention_binding(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase=phrase,
        context_command=prepared_context_command,
        context_outcome=prepared_context_outcome,
        mention_command=prepared_mention_detection_command,
    )
    mention = prepared_mention_detection_command.detection.mention
    if mention is None:  # Defensive: validation above proves this is unreachable.
        raise ValueError("candidate processing requires a detected EntityMention")
    authority = prepared_context_command.request.processing_authority
    candidate_set = _build_candidate_set(
        tenant_id=tenant_id,
        observation_id=observation_id,
        mention_id=mention.mention_id,
        mention_version=mention.mention_version,
        snapshot=snapshot,
        authority=authority,
        candidates=candidates,
        now=now,
    )
    selected_candidate = _select_candidate(
        candidate_set=candidate_set,
        model_candidate_id=model_candidate_id,
        model_canonical_ref=model_canonical_ref,
    )
    model_output = {
        "candidate_id": model_candidate_id,
        "canonical_ref": model_canonical_ref,
        "confidence": model_confidence,
        "reasoning": model_reasoning,
        "closed_set_match": selected_candidate is not None,
    }
    assessment = _build_assessment(
        candidate_set=candidate_set,
        selected_candidate=selected_candidate,
        candidate_inputs=candidates,
        model_canonical_ref=model_canonical_ref,
        model_confidence=model_confidence,
        now=now,
    )
    admission, current_fate = _build_admission(
        tenant_id=tenant_id,
        observation_id=observation_id,
        source_channel=source_channel,
        assessment=assessment,
        context_disposition=snapshot.sufficiency_verdict.disposition,
        selected_candidate=selected_candidate,
        has_independent_identity_evidence=bool(
            _independent_evidence_for_candidate(
                selected_candidate=selected_candidate,
                candidate_inputs=candidates,
            )
        ),
        model_canonical_ref=model_canonical_ref,
        confidence=model_confidence,
        high_confidence=high_confidence,
        review_min=review_min,
        now=now,
    )
    assessed_ref = None
    if selected_candidate is not None:
        assessed_ref = {
            "type": selected_candidate.candidate_type,
            "id": selected_candidate.canonical_referent_id,
            "version": selected_candidate.canonical_referent_version,
        }
    return GroundingEpisode(
        context_selection_command=prepared_context_command,
        context_selection_outcome=prepared_context_outcome,
        mention_detection_command=prepared_mention_detection_command,
        context_snapshot=snapshot,
        selection_dependency=dependency,
        candidate_set=candidate_set,
        assessment=assessment,
        admission=admission,
        current_fate=current_fate,
        selected_candidate_id=(
            selected_candidate.candidate_id if selected_candidate else None
        ),
        assessed_canonical_ref=assessed_ref,
        admitted_canonical_ref=(
            assessed_ref if admission.selected_referent is not None else None
        ),
        model_output=model_output,
    )


def prepare_context_selection(
    *,
    tenant_id: UUID,
    observation_id: UUID,
    phrase: str,
    occurred_at: datetime,
    source_channel: str,
    source_space: str,
    topology_incomplete: bool,
    boundary_hypotheses: tuple[dict[str, Any], ...],
    context_observations: tuple[ContextObservationInput, ...],
    selection_dependency_refs: tuple[str, ...],
    now: datetime,
) -> tuple[CommitInterpretationContextCommand, ContextSelectionOutcome]:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if occurred_at.tzinfo is None or occurred_at.utcoffset() is None:
        raise ValueError("occurred_at must be timezone-aware")
    focal_revision_id = f"observation:{observation_id}:v1"
    eligible_context = tuple(
        item
        for item in context_observations
        if item.observation_id != observation_id and item.occurred_at <= occurred_at
    )
    authority = _processing_authority(
        tenant_id=tenant_id,
        focal_revision_id=focal_revision_id,
        source_channel=source_channel,
        context_observations=eligible_context,
        now=now,
    )
    source_spaces = tuple(
        sorted({source_space, *(item.source_space for item in eligible_context)})
    )
    request = InterpretationContextRequest(
        request_id=str(uuid7()),
        tenant_id=tenant_id,
        focal_event_revision_ids=(focal_revision_id,),
        mode=InterpretationMode.AS_KNOWN_AT_CUTOFF,
        effective_query_time=now,
        evidence_cutoff=occurred_at,
        knowledge_cutoff=max(now, occurred_at),
        source_topology_version="observation-source-topology-v1",
        processing_authority=authority,
        allowed_source_spaces=RestrictionSet.only(*source_spaces),
        risk_tier=ContextRiskTier.MEDIUM,
        required_probe_surfaces=(
            "source_topology",
            "temporal_alternatives",
            "boundary_sensitivity",
        ),
        budget=ContextBudget(
            max_events=20,
            max_topology_hops=2,
            max_source_reads=3,
            max_model_calls=1,
            max_tokens=2048,
            max_latency_ms=10_000,
        ),
        policy_versions=(_CONTEXT_POLICY_VERSION,),
        self_contained_source=source_channel != "slack:message",
    )
    focal = SelectedContextItem(
        event_revision_id=focal_revision_id,
        source_space=source_space,
        emitted_at=occurred_at,
        layer=CandidateContextLayer.FOCAL,
        inclusion_reasons=("focal source observation",),
        source_version=focal_revision_id,
        authority_label=source_channel,
        relation_to_focal="focal",
    )
    context_items = tuple(
        SelectedContextItem(
            event_revision_id=f"observation:{item.observation_id}:v1",
            source_space=item.source_space,
            emitted_at=item.occurred_at,
            layer=_context_layer(item.inclusion_layer),
            inclusion_reasons=item.inclusion_reasons,
            source_version=f"observation:{item.observation_id}:v1",
            authority_label=item.source_channel,
            relation_to_focal=item.inclusion_layer,
        )
        for item in eligible_context
    )
    groups: list[tuple[SelectedContextItem, ...]] = [(focal,)]
    for layer in (
        CandidateContextLayer.SOURCE_TOPOLOGY,
        CandidateContextLayer.TEMPORAL,
    ):
        items = tuple(item for item in context_items if item.layer is layer)
        if items:
            groups.append((focal, *items))
    if context_items:
        groups.append((focal, *context_items))
    unique_groups: list[tuple[SelectedContextItem, ...]] = []
    seen_groups: set[tuple[str, ...]] = set()
    for group in groups:
        identity = tuple(item.event_revision_id for item in group)
        if identity not in seen_groups:
            seen_groups.add(identity)
            unique_groups.append(group)

    alternatives = tuple(
        json.dumps(item, sort_keys=True, separators=(",", ":"))
        for item in boundary_hypotheses
    ) or ("same-source-space temporal boundary remains provisional",)
    context_dependent = _phrase_requires_context(phrase)
    candidates: list[ConversationContextCandidate] = []
    probes: list[ContextProbeEnvelope] = []
    has_topology_basis = any(
        item.get("kind") == "source_topology" for item in boundary_hypotheses
    )
    has_temporal_basis = any(
        item.get("kind") == "same_source_space_temporal"
        for item in boundary_hypotheses
    )
    for selected in unique_groups:
        layer_coverage = tuple(dict.fromkeys(item.layer for item in selected))
        hypotheses: tuple[ConversationEpisodeHypothesis, ...] = ()
        if source_channel == "slack:message":
            hypotheses = (
                ConversationEpisodeHypothesis.build(
                    membership_weights={
                        item.event_revision_id: 1.0 for item in selected
                    },
                    boundary_alternatives=alternatives,
                    topic_state=f"temporary context for {phrase}",
                    continuity_evidence_refs=tuple(
                        item.event_revision_id for item in selected[1:]
                    ),
                    split_merge_evidence_refs=(),
                    boundary_confidence=0.35 if topology_incomplete else 0.7,
                    generator_version="slack-boundary-hypothesis-v2",
                    configuration_version=_CONTEXT_POLICY_VERSION,
                ),
            )
        omitted: dict[str, str] = {}
        if (
            source_channel == "slack:message"
            and CandidateContextLayer.SOURCE_TOPOLOGY not in layer_coverage
        ):
            omitted["source_topology"] = "not included in this candidate"
        if (
            source_channel == "slack:message"
            and CandidateContextLayer.TEMPORAL not in layer_coverage
        ):
            omitted["temporal_alternatives"] = "not included in this candidate"
        candidate = ConversationContextCandidate.build(
            candidate_id=uuid7(),
            request_id=request.request_id,
            selected_items=selected,
            topology_edge_ids=(),
            embedded_episode_hypotheses=hypotheses,
            discourse_referents=(),
            layer_coverage=layer_coverage,
            omitted_lane_reasons=omitted,
            cost=ContextCandidateCost(
                event_count=len(selected),
                token_count=128 * len(selected),
                source_reads=min(3, len(selected)),
                model_calls=1,
                latency_ms=50 + 10 * len(selected),
            ),
            generator_version="resolver-context-candidates-v2",
            configuration_version=_CONTEXT_POLICY_VERSION,
        )
        completed: list[str] = []
        if source_channel != "slack:message" or not context_dependent or (
            not topology_incomplete
            and (
                CandidateContextLayer.SOURCE_TOPOLOGY in layer_coverage
                or has_topology_basis
            )
        ):
            completed.append("source_topology")
        if source_channel != "slack:message" or not context_dependent or (
            CandidateContextLayer.TEMPORAL in layer_coverage or has_temporal_basis
        ):
            completed.append("temporal_alternatives")
        if source_channel != "slack:message" or not context_dependent:
            completed.append("boundary_sensitivity")
        probe = ContextProbeResult(
            probe_id=f"context-light-probe:{candidate.candidate_id}",
            probe_version="resolver-context-light-probe-v1",
            tested_context_hash=candidate.candidate_content_hash,
            unresolved_dependency_refs=((phrase,) if context_dependent else ()),
            alternative_interpretation_refs=(),
            perturbation_results={
                "boundary_substitution": 1.0 if context_dependent else 0.0
            },
            future_or_authority_incident_refs=(),
            expected_value_of_expansion=0.8 if context_dependent else 0.0,
            cost_of_expansion=0.2,
        )
        probes.append(
            ContextProbeEnvelope(
                candidate_id=candidate.candidate_id,
                probe=probe,
                completed_probe_surfaces=tuple(completed),
                failed_probe_surfaces={},
                semantic_output_digest=canonical_sha256(
                    {
                        "phrase": phrase.casefold(),
                        "context_dependent": context_dependent,
                    }
                ),
                contamination_score=min(1.0, 0.02 * (len(selected) - 1)),
            )
        )
        candidates.append(candidate)

    snapshot_id = uuid7()
    dependency_id = uuid7()
    context = SemanticWriteContext(
        command_id=uuid7(),
        tenant_id=tenant_id,
        processing_authority=authority,
        writer_scope_epoch=WriterScopeEpoch(
            scope_id="legacy-grounding-annotation",
            tenant_id=tenant_id,
            semantic_responsibility="interpretation_context",
            source_partition=source_space,
            writer_owner="GroundingAnnotationAppender",
            epoch=1,
            state=WriterCutoverState.LEGACY,
        ),
        idempotency_key=(
            f"context:{observation_id}:{canonical_sha256(phrase)}:v1"
        ),
        issued_at=now - timedelta(microseconds=1),
        expires_at=now + timedelta(hours=1),
    )
    command = CommitInterpretationContextCommand(
        context=context,
        proposed_snapshot_id=snapshot_id,
        proposed_dependency_id=dependency_id,
        selection_subject=f"entity-mention:{phrase}",
        focal_observation_id=observation_id,
        request=request,
        candidates=tuple(candidates),
        probes=tuple(probes),
        policy=ContextSelectionPolicy(
            policy_version="resolver-context-selection-v2",
            max_semantic_perturbation=0.1,
            max_contamination_score=0.2,
        ),
        expected=InterpretationContextHeadExpectation(
            expected_aggregate_version=0
        ),
        invalidation_keys=selection_dependency_refs
        or (f"observation:{observation_id}",),
        prepared_at=now,
    )
    outcome = select_context(
        command,
        aggregate_version=1,
        snapshot_id=snapshot_id,
        dependency_id=dependency_id,
        frozen_at=now,
    )
    return command, outcome


def _processing_authority(
    *,
    tenant_id: UUID,
    focal_revision_id: str,
    source_channel: str,
    context_observations: tuple[ContextObservationInput, ...],
    now: datetime,
) -> ProcessingAuthorityContext:
    event_ids = (
        focal_revision_id,
        *(
            f"observation:{item.observation_id}:v1"
            for item in context_observations
        ),
    )
    source_labels = tuple(
        sorted({source_channel, *(item.source_channel for item in context_observations)})
    )
    return ProcessingAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id="service:entity-resolver",
        purpose="company-physics-grounding",
        operation="construct-grounding-assessment",
        object_types=RestrictionSet.only(
            "observation", "conversation_event_revision", "entity_alias"
        ),
        object_ids=RestrictionSet.only(*event_ids),
        fields=RestrictionSet.only(
            "content",
            "content_text",
            "occurred_at",
            "entities_mentioned",
            "resolved_entity_ref",
        ),
        source_labels=RestrictionSet.only(*source_labels),
        authority_basis_refs=frozenset({"service-policy:entity-resolver-v1"}),
        policy_version="entity-resolver-processing-authority-v2",
        authority_epoch=1,
        decision_time=now - timedelta(microseconds=1),
        expires_at=now + timedelta(hours=1),
    )


def _phrase_requires_context(phrase: str) -> bool:
    normalized = " ".join(phrase.casefold().split())
    tokens = {
        token.strip(".,!?;:()[]{}\"'")
        for token in normalized.split()
    }
    context_words = {
        "it",
        "this",
        "that",
        "these",
        "those",
        "they",
        "them",
        "he",
        "she",
        "we",
        "same",
        "again",
        "here",
        "there",
        "above",
        "former",
        "latter",
    }
    return bool(tokens & context_words) or normalized.startswith("the ")


def _context_layer(value: str) -> CandidateContextLayer:
    if value == "source_topology":
        return CandidateContextLayer.SOURCE_TOPOLOGY
    if value == "temporal_candidate":
        return CandidateContextLayer.TEMPORAL
    return CandidateContextLayer.DISCOURSE


def _build_candidate_set(
    *,
    tenant_id: UUID,
    observation_id: UUID,
    mention_id: str,
    mention_version: int,
    snapshot: InterpretationContextSnapshot,
    authority: ProcessingAuthorityContext,
    candidates: tuple[GroundingCandidateInput, ...],
    now: datetime,
) -> EntityCandidateSet:
    deduped: dict[str, GroundingCandidateInput] = {}
    for item in candidates:
        ref = item.canonical_ref
        if not isinstance(ref.get("id"), str) or not ref.get("id"):
            continue
        if not isinstance(ref.get("type"), str) or not ref.get("type"):
            continue
        deduped.setdefault(candidate_id_for_ref(ref), item)
    allowed_sources = tuple(sorted({item.candidate_source for item in deduped.values()}))
    allowed_types = tuple(
        sorted({str(item.canonical_ref["type"]) for item in deduped.values()})
    )
    request = EntityCandidateGenerationRequest.build(
        request_id=str(uuid7()),
        tenant_id=tenant_id,
        mention_ref=f"mention:{mention_id}:v{mention_version}",
        mention_version=mention_version,
        entity_type_assessment_refs=(),
        local_role_binding_refs=(),
        context_snapshot_ref=snapshot.snapshot_id,
        registry_as_of_cutoff=now,
        processing_authority_fingerprint=authority.fingerprint,
        permitted_candidate_sources=RestrictionSet.only(*allowed_sources),
        permitted_candidate_types=RestrictionSet.only(*allowed_types),
        required_retrieval_lanes=("source_mentions", "tenant_aliases"),
        generator_version="entity-resolver-bounded-candidates-v1",
        index_versions=("entity-aliases-normalized-v1",),
        model_versions=(),
        configuration_version=_CANDIDATE_POLICY_VERSION,
        budget=CandidateGenerationBudget(
            max_candidates=33,
            max_source_reads=2,
            max_index_queries=1,
            max_model_calls=1,
            max_latency_ms=10_000,
        ),
    )
    contract_candidates = [
        EntityCandidate(
            candidate_id=candidate_id,
            kind=EntityCandidateKind.CANONICAL_REFERENT,
            canonical_referent_id=str(item.canonical_ref["id"]),
            canonical_referent_version=int(item.canonical_ref.get("version", 1)),
            candidate_source=item.candidate_source,
            candidate_type=str(item.canonical_ref["type"]),
            authorized_positive_evidence_refs=item.positive_evidence_refs,
            authorized_negative_evidence_refs=item.negative_evidence_refs,
        )
        for candidate_id, item in sorted(deduped.items())
    ]
    contract_candidates.extend(
        (
            EntityCandidate(
                candidate_id=_SPECIAL_NONE,
                kind=EntityCandidateKind.NONE_OF_THE_ABOVE,
                authorized_positive_evidence_refs=(),
                authorized_negative_evidence_refs=(),
            ),
            EntityCandidate(
                candidate_id=_SPECIAL_NOVEL,
                kind=EntityCandidateKind.NOVEL_REFERENT,
                authorized_positive_evidence_refs=(),
                authorized_negative_evidence_refs=(),
            ),
            EntityCandidate(
                candidate_id=_SPECIAL_UNKNOWN,
                kind=EntityCandidateKind.UNKNOWN,
                authorized_positive_evidence_refs=(),
                authorized_negative_evidence_refs=(),
            ),
        )
    )
    request_digest = request.generation_request_digest
    result_id = str(uuid7())
    set_id = str(uuid7())
    return EntityCandidateSet(
        candidate_set_id=set_id,
        candidate_set_version=1,
        request=request,
        command_result=CommandResult(
            result_id=result_id,
            command_id=request.request_id,
            canonical_request_hash=request_digest,
            writer_scope_id="entity-candidate-generator",
            writer_epoch=1,
            status=CommandResultStatus.APPLIED,
            committed_aggregate_versions=(
                CommittedAggregateVersion(
                    semantic_responsibility="entity-candidate-set",
                    aggregate_id=set_id,
                    committed_version=1,
                ),
            ),
            event_ids=(f"candidate-set-committed:{set_id}",),
        ),
        lane_fates=(
            CandidateLaneFate(
                lane_id="source_mentions",
                fate=CandidateLaneFateKind.COMPLETE,
                reason_class=CandidateLaneReasonClass.COMPLETED,
                artifact_refs=(f"observation:{observation_id}:entities-mentioned",),
            ),
            CandidateLaneFate(
                lane_id="tenant_aliases",
                fate=CandidateLaneFateKind.COMPLETE,
                reason_class=CandidateLaneReasonClass.COMPLETED,
                artifact_refs=("entity-aliases:tenant-scoped-snapshot",),
            ),
        ),
        candidates=tuple(contract_candidates),
        registry_version="legacy-company-object-refs-v1",
        expires_at=now + timedelta(days=1),
    )


def _validate_detected_mention_binding(
    *,
    tenant_id: UUID,
    observation_id: UUID,
    phrase: str,
    context_command: CommitInterpretationContextCommand,
    context_outcome: ContextSelectionOutcome,
    mention_command: CommitEntityMentionDetectionCommand,
) -> None:
    """Require candidate processing to start from one exact durable mention."""

    detection = mention_command.detection
    snapshot = context_outcome.snapshot
    if detection.fate is not EntityMentionDetectionFate.DETECTED:
        raise ValueError("candidate processing requires a detected mention fate")
    if detection.mention is None:
        raise ValueError("candidate processing requires a durable EntityMention")
    if detection.tenant_id != tenant_id:
        raise ValueError("mention detection tenant differs from grounding tenant")
    if detection.source_observation_id != observation_id:
        raise ValueError("mention detection source observation differs from grounding source")
    if context_command.focal_observation_id != observation_id:
        raise ValueError("context command source observation differs from grounding source")
    if detection.candidate_surface != phrase:
        raise ValueError("mention detection phrase differs from grounding phrase")
    if detection.context_snapshot_id != UUID(snapshot.snapshot_id):
        raise ValueError("mention detection binds a different context snapshot")
    if detection.context_snapshot_digest != snapshot.snapshot_content_hash:
        raise ValueError("mention detection binds a different context snapshot digest")
    if detection.source_revision_id not in context_command.request.focal_event_revision_ids:
        raise ValueError("mention detection source revision is not a focal context revision")
    if (
        mention_command.context.processing_authority.fingerprint
        != context_command.request.processing_authority.fingerprint
    ):
        raise ValueError("mention detection uses a different processing authority")


def _select_candidate(
    *,
    candidate_set: EntityCandidateSet,
    model_candidate_id: str | None,
    model_canonical_ref: dict[str, Any] | None,
) -> EntityCandidate | None:
    canonical = [
        item
        for item in candidate_set.candidates
        if item.kind is EntityCandidateKind.CANONICAL_REFERENT
    ]
    if model_candidate_id:
        for item in canonical:
            if item.candidate_id == model_candidate_id:
                return item
        return None
    if model_canonical_ref:
        wanted = canonical_sha256(model_canonical_ref)
        for item in canonical:
            materialized = {
                "type": item.candidate_type,
                "id": item.canonical_referent_id,
            }
            if model_canonical_ref.get("version") is not None:
                materialized["version"] = item.canonical_referent_version
            if canonical_sha256(materialized) == wanted:
                return item
    return None


def _build_assessment(
    *,
    candidate_set: EntityCandidateSet,
    selected_candidate: EntityCandidate | None,
    candidate_inputs: tuple[GroundingCandidateInput, ...],
    model_canonical_ref: dict[str, Any] | None,
    model_confidence: float,
    now: datetime,
) -> ResolutionAssessment:
    ids = [item.candidate_id for item in candidate_set.candidates]
    distribution = {candidate_id: 0.0 for candidate_id in ids}
    if selected_candidate is not None:
        primary = selected_candidate.candidate_id
        primary_probability = model_confidence
    elif model_canonical_ref is None:
        primary = _SPECIAL_NONE
        primary_probability = model_confidence
    else:
        primary = _SPECIAL_UNKNOWN
        primary_probability = 1.0
    distribution[primary] = primary_probability
    others = [candidate_id for candidate_id in ids if candidate_id != primary]
    residual = 1.0 - primary_probability
    if others:
        share = residual / len(others)
        for candidate_id in others:
            distribution[candidate_id] = share
    evidence_refs = _independent_evidence_for_candidate(
        selected_candidate=selected_candidate,
        candidate_inputs=candidate_inputs,
    )
    return ResolutionAssessment(
        assessment_id=str(uuid7()),
        assessment_version=1,
        candidate_set=candidate_set,
        candidate_distribution=distribution,
        identity_evidence_refs=evidence_refs,
        evidence_dependence_groups={
            ref: "independently_governed_identity_basis" for ref in evidence_refs
        },
        decisive_evidence_refs=(
            evidence_refs if selected_candidate is not None and model_confidence >= 0.8 else ()
        ),
        missing_discriminators=(
            ()
            if (
                selected_candidate is not None
                and model_confidence >= 0.8
                and evidence_refs
            )
            else ("independent identity discriminator",)
        ),
        temporal_compatibility_refs=(),
        calibration_cohort="legacy-unstructured-phrase-resolution",
        scorer_and_calibration_version=_SCORER_VERSION,
        assessed_at=now,
        expires_at=min(candidate_set.expires_at, now + timedelta(hours=12)),
    )


def _build_admission(
    *,
    tenant_id: UUID,
    observation_id: UUID,
    source_channel: str,
    assessment: ResolutionAssessment,
    context_disposition: SufficiencyDisposition,
    selected_candidate: EntityCandidate | None,
    has_independent_identity_evidence: bool,
    model_canonical_ref: dict[str, Any] | None,
    confidence: float,
    high_confidence: float,
    review_min: float,
    now: datetime,
) -> tuple[GroundingAdmissionDecision, str]:
    selected: ReferentVersionRef | None = None
    if (
        selected_candidate is not None
        and confidence > high_confidence
        and has_independent_identity_evidence
        and context_disposition is SufficiencyDisposition.OPERATIONALLY_SUFFICIENT
    ):
        disposition = GroundingAdmissionDisposition.SINGLE_REFERENT
        selected = ReferentVersionRef(
            referent_id=selected_candidate.canonical_referent_id or "",
            referent_version=selected_candidate.canonical_referent_version or 1,
        )
        reasons = ("closed_set_candidate_above_consumer_risk_threshold",)
        fate = "resolved_for_consumer"
    elif selected_candidate is not None and confidence >= review_min:
        disposition = GroundingAdmissionDisposition.REVIEW
        reasons = (
            (
                f"context_not_operationally_sufficient:{context_disposition.value}"
                if context_disposition
                is not SufficiencyDisposition.OPERATIONALLY_SUFFICIENT
                else (
                    "material_candidate_requires_human_discriminator"
                    if has_independent_identity_evidence
                    else "independent_identity_evidence_required"
                )
            ),
        )
        fate = "review"
    elif model_canonical_ref is not None and selected_candidate is None:
        disposition = GroundingAdmissionDisposition.ABSTENTION
        reasons = ("model_output_outside_authorized_candidate_set",)
        fate = "abstained"
    else:
        disposition = GroundingAdmissionDisposition.MENTION_LOCAL_ONLY
        reasons = ("identity_unresolved_preserve_local_evidence",)
        fate = "unresolved"

    authority = ConsumptionAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id="service:observation-grounding-consumer",
        purpose="company-physics-grounding",
        operation="consume-resolution-assessment",
        object_types=RestrictionSet.only("grounding_assessment"),
        object_ids=RestrictionSet.only(assessment.assessment_id, str(observation_id)),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only(source_channel),
        authority_basis_refs=frozenset({"service-policy:grounding-consumer-v1"}),
        policy_version=_ADMISSION_POLICY_VERSION,
        authority_epoch=1,
        decision_time=now - timedelta(microseconds=1),
        expires_at=assessment.expires_at,
    )
    return (
        GroundingAdmissionDecision(
            decision_id=str(uuid7()),
            decision_version=1,
            assessment=assessment,
            consumer="observation-grounding-sidecar",
            purpose=authority.purpose,
            operation=authority.operation,
            risk_tier="medium",
            blast_radius="tenant-local-derived-consumers",
            expected_loss=max(0.0, 1.0 - confidence),
            consumption_authority=authority,
            consumer_supports_distributions=False,
            disposition=disposition,
            selected_referent=selected,
            permitted_distribution={},
            genuine_source_binding=None,
            reason_codes=reasons,
            decided_at=now,
            expires_at=assessment.expires_at,
        ),
        fate,
    )


def _independent_evidence_for_candidate(
    *,
    selected_candidate: EntityCandidate | None,
    candidate_inputs: tuple[GroundingCandidateInput, ...],
) -> tuple[str, ...]:
    if selected_candidate is None:
        return ()
    refs: list[str] = []
    seen: set[str] = set()
    for item in candidate_inputs:
        if candidate_id_for_ref(item.canonical_ref) != selected_candidate.candidate_id:
            continue
        for ref in item.independent_identity_evidence_refs:
            if ref not in seen:
                seen.add(ref)
                refs.append(ref)
    return tuple(refs)


__all__ = [
    "ContextObservationInput",
    "GroundingCandidateInput",
    "GroundingEpisode",
    "build_grounding_episode",
    "candidate_id_for_ref",
    "prepare_context_selection",
]
