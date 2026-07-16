"""C0b evidence, conversational reconstruction, and entity-grounding contracts."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Self
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)

from lib.contracts.kernel import (
    BitemporalInterval,
    CommandResult,
    CommandResultStatus,
    ConsumptionAuthorityContext,
    ProcessingAuthorityContext,
    RestrictionSet,
    canonical_sha256,
)


class _PerceptionContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        use_enum_values=False,
    )


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _probabilities_are_normalized(distribution: dict[str, float], *, name: str) -> None:
    if not distribution:
        raise ValueError(f"{name} cannot be empty")
    if any(value < 0.0 or value > 1.0 for value in distribution.values()):
        raise ValueError(f"{name} values must lie in [0, 1]")
    if abs(sum(distribution.values()) - 1.0) > 1e-6:
        raise ValueError(f"{name} must sum to one")


def _normalize_build_values(model_type, values: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for name, value in values.items():
        field = model_type.model_fields.get(name)
        if field is None:
            normalized[name] = value
        else:
            normalized[name] = TypeAdapter(field.annotation).validate_python(value)
    return normalized


class SourceRetentionFate(StrEnum):
    PAYLOAD_AVAILABLE = "payload_available"
    LEGALLY_REDACTED_TOMBSTONE = "legally_redacted_tombstone"
    HISTORY_UNAVAILABLE = "history_unavailable"


class ConversationEventKind(StrEnum):
    MESSAGE = "message"
    EDIT = "edit"
    DELETION = "deletion"
    REACTION = "reaction"
    ATTACHMENT = "attachment"
    REPLY = "reply"


class EvidenceCoordinate(_PerceptionContract):
    evidence_record_id: str = Field(min_length=1)
    source_system: str = Field(min_length=1)
    source_object_id: str = Field(min_length=1)
    source_revision: str = Field(min_length=1)
    field_path: str | None = None
    span_start: int | None = Field(default=None, ge=0)
    span_end: int | None = Field(default=None, ge=0)
    time_range_start: datetime | None = None
    time_range_end: datetime | None = None

    @field_validator("time_range_start", "time_range_end")
    @classmethod
    def coordinate_times_are_aware(cls, value: datetime | None, info) -> datetime | None:
        return _aware(value, field_name=info.field_name) if value else None

    @model_validator(mode="after")
    def span_and_time_ranges_are_complete(self) -> Self:
        if (self.span_start is None) != (self.span_end is None):
            raise ValueError("text coordinates require both span_start and span_end")
        if self.span_start is not None and self.span_end <= self.span_start:
            raise ValueError("span_end must be after span_start")
        if (self.time_range_start is None) != (self.time_range_end is None):
            raise ValueError("time coordinates require both start and end")
        if self.time_range_start and self.time_range_end <= self.time_range_start:
            raise ValueError("time range end must follow start")
        if self.field_path is None and self.span_start is None and self.time_range_start is None:
            raise ValueError("coordinate requires a field, span, or time range")
        return self


class ConversationEventRevision(_PerceptionContract):
    tenant_id: UUID
    event_id: str = Field(min_length=1)
    source_system: str = Field(min_length=1)
    source_event_id: str = Field(min_length=1)
    revision_id: str = Field(min_length=1)
    revision_number: int = Field(ge=1)
    kind: ConversationEventKind
    author_source_id: str = Field(min_length=1)
    emitted_at: datetime
    observed_at: datetime
    content_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    raw_evidence_ref: str | None = None
    retention_fate: SourceRetentionFate
    retention_reason: str | None = None
    supersedes_revision_id: str | None = None
    source_thread_id: str | None = None
    source_reply_to_id: str | None = None
    quoted_source_event_ids: tuple[str, ...] = ()
    linked_source_object_ids: tuple[str, ...] = ()

    @field_validator("emitted_at", "observed_at")
    @classmethod
    def event_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def retention_and_revision_are_explicit(self) -> Self:
        if self.observed_at < self.emitted_at:
            raise ValueError("observed_at cannot precede emitted_at")
        payload_available = self.retention_fate is SourceRetentionFate.PAYLOAD_AVAILABLE
        if payload_available != bool(self.content_hash and self.raw_evidence_ref):
            raise ValueError("available payload requires both content hash and raw evidence ref")
        if not payload_available and not self.retention_reason:
            raise ValueError("redacted or unavailable history requires a typed reason")
        if self.revision_number > 1 and not self.supersedes_revision_id:
            raise ValueError("later revisions require supersedes_revision_id")
        return self


class ConversationTopologyKind(StrEnum):
    REPLY_TO = "reply_to"
    THREAD_ROOT = "thread_root"
    EDIT_OF = "edit_of"
    QUOTES = "quotes"
    LINKS = "links"
    PARTICIPANT = "participant"


class ConversationTopologyEdge(_PerceptionContract):
    edge_id: str = Field(min_length=1)
    kind: ConversationTopologyKind
    from_event_revision_id: str = Field(min_length=1)
    to_event_or_object_id: str = Field(min_length=1)
    source_basis_refs: tuple[str, ...] = Field(min_length=1)
    projector_version: str = Field(min_length=1)
    authority_label_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")


class InterpretationMode(StrEnum):
    AS_KNOWN_AT_CUTOFF = "as_known_at_cutoff"
    RETROSPECTIVE_CURRENT = "retrospective_current"


class ContextRiskTier(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CONSEQUENTIAL = "consequential"


class ContextBudget(_PerceptionContract):
    max_events: int = Field(ge=1)
    max_topology_hops: int = Field(ge=0)
    max_source_reads: int = Field(ge=0)
    max_model_calls: int = Field(ge=0)
    max_tokens: int = Field(ge=0)
    max_latency_ms: int = Field(ge=1)


class InterpretationContextRequest(_PerceptionContract):
    request_id: str = Field(min_length=1)
    tenant_id: UUID
    focal_event_revision_ids: tuple[str, ...] = Field(min_length=1)
    mode: InterpretationMode
    effective_query_time: datetime
    evidence_cutoff: datetime
    knowledge_cutoff: datetime
    source_topology_version: str = Field(min_length=1)
    processing_authority: ProcessingAuthorityContext
    allowed_source_spaces: RestrictionSet
    risk_tier: ContextRiskTier
    required_probe_surfaces: tuple[str, ...] = Field(min_length=1)
    budget: ContextBudget
    policy_versions: tuple[str, ...] = Field(min_length=1)
    self_contained_source: bool = False

    @field_validator("effective_query_time", "evidence_cutoff", "knowledge_cutoff")
    @classmethod
    def request_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def authority_and_cutoff_are_coherent(self) -> Self:
        if self.processing_authority.tenant_id != self.tenant_id:
            raise ValueError("processing authority tenant must match context tenant")
        if self.evidence_cutoff > self.knowledge_cutoff:
            raise ValueError("evidence cutoff cannot follow knowledge cutoff")
        if not self.processing_authority.is_live(self.effective_query_time):
            raise ValueError("context construction requires live processing authority")
        return self


class CandidateContextLayer(StrEnum):
    FOCAL = "focal"
    SOURCE_TOPOLOGY = "source_topology"
    TEMPORAL = "temporal"
    PARTICIPANT = "participant"
    DISCOURSE = "discourse"
    SOURCE_REFERENCE = "source_reference"
    EXTERNAL_LINK = "external_link"
    CROSS_CHANNEL = "cross_channel"


class SelectedContextItem(_PerceptionContract):
    event_revision_id: str = Field(min_length=1)
    source_space: str = Field(min_length=1)
    emitted_at: datetime
    layer: CandidateContextLayer
    inclusion_reasons: tuple[str, ...] = Field(min_length=1)
    source_version: str = Field(min_length=1)
    authority_label: str = Field(min_length=1)
    relation_to_focal: str = Field(min_length=1)

    @field_validator("emitted_at")
    @classmethod
    def emitted_time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="emitted_at")


class ConversationEpisodeHypothesis(_PerceptionContract):
    """Temporary search state; selected contents are embedded, never independently owned."""

    membership_weights: dict[str, float] = Field(min_length=1)
    boundary_alternatives: tuple[str, ...] = Field(min_length=1)
    topic_state: str = Field(min_length=1)
    continuity_evidence_refs: tuple[str, ...]
    split_merge_evidence_refs: tuple[str, ...]
    boundary_confidence: float = Field(ge=0.0, le=1.0)
    generator_version: str = Field(min_length=1)
    configuration_version: str = Field(min_length=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def weights_and_hash_are_valid(self) -> Self:
        if any(value <= 0.0 or value > 1.0 for value in self.membership_weights.values()):
            raise ValueError("episode membership weights must lie in (0, 1]")
        expected = canonical_sha256(self.model_dump(mode="json", exclude={"content_hash"}))
        if self.content_hash != expected:
            raise ValueError("episode content_hash does not match embedded contents")
        return self

    @classmethod
    def build(cls, **values: Any) -> ConversationEpisodeHypothesis:
        payload = dict(values)
        payload.pop("content_hash", None)
        normalized = _normalize_build_values(cls, payload)
        digest = canonical_sha256(
            cls.model_construct(**normalized).model_dump(
                mode="json",
                exclude={"content_hash"},
            )
        )
        return cls(**payload, content_hash=digest)


class DiscourseReferentKind(StrEnum):
    PRONOUN = "pronoun"
    NOMINAL = "nominal"
    ELLIPSIS = "ellipsis"
    GROUP = "group"
    DEICTIC = "deictic"


class DiscourseReferent(_PerceptionContract):
    referent_id: str = Field(min_length=1)
    kind: DiscourseReferentKind
    anchor: EvidenceCoordinate
    candidate_antecedent_refs: tuple[str, ...]
    supporting_event_revision_ids: tuple[str, ...]
    normalized_time_alternatives: tuple[str, ...] = ()
    confidence_distribution: dict[str, float]
    unresolved: bool = False
    authenticated_source_mapping_ref: str | None = None

    @model_validator(mode="after")
    def referent_uncertainty_is_explicit(self) -> Self:
        _probabilities_are_normalized(
            self.confidence_distribution,
            name="discourse referent distribution",
        )
        if not self.candidate_antecedent_refs and not self.unresolved:
            raise ValueError("referent without antecedents must remain unresolved")
        return self


class ContextProbeResult(_PerceptionContract):
    probe_id: str = Field(min_length=1)
    probe_version: str = Field(min_length=1)
    tested_context_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    unresolved_dependency_refs: tuple[str, ...]
    alternative_interpretation_refs: tuple[str, ...]
    perturbation_results: dict[str, float]
    future_or_authority_incident_refs: tuple[str, ...] = ()
    expected_value_of_expansion: float
    cost_of_expansion: float = Field(ge=0.0)


class SufficiencyDisposition(StrEnum):
    OPERATIONALLY_SUFFICIENT = "operationally_sufficient"
    SUFFICIENT_WITH_OMISSIONS = "sufficient_with_omissions"
    MULTI_CONTEXT = "multi_context_hypotheses"
    NEEDS_EXPANSION = "needs_expansion"
    NEEDS_CLARIFICATION = "needs_clarification"
    BUDGET_EXHAUSTED = "budget_exhausted"
    NON_IDENTIFIABLE = "non_identifiable"


class OperationalSufficiencyVerdict(_PerceptionContract):
    verdict_id: str = Field(min_length=1)
    probe_refs: tuple[str, ...] = Field(min_length=1)
    risk_tier: ContextRiskTier
    perturbation_policy_version: str = Field(min_length=1)
    budget: ContextBudget
    disposition: SufficiencyDisposition
    omissions: tuple[str, ...]
    unresolved_references: tuple[str, ...]
    stop_reason: str = Field(min_length=1)

    @model_validator(mode="after")
    def partial_fates_name_their_limits(self) -> Self:
        if self.disposition in {
            SufficiencyDisposition.SUFFICIENT_WITH_OMISSIONS,
            SufficiencyDisposition.BUDGET_EXHAUSTED,
            SufficiencyDisposition.NON_IDENTIFIABLE,
        } and not (self.omissions or self.unresolved_references):
            raise ValueError("partial context fate must name omissions or unresolved references")
        return self


class InterpretationContextSnapshot(_PerceptionContract):
    snapshot_id: str = Field(min_length=1)
    snapshot_version: int = Field(ge=1)
    request: InterpretationContextRequest
    focal_event_revision_ids: tuple[str, ...] = Field(min_length=1)
    selected_items: tuple[SelectedContextItem, ...] = Field(min_length=1)
    topology_edge_ids: tuple[str, ...]
    embedded_episode_hypotheses: tuple[ConversationEpisodeHypothesis, ...]
    discourse_referents: tuple[DiscourseReferent, ...]
    sufficiency_verdict: OperationalSufficiencyVerdict
    inherited_processing_authority: ProcessingAuthorityContext
    frozen_at: datetime
    snapshot_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    model_and_policy_versions: tuple[str, ...] = Field(min_length=1)

    @field_validator("frozen_at")
    @classmethod
    def frozen_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="frozen_at")

    @model_validator(mode="after")
    def snapshot_is_authorized_cutoff_bound_and_self_contained(self) -> Self:
        if self.request.tenant_id != self.inherited_processing_authority.tenant_id:
            raise ValueError("snapshot authority tenant must match request tenant")
        if not self.inherited_processing_authority.is_no_broader_than(
            self.request.processing_authority
        ):
            raise ValueError("snapshot cannot broaden request processing authority")
        if not self.inherited_processing_authority.is_live(self.frozen_at):
            raise ValueError("snapshot freeze requires live processing authority")
        if set(self.focal_event_revision_ids) != set(
            self.request.focal_event_revision_ids
        ):
            raise ValueError("snapshot focal events must equal the request focal events")
        item_ids = {item.event_revision_id for item in self.selected_items}
        if not set(self.focal_event_revision_ids) <= item_ids:
            raise ValueError("snapshot must include every focal event revision")
        for item in self.selected_items:
            if item.emitted_at > self.request.evidence_cutoff:
                raise ValueError("snapshot cannot contain evidence after its cutoff")
            if not self.request.allowed_source_spaces.permits(item.source_space):
                raise ValueError("snapshot contains a disallowed source space")
            if not self.inherited_processing_authority.source_labels.permits(
                item.authority_label
            ):
                raise ValueError("snapshot contains an impermissible authority label")
        for hypothesis in self.embedded_episode_hypotheses:
            if not set(hypothesis.membership_weights) <= item_ids:
                raise ValueError("episode hypothesis references unselected context")
        if not self.embedded_episode_hypotheses and not self.request.self_contained_source:
            raise ValueError("non-self-contained context requires an embedded boundary hypothesis")
        expected = canonical_sha256(
            self.model_dump(mode="json", exclude={"snapshot_content_hash"})
        )
        if self.snapshot_content_hash != expected:
            raise ValueError("snapshot_content_hash does not match frozen contents")
        return self

    @classmethod
    def build(cls, **values: Any) -> InterpretationContextSnapshot:
        payload = dict(values)
        payload.pop("snapshot_content_hash", None)
        normalized = _normalize_build_values(cls, payload)
        digest = canonical_sha256(
            cls.model_construct(**normalized).model_dump(
                mode="json",
                exclude={"snapshot_content_hash"},
            )
        )
        return cls(**payload, snapshot_content_hash=digest)


class SelectionDependency(_PerceptionContract):
    dependency_id: str = Field(min_length=1)
    snapshot_id: str = Field(min_length=1)
    snapshot_version: int = Field(ge=1)
    embedded_hypothesis_hashes: tuple[str, ...]
    selected_event_revision_ids: tuple[str, ...] = Field(min_length=1)
    topology_versions: tuple[str, ...]
    participant_and_role_versions: tuple[str, ...]
    discourse_referent_versions: tuple[str, ...]
    linked_object_versions: tuple[str, ...]
    invalidation_keys: tuple[str, ...] = Field(min_length=1)


class SourceAssertionKind(StrEnum):
    ASSERTED = "asserted"
    ASKED = "asked"
    RECOMMENDED = "recommended"
    PROMISED = "promised"
    APPROVED = "approved"
    REJECTED = "rejected"
    CORRECTED = "corrected"
    HYPOTHESIZED = "hypothesized"


class SourceAssertion(_PerceptionContract):
    assertion_id: str = Field(min_length=1)
    assertion_version: int = Field(ge=1)
    context_snapshot_id: str | None = None
    coordinates: tuple[EvidenceCoordinate, ...] = Field(min_length=1)
    current_speaker_or_author: str = Field(min_length=1)
    attributed_speaker_or_author: str | None = None
    kind: SourceAssertionKind
    expressed_content: str = Field(min_length=1)
    source_status: str = Field(min_length=1)
    extractor_version: str = Field(min_length=1)
    uncertainty: float = Field(ge=0.0, le=1.0)


class Modality(StrEnum):
    ACTUAL = "actual"
    POSSIBLE = "possible"
    PROBABLE = "probable"
    REQUIRED = "required"
    PERMITTED = "permitted"
    INTENDED = "intended"
    HYPOTHETICAL = "hypothetical"


class SemanticArgument(_PerceptionContract):
    argument_id: str = Field(min_length=1)
    role: str = Field(min_length=1)
    local_value: str | None = None
    mention_anchor_refs: tuple[str, ...] = ()
    implicit: bool = False
    confidence: float = Field(ge=0.0, le=1.0)

    @model_validator(mode="after")
    def argument_has_value_or_anchor(self) -> Self:
        if not (self.local_value or self.mention_anchor_refs or self.implicit):
            raise ValueError("semantic argument requires value, anchor, or implicit role")
        return self


class SemanticFrameCandidate(_PerceptionContract):
    frame_id: str = Field(min_length=1)
    frame_version: int = Field(ge=1)
    source_assertion_id: str = Field(min_length=1)
    predicate_or_event_type: str = Field(min_length=1)
    arguments: tuple[SemanticArgument, ...]
    negated: bool = False
    modality: Modality
    conditional_expression: str | None = None
    quantity: float | None = None
    quantity_unit: str | None = None
    temporal_scope: BitemporalInterval | None = None
    tense_and_aspect: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    extractor_version: str = Field(min_length=1)

    @model_validator(mode="after")
    def frame_fields_are_coherent(self) -> Self:
        argument_ids = [item.argument_id for item in self.arguments]
        if len(argument_ids) != len(set(argument_ids)):
            raise ValueError("semantic frame argument IDs must be unique")
        if (self.quantity is None) != (self.quantity_unit is None):
            raise ValueError("quantity and unit must appear together")
        return self


class SpeechActKind(StrEnum):
    REPORT = "report"
    QUESTION = "question"
    RECOMMENDATION = "recommendation"
    PROMISE = "promise"
    APPROVAL = "approval"
    REJECTION = "rejection"
    CORRECTION = "correction"
    HYPOTHETICAL = "hypothetical"


class SpeechActCandidate(_PerceptionContract):
    speech_act_id: str = Field(min_length=1)
    source_assertion_id: str = Field(min_length=1)
    distribution: dict[SpeechActKind, float]
    authority_cue_refs: tuple[str, ...]
    extractor_version: str = Field(min_length=1)

    @model_validator(mode="after")
    def speech_act_distribution_is_normalized(self) -> Self:
        _probabilities_are_normalized(
            {kind.value: value for kind, value in self.distribution.items()},
            name="speech act distribution",
        )
        return self


class MentionAnchorKind(StrEnum):
    EXPLICIT = "explicit"
    IMPLICIT_REFERENT = "implicit_referent"


class MentionAnchor(_PerceptionContract):
    anchor_id: str = Field(min_length=1)
    kind: MentionAnchorKind
    coordinate: EvidenceCoordinate
    surface_form: str | None = None
    triggering_frame_id: str | None = None
    omitted_role: str | None = None
    supporting_context_refs: tuple[str, ...] = ()
    inference_basis: str | None = None

    @model_validator(mode="after")
    def explicit_and_implicit_anchors_do_not_collapse(self) -> Self:
        if self.kind is MentionAnchorKind.EXPLICIT:
            if not self.surface_form:
                raise ValueError("explicit mention anchor requires source surface form")
            if self.coordinate.span_start is None and self.coordinate.field_path is None:
                raise ValueError("explicit mention requires exact span or field coordinate")
            if self.triggering_frame_id or self.omitted_role or self.inference_basis:
                raise ValueError("explicit mention cannot carry implicit-anchor fields")
        else:
            if self.surface_form:
                raise ValueError("implicit referent cannot fabricate a surface form")
            if not (
                self.triggering_frame_id
                and self.omitted_role
                and self.supporting_context_refs
                and self.inference_basis
            ):
                raise ValueError("implicit referent requires frame, role, context and basis")
        return self


class EntityMention(_PerceptionContract):
    mention_id: str = Field(min_length=1)
    mention_version: int = Field(ge=1)
    primary_anchor: MentionAnchor
    alternate_anchors: tuple[MentionAnchor, ...] = ()
    context_snapshot_id: str | None = None
    source_assertion_and_frame_refs: tuple[str, ...] = Field(min_length=1)
    detection_confidence: float = Field(ge=0.0, le=1.0)
    extractor_version: str = Field(min_length=1)
    correction_predecessor_ref: str | None = None


class EntityTypeAssessment(_PerceptionContract):
    assessment_id: str = Field(min_length=1)
    assessment_version: int = Field(ge=1)
    mention_or_referent_ref: str = Field(min_length=1)
    type_distribution: dict[str, float] = Field(min_length=1)
    evidence_basis_refs: tuple[str, ...] = Field(min_length=1)
    temporal_scope: BitemporalInterval
    model_and_calibration_version: str = Field(min_length=1)

    @model_validator(mode="after")
    def type_assessment_is_open_world(self) -> Self:
        _probabilities_are_normalized(self.type_distribution, name="entity type distribution")
        if "unknown" not in self.type_distribution:
            raise ValueError("open-world type distribution requires an unknown option")
        return self


class LocalRoleBinding(_PerceptionContract):
    binding_id: str = Field(min_length=1)
    frame_id: str = Field(min_length=1)
    mention_id: str = Field(min_length=1)
    role_distribution: dict[str, float] = Field(min_length=1)
    local_coreference_refs: tuple[str, ...] = ()
    extractor_version: str = Field(min_length=1)

    @model_validator(mode="after")
    def roles_are_normalized(self) -> Self:
        _probabilities_are_normalized(self.role_distribution, name="local role distribution")
        return self


class CandidateGenerationBudget(_PerceptionContract):
    max_candidates: int = Field(ge=1)
    max_source_reads: int = Field(ge=0)
    max_index_queries: int = Field(ge=0)
    max_model_calls: int = Field(ge=0)
    max_latency_ms: int = Field(ge=1)


class EntityCandidateGenerationRequest(_PerceptionContract):
    request_id: str = Field(min_length=1)
    tenant_id: UUID
    mention_ref: str = Field(min_length=1)
    mention_version: int = Field(ge=1)
    entity_type_assessment_refs: tuple[str, ...]
    local_role_binding_refs: tuple[str, ...]
    context_snapshot_ref: str = Field(min_length=1)
    registry_as_of_cutoff: datetime
    processing_authority_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    permitted_candidate_sources: RestrictionSet
    permitted_candidate_types: RestrictionSet
    required_retrieval_lanes: tuple[str, ...] = Field(min_length=1)
    generator_version: str = Field(min_length=1)
    index_versions: tuple[str, ...] = Field(min_length=1)
    model_versions: tuple[str, ...]
    configuration_version: str = Field(min_length=1)
    budget: CandidateGenerationBudget
    redrive_of_request_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    generation_request_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("registry_as_of_cutoff")
    @classmethod
    def registry_cutoff_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="registry_as_of_cutoff")

    @model_validator(mode="after")
    def digest_and_lanes_are_canonical(self) -> Self:
        if tuple(sorted(set(self.required_retrieval_lanes))) != self.required_retrieval_lanes:
            raise ValueError("required retrieval lanes must be sorted and unique")
        expected = canonical_sha256(
            self.model_dump(
                mode="json",
                exclude={"generation_request_digest", "request_id"},
            )
        )
        if self.generation_request_digest != expected:
            raise ValueError("generation_request_digest does not match semantic inputs")
        return self

    @classmethod
    def build(cls, **values: Any) -> EntityCandidateGenerationRequest:
        payload = dict(values)
        payload.pop("generation_request_digest", None)
        normalized = _normalize_build_values(cls, payload)
        digest = canonical_sha256(
            cls.model_construct(**normalized).model_dump(
                mode="json",
                exclude={"generation_request_digest", "request_id"},
            )
        )
        return cls(**payload, generation_request_digest=digest)


class CandidateLaneFateKind(StrEnum):
    COMPLETE = "complete"
    UNAVAILABLE = "unavailable"
    DEFERRED = "deferred"
    FAILED = "failed"


class CandidateLaneReasonClass(StrEnum):
    COMPLETED = "completed"
    POLICY_DISABLED = "policy_disabled"
    CONFIGURATION_UNAVAILABLE = "configuration_unavailable"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    BUDGET_EXHAUSTED = "budget_exhausted"
    RETRY_SCHEDULED = "retry_scheduled"
    TERMINAL_PROVIDER_FAILURE = "terminal_provider_failure"


class CandidateLaneFate(_PerceptionContract):
    lane_id: str = Field(min_length=1)
    fate: CandidateLaneFateKind
    reason_class: CandidateLaneReasonClass
    artifact_refs: tuple[str, ...]

    @model_validator(mode="after")
    def complete_lane_uses_complete_reason(self) -> Self:
        if (self.fate is CandidateLaneFateKind.COMPLETE) != (
            self.reason_class is CandidateLaneReasonClass.COMPLETED
        ):
            raise ValueError("complete lane and completed reason must occur together")
        return self


class EntityCandidateKind(StrEnum):
    CANONICAL_REFERENT = "canonical_referent"
    NONE_OF_THE_ABOVE = "none_of_the_above"
    NOVEL_REFERENT = "novel_referent"
    UNKNOWN = "unknown"


class EntityCandidate(_PerceptionContract):
    candidate_id: str = Field(min_length=1)
    kind: EntityCandidateKind
    canonical_referent_id: str | None = None
    canonical_referent_version: int | None = Field(default=None, ge=1)
    candidate_source: str | None = None
    candidate_type: str | None = None
    authorized_positive_evidence_refs: tuple[str, ...]
    authorized_negative_evidence_refs: tuple[str, ...]

    @model_validator(mode="after")
    def canonical_candidate_has_registry_identity_only_when_canonical(self) -> Self:
        has_referent = bool(
            self.canonical_referent_id and self.canonical_referent_version is not None
        )
        if (self.kind is EntityCandidateKind.CANONICAL_REFERENT) != has_referent:
            raise ValueError("only canonical candidates carry a referent ID and version")
        if self.kind is EntityCandidateKind.CANONICAL_REFERENT and not (
            self.candidate_source and self.candidate_type
        ):
            raise ValueError("canonical candidate requires authorized source and type")
        return self


class EntityCandidateSet(_PerceptionContract):
    candidate_set_id: str = Field(min_length=1)
    candidate_set_version: int = Field(ge=1)
    request: EntityCandidateGenerationRequest
    command_result: CommandResult
    lane_fates: tuple[CandidateLaneFate, ...] = Field(min_length=1)
    candidates: tuple[EntityCandidate, ...] = Field(min_length=3)
    registry_version: str = Field(min_length=1)
    expires_at: datetime

    @field_validator("expires_at")
    @classmethod
    def candidate_set_expiry_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="expires_at")

    @model_validator(mode="after")
    def set_is_complete_authorized_and_bound_to_one_request(self) -> Self:
        if self.command_result.status not in {
            CommandResultStatus.APPLIED,
            CommandResultStatus.DUPLICATE,
        }:
            raise ValueError("candidate set requires an applied or duplicate command result")
        if self.command_result.canonical_request_hash != self.request.generation_request_digest:
            raise ValueError("candidate set result must bind the generation request digest")
        lane_ids = [item.lane_id for item in self.lane_fates]
        if tuple(sorted(lane_ids)) != self.request.required_retrieval_lanes:
            raise ValueError("candidate set requires exactly one fate for every required lane")
        candidate_ids = [item.candidate_id for item in self.candidates]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("candidate IDs must be unique")
        kinds = {item.kind for item in self.candidates}
        for required_kind in {
            EntityCandidateKind.NONE_OF_THE_ABOVE,
            EntityCandidateKind.NOVEL_REFERENT,
            EntityCandidateKind.UNKNOWN,
        }:
            if required_kind not in kinds:
                raise ValueError("candidate set requires none, novel and unknown options")
        for candidate in self.candidates:
            if candidate.kind is not EntityCandidateKind.CANONICAL_REFERENT:
                continue
            if not self.request.permitted_candidate_sources.permits(candidate.candidate_source or ""):
                raise ValueError("candidate source is outside processing authority")
            if not self.request.permitted_candidate_types.permits(candidate.candidate_type or ""):
                raise ValueError("candidate type is outside processing authority")
        return self


class CandidateGenerationFateKind(StrEnum):
    OPEN_RETRYABLE = "open_retryable"
    SET_COMMITTED = "set_committed"
    TERMINAL_NO_SET = "terminal_no_set"


class EntityCandidateGenerationFate(_PerceptionContract):
    request: EntityCandidateGenerationRequest
    kind: CandidateGenerationFateKind
    retryable_results: tuple[CommandResult, ...] = ()
    candidate_set: EntityCandidateSet | None = None
    terminal_result: CommandResult | None = None

    @model_validator(mode="after")
    def exactly_one_current_fate_is_visible(self) -> Self:
        for result in self.retryable_results:
            if result.status is not CommandResultStatus.REJECTED_RETRYABLE:
                raise ValueError("retry history may contain only retryable results")
            if result.canonical_request_hash != self.request.generation_request_digest:
                raise ValueError("retry result must bind the same request digest")
        if self.kind is CandidateGenerationFateKind.OPEN_RETRYABLE:
            if not self.retryable_results or self.candidate_set or self.terminal_result:
                raise ValueError("open request requires retry history and no terminal output")
        elif self.kind is CandidateGenerationFateKind.SET_COMMITTED:
            if self.candidate_set is None or self.terminal_result is not None:
                raise ValueError("set-committed fate requires exactly one candidate set")
            if self.candidate_set.request.generation_request_digest != self.request.generation_request_digest:
                raise ValueError("candidate set belongs to a different request")
        else:
            if self.terminal_result is None or self.candidate_set is not None:
                raise ValueError("terminal no-set fate requires one terminal result")
            if self.terminal_result.status not in {
                CommandResultStatus.REJECTED_TERMINAL,
                CommandResultStatus.IDEMPOTENCY_CONFLICT,
            }:
                raise ValueError("no-set result must be terminal")
            if self.terminal_result.canonical_request_hash != self.request.generation_request_digest:
                raise ValueError("terminal result must bind the request digest")
        return self


class ResolutionAssessment(_PerceptionContract):
    assessment_id: str = Field(min_length=1)
    assessment_version: int = Field(ge=1)
    candidate_set: EntityCandidateSet
    candidate_distribution: dict[str, float] = Field(min_length=1)
    identity_evidence_refs: tuple[str, ...]
    evidence_dependence_groups: dict[str, str]
    decisive_evidence_refs: tuple[str, ...]
    missing_discriminators: tuple[str, ...]
    temporal_compatibility_refs: tuple[str, ...]
    calibration_cohort: str = Field(min_length=1)
    scorer_and_calibration_version: str = Field(min_length=1)
    assessed_at: datetime
    expires_at: datetime
    correction_predecessor_ref: str | None = None

    @field_validator("assessed_at", "expires_at")
    @classmethod
    def assessment_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def distribution_is_evidence_relative_and_closed(self) -> Self:
        _probabilities_are_normalized(
            self.candidate_distribution,
            name="resolution candidate distribution",
        )
        candidate_ids = {item.candidate_id for item in self.candidate_set.candidates}
        if set(self.candidate_distribution) != candidate_ids:
            raise ValueError("resolution distribution must cover every and only set candidates")
        if self.expires_at <= self.assessed_at:
            raise ValueError("resolution assessment expiry must follow assessment")
        return self


class EntityLifecycleStatus(StrEnum):
    ACTIVE = "active"
    DISPUTED = "disputed"
    SUPERSEDED = "superseded"
    MERGED = "merged"
    SPLIT = "split"
    RETIRED = "retired"


class CanonicalReferent(_PerceptionContract):
    tenant_id: UUID
    referent_id: str = Field(min_length=1)
    referent_version: int = Field(ge=1)
    lifecycle_status: EntityLifecycleStatus
    predecessor_referent_refs: tuple[str, ...]
    successor_referent_refs: tuple[str, ...]
    birth_decision_ref: str = Field(min_length=1)
    positive_existence_evidence_refs: tuple[str, ...] = Field(min_length=1)


class SourceIdentityBinding(_PerceptionContract):
    binding_id: str = Field(min_length=1)
    binding_version: int = Field(ge=1)
    tenant_id: UUID
    source_system: str = Field(min_length=1)
    source_native_identifier: str = Field(min_length=1)
    source_identity_authority_ref: str = Field(min_length=1)
    canonical_referent_id: str = Field(min_length=1)
    canonical_referent_version: int = Field(ge=1)
    temporal_scope: BitemporalInterval
    evidence_refs: tuple[str, ...] = Field(min_length=1)


class ReferentTrackHypothesis(_PerceptionContract):
    track_id: str = Field(min_length=1)
    track_version: int = Field(ge=1)
    mention_weights: dict[str, float] = Field(min_length=1)
    novelty_evidence_refs: tuple[str, ...]
    existence_evidence_refs: tuple[str, ...]
    split_alternative_refs: tuple[str, ...]
    unresolved_reason: str = Field(min_length=1)

    @model_validator(mode="after")
    def track_weights_are_bounded(self) -> Self:
        if any(value <= 0.0 or value > 1.0 for value in self.mention_weights.values()):
            raise ValueError("referent track weights must lie in (0, 1]")
        return self


class GroundingAdmissionDisposition(StrEnum):
    SINGLE_REFERENT = "single_referent"
    CANDIDATE_DISTRIBUTION = "candidate_distribution"
    MENTION_LOCAL_ONLY = "mention_local_only"
    CLARIFICATION = "clarification"
    REVIEW = "review"
    ABSTENTION = "abstention"


class ReferentVersionRef(_PerceptionContract):
    referent_id: str = Field(min_length=1)
    referent_version: int = Field(ge=1)


class GroundingAdmissionDecision(_PerceptionContract):
    decision_id: str = Field(min_length=1)
    decision_version: int = Field(ge=1)
    assessment: ResolutionAssessment
    consumer: str = Field(min_length=1)
    purpose: str = Field(min_length=1)
    operation: str = Field(min_length=1)
    risk_tier: str = Field(min_length=1)
    blast_radius: str = Field(min_length=1)
    expected_loss: float = Field(ge=0.0)
    consumption_authority: ConsumptionAuthorityContext
    consumer_supports_distributions: bool
    disposition: GroundingAdmissionDisposition
    selected_referent: ReferentVersionRef | None = None
    permitted_distribution: dict[str, float] = Field(default_factory=dict)
    genuine_source_binding: SourceIdentityBinding | None = None
    reason_codes: tuple[str, ...] = Field(min_length=1)
    decided_at: datetime
    expires_at: datetime

    @field_validator("decided_at", "expires_at")
    @classmethod
    def admission_times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def admission_preserves_assessment_and_consumer_risk(self) -> Self:
        if self.expires_at <= self.decided_at:
            raise ValueError("grounding admission expiry must follow decision")
        if not self.consumption_authority.is_live(self.decided_at):
            raise ValueError("grounding admission requires live consumption authority")
        if self.consumption_authority.tenant_id != self.assessment.candidate_set.request.tenant_id:
            raise ValueError("consumption authority tenant must match the assessment tenant")
        if self.consumption_authority.purpose != self.purpose:
            raise ValueError("grounding purpose must match consumption authority")
        if self.consumption_authority.operation != self.operation:
            raise ValueError("grounding operation must match consumption authority")
        candidate_ids = {item.candidate_id for item in self.assessment.candidate_set.candidates}
        if self.disposition is GroundingAdmissionDisposition.SINGLE_REFERENT:
            if self.selected_referent is None or self.permitted_distribution:
                raise ValueError("single-referent admission requires only a selected referent")
            matching = {
                (item.canonical_referent_id, item.canonical_referent_version)
                for item in self.assessment.candidate_set.candidates
                if item.kind is EntityCandidateKind.CANONICAL_REFERENT
            }
            if (self.selected_referent.referent_id, self.selected_referent.referent_version) not in matching:
                raise ValueError("selected referent is absent from the assessed candidate set")
        elif self.disposition is GroundingAdmissionDisposition.CANDIDATE_DISTRIBUTION:
            if not self.consumer_supports_distributions or self.selected_referent:
                raise ValueError("distribution admission requires a capable consumer and no top-one selection")
            _probabilities_are_normalized(
                self.permitted_distribution,
                name="permitted grounding distribution",
            )
            if not set(self.permitted_distribution) <= candidate_ids:
                raise ValueError("permitted distribution references unassessed candidates")
        elif self.selected_referent or self.permitted_distribution:
            raise ValueError("local, clarification, review and abstention cannot imply identity")
        if self.genuine_source_binding:
            if self.genuine_source_binding.tenant_id != self.consumption_authority.tenant_id:
                raise ValueError("source binding tenant must match consumption authority")
            if self.selected_referent is None:
                raise ValueError("source binding can accompany only a selected referent")
            if (
                self.genuine_source_binding.canonical_referent_id
                != self.selected_referent.referent_id
                or self.genuine_source_binding.canonical_referent_version
                != self.selected_referent.referent_version
            ):
                raise ValueError("source binding and selected referent must match")
        return self


class GroundingContinuity(_PerceptionContract):
    downstream_object_ref: str = Field(min_length=1)
    mention_ref: str = Field(min_length=1)
    mention_version: int = Field(ge=1)
    resolution_assessment_ref: str = Field(min_length=1)
    resolution_assessment_version: int = Field(ge=1)
    grounding_admission_ref: str = Field(min_length=1)
    grounding_admission_version: int = Field(ge=1)
    selected_referent: ReferentVersionRef | None = None
    source_identity_binding_ref: str | None = None
    source_identity_binding_version: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def optional_binding_is_complete(self) -> Self:
        if (self.source_identity_binding_ref is None) != (
            self.source_identity_binding_version is None
        ):
            raise ValueError("source binding reference and version must appear together")
        if self.source_identity_binding_ref and not self.selected_referent:
            raise ValueError("source binding continuity requires a selected referent")
        return self


class RawDurabilityState(StrEnum):
    RECEIVED = "received"
    CAPTURE_RETRYABLE = "capture_retryable"
    CAPTURE_RETRY_SCHEDULED = "capture_retry_scheduled"
    RAW_DURABLE = "raw_durable"
    TERMINAL_CAPTURE_REJECTED = "terminal_capture_rejected"
    CAPTURE_EXHAUSTED = "capture_exhausted"
    CAPTURE_ESCALATED = "capture_escalated"


_CAPTURE_TRANSITIONS = {
    RawDurabilityState.RECEIVED: {
        RawDurabilityState.CAPTURE_RETRYABLE,
        RawDurabilityState.RAW_DURABLE,
        RawDurabilityState.TERMINAL_CAPTURE_REJECTED,
    },
    RawDurabilityState.CAPTURE_RETRYABLE: {
        RawDurabilityState.CAPTURE_RETRY_SCHEDULED,
        RawDurabilityState.CAPTURE_EXHAUSTED,
        RawDurabilityState.CAPTURE_ESCALATED,
    },
    RawDurabilityState.CAPTURE_RETRY_SCHEDULED: {
        RawDurabilityState.RAW_DURABLE,
        RawDurabilityState.CAPTURE_RETRYABLE,
        RawDurabilityState.TERMINAL_CAPTURE_REJECTED,
        RawDurabilityState.CAPTURE_EXHAUSTED,
        RawDurabilityState.CAPTURE_ESCALATED,
    },
}


class CaptureAttempt(_PerceptionContract):
    attempt_id: str = Field(min_length=1)
    attempt_generation: int = Field(ge=1)
    adapter_version: str = Field(min_length=1)
    storage_version: str = Field(min_length=1)
    authority_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: RawDurabilityState
    occurred_at: datetime
    failure_or_terminal_reason: str | None = None

    @field_validator("occurred_at")
    @classmethod
    def capture_time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="occurred_at")


class CaptureAttemptTransition(_PerceptionContract):
    before: CaptureAttempt
    after: CaptureAttempt

    @model_validator(mode="after")
    def transition_is_legal_and_versioned(self) -> Self:
        if self.before.attempt_id != self.after.attempt_id:
            raise ValueError("capture transition cannot change attempt identity")
        if self.before.attempt_generation != self.after.attempt_generation:
            raise ValueError("capture transition cannot change attempt generation")
        if self.before.state in {
            RawDurabilityState.RAW_DURABLE,
            RawDurabilityState.TERMINAL_CAPTURE_REJECTED,
            RawDurabilityState.CAPTURE_EXHAUSTED,
            RawDurabilityState.CAPTURE_ESCALATED,
        }:
            if self.after.state is not self.before.state:
                raise ValueError("terminal capture state is immutable")
        elif self.after.state not in _CAPTURE_TRANSITIONS.get(self.before.state, set()):
            raise ValueError("illegal capture state transition")
        if self.after.occurred_at < self.before.occurred_at:
            raise ValueError("capture transition time cannot go backward")
        return self


class ProcessingGenerationState(StrEnum):
    PENDING = "pending"
    NORMALIZING = "normalizing"
    NORMALIZED = "normalized"
    OBSERVATION_COMMITTING = "observation_committing"
    OBSERVATION_COMMITTED = "observation_committed"
    RETRYABLE = "retryable"
    RETRY_SCHEDULED = "retry_scheduled"
    QUARANTINED = "quarantined"
    REDRIVE_AUTHORIZED = "redrive_authorized"
    SUPERSEDED_BY_NEW_GENERATION = "superseded_by_new_generation"
    TERMINAL_REJECTED = "terminal_rejected"
    PROCESSING_EXHAUSTED = "processing_exhausted"
    PROCESSING_ESCALATED = "processing_escalated"


_PROCESSING_TRANSITIONS = {
    ProcessingGenerationState.PENDING: {
        ProcessingGenerationState.NORMALIZING,
        ProcessingGenerationState.RETRYABLE,
        ProcessingGenerationState.QUARANTINED,
        ProcessingGenerationState.TERMINAL_REJECTED,
    },
    ProcessingGenerationState.NORMALIZING: {
        ProcessingGenerationState.NORMALIZED,
        ProcessingGenerationState.RETRYABLE,
        ProcessingGenerationState.QUARANTINED,
        ProcessingGenerationState.TERMINAL_REJECTED,
    },
    ProcessingGenerationState.NORMALIZED: {
        ProcessingGenerationState.OBSERVATION_COMMITTING,
        ProcessingGenerationState.RETRYABLE,
        ProcessingGenerationState.QUARANTINED,
        ProcessingGenerationState.TERMINAL_REJECTED,
    },
    ProcessingGenerationState.OBSERVATION_COMMITTING: {
        ProcessingGenerationState.OBSERVATION_COMMITTED,
        ProcessingGenerationState.RETRYABLE,
        ProcessingGenerationState.QUARANTINED,
        ProcessingGenerationState.TERMINAL_REJECTED,
    },
    ProcessingGenerationState.RETRYABLE: {
        ProcessingGenerationState.RETRY_SCHEDULED,
        ProcessingGenerationState.PROCESSING_EXHAUSTED,
        ProcessingGenerationState.PROCESSING_ESCALATED,
    },
    ProcessingGenerationState.RETRY_SCHEDULED: {ProcessingGenerationState.PENDING},
    ProcessingGenerationState.QUARANTINED: {
        ProcessingGenerationState.REDRIVE_AUTHORIZED,
        ProcessingGenerationState.PROCESSING_EXHAUSTED,
        ProcessingGenerationState.PROCESSING_ESCALATED,
    },
    ProcessingGenerationState.REDRIVE_AUTHORIZED: {
        ProcessingGenerationState.SUPERSEDED_BY_NEW_GENERATION
    },
}


_PROCESSING_TERMINAL_STATES = {
    ProcessingGenerationState.OBSERVATION_COMMITTED,
    ProcessingGenerationState.SUPERSEDED_BY_NEW_GENERATION,
    ProcessingGenerationState.TERMINAL_REJECTED,
    ProcessingGenerationState.PROCESSING_EXHAUSTED,
    ProcessingGenerationState.PROCESSING_ESCALATED,
}


class ProcessingGeneration(_PerceptionContract):
    generation_id: str = Field(min_length=1)
    generation_number: int = Field(ge=1)
    parent_generation_id: str | None = None
    raw_reference: str = Field(min_length=1)
    mapping_version: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    configuration_version: str = Field(min_length=1)
    processing_authority_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    generation_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: ProcessingGenerationState
    occurred_at: datetime
    terminal_reason: str | None = None

    @field_validator("occurred_at")
    @classmethod
    def generation_time_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="occurred_at")

    @model_validator(mode="after")
    def terminal_state_has_an_explicit_reason(self) -> Self:
        reason_required = self.state in {
            ProcessingGenerationState.TERMINAL_REJECTED,
            ProcessingGenerationState.PROCESSING_EXHAUSTED,
            ProcessingGenerationState.PROCESSING_ESCALATED,
        }
        if reason_required and not self.terminal_reason:
            raise ValueError("failed terminal processing state requires terminal_reason")
        return self


class ProcessingGenerationTransition(_PerceptionContract):
    before: ProcessingGeneration
    after: ProcessingGeneration

    @model_validator(mode="after")
    def transition_is_legal(self) -> Self:
        if self.before.generation_id != self.after.generation_id:
            raise ValueError("processing transition cannot change generation identity")
        if self.before.generation_number != self.after.generation_number:
            raise ValueError("processing transition cannot change generation number")
        if self.before.state in _PROCESSING_TERMINAL_STATES:
            if self.after.state is not self.before.state:
                raise ValueError("terminal processing state is immutable")
        elif self.after.state not in _PROCESSING_TRANSITIONS.get(self.before.state, set()):
            raise ValueError("illegal processing-generation transition")
        if self.after.occurred_at < self.before.occurred_at:
            raise ValueError("processing transition time cannot go backward")
        return self


class IngestionReceipt(_PerceptionContract):
    receipt_id: str = Field(min_length=1)
    tenant_id: UUID
    source_system: str = Field(min_length=1)
    authenticated_delivery_id: str = Field(min_length=1)
    source_cursor_or_offset: str = Field(min_length=1)
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    raw_durability_state: RawDurabilityState
    capture_attempts: tuple[CaptureAttempt, ...] = Field(min_length=1)
    raw_reference: str | None = None
    external_acknowledged_at: datetime | None = None
    processing_generations: tuple[ProcessingGeneration, ...] = ()
    current_processing_generation_id: str | None = None

    @field_validator("external_acknowledged_at")
    @classmethod
    def ack_time_is_aware(cls, value: datetime | None) -> datetime | None:
        return _aware(value, field_name="external_acknowledged_at") if value else None

    @model_validator(mode="after")
    def receipt_preserves_raw_durability_and_generation_lineage(self) -> Self:
        durable = self.raw_durability_state is RawDurabilityState.RAW_DURABLE
        if durable != bool(self.raw_reference):
            raise ValueError("raw durable receipt requires exactly one raw reference")
        if self.external_acknowledged_at and not durable:
            raise ValueError("external acknowledgement cannot precede raw durability")
        if self.processing_generations and not durable:
            raise ValueError("processing generations require raw durability")
        attempt_generations = [item.attempt_generation for item in self.capture_attempts]
        if len(attempt_generations) != len(set(attempt_generations)):
            raise ValueError("capture attempt generations must be unique")
        if attempt_generations != sorted(attempt_generations):
            raise ValueError("capture attempts must be ordered by generation")
        if self.capture_attempts[-1].state is not self.raw_durability_state:
            raise ValueError("receipt raw durability must equal the latest capture attempt")
        generation_ids = [item.generation_id for item in self.processing_generations]
        if len(generation_ids) != len(set(generation_ids)):
            raise ValueError("processing generation IDs must be unique")
        by_id = {item.generation_id: item for item in self.processing_generations}
        for generation in self.processing_generations:
            if generation.raw_reference != self.raw_reference:
                raise ValueError("processing generation belongs to a different raw reference")
            if generation.parent_generation_id:
                parent = by_id.get(generation.parent_generation_id)
                if parent is None or parent.generation_number >= generation.generation_number:
                    raise ValueError("generation parent must exist and precede its child")
                if parent.state is not ProcessingGenerationState.SUPERSEDED_BY_NEW_GENERATION:
                    raise ValueError("a processing successor requires a superseded parent")
        if self.processing_generations:
            if self.current_processing_generation_id not in by_id:
                raise ValueError("receipt must name one current processing generation")
            current = by_id[self.current_processing_generation_id or ""]
            if current.state is ProcessingGenerationState.SUPERSEDED_BY_NEW_GENERATION:
                raise ValueError("superseded generation cannot be current")
            current_ids = {
                generation.generation_id
                for generation in self.processing_generations
                if generation.state is not ProcessingGenerationState.SUPERSEDED_BY_NEW_GENERATION
                and not any(
                    child.parent_generation_id == generation.generation_id
                    for child in self.processing_generations
                )
            }
            if current_ids != {self.current_processing_generation_id}:
                raise ValueError("processing lineage must reduce to exactly one current head")
        elif self.current_processing_generation_id is not None:
            raise ValueError("receipt without processing generations cannot name a current head")
        return self


__all__ = [
    "CandidateGenerationBudget",
    "CandidateGenerationFateKind",
    "CandidateLaneFate",
    "CandidateLaneFateKind",
    "CandidateLaneReasonClass",
    "CanonicalReferent",
    "CaptureAttempt",
    "CaptureAttemptTransition",
    "ContextBudget",
    "ContextProbeResult",
    "ContextRiskTier",
    "ConversationEpisodeHypothesis",
    "ConversationEventKind",
    "ConversationEventRevision",
    "ConversationTopologyEdge",
    "ConversationTopologyKind",
    "DiscourseReferent",
    "DiscourseReferentKind",
    "EntityCandidate",
    "EntityCandidateGenerationFate",
    "EntityCandidateGenerationRequest",
    "EntityCandidateKind",
    "EntityCandidateSet",
    "EntityLifecycleStatus",
    "EntityMention",
    "EntityTypeAssessment",
    "EvidenceCoordinate",
    "GroundingAdmissionDecision",
    "GroundingAdmissionDisposition",
    "GroundingContinuity",
    "IngestionReceipt",
    "InterpretationContextRequest",
    "InterpretationContextSnapshot",
    "InterpretationMode",
    "LocalRoleBinding",
    "MentionAnchor",
    "MentionAnchorKind",
    "Modality",
    "OperationalSufficiencyVerdict",
    "ProcessingGeneration",
    "ProcessingGenerationState",
    "ProcessingGenerationTransition",
    "RawDurabilityState",
    "ReferentTrackHypothesis",
    "ReferentVersionRef",
    "ResolutionAssessment",
    "SelectedContextItem",
    "SelectionDependency",
    "SemanticArgument",
    "SemanticFrameCandidate",
    "SourceAssertion",
    "SourceAssertionKind",
    "SourceIdentityBinding",
    "SourceRetentionFate",
    "SpeechActCandidate",
    "SpeechActKind",
    "SufficiencyDisposition",
]
