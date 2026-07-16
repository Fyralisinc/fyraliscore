"""Durable conversational context-selection contracts.

The source event remains evidence. Candidates, episode hypotheses, and probe
outputs are pre-truth search state. Only the selected, cutoff- and
authority-safe :class:`InterpretationContextSnapshot` becomes a durable
grounding annotation, with exact dependencies for later invalidation.
"""

from __future__ import annotations

from datetime import datetime
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

from lib.contracts.agency import AgencyWriteContext
from lib.contracts.kernel import canonical_sha256
from lib.contracts.perception import (
    CandidateContextLayer,
    ContextProbeResult,
    ConversationEpisodeHypothesis,
    DiscourseReferent,
    InterpretationContextRequest,
    InterpretationContextSnapshot,
    SelectedContextItem,
    SelectionDependency,
    SufficiencyDisposition,
)


class _ConversationContextContract(BaseModel):
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


def _normalize(model_type, values: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for name, value in values.items():
        field = model_type.model_fields.get(name)
        normalized[name] = (
            TypeAdapter(field.annotation).validate_python(value) if field else value
        )
    return normalized


class ContextCandidateCost(_ConversationContextContract):
    event_count: int = Field(ge=1)
    token_count: int = Field(ge=0)
    source_reads: int = Field(ge=0)
    model_calls: int = Field(ge=0)
    latency_ms: int = Field(ge=0)


class ConversationContextCandidate(_ConversationContextContract):
    candidate_id: UUID
    request_id: str = Field(min_length=1)
    selected_items: tuple[SelectedContextItem, ...] = Field(min_length=1)
    topology_edge_ids: tuple[str, ...] = ()
    embedded_episode_hypotheses: tuple[ConversationEpisodeHypothesis, ...] = ()
    discourse_referents: tuple[DiscourseReferent, ...] = ()
    layer_coverage: tuple[CandidateContextLayer, ...] = Field(min_length=1)
    omitted_lane_reasons: dict[str, str] = Field(default_factory=dict)
    cost: ContextCandidateCost
    generator_version: str = Field(min_length=1)
    configuration_version: str = Field(min_length=1)
    candidate_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def candidate_is_exact_and_self_consistent(self) -> Self:
        item_ids = [item.event_revision_id for item in self.selected_items]
        if len(item_ids) != len(set(item_ids)):
            raise ValueError("context candidate event revisions must be unique")
        if self.cost.event_count != len(self.selected_items):
            raise ValueError("context candidate event cost must equal selected items")
        if CandidateContextLayer.FOCAL not in self.layer_coverage:
            raise ValueError("every context candidate must cover the focal layer")
        if len(self.layer_coverage) != len(set(self.layer_coverage)):
            raise ValueError("context candidate layers must be unique")
        if len(self.topology_edge_ids) != len(set(self.topology_edge_ids)):
            raise ValueError("context candidate topology edges must be unique")
        for hypothesis in self.embedded_episode_hypotheses:
            if not set(hypothesis.membership_weights) <= set(item_ids):
                raise ValueError("episode hypothesis references unselected context")
        expected = canonical_sha256(
            self.model_dump(mode="json", exclude={"candidate_content_hash"})
        )
        if self.candidate_content_hash != expected:
            raise ValueError("candidate_content_hash does not match candidate contents")
        return self

    @classmethod
    def build(cls, **values: Any) -> ConversationContextCandidate:
        payload = dict(values)
        payload.pop("candidate_content_hash", None)
        normalized = _normalize(cls, payload)
        digest = canonical_sha256(
            cls.model_construct(**normalized).model_dump(
                mode="json", exclude={"candidate_content_hash"}
            )
        )
        return cls(**payload, candidate_content_hash=digest)


class ContextProbeEnvelope(_ConversationContextContract):
    candidate_id: UUID
    probe: ContextProbeResult
    completed_probe_surfaces: tuple[str, ...] = ()
    failed_probe_surfaces: dict[str, str] = Field(default_factory=dict)
    semantic_output_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    contamination_score: float = Field(ge=0.0, le=1.0)

    @model_validator(mode="after")
    def probe_surface_fates_are_disjoint(self) -> Self:
        if len(self.completed_probe_surfaces) != len(
            set(self.completed_probe_surfaces)
        ):
            raise ValueError("completed probe surfaces must be unique")
        overlap = set(self.completed_probe_surfaces) & set(self.failed_probe_surfaces)
        if overlap:
            raise ValueError("one probe surface cannot both complete and fail")
        if any(abs(value) > 1.0 for value in self.probe.perturbation_results.values()):
            raise ValueError("probe perturbation deltas must lie in [-1, 1]")
        return self


class ContextSelectionPolicy(_ConversationContextContract):
    policy_version: str = Field(min_length=1)
    max_semantic_perturbation: float = Field(ge=0.0, le=1.0)
    max_contamination_score: float = Field(ge=0.0, le=1.0)
    event_weight: float = Field(default=1.0, ge=0.0)
    token_per_thousand_weight: float = Field(default=1.0, ge=0.0)
    source_read_weight: float = Field(default=1.0, ge=0.0)
    model_call_weight: float = Field(default=4.0, ge=0.0)
    latency_second_weight: float = Field(default=1.0, ge=0.0)
    multi_context_cost_tolerance: float = Field(default=0.05, ge=0.0)
    max_multi_context_alternatives: int = Field(default=3, ge=2, le=8)

    def cost_score(self, cost: ContextCandidateCost) -> float:
        return (
            cost.event_count * self.event_weight
            + (cost.token_count / 1_000) * self.token_per_thousand_weight
            + cost.source_reads * self.source_read_weight
            + cost.model_calls * self.model_call_weight
            + (cost.latency_ms / 1_000) * self.latency_second_weight
        )


class InterpretationContextHeadExpectation(_ConversationContextContract):
    expected_aggregate_version: int = Field(ge=0)
    expected_snapshot_id: UUID | None = None

    @model_validator(mode="after")
    def creation_and_successor_expectations_are_coherent(self) -> Self:
        if (self.expected_aggregate_version == 0) != (
            self.expected_snapshot_id is None
        ):
            raise ValueError(
                "new context expects version zero and no snapshot; successors require both"
            )
        return self


class CommitInterpretationContextCommand(_ConversationContextContract):
    context: AgencyWriteContext
    proposed_snapshot_id: UUID
    proposed_dependency_id: UUID
    selection_subject: str = Field(min_length=1)
    focal_observation_id: UUID | None = None
    request: InterpretationContextRequest
    candidates: tuple[ConversationContextCandidate, ...] = Field(min_length=1)
    probes: tuple[ContextProbeEnvelope, ...] = Field(min_length=1)
    policy: ContextSelectionPolicy
    expected: InterpretationContextHeadExpectation
    invalidation_keys: tuple[str, ...] = Field(min_length=1)
    linked_object_versions: tuple[str, ...] = ()
    participant_and_role_versions: tuple[str, ...] = ()
    search_exhausted: bool = False
    prepared_at: datetime

    @field_validator("prepared_at")
    @classmethod
    def prepared_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="prepared_at")

    @model_validator(mode="after")
    def command_is_authorized_complete_and_non_circular(self) -> Self:
        if self.context.tenant_id != self.request.tenant_id:
            raise ValueError("context command and request tenants must match")
        if self.context.processing_authority.fingerprint != (
            self.request.processing_authority.fingerprint
        ):
            raise ValueError("context command must preserve exact processing authority")
        writer_scope = self.context.writer_scope_epoch
        if writer_scope.semantic_responsibility != "interpretation_context":
            raise ValueError("context command requires interpretation_context scope")
        if writer_scope.writer_owner != "GroundingAnnotationAppender":
            raise ValueError("GroundingAnnotationAppender is the sole context writer")
        if not self.request.allowed_source_spaces.permits(writer_scope.source_partition):
            raise ValueError("writer source partition is outside request authority")
        if self.prepared_at < self.context.issued_at:
            raise ValueError("context cannot be prepared before command issuance")
        if self.prepared_at >= self.context.expires_at:
            raise ValueError("context was prepared after command expiry")

        candidate_ids = [candidate.candidate_id for candidate in self.candidates]
        probe_ids = [probe.candidate_id for probe in self.probes]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("context candidate IDs must be unique")
        if len(probe_ids) != len(set(probe_ids)):
            raise ValueError("context probes must be unique per candidate")
        if set(candidate_ids) != set(probe_ids):
            raise ValueError("every context candidate requires exactly one probe fate")
        if any(
            candidate.request_id != self.request.request_id
            for candidate in self.candidates
        ):
            raise ValueError("context candidate request IDs must match the command request")

        focal = set(self.request.focal_event_revision_ids)
        probes = {probe.candidate_id: probe for probe in self.probes}
        for candidate in self.candidates:
            item_ids = {item.event_revision_id for item in candidate.selected_items}
            if not focal <= item_ids:
                raise ValueError("every context candidate must contain every focal revision")
            for item in candidate.selected_items:
                if item.emitted_at > self.request.evidence_cutoff:
                    raise ValueError("context candidate contains future evidence")
                if not self.request.allowed_source_spaces.permits(item.source_space):
                    raise ValueError("context candidate contains disallowed source space")
                if not self.request.processing_authority.source_labels.permits(
                    item.authority_label
                ):
                    raise ValueError("context candidate contains impermissible source label")
                if not self.request.processing_authority.object_ids.permits(
                    item.event_revision_id
                ):
                    raise ValueError(
                        "context candidate event revision is outside processing authority"
                    )
            if (
                probes[candidate.candidate_id].probe.tested_context_hash
                != candidate.candidate_content_hash
            ):
                raise ValueError("context probe hash must name its exact candidate")
        return self

    @property
    def selection_key(self) -> str:
        return canonical_sha256(
            {
                "tenant_id": str(self.context.tenant_id),
                "focal_event_revision_ids": self.request.focal_event_revision_ids,
                "selection_subject": self.selection_subject,
                "mode": self.request.mode.value,
                "purpose": self.request.processing_authority.purpose,
                "operation": self.request.processing_authority.operation,
                "source_partition": self.context.writer_scope_epoch.source_partition,
            }
        )

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ContextSelectionOutcome(_ConversationContextContract):
    selection_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    aggregate_version: int = Field(ge=1)
    snapshot: InterpretationContextSnapshot
    dependency: SelectionDependency
    selected_candidate_ids: tuple[UUID, ...] = Field(min_length=1)
    eligible_candidate_ids: tuple[UUID, ...]
    disposition: SufficiencyDisposition
    rationale_codes: tuple[str, ...] = Field(min_length=1)
    selected_cost_score: float = Field(ge=0.0)
    decision_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def outcome_matches_snapshot_and_digest(self) -> Self:
        if self.snapshot.sufficiency_verdict.disposition is not self.disposition:
            raise ValueError("outcome disposition must equal snapshot verdict")
        if self.dependency.snapshot_id != self.snapshot.snapshot_id:
            raise ValueError("selection dependency must bind the selected snapshot")
        material = self.model_dump(mode="json", exclude={"decision_digest"})
        if self.decision_digest != canonical_sha256(material):
            raise ValueError("decision_digest does not match selection outcome")
        return self

    @classmethod
    def build(cls, **values: Any) -> ContextSelectionOutcome:
        payload = dict(values)
        payload.pop("decision_digest", None)
        normalized = _normalize(cls, payload)
        digest = canonical_sha256(
            cls.model_construct(**normalized).model_dump(
                mode="json", exclude={"decision_digest"}
            )
        )
        return cls(**payload, decision_digest=digest)


__all__ = [
    "CommitInterpretationContextCommand",
    "ContextCandidateCost",
    "ContextProbeEnvelope",
    "ContextSelectionOutcome",
    "ContextSelectionPolicy",
    "ConversationContextCandidate",
    "InterpretationContextHeadExpectation",
]
