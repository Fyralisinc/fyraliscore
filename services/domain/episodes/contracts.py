"""Versioned values the future episode constructor must emit.

These models define semantics and validation only. They deliberately contain
no topic router, clustering heuristic, lifecycle worker, or persistence code.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EpisodeConstitution(_Contract):
    constitution_version: Literal[1] = 1
    episode_is_evidence_batch: Literal[True] = True
    snapshot_is_immutable: Literal[True] = True
    membership_is_asserted: Literal[True] = True
    contradictions_are_preserved: Literal[True] = True
    multiple_membership_allowed: Literal[True] = True
    settlement_claims_completeness_not_truth: Literal[True] = True


class TopicIntent(_Contract):
    id: UUID
    tenant_id: UUID
    origin: Literal["automatic", "query_seeded", "human_pinned"]
    label: str = Field(min_length=1)
    query_text: str | None = None
    requester_actor_id: UUID | None = None
    seed_entity_refs: tuple[dict[str, Any], ...] = ()
    seed_claim_ids: tuple[UUID, ...] = ()
    valid_time_start: datetime | None = None
    valid_time_end: datetime | None = None
    router_name: str = Field(min_length=1)
    router_version: str = Field(min_length=1)
    status: Literal["active", "superseded", "archived"] = "active"
    created_at: datetime

    @model_validator(mode="after")
    def validate_origin_and_window(self) -> "TopicIntent":
        if self.origin == "query_seeded":
            if not self.query_text or self.requester_actor_id is None:
                raise ValueError("query-seeded topics require query text and requester")
        if self.valid_time_start and self.valid_time_end:
            if self.valid_time_end < self.valid_time_start:
                raise ValueError("topic valid-time window is reversed")
        return self


class MembershipReason(_Contract):
    code: Literal[
        "entity_overlap",
        "claim_overlap",
        "relation_path",
        "thread_or_container",
        "temporal_proximity",
        "lexical_semantic_match",
        "query_match",
        "human_decision",
        "hard_negative",
        "authorization_boundary",
    ]
    weight: float
    detail: dict[str, Any] = Field(default_factory=dict)


class EpisodeMembershipAssertion(_Contract):
    id: UUID
    tenant_id: UUID
    topic_id: UUID
    episode_id: UUID
    observation_id: UUID
    evidence_id: UUID
    claim_ids: tuple[UUID, ...] = ()
    identity_assertion_ids: tuple[UUID, ...] = ()
    decision: Literal["include", "exclude", "hold"]
    score: float = Field(ge=0, le=1)
    reasons: tuple[MembershipReason, ...] = ()
    router_name: str = Field(min_length=1)
    router_version: str = Field(min_length=1)
    feature_schema_version: int = Field(ge=1)
    feature_snapshot: dict[str, Any]
    status: Literal["proposed", "accepted", "rejected", "superseded"]
    supersedes_assertion_id: UUID | None = None
    created_at: datetime

    @model_validator(mode="after")
    def require_explanation(self) -> "EpisodeMembershipAssertion":
        if not self.reasons:
            raise ValueError("membership decisions require at least one reason")
        return self


class EpisodeContradiction(_Contract):
    id: UUID
    claim_ids: tuple[UUID, ...]
    kind: Literal[
        "opposite_polarity",
        "incompatible_values",
        "competing_temporal_state",
        "identity_ambiguity",
    ]
    status: Literal["unresolved", "contextualized", "resolved"] = "unresolved"
    explanation: str | None = None

    @model_validator(mode="after")
    def require_multiple_claims(self) -> "EpisodeContradiction":
        if len(set(self.claim_ids)) < 2:
            raise ValueError("a contradiction requires at least two distinct claims")
        return self


class EpisodeAccessManifest(_Contract):
    visibility: Literal["public", "tenant", "restricted", "unknown"]
    audience: tuple[dict[str, Any], ...] = ()
    policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_policy_hashes: tuple[str, ...] = Field(min_length=1)
    composition_version: str = Field(min_length=1)
    evaluated_at: datetime


class EpisodeCoverage(_Contract):
    eligible_observation_count: int = Field(ge=0)
    included_observation_count: int = Field(ge=0)
    reviewed_exclusion_count: int = Field(ge=0)
    unresolved_candidate_count: int = Field(ge=0)
    coverage_recall_proxy: float = Field(ge=0, le=1)
    contamination_precision_proxy: float = Field(ge=0, le=1)
    citation_completeness: float = Field(ge=0, le=1)
    contradiction_preservation: float = Field(ge=0, le=1)
    authorization_violation_count: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_counts(self) -> "EpisodeCoverage":
        if self.included_observation_count > self.eligible_observation_count:
            raise ValueError("included observations exceed evaluated candidates")
        return self


class EpisodeSettlement(_Contract):
    reason: Literal[
        "quiet_period",
        "explicit_close",
        "query_scope_satisfied",
        "superseded",
    ]
    rule_version: str = Field(min_length=1)
    event_time_watermark: datetime
    ingestion_time_watermark: datetime
    settled_at: datetime


class _EpisodeSnapshotPayload(_Contract):
    schema_version: Literal[1] = 1
    constitution_version: Literal[1] = 1
    id: UUID
    tenant_id: UUID
    topic_id: UUID
    episode_id: UUID
    version: int = Field(ge=1)
    lifecycle_state: Literal["open", "dormant", "settled", "reopened", "superseded"]
    prior_snapshot_id: UUID | None = None
    observation_ids: tuple[UUID, ...] = Field(min_length=1)
    evidence_ids: tuple[UUID, ...] = Field(min_length=1)
    claim_ids: tuple[UUID, ...]
    membership_assertion_ids: tuple[UUID, ...] = Field(min_length=1)
    contradictions: tuple[EpisodeContradiction, ...] = ()
    access: EpisodeAccessManifest
    coverage: EpisodeCoverage
    settlement: EpisodeSettlement | None = None
    opened_at: datetime
    cutoff_at: datetime
    created_at: datetime

    @model_validator(mode="after")
    def validate_snapshot(self) -> "_EpisodeSnapshotPayload":
        for name in (
            "observation_ids",
            "evidence_ids",
            "claim_ids",
            "membership_assertion_ids",
        ):
            values = getattr(self, name)
            if len(set(values)) != len(values):
                raise ValueError(f"{name} must not contain duplicates")
        if self.lifecycle_state == "settled" and self.settlement is None:
            raise ValueError("settled snapshots require a settlement decision")
        if self.cutoff_at < self.opened_at:
            raise ValueError("snapshot cutoff precedes episode opening")
        return self


def snapshot_manifest_hash(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class EpisodeSnapshot(_EpisodeSnapshotPayload):
    snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def verify_manifest_hash(self) -> "EpisodeSnapshot":
        payload = self.model_dump(mode="json", exclude={"snapshot_hash"})
        if self.snapshot_hash != snapshot_manifest_hash(payload):
            raise ValueError("episode snapshot hash does not match its manifest")
        return self

    @classmethod
    def seal(cls, **value: Any) -> "EpisodeSnapshot":
        payload = _EpisodeSnapshotPayload.model_validate(value)
        canonical = payload.model_dump(mode="json")
        return cls(
            **canonical,
            snapshot_hash=snapshot_manifest_hash(canonical),
        )


class ReasoningEpisodeInput(_Contract):
    contract_version: Literal[1] = 1
    tenant_id: UUID
    episode_snapshot_id: UUID
    episode_snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    mode: Literal["automatic_update", "query_answer"]
    requester_actor_id: UUID | None = None
    query_text: str | None = None
    authorized_evidence_ids: tuple[UUID, ...]
    claim_ids: tuple[UUID, ...]
    contradiction_ids: tuple[UUID, ...]
    created_at: datetime

    @model_validator(mode="after")
    def validate_query_mode(self) -> "ReasoningEpisodeInput":
        if self.mode == "query_answer":
            if not self.query_text or self.requester_actor_id is None:
                raise ValueError("query reasoning requires query text and requester")
        return self


__all__ = [
    "EpisodeAccessManifest",
    "EpisodeConstitution",
    "EpisodeContradiction",
    "EpisodeCoverage",
    "EpisodeMembershipAssertion",
    "EpisodeSettlement",
    "EpisodeSnapshot",
    "MembershipReason",
    "ReasoningEpisodeInput",
    "TopicIntent",
    "snapshot_manifest_hash",
]
