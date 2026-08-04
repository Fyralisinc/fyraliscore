"""Deterministic, explainable entity candidate ranking and decisions."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .foundation import EntityMentionRow


def canonical_ref_key(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CandidateSeed(_Contract):
    candidate_ref: dict[str, Any]
    retrieval_method: Literal[
        "deterministic_source_ref",
        "accepted_principal_mapping",
        "structured_hint",
        "exact_alias",
        "fuzzy_alias",
        "actor_name",
        "context_provider",
    ]
    features: dict[str, float] = Field(default_factory=dict)
    expected_type: str | None = None

    @model_validator(mode="after")
    def validate_ref_and_features(self) -> "CandidateSeed":
        if not self.candidate_ref:
            raise ValueError("candidate refs cannot be empty")
        if any(value < 0 or value > 1 for value in self.features.values()):
            raise ValueError("candidate feature values must be in [0,1]")
        return self


class IdentityConstraintValue(_Contract):
    id: UUID
    kind: Literal["must_link", "cannot_link"]
    left_ref: dict[str, Any]
    right_ref: dict[str, Any]
    authority: Literal["source", "system", "human"]
    valid_from: datetime
    valid_to: datetime | None = None


class IdentityConstraintCreate(_Contract):
    tenant_id: UUID
    kind: Literal["must_link", "cannot_link"]
    left_ref: dict[str, Any]
    right_ref: dict[str, Any]
    authority: Literal["source", "system", "human"]
    evidence_id: UUID | None = None
    provenance: dict[str, Any] = Field(default_factory=dict)
    valid_from: datetime
    valid_to: datetime | None = None

    @model_validator(mode="after")
    def validate_refs_and_window(self) -> "IdentityConstraintCreate":
        if not self.left_ref or not self.right_ref:
            raise ValueError("identity constraints require two refs")
        if self.valid_to and self.valid_to < self.valid_from:
            raise ValueError("constraint valid-time window is reversed")
        return self


class RankedCandidate(_Contract):
    candidate_ref: dict[str, Any]
    candidate_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    retrieval_methods: tuple[str, ...]
    features: dict[str, float]
    score: float = Field(ge=0, le=1)
    rank: int = Field(ge=1)
    constraint_outcome: Literal[
        "allowed", "must_link", "cannot_link", "type_rejected"
    ]
    reasons: tuple[str, ...]


class ResolutionDecision(_Contract):
    mention_id: UUID
    outcome: Literal["resolved", "probable", "ambiguous", "unresolved"]
    selected_ref: dict[str, Any] | None = None
    confidence: float = Field(ge=0, le=1)
    alternatives: tuple[dict[str, Any], ...] = ()
    reasons: tuple[str, ...]

    @model_validator(mode="after")
    def validate_selection(self) -> "ResolutionDecision":
        if self.outcome in {"resolved", "probable"} and self.selected_ref is None:
            raise ValueError("resolved decisions require a selected ref")
        if self.outcome == "unresolved" and self.selected_ref is not None:
            raise ValueError("unresolved decisions cannot select a ref")
        return self


class ResolutionThreshold(_Contract):
    auto_accept: float = Field(ge=0, le=1)
    probable: float = Field(ge=0, le=1)
    ambiguity_floor: float = Field(ge=0, le=1)
    minimum_margin: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def validate_order(self) -> "ResolutionThreshold":
        if not self.auto_accept >= self.probable >= self.ambiguity_floor:
            raise ValueError("resolution thresholds are not monotonic")
        return self


DEFAULT_THRESHOLDS: dict[str, ResolutionThreshold] = {
    "person": ResolutionThreshold(
        auto_accept=0.98, probable=0.85, ambiguity_floor=0.55, minimum_margin=0.12
    ),
    "default": ResolutionThreshold(
        auto_accept=0.95, probable=0.82, ambiguity_floor=0.55, minimum_margin=0.15
    ),
}


_WEIGHTS = {
    "exact_alias": 0.40,
    "alias_confidence": 0.25,
    "name_similarity": 0.45,
    "type_compatibility": 0.10,
    "context_similarity": 0.05,
}


def _ref_type(ref: dict[str, Any]) -> str:
    value = str(ref.get("type") or ref.get("kind") or "unknown")
    return "person" if value in {"actor", "person", "source_principal"} else value


def _type_compatible(mention: EntityMentionRow, candidate: CandidateSeed) -> bool:
    if not mention.expected_types:
        return True
    candidate_type = candidate.expected_type or _ref_type(candidate.candidate_ref)
    return candidate_type in mention.expected_types


def _merge_seeds(seeds: list[CandidateSeed]) -> list[CandidateSeed]:
    grouped: dict[str, list[CandidateSeed]] = {}
    for seed in seeds:
        grouped.setdefault(canonical_ref_key(seed.candidate_ref), []).append(seed)
    merged: list[CandidateSeed] = []
    for key in sorted(grouped):
        values = grouped[key]
        features: dict[str, float] = {}
        for value in values:
            for name, score in value.features.items():
                features[name] = max(features.get(name, 0.0), score)
        methods = sorted({item.retrieval_method for item in values})
        # The method list is retained later; encode all methods as binary features
        # while preserving one legal seed method for this intermediate value.
        for method in methods:
            features[f"retrieval:{method}"] = 1.0
        merged.append(
            CandidateSeed(
                candidate_ref=values[0].candidate_ref,
                retrieval_method=values[0].retrieval_method,
                features=features,
                expected_type=next(
                    (item.expected_type for item in values if item.expected_type), None
                ),
            )
        )
    return merged


def _constraint_for(
    mention: EntityMentionRow,
    seed: CandidateSeed,
    constraints: list[IdentityConstraintValue],
    at: datetime,
) -> IdentityConstraintValue | None:
    mention_refs = (
        {"kind": "mention", "id": str(mention.id)},
        {"kind": "source_reference", "id": str(mention.source_reference_id)}
        if mention.source_reference_id
        else {},
    )
    for constraint in constraints:
        if constraint.valid_from > at or (
            constraint.valid_to is not None and constraint.valid_to < at
        ):
            continue
        left_matches = constraint.left_ref in mention_refs
        right_matches = constraint.right_ref == seed.candidate_ref
        reverse_matches = (
            constraint.right_ref in mention_refs
            and constraint.left_ref == seed.candidate_ref
        )
        if (left_matches and right_matches) or reverse_matches:
            return constraint
    return None


def _score(seed: CandidateSeed) -> float:
    if seed.features.get("retrieval:accepted_principal_mapping") == 1.0:
        return 1.0
    if seed.features.get("retrieval:deterministic_source_ref") == 1.0:
        return 1.0
    if seed.features.get("retrieval:structured_hint") == 1.0:
        return 0.99
    return min(
        1.0,
        sum(_WEIGHTS.get(name, 0.0) * value for name, value in seed.features.items()),
    )


def rank_candidates(
    mention: EntityMentionRow,
    seeds: list[CandidateSeed],
    *,
    constraints: list[IdentityConstraintValue],
    evaluated_at: datetime,
) -> list[RankedCandidate]:
    provisional: list[tuple[CandidateSeed, float, str, tuple[str, ...]]] = []
    for seed in _merge_seeds(seeds):
        methods = tuple(
            sorted(
                name.removeprefix("retrieval:")
                for name, value in seed.features.items()
                if name.startswith("retrieval:") and value == 1.0
            )
        )
        if not _type_compatible(mention, seed):
            provisional.append((seed, 0.0, "type_rejected", ("type_mismatch",)))
            continue
        constraint = _constraint_for(mention, seed, constraints, evaluated_at)
        if constraint and constraint.kind == "cannot_link":
            provisional.append(
                (seed, 0.0, "cannot_link", (f"constraint:{constraint.id}",))
            )
            continue
        if constraint and constraint.kind == "must_link":
            provisional.append(
                (seed, 1.0, "must_link", (f"constraint:{constraint.id}",))
            )
            continue
        score = _score(seed)
        reasons = tuple(
            f"{name}={value:.3f}"
            for name, value in sorted(seed.features.items())
            if not name.startswith("retrieval:") and value > 0
        )
        provisional.append((seed, score, "allowed", reasons))

    provisional.sort(
        key=lambda item: (-item[1], canonical_ref_key(item[0].candidate_ref))
    )
    return [
        RankedCandidate(
            candidate_ref=seed.candidate_ref,
            candidate_key=canonical_ref_key(seed.candidate_ref),
            retrieval_methods=tuple(
                sorted(
                    name.removeprefix("retrieval:")
                    for name, value in seed.features.items()
                    if name.startswith("retrieval:") and value == 1.0
                )
            ),
            features={
                name: value
                for name, value in seed.features.items()
                if not name.startswith("retrieval:")
            },
            score=score,
            rank=index,
            constraint_outcome=outcome,  # type: ignore[arg-type]
            reasons=reasons,
        )
        for index, (seed, score, outcome, reasons) in enumerate(provisional, start=1)
    ]


def decide_resolution(
    mention: EntityMentionRow,
    ranked: list[RankedCandidate],
    *,
    thresholds: dict[str, ResolutionThreshold] | None = None,
) -> ResolutionDecision:
    thresholds = thresholds or DEFAULT_THRESHOLDS
    expected = mention.expected_types[0] if mention.expected_types else "default"
    policy = thresholds.get(expected, thresholds["default"])
    viable = [
        item
        for item in ranked
        if item.constraint_outcome in {"allowed", "must_link"}
        and item.score >= policy.ambiguity_floor
    ]
    if not viable:
        return ResolutionDecision(
            mention_id=mention.id,
            outcome="unresolved",
            confidence=0.0,
            reasons=("no_candidate_passed_constraints_and_floor",),
        )

    top = viable[0]
    runner_up = viable[1] if len(viable) > 1 else None
    margin = top.score - (runner_up.score if runner_up else 0.0)
    deterministic = top.constraint_outcome == "must_link" or any(
        method in {
            "deterministic_source_ref",
            "accepted_principal_mapping",
            "structured_hint",
        }
        for method in top.retrieval_methods
    )
    alternatives = tuple(item.candidate_ref for item in viable[1:])
    reasons = (*top.reasons, f"margin={margin:.3f}")
    if deterministic or (
        top.score >= policy.auto_accept and margin >= policy.minimum_margin
    ):
        return ResolutionDecision(
            mention_id=mention.id,
            outcome="resolved",
            selected_ref=top.candidate_ref,
            confidence=top.score,
            alternatives=alternatives,
            reasons=reasons,
        )
    if top.score >= policy.probable and margin >= policy.minimum_margin:
        return ResolutionDecision(
            mention_id=mention.id,
            outcome="probable",
            selected_ref=top.candidate_ref,
            confidence=top.score,
            alternatives=alternatives,
            reasons=reasons,
        )
    return ResolutionDecision(
        mention_id=mention.id,
        outcome="ambiguous",
        selected_ref=top.candidate_ref,
        confidence=top.score,
        alternatives=tuple(item.candidate_ref for item in viable[1:]),
        reasons=(*reasons, "acceptance_margin_not_met"),
    )


class IdentitySnapshotItem(_Contract):
    mention_id: UUID
    outcome: Literal["resolved", "probable", "ambiguous", "unresolved"]
    selected_ref: dict[str, Any] | None = None
    confidence: float = Field(ge=0, le=1)
    assertion_id: UUID | None = None
    alternatives: tuple[dict[str, Any], ...] = ()
    reasons: tuple[str, ...] = ()


class _SnapshotPayload(_Contract):
    schema_version: Literal[1] = 1
    id: UUID
    tenant_id: UUID
    resolver_run_id: UUID
    input_kind: Literal["observation", "query", "reprocess"]
    observation_id: UUID | None = None
    observation_occurred_at: datetime | None = None
    requester_actor_id: UUID | None = None
    resolution_status: Literal["complete", "partial"]
    items: tuple[IdentitySnapshotItem, ...]
    access_policy_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    resolver_name: str
    resolver_version: str
    policy_version: str
    created_at: datetime

    @model_validator(mode="after")
    def validate_origin(self) -> "_SnapshotPayload":
        if self.input_kind != "query" and (
            self.observation_id is None or self.observation_occurred_at is None
        ):
            raise ValueError("non-query snapshots require an observation")
        if len({item.mention_id for item in self.items}) != len(self.items):
            raise ValueError("snapshot mentions must be unique")
        return self


def snapshot_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


class IdentityResolutionSnapshot(_SnapshotPayload):
    snapshot_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def verify_hash(self) -> "IdentityResolutionSnapshot":
        payload = self.model_dump(mode="json", exclude={"snapshot_hash"})
        if snapshot_hash(payload) != self.snapshot_hash:
            raise ValueError("identity snapshot hash does not match manifest")
        return self

    @classmethod
    def seal(cls, **value: Any) -> "IdentityResolutionSnapshot":
        payload = _SnapshotPayload.model_validate(value)
        canonical = payload.model_dump(mode="json")
        return cls(**canonical, snapshot_hash=snapshot_hash(canonical))


__all__ = [
    "CandidateSeed",
    "DEFAULT_THRESHOLDS",
    "IdentityConstraintValue",
    "IdentityConstraintCreate",
    "IdentityResolutionSnapshot",
    "IdentitySnapshotItem",
    "RankedCandidate",
    "ResolutionDecision",
    "ResolutionThreshold",
    "canonical_ref_key",
    "decide_resolution",
    "rank_candidates",
    "snapshot_hash",
]
