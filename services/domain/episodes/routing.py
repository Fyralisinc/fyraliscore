"""Pure, explainable cross-source episode routing policy."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from .contracts import MembershipReason


_TOKENS = re.compile(r"[A-Za-z][A-Za-z0-9_-]{2,}")
_STOP = {
    "about", "after", "also", "and", "are", "before", "been", "being",
    "but", "can", "did", "for", "from", "has", "have", "into", "its",
    "not", "now", "our", "that", "the", "their", "this", "was", "were",
    "what", "when", "where", "which", "who", "will", "with", "would",
}
_STRONG_TYPES = {
    "audit", "goal", "project", "workstream", "initiative", "milestone",
    "incident", "customer", "account", "service", "software_system",
    "repository", "work_item", "topic_phrase",
}


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RoutingSignal(_Frozen):
    tenant_id: UUID
    observation_id: UUID
    evidence_id: UUID
    identity_snapshot_id: UUID
    occurred_at: datetime
    ingested_at: datetime
    source: str
    installation_scope: str
    content_text: str
    primary_anchor: dict[str, Any]
    anchor_refs: tuple[dict[str, Any], ...]
    participant_refs: tuple[dict[str, Any], ...] = ()
    claim_ids: tuple[UUID, ...] = ()
    identity_assertion_ids: tuple[UUID, ...] = ()
    claim_predicates: tuple[str, ...] = ()
    lexical_terms: tuple[str, ...] = ()
    structure_keys: tuple[str, ...] = ()
    topic_label: str
    explicit_topic: bool = False


class TopicCandidate(_Frozen):
    topic_id: UUID
    episode_id: UUID
    primary_anchor: dict[str, Any]
    anchor_refs: tuple[dict[str, Any], ...]
    claim_predicates: tuple[str, ...]
    lexical_terms: tuple[str, ...]
    structure_keys: tuple[str, ...] = ()
    last_event_at: datetime


class MembershipDecisionValue(_Frozen):
    topic_id: UUID
    episode_id: UUID
    decision: Literal["include", "exclude", "hold"]
    score: float = Field(ge=0, le=1)
    reasons: tuple[MembershipReason, ...]
    feature_snapshot: dict[str, Any]


def canonical_ref(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def topic_key(*, tenant_id: UUID, origin: str, primary_anchor: dict[str, Any]) -> str:
    payload = {
        "tenant_id": str(tenant_id),
        "origin_scope": "query" if origin == "query_seeded" else "shared",
        "primary_anchor": primary_anchor,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def lexical_terms(text: str, *, limit: int = 40) -> tuple[str, ...]:
    values = {match.group(0).lower() for match in _TOKENS.finditer(text)}
    return tuple(sorted(values.difference(_STOP)))[:limit]


def anchor_is_strong(anchor: dict[str, Any]) -> bool:
    return str(anchor.get("type") or anchor.get("kind")) in _STRONG_TYPES


def _overlap(left: tuple[str, ...], right: tuple[str, ...]) -> float:
    a, b = set(left), set(right)
    if not a or not b:
        return 0.0
    return len(a.intersection(b)) / len(a.union(b))


def _refs(values: tuple[dict[str, Any], ...]) -> set[str]:
    return {canonical_ref(value) for value in values}


def score_membership(
    signal: RoutingSignal, candidate: TopicCandidate
) -> MembershipDecisionValue:
    signal_refs = _refs(signal.anchor_refs)
    candidate_refs = _refs(candidate.anchor_refs)
    shared_refs = signal_refs.intersection(candidate_refs)
    primary_equal = canonical_ref(signal.primary_anchor) == canonical_ref(
        candidate.primary_anchor
    )
    entity_overlap = (
        len(shared_refs) / min(len(signal_refs), len(candidate_refs))
        if signal_refs and candidate_refs
        else 0.0
    )
    claim_overlap = _overlap(signal.claim_predicates, candidate.claim_predicates)
    lexical_overlap = _overlap(signal.lexical_terms, candidate.lexical_terms)
    structure_overlap = _overlap(signal.structure_keys, candidate.structure_keys)
    hours = abs((signal.occurred_at - candidate.last_event_at).total_seconds()) / 3600
    temporal = max(0.0, 1.0 - hours / (24 * 30))
    conflicting_primary = (
        anchor_is_strong(signal.primary_anchor)
        and anchor_is_strong(candidate.primary_anchor)
        and str(signal.primary_anchor.get("type"))
        == str(candidate.primary_anchor.get("type"))
        and not primary_equal
    )

    score = min(
        1.0,
        (0.70 if primary_equal else 0.45 * entity_overlap)
        + 0.18 * claim_overlap
        + 0.16 * structure_overlap
        + 0.12 * lexical_overlap
        + 0.04 * temporal,
    )
    if conflicting_primary:
        score = min(score, 0.20)

    reasons: list[MembershipReason] = []
    if primary_equal or entity_overlap:
        reasons.append(
            MembershipReason(
                code="entity_overlap",
                weight=0.70 if primary_equal else 0.45 * entity_overlap,
                detail={"primary_equal": primary_equal, "shared": sorted(shared_refs)},
            )
        )
    if claim_overlap:
        reasons.append(MembershipReason(code="claim_overlap", weight=0.18 * claim_overlap))
    if structure_overlap:
        reasons.append(
            MembershipReason(code="thread_or_container", weight=0.16 * structure_overlap)
        )
    if lexical_overlap:
        reasons.append(
            MembershipReason(
                code="lexical_semantic_match", weight=0.12 * lexical_overlap
            )
        )
    if temporal:
        reasons.append(MembershipReason(code="temporal_proximity", weight=0.04 * temporal))
    if conflicting_primary:
        reasons.append(
            MembershipReason(
                code="hard_negative", weight=-1.0,
                detail={"reason": "conflicting_stable_primary_anchor"},
            )
        )
    if not reasons:
        reasons.append(
            MembershipReason(code="hard_negative", weight=-1.0, detail={"reason": "no_overlap"})
        )

    if score >= 0.65 and not conflicting_primary:
        decision: Literal["include", "exclude", "hold"] = "include"
    elif score >= 0.30 and not conflicting_primary:
        decision = "hold"
    else:
        decision = "exclude"
    features = {
        "primary_anchor_equal": primary_equal,
        "entity_overlap": entity_overlap,
        "claim_overlap": claim_overlap,
        "structure_overlap": structure_overlap,
        "lexical_overlap": lexical_overlap,
        "temporal_proximity": temporal,
        "conflicting_primary_anchor": conflicting_primary,
        "source": signal.source,
        "installation_scope": signal.installation_scope,
    }
    return MembershipDecisionValue(
        topic_id=candidate.topic_id,
        episode_id=candidate.episode_id,
        decision=decision,
        score=score,
        reasons=tuple(reasons),
        feature_snapshot=features,
    )


__all__ = [
    "MembershipDecisionValue", "RoutingSignal", "TopicCandidate",
    "anchor_is_strong", "canonical_ref", "lexical_terms", "score_membership",
    "topic_key",
]
