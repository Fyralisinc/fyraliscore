"""Candidate generation and scoring for relationship intelligence.

The important architectural boundary: candidates are not accepted truth.
They are inspectable hypotheses that may later become `model_edges` or
composite `situation` Models after LLM/human adjudication.

Candidate generation is structured per-edge-kind. Each kind has a small
rule function that returns a candidate only when its preconditions are
satisfied. Topology and other deterministic callers loop the relevant
rules across pairs; the loop never coerces a pair into a kind that does
not fit. Heuristic "pick one kind per pair" generators are intentionally
gone — they produced candidate spam.
"""

from __future__ import annotations

import math
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Literal, Optional, Sequence
from uuid import UUID

from lib.shared.ids import uuid7
from lib.shared.memory_grammar import derive_memory_grammar
from services.reasoning.judgment.scoring import JudgmentScores, clamp_score


CandidateKind = Literal["edge", "situation", "edge_type"]
CandidateLifecycleStage = Literal["memory_proposal"]
CandidateOriginStage = Literal["pattern_discovery", "direct_proposal"]
TopologyPatternKind = Literal["pair", "situation", "edge_type"]
CandidateBasis = Literal[
    "observed",
    "inferred",
    "correlated",
    "topology_suggested",
    "causal_hypothesis",
    "causal_confirmed",
    "ontology_gap",
]
ReviewStatus = Literal[
    "candidate",
    "needs_review",
    "accepted",
    "rejected",
    "contested",
    "retired",
]


_CAUSAL_EDGE_KINDS = {"causes", "explains", "blocks", "enables"}
_SITUATION_PRESSURE_TYPES = {
    "capacity",
    "trust",
    "revenue",
    "compliance",
    "decision",
    "execution",
    "market",
    "resource",
}


# Edge kinds that deterministic / topology generators are allowed to
# emit. Everything else is LLM-only — Think must propose those kinds
# directly; topology should not fabricate them.
TOPOLOGY_EMITTABLE_EDGE_KINDS: frozenset[str] = frozenset(
    {
        "same_issue_as",
        "supports",
        "analogous_to",
        "blocks",
        "early_warning_for",
        "contradicts",
        "weakens",
        "explains",
        "predicts",
        "contributes_to_resolution",
        "causes",
        "enables",
    }
)


def candidate_lifecycle_metadata(
    metadata: dict[str, Any] | None,
    *,
    candidate_kind: CandidateKind,
    source: str,
) -> dict[str, Any]:
    """Annotate candidates with one lifecycle vocabulary.

    `relationship_candidates` is the one persisted pre-truth candidate
    lifecycle. Topology is an upstream discovery origin for some of those
    proposals, not a second candidate system.
    """

    out = dict(metadata or {})
    raw_lifecycle = out.get("candidate_lifecycle")
    lifecycle = dict(raw_lifecycle) if isinstance(raw_lifecycle, dict) else {}
    lifecycle.setdefault("stage", "memory_proposal")
    lifecycle.setdefault("proposal_kind", candidate_kind)
    lifecycle.setdefault("origin", source)
    lifecycle.setdefault(
        "origin_stage",
        "pattern_discovery" if source == "latent_topology" else "direct_proposal",
    )
    out["candidate_lifecycle"] = lifecycle
    return out


def make_topology_candidate_metadata(
    *,
    proposal_kind: CandidateKind,
    pattern_kind: TopologyPatternKind,
    score_components: dict[str, Any],
    impact_signatures: Sequence[dict[str, Any]],
    selection_sources: Sequence[str] = (),
) -> dict[str, Any]:
    """Build metadata for topology-discovered relationship proposals."""

    legacy_object_type = f"{pattern_kind}_candidate"
    return candidate_lifecycle_metadata(
        {
            "candidate_lifecycle": {
                "stage": "memory_proposal",
                "origin": "latent_topology",
                "origin_stage": "pattern_discovery",
                "proposal_kind": proposal_kind,
                "discovery_pattern_kind": pattern_kind,
            },
            # Kept for compatibility with older reports/tests. New code should
            # prefer `candidate_lifecycle`.
            "topology": {
                "kind": "latent_relationship_field",
                "object_type": legacy_object_type,
                "selection_sources": list(selection_sources),
                "score_components": score_components,
                "impact_signatures": list(impact_signatures),
            },
        },
        candidate_kind=proposal_kind,
        source="latent_topology",
    )


@dataclass(frozen=True)
class ModelSignal:
    """Small model projection used by deterministic candidate generation."""

    id: UUID
    natural: str
    proposition_kind: str
    confidence: float = 0.5
    activation: float = 0.0
    scope_entities: tuple[tuple[str, UUID], ...] = ()
    scope_actors: tuple[UUID, ...] = ()
    created_at: datetime | None = None
    # Optional richer features used by some per-kind rules. These are
    # all best-effort: rules degrade gracefully when fields are absent.
    embedding: tuple[float, ...] = ()
    workstream: str | None = None
    time_shape: str = "unspecified"
    polarity: str | None = None  # 'positive' | 'negative' | None
    proposition: dict[str, Any] = field(default_factory=dict)
    is_leading_indicator: bool = False
    historical_cooccurrence_with: tuple[UUID, ...] = ()
    blocker_targets: tuple[UUID, ...] = ()
    blocker_text_refs: tuple[str, ...] = ()
    capability_surface: str | None = None
    evidence_event_ids: tuple[UUID, ...] = ()


@dataclass(frozen=True)
class RelationshipCandidate:
    id: UUID
    tenant_id: UUID
    candidate_kind: CandidateKind
    basis: CandidateBasis
    explanation: str
    scores: JudgmentScores
    review_status: ReviewStatus = "candidate"
    source_model_id: UUID | None = None
    target_model_id: UUID | None = None
    edge_kind: str | None = None
    member_model_ids: tuple[UUID, ...] = ()
    evidence_event_ids: tuple[UUID, ...] = ()
    evidence_model_ids: tuple[UUID, ...] = ()
    counterevidence_model_ids: tuple[UUID, ...] = ()
    proposed_proposition: dict[str, Any] | None = None
    source: str = "relationship_candidate_service"
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def judgment_leverage_score(self) -> float:
        return self.scores.judgment_leverage

    def to_record(self) -> dict[str, Any]:
        scores = self.scores.as_dict()
        metadata = candidate_lifecycle_metadata(
            self.metadata,
            candidate_kind=self.candidate_kind,
            source=self.source,
        )
        return {
            "id": self.id,
            "tenant_id": self.tenant_id,
            "candidate_kind": self.candidate_kind,
            "basis": self.basis,
            "source_model_id": self.source_model_id,
            "target_model_id": self.target_model_id,
            "edge_kind": self.edge_kind,
            "member_model_ids": list(self.member_model_ids),
            "evidence_event_ids": list(self.evidence_event_ids),
            "evidence_model_ids": list(self.evidence_model_ids),
            "counterevidence_model_ids": list(self.counterevidence_model_ids),
            "proposed_proposition": self.proposed_proposition,
            "explanation": self.explanation,
            "novelty_score": scores["novelty"],
            "impact_score": scores["impact"],
            "actionability_score": scores["actionability"],
            "urgency_score": scores["urgency"],
            "uncertainty_score": scores["uncertainty"],
            "authority_required_score": scores["authority_required"],
            "reversibility_score": scores["reversibility"],
            "confidence_score": scores["confidence"],
            "judgment_leverage_score": scores["judgment_leverage"],
            "source": self.source,
            "review_status": self.review_status,
            "metadata": metadata,
        }


def make_edge_candidate(
    *,
    tenant_id: UUID,
    source_model_id: UUID,
    target_model_id: UUID,
    edge_kind: str,
    basis: CandidateBasis,
    explanation: str,
    scores: JudgmentScores,
    evidence_model_ids: tuple[UUID, ...] = (),
    evidence_event_ids: tuple[UUID, ...] = (),
    mechanism_summary: str | None = None,
    intervention_surface: str | None = None,
    expected_delay: str | None = None,
    confounders: tuple[str, ...] = (),
    metadata: dict[str, Any] | None = None,
    source: str = "relationship_candidate_service",
    review_status: ReviewStatus = "candidate",
) -> RelationshipCandidate:
    if source_model_id == target_model_id:
        raise ValueError("relationship candidate cannot be a self-edge")
    if basis in {"causal_hypothesis", "causal_confirmed"}:
        if not mechanism_summary or not mechanism_summary.strip():
            raise ValueError("causal relationship candidate requires mechanism_summary")
    candidate_metadata = dict(metadata or {})
    if (
        mechanism_summary
        or intervention_surface
        or expected_delay
        or confounders
        or basis in {"causal_hypothesis", "causal_confirmed"}
    ):
        candidate_metadata["causal"] = {
            "basis": basis,
            "mechanism_summary": mechanism_summary,
            "intervention_surface": intervention_surface,
            "expected_delay": expected_delay,
            "confounders": list(confounders),
        }
    return RelationshipCandidate(
        id=uuid7(),
        tenant_id=tenant_id,
        candidate_kind="edge",
        basis=basis,
        source_model_id=source_model_id,
        target_model_id=target_model_id,
        edge_kind=edge_kind,
        member_model_ids=(source_model_id, target_model_id),
        evidence_model_ids=evidence_model_ids or (source_model_id, target_model_id),
        evidence_event_ids=evidence_event_ids,
        explanation=explanation,
        scores=scores,
        source=source,
        metadata=candidate_metadata,
        review_status=review_status,
    )


def make_situation_candidate(
    *,
    tenant_id: UUID,
    situation: str,
    summary: str,
    relationship_summary: str,
    member_model_ids: tuple[UUID, ...],
    basis: CandidateBasis,
    scores: JudgmentScores,
    evidence_event_ids: tuple[UUID, ...] = (),
    metadata: dict[str, Any] | None = None,
    source: str = "relationship_candidate_service",
    review_status: ReviewStatus = "candidate",
) -> RelationshipCandidate:
    members = tuple(dict.fromkeys(member_model_ids))
    if len(members) < 2:
        raise ValueError("situation candidate requires at least two member models")
    candidate_metadata = dict(metadata or {})
    pressure_type = candidate_metadata.get("pressure_type")
    if pressure_type not in _SITUATION_PRESSURE_TYPES:
        pressure_type = "execution"
    proposition = {
        "kind": "belief",
        "legacy_kind": "situation",
        "claim_role": "situation",
        "abstraction_level": "composite",
        "time_mode": "current",
        "modality": "inferred",
        "polarity": "mixed",
        "situation": situation,
        "summary": summary,
        "member_model_ids": [str(m) for m in members],
        "relationship_summary": relationship_summary,
        "status": "forming",
        "pressure_type": pressure_type,
        "shared_mechanism": relationship_summary,
        "judgment_change": (
            "Treat the member models as one composite situation because "
            f"{relationship_summary}"
        ),
        "open_falsifier": (
            "The shared mechanism no longer holds, or the member models "
            "stop being jointly true."
        ),
    }
    return RelationshipCandidate(
        id=uuid7(),
        tenant_id=tenant_id,
        candidate_kind="situation",
        basis=basis,
        member_model_ids=members,
        evidence_model_ids=members,
        evidence_event_ids=evidence_event_ids,
        proposed_proposition=proposition,
        explanation=relationship_summary,
        scores=scores,
        source=source,
        metadata=candidate_metadata,
        review_status=review_status,
    )


def make_edge_type_candidate(
    *,
    tenant_id: UUID,
    proposed_edge_kind: str,
    description: str,
    relationship_summary: str,
    scores: JudgmentScores,
    parent_kind: str | None = None,
    nearest_existing_kind: str | None = None,
    directionality: Literal["directed", "symmetric", "unknown"] = "unknown",
    inverse_label: str | None = None,
    dropped_dimensions: tuple[str, ...] = (),
    evidence_model_ids: tuple[UUID, ...] = (),
    evidence_event_ids: tuple[UUID, ...] = (),
    example_source_model_id: UUID | None = None,
    example_target_model_id: UUID | None = None,
    promotion_criteria: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    source: str = "relationship_ontology_gap",
    review_status: ReviewStatus = "needs_review",
) -> RelationshipCandidate:
    """Create a pre-truth proposal for a missing edge kind.

    This is intentionally not a `model_edges` row and not a normal edge
    candidate. It records that a valuable relationship was observed, but the
    current ontology cannot represent it without losing useful semantics.
    """
    normalized_kind = proposed_edge_kind.strip()
    if not normalized_kind:
        raise ValueError("edge type candidate requires proposed_edge_kind")
    if not description.strip():
        raise ValueError("edge type candidate requires description")
    if not relationship_summary.strip():
        raise ValueError("edge type candidate requires relationship_summary")
    if (
        example_source_model_id is None
        and example_target_model_id is not None
        or example_source_model_id is not None
        and example_target_model_id is None
    ):
        raise ValueError(
            "edge type candidate examples require both source and target model ids",
        )
    if (
        example_source_model_id is not None
        and example_source_model_id == example_target_model_id
    ):
        raise ValueError("edge type candidate example cannot be a self-edge")

    example_models: tuple[UUID, ...] = ()
    examples: list[dict[str, str]] = []
    if example_source_model_id is not None and example_target_model_id is not None:
        example_models = (example_source_model_id, example_target_model_id)
        examples.append(
            {
                "source_model_id": str(example_source_model_id),
                "target_model_id": str(example_target_model_id),
            }
        )

    proposal = {
        "kind": "ontology_gap",
        "proposed_edge_kind": normalized_kind,
        "description": description.strip(),
        "relationship_summary": relationship_summary.strip(),
        "parent_kind": parent_kind or nearest_existing_kind,
        "nearest_existing_kind": nearest_existing_kind,
        "directionality": directionality,
        "inverse_label": inverse_label,
        "dropped_dimensions": list(dropped_dimensions),
        "examples": examples,
        "promotion_criteria": promotion_criteria
        or {
            "minimum_distinct_examples": 3,
            "requires_human_or_llm_adjudication": True,
            "requires_registry_spec": True,
        },
    }
    candidate_metadata = dict(metadata or {})
    candidate_metadata["ontology_gap"] = {
        "proposed_edge_kind": normalized_kind,
        "nearest_existing_kind": nearest_existing_kind,
        "parent_kind": parent_kind or nearest_existing_kind,
        "dropped_dimensions": list(dropped_dimensions),
        "retrieval_fallback_kind": parent_kind or nearest_existing_kind,
    }
    return RelationshipCandidate(
        id=uuid7(),
        tenant_id=tenant_id,
        candidate_kind="edge_type",
        basis="ontology_gap",
        member_model_ids=example_models,
        evidence_model_ids=evidence_model_ids or example_models,
        evidence_event_ids=evidence_event_ids,
        proposed_proposition=proposal,
        explanation=(
            f"Proposed edge type `{normalized_kind}` because "
            f"{relationship_summary.strip()}"
        ),
        scores=scores,
        source=source,
        metadata=candidate_metadata,
        review_status=review_status,
    )


def rank_candidates(
    candidates: list[RelationshipCandidate],
    *,
    limit: int | None = None,
) -> list[RelationshipCandidate]:
    ordered = sorted(
        candidates,
        key=lambda c: (
            -c.judgment_leverage_score,
            -clamp_score(c.scores.impact),
            -clamp_score(c.scores.confidence),
            str(c.id),
        ),
    )
    return ordered[:limit] if limit is not None else ordered


# =====================================================================
# Per-edge-kind candidate rules.
#
# Each rule is a pure function (tenant_id, scope_metadata, left, right)
# -> RelationshipCandidate | None. A rule returns None when its
# preconditions are not satisfied; the loop tries every rule on every
# pair, so a pair can produce zero, one, or several candidates of
# DIFFERENT kinds, but never a coerced "least-bad" kind.
# =====================================================================


CandidateRule = Callable[
    [UUID, dict[str, Any], ModelSignal, ModelSignal],
    Optional[RelationshipCandidate],
]


def _cosine(left: tuple[float, ...], right: tuple[float, ...]) -> float | None:
    if not left or not right or len(left) != len(right):
        return None
    num = sum(float(a) * float(b) for a, b in zip(left, right))
    da = math.sqrt(sum(float(a) * float(a) for a in left))
    db = math.sqrt(sum(float(b) * float(b) for b in right))
    if da == 0.0 or db == 0.0:
        return None
    return num / (da * db)


def _normalize_text(text: str) -> str:
    return " ".join(text.lower().split())


def _scope_entity_set(sig: ModelSignal) -> set[tuple[str, UUID]]:
    return {(t, eid) for (t, eid) in sig.scope_entities}


def _shared_entities(left: ModelSignal, right: ModelSignal) -> set[tuple[str, UUID]]:
    return _scope_entity_set(left) & _scope_entity_set(right)


def _same_workstream(left: ModelSignal, right: ModelSignal) -> bool:
    if not left.workstream or not right.workstream:
        return False
    return left.workstream == right.workstream


def _same_scope(left: ModelSignal, right: ModelSignal) -> bool:
    return bool(_shared_entities(left, right)) or _same_workstream(left, right)


_CONCRETE_EDGE_SCOPE_TYPES = frozenset(
    {
        "customer",
        "customer_resource",
        "commitment",
    }
)


def _shared_concrete_entities(
    left: ModelSignal,
    right: ModelSignal,
) -> set[tuple[str, UUID]]:
    return {
        scope
        for scope in _shared_entities(left, right)
        if scope[0] in _CONCRETE_EDGE_SCOPE_TYPES
    }


def _has_concrete_scope(signal: ModelSignal) -> bool:
    return any(scope_type in _CONCRETE_EDGE_SCOPE_TYPES for scope_type, _ in signal.scope_entities)


def _precise_edge_scope_compatible(left: ModelSignal, right: ModelSignal) -> bool:
    """Return whether a pair is scoped tightly enough for a direct precise edge.

    Broad objects such as goals/decisions can gather useful retrieval context,
    but they are too coarse to justify causal/blocking/warning edges across
    unrelated customers. Allow no-scope/internal pairs for legacy callers, and
    otherwise require a shared concrete business object.
    """
    if _shared_concrete_entities(left, right):
        return True
    if not left.scope_entities and not right.scope_entities:
        return True
    if _has_concrete_scope(left) or _has_concrete_scope(right):
        return False
    return False


_DIAGNOSTIC_EDGE_ENDPOINT_TERMS = (
    "unrecorded mutation",
    "state discontinuity",
    "consecutive audit events",
    "mutation gap",
    "missing transition",
)


def _signal_text(signal: ModelSignal) -> str:
    return " ".join(
        [
            signal.natural,
            json.dumps(signal.proposition, sort_keys=True, default=str),
        ]
    ).lower()


def _is_diagnostic_endpoint(signal: ModelSignal) -> bool:
    text = _signal_text(signal)
    return any(term in text for term in _DIAGNOSTIC_EDGE_ENDPOINT_TERMS)


def _is_composite_endpoint(signal: ModelSignal) -> bool:
    if signal.proposition_kind == "situation":
        return True
    proposition = signal.proposition or {}
    if proposition.get("claim_role") == "situation":
        return True
    if proposition.get("abstraction_level") == "composite":
        return True
    return False


def _eligible_precise_edge_endpoints(
    left: ModelSignal,
    right: ModelSignal,
) -> bool:
    return not (
        _is_diagnostic_endpoint(left)
        or _is_diagnostic_endpoint(right)
        or _is_composite_endpoint(left)
        or _is_composite_endpoint(right)
    )


def _contains_any(text: str, phrases: Sequence[str]) -> bool:
    normalized = _normalize_text(text)
    return any(phrase in normalized for phrase in phrases)


def _orient_by_activation(
    left: ModelSignal, right: ModelSignal
) -> tuple[ModelSignal, ModelSignal]:
    return (left, right) if left.activation >= right.activation else (right, left)


def _undirected_orient(
    left: ModelSignal, right: ModelSignal
) -> tuple[ModelSignal, ModelSignal]:
    return (left, right) if str(left.id) < str(right.id) else (right, left)


def _avg_scores(
    left: ModelSignal,
    right: ModelSignal,
    *,
    impact_boost: float = 0.0,
    actionability: float = 0.40,
    uncertainty: float = 0.45,
    novelty: float = 0.50,
) -> JudgmentScores:
    return JudgmentScores(
        impact=clamp_score(max(left.activation, right.activation) + impact_boost),
        urgency=clamp_score(max(left.activation, right.activation) * 0.6),
        uncertainty=uncertainty,
        reversibility=0.45,
        authority_required=0.35,
        actionability=actionability,
        novelty=novelty,
        confidence=(float(left.confidence) + float(right.confidence)) / 2.0,
    )


def _explanation(edge_kind: str, reason: str, scope_meta: dict[str, Any]) -> str:
    scope_hint = ""
    if "scope_type" in scope_meta and "scope_id" in scope_meta:
        scope_hint = f" (scope {scope_meta['scope_type']}:{scope_meta['scope_id']})"
    return f"`{edge_kind}` candidate{scope_hint}: {reason}"


def _rule_same_issue_as(
    tenant_id: UUID,
    scope_meta: dict[str, Any],
    left: ModelSignal,
    right: ModelSignal,
) -> RelationshipCandidate | None:
    # Trigger 1: very-high cosine + shared entity + same workstream.
    cos = _cosine(left.embedding, right.embedding)
    entities_shared = bool(_shared_concrete_entities(left, right))
    same_ws = _same_workstream(left, right)
    cosine_hit = cos is not None and cos >= 0.85 and entities_shared and same_ws
    # Trigger 2: identical normalized natural text (independent of scope).
    text_hit = _normalize_text(left.natural) == _normalize_text(right.natural) and bool(
        left.natural.strip()
    )
    if not (cosine_hit or text_hit):
        return None
    source, target = _undirected_orient(left, right)
    reason = (
        "identical normalized text"
        if text_hit
        else f"cosine {cos:.2f} + same entity + same workstream"
    )
    metadata = {
        **scope_meta,
        "rule": {
            "edge_kind": "same_issue_as",
            "cosine": cos,
            "shared_entities": [
                {"type": t, "id": str(eid)}
                for (t, eid) in _shared_entities(left, right)
            ],
            "identical_text": text_hit,
        },
    }
    return make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=source.id,
        target_model_id=target.id,
        edge_kind="same_issue_as",
        basis="inferred",
        explanation=_explanation("same_issue_as", reason, scope_meta),
        scores=_avg_scores(left, right, uncertainty=0.30),
        metadata=metadata,
    )


def _rule_supports(
    tenant_id: UUID,
    scope_meta: dict[str, Any],
    left: ModelSignal,
    right: ModelSignal,
) -> RelationshipCandidate | None:
    shared_events = set(left.evidence_event_ids) & set(right.evidence_event_ids)
    if not shared_events and not _precise_edge_scope_compatible(left, right):
        return None
    # source provides high-confidence evidence; target activation rising.
    source: ModelSignal | None = None
    target: ModelSignal | None = None
    if shared_events:
        source, target = _orient_by_activation(left, right)
    else:
        # high-confidence source + activating target (proxy for "increasing")
        for cand_source, cand_target in ((left, right), (right, left)):
            if cand_source.confidence >= 0.75 and cand_target.activation >= 0.55:
                source, target = cand_source, cand_target
                break
    if source is None or target is None:
        return None
    metadata = {
        **scope_meta,
        "rule": {
            "edge_kind": "supports",
            "shared_evidence_event_ids": [
                str(e) for e in sorted(shared_events, key=str)
            ],
            "source_confidence": source.confidence,
            "target_activation": target.activation,
        },
    }
    reason = (
        f"shared evidence events ({len(shared_events)})"
        if shared_events
        else "high source confidence + target activation increasing"
    )
    return make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=source.id,
        target_model_id=target.id,
        edge_kind="supports",
        basis="observed" if shared_events else "inferred",
        explanation=_explanation("supports", reason, scope_meta),
        scores=_avg_scores(left, right, uncertainty=0.35, actionability=0.45),
        metadata=metadata,
        evidence_event_ids=tuple(sorted(shared_events, key=str)),
    )


def _rule_analogous_to(
    tenant_id: UUID,
    scope_meta: dict[str, Any],
    left: ModelSignal,
    right: ModelSignal,
) -> RelationshipCandidate | None:
    cos = _cosine(left.embedding, right.embedding)
    if cos is None or cos < 0.75:
        return None
    if _shared_entities(left, right):
        return None
    if _same_workstream(left, right):
        return None
    source, target = _undirected_orient(left, right)
    metadata = {
        **scope_meta,
        "rule": {
            "edge_kind": "analogous_to",
            "cosine": cos,
            "different_scope": True,
        },
    }
    return make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=source.id,
        target_model_id=target.id,
        edge_kind="analogous_to",
        basis="inferred",
        explanation=_explanation(
            "analogous_to",
            f"cosine {cos:.2f} across different entities/workstreams",
            scope_meta,
        ),
        scores=_avg_scores(left, right, uncertainty=0.55, novelty=0.65),
        metadata=metadata,
    )


_BLOCKER_PHRASES = (
    "blocked by",
    "blocked on",
    "blocking",
    "waiting on",
    "prerequisite",
    "depends on",
    "dependency on",
    "requires",
)


_BLOCKER_REF_STOPWORDS = {
    "approval",
    "blocked",
    "blocking",
    "cannot",
    "decision",
    "dependency",
    "depends",
    "evidence",
    "missing",
    "prerequisite",
    "requires",
    "review",
    "waiting",
}


def _blocker_ref_tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9][a-z0-9_-]{2,}", text.lower())
        if token not in _BLOCKER_REF_STOPWORDS
    }


def _detect_dependency_basis(source: ModelSignal, target: ModelSignal) -> str | None:
    if target.id in source.blocker_targets:
        return "explicit_blocker_target_reference"
    text = source.natural.lower()
    target_text = target.natural.lower().strip()
    if any(p in text for p in _BLOCKER_PHRASES):
        if target_text and target_text[:60] in text:
            return "phrase_and_text_overlap"
        for ref in source.blocker_text_refs:
            if ref.lower() in target_text or target_text[:40] in ref.lower():
                return "phrase_and_named_resource"
            if len(_blocker_ref_tokens(ref) & _blocker_ref_tokens(target_text)) >= 1:
                return "phrase_and_named_resource"
    return None


def _rule_blocks(
    tenant_id: UUID,
    scope_meta: dict[str, Any],
    left: ModelSignal,
    right: ModelSignal,
) -> RelationshipCandidate | None:
    if not _eligible_precise_edge_endpoints(left, right):
        return None
    if not _precise_edge_scope_compatible(left, right):
        return None
    # REJECT pure pressure overlap. Require a dependency surface: one
    # model concretely names the other as a blocker / prerequisite.
    dep_lr = _detect_dependency_basis(left, right)
    dep_rl = _detect_dependency_basis(right, left)
    if dep_lr is None and dep_rl is None:
        return None
    if dep_lr is not None and dep_rl is None:
        # left says it is blocked by / waiting on right; the blocker is the
        # edge source and the blocked model is the target.
        source, target, basis_reason = right, left, dep_lr
    elif dep_rl is not None and dep_lr is None:
        source, target, basis_reason = left, right, dep_rl
    else:
        # Mutual phrasing: prefer the higher-activation as source.
        source, target = _orient_by_activation(left, right)
        basis_reason = dep_lr or dep_rl or "mutual_dependency_phrasing"
    mechanism = (
        f"Source Model concretely names the target as a blocker / prerequisite "
        f"({basis_reason})."
    )
    metadata = {
        **scope_meta,
        "rule": {
            "edge_kind": "blocks",
            "dependency_basis": basis_reason,
        },
        "mechanism": mechanism,
        "dependency_basis": basis_reason,
    }
    return make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=source.id,
        target_model_id=target.id,
        edge_kind="blocks",
        basis="causal_hypothesis",
        explanation=_explanation("blocks", basis_reason, scope_meta),
        scores=_avg_scores(left, right, actionability=0.65, uncertainty=0.45),
        metadata=metadata,
        mechanism_summary=mechanism,
        intervention_surface="remove blocker, clarify owner, or unblock dependency",
        expected_delay="unknown",
    )


def _rule_early_warning_for(
    tenant_id: UUID,
    scope_meta: dict[str, Any],
    left: ModelSignal,
    right: ModelSignal,
) -> RelationshipCandidate | None:
    if not _eligible_precise_edge_endpoints(left, right):
        return None
    if not _precise_edge_scope_compatible(left, right):
        return None
    candidates: list[tuple[ModelSignal, ModelSignal, str]] = []
    for source, target in ((left, right), (right, left)):
        leading = source.time_shape == "leading" or source.is_leading_indicator
        if not leading:
            continue
        historical = (
            target.id in source.historical_cooccurrence_with
            or source.id in target.historical_cooccurrence_with
            or source.is_leading_indicator
        )
        if not historical:
            continue
        evidence_kind = (
            "historical_cooccurrence"
            if (
                target.id in source.historical_cooccurrence_with
                or source.id in target.historical_cooccurrence_with
            )
            else "known_leading_indicator_pattern"
        )
        candidates.append((source, target, evidence_kind))
    if not candidates:
        return None
    source, target, evidence_kind = candidates[0]
    lead_time_evidence = {
        "kind": evidence_kind,
        "source_time_shape": source.time_shape,
    }
    metadata = {
        **scope_meta,
        "rule": {
            "edge_kind": "early_warning_for",
            "lead_time_evidence": lead_time_evidence,
        },
        "lead_time_evidence": lead_time_evidence,
        "historical_basis": evidence_kind,
    }
    return make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=source.id,
        target_model_id=target.id,
        edge_kind="early_warning_for",
        basis="inferred",
        explanation=_explanation(
            "early_warning_for",
            f"source is leading + {evidence_kind}",
            scope_meta,
        ),
        scores=_avg_scores(left, right, actionability=0.60, uncertainty=0.55),
        metadata=metadata,
    )


def _rule_contradicts(
    tenant_id: UUID,
    scope_meta: dict[str, Any],
    left: ModelSignal,
    right: ModelSignal,
) -> RelationshipCandidate | None:
    if left.proposition_kind != "state" or right.proposition_kind != "state":
        if left.proposition_kind != "belief" or right.proposition_kind != "belief":
            return None
    if not left.polarity or not right.polarity:
        return None
    if left.polarity == right.polarity:
        return None
    # Must share scope to be claims over the same subject.
    if not _precise_edge_scope_compatible(left, right):
        return None
    source, target = _undirected_orient(left, right)
    metadata = {
        **scope_meta,
        "rule": {
            "edge_kind": "contradicts",
            "left_polarity": left.polarity,
            "right_polarity": right.polarity,
        },
    }
    return make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=source.id,
        target_model_id=target.id,
        edge_kind="contradicts",
        basis="inferred",
        explanation=_explanation(
            "contradicts",
            f"opposing polarities ({left.polarity} vs {right.polarity}) over shared scope",
            scope_meta,
        ),
        scores=_avg_scores(left, right, uncertainty=0.40, actionability=0.50),
        metadata=metadata,
    )


_WEAKENING_PHRASES = (
    "weakens",
    "undermines",
    "casts doubt",
    "less likely",
    "no longer supports",
    "evidence against",
    "conflicts with",
    "stale",
)


def _rule_weakens(
    tenant_id: UUID,
    scope_meta: dict[str, Any],
    left: ModelSignal,
    right: ModelSignal,
) -> RelationshipCandidate | None:
    if not _eligible_precise_edge_endpoints(left, right):
        return None
    if not _precise_edge_scope_compatible(left, right):
        return None
    source: ModelSignal | None = None
    target: ModelSignal | None = None
    for cand_source, cand_target in ((left, right), (right, left)):
        phrase_hit = _contains_any(cand_source.natural, _WEAKENING_PHRASES)
        polarity_hit = (
            cand_source.polarity == "negative"
            and cand_target.polarity == "positive"
            and cand_source.confidence >= cand_target.confidence + 0.10
        )
        if (phrase_hit and cand_target.polarity == "positive") or polarity_hit:
            source, target = cand_source, cand_target
            break
    if source is None or target is None:
        return None
    metadata = {
        **scope_meta,
        "rule": {
            "edge_kind": "weakens",
            "source_polarity": source.polarity,
            "target_polarity": target.polarity,
            "source_confidence": source.confidence,
            "target_confidence": target.confidence,
        },
    }
    return make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=source.id,
        target_model_id=target.id,
        edge_kind="weakens",
        basis="inferred",
        explanation=_explanation(
            "weakens",
            "source supplies partial counterevidence over shared scope",
            scope_meta,
        ),
        scores=_avg_scores(left, right, uncertainty=0.50, actionability=0.45),
        metadata=metadata,
    )


_EXPLANATION_PHRASES = (
    "because",
    "due to",
    "root cause",
    "explains",
    "driven by",
    "caused by",
)


def _rule_explains(
    tenant_id: UUID,
    scope_meta: dict[str, Any],
    left: ModelSignal,
    right: ModelSignal,
) -> RelationshipCandidate | None:
    if not _eligible_precise_edge_endpoints(left, right):
        return None
    if not _precise_edge_scope_compatible(left, right):
        return None
    source: ModelSignal | None = None
    target: ModelSignal | None = None
    for cand_source, cand_target in ((left, right), (right, left)):
        if _contains_any(cand_source.natural, _EXPLANATION_PHRASES):
            source, target = cand_source, cand_target
            break
    if source is None or target is None:
        return None
    mechanism = (
        "Source Model states a reason, driver, or root cause for the target context."
    )
    metadata = {
        **scope_meta,
        "rule": {"edge_kind": "explains"},
        "mechanism": mechanism,
    }
    return make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=source.id,
        target_model_id=target.id,
        edge_kind="explains",
        basis="causal_hypothesis",
        explanation=_explanation(
            "explains",
            "explicit explanatory phrasing",
            scope_meta,
        ),
        scores=_avg_scores(left, right, actionability=0.50, uncertainty=0.55),
        metadata=metadata,
        mechanism_summary=mechanism,
        intervention_surface="verify the stated mechanism or remove the driver",
        expected_delay="unknown",
    )


_CAUSE_PHRASES = (
    "causes",
    "caused",
    "leads to",
    "resulting in",
    "results in",
    "drives",
    "triggered",
)


def _rule_causes(
    tenant_id: UUID,
    scope_meta: dict[str, Any],
    left: ModelSignal,
    right: ModelSignal,
) -> RelationshipCandidate | None:
    if not _precise_edge_scope_compatible(left, right):
        return None
    source: ModelSignal | None = None
    target: ModelSignal | None = None
    for cand_source, cand_target in ((left, right), (right, left)):
        if _contains_any(cand_source.natural, _CAUSE_PHRASES):
            source, target = cand_source, cand_target
            break
    if source is None or target is None:
        return None
    mechanism = "Source Model names a causal driver that can produce the target state."
    metadata = {
        **scope_meta,
        "rule": {"edge_kind": "causes"},
        "mechanism": mechanism,
    }
    return make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=source.id,
        target_model_id=target.id,
        edge_kind="causes",
        basis="causal_hypothesis",
        explanation=_explanation("causes", "explicit causal phrasing", scope_meta),
        scores=_avg_scores(left, right, actionability=0.55, uncertainty=0.55),
        metadata=metadata,
        mechanism_summary=mechanism,
        intervention_surface="change or remove the causal driver",
        expected_delay="unknown",
    )


_RESOLUTION_PHRASES = (
    "resolved",
    "unblocked",
    "mitigated",
    "remediated",
    "fixed",
    "closed",
    "cleared",
    "now available",
    "approved",
)
_STRONG_POSITIVE_RESOLUTION_PHRASES = (
    "unblocked",
    "mitigated",
    "remediated",
    "fixed",
    "closed",
    "cleared",
    "now available",
    "approved",
)
_RESOLUTION_NEGATION_PHRASES = (
    "unresolved",
    "mostly resolved",
    "treated as mostly resolved",
)
_PRESSURE_PHRASES = (
    "blocked",
    "risk",
    "concern",
    "waiting on",
    "missing",
    "gap",
    "issue",
    "exception",
)


def _contains_resolution_signal(text: str) -> bool:
    normalized = _normalize_text(text)
    if any(phrase in normalized for phrase in _RESOLUTION_NEGATION_PHRASES):
        return False
    return any(phrase in normalized for phrase in _RESOLUTION_PHRASES)


def _contains_strong_positive_resolution_signal(text: str) -> bool:
    normalized = _normalize_text(text)
    if any(phrase in normalized for phrase in _RESOLUTION_NEGATION_PHRASES):
        return False
    return any(phrase in normalized for phrase in _STRONG_POSITIVE_RESOLUTION_PHRASES)


def _rule_contributes_to_resolution(
    tenant_id: UUID,
    scope_meta: dict[str, Any],
    left: ModelSignal,
    right: ModelSignal,
) -> RelationshipCandidate | None:
    if not _eligible_precise_edge_endpoints(left, right):
        return None
    if not _precise_edge_scope_compatible(left, right):
        return None
    source: ModelSignal | None = None
    target: ModelSignal | None = None
    for cand_source, cand_target in ((left, right), (right, left)):
        source_resolves = _contains_resolution_signal(cand_source.natural)
        target_has_pressure = _contains_any(cand_target.natural, _PRESSURE_PHRASES)
        source_not_negative = (
            cand_source.polarity != "negative"
            or _contains_strong_positive_resolution_signal(cand_source.natural)
        )
        target_is_pressure = (
            cand_target.polarity == "negative" or _claim_role(cand_target) == "concern"
        )
        if (
            source_resolves
            and target_has_pressure
            and source_not_negative
            and target_is_pressure
        ):
            source, target = cand_source, cand_target
            break
    if source is None or target is None:
        return None
    metadata = {
        **scope_meta,
        "rule": {"edge_kind": "contributes_to_resolution"},
    }
    return make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=source.id,
        target_model_id=target.id,
        edge_kind="contributes_to_resolution",
        basis="inferred",
        explanation=_explanation(
            "contributes_to_resolution",
            "resolution evidence addresses an active pressure model",
            scope_meta,
        ),
        scores=_avg_scores(left, right, actionability=0.70, uncertainty=0.40),
        metadata=metadata,
    )


def _is_prediction_signal(signal: ModelSignal) -> bool:
    proposition = signal.proposition or {}
    return (
        signal.proposition_kind == "prediction"
        or proposition.get("kind") == "prediction"
        or proposition.get("claim_role") == "prediction"
    )


def _rule_predicts(
    tenant_id: UUID,
    scope_meta: dict[str, Any],
    left: ModelSignal,
    right: ModelSignal,
) -> RelationshipCandidate | None:
    if not _precise_edge_scope_compatible(left, right):
        return None
    source: ModelSignal | None = None
    target: ModelSignal | None = None
    for cand_source, cand_target in ((left, right), (right, left)):
        if _is_prediction_signal(cand_source) and not _is_prediction_signal(
            cand_target
        ):
            source, target = cand_source, cand_target
            break
    if source is None or target is None:
        return None
    metadata = {
        **scope_meta,
        "rule": {
            "edge_kind": "predicts",
            "source_time_shape": source.time_shape,
        },
    }
    return make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=source.id,
        target_model_id=target.id,
        edge_kind="predicts",
        basis="inferred",
        explanation=_explanation(
            "predicts",
            "prediction claim points to a future/observable target state",
            scope_meta,
        ),
        scores=_avg_scores(left, right, actionability=0.55, uncertainty=0.65),
        metadata=metadata,
    )


def _rule_enables(
    tenant_id: UUID,
    scope_meta: dict[str, Any],
    left: ModelSignal,
    right: ModelSignal,
) -> RelationshipCandidate | None:
    if not _precise_edge_scope_compatible(left, right):
        return None
    for source, target in ((left, right), (right, left)):
        if not source.capability_surface:
            continue
        if _claim_role(target) != "capability":
            continue
        metadata = {
            **scope_meta,
            "rule": {
                "edge_kind": "enables",
                "capability_surface": source.capability_surface,
            },
            "mechanism": (
                f"Source provides capability `{source.capability_surface}` "
                "that target capability_assessment evaluates."
            ),
        }
        return make_edge_candidate(
            tenant_id=tenant_id,
            source_model_id=source.id,
            target_model_id=target.id,
            edge_kind="enables",
            basis="causal_hypothesis",
            explanation=_explanation(
                "enables",
                f"capability `{source.capability_surface}` enables capability_assessment",
                scope_meta,
            ),
            scores=_avg_scores(left, right, actionability=0.55, uncertainty=0.45),
            metadata=metadata,
            mechanism_summary=metadata["mechanism"],
            intervention_surface="preserve prerequisite or reinforce capability",
            expected_delay="unknown",
        )
    return None


def _claim_role(model: ModelSignal) -> str:
    if model.proposition_kind == "capability_assessment":
        return "capability"
    if model.proposition_kind == "situation":
        return "situation"
    if model.proposition_kind == "concern":
        return "concern"
    grammar = derive_memory_grammar(model.proposition)
    return grammar.claim_role


_CANDIDATE_RULES: dict[str, CandidateRule] = {
    "same_issue_as": _rule_same_issue_as,
    "supports": _rule_supports,
    "analogous_to": _rule_analogous_to,
    "blocks": _rule_blocks,
    "early_warning_for": _rule_early_warning_for,
    "contradicts": _rule_contradicts,
    "weakens": _rule_weakens,
    "explains": _rule_explains,
    "causes": _rule_causes,
    "contributes_to_resolution": _rule_contributes_to_resolution,
    "predicts": _rule_predicts,
    "enables": _rule_enables,
}


def candidate_rules() -> dict[str, CandidateRule]:
    """Return a copy of the per-edge-kind rule registry."""
    return dict(_CANDIDATE_RULES)


def generate_scope_overlap_candidates(
    *,
    tenant_id: UUID,
    models: list[ModelSignal],
    max_candidates: int = 20,
) -> list[RelationshipCandidate]:
    """Generate candidates per edge-kind rule across pairs sharing a scope.

    Each rule decides for itself whether the pair satisfies its
    preconditions. A pair can yield zero, one, or several different-kind
    candidates. The legacy "pick one heuristic kind per pair" behaviour
    is intentionally gone.
    """
    by_scope: dict[tuple[str, UUID], list[ModelSignal]] = {}
    for m in models:
        for scope in m.scope_entities:
            by_scope.setdefault(scope, []).append(m)

    out: list[RelationshipCandidate] = []
    seen_pairs: set[tuple[UUID, UUID, str]] = set()
    rules = _CANDIDATE_RULES
    for scope, group in by_scope.items():
        if len(group) < 2:
            continue
        group_sorted = sorted(
            group,
            key=lambda m: (-float(m.activation), -float(m.confidence), str(m.id)),
        )[:8]
        scope_meta = {
            "scope": {"type": scope[0], "id": str(scope[1])},
            "scope_type": scope[0],
            "scope_id": str(scope[1]),
        }
        for i, left in enumerate(group_sorted):
            for right in group_sorted[i + 1 :]:
                for edge_kind, rule in rules.items():
                    candidate = rule(tenant_id, scope_meta, left, right)
                    if candidate is None:
                        continue
                    pair_key = (
                        candidate.source_model_id or left.id,
                        candidate.target_model_id or right.id,
                        edge_kind,
                    )
                    if pair_key in seen_pairs:
                        continue
                    seen_pairs.add(pair_key)
                    out.append(candidate)
    return rank_candidates(out, limit=max_candidates)


__all__ = [
    "CandidateBasis",
    "CandidateKind",
    "CandidateRule",
    "JudgmentScores",
    "ModelSignal",
    "RelationshipCandidate",
    "TOPOLOGY_EMITTABLE_EDGE_KINDS",
    "candidate_rules",
    "generate_scope_overlap_candidates",
    "make_edge_candidate",
    "make_edge_type_candidate",
    "make_situation_candidate",
    "rank_candidates",
]
