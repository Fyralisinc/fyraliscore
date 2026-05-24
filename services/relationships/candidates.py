"""Candidate generation and scoring for relationship intelligence.

The important architectural boundary: candidates are not accepted truth.
They are inspectable hypotheses that may later become `model_edges` or
composite `situation` Models after LLM/human adjudication.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from lib.shared.ids import uuid7
from services.judgment.scoring import JudgmentScores, clamp_score


CandidateKind = Literal["edge", "situation"]
CandidateBasis = Literal[
    "observed",
    "inferred",
    "correlated",
    "topology_suggested",
    "causal_hypothesis",
    "causal_confirmed",
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
            "metadata": self.metadata,
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
    proposition = {
        "kind": "situation",
        "situation": situation,
        "summary": summary,
        "member_model_ids": [str(m) for m in members],
        "relationship_summary": relationship_summary,
        "status": "forming",
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
        metadata=metadata or {},
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


def generate_scope_overlap_candidates(
    *,
    tenant_id: UUID,
    models: list[ModelSignal],
    max_candidates: int = 20,
) -> list[RelationshipCandidate]:
    """Generate cheap candidates from shared organizational scope.

    This is intentionally conservative. It produces only inspectable
    candidates; accepted truth still requires later validation.
    """
    by_scope: dict[tuple[str, UUID], list[ModelSignal]] = {}
    for m in models:
        for scope in m.scope_entities:
            by_scope.setdefault(scope, []).append(m)

    out: list[RelationshipCandidate] = []
    seen_pairs: set[tuple[UUID, UUID, str]] = set()
    for scope, group in by_scope.items():
        if len(group) < 2:
            continue
        group_sorted = sorted(
            group,
            key=lambda m: (-float(m.activation), -float(m.confidence), str(m.id)),
        )[:8]
        for i, left in enumerate(group_sorted):
            for right in group_sorted[i + 1:]:
                kind = _heuristic_edge_kind(left, right)
                source, target = _orient_pair(left, right, kind)
                key = (source.id, target.id, kind)
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                explanation = (
                    f"Both Models touch {scope[0]}:{scope[1]} and their "
                    f"proposition kinds ({source.proposition_kind}, "
                    f"{target.proposition_kind}) suggest {kind} should be reviewed."
                )
                basis: CandidateBasis = "inferred"
                causal_kwargs: dict[str, Any] = {}
                if kind in _CAUSAL_EDGE_KINDS:
                    basis = "causal_hypothesis"
                    causal_kwargs = {
                        "mechanism_summary": _heuristic_mechanism_summary(
                            source,
                            target,
                            kind,
                        ),
                        "intervention_surface": _heuristic_intervention_surface(
                            kind
                        ),
                        "expected_delay": "unknown",
                    }
                scores = JudgmentScores(
                    impact=max(left.activation, right.activation),
                    uncertainty=0.55 if kind in {"causes", "early_warning_for"} else 0.35,
                    urgency=max(left.activation, right.activation) * 0.6,
                    reversibility=0.45,
                    authority_required=0.35,
                    actionability=0.60 if kind in {"blocks", "early_warning_for"} else 0.40,
                    novelty=0.50,
                    confidence=(float(left.confidence) + float(right.confidence)) / 2,
                )
                out.append(
                    make_edge_candidate(
                        tenant_id=tenant_id,
                        source_model_id=source.id,
                        target_model_id=target.id,
                        edge_kind=kind,
                        basis=basis,
                        explanation=explanation,
                        scores=scores,
                        metadata={"scope": {"type": scope[0], "id": str(scope[1])}},
                        **causal_kwargs,
                    )
                )
    return rank_candidates(out, limit=max_candidates)


def _heuristic_edge_kind(left: ModelSignal, right: ModelSignal) -> str:
    kinds = {left.proposition_kind, right.proposition_kind}
    text = f"{left.natural} {right.natural}".lower()
    if "contradict" in text or "cannot both" in text:
        return "contradicts"
    if "block" in text or "blocked" in text or "waiting on" in text:
        return "blocks"
    if "risk" in text or "churn" in text or "renewal" in text:
        if "prediction" in kinds or "concern" in kinds:
            return "early_warning_for"
    if "concern" in kinds and "prediction" in kinds:
        return "early_warning_for"
    if "hypothesis" in kinds or "relation" in kinds:
        return "explains"
    return "co_occurs_with"


def _heuristic_mechanism_summary(
    source: ModelSignal,
    target: ModelSignal,
    edge_kind: str,
) -> str:
    if edge_kind == "blocks":
        return (
            "The source Model describes a constraint or blocker that may "
            "delay, prevent, or degrade the target Model's outcome."
        )
    if edge_kind == "enables":
        return (
            "The source Model describes a capability or prerequisite that may "
            "make the target Model's outcome more likely."
        )
    if edge_kind == "explains":
        return (
            "The source Model offers a plausible mechanism for why the target "
            "Model is occurring."
        )
    return (
        "The source Model may causally influence the target Model; the "
        "mechanism should be reviewed before acceptance."
    )


def _heuristic_intervention_surface(edge_kind: str) -> str | None:
    if edge_kind == "blocks":
        return "remove blocker, clarify owner, change priority, or unblock dependency"
    if edge_kind == "enables":
        return "preserve prerequisite, allocate support, or reinforce capability"
    if edge_kind == "explains":
        return "test mechanism and inspect downstream leverage point"
    return None


def _orient_pair(
    left: ModelSignal,
    right: ModelSignal,
    edge_kind: str,
) -> tuple[ModelSignal, ModelSignal]:
    if edge_kind in {"contradicts", "co_occurs_with"}:
        return (left, right) if str(left.id) < str(right.id) else (right, left)
    if left.proposition_kind == "concern" and right.proposition_kind != "concern":
        return left, right
    if right.proposition_kind == "concern" and left.proposition_kind != "concern":
        return right, left
    if left.proposition_kind == "hypothesis" and right.proposition_kind != "hypothesis":
        return left, right
    if right.proposition_kind == "hypothesis" and left.proposition_kind != "hypothesis":
        return right, left
    return (left, right) if left.activation >= right.activation else (right, left)


__all__ = [
    "CandidateBasis",
    "CandidateKind",
    "JudgmentScores",
    "ModelSignal",
    "RelationshipCandidate",
    "generate_scope_overlap_candidates",
    "make_edge_candidate",
    "make_situation_candidate",
    "rank_candidates",
]
