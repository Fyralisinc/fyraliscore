"""Conservative compiler from pair evidence to relationship candidates."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from lib.shared.edge_registry import EDGE_REGISTRY
from services.reasoning.judgment.scoring import JudgmentScores, clamp_score
from services.reasoning.relationships.candidates import (
    RelationshipCandidate,
    make_edge_candidate,
)

from .types import ModelPairEvidence


_PRIMITIVE_EDGE_KIND_DEFAULTS: dict[str, str] = {
    "DEPENDENCY": "blocks",
    "BLOCKER": "blocks",
    "ENABLEMENT": "enables",
    "COUNTEREVIDENCE": "weakens",
    "RECURRENCE": "same_issue_as",
    "GOAL_IMPACT": "supports",
    "PREDICTION": "early_warning_for",
    "TEMPORAL": "early_warning_for",
    "CAUSAL": "causes",
    "EXPLANATION": "explains",
    "RESOLUTION": "contributes_to_resolution",
}


@dataclass(frozen=True)
class EdgeCompilerConfig:
    """Promotion gates for pair-evidence aggregates.

    The defaults intentionally reject raw co-retrieval. A pair needs direct
    relation proof, valid co-use, T4 acceptance, or a Think edge op before it
    can enter the relationship candidate lifecycle.
    """

    min_confidence: float = 0.60
    min_positive_signals: int = 2
    min_direction_votes_for_directed: int = 1
    max_rejects_without_accept: int = 0
    retrieval_only_allowed: bool = False


def compile_pair_evidence_candidate(
    evidence: ModelPairEvidence,
    *,
    config: EdgeCompilerConfig | None = None,
) -> RelationshipCandidate | None:
    """Return a relationship candidate when pair evidence is promotable."""
    config = config or EdgeCompilerConfig()
    confidence = confidence_from_pair_evidence(evidence)
    if confidence < config.min_confidence:
        return None
    if _is_negative_memory_dominant(evidence, config):
        return None
    if not config.retrieval_only_allowed and _is_retrieval_only(evidence):
        return None
    if _positive_signal_count(evidence) < config.min_positive_signals:
        return None

    edge_kind = _choose_edge_kind(evidence)
    if edge_kind is None:
        return None
    spec = EDGE_REGISTRY.get(edge_kind)
    if spec is None or not spec.enabled_for_writes:
        return None

    source_model_id, target_model_id = _oriented_pair(
        evidence,
        directed=spec.is_directed,
        min_votes=config.min_direction_votes_for_directed,
    )
    if source_model_id is None or target_model_id is None:
        return None

    basis = "observed" if evidence.explicit_relation_count else "inferred"
    if evidence.t4_accept_count > 0 or evidence.think_edge_op_count > 0:
        basis = "causal_confirmed" if edge_kind in _CAUSALISH_KINDS else "observed"

    scores = _scores(evidence, confidence)
    return make_edge_candidate(
        tenant_id=evidence.tenant_id,
        source_model_id=source_model_id,
        target_model_id=target_model_id,
        edge_kind=edge_kind,
        basis=basis,
        explanation=_explanation(evidence, edge_kind, confidence),
        scores=scores,
        evidence_model_ids=(evidence.model_a_id, evidence.model_b_id),
        mechanism_summary=_mechanism_summary(evidence, edge_kind)
        if basis in {"causal_hypothesis", "causal_confirmed"}
        else None,
        metadata={
            "edge_intelligence": {
                "source": "model_pair_evidence",
                "model_pair_evidence_id": str(evidence.id),
                "primitive": evidence.primitive,
                "counts": _counts(evidence),
                "direction_votes": dict(evidence.direction_votes),
                "edge_kind_votes": dict(evidence.edge_kind_votes),
                "confidence_score": confidence,
            }
        },
        source="edge_intelligence_kernel",
    )


def confidence_from_pair_evidence(evidence: ModelPairEvidence) -> float:
    """Score aggregate pair evidence without treating co-retrieval as truth."""
    positive = (
        2.4 * evidence.explicit_relation_count
        + 1.7 * evidence.co_used_valid_diff_count
        + 1.5 * evidence.think_edge_op_count
        + 2.2 * evidence.t4_accept_count
        + 0.35 * evidence.co_retrieved_count
        + 1.0 * evidence.positive_outcome_count
    )
    negative = (
        2.2 * evidence.t4_reject_count
        + 0.9 * evidence.no_edge_count
        + 1.2 * evidence.negative_outcome_count
    )
    if positive <= 0.0:
        return 0.0
    # Smoothing means one weak co-retrieval cannot cross promotion gates.
    return clamp_score(positive / (positive + negative + 3.0))


_CAUSALISH_KINDS = {
    "blocks",
    "causes",
    "explains",
    "enables",
    "contributes_to_resolution",
}


def _choose_edge_kind(evidence: ModelPairEvidence) -> str | None:
    voted = evidence.strongest_edge_kind
    if voted:
        return voted
    return _PRIMITIVE_EDGE_KIND_DEFAULTS.get(evidence.primitive)


def _oriented_pair(
    evidence: ModelPairEvidence,
    *,
    directed: bool,
    min_votes: int,
) -> tuple[UUID | None, UUID | None]:
    if not directed:
        return evidence.model_a_id, evidence.model_b_id
    direction = evidence.strongest_direction
    votes = int(evidence.direction_votes.get(direction, 0))
    if votes < min_votes:
        return None, None
    if direction == "a_to_b":
        return evidence.model_a_id, evidence.model_b_id
    if direction == "b_to_a":
        return evidence.model_b_id, evidence.model_a_id
    return None, None


def _positive_signal_count(evidence: ModelPairEvidence) -> int:
    return (
        evidence.explicit_relation_count
        + evidence.co_used_valid_diff_count
        + evidence.think_edge_op_count
        + evidence.t4_accept_count
        + evidence.positive_outcome_count
    )


def _is_retrieval_only(evidence: ModelPairEvidence) -> bool:
    return _positive_signal_count(evidence) == 0 and evidence.co_retrieved_count > 0


def _is_negative_memory_dominant(
    evidence: ModelPairEvidence,
    config: EdgeCompilerConfig,
) -> bool:
    if evidence.t4_accept_count > 0:
        return False
    return evidence.t4_reject_count > config.max_rejects_without_accept


def _scores(evidence: ModelPairEvidence, confidence: float) -> JudgmentScores:
    positive = _positive_signal_count(evidence)
    return JudgmentScores(
        impact=clamp_score(0.42 + 0.08 * positive + 0.05 * evidence.t4_accept_count),
        uncertainty=clamp_score(
            0.45 - 0.08 * evidence.t4_accept_count
            + 0.12 * evidence.t4_reject_count
        ),
        urgency=0.5,
        actionability=clamp_score(0.45 + 0.07 * evidence.co_used_valid_diff_count),
        authority_required=0.45,
        novelty=0.35,
        confidence=confidence,
    )


def _explanation(
    evidence: ModelPairEvidence,
    edge_kind: str,
    confidence: float,
) -> str:
    return (
        f"Pair evidence under {evidence.primitive} supports `{edge_kind}` "
        f"with confidence {confidence:.2f}: "
        f"{_counts(evidence)}."
    )


def _mechanism_summary(evidence: ModelPairEvidence, edge_kind: str) -> str:
    return (
        f"Aggregated pair evidence indicates `{edge_kind}` under "
        f"{evidence.primitive}: {_counts(evidence)}."
    )


def _counts(evidence: ModelPairEvidence) -> dict[str, int]:
    return {
        "co_retrieved": evidence.co_retrieved_count,
        "co_used_valid_diff": evidence.co_used_valid_diff_count,
        "explicit_relation": evidence.explicit_relation_count,
        "think_edge_op": evidence.think_edge_op_count,
        "t4_accept": evidence.t4_accept_count,
        "t4_reject": evidence.t4_reject_count,
        "no_edge": evidence.no_edge_count,
        "positive_outcome": evidence.positive_outcome_count,
        "negative_outcome": evidence.negative_outcome_count,
    }
