"""Marginal-utility gates for expensive execution work."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Literal


UtilityDecision = Literal["run", "suppress"]

_CRITICAL_PRIMITIVES = frozenset(
    {"COUNTEREVIDENCE", "DEPENDENCY", "COMMITMENT", "OWNERSHIP"}
)
_BUSINESS_CRITICAL_TERMS = frozenset(
    {
        "audit",
        "blocked",
        "blocker",
        "churn",
        "compliance",
        "customer",
        "deadline",
        "legal",
        "procurement",
        "renewal",
        "revenue",
        "risk",
        "security",
        "soc2",
    }
)
_HIGH_LEVERAGE_EDGE_KINDS = frozenset(
    {
        "blocks",
        "causes",
        "contradicts",
        "contributes_to_resolution",
        "early_warning_for",
        "enables",
        "explains",
        "predicts",
        "weakens",
    }
)
_GENERIC_EDGE_KINDS = frozenset({"analogous_to", "same_issue_as", "supports"})


@dataclass(frozen=True, slots=True)
class UtilityGovernorDecision:
    work_kind: str
    decision: UtilityDecision
    score: float
    reason: str
    features: dict[str, Any] = field(default_factory=dict)

    @property
    def should_run(self) -> bool:
        return self.decision == "run"

    def as_note(self) -> dict[str, Any]:
        return {
            "work_kind": self.work_kind,
            "decision": self.decision,
            "score": round(self.score, 4),
            "reason": self.reason,
            "features": self.features,
        }


def question_planning_utility(
    *,
    trigger_kind: str,
    trigger_text: str,
    deterministic_primitives: Iterable[str],
    deterministic_count: int,
    evidence_count: int,
    model_count: int,
    unknown_count: int,
    round_index: int,
    skip_threshold: float = 0.68,
) -> UtilityGovernorDecision:
    """Decide whether an LLM question-planning call has enough marginal value."""

    primitives = frozenset(str(value or "").upper() for value in deterministic_primitives)
    critical_coverage = len(primitives & _CRITICAL_PRIMITIVES)
    business_critical = _contains_any(trigger_text, _BUSINESS_CRITICAL_TERMS)
    features = {
        "trigger_kind": trigger_kind,
        "round": round_index,
        "deterministic_count": deterministic_count,
        "deterministic_primitives": sorted(primitives),
        "critical_primitive_coverage": critical_coverage,
        "evidence_count": evidence_count,
        "model_count": model_count,
        "unknown_count": unknown_count,
        "business_critical": business_critical,
        "skip_threshold": skip_threshold,
    }
    if trigger_kind != "T1":
        return UtilityGovernorDecision(
            work_kind="question_planning",
            decision="suppress",
            score=1.0,
            reason="non_t1_trigger_uses_seeded_retrieval",
            features=features,
        )
    if deterministic_count < 3 or critical_coverage < 2:
        return UtilityGovernorDecision(
            work_kind="question_planning",
            decision="run",
            score=0.0,
            reason="deterministic_plan_has_insufficient_question_coverage",
            features=features,
        )
    if evidence_count < 5:
        return UtilityGovernorDecision(
            work_kind="question_planning",
            decision="run",
            score=0.0,
            reason="primary_context_too_sparse_for_planner_skip",
            features=features,
        )

    coverage_score = min(1.0, deterministic_count / 5.0) * 0.30
    critical_score = min(1.0, critical_coverage / len(_CRITICAL_PRIMITIVES)) * 0.18
    evidence_score = min(1.0, evidence_count / 12.0) * 0.30
    model_score = min(1.0, model_count / 6.0) * 0.14
    round_score = 0.08 if round_index > 1 else 0.02
    uncertainty_penalty = min(0.22, unknown_count * 0.035)
    critical_penalty = 0.06 if business_critical else 0.0
    score = _clamp01(
        coverage_score
        + critical_score
        + evidence_score
        + model_score
        + round_score
        - uncertainty_penalty
        - critical_penalty
    )
    if score >= skip_threshold:
        return UtilityGovernorDecision(
            work_kind="question_planning",
            decision="suppress",
            score=score,
            reason="deterministic_questions_cover_high_value_uncertainty",
            features=features,
        )
    return UtilityGovernorDecision(
        work_kind="question_planning",
        decision="run",
        score=score,
        reason="planner_marginal_value_still_above_budget_gate",
        features=features,
    )


def downstream_trigger_utility(
    *,
    candidate_kind: str | None,
    edge_kind: str | None,
    basis: str | None,
    source: str | None,
    leverage_score: float,
    member_count: int,
    metadata: dict[str, Any] | None = None,
    run_threshold: float = 0.78,
) -> UtilityGovernorDecision:
    """Decide whether a relationship candidate should enqueue T4 Think."""

    candidate_kind = str(candidate_kind or "")
    edge_kind = str(edge_kind or "")
    basis = str(basis or "")
    source = str(source or "")
    score_components = _topology_score_components(metadata)
    precise_scope_rule = (
        candidate_kind == "edge"
        and source == "relationship_candidate_service"
        and edge_kind in _HIGH_LEVERAGE_EDGE_KINDS
    )
    high_leverage_edge = candidate_kind == "edge" and edge_kind in _HIGH_LEVERAGE_EDGE_KINDS
    generic_edge = candidate_kind == "edge" and edge_kind in _GENERIC_EDGE_KINDS
    actionability = _score_component(score_components, "actionability")
    business_leverage = _score_component(score_components, "business_leverage")
    structural_surprise = _score_component(score_components, "structural_surprise")
    novelty = _score_component(score_components, "novelty")
    evidence_quality = _score_component(score_components, "evidence_quality")
    is_latent_topology = source == "latent_topology"

    utility = _clamp01(float(leverage_score or 0.0))
    if high_leverage_edge:
        utility += 0.12
    if precise_scope_rule:
        utility += 0.06
    if basis in {"causal_hypothesis", "causal_confirmed", "ontology_gap"}:
        utility += 0.07
    if candidate_kind == "edge_type":
        utility += 0.04
    if candidate_kind == "situation" and member_count >= 4:
        utility += 0.04
    utility += 0.04 * max(actionability, business_leverage, structural_surprise)
    if is_latent_topology:
        utility += 0.04 * max(novelty, evidence_quality)
    if generic_edge:
        utility -= 0.18
    if is_latent_topology and not high_leverage_edge and candidate_kind != "situation":
        utility -= 0.08
    if (
        is_latent_topology
        and high_leverage_edge
        and max(actionability, business_leverage) < 0.62
    ):
        utility -= 0.12
    if candidate_kind == "situation" and max(actionability, business_leverage) < 0.55:
        utility -= 0.10
    if member_count <= 1 and candidate_kind != "edge":
        utility -= 0.08
    utility = _clamp01(utility)

    features = {
        "candidate_kind": candidate_kind,
        "edge_kind": edge_kind or None,
        "basis": basis or None,
        "source": source or None,
        "leverage_score": round(float(leverage_score or 0.0), 4),
        "member_count": member_count,
        "high_leverage_edge": high_leverage_edge,
        "generic_edge": generic_edge,
        "precise_scope_rule": precise_scope_rule,
        "actionability": actionability,
        "business_leverage": business_leverage,
        "structural_surprise": structural_surprise,
        "novelty": novelty,
        "evidence_quality": evidence_quality,
        "is_latent_topology": is_latent_topology,
        "run_threshold": run_threshold,
    }
    if utility >= run_threshold:
        return UtilityGovernorDecision(
            work_kind="downstream_t4",
            decision="run",
            score=utility,
            reason="candidate_has_high_marginal_adjudication_value",
            features=features,
        )
    return UtilityGovernorDecision(
        work_kind="downstream_t4",
        decision="suppress",
        score=utility,
        reason="candidate_is_low_specificity_or_redundant_for_followup_think",
        features=features,
    )


def _contains_any(text: str, terms: frozenset[str]) -> bool:
    lower = str(text or "").casefold()
    return any(term in lower for term in terms)


def _clamp01(value: float) -> float:
    return min(1.0, max(0.0, float(value)))


def _topology_score_components(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        return {}
    topology = metadata.get("topology")
    if not isinstance(topology, dict):
        return {}
    components = topology.get("score_components")
    return components if isinstance(components, dict) else {}


def _score_component(components: dict[str, Any], name: str) -> float:
    try:
        return _clamp01(float(components.get(name) or 0.0))
    except (TypeError, ValueError):
        return 0.0


__all__ = [
    "UtilityGovernorDecision",
    "downstream_trigger_utility",
    "question_planning_utility",
]
