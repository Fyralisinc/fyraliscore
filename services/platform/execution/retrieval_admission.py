"""Budget admission for staged inquiry retrieval actions.

This module is deliberately small: it does not retrieve data and it does not
know about prompts. It looks at the context already gathered in earlier stages
and decides whether a later, more expensive action is still worth running.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services.reasoning.retrieval.pathways import PathwayResult

from .types import RetrievalAction


@dataclass(frozen=True, slots=True)
class RetrievalCoverage:
    semantic_terms_models: int = 0
    cheap_context_models: int = 0
    temporal_records: int = 0

    def notes(self) -> dict[str, int]:
        return {
            "semantic_terms_models": self.semantic_terms_models,
            "cheap_context_models": self.cheap_context_models,
            "temporal_records": self.temporal_records,
        }


@dataclass(frozen=True, slots=True)
class RetrievalAdmissionDecision:
    admitted: bool
    reason: str = "admitted"
    coverage: RetrievalCoverage = RetrievalCoverage()


def decide_action_admission(
    action: RetrievalAction,
    prior_results: list[PathwayResult],
) -> RetrievalAdmissionDecision:
    """Return whether ``action`` should run after earlier-stage coverage."""

    coverage = summarize_retrieval_coverage(prior_results)
    if action.filters.get("_sage_route_utility_skip"):
        reason = str(
            action.filters.get("_sage_policy_reason")
            or "negative_sage_route_utility"
        )
        return RetrievalAdmissionDecision(
            admitted=False,
            reason=f"sage_route_utility_skip:{reason}",
            coverage=coverage,
        )
    if action.path == "semantic" and action.filters.get(
        "_semantic_fallback_after_terms"
    ):
        decision = _dense_semantic_fallback_decision(action, coverage)
        if decision is not None:
            return decision
    if action.path == "temporal" and action.filters.get(
        "_temporal_nearby_fallback_after_cheap_context"
    ):
        decision = _nearby_temporal_decision(action, coverage)
        if decision is not None:
            return decision
    if action.path == "temporal" and action.filters.get(
        "_temporal_broad_fallback_after_nearby"
    ):
        decision = _broad_temporal_decision(action, coverage)
        if decision is not None:
            return decision
    return RetrievalAdmissionDecision(admitted=True, coverage=coverage)


def summarize_retrieval_coverage(
    results: list[PathwayResult],
) -> RetrievalCoverage:
    semantic_terms_model_ids: set[Any] = set()
    cheap_model_ids: set[Any] = set()
    temporal_model_ids: set[Any] = set()
    temporal_observation_ids: set[Any] = set()
    for result in results:
        source = result.source_pathway
        for model in result.models:
            mid = getattr(model, "id", None)
            if mid is None:
                continue
            if source == "L":
                semantic_terms_model_ids.add(mid)
            if source != "B":
                cheap_model_ids.add(mid)
            if source == "C":
                temporal_model_ids.add(mid)
        if source == "C":
            for observation in result.observations:
                oid = getattr(observation, "id", None)
                if oid is not None:
                    temporal_observation_ids.add(oid)
    return RetrievalCoverage(
        semantic_terms_models=len(semantic_terms_model_ids),
        cheap_context_models=len(cheap_model_ids),
        temporal_records=len(temporal_model_ids) + len(temporal_observation_ids),
    )


def _positive_int_filter(
    action: RetrievalAction,
    key: str,
    default: int,
) -> int:
    try:
        return max(1, int(action.filters.get(key) or default))
    except (TypeError, ValueError):
        return max(1, int(default))


def _dense_semantic_fallback_decision(
    action: RetrievalAction,
    coverage: RetrievalCoverage,
) -> RetrievalAdmissionDecision | None:
    term_threshold = _positive_int_filter(
        action,
        "_fallback_min_semantic_terms_models",
        3,
    )
    if coverage.semantic_terms_models >= term_threshold:
        return RetrievalAdmissionDecision(
            admitted=False,
            reason=(
                "semantic_terms_sufficient:"
                f"{coverage.semantic_terms_models}>={term_threshold}"
            ),
            coverage=coverage,
        )
    cheap_threshold = _positive_int_filter(
        action,
        "_fallback_min_cheap_context_models",
        0,
    )
    if (
        cheap_threshold > 0
        and coverage.cheap_context_models >= cheap_threshold
    ):
        return RetrievalAdmissionDecision(
            admitted=False,
            reason=(
                "cheap_context_sufficient:"
                f"{coverage.cheap_context_models}>={cheap_threshold}"
            ),
            coverage=coverage,
        )
    return None


def _nearby_temporal_decision(
    action: RetrievalAction,
    coverage: RetrievalCoverage,
) -> RetrievalAdmissionDecision | None:
    cheap_threshold = _positive_int_filter(
        action,
        "_fallback_min_temporal_cheap_context_models",
        8,
    )
    term_threshold = _positive_int_filter(
        action,
        "_fallback_min_temporal_semantic_terms_models",
        3,
    )
    if (
        coverage.cheap_context_models >= cheap_threshold
        and coverage.semantic_terms_models >= term_threshold
    ):
        return RetrievalAdmissionDecision(
            admitted=False,
            reason=(
                "temporal_cheap_context_sufficient:"
                f"cheap={coverage.cheap_context_models}>={cheap_threshold},"
                f"semantic_terms={coverage.semantic_terms_models}>={term_threshold}"
            ),
            coverage=coverage,
        )
    return None


def _broad_temporal_decision(
    action: RetrievalAction,
    coverage: RetrievalCoverage,
) -> RetrievalAdmissionDecision | None:
    threshold = _positive_int_filter(
        action,
        "_fallback_min_temporal_records",
        2,
    )
    if coverage.temporal_records >= threshold:
        return RetrievalAdmissionDecision(
            admitted=False,
            reason=f"temporal_nearby_sufficient:{coverage.temporal_records}>={threshold}",
            coverage=coverage,
        )
    return None


__all__ = [
    "RetrievalAdmissionDecision",
    "RetrievalCoverage",
    "decide_action_admission",
    "summarize_retrieval_coverage",
]
