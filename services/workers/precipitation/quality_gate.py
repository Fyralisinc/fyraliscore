"""Quality gate for precipitation enablement evidence.

Precipitation is a weak pattern-proposal source. This module turns shadow-run or
synthetic benchmark observations into a conservative verdict so broad enablement
requires evidence instead of an architectural assumption.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal, Sequence


PrecipitationQualityVerdict = Literal[
    "weak_evidence_only",
    "shadow_ready",
    "enablement_candidate",
]


@dataclass(frozen=True, slots=True)
class PrecipitationQualityObservation:
    """One labeled precipitation quality observation."""

    case_id: str
    expected_candidate: bool
    candidate_proposed: bool
    semantic_review_required: bool
    candidate_had_review_features: bool
    candidate_had_counterexample_search: bool
    promoted_without_review: bool = False
    runtime_ms: float | None = None
    notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PrecipitationQualityReport:
    """Conservative summary for precipitation release decisions."""

    verdict: PrecipitationQualityVerdict
    case_count: int
    true_positive: int
    false_positive: int
    true_negative: int
    false_negative: int
    precision: float
    recall: float
    false_positive_rate: float
    review_gate_rate: float
    review_feature_rate: float
    counterexample_search_rate: float
    max_runtime_ms: float | None
    reasons: tuple[str, ...]

    @property
    def broad_enablement_allowed(self) -> bool:
        return self.verdict == "enablement_candidate"

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["broad_enablement_allowed"] = self.broad_enablement_allowed
        return data


def observation_from_review_payload(
    *,
    case_id: str,
    expected_candidate: bool,
    payload: dict[str, Any] | None,
    runtime_ms: float | None = None,
    promoted_without_review: bool = False,
    notes: Sequence[str] = (),
) -> PrecipitationQualityObservation:
    """Build a quality observation from a T4 pattern-review trigger payload."""

    payload = payload or {}
    observed_tendency = _dict_value(payload.get("observed_tendency"))
    review_features = _dict_value(observed_tendency.get("review_features"))
    has_review_features = bool(
        review_features.get("review_caution")
        and isinstance(review_features.get("feature_axes"), list)
    )
    has_counterexample_search = (
        "candidate_counterexample_count" in review_features
        and "cross_cluster_counterexample_count" in review_features
    )
    return PrecipitationQualityObservation(
        case_id=case_id,
        expected_candidate=expected_candidate,
        candidate_proposed=bool(payload.get("pattern_candidate_id")),
        semantic_review_required=payload.get("review_mode") == "semantic_required",
        candidate_had_review_features=has_review_features,
        candidate_had_counterexample_search=has_counterexample_search,
        promoted_without_review=promoted_without_review,
        runtime_ms=runtime_ms,
        notes=tuple(str(note) for note in notes if note),
    )


def assess_precipitation_quality(
    observations: Sequence[PrecipitationQualityObservation],
    *,
    min_cases_for_shadow: int = 4,
    min_cases_for_enablement: int = 20,
    min_precision: float = 0.92,
    min_recall: float = 0.75,
    max_false_positive_rate: float = 0.08,
    require_semantic_review: bool = True,
    max_runtime_ms: float | None = None,
) -> PrecipitationQualityReport:
    """Assess whether precipitation evidence supports shadow or broad rollout."""

    cases = tuple(observations)
    true_positive = sum(1 for case in cases if case.expected_candidate and case.candidate_proposed)
    false_positive = sum(1 for case in cases if not case.expected_candidate and case.candidate_proposed)
    true_negative = sum(1 for case in cases if not case.expected_candidate and not case.candidate_proposed)
    false_negative = sum(1 for case in cases if case.expected_candidate and not case.candidate_proposed)
    predicted_positive = true_positive + false_positive
    actual_positive = true_positive + false_negative
    actual_negative = true_negative + false_positive
    precision = _ratio(true_positive, predicted_positive, default=1.0)
    recall = _ratio(true_positive, actual_positive, default=1.0)
    false_positive_rate = _ratio(false_positive, actual_negative, default=0.0)
    review_gate_rate = _ratio(
        sum(1 for case in cases if not case.candidate_proposed or case.semantic_review_required),
        len(cases),
        default=1.0,
    )
    review_feature_rate = _candidate_rate(
        cases,
        lambda case: case.candidate_had_review_features,
    )
    counterexample_search_rate = _candidate_rate(
        cases,
        lambda case: case.candidate_had_counterexample_search,
    )
    runtimes = [float(case.runtime_ms) for case in cases if case.runtime_ms is not None]
    observed_max_runtime_ms = max(runtimes) if runtimes else None
    reasons = _quality_reasons(
        cases,
        min_cases_for_shadow=min_cases_for_shadow,
        min_cases_for_enablement=min_cases_for_enablement,
        min_precision=min_precision,
        min_recall=min_recall,
        max_false_positive_rate=max_false_positive_rate,
        precision=precision,
        recall=recall,
        false_positive_rate=false_positive_rate,
        review_gate_rate=review_gate_rate,
        review_feature_rate=review_feature_rate,
        counterexample_search_rate=counterexample_search_rate,
        require_semantic_review=require_semantic_review,
        max_runtime_ms=max_runtime_ms,
        observed_max_runtime_ms=observed_max_runtime_ms,
    )
    verdict = _quality_verdict(
        cases,
        reasons=reasons,
        min_cases_for_shadow=min_cases_for_shadow,
        min_cases_for_enablement=min_cases_for_enablement,
    )
    return PrecipitationQualityReport(
        verdict=verdict,
        case_count=len(cases),
        true_positive=true_positive,
        false_positive=false_positive,
        true_negative=true_negative,
        false_negative=false_negative,
        precision=round(precision, 4),
        recall=round(recall, 4),
        false_positive_rate=round(false_positive_rate, 4),
        review_gate_rate=round(review_gate_rate, 4),
        review_feature_rate=round(review_feature_rate, 4),
        counterexample_search_rate=round(counterexample_search_rate, 4),
        max_runtime_ms=(
            round(observed_max_runtime_ms, 4)
            if observed_max_runtime_ms is not None
            else None
        ),
        reasons=tuple(reasons),
    )


def _quality_reasons(
    cases: tuple[PrecipitationQualityObservation, ...],
    *,
    min_cases_for_shadow: int,
    min_cases_for_enablement: int,
    min_precision: float,
    min_recall: float,
    max_false_positive_rate: float,
    precision: float,
    recall: float,
    false_positive_rate: float,
    review_gate_rate: float,
    review_feature_rate: float,
    counterexample_search_rate: float,
    require_semantic_review: bool,
    max_runtime_ms: float | None,
    observed_max_runtime_ms: float | None,
) -> list[str]:
    reasons: list[str] = []
    if len(cases) < min_cases_for_shadow:
        reasons.append("insufficient_shadow_cases")
    if len(cases) < min_cases_for_enablement:
        reasons.append("insufficient_enablement_cases")
    if precision < min_precision:
        reasons.append("precision_below_gate")
    if recall < min_recall:
        reasons.append("recall_below_gate")
    if false_positive_rate > max_false_positive_rate:
        reasons.append("false_positive_rate_above_gate")
    if require_semantic_review and review_gate_rate < 1.0:
        reasons.append("semantic_review_not_required_for_all_candidates")
    if any(case.promoted_without_review for case in cases):
        reasons.append("promotion_without_review_observed")
    if review_feature_rate < 1.0:
        reasons.append("review_features_missing_for_some_candidates")
    if counterexample_search_rate < 1.0:
        reasons.append("counterexample_search_missing_for_some_candidates")
    if (
        max_runtime_ms is not None
        and observed_max_runtime_ms is not None
        and observed_max_runtime_ms > max_runtime_ms
    ):
        reasons.append("runtime_above_gate")
    if not reasons:
        reasons.append("enablement_gate_passed")
    return reasons


def _quality_verdict(
    cases: tuple[PrecipitationQualityObservation, ...],
    *,
    reasons: Sequence[str],
    min_cases_for_shadow: int,
    min_cases_for_enablement: int,
) -> PrecipitationQualityVerdict:
    blocking = set(reasons)
    if (
        len(cases) >= min_cases_for_enablement
        and blocking == {"enablement_gate_passed"}
    ):
        return "enablement_candidate"
    hard_blockers = {
        "precision_below_gate",
        "recall_below_gate",
        "false_positive_rate_above_gate",
        "semantic_review_not_required_for_all_candidates",
        "promotion_without_review_observed",
        "review_features_missing_for_some_candidates",
        "counterexample_search_missing_for_some_candidates",
        "runtime_above_gate",
    }
    if len(cases) >= min_cases_for_shadow and not blocking.intersection(hard_blockers):
        return "shadow_ready"
    return "weak_evidence_only"


def _candidate_rate(
    cases: tuple[PrecipitationQualityObservation, ...],
    predicate,
) -> float:
    candidates = [case for case in cases if case.candidate_proposed]
    if not candidates:
        return 1.0
    passed = sum(1 for case in candidates if predicate(case))
    return _ratio(passed, len(candidates), default=1.0)


def _ratio(numerator: int | float, denominator: int | float, *, default: float) -> float:
    return float(default) if not denominator else float(numerator) / float(denominator)


def _dict_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


__all__ = [
    "PrecipitationQualityObservation",
    "PrecipitationQualityReport",
    "PrecipitationQualityVerdict",
    "assess_precipitation_quality",
    "observation_from_review_payload",
]
