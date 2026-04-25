"""Layer 2 evaluator — belief-correctness, five sub-evaluations.

Public entry point: `LayerTwoEvaluator`. It implements the `Evaluator`
protocol from `lsob_contracts.protocols`. The evaluator is intentionally
tolerant: if the SUT cannot serve a query at a checkpoint (raising on
`query_beliefs_at`), the sub-metric records a single
`layer_not_applicable=1.0` result rather than crashing the whole run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

from lsob_contracts import (
    Belief,
    BeliefQuery,
    EvalResult,
    EvaluationContext,
    GroundTruth,
)

from lsob_evaluator_l2 import metrics as M
from lsob_evaluator_l2.query_builders import (
    commitment_queries,
    customer_queries,
    pattern_queries,
    prediction_queries,
)

LAYER_ID = 2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _belief_text(beliefs: list[Belief]) -> str:
    return " ".join(
        f"{b.proposition} :: {','.join(b.entities)}" for b in beliefs
    ).lower()


def _na(metric_name: str, breakdown: dict[str, Any] | None = None) -> EvalResult:
    """Produce a layer_not_applicable-style EvalResult."""
    br = dict(breakdown or {})
    br["layer_not_applicable"] = True
    return EvalResult(
        layer_id=LAYER_ID,
        metric_name=metric_name,
        value=0.0,
        confidence_interval=None,
        breakdown_by=br,
    )


# ---------------------------------------------------------------------------
# Sub-evaluator: commitment state
# ---------------------------------------------------------------------------

# SUT proposition text may contain any of several phrasings. We recognize
# a canonical vocabulary by substring.
_COMMITMENT_STATE_TERMS: tuple[str, ...] = (
    "slipped_but_completed",
    "will_be_cancelled",
    "will_succeed",
    "will_slip",
    "succeeded",
    "cancelled",
    "open",
)


def classify_commitment_state(beliefs: list[Belief]) -> str | None:
    """Extract a canonical commitment state from a bag of beliefs."""
    text = _belief_text(beliefs)
    if not text:
        return None
    for term in _COMMITMENT_STATE_TERMS:
        if term in text:
            return term
    return None


async def evaluate_commitments(
    ground_truths: list[GroundTruth], sut: Any
) -> list[EvalResult]:
    """Metric: state_accuracy across all commitments × checkpoints."""
    predicted: list[str] = []
    actual: list[str] = []
    per_entity: list[dict[str, Any]] = []
    unreachable = 0
    total_queries = 0

    for gt in ground_truths:
        truth_by_id = {c["id"]: c["true_outcome"] for c in gt.commitments}
        for q in commitment_queries(gt):
            total_queries += 1
            try:
                beliefs = await sut.query_beliefs_at(q)
            except Exception:
                unreachable += 1
                continue
            pred = classify_commitment_state(beliefs) or "__unknown__"
            truth = truth_by_id[q.entity_ref.id]
            predicted.append(pred)
            actual.append(truth)
            per_entity.append(
                {
                    "checkpoint": gt.timestamp.isoformat(),
                    "commitment_id": q.entity_ref.id,
                    "predicted": pred,
                    "actual": truth,
                    "correct": pred == truth,
                }
            )

    if total_queries and unreachable == total_queries:
        return [_na("state_accuracy", {"commitments_queried": total_queries})]
    if not predicted:
        return [_na("state_accuracy", {"commitments_queried": 0})]

    acc = M.state_accuracy(predicted, actual)
    hits = [1.0 if p == a else 0.0 for p, a in zip(predicted, actual)]
    ci = M.bootstrap_ci(hits)
    return [
        EvalResult(
            layer_id=LAYER_ID,
            metric_name="state_accuracy",
            value=acc,
            confidence_interval=ci,
            breakdown_by={
                "commitments_queried": total_queries,
                "unreachable": unreachable,
                "per_entity": per_entity,
            },
        )
    ]


# ---------------------------------------------------------------------------
# Sub-evaluator: prediction accuracy
# ---------------------------------------------------------------------------


_PREDICTION_TRUE_TOKENS = ("-> true", "outcome=true", "resolves=true", "true")
_PREDICTION_FALSE_TOKENS = ("-> false", "outcome=false", "resolves=false", "false")


def classify_prediction_outcome(beliefs: list[Belief]) -> bool | None:
    """Return True for a 'true' outcome belief, False for 'false', None if ambiguous."""
    text = _belief_text(beliefs)
    if not text:
        return None
    # Check 'false' first, since "true" is a substring of "truest"/"trust" etc.,
    # but "false" is more specific.
    has_false = any(tok in text for tok in _PREDICTION_FALSE_TOKENS)
    has_true = any(tok in text for tok in _PREDICTION_TRUE_TOKENS if tok != "true")
    if not has_true and not has_false:
        # Fallback: raw token presence.
        has_true = " true" in text or text.endswith("true")
    if has_true and not has_false:
        return True
    if has_false and not has_true:
        return False
    return None


def _in_window(pred: dict[str, Any], start: datetime, end: datetime) -> bool:
    ts_raw = pred.get("resolves_at")
    if ts_raw is None:
        return False
    ts = ts_raw if isinstance(ts_raw, datetime) else datetime.fromisoformat(
        ts_raw.replace("Z", "+00:00")
    )
    return start <= ts <= end


async def evaluate_predictions(
    ground_truths: list[GroundTruth], sut: Any, corpus_start: datetime, corpus_end: datetime
) -> list[EvalResult]:
    predicted: list[bool] = []
    actual: list[bool] = []
    per_entity: list[dict[str, Any]] = []
    unreachable = 0
    total_queries = 0

    for gt in ground_truths:
        preds_by_id = {
            p["prediction_id"]: p for p in gt.predictions_that_will_resolve
        }
        for q in prediction_queries(gt):
            pr = preds_by_id[q.entity_ref.id]
            # Only resolutions within the eval window count.
            if not _in_window(pr, corpus_start, corpus_end):
                continue
            total_queries += 1
            try:
                beliefs = await sut.query_beliefs_at(q)
            except Exception:
                unreachable += 1
                continue
            p_out = classify_prediction_outcome(beliefs)
            a_out = pr["outcome"] == "true"
            if p_out is None:
                # Unclassifiable -> treat as opposite of truth (a miss).
                p_out = not a_out
            predicted.append(p_out)
            actual.append(a_out)
            per_entity.append(
                {
                    "checkpoint": gt.timestamp.isoformat(),
                    "prediction_id": q.entity_ref.id,
                    "predicted": p_out,
                    "actual": a_out,
                    "correct": p_out == a_out,
                }
            )

    if total_queries and unreachable == total_queries:
        return [
            _na("accuracy", {"predictions_queried": total_queries}),
            _na("false_positive_rate", {"predictions_queried": total_queries}),
            _na("false_negative_rate", {"predictions_queried": total_queries}),
        ]
    if not predicted:
        return [
            _na("accuracy", {"predictions_queried": 0}),
            _na("false_positive_rate", {"predictions_queried": 0}),
            _na("false_negative_rate", {"predictions_queried": 0}),
        ]

    acc, fpr, fnr = M.accuracy_fpr_fnr(predicted, actual)
    hits = [1.0 if p == a else 0.0 for p, a in zip(predicted, actual)]
    ci = M.bootstrap_ci(hits)
    common = {
        "predictions_queried": total_queries,
        "unreachable": unreachable,
        "per_entity": per_entity,
    }
    return [
        EvalResult(
            layer_id=LAYER_ID,
            metric_name="accuracy",
            value=acc,
            confidence_interval=ci,
            breakdown_by=common,
        ),
        EvalResult(
            layer_id=LAYER_ID,
            metric_name="false_positive_rate",
            value=fpr,
            confidence_interval=None,
            breakdown_by=common,
        ),
        EvalResult(
            layer_id=LAYER_ID,
            metric_name="false_negative_rate",
            value=fnr,
            confidence_interval=None,
            breakdown_by=common,
        ),
    ]


# ---------------------------------------------------------------------------
# Sub-evaluator: customer health
# ---------------------------------------------------------------------------


def classify_customer_health(beliefs: list[Belief]) -> str | None:
    text = _belief_text(beliefs)
    if not text:
        return None
    # Prefer later-in-ladder matches so "critical" wins over "healthy" when
    # both tokens appear.
    for rung in reversed(M.HEALTH_LADDER):
        if rung in text:
            return rung
    return None


async def evaluate_customer_health(
    ground_truths: list[GroundTruth], sut: Any
) -> list[EvalResult]:
    predicted: list[str] = []
    actual: list[str] = []
    per_entity: list[dict[str, Any]] = []
    unreachable = 0
    total_queries = 0

    for gt in ground_truths:
        truth_by_id = {c["id"]: c["true_health"] for c in gt.customers}
        for q in customer_queries(gt):
            total_queries += 1
            try:
                beliefs = await sut.query_beliefs_at(q)
            except Exception:
                unreachable += 1
                continue
            pred = classify_customer_health(beliefs) or "healthy"
            truth = truth_by_id[q.entity_ref.id]
            predicted.append(pred)
            actual.append(truth)
            per_entity.append(
                {
                    "checkpoint": gt.timestamp.isoformat(),
                    "customer_id": q.entity_ref.id,
                    "predicted": pred,
                    "actual": truth,
                    "correct": pred == truth,
                }
            )

    if total_queries and unreachable == total_queries:
        return [
            _na("health_accuracy", {"customers_queried": total_queries}),
            _na("mean_ordinal_distance", {"customers_queried": total_queries}),
        ]
    if not predicted:
        return [
            _na("health_accuracy", {"customers_queried": 0}),
            _na("mean_ordinal_distance", {"customers_queried": 0}),
        ]

    acc = M.state_accuracy(predicted, actual)
    mod = M.mean_ordinal_distance(predicted, actual)
    hits = [1.0 if p == a else 0.0 for p, a in zip(predicted, actual)]
    ci = M.bootstrap_ci(hits)
    common = {
        "customers_queried": total_queries,
        "unreachable": unreachable,
        "per_entity": per_entity,
    }
    return [
        EvalResult(
            layer_id=LAYER_ID,
            metric_name="health_accuracy",
            value=acc,
            confidence_interval=ci,
            breakdown_by=common,
        ),
        EvalResult(
            layer_id=LAYER_ID,
            metric_name="mean_ordinal_distance",
            value=mod,
            confidence_interval=None,
            breakdown_by=common,
        ),
    ]


# ---------------------------------------------------------------------------
# Sub-evaluators: pattern recall + precision
# ---------------------------------------------------------------------------


def _pattern_matches(pid: str, beliefs: list[Belief]) -> bool:
    pid_l = pid.lower()
    for b in beliefs:
        if pid in b.entities:
            return True
        if pid_l in b.proposition.lower():
            return True
    return False


def _months_between(a: datetime, b: datetime) -> float:
    delta_days = (b - a).total_seconds() / 86400.0
    return delta_days / 30.0


@dataclass
class _PatternProbe:
    pattern_id: str
    detected: bool
    detection_latency_months: float | None


async def evaluate_patterns(
    ground_truths: list[GroundTruth], sut: Any
) -> list[EvalResult]:
    probes: list[_PatternProbe] = []
    spurious_patterns = 0
    matched_patterns = 0
    unreachable = 0
    total_queries = 0

    for gt in ground_truths:
        truth_ids = {p["id"] for p in gt.patterns}
        eligible_by_id: dict[str, datetime] = {}
        for p in gt.patterns:
            raw = p.get("detection_eligible_after")
            if raw is None:
                continue
            if isinstance(raw, datetime):
                eligible_by_id[p["id"]] = raw
            else:
                eligible_by_id[p["id"]] = datetime.fromisoformat(
                    raw.replace("Z", "+00:00")
                )

        # Recall: iterate ground-truth patterns, query SUT.
        for q in pattern_queries(gt):
            total_queries += 1
            try:
                beliefs = await sut.query_beliefs_at(q)
            except Exception:
                unreachable += 1
                continue
            detected = _pattern_matches(q.entity_ref.id, beliefs)
            latency: float | None = None
            if detected and q.entity_ref.id in eligible_by_id:
                latency = _months_between(
                    eligible_by_id[q.entity_ref.id], gt.timestamp
                )
            probes.append(
                _PatternProbe(
                    pattern_id=q.entity_ref.id,
                    detected=detected,
                    detection_latency_months=latency,
                )
            )
            if detected:
                matched_patterns += 1

            # Precision sanity: any belief whose entities include ids that
            # are NOT part of ground-truth patterns counts as a false
            # detection. Patterns the SUT returns for a GT query but which
            # reference a different pattern id are spurious.
            for b in beliefs:
                for ent in b.entities:
                    if ent and ent not in truth_ids:
                        spurious_patterns += 1

    if total_queries and unreachable == total_queries:
        return [
            _na("detection_recall", {"patterns_queried": total_queries}),
            _na("detection_latency_months", {"patterns_queried": total_queries}),
            _na("false_pattern_rate", {"patterns_queried": total_queries}),
        ]
    if not probes:
        return [
            _na("detection_recall", {"patterns_queried": 0}),
            _na("detection_latency_months", {"patterns_queried": 0}),
            _na("false_pattern_rate", {"patterns_queried": 0}),
        ]

    recall = sum(1 for p in probes if p.detected) / len(probes)
    latencies = [
        p.detection_latency_months
        for p in probes
        if p.detected and p.detection_latency_months is not None
    ]
    mean_latency = sum(latencies) / len(latencies) if latencies else 0.0
    false_rate = M.false_pattern_rate(matched_patterns, spurious_patterns)

    common = {
        "patterns_queried": total_queries,
        "unreachable": unreachable,
        "matched_patterns": matched_patterns,
        "spurious_patterns": spurious_patterns,
        "per_pattern": [
            {
                "pattern_id": p.pattern_id,
                "detected": p.detected,
                "latency_months": p.detection_latency_months,
            }
            for p in probes
        ],
    }
    detected_flags = [1.0 if p.detected else 0.0 for p in probes]
    ci = M.bootstrap_ci(detected_flags)
    return [
        EvalResult(
            layer_id=LAYER_ID,
            metric_name="detection_recall",
            value=recall,
            confidence_interval=ci,
            breakdown_by=common,
        ),
        EvalResult(
            layer_id=LAYER_ID,
            metric_name="detection_latency_months",
            value=mean_latency,
            confidence_interval=None,
            breakdown_by=common,
        ),
        EvalResult(
            layer_id=LAYER_ID,
            metric_name="false_pattern_rate",
            value=false_rate,
            confidence_interval=None,
            breakdown_by=common,
        ),
    ]


# ---------------------------------------------------------------------------
# Aggregator
# ---------------------------------------------------------------------------


class LayerTwoEvaluator:
    """Aggregate belief-correctness evaluator (Layer 2)."""

    layer_id: int = LAYER_ID
    metric_names: list[str] = [
        "state_accuracy",
        "accuracy",
        "false_positive_rate",
        "false_negative_rate",
        "health_accuracy",
        "mean_ordinal_distance",
        "detection_recall",
        "detection_latency_months",
        "false_pattern_rate",
    ]

    async def evaluate(self, ctx: EvaluationContext) -> list[EvalResult]:
        gts = list(ctx.corpus.ground_truth)
        sut = ctx.sut
        results: list[EvalResult] = []
        results.extend(await evaluate_commitments(gts, sut))
        results.extend(
            await evaluate_predictions(
                gts, sut, ctx.corpus.meta.start_date, ctx.corpus.meta.end_date
            )
        )
        results.extend(await evaluate_customer_health(gts, sut))
        results.extend(await evaluate_patterns(gts, sut))
        # Stamp run_id on every result when we have one.
        if ctx.run_id:
            for r in results:
                r.run_id = ctx.run_id
        return results


__all__ = [
    "LayerTwoEvaluator",
    "LAYER_ID",
    "evaluate_commitments",
    "evaluate_customer_health",
    "evaluate_patterns",
    "evaluate_predictions",
    "classify_commitment_state",
    "classify_customer_health",
    "classify_prediction_outcome",
]
