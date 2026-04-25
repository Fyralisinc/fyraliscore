"""LayerFiveEvaluator: temporal-dynamics metrics for LSOB.

Five sub-evaluations — calibration trajectory, pattern precipitation latency,
belief stability, retrieval quality drift, and shock recovery — run over the
full corpus duration. Each sub-evaluation emits one or more `EvalResult`
instances; missing capabilities degrade gracefully to `layer_not_applicable`.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any

from lsob_contracts import (
    Belief,
    BeliefQuery,
    Corpus,
    EntityRef,
    EvalResult,
    EvaluationContext,
    GroundTruth,
    PatternTruth,
    TurbulenceEvent,
)

from lsob_evaluator_l5.metrics import linear_regression, mean_median_p90
from lsob_evaluator_l5.stability import StableWindow, find_stable_windows

logger = logging.getLogger(__name__)

_METRIC_NAMES = (
    "calibration_trajectory_slope",
    "calibration_trajectory_r2",
    "pattern_precipitation_latency_mean",
    "pattern_precipitation_latency_median",
    "pattern_precipitation_latency_p90",
    "belief_churn_per_month",
    "retrieval_recall_at_10_trajectory",
    "shock_recovery_months",
)


def _month_key(ts: datetime) -> str:
    return f"{ts.year:04d}-{ts.month:02d}"


def _checkpoint_sorted(corpus: Corpus) -> list[GroundTruth]:
    return sorted(corpus.ground_truth, key=lambda g: g.timestamp)


def _turbulence_from_corpus(corpus: Corpus) -> list[TurbulenceEvent]:
    """Extract TurbulenceEvents wherever a corpus may carry them.

    Contract-level corpora don't declare turbulence directly, but various
    channels may carry them: (a) an instance attribute `turbulence_events` set
    by the simulation layer; (b) per-checkpoint `turbulence` sidecars; (c) the
    corpus meta attribute `turbulence_events`.
    """
    events: list[TurbulenceEvent] = []
    direct = getattr(corpus, "turbulence_events", None)
    if direct:
        for ev in direct:
            if isinstance(ev, TurbulenceEvent):
                events.append(ev)
            elif isinstance(ev, dict):
                try:
                    events.append(TurbulenceEvent(**ev))
                except Exception:
                    continue
    meta_direct = getattr(corpus.meta, "turbulence_events", None)
    if meta_direct:
        for ev in meta_direct:
            if isinstance(ev, TurbulenceEvent):
                events.append(ev)
            elif isinstance(ev, dict):
                try:
                    events.append(TurbulenceEvent(**ev))
                except Exception:
                    continue
    for gt in corpus.ground_truth:
        sidecar = getattr(gt, "turbulence", None) or gt.__dict__.get("turbulence")
        if not sidecar:
            continue
        if isinstance(sidecar, list):
            for ev in sidecar:
                if isinstance(ev, TurbulenceEvent):
                    events.append(ev)
                elif isinstance(ev, dict):
                    try:
                        events.append(TurbulenceEvent(**ev))
                    except Exception:
                        continue
    # Deduplicate by event_id.
    seen: set[str] = set()
    deduped: list[TurbulenceEvent] = []
    for ev in events:
        if ev.event_id in seen:
            continue
        seen.add(ev.event_id)
        deduped.append(ev)
    deduped.sort(key=lambda e: e.scheduled_at)
    return deduped


def _patterns_from_corpus(corpus: Corpus) -> list[PatternTruth]:
    """Extract PatternTruth objects the corpus carries about its gold patterns.

    Tolerant: supports dict-serialized patterns on `GroundTruth.patterns` and
    model-object patterns hanging off either the meta object or the corpus
    itself. We keep only the earliest emergence per pattern_id to define the
    detection latency starting point.
    """
    raw: list[PatternTruth] = []

    def _try(ev: Any) -> None:
        if isinstance(ev, PatternTruth):
            raw.append(ev)
        elif isinstance(ev, dict):
            try:
                raw.append(PatternTruth(**ev))
            except Exception:
                pass

    direct = getattr(corpus, "pattern_truths", None) or getattr(
        corpus, "patterns", None
    )
    if direct:
        for ev in direct:
            _try(ev)
    meta_direct = getattr(corpus.meta, "pattern_truths", None) or getattr(
        corpus.meta, "patterns", None
    )
    if meta_direct:
        for ev in meta_direct:
            _try(ev)
    # Dict-shaped patterns on each checkpoint's `patterns` list are optional.
    for gt in corpus.ground_truth:
        for ev in gt.patterns:
            _try(ev)
    # Keep earliest-emerging record per id.
    by_id: dict[str, PatternTruth] = {}
    for p in raw:
        existing = by_id.get(p.pattern_id)
        if existing is None or p.emergence_at < existing.emergence_at:
            by_id[p.pattern_id] = p
    return sorted(by_id.values(), key=lambda p: p.emergence_at)


class LayerFiveEvaluator:
    """Layer 5 (temporal dynamics) evaluator — composes 5 sub-evaluations."""

    layer_id: int = 5
    metric_names: list[str] = list(_METRIC_NAMES)

    async def evaluate(self, ctx: EvaluationContext) -> list[EvalResult]:
        results: list[EvalResult] = []
        results.extend(await self._calibration_trajectory(ctx))
        results.extend(await self._pattern_precipitation_latency(ctx))
        results.extend(await self._belief_stability(ctx))
        results.extend(await self._retrieval_drift(ctx))
        results.extend(await self._shock_recovery(ctx))
        return results

    # ------------------------------------------------------------------
    # 1. Calibration improvement trajectory
    # ------------------------------------------------------------------

    async def _calibration_trajectory(
        self, ctx: EvaluationContext
    ) -> list[EvalResult]:
        try:
            from lsob_evaluator_l3.metrics import ece  # type: ignore
        except ImportError:
            logger.warning(
                "lsob_evaluator_l3.metrics.ece not importable; "
                "calibration trajectory not applicable"
            )
            return [
                EvalResult(
                    layer_id=5,
                    metric_name="calibration_trajectory_slope",
                    value=0.0,
                    breakdown_by={"reason": "layer_not_applicable"},
                    run_id=ctx.run_id,
                )
            ]

        gts = _checkpoint_sorted(ctx.corpus)
        months: list[str] = []
        eces: list[float] = []
        breakdown_months: dict[str, float] = {}

        for idx, gt in enumerate(gts):
            predictions: list[tuple[float, bool]] = []
            for pred in gt.predictions_that_will_resolve:
                conf = pred.get("asserted_confidence") or pred.get("confidence")
                outcome = pred.get("outcome")
                if conf is None or outcome is None:
                    continue
                # Outcomes may arrive as "true"/"false" or booleans.
                correct = outcome is True or str(outcome).lower() == "true"
                predictions.append((float(conf), correct))
            if not predictions:
                continue
            try:
                # L3's `ece` takes an iterable of (confidence, outcome) tuples.
                value = float(ece(predictions))
            except TypeError:
                # Tolerant fallback: some implementations may take (confs, outs)
                # as two sequences instead of tuple-iterables.
                try:
                    confidences = [p[0] for p in predictions]
                    correctness = [p[1] for p in predictions]
                    value = float(ece(confidences, correctness))  # type: ignore[arg-type]
                except Exception as exc:  # noqa: BLE001
                    logger.warning("ece() call failed: %s", exc)
                    continue
            except Exception as exc:  # noqa: BLE001
                logger.warning("ece() call failed: %s", exc)
                continue
            month = _month_key(gt.timestamp)
            months.append(month)
            eces.append(value)
            breakdown_months[month] = value

        if len(eces) < 2:
            return [
                EvalResult(
                    layer_id=5,
                    metric_name="calibration_trajectory_slope",
                    value=0.0,
                    breakdown_by={
                        "reason": "insufficient_checkpoints",
                        "n_points": len(eces),
                        "by_month": breakdown_months,
                    },
                    run_id=ctx.run_id,
                ),
                EvalResult(
                    layer_id=5,
                    metric_name="calibration_trajectory_r2",
                    value=0.0,
                    breakdown_by={
                        "reason": "insufficient_checkpoints",
                        "n_points": len(eces),
                    },
                    run_id=ctx.run_id,
                ),
            ]

        xs = [float(i) for i in range(len(eces))]
        slope, r2 = linear_regression(xs, eces)
        return [
            EvalResult(
                layer_id=5,
                metric_name="calibration_trajectory_slope",
                value=slope,
                breakdown_by={
                    "by_month": breakdown_months,
                    "n_points": len(eces),
                    "claim_met": slope < 0,
                },
                run_id=ctx.run_id,
            ),
            EvalResult(
                layer_id=5,
                metric_name="calibration_trajectory_r2",
                value=r2,
                breakdown_by={"n_points": len(eces)},
                run_id=ctx.run_id,
            ),
        ]

    # ------------------------------------------------------------------
    # 2. Pattern precipitation latency
    # ------------------------------------------------------------------

    async def _pattern_precipitation_latency(
        self, ctx: EvaluationContext
    ) -> list[EvalResult]:
        patterns = _patterns_from_corpus(ctx.corpus)
        if not patterns:
            return [
                EvalResult(
                    layer_id=5,
                    metric_name="pattern_precipitation_latency_mean",
                    value=0.0,
                    breakdown_by={"reason": "no patterns in corpus"},
                    run_id=ctx.run_id,
                )
            ]

        checkpoints = _checkpoint_sorted(ctx.corpus)
        per_pattern_latency: dict[str, float | None] = {}
        time_travel_broken = False
        for p in patterns:
            detection_latency: float | None = None
            for gt in checkpoints:
                if gt.timestamp < p.emergence_at:
                    continue
                query = BeliefQuery(
                    query_id=f"l5-pattern-{p.pattern_id}-{_month_key(gt.timestamp)}",
                    entity_ref=EntityRef(kind="pattern", id=p.pattern_id),
                    timestamp=gt.timestamp,
                    proposition_kind="pattern",
                    k=5,
                )
                try:
                    beliefs = await ctx.sut.query_beliefs_at(query)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "time-travel query failed for %s: %s",
                        p.pattern_id,
                        exc,
                    )
                    time_travel_broken = True
                    break
                if beliefs and any(
                    p.pattern_id in (b.entities or []) or p.pattern_id in b.proposition
                    for b in beliefs
                ):
                    months = _months_between(p.emergence_at, gt.timestamp)
                    detection_latency = float(months)
                    break
            per_pattern_latency[p.pattern_id] = detection_latency

        latencies = [v for v in per_pattern_latency.values() if v is not None]
        breakdown: dict[str, Any] = {
            "by_pattern": {k: (v if v is not None else "not_detected")
                           for k, v in per_pattern_latency.items()},
            "n_patterns": len(patterns),
            "n_detected": len(latencies),
        }
        if time_travel_broken:
            breakdown["note"] = "time_travel_not_supported"

        mean, median, p90 = mean_median_p90(latencies)
        return [
            EvalResult(
                layer_id=5,
                metric_name="pattern_precipitation_latency_mean",
                value=mean,
                breakdown_by=breakdown,
                run_id=ctx.run_id,
            ),
            EvalResult(
                layer_id=5,
                metric_name="pattern_precipitation_latency_median",
                value=median,
                breakdown_by={"n_detected": len(latencies)},
                run_id=ctx.run_id,
            ),
            EvalResult(
                layer_id=5,
                metric_name="pattern_precipitation_latency_p90",
                value=p90,
                breakdown_by={"n_detected": len(latencies)},
                run_id=ctx.run_id,
            ),
        ]

    # ------------------------------------------------------------------
    # 3. Belief stability for stable truths
    # ------------------------------------------------------------------

    async def _belief_stability(
        self, ctx: EvaluationContext
    ) -> list[EvalResult]:
        windows = find_stable_windows(_checkpoint_sorted(ctx.corpus), window=6)
        if not windows:
            return [
                EvalResult(
                    layer_id=5,
                    metric_name="belief_churn_per_month",
                    value=0.0,
                    breakdown_by={
                        "reason": "no stable 6-month windows in ground truth"
                    },
                    run_id=ctx.run_id,
                )
            ]

        churn_samples: list[float] = []
        per_window: list[dict[str, Any]] = []
        time_travel_broken = False

        for w in windows:
            churn, broke = await _count_churn_for_window(ctx.sut, w)
            if broke:
                time_travel_broken = True
                continue
            churn_per_month = churn / max(len(w.checkpoint_timestamps), 1)
            churn_samples.append(churn_per_month)
            per_window.append(
                {
                    "entity_kind": w.entity_kind,
                    "entity_id": w.entity_id,
                    "field": w.field,
                    "stable_value": w.value,
                    "changes_observed": churn,
                    "churn_per_month": churn_per_month,
                }
            )

        if not churn_samples:
            return [
                EvalResult(
                    layer_id=5,
                    metric_name="belief_churn_per_month",
                    value=0.0,
                    breakdown_by={
                        "reason": "time_travel_not_supported"
                        if time_travel_broken
                        else "no samples",
                        "n_windows": len(windows),
                    },
                    run_id=ctx.run_id,
                )
            ]

        mean_churn = float(sum(churn_samples) / len(churn_samples))
        breakdown: dict[str, Any] = {
            "n_windows": len(windows),
            "per_window": per_window,
            "excessive": mean_churn > 0.5,
        }
        if time_travel_broken:
            breakdown["note"] = "time_travel_not_supported"
        return [
            EvalResult(
                layer_id=5,
                metric_name="belief_churn_per_month",
                value=mean_churn,
                breakdown_by=breakdown,
                run_id=ctx.run_id,
            )
        ]

    # ------------------------------------------------------------------
    # 4. Retrieval quality drift
    # ------------------------------------------------------------------

    async def _retrieval_drift(
        self, ctx: EvaluationContext
    ) -> list[EvalResult]:
        try:
            from lsob_evaluator_l1.l1_protocol import RetrievalCapableSUT
            from lsob_evaluator_l1.semantic import (  # type: ignore
                SemanticPathwayEvaluator,
            )
        except ImportError:
            logger.warning(
                "lsob_evaluator_l1 imports missing; retrieval drift skipped"
            )
            return [
                EvalResult(
                    layer_id=5,
                    metric_name="retrieval_recall_at_10_trajectory",
                    value=0.0,
                    breakdown_by={"reason": "layer_not_applicable"},
                    run_id=ctx.run_id,
                )
            ]

        if not isinstance(ctx.sut, RetrievalCapableSUT):
            return [
                EvalResult(
                    layer_id=5,
                    metric_name="retrieval_recall_at_10_trajectory",
                    value=0.0,
                    breakdown_by={"reason": "layer_not_applicable"},
                    run_id=ctx.run_id,
                )
            ]

        months = len(_checkpoint_sorted(ctx.corpus))
        anchors = [m for m in (3, 6, 9, 12) if m <= months]
        if not anchors and months > 0:
            # Use whatever we have (very small corpus) — the last checkpoint.
            anchors = [months]
        trajectory: dict[str, float] = {}
        sub_evaluator = SemanticPathwayEvaluator()

        gts = _checkpoint_sorted(ctx.corpus)
        for month_idx in anchors:
            sliced_corpus = ctx.corpus.model_copy(
                update={"ground_truth": gts[:month_idx]}
            )
            sub_ctx = EvaluationContext(
                corpus=sliced_corpus,
                sut=ctx.sut,
                ground_truth_checkpoint=gts[month_idx - 1].timestamp,
                run_id=ctx.run_id,
                extras={**ctx.extras, "l5_anchor_month": month_idx},
            )
            try:
                sub_results = await sub_evaluator.evaluate(sub_ctx)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "SemanticPathwayEvaluator anchor %s failed: %s",
                    month_idx,
                    exc,
                )
                continue
            for r in sub_results:
                if r.metric_name == "semantic_recall_at_10":
                    trajectory[f"month_{month_idx}"] = r.value

        if not trajectory:
            return [
                EvalResult(
                    layer_id=5,
                    metric_name="retrieval_recall_at_10_trajectory",
                    value=0.0,
                    breakdown_by={"reason": "no retrieval anchors available"},
                    run_id=ctx.run_id,
                )
            ]

        # Fit a slope over the trajectory for drift detection.
        ordered = sorted(trajectory.items(), key=lambda kv: kv[0])
        xs = [float(i) for i in range(len(ordered))]
        ys = [v for _, v in ordered]
        slope, r2 = linear_regression(xs, ys)
        mean_recall = float(sum(ys) / len(ys))
        return [
            EvalResult(
                layer_id=5,
                metric_name="retrieval_recall_at_10_trajectory",
                value=mean_recall,
                breakdown_by={
                    "by_anchor": trajectory,
                    "slope": slope,
                    "r2": r2,
                },
                run_id=ctx.run_id,
            )
        ]

    # ------------------------------------------------------------------
    # 5. Recovery from shocks
    # ------------------------------------------------------------------

    async def _shock_recovery(
        self, ctx: EvaluationContext
    ) -> list[EvalResult]:
        shocks = _turbulence_from_corpus(ctx.corpus)
        if not shocks:
            return [
                EvalResult(
                    layer_id=5,
                    metric_name="recovery_na",
                    value=0.0,
                    breakdown_by={"reason": "no shocks in corpus"},
                    run_id=ctx.run_id,
                )
            ]

        gts = _checkpoint_sorted(ctx.corpus)
        if len(gts) < 2:
            return [
                EvalResult(
                    layer_id=5,
                    metric_name="shock_recovery_months",
                    value=0.0,
                    breakdown_by={"reason": "insufficient_checkpoints"},
                    run_id=ctx.run_id,
                )
            ]

        per_shock: list[dict[str, Any]] = []
        recovery_times: list[float] = []
        time_travel_broken = False
        for shock in shocks:
            pre_idxs = [i for i, g in enumerate(gts) if g.timestamp < shock.scheduled_at]
            post_idxs = [i for i, g in enumerate(gts) if g.timestamp >= shock.scheduled_at]
            if not pre_idxs or not post_idxs:
                per_shock.append(
                    {
                        "event_id": shock.event_id,
                        "months_to_recover": None,
                        "reason": "shock outside corpus range",
                    }
                )
                continue
            pre_accuracies: list[float] = []
            for i in pre_idxs:
                acc, broke = await _belief_accuracy_at(ctx.sut, gts[i])
                if broke:
                    time_travel_broken = True
                    break
                if acc is not None:
                    pre_accuracies.append(acc)
            if time_travel_broken:
                break
            if not pre_accuracies:
                per_shock.append(
                    {
                        "event_id": shock.event_id,
                        "months_to_recover": None,
                        "reason": "no pre-shock baseline",
                    }
                )
                continue
            baseline = sum(pre_accuracies) / len(pre_accuracies)
            recovery_months: float | None = None
            for offset, i in enumerate(post_idxs, start=1):
                acc, broke = await _belief_accuracy_at(ctx.sut, gts[i])
                if broke:
                    time_travel_broken = True
                    break
                if acc is None:
                    continue
                if acc >= baseline:
                    recovery_months = float(offset)
                    break
            if time_travel_broken:
                break
            per_shock.append(
                {
                    "event_id": shock.event_id,
                    "kind": shock.kind.value if hasattr(shock.kind, "value") else shock.kind,
                    "baseline": baseline,
                    "months_to_recover": recovery_months,
                }
            )
            if recovery_months is not None:
                recovery_times.append(recovery_months)

        breakdown: dict[str, Any] = {
            "per_shock": per_shock,
            "n_shocks": len(shocks),
        }
        if time_travel_broken:
            breakdown["note"] = "time_travel_not_supported"

        if not recovery_times:
            return [
                EvalResult(
                    layer_id=5,
                    metric_name="shock_recovery_months",
                    value=0.0,
                    breakdown_by=breakdown,
                    run_id=ctx.run_id,
                )
            ]
        mean_recovery = float(sum(recovery_times) / len(recovery_times))
        return [
            EvalResult(
                layer_id=5,
                metric_name="shock_recovery_months",
                value=mean_recovery,
                breakdown_by=breakdown,
                run_id=ctx.run_id,
            )
        ]


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _months_between(a: datetime, b: datetime) -> int:
    """Month-granularity distance from `a` to `b` (ignores day-of-month)."""
    return (b.year - a.year) * 12 + (b.month - a.month)


async def _count_churn_for_window(
    sut: Any, window: StableWindow
) -> tuple[int, bool]:
    """Count how many times the SUT's belief *value* changes across `window`.

    Returns (changes, time_travel_broken).
    """
    last_value: str | None = None
    changes = 0
    for ts in window.checkpoint_timestamps:
        query = BeliefQuery(
            query_id=f"l5-stab-{window.entity_kind}-{window.entity_id}-{_month_key(ts)}",
            entity_ref=EntityRef(kind=window.entity_kind, id=window.entity_id),
            timestamp=ts,
            proposition_kind=None,
            k=1,
        )
        try:
            beliefs = await sut.query_beliefs_at(query)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "stability query failed for %s/%s: %s",
                window.entity_kind,
                window.entity_id,
                exc,
            )
            return 0, True
        current = _extract_value(beliefs)
        if last_value is not None and current is not None and current != last_value:
            changes += 1
        if current is not None:
            last_value = current
    return changes, False


def _extract_value(beliefs: list[Belief]) -> str | None:
    """Pull the primary value from the first belief.

    Looks at `proposition`: if it contains "=", keep only the RHS; else use
    the whole proposition string. Empty result yields None so we can skip
    churn counting rather than pretending all missing ticks are distinct.
    """
    if not beliefs:
        return None
    prop = beliefs[0].proposition
    if "=" in prop:
        return prop.split("=", 1)[1].strip()
    return prop.strip() or None


async def _belief_accuracy_at(
    sut: Any, gt: GroundTruth
) -> tuple[float | None, bool]:
    """Proxy for Layer 2 belief accuracy at a single checkpoint.

    Compares belief value per (commitment, true_outcome) and (customer,
    true_health). Returns (accuracy, time_travel_broken).
    """
    correct = 0
    total = 0
    for c in gt.commitments:
        cid = c.get("id") or c.get("commitment_id")
        true_val = c.get("true_outcome")
        if cid is None or true_val is None:
            continue
        query = BeliefQuery(
            query_id=f"l5-acc-commit-{cid}-{_month_key(gt.timestamp)}",
            entity_ref=EntityRef(kind="commitment", id=str(cid)),
            timestamp=gt.timestamp,
            k=1,
        )
        try:
            beliefs = await sut.query_beliefs_at(query)
        except Exception as exc:  # noqa: BLE001
            logger.warning("belief accuracy query failed: %s", exc)
            return None, True
        predicted = _extract_value(beliefs)
        if predicted is None:
            continue
        total += 1
        if predicted == str(true_val):
            correct += 1
    for cu in gt.customers:
        cuid = cu.get("id") or cu.get("customer_id")
        true_val = cu.get("true_health")
        if cuid is None or true_val is None:
            continue
        query = BeliefQuery(
            query_id=f"l5-acc-cust-{cuid}-{_month_key(gt.timestamp)}",
            entity_ref=EntityRef(kind="customer", id=str(cuid)),
            timestamp=gt.timestamp,
            k=1,
        )
        try:
            beliefs = await sut.query_beliefs_at(query)
        except Exception as exc:  # noqa: BLE001
            logger.warning("belief accuracy query failed: %s", exc)
            return None, True
        predicted = _extract_value(beliefs)
        if predicted is None:
            continue
        total += 1
        if predicted == str(true_val):
            correct += 1
    if total == 0:
        return None, False
    return correct / total, False
