"""Layer 3 (calibration) evaluator.

The evaluator ingests a ``Corpus`` + a ``SystemUnderTest``, pulls each resolved prediction's
corresponding belief from the SUT, and emits a list of ``EvalResult`` objects covering:

* Brier score (overall + stratified by ``proposition_kind``)
* ECE (10 equal-frequency bins) overall + per-actor
* Sharpness (mean, variance, histogram-collapse flag)
* Temporal trend: per-month ECE, plus slope + R^2 of ECE vs time
* Predictions-not-made count (when the SUT has no matching belief)

A reliability-diagram PNG is written to ``ctx.extras["output_dir"]`` (default ``/tmp/lsob-l3``).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lsob_contracts import (
    Belief,
    BeliefQuery,
    Corpus,
    EntityRef,
    EvalResult,
    EvaluationContext,
)

from lsob_evaluator_l3.metrics import (
    Prediction,
    bin_stats,
    brier_score,
    ece,
    linear_regression,
    sharpness,
)


DEFAULT_OUTPUT_DIR = "/tmp/lsob-l3"


@dataclass
class _ResolvedPrediction:
    prediction_id: str
    proposition: str
    proposition_kind: str
    actor_id: str
    asserted_confidence: float  # SUT-supplied, not ground-truth
    outcome: bool
    resolves_at: datetime
    belief_entities: list[str] = field(default_factory=list)


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        s = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    raise TypeError(f"cannot parse timestamp from {value!r}")


def _extract_actor_from_belief(belief: Belief | None, fallback: str) -> str:
    if belief is None:
        return fallback
    for e in belief.entities:
        if e.startswith("actor:"):
            return e.split(":", 1)[1]
    return fallback


def _outcome_to_bool(outcome: Any) -> bool:
    if isinstance(outcome, bool):
        return outcome
    if isinstance(outcome, str):
        return outcome.strip().lower() == "true"
    raise ValueError(f"unexpected outcome value: {outcome!r}")


class LayerThreeEvaluator:
    """Calibration evaluator, conforming to the ``Evaluator`` Protocol (layer_id=3)."""

    layer_id: int = 3
    metric_names: list[str] = [
        "brier",
        "brier_by_kind",
        "ece",
        "ece_by_actor",
        "ece_monthly",
        "ece_trend_slope",
        "ece_trend_r2",
        "sharpness_mean",
        "sharpness_variance",
        "sharpness_collapsed",
        "predictions_not_made",
        "reliability_diagram_path",
    ]

    def __init__(self, n_bins: int = 10) -> None:
        self.n_bins = n_bins

    async def evaluate(self, ctx: EvaluationContext) -> list[EvalResult]:
        output_dir = Path(ctx.extras.get("output_dir", DEFAULT_OUTPUT_DIR))
        output_dir.mkdir(parents=True, exist_ok=True)

        raw_predictions = self._collect_ground_truth_predictions(ctx.corpus)
        resolved, not_made = await self._resolve_predictions_via_sut(ctx, raw_predictions)

        results: list[EvalResult] = []

        # (1) Brier overall + per-kind
        preds_overall: list[Prediction] = [(p.asserted_confidence, p.outcome) for p in resolved]
        brier_all = brier_score(preds_overall)
        results.append(
            EvalResult(
                layer_id=self.layer_id,
                metric_name="brier",
                value=brier_all,
                run_id=ctx.run_id,
                breakdown_by={"n_predictions": len(resolved)},
            )
        )
        by_kind: dict[str, list[Prediction]] = defaultdict(list)
        for p in resolved:
            by_kind[p.proposition_kind or "unknown"].append((p.asserted_confidence, p.outcome))
        for kind, preds in sorted(by_kind.items()):
            results.append(
                EvalResult(
                    layer_id=self.layer_id,
                    metric_name="brier_by_kind",
                    value=brier_score(preds),
                    run_id=ctx.run_id,
                    breakdown_by={"proposition_kind": kind, "n": len(preds)},
                )
            )

        # (2) ECE overall
        ece_all = ece(preds_overall, n_bins=self.n_bins, mode="equal_frequency")
        results.append(
            EvalResult(
                layer_id=self.layer_id,
                metric_name="ece",
                value=ece_all,
                run_id=ctx.run_id,
                breakdown_by={"n_predictions": len(resolved), "n_bins": self.n_bins},
            )
        )

        # (3) Reliability diagram
        stats = bin_stats(preds_overall, n_bins=self.n_bins, mode="equal_frequency")
        png_path = output_dir / f"reliability_{ctx.run_id}.png"
        if preds_overall:
            # Deferred import — keeps matplotlib optional at import time for pure-metric users.
            from lsob_evaluator_l3.plots import reliability_diagram

            reliability_diagram(stats, png_path)
        results.append(
            EvalResult(
                layer_id=self.layer_id,
                metric_name="reliability_diagram_path",
                value=float(png_path.exists()),
                run_id=ctx.run_id,
                breakdown_by={"path": str(png_path)},
            )
        )

        # (4) Sharpness
        sr = sharpness([p.asserted_confidence for p in resolved])
        results.append(
            EvalResult(
                layer_id=self.layer_id,
                metric_name="sharpness_mean",
                value=sr.mean,
                run_id=ctx.run_id,
            )
        )
        results.append(
            EvalResult(
                layer_id=self.layer_id,
                metric_name="sharpness_variance",
                value=sr.variance,
                run_id=ctx.run_id,
                breakdown_by={
                    "histogram_counts": list(sr.histogram_counts),
                    "histogram_edges": list(sr.histogram_edges),
                },
            )
        )
        results.append(
            EvalResult(
                layer_id=self.layer_id,
                metric_name="sharpness_collapsed",
                value=1.0 if sr.collapsed_near_half else 0.0,
                run_id=ctx.run_id,
            )
        )

        # (5) Per-actor ECE
        by_actor: dict[str, list[Prediction]] = defaultdict(list)
        for p in resolved:
            by_actor[p.actor_id].append((p.asserted_confidence, p.outcome))
        for actor_id, preds in sorted(by_actor.items()):
            results.append(
                EvalResult(
                    layer_id=self.layer_id,
                    metric_name="ece_by_actor",
                    value=ece(preds, n_bins=self.n_bins, mode="equal_frequency"),
                    run_id=ctx.run_id,
                    breakdown_by={"actor_id": actor_id, "n": len(preds)},
                )
            )

        # (6) Monthly ECE + temporal trend
        monthly = self._monthly_ece(resolved)
        for month_key, value, n in monthly:
            results.append(
                EvalResult(
                    layer_id=self.layer_id,
                    metric_name="ece_monthly",
                    value=value,
                    run_id=ctx.run_id,
                    breakdown_by={"month": month_key, "n": n},
                )
            )
        if len(monthly) >= 2:
            xs = [float(i) for i in range(len(monthly))]
            ys = [m[1] for m in monthly]
            slope, _intercept, r2 = linear_regression(xs, ys)
        else:
            slope, r2 = 0.0, 0.0
        results.append(
            EvalResult(
                layer_id=self.layer_id,
                metric_name="ece_trend_slope",
                value=slope,
                run_id=ctx.run_id,
                breakdown_by={"n_months": len(monthly)},
            )
        )
        results.append(
            EvalResult(
                layer_id=self.layer_id,
                metric_name="ece_trend_r2",
                value=r2,
                run_id=ctx.run_id,
                breakdown_by={"n_months": len(monthly)},
            )
        )

        # (7) Predictions-not-made counter
        results.append(
            EvalResult(
                layer_id=self.layer_id,
                metric_name="predictions_not_made",
                value=float(not_made),
                run_id=ctx.run_id,
                breakdown_by={
                    "n_predictions_total": len(raw_predictions),
                    "n_predictions_resolved": len(resolved),
                },
            )
        )

        return results

    # ----------------------------------------------------------------- helpers
    @staticmethod
    def _collect_ground_truth_predictions(corpus: Corpus) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for gt in corpus.ground_truth:
            for pred in gt.predictions_that_will_resolve:
                out.append(pred)
        return out

    async def _resolve_predictions_via_sut(
        self,
        ctx: EvaluationContext,
        raw_predictions: list[dict[str, Any]],
    ) -> tuple[list[_ResolvedPrediction], int]:
        resolved: list[_ResolvedPrediction] = []
        not_made = 0
        for pred in raw_predictions:
            pid = pred["prediction_id"]
            resolves_at = _parse_ts(pred["resolves_at"])
            actor_hint = pred.get("actor_id", "unknown")
            entity_ref = EntityRef(kind="actor", id=actor_hint)
            query = BeliefQuery(
                query_id=pid,
                entity_ref=entity_ref,
                timestamp=resolves_at,
                proposition_kind=pred.get("proposition_kind"),
                k=1,
            )
            beliefs = await ctx.sut.query_beliefs_at(query)
            if not beliefs:
                not_made += 1
                continue
            belief = beliefs[0]
            actor = _extract_actor_from_belief(belief, actor_hint)
            resolved.append(
                _ResolvedPrediction(
                    prediction_id=pid,
                    proposition=pred["proposition"],
                    proposition_kind=belief.proposition_kind
                    or pred.get("proposition_kind")
                    or "unknown",
                    actor_id=actor,
                    asserted_confidence=float(belief.asserted_confidence),
                    outcome=_outcome_to_bool(pred["outcome"]),
                    resolves_at=resolves_at,
                    belief_entities=list(belief.entities),
                )
            )
        return resolved, not_made

    def _monthly_ece(
        self, resolved: list[_ResolvedPrediction]
    ) -> list[tuple[str, float, int]]:
        if not resolved:
            return []
        buckets: dict[str, list[Prediction]] = defaultdict(list)
        for p in resolved:
            key = f"{p.resolves_at.year:04d}-{p.resolves_at.month:02d}"
            buckets[key].append((p.asserted_confidence, p.outcome))
        out: list[tuple[str, float, int]] = []
        for key in sorted(buckets.keys()):
            preds = buckets[key]
            out.append(
                (key, ece(preds, n_bins=self.n_bins, mode="equal_frequency"), len(preds))
            )
        return out
