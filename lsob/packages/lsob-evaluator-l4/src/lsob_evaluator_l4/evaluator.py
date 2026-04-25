"""LayerFourEvaluator — surfacing-quality sub-evaluations.

Four sub-evaluations:

1. At-risk commitment precision / recall / F1
2. Customer risk precision / recall / F1
3. Anomaly precision
4. Alert fatigue (emitted/genuine anomaly ratio, reported per month)

Metrics 3 and 4 require the SUT to implement :class:`AnomalyEmittingSUT`
(i.e. an ``async emitted_anomalies(start, end)`` method). When that surface
is missing the evaluator still runs metrics 1 and 2 but emits
``layer_not_applicable`` EvalResults for 3 and 4.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any

from lsob_contracts import (
    AtRiskReport,
    EvalResult,
    EvaluationContext,
)

from lsob_evaluator_l4.l4_protocol import AnomalyEmittingSUT
from lsob_evaluator_l4.metrics import (
    DEFAULT_WINDOW,
    derive_degrading_customers,
    derive_positive_commitments,
    extract_ground_truth_timestamp,
    precision_recall_f1,
    turbulence_events_from_ground_truth,
)

_ANOMALY_WINDOW = timedelta(weeks=2)

_METRIC_NAMES = [
    "at_risk_commitment_precision",
    "at_risk_commitment_recall",
    "at_risk_commitment_f1",
    "customer_risk_precision",
    "customer_risk_recall",
    "customer_risk_f1",
    "anomaly_precision",
    "alert_fatigue_ratio",
]


def _month_key(ts: datetime) -> str:
    return f"{ts.year:04d}-{ts.month:02d}"


def _collect_commitment_ids(report: AtRiskReport) -> set[str]:
    return {
        item.entity_ref.id
        for item in report.items
        if item.entity_ref.kind == "commitment"
    }


def _collect_customer_ids(report: AtRiskReport) -> set[str]:
    return {
        item.entity_ref.id
        for item in report.items
        if item.entity_ref.kind == "customer"
    }


def _make_result(
    metric_name: str,
    value: float,
    breakdown: dict[str, Any],
    run_id: str | None,
) -> EvalResult:
    return EvalResult(
        layer_id=4,
        metric_name=metric_name,
        value=value,
        breakdown_by=breakdown,
        run_id=run_id,
    )


def _make_not_applicable(metric_name: str, run_id: str | None) -> EvalResult:
    return EvalResult(
        layer_id=4,
        metric_name=metric_name,
        value=0.0,
        breakdown_by={"layer_not_applicable": True},
        run_id=run_id,
    )


class LayerFourEvaluator:
    layer_id: int = 4
    metric_names: list[str] = list(_METRIC_NAMES)

    def __init__(
        self,
        *,
        commitment_window: timedelta = DEFAULT_WINDOW,
        customer_window: timedelta = DEFAULT_WINDOW,
        anomaly_window: timedelta = _ANOMALY_WINDOW,
    ) -> None:
        self.commitment_window = commitment_window
        self.customer_window = customer_window
        self.anomaly_window = anomaly_window

    async def evaluate(self, ctx: EvaluationContext) -> list[EvalResult]:
        corpus = ctx.corpus
        sut = ctx.sut
        ground_truth = list(corpus.ground_truth)
        results: list[EvalResult] = []

        # Metric 1 & 2: always run — they only need `query_at_risk_at`.
        results.extend(
            await self._eval_at_risk(
                sut=sut,
                ground_truth=ground_truth,
                run_id=ctx.run_id,
            )
        )

        # Metrics 3 & 4: require AnomalyEmittingSUT.
        if isinstance(sut, AnomalyEmittingSUT):
            results.extend(
                await self._eval_anomalies_and_fatigue(
                    sut=sut,
                    corpus_start=corpus.meta.start_date,
                    corpus_end=corpus.meta.end_date,
                    ground_truth=ground_truth,
                    run_id=ctx.run_id,
                )
            )
        else:
            results.append(
                _make_not_applicable("anomaly_precision", ctx.run_id)
            )
            results.append(
                _make_not_applicable("alert_fatigue_ratio", ctx.run_id)
            )

        return results

    # -------------------------------------------------------------------
    # Sub-evaluations 1 & 2 — at-risk commitments / customers
    # -------------------------------------------------------------------

    async def _eval_at_risk(
        self,
        *,
        sut: Any,
        ground_truth: list[Any],
        run_id: str | None,
    ) -> list[EvalResult]:
        per_checkpoint_commit: list[tuple[str, int, int, int]] = []
        per_checkpoint_customer: list[tuple[str, int, int, int]] = []

        for gt in ground_truth:
            checkpoint = extract_ground_truth_timestamp(gt)
            if checkpoint is None:
                continue
            report = await sut.query_at_risk_at(checkpoint)
            month = _month_key(checkpoint)

            # Commitments
            positives = derive_positive_commitments(
                [gt], checkpoint, self.commitment_window
            )
            predicted = _collect_commitment_ids(report)
            tp = len(predicted & positives)
            fp = len(predicted - positives)
            fn = len(positives - predicted)
            per_checkpoint_commit.append((month, tp, fp, fn))

            # Customers
            cust_positives = derive_degrading_customers(
                [gt], checkpoint, self.customer_window
            )
            cust_predicted = _collect_customer_ids(report)
            tp_c = len(cust_predicted & cust_positives)
            fp_c = len(cust_predicted - cust_positives)
            fn_c = len(cust_positives - cust_predicted)
            per_checkpoint_customer.append((month, tp_c, fp_c, fn_c))

        results: list[EvalResult] = []
        results.extend(
            self._aggregate_prf1(
                per_checkpoint_commit,
                prefix="at_risk_commitment",
                run_id=run_id,
            )
        )
        results.extend(
            self._aggregate_prf1(
                per_checkpoint_customer,
                prefix="customer_risk",
                run_id=run_id,
            )
        )
        return results

    def _aggregate_prf1(
        self,
        entries: list[tuple[str, int, int, int]],
        *,
        prefix: str,
        run_id: str | None,
    ) -> list[EvalResult]:
        total_tp = sum(e[1] for e in entries)
        total_fp = sum(e[2] for e in entries)
        total_fn = sum(e[3] for e in entries)
        precision, recall, f1 = precision_recall_f1(total_tp, total_fp, total_fn)

        by_month: dict[str, dict[str, float]] = {}
        grouped: dict[str, list[tuple[int, int, int]]] = defaultdict(list)
        for month, tp, fp, fn in entries:
            grouped[month].append((tp, fp, fn))
        for month, rows in grouped.items():
            m_tp = sum(r[0] for r in rows)
            m_fp = sum(r[1] for r in rows)
            m_fn = sum(r[2] for r in rows)
            p, r, f = precision_recall_f1(m_tp, m_fp, m_fn)
            by_month[month] = {
                "precision": p,
                "recall": r,
                "f1": f,
                "tp": float(m_tp),
                "fp": float(m_fp),
                "fn": float(m_fn),
            }

        shared_breakdown = {
            "by_month": by_month,
            "tp": float(total_tp),
            "fp": float(total_fp),
            "fn": float(total_fn),
            "n_checkpoints": len(entries),
        }

        return [
            _make_result(
                f"{prefix}_precision", precision, shared_breakdown, run_id
            ),
            _make_result(
                f"{prefix}_recall", recall, shared_breakdown, run_id
            ),
            _make_result(f"{prefix}_f1", f1, shared_breakdown, run_id),
        ]

    # -------------------------------------------------------------------
    # Sub-evaluations 3 & 4 — anomaly precision / alert fatigue
    # -------------------------------------------------------------------

    async def _eval_anomalies_and_fatigue(
        self,
        *,
        sut: AnomalyEmittingSUT,
        corpus_start: datetime,
        corpus_end: datetime,
        ground_truth: list[Any],
        run_id: str | None,
    ) -> list[EvalResult]:
        genuine_events = turbulence_events_from_ground_truth(ground_truth)
        emitted = await sut.emitted_anomalies(corpus_start, corpus_end)

        # Normalize emitted anomaly timestamps.
        normalized_emitted = [
            {
                **a,
                "timestamp": _to_datetime(a["timestamp"]),
            }
            for a in emitted
            if "timestamp" in a
        ]

        # 3) Anomaly precision — each emitted anomaly is a TP if there's a
        #    genuine event within ±anomaly_window.
        tp = 0
        fp = 0
        tp_by_month: dict[str, int] = defaultdict(int)
        fp_by_month: dict[str, int] = defaultdict(int)
        for anomaly in normalized_emitted:
            ts: datetime = anomaly["timestamp"]
            if _has_nearby_event(ts, genuine_events, self.anomaly_window):
                tp += 1
                tp_by_month[_month_key(ts)] += 1
            else:
                fp += 1
                fp_by_month[_month_key(ts)] += 1
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        anomaly_breakdown = {
            "tp": float(tp),
            "fp": float(fp),
            "n_emitted": float(len(normalized_emitted)),
            "n_genuine": float(len(genuine_events)),
            "by_month": {
                m: {
                    "tp": float(tp_by_month.get(m, 0)),
                    "fp": float(fp_by_month.get(m, 0)),
                }
                for m in sorted(set(tp_by_month) | set(fp_by_month))
            },
        }

        # 4) Alert fatigue — emitted/genuine counts per month; aggregate
        #    value is the mean ratio across months (months without genuine
        #    events contribute infinity-capped-at-emitted-count, but we skip
        #    empty months entirely when reporting the scalar).
        emitted_by_month: dict[str, int] = defaultdict(int)
        for a in normalized_emitted:
            emitted_by_month[_month_key(a["timestamp"])] += 1
        genuine_by_month: dict[str, int] = defaultdict(int)
        for ev in genuine_events:
            genuine_by_month[_month_key(ev["timestamp"])] += 1

        all_months = sorted(set(emitted_by_month) | set(genuine_by_month))
        per_month_ratio: dict[str, float] = {}
        defined_ratios: list[float] = []
        for month in all_months:
            emitted_count = emitted_by_month.get(month, 0)
            genuine_count = genuine_by_month.get(month, 0)
            if genuine_count == 0:
                # No ground-truth anomalies this month — a finite ratio is
                # undefined. Report it as None and skip from aggregation.
                per_month_ratio[month] = float(emitted_count) if emitted_count == 0 else float("inf")
                if emitted_count == 0:
                    defined_ratios.append(0.0)
            else:
                ratio = emitted_count / genuine_count
                per_month_ratio[month] = ratio
                defined_ratios.append(ratio)

        # Scalar aggregate: use totals if any genuine events, else 0.0 when
        # there are no emissions or inf otherwise.
        total_emitted = sum(emitted_by_month.values())
        total_genuine = sum(genuine_by_month.values())
        if total_genuine > 0:
            fatigue = total_emitted / total_genuine
        elif total_emitted == 0:
            fatigue = 0.0
        else:
            fatigue = float("inf")

        fatigue_breakdown = {
            "total_emitted": float(total_emitted),
            "total_genuine": float(total_genuine),
            "by_month": {
                m: {
                    "emitted": float(emitted_by_month.get(m, 0)),
                    "genuine": float(genuine_by_month.get(m, 0)),
                    "ratio": per_month_ratio[m],
                }
                for m in all_months
            },
        }

        return [
            _make_result(
                "anomaly_precision", precision, anomaly_breakdown, run_id
            ),
            _make_result(
                "alert_fatigue_ratio",
                fatigue if fatigue != float("inf") else float(total_emitted),
                fatigue_breakdown,
                run_id,
            ),
        ]


def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise TypeError(f"cannot coerce {type(value).__name__} to datetime")


def _has_nearby_event(
    anomaly_ts: datetime,
    events: Iterable[dict[str, Any]],
    window: timedelta,
) -> bool:
    for ev in events:
        delta = abs(anomaly_ts - ev["timestamp"])
        if delta <= window:
            return True
    return False
