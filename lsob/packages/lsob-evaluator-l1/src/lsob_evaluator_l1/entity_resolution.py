"""EntityResolutionEvaluator: precision, recall, accuracy on ambiguous phrases."""

from __future__ import annotations

from collections import defaultdict

from lsob_contracts import EvalResult, EvaluationContext

from lsob_evaluator_l1._query_gen import build_entity_probes
from lsob_evaluator_l1.bootstrap import _bootstrap_ci
from lsob_evaluator_l1.l1_protocol import RetrievalCapableSUT

_METRIC_NAMES = (
    "entity_resolution_precision",
    "entity_resolution_recall",
    "entity_resolution_accuracy",
)


class EntityResolutionEvaluator:
    layer_id: int = 1
    metric_names: list[str] = list(_METRIC_NAMES)

    async def evaluate(self, ctx: EvaluationContext) -> list[EvalResult]:
        sut = ctx.sut
        if not isinstance(sut, RetrievalCapableSUT):
            return []
        probes = build_entity_probes(ctx.corpus)

        # Per-probe boolean vectors so we can bootstrap each metric.
        correct: list[float] = []  # accuracy contribution per probe (0/1)
        precision_terms: list[float] = []  # only when SUT returned a guess
        recall_terms: list[float] = []  # only when gold is not None
        per_month_counts: dict[str, dict[str, int]] = defaultdict(
            lambda: {"tp": 0, "fp": 0, "fn": 0, "tn": 0}
        )

        for probe in probes:
            guess = await sut.retrieval_entity_resolve(
                probe.phrase, probe.author_id
            )
            is_correct = guess == probe.gold_entity_id
            correct.append(1.0 if is_correct else 0.0)

            bucket = per_month_counts[probe.month]
            if probe.gold_entity_id is None and guess is None:
                bucket["tn"] += 1
            elif probe.gold_entity_id is None and guess is not None:
                bucket["fp"] += 1
                precision_terms.append(0.0)
            elif probe.gold_entity_id is not None and guess is None:
                bucket["fn"] += 1
                recall_terms.append(0.0)
            else:
                # both non-None
                if is_correct:
                    bucket["tp"] += 1
                    precision_terms.append(1.0)
                    recall_terms.append(1.0)
                else:
                    bucket["fp"] += 1
                    bucket["fn"] += 1
                    precision_terms.append(0.0)
                    recall_terms.append(0.0)

        def _mean(xs: list[float]) -> float:
            return sum(xs) / len(xs) if xs else 0.0

        per_month_breakdown = {
            m: {
                "precision": (
                    c["tp"] / (c["tp"] + c["fp"])
                    if (c["tp"] + c["fp"])
                    else 0.0
                ),
                "recall": (
                    c["tp"] / (c["tp"] + c["fn"])
                    if (c["tp"] + c["fn"])
                    else 0.0
                ),
                "accuracy": (
                    (c["tp"] + c["tn"]) / sum(c.values())
                    if sum(c.values())
                    else 0.0
                ),
            }
            for m, c in per_month_counts.items()
        }

        raw: dict[str, list[float]] = {
            "entity_resolution_precision": precision_terms,
            "entity_resolution_recall": recall_terms,
            "entity_resolution_accuracy": correct,
        }
        results: list[EvalResult] = []
        for name, values in raw.items():
            results.append(
                EvalResult(
                    layer_id=1,
                    metric_name=name,
                    value=_mean(values),
                    confidence_interval=_bootstrap_ci(values) if values else None,
                    breakdown_by={
                        "by_month": per_month_breakdown,
                        "n_samples": len(values),
                    },
                    run_id=ctx.run_id,
                )
            )
        return results
