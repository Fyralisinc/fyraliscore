"""RerankerEvaluator: nDCG@10 and Kendall tau on re-ranking tasks."""

from __future__ import annotations

from collections import defaultdict

from lsob_contracts import EvalResult, EvaluationContext

from lsob_evaluator_l1._query_gen import build_reranker_probes
from lsob_evaluator_l1.bootstrap import _bootstrap_ci
from lsob_evaluator_l1.l1_protocol import RetrievalCapableSUT
from lsob_evaluator_l1.metrics import kendall_tau, ndcg_at_k

_METRIC_NAMES = ("reranker_ndcg_at_10", "reranker_kendall_tau")


class RerankerEvaluator:
    layer_id: int = 1
    metric_names: list[str] = list(_METRIC_NAMES)

    async def evaluate(self, ctx: EvaluationContext) -> list[EvalResult]:
        sut = ctx.sut
        if not isinstance(sut, RetrievalCapableSUT):
            return []
        probes = build_reranker_probes(ctx.corpus)
        ndcg_vals: list[float] = []
        tau_vals: list[float] = []
        by_month: dict[str, dict[str, list[float]]] = {
            "reranker_ndcg_at_10": defaultdict(list),
            "reranker_kendall_tau": defaultdict(list),
        }
        for probe in probes:
            reranked = await sut.retrieval_rerank(
                list(probe.candidates), probe.query_text
            )
            # Graded gains: top of gold_order gets highest gain.
            n = len(probe.gold_order)
            relevance = {
                item: float(n - i) for i, item in enumerate(probe.gold_order)
            }
            ndcg = ndcg_at_k(reranked, relevance, 10)
            tau = kendall_tau(reranked, probe.gold_order)
            ndcg_vals.append(ndcg)
            tau_vals.append(tau)
            by_month["reranker_ndcg_at_10"][probe.month].append(ndcg)
            by_month["reranker_kendall_tau"][probe.month].append(tau)

        def _mean(xs: list[float]) -> float:
            return sum(xs) / len(xs) if xs else 0.0

        results: list[EvalResult] = []
        for name, values in (
            ("reranker_ndcg_at_10", ndcg_vals),
            ("reranker_kendall_tau", tau_vals),
        ):
            results.append(
                EvalResult(
                    layer_id=1,
                    metric_name=name,
                    value=_mean(values),
                    confidence_interval=_bootstrap_ci(values) if values else None,
                    breakdown_by={
                        "by_month": {
                            m: _mean(vs) for m, vs in by_month[name].items()
                        },
                        "n_queries": len(values),
                    },
                    run_id=ctx.run_id,
                )
            )
        return results
