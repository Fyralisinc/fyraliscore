"""SemanticPathwayEvaluator: recall@k, MRR, nDCG@10 for semantic retrieval."""

from __future__ import annotations

from collections import defaultdict

from lsob_contracts import EvalResult, EvaluationContext

from lsob_evaluator_l1._query_gen import build_semantic_probes
from lsob_evaluator_l1.bootstrap import _bootstrap_ci
from lsob_evaluator_l1.l1_protocol import RetrievalCapableSUT
from lsob_evaluator_l1.metrics import mrr, ndcg_at_k, recall_at_k

_METRIC_NAMES = (
    "semantic_recall_at_5",
    "semantic_recall_at_10",
    "semantic_recall_at_20",
    "semantic_mrr",
    "semantic_ndcg_at_10",
)


class SemanticPathwayEvaluator:
    layer_id: int = 1
    metric_names: list[str] = list(_METRIC_NAMES)

    async def evaluate(self, ctx: EvaluationContext) -> list[EvalResult]:
        sut = ctx.sut
        if not isinstance(sut, RetrievalCapableSUT):
            return []  # composite decides the "not applicable" framing
        probes = build_semantic_probes(ctx.corpus)
        per_metric: dict[str, list[float]] = {m: [] for m in _METRIC_NAMES}
        per_metric_by_month: dict[str, dict[str, list[float]]] = {
            m: defaultdict(list) for m in _METRIC_NAMES
        }
        per_metric_by_kind: dict[str, dict[str, list[float]]] = {
            m: defaultdict(list) for m in _METRIC_NAMES
        }
        for probe in probes:
            retrieved = await sut.retrieval_semantic(probe.query_text, k=20)
            relevance = {item: 1.0 for item in probe.relevant_item_ids}
            scores = {
                "semantic_recall_at_5": recall_at_k(
                    retrieved, probe.relevant_item_ids, 5
                ),
                "semantic_recall_at_10": recall_at_k(
                    retrieved, probe.relevant_item_ids, 10
                ),
                "semantic_recall_at_20": recall_at_k(
                    retrieved, probe.relevant_item_ids, 20
                ),
                "semantic_mrr": mrr(retrieved, probe.relevant_item_ids),
                "semantic_ndcg_at_10": ndcg_at_k(retrieved, relevance, 10),
            }
            for name, value in scores.items():
                per_metric[name].append(value)
                per_metric_by_month[name][probe.month].append(value)
                per_metric_by_kind[name][probe.proposition_kind].append(value)

        results: list[EvalResult] = []
        for name, values in per_metric.items():
            mean = sum(values) / len(values) if values else 0.0
            ci = _bootstrap_ci(values) if values else None
            breakdown = {
                "by_month": {
                    m: sum(vs) / len(vs) for m, vs in per_metric_by_month[name].items()
                },
                "by_proposition_kind": {
                    k: sum(vs) / len(vs)
                    for k, vs in per_metric_by_kind[name].items()
                },
                "n_queries": len(values),
            }
            results.append(
                EvalResult(
                    layer_id=1,
                    metric_name=name,
                    value=mean,
                    confidence_interval=ci,
                    breakdown_by=breakdown,
                    run_id=ctx.run_id,
                )
            )
        return results
