"""LayerOneEvaluator: composes the three sub-evaluators behind the Evaluator Protocol."""

from __future__ import annotations

from lsob_contracts import EvalResult, EvaluationContext

from lsob_evaluator_l1.entity_resolution import EntityResolutionEvaluator
from lsob_evaluator_l1.l1_protocol import RetrievalCapableSUT
from lsob_evaluator_l1.reranker import RerankerEvaluator
from lsob_evaluator_l1.semantic import SemanticPathwayEvaluator


class LayerOneEvaluator:
    """Top-level Layer 1 evaluator. Implements the `Evaluator` Protocol."""

    layer_id: int = 1
    metric_names: list[str] = [
        *SemanticPathwayEvaluator.metric_names,
        *EntityResolutionEvaluator.metric_names,
        *RerankerEvaluator.metric_names,
    ]

    def __init__(self) -> None:
        self.semantic = SemanticPathwayEvaluator()
        self.entity = EntityResolutionEvaluator()
        self.reranker = RerankerEvaluator()

    async def evaluate(self, ctx: EvaluationContext) -> list[EvalResult]:
        if not isinstance(ctx.sut, RetrievalCapableSUT):
            return [
                EvalResult(
                    layer_id=1,
                    metric_name="layer_not_applicable",
                    value=0.0,
                    confidence_interval=None,
                    breakdown_by={
                        "reason": "SUT does not implement RetrievalCapableSUT",
                        "sut_type": type(ctx.sut).__name__,
                    },
                    run_id=ctx.run_id,
                )
            ]
        results: list[EvalResult] = []
        results.extend(await self.semantic.evaluate(ctx))
        results.extend(await self.entity.evaluate(ctx))
        results.extend(await self.reranker.evaluate(ctx))
        return results
