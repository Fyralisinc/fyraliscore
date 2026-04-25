"""In-package mock registries so the harness stays runnable even before
Streams B (evaluators) and C (baselines) are merged. Exercised by tests.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from lsob_contracts import (
    AblationConfig,
    AtRiskReport,
    Belief,
    BeliefQuery,
    DiffOp,
    EvalResult,
    EvaluationContext,
    Signal,
    SUTConfig,
    Trigger,
)

from lsob_harness.phases import EvaluatorPhase


class MockSUT:
    """A tiny, deterministic SUT that just counts ingested signals."""

    name: str
    max_concurrent_ingestion: int = 1

    def __init__(self, name: str = "mock-sut") -> None:
        self.name = name
        self._ingested: list[Signal] = []
        self._ablation: AblationConfig | None = None
        self._started = False

    async def startup(self, config: SUTConfig) -> None:
        self._started = True
        self._config = config

    async def apply_ablation(self, ablation: AblationConfig) -> None:
        self._ablation = ablation

    async def ingest_signal(self, signal: Signal) -> None:
        self._ingested.append(signal)

    async def query_beliefs_at(self, query: BeliefQuery) -> list[Belief]:
        return []

    async def query_at_risk_at(self, timestamp: datetime) -> AtRiskReport:
        return AtRiskReport(timestamp=timestamp, items=[])

    async def produce_diff_for_trigger(self, trigger: Trigger) -> DiffOp:
        return DiffOp(diff_id=f"mock-{trigger.trigger_id}", produced_at=trigger.timestamp)

    async def shutdown(self) -> None:
        self._started = False

    @property
    def ingested_count(self) -> int:
        return len(self._ingested)


class MockSUTRegistry:
    """Fallback registry used when ``lsob_baselines`` isn't installed."""

    _names = ("mock", "mock-sut")

    @classmethod
    def construct(cls, name: str, config: SUTConfig) -> MockSUT:
        if name not in cls._names:
            raise KeyError(f"unknown mock SUT: {name}")
        return MockSUT(name=name)

    @classmethod
    def list_names(cls) -> list[str]:
        return list(cls._names)


class NoopEvaluator:
    """Minimal evaluator that emits a single zero-value metric per layer."""

    runs_at: EvaluatorPhase = EvaluatorPhase.per_month

    def __init__(
        self,
        layer_id: int,
        metric_name: str = "noop",
        phase: EvaluatorPhase = EvaluatorPhase.per_month,
    ) -> None:
        self.layer_id = layer_id
        self.metric_names = [metric_name]
        self.runs_at = phase

    async def evaluate(self, ctx: EvaluationContext) -> list[EvalResult]:
        value: float = float(getattr(ctx.sut, "ingested_count", 0))
        return [
            EvalResult(
                layer_id=self.layer_id,
                metric_name=self.metric_names[0],
                value=value,
                breakdown_by={"checkpoint": ctx.ground_truth_checkpoint.isoformat()},
                run_id=ctx.run_id,
            )
        ]


class MockEvaluatorRegistry:
    """Fallback evaluator registry: one NoopEvaluator per requested layer."""

    @classmethod
    def construct_for_layers(cls, layers: list[int]) -> list[Any]:
        out: list[Any] = []
        for layer in layers:
            phase = (
                EvaluatorPhase.final if layer in (3, 5, 6) else EvaluatorPhase.per_month
            )
            out.append(NoopEvaluator(layer_id=layer, metric_name=f"L{layer}.noop", phase=phase))
        return out
