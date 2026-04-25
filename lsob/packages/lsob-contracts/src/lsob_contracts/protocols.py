"""Protocol interfaces every baseline / evaluator / harness piece codes against."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from lsob_contracts.diff import DiffOp
from lsob_contracts.models import (
    AblationConfig,
    EvalResult,
    Signal,
    SUTConfig,
    Trigger,
)
from lsob_contracts.queries import (
    AtRiskReport,
    Belief,
    BeliefQuery,
    EvaluationContext,
)


@runtime_checkable
class SystemUnderTest(Protocol):
    name: str
    max_concurrent_ingestion: int

    async def startup(self, config: SUTConfig) -> None: ...

    async def apply_ablation(self, ablation: AblationConfig) -> None: ...

    async def ingest_signal(self, signal: Signal) -> None: ...

    async def query_beliefs_at(
        self, query: BeliefQuery
    ) -> list[Belief]: ...

    async def query_at_risk_at(
        self, timestamp: datetime
    ) -> AtRiskReport: ...

    async def produce_diff_for_trigger(
        self, trigger: Trigger
    ) -> DiffOp: ...

    async def shutdown(self) -> None: ...


@runtime_checkable
class Evaluator(Protocol):
    layer_id: int
    metric_names: list[str]

    async def evaluate(self, ctx: EvaluationContext) -> list[EvalResult]: ...


@runtime_checkable
class Baseline(Protocol):
    name: str

    def construct_sut(self, config: SUTConfig) -> SystemUnderTest: ...
