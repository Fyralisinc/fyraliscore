"""MemGPT-style two-tier memory baseline.

Implements a simplified version of the tiered-memory pattern:

* **Working memory**: the most recent ``working_capacity`` signals
  (default 200). Cheap to scan, used for the "hot" context window.
* **Recall memory**: every signal that has ever been ingested; scanned
  only on a miss from working memory.

Eviction from working to recall is strict recency. The real MemGPT
additionally gates eviction through an LLM; that is deferred to Phase 2
via the ``heavy`` optional-dependency group.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime

from lsob_contracts import (
    AblationConfig,
    AtRiskReport,
    Belief,
    BeliefQuery,
    DiffOp,
    Signal,
    SUTConfig,
    Trigger,
)

from ._base import (
    BaselineState,
    make_belief_from_signals,
    signals_mentioning,
    simple_at_risk_from_signals,
)
from .diff_translator import DiffTranslator, TemplateDiffTranslator
from .registry import REGISTRY


class MemGPTStyleBaseline:
    """Two-tier recency-evicting memory adapter."""

    name = "memgpt-style"
    max_concurrent_ingestion = 4

    def __init__(
        self,
        *,
        working_capacity: int = 200,
        translator: DiffTranslator | None = None,
    ) -> None:
        self.working_capacity = working_capacity
        self._translator: DiffTranslator = translator or TemplateDiffTranslator()
        self._working: deque[Signal] = deque()
        self._recall: list[Signal] = []
        self._state = BaselineState()

    async def startup(self, config: SUTConfig) -> None:
        self._state.config = config
        self._state.started = True

    async def apply_ablation(self, ablation: AblationConfig) -> None:
        self._state.ablation = ablation

    async def ingest_signal(self, signal: Signal) -> None:
        self._state.signals.append(signal)
        self._recall.append(signal)
        self._working.append(signal)
        while len(self._working) > self.working_capacity:
            self._working.popleft()

    def _search(self, query_fn, timestamp: datetime) -> list[Signal]:
        working_hits = [s for s in self._working if s.timestamp <= timestamp and query_fn(s)]
        if working_hits:
            return working_hits
        return [s for s in self._recall if s.timestamp <= timestamp and query_fn(s)]

    async def query_beliefs_at(self, query: BeliefQuery) -> list[Belief]:
        key = query.entity_ref.id

        def matches(s: Signal) -> bool:
            if key in s.content_text:
                return True
            return any(v == key for v in s.metadata.values() if isinstance(v, str))

        hits = self._search(matches, query.timestamp)
        if not hits:
            hits = signals_mentioning(
                self._state.signals, query.entity_ref, query.timestamp
            )
        if not hits:
            return []
        return [
            make_belief_from_signals(
                query,
                hits,
                proposition_kind=query.proposition_kind or "memgpt_summary",
                confidence=0.5,
                source="memgpt-style",
            )
        ][: max(1, query.k)]

    async def query_at_risk_at(self, timestamp: datetime) -> AtRiskReport:
        return simple_at_risk_from_signals(self._state.signals, timestamp)

    async def produce_diff_for_trigger(self, trigger: Trigger) -> DiffOp:
        ref = trigger.payload.get("entity_ref")

        def matches(s: Signal) -> bool:
            if isinstance(ref, str) and ref and (ref in s.content_text or ref in s.metadata.values()):
                return True
            return trigger.kind.lower() in s.content_text.lower()

        hits = self._search(matches, trigger.timestamp)
        context = "\n".join(s.content_text for s in hits[-10:])
        return self._translator.translate(
            trigger=trigger,
            retrieved_context=context,
            evidence_signal_ids=[s.signal_id for s in hits],
            entities=[ref] if isinstance(ref, str) and ref else [],
        )

    async def shutdown(self) -> None:
        self._state.started = False
        self._working.clear()
        self._recall.clear()


def _factory(config: SUTConfig) -> MemGPTStyleBaseline:
    params = config.params or {}
    return MemGPTStyleBaseline(
        working_capacity=int(params.get("working_capacity", 200)),
    )


REGISTRY.register("memgpt-style", _factory)


__all__ = ["MemGPTStyleBaseline"]
