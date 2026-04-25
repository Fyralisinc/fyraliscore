"""LlamaIndex-knowledge-graph-style baseline.

Production version would use ``llama_index.KnowledgeGraphIndex`` for
ingestion and a ``TreeIndex`` for queries. Phase 1 implements an
in-memory adjacency-dict KG with regex-based entity extraction, so the
tests can run without ``llama_index``. If ``llama_index`` is importable
(heavy optional dep group), we flag it on the instance; the fallback
path is always active in Phase 1.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

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
from .common import extract_entities
from .diff_translator import DiffTranslator, TemplateDiffTranslator
from .registry import REGISTRY


def _llama_index_available() -> bool:
    try:  # pragma: no cover - optional dep
        import llama_index  # noqa: F401

        return True
    except Exception:  # pragma: no cover
        return False


@dataclass
class _Node:
    entity: str
    signal_ids: list[str] = field(default_factory=list)
    neighbours: set[str] = field(default_factory=set)


class LlamaIndexKGBaseline:
    """In-memory KG over entities extracted from signal text."""

    name = "llamaindex-kg"
    max_concurrent_ingestion = 8

    def __init__(
        self,
        *,
        max_triples_per_signal: int = 8,
        retrieval_hops: int = 2,
        translator: DiffTranslator | None = None,
    ) -> None:
        self.max_triples_per_signal = max_triples_per_signal
        self.retrieval_hops = retrieval_hops
        self._translator: DiffTranslator = translator or TemplateDiffTranslator()
        self._nodes: dict[str, _Node] = {}
        self._signals_by_entity: dict[str, list[str]] = defaultdict(list)
        self._state = BaselineState()
        self._llama = _llama_index_available()

    async def startup(self, config: SUTConfig) -> None:
        self._state.config = config
        self._state.started = True

    async def apply_ablation(self, ablation: AblationConfig) -> None:
        self._state.ablation = ablation

    async def ingest_signal(self, signal: Signal) -> None:
        self._state.signals.append(signal)
        ents = extract_entities(signal.content_text)[: self.max_triples_per_signal]
        # Also pull entity refs from metadata.
        for v in signal.metadata.values():
            if isinstance(v, str) and v not in ents:
                ents.append(v)
        for e in ents:
            node = self._nodes.setdefault(e, _Node(entity=e))
            node.signal_ids.append(signal.signal_id)
            self._signals_by_entity[e].append(signal.signal_id)
        # Add co-occurrence edges.
        for i, a in enumerate(ents):
            for b in ents[i + 1 :]:
                self._nodes[a].neighbours.add(b)
                self._nodes[b].neighbours.add(a)

    def _neighbourhood(self, entity: str, hops: int) -> set[str]:
        if entity not in self._nodes:
            return set()
        seen = {entity}
        frontier = {entity}
        for _ in range(max(0, hops)):
            nxt: set[str] = set()
            for n in frontier:
                nxt |= self._nodes[n].neighbours
            nxt -= seen
            seen |= nxt
            frontier = nxt
            if not frontier:
                break
        return seen

    async def query_beliefs_at(self, query: BeliefQuery) -> list[Belief]:
        entity = query.entity_ref.id
        hood = self._neighbourhood(entity, self.retrieval_hops)
        signal_ids: set[str] = set()
        for e in hood:
            signal_ids.update(self._signals_by_entity.get(e, []))
        sigs = [
            s
            for s in self._state.signals
            if s.signal_id in signal_ids and s.timestamp <= query.timestamp
        ]
        if not sigs:
            sigs = signals_mentioning(
                self._state.signals, query.entity_ref, query.timestamp
            )
        if not sigs:
            return []
        return [
            make_belief_from_signals(
                query,
                sigs,
                proposition_kind=query.proposition_kind or "kg_summary",
                confidence=0.5,
                source="llamaindex-kg",
            )
        ][: max(1, query.k)]

    async def query_at_risk_at(self, timestamp: datetime) -> AtRiskReport:
        return simple_at_risk_from_signals(self._state.signals, timestamp)

    async def produce_diff_for_trigger(self, trigger: Trigger) -> DiffOp:
        anchor = ""
        ref = trigger.payload.get("entity_ref")
        if isinstance(ref, str):
            anchor = ref
        if not anchor:
            ents = extract_entities(str(trigger.payload))
            anchor = ents[0] if ents else ""
        hood = self._neighbourhood(anchor, self.retrieval_hops) if anchor else set()
        signal_ids: set[str] = set()
        for e in hood:
            signal_ids.update(self._signals_by_entity.get(e, []))
        sigs = [s for s in self._state.signals if s.signal_id in signal_ids]
        context = "\n".join(s.content_text for s in sigs[-10:])
        return self._translator.translate(
            trigger=trigger,
            retrieved_context=context,
            evidence_signal_ids=[s.signal_id for s in sigs],
            entities=sorted(hood),
        )

    async def shutdown(self) -> None:
        self._state.started = False
        self._nodes.clear()
        self._signals_by_entity.clear()


def _factory(config: SUTConfig) -> LlamaIndexKGBaseline:
    params = config.params or {}
    return LlamaIndexKGBaseline(
        max_triples_per_signal=int(params.get("max_triples_per_signal", 8)),
        retrieval_hops=int(params.get("retrieval_hops", 2)),
    )


REGISTRY.register("llamaindex-kg", _factory)


__all__ = ["LlamaIndexKGBaseline"]
