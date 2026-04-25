"""GraphRAG-style baseline.

Implements Microsoft's GraphRAG pattern in miniature:

1. Entity extraction from every signal (regex-based; see
   :func:`lsob_baselines.common.extract_entities`).
2. A co-occurrence graph over those entities.
3. Community detection via connected components.
4. Hierarchical summaries — one short summary per community, derived
   heuristically from the most recent signals touching that community.
5. Query-time traversal: pick the community the query entity belongs to,
   feed its summary + signals to the diff translator.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
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
from .common import extract_entities
from .diff_translator import DiffTranslator, TemplateDiffTranslator
from .registry import REGISTRY


@dataclass
class _Community:
    community_id: int
    entities: set[str] = field(default_factory=set)
    signal_ids: set[str] = field(default_factory=set)
    summary: str = ""


class GraphRAGBaseline:
    """Entity-community + hierarchical-summary memory adapter."""

    name = "graphrag"
    max_concurrent_ingestion = 4

    def __init__(
        self,
        *,
        summary_max_signals: int = 5,
        translator: DiffTranslator | None = None,
    ) -> None:
        self.summary_max_signals = summary_max_signals
        self._translator: DiffTranslator = translator or TemplateDiffTranslator()
        self._edges: dict[str, set[str]] = defaultdict(set)
        self._entity_signals: dict[str, list[str]] = defaultdict(list)
        self._signals_by_id: dict[str, Signal] = {}
        self._state = BaselineState()
        self._communities: list[_Community] = []
        self._dirty = True

    async def startup(self, config: SUTConfig) -> None:
        self._state.config = config
        self._state.started = True

    async def apply_ablation(self, ablation: AblationConfig) -> None:
        self._state.ablation = ablation

    async def ingest_signal(self, signal: Signal) -> None:
        self._state.signals.append(signal)
        self._signals_by_id[signal.signal_id] = signal
        ents = extract_entities(signal.content_text)
        for v in signal.metadata.values():
            if isinstance(v, str) and v not in ents:
                ents.append(v)
        for e in ents:
            self._entity_signals[e].append(signal.signal_id)
            self._edges.setdefault(e, set())
        for i, a in enumerate(ents):
            for b in ents[i + 1 :]:
                self._edges[a].add(b)
                self._edges[b].add(a)
        self._dirty = True

    def _recompute_communities(self) -> None:
        """Connected components over ``self._edges``."""

        visited: set[str] = set()
        communities: list[_Community] = []
        next_id = 0
        for node in self._edges:
            if node in visited:
                continue
            stack = [node]
            comp_entities: set[str] = set()
            while stack:
                cur = stack.pop()
                if cur in visited:
                    continue
                visited.add(cur)
                comp_entities.add(cur)
                for nb in self._edges.get(cur, ()):  # pragma: no branch
                    if nb not in visited:
                        stack.append(nb)
            comm = _Community(community_id=next_id, entities=comp_entities)
            next_id += 1
            for e in comp_entities:
                comm.signal_ids.update(self._entity_signals.get(e, []))
            recent = sorted(
                (self._signals_by_id[sid] for sid in comm.signal_ids if sid in self._signals_by_id),
                key=lambda s: s.timestamp,
            )[-self.summary_max_signals :]
            comm.summary = " | ".join(f"{s.timestamp.date()}:{s.content_text[:60]}" for s in recent)
            communities.append(comm)
        self._communities = communities
        self._dirty = False

    def _community_for(self, entity: str) -> _Community | None:
        if self._dirty:
            self._recompute_communities()
        for c in self._communities:
            if entity in c.entities:
                return c
        return None

    async def query_beliefs_at(self, query: BeliefQuery) -> list[Belief]:
        if self._dirty:
            self._recompute_communities()
        entity = query.entity_ref.id
        comm = self._community_for(entity)
        if comm is None:
            sigs = signals_mentioning(
                self._state.signals, query.entity_ref, query.timestamp
            )
        else:
            sigs = [
                self._signals_by_id[sid]
                for sid in comm.signal_ids
                if sid in self._signals_by_id
                and self._signals_by_id[sid].timestamp <= query.timestamp
            ]
        if not sigs:
            return []
        return [
            make_belief_from_signals(
                query,
                sigs,
                proposition_kind=query.proposition_kind or "community_summary",
                confidence=0.5,
                source="graphrag",
            )
        ][: max(1, query.k)]

    async def query_at_risk_at(self, timestamp: datetime) -> AtRiskReport:
        return simple_at_risk_from_signals(self._state.signals, timestamp)

    async def produce_diff_for_trigger(self, trigger: Trigger) -> DiffOp:
        if self._dirty:
            self._recompute_communities()
        anchor = trigger.payload.get("entity_ref")
        comm = self._community_for(anchor) if isinstance(anchor, str) else None
        if comm is None:
            # fallback to largest community
            comm = max(self._communities, key=lambda c: len(c.signal_ids), default=None)
        if comm is None:
            return self._translator.translate(
                trigger=trigger,
                retrieved_context="",
                evidence_signal_ids=[],
                entities=[],
            )
        context_lines = [f"community#{comm.community_id}: {comm.summary}"]
        for sid in list(comm.signal_ids)[:10]:
            sig = self._signals_by_id.get(sid)
            if sig:
                context_lines.append(sig.content_text)
        return self._translator.translate(
            trigger=trigger,
            retrieved_context="\n".join(context_lines),
            evidence_signal_ids=sorted(comm.signal_ids),
            entities=sorted(comm.entities),
        )

    async def shutdown(self) -> None:
        self._state.started = False
        self._edges.clear()
        self._entity_signals.clear()
        self._signals_by_id.clear()
        self._communities.clear()


def _factory(config: SUTConfig) -> GraphRAGBaseline:
    params = config.params or {}
    return GraphRAGBaseline(
        summary_max_signals=int(params.get("summary_max_signals", 5)),
    )


REGISTRY.register("graphrag", _factory)


__all__ = ["GraphRAGBaseline"]
