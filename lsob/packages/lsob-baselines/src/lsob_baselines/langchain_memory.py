"""LangChain-memory-style baseline.

Mimics the semantics of LangChain's
``ConversationSummaryBufferMemory`` + ``VectorStoreRetrieverMemory``:

* A short rolling buffer of the most recent signals.
* A running summary of older signals (heuristic concatenation in Phase 1;
  the real LangChain version would call an LLM).
* A vector-store-style retriever over older content.

``langchain_core`` is lazy-imported: if it is available (the ``heavy``
optional-dependency group), the class will log that fact; otherwise it
falls back to the in-memory implementation. Tests never require
``langchain_core``.
"""

from __future__ import annotations

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
from .common import cosine_similarity, hash_embedding, top_k
from .diff_translator import DiffTranslator, TemplateDiffTranslator
from .registry import REGISTRY


def _langchain_available() -> bool:
    try:  # pragma: no cover - optional dep
        import langchain_core  # noqa: F401

        return True
    except Exception:  # pragma: no cover
        return False


@dataclass
class _MemoryEntry:
    signal_id: str
    text: str
    embedding: Any
    timestamp: datetime
    metadata: dict[str, Any] = field(default_factory=dict)


class LangChainMemoryBaseline:
    """Buffer + summary + vector-retrieval memory adapter."""

    name = "langchain-memory"
    max_concurrent_ingestion = 8

    def __init__(
        self,
        *,
        buffer_max: int = 20,
        summary_max_chars: int = 2000,
        embedding_dim: int = 128,
        retrieval_k: int = 5,
        translator: DiffTranslator | None = None,
    ) -> None:
        self.buffer_max = buffer_max
        self.summary_max_chars = summary_max_chars
        self.embedding_dim = embedding_dim
        self.retrieval_k = retrieval_k
        self._translator: DiffTranslator = translator or TemplateDiffTranslator()
        self._buffer: list[_MemoryEntry] = []
        self._archive: list[_MemoryEntry] = []
        self._summary: str = ""
        self._state = BaselineState()
        self._langchain = _langchain_available()

    async def startup(self, config: SUTConfig) -> None:
        self._state.config = config
        self._state.started = True

    async def apply_ablation(self, ablation: AblationConfig) -> None:
        self._state.ablation = ablation

    async def ingest_signal(self, signal: Signal) -> None:
        self._state.signals.append(signal)
        entry = _MemoryEntry(
            signal_id=signal.signal_id,
            text=signal.content_text,
            embedding=hash_embedding(signal.content_text, dim=self.embedding_dim),
            timestamp=signal.timestamp,
            metadata=dict(signal.metadata),
        )
        self._buffer.append(entry)
        while len(self._buffer) > self.buffer_max:
            evicted = self._buffer.pop(0)
            self._archive.append(evicted)
            self._extend_summary(evicted.text)

    def _extend_summary(self, text: str) -> None:
        addition = text.strip().replace("\n", " ")
        if not addition:
            return
        if self._summary:
            self._summary = f"{self._summary} | {addition}"
        else:
            self._summary = addition
        if len(self._summary) > self.summary_max_chars:
            # Drop the oldest half — analogous to LangChain's summary prune.
            self._summary = self._summary[-self.summary_max_chars :]

    def _retrieve(self, probe: str, before: datetime, k: int) -> list[_MemoryEntry]:
        q = hash_embedding(probe, dim=self.embedding_dim)
        scored: list[tuple[float, _MemoryEntry]] = []
        for e in (*self._buffer, *self._archive):
            if e.timestamp > before:
                continue
            scored.append((cosine_similarity(q, e.embedding), e))
        return [e for _, e in top_k(scored, k)]  # type: ignore[misc]

    async def query_beliefs_at(self, query: BeliefQuery) -> list[Belief]:
        probe = f"{query.entity_ref.kind} {query.entity_ref.id}"
        hits = self._retrieve(probe, query.timestamp, self.retrieval_k)
        matched_ids = {h.signal_id for h in hits}
        signals = [s for s in self._state.signals if s.signal_id in matched_ids]
        if not signals:
            signals = signals_mentioning(
                self._state.signals, query.entity_ref, query.timestamp
            )
        if not signals:
            return []
        return [
            make_belief_from_signals(
                query,
                signals,
                proposition_kind=query.proposition_kind or "buffer_summary",
                confidence=0.5,
                source="langchain-memory",
            )
        ][: max(1, query.k)]

    async def query_at_risk_at(self, timestamp: datetime) -> AtRiskReport:
        return simple_at_risk_from_signals(self._state.signals, timestamp)

    async def produce_diff_for_trigger(self, trigger: Trigger) -> DiffOp:
        probe = f"{trigger.kind} {trigger.payload}"
        hits = self._retrieve(probe, trigger.timestamp, self.retrieval_k)
        context_parts = [self._summary] if self._summary else []
        context_parts.extend(h.text for h in hits)
        context = "\n".join(context_parts)
        return self._translator.translate(
            trigger=trigger,
            retrieved_context=context,
            evidence_signal_ids=[h.signal_id for h in hits],
            entities=[],
        )

    async def shutdown(self) -> None:
        self._state.started = False
        self._buffer.clear()
        self._archive.clear()
        self._summary = ""


def _factory(config: SUTConfig) -> LangChainMemoryBaseline:
    params = config.params or {}
    return LangChainMemoryBaseline(
        buffer_max=int(params.get("buffer_max", 20)),
        summary_max_chars=int(params.get("summary_max_chars", 2000)),
        embedding_dim=int(params.get("embedding_dim", 128)),
        retrieval_k=int(params.get("retrieval_k", 5)),
    )


REGISTRY.register("langchain-memory", _factory)


__all__ = ["LangChainMemoryBaseline"]
