"""Vanilla RAG baseline.

Production configuration would flatten signals into documents, chunk them
(~512 tokens), embed with ``nomic-embed-text`` via Ollama, store in
Postgres+pgvector, retrieve top-k on queries, and generate diffs via an
LLM-mediated translator.

Phase 1 implementation uses an in-memory cosine-similarity store over
``hash_embedding`` (see :mod:`lsob_baselines.common`) so that tests are
fully hermetic. The production knobs are still documented in the YAML
config and exposed on the class constructor.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np

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
from .common import chunk_text, cosine_similarity, hash_embedding, top_k
from .diff_translator import DiffTranslator, TemplateDiffTranslator
from .registry import REGISTRY


@dataclass
class _Chunk:
    chunk_id: str
    signal_id: str
    text: str
    embedding: np.ndarray
    timestamp: datetime
    metadata: dict[str, Any] = field(default_factory=dict)


class VanillaRAGBaseline:
    """In-memory RAG over sentence-split chunks with hash-based embeddings.

    Production knobs (documented, not yet wired in Phase 1):

    * ``embedding_model``: ``nomic-embed-text`` via Ollama
    * ``vector_store``: pgvector
    * ``chunk_size_tokens``: 512 (we approximate with ``chunk_size_chars``)
    * ``retrieval_k``: number of chunks to retrieve per query
    """

    name = "vanilla-rag"
    max_concurrent_ingestion = 16

    def __init__(
        self,
        *,
        embedding_dim: int = 128,
        chunk_size_chars: int = 512,
        retrieval_k: int = 6,
        translator: DiffTranslator | None = None,
    ) -> None:
        self.embedding_dim = embedding_dim
        self.chunk_size_chars = chunk_size_chars
        self.retrieval_k = retrieval_k
        self._translator: DiffTranslator = translator or TemplateDiffTranslator()
        self._chunks: list[_Chunk] = []
        self._state = BaselineState()

    async def startup(self, config: SUTConfig) -> None:
        self._state.config = config
        self._state.started = True
        # Allow overriding knobs via SUTConfig.params without re-instantiating.
        params = config.params or {}
        self.embedding_dim = int(params.get("embedding_dim", self.embedding_dim))
        self.chunk_size_chars = int(params.get("chunk_size_chars", self.chunk_size_chars))
        self.retrieval_k = int(params.get("retrieval_k", self.retrieval_k))

    async def apply_ablation(self, ablation: AblationConfig) -> None:
        self._state.ablation = ablation

    async def ingest_signal(self, signal: Signal) -> None:
        self._state.signals.append(signal)
        for idx, chunk in enumerate(chunk_text(signal.content_text, self.chunk_size_chars) or [signal.content_text]):
            emb = hash_embedding(chunk, dim=self.embedding_dim)
            self._chunks.append(
                _Chunk(
                    chunk_id=f"{signal.signal_id}:{idx}",
                    signal_id=signal.signal_id,
                    text=chunk,
                    embedding=emb,
                    timestamp=signal.timestamp,
                    metadata=dict(signal.metadata),
                )
            )

    def _retrieve(self, query_text: str, before: datetime, k: int) -> list[_Chunk]:
        if not self._chunks:
            return []
        q = hash_embedding(query_text, dim=self.embedding_dim)
        scored: list[tuple[float, _Chunk]] = []
        for c in self._chunks:
            if c.timestamp > before:
                continue
            scored.append((cosine_similarity(q, c.embedding), c))
        top = top_k(scored, k)
        return [c for _, c in top]  # type: ignore[misc]

    async def query_beliefs_at(self, query: BeliefQuery) -> list[Belief]:
        # Retrieve against entity id; fallback to signals_mentioning if sparse.
        probe = f"{query.entity_ref.kind} {query.entity_ref.id}"
        hits = self._retrieve(probe, query.timestamp, self.retrieval_k)
        matched_ids = {h.signal_id for h in hits}
        matched_signals = [s for s in self._state.signals if s.signal_id in matched_ids]
        if not matched_signals:
            matched_signals = signals_mentioning(
                self._state.signals, query.entity_ref, query.timestamp
            )
        if not matched_signals:
            return []
        return [
            make_belief_from_signals(
                query,
                matched_signals,
                proposition_kind=query.proposition_kind or "rag_summary",
                confidence=0.45,
                source="vanilla-rag",
            )
        ][: max(1, query.k)]

    async def query_at_risk_at(self, timestamp: datetime) -> AtRiskReport:
        return simple_at_risk_from_signals(self._state.signals, timestamp)

    async def produce_diff_for_trigger(self, trigger: Trigger) -> DiffOp:
        probe = f"{trigger.kind} {trigger.payload}"
        hits = self._retrieve(probe, trigger.timestamp, self.retrieval_k)
        context = "\n".join(h.text for h in hits)
        return self._translator.translate(
            trigger=trigger,
            retrieved_context=context,
            evidence_signal_ids=[h.signal_id for h in hits],
            entities=[],
        )

    async def shutdown(self) -> None:
        self._state.started = False
        self._chunks.clear()


def _factory(config: SUTConfig) -> VanillaRAGBaseline:
    params = config.params or {}
    return VanillaRAGBaseline(
        embedding_dim=int(params.get("embedding_dim", 128)),
        chunk_size_chars=int(params.get("chunk_size_chars", 512)),
        retrieval_k=int(params.get("retrieval_k", 6)),
    )


REGISTRY.register("vanilla-rag", _factory)


__all__ = ["VanillaRAGBaseline"]
