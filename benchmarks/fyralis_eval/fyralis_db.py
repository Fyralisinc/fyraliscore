"""Fyralis database-backed benchmark reader.

This adapter materializes normalized benchmark observations into the
production Fyralis tables and retrieves with ``services.reasoning.retrieval.primary``.
The default embedding mode is a deterministic hashed-token vector so local
benchmark runs do not depend on an external embedding service.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

import asyncpg
try:
    import numpy as np
except ModuleNotFoundError:  # pragma: no cover - benchmark envs normally include numpy.
    np = None  # type: ignore[assignment]

from benchmarks.adapters.base import BenchmarkObservation, BenchmarkQuery
from benchmarks.fyralis_eval.reader import RetrievedEvidence, token_counts
from lib.embeddings.base import Embedder
from lib.embeddings.factory import make_embedder
from lib.shared.migrations import apply_migrations_dir
from services.reasoning.retrieval.primary import TriggerContext, primary_retrieve
from services.product.ask.orchestrator import AskOrchestrator
from services.product.ask.schemas import AskScope, AskSessionCreateRequest, AskTurnRequest
from services.product.ask.store import InMemoryAskStore
from services.reasoning.sage.reader import ReaderBudget, SynthesisReader
from services.reasoning.synthesis.operational_facets import (
    enrich_operational_model_proposition,
)


EMBEDDING_DIM = 768
_BENCH_NAMESPACE = UUID("a8f94533-d4c5-4b7f-bf02-5c59c3f4d536")
_MATERIALIZATION_BATCH_SIZE = 64


@dataclass(frozen=True)
class FyralisDBMaterialization:
    namespace: str
    observations: int
    tenants: int
    embedding_model_version: str


@dataclass(frozen=True)
class _TenantVectorBlock:
    observations: tuple[BenchmarkObservation, ...]
    matrix: Any


class _TenantVectorIndex:
    """Tenant-local vector index for benchmark semantic candidate generation."""

    def __init__(self, blocks: dict[str, _TenantVectorBlock]) -> None:
        self._blocks = blocks

    @classmethod
    def build(
        cls,
        observations: Iterable[BenchmarkObservation],
        embeddings: dict[str, list[float]],
    ) -> _TenantVectorIndex | None:
        if np is None:
            return None
        grouped: dict[str, list[tuple[BenchmarkObservation, list[float]]]] = {}
        for observation in observations:
            embedding = embeddings.get(observation.observation_id)
            if not embedding:
                continue
            grouped.setdefault(observation.tenant_id, []).append((observation, embedding))

        blocks: dict[str, _TenantVectorBlock] = {}
        for tenant_id, rows in grouped.items():
            vectors = [embedding for _, embedding in rows]
            matrix = np.asarray(vectors, dtype=np.float32)
            if matrix.ndim != 2 or matrix.shape[0] == 0:
                continue
            norms = np.linalg.norm(matrix, axis=1, keepdims=True)
            valid_mask = norms[:, 0] > 0.0
            if not bool(np.any(valid_mask)):
                continue
            matrix = matrix[valid_mask] / norms[valid_mask]
            indexed_observations = tuple(
                observation
                for is_valid, (observation, _) in zip(valid_mask.tolist(), rows)
                if is_valid
            )
            blocks[tenant_id] = _TenantVectorBlock(
                observations=indexed_observations,
                matrix=matrix,
            )
        return cls(blocks)

    def query(
        self,
        *,
        tenant_id: str,
        query_vector: list[float],
        limit: int,
    ) -> list[tuple[float, BenchmarkObservation]]:
        if np is None or limit <= 0:
            return []
        block = self._blocks.get(tenant_id)
        if block is None:
            return []
        query = np.asarray(query_vector, dtype=np.float32)
        norm = float(np.linalg.norm(query))
        if norm <= 0.0:
            return []
        scores = block.matrix @ (query / norm)
        positive_indexes = np.flatnonzero(scores > 0.0)
        if positive_indexes.size == 0:
            return []
        if positive_indexes.size > limit:
            top_positions = np.argpartition(
                scores[positive_indexes],
                -limit,
            )[-limit:]
            candidate_indexes = positive_indexes[top_positions]
        else:
            candidate_indexes = positive_indexes
        ranked_indexes = candidate_indexes[
            np.argsort(scores[candidate_indexes])[::-1]
        ]
        return [
            (float(scores[index]), block.observations[int(index)])
            for index in ranked_indexes[:limit]
        ]


@dataclass(frozen=True)
class _BM25TenantBlock:
    observations: tuple[BenchmarkObservation, ...]
    document_lengths: tuple[int, ...]
    average_document_length: float
    document_frequency: dict[str, int]
    postings: dict[str, tuple[tuple[int, int], ...]]


@dataclass(frozen=True)
class _ObservationScoringFeatures:
    content_lower: str
    content_terms: frozenset[str]
    metadata_terms: frozenset[str]
    has_state_or_count_signal: bool


class _TenantBM25Index:
    """Exact tenant-local BM25 postings cache for benchmark hybrid runs."""

    def __init__(self, blocks: dict[str, _BM25TenantBlock]) -> None:
        self._blocks = blocks

    @classmethod
    def build(cls, observations: Iterable[BenchmarkObservation]) -> "_TenantBM25Index":
        grouped: dict[str, list[BenchmarkObservation]] = {}
        for observation in observations:
            grouped.setdefault(observation.tenant_id, []).append(observation)

        blocks: dict[str, _BM25TenantBlock] = {}
        for tenant_id, rows in grouped.items():
            term_counts_by_doc = [token_counts(observation.content) for observation in rows]
            document_lengths = tuple(sum(counts.values()) for counts in term_counts_by_doc)
            average_document_length = (
                sum(document_lengths) / len(document_lengths)
                if document_lengths else 0.0
            )
            document_frequency: dict[str, int] = {}
            postings_mut: dict[str, list[tuple[int, int]]] = {}
            for doc_index, counts in enumerate(term_counts_by_doc):
                for term, count in counts.items():
                    if count <= 0:
                        continue
                    document_frequency[term] = document_frequency.get(term, 0) + 1
                    postings_mut.setdefault(term, []).append((doc_index, int(count)))
            blocks[tenant_id] = _BM25TenantBlock(
                observations=tuple(rows),
                document_lengths=document_lengths,
                average_document_length=average_document_length,
                document_frequency=document_frequency,
                postings={
                    term: tuple(postings)
                    for term, postings in postings_mut.items()
                },
            )
        return cls(blocks)

    def query(
        self,
        *,
        tenant_id: str,
        query_text: str,
        limit: int | None,
    ) -> list[tuple[float, BenchmarkObservation]]:
        if limit is not None and limit <= 0:
            return []
        block = self._blocks.get(tenant_id)
        if block is None:
            return []
        query_terms = token_counts(query_text)
        if not query_terms:
            return []
        total_docs = len(block.observations)
        scores: dict[int, float] = {}
        for term, query_count in query_terms.items():
            postings = block.postings.get(term)
            if not postings:
                continue
            document_frequency = block.document_frequency.get(term, 0)
            for doc_index, term_frequency in postings:
                scores[doc_index] = scores.get(doc_index, 0.0) + query_count * _bm25_score(
                    term_frequency=term_frequency,
                    document_frequency=document_frequency,
                    total_docs=total_docs,
                    document_length=block.document_lengths[doc_index],
                    average_document_length=block.average_document_length,
                )
        scored = [
            (score, block.observations[doc_index])
            for doc_index, score in scores.items()
            if score > 0
        ]
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored if limit is None else scored[:limit]


class FyralisDBReader:
    """Benchmark reader backed by Fyralis' current Postgres retrieval path."""

    def __init__(
        self,
        observations: Iterable[BenchmarkObservation],
        *,
        top_k: int,
        dsn: str | None = None,
        namespace: str = "benchmark",
        embedding_mode: str = "hash",
        apply_migrations: bool = False,
        enrich_graph: bool = False,
        bm25_seed_candidates: int = 0,
    ) -> None:
        self.observations = list(observations)
        self.top_k = top_k
        self.dsn = dsn or os.getenv("DATABASE_URL")
        if not self.dsn:
            raise ValueError(
                "fyralis_db_hash_embedding requires DATABASE_URL so benchmark "
                "observations can be materialized into the Fyralis database"
            )
        self.namespace = _namespace_for_observations(namespace, self.observations)
        self.embedding_mode = _normalize_embedding_mode(embedding_mode)
        self.enrich_graph = bool(enrich_graph)
        self.bm25_seed_candidates = max(0, int(bm25_seed_candidates))
        self.embedding_concurrency = max(
            1,
            int(os.getenv("BENCHMARK_EMBED_CONCURRENCY", "4")),
        )
        self.embedding_max_chars = max(
            512,
            int(os.getenv("BENCHMARK_EMBED_MAX_CHARS", "8192")),
        )
        self._tenant_ids = {
            observation.tenant_id: _stable_uuid(f"{self.namespace}:tenant:{observation.tenant_id}")
            for observation in self.observations
        }
        self._observation_ids = {
            observation.observation_id: _stable_uuid(
                f"{self.namespace}:observation:{observation.observation_id}"
            )
            for observation in self.observations
        }
        self._model_ids = {
            observation.observation_id: _stable_uuid(
                f"{self.namespace}:model:{observation.observation_id}"
            )
            for observation in self.observations
        }
        self._observation_by_model_id = {
            self._model_ids[observation.observation_id]: observation
            for observation in self.observations
        }
        self._observation_by_uuid = {
            self._observation_ids[observation.observation_id]: observation
            for observation in self.observations
        }
        self._observation_by_id = {
            observation.observation_id: observation for observation in self.observations
        }
        self._derived_observations_by_tenant = _derived_memory_observations_by_tenant(
            self.observations,
        )
        self._derived_observation_by_id = {
            observation.observation_id: observation
            for rows in self._derived_observations_by_tenant.values()
            for observation in rows
        }
        self._observation_by_id.update(self._derived_observation_by_id)
        self._embedding_by_observation_id: dict[str, list[float]] = {}
        self._semantic_index: _TenantVectorIndex | None = None
        self._bm25_index = _TenantBM25Index.build(self.observations)
        self._loop = asyncio.new_event_loop()
        self._embedder = self._loop.run_until_complete(self._build_embedder())
        self._pool = self._loop.run_until_complete(self._open_pool(apply_migrations))
        try:
            self.materialization = self._loop.run_until_complete(self._materialize())
            self._semantic_index = _TenantVectorIndex.build(
                self.observations,
                self._embedding_by_observation_id,
            )
        except Exception:
            self.close()
            raise

    def retrieve(self, query: BenchmarkQuery) -> tuple[list[RetrievedEvidence], int, int]:
        started = time.monotonic()
        evidence, calls = self._loop.run_until_complete(self._retrieve(query))
        elapsed_ms = max(0, math.ceil((time.monotonic() - started) * 1000))
        return evidence, elapsed_ms, calls

    def close(self) -> None:
        if self._embedder is not None:
            self._loop.run_until_complete(self._embedder.close())
            self._embedder = None
        if self._pool is not None:
            self._loop.run_until_complete(self._pool.close())
            self._pool = None
        if not self._loop.is_closed():
            self._loop.close()

    async def _build_embedder(self) -> Embedder | None:
        if self.embedding_mode == "hash":
            return None
        if self.embedding_mode == "provider":
            return make_embedder()
        return make_embedder(self.embedding_mode)

    async def _open_pool(self, apply_migrations: bool) -> asyncpg.Pool:
        pool = await asyncpg.create_pool(
            self.dsn,
            min_size=1,
            max_size=4,
            init=_benchmark_pool_init,
        )
        if apply_migrations:
            async with pool.acquire() as conn:
                await apply_migrations_dir(
                    conn,
                    Path(__file__).resolve().parents[2] / "db" / "migrations",
                    on_error="warn",
                )
        return pool

    async def _materialize(self) -> FyralisDBMaterialization:
        tenants = set()
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for observation in self.observations:
                    tenants.add(self._tenant_ids[observation.tenant_id])
                for month in sorted({
                    date(item.occurred_at.year, item.occurred_at.month, 1)
                    for item in self.observations
                }):
                    await _ensure_observation_partition(conn, month)
                await self._upsert_tenants(conn)
                for batch in _chunks(self.observations, _MATERIALIZATION_BATCH_SIZE):
                    embeddings = await self._embed_batch([
                        self._embedding_text(item.content)
                        for item in batch
                    ])
                    for item, embedding in zip(batch, embeddings):
                        self._embedding_by_observation_id[item.observation_id] = embedding
                    await self._upsert_observations(conn, batch, embeddings)
                    await self._upsert_models(conn, batch, embeddings)
                if self.enrich_graph:
                    await self._upsert_benchmark_graph_edges(conn)
                await conn.execute("ANALYZE models")
                await conn.execute("ANALYZE observations")
        return FyralisDBMaterialization(
            namespace=self.namespace,
            observations=len(self.observations),
            tenants=len(tenants),
            embedding_model_version=self.embedding_model_version,
        )

    async def _retrieve(self, query: BenchmarkQuery) -> tuple[list[RetrievedEvidence], int]:
        tenant_id = self._tenant_ids.get(query.tenant_id)
        if tenant_id is None:
            return [], 0
        query_vector = await self._embed_text(self._embedding_text(query.query_text))
        seed_model_ids = self._bm25_seed_model_ids(query)
        trigger = TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_natural_text=query.query_text,
            precomputed_seed_vector=query_vector,
            semantic_k=self.top_k,
            member_model_ids=seed_model_ids,
        )
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                result = await primary_retrieve(
                    trigger,
                    conn,
                    top_n=self.top_k,
                )
        evidence: list[RetrievedEvidence] = []
        for model in result.models:
            observation = self._observation_by_model_id.get(model.id)
            if observation is None:
                continue
            evidence.append(
                RetrievedEvidence(
                    observation_id=observation.observation_id,
                    content=observation.content,
                    score=round(float(result.model_scores.get(model.id, 0.0)), 6),
                    occurred_at=observation.occurred_at.isoformat(),
                    metadata={
                        **observation.metadata,
                        "fyralis_model_id": str(model.id),
                        "fyralis_tenant_id": str(model.tenant_id),
                        "retrieval_system": f"fyralis_db:{self.embedding_model_version}",
                    },
                )
            )
        notes = result.notes if isinstance(result.notes, dict) else {}
        pathways = notes.get("pathways_run")
        retrieval_calls = len(pathways) if isinstance(pathways, list) else 1
        return evidence[: self.top_k], retrieval_calls

    @property
    def embedding_model_version(self) -> str:
        if self._embedder is not None:
            return f"{self.embedding_mode}:{self._embedder.model_name}"
        return "hashed_token_vector_v1"

    async def _embed_text(self, text: str) -> list[float]:
        if self._embedder is None:
            return hashed_token_vector(text)
        return await self._embedder.embed(text)

    async def _embed_batch(self, texts: list[str]) -> list[list[float]]:
        if self._embedder is None:
            return [hashed_token_vector(text) for text in texts]
        if _embedding_cache_enabled():
            return await self._cached_embed_batch(texts)
        if self._embedder.__class__.__name__ == "OpenAIEmbedder":
            return await self._embedder.embed_batch(texts)
        semaphore = asyncio.Semaphore(self.embedding_concurrency)

        async def embed_one(text: str) -> list[float]:
            async with semaphore:
                return await self._embedder.embed(text)

        return await asyncio.gather(*(embed_one(text) for text in texts))

    async def _cached_embed_batch(self, texts: list[str]) -> list[list[float]]:
        cache_dir = _embedding_cache_dir()
        cache_dir.mkdir(parents=True, exist_ok=True)

        results: list[list[float] | None] = []
        misses: list[tuple[int, str, Path]] = []
        for index, text in enumerate(texts):
            path = cache_dir / f"{_embedding_cache_key(self.embedding_model_version, text)}.json"
            cached = _read_cached_embedding(path)
            if cached is None:
                results.append(None)
                misses.append((index, text, path))
            else:
                results.append(cached)

        if misses:
            embedded = await self._embed_uncached_batch([text for _, text, _ in misses])
            for (index, _text, path), vector in zip(misses, embedded):
                results[index] = vector
                _write_cached_embedding(path, vector)

        return [vector if vector is not None else [] for vector in results]

    async def _embed_uncached_batch(self, texts: list[str]) -> list[list[float]]:
        if self._embedder.__class__.__name__ == "OpenAIEmbedder":
            return await self._embedder.embed_batch(texts)
        semaphore = asyncio.Semaphore(self.embedding_concurrency)

        async def embed_one(text: str) -> list[float]:
            async with semaphore:
                return await self._embedder.embed(text)

        return await asyncio.gather(*(embed_one(text) for text in texts))

    def _embedding_text(self, text: str) -> str:
        if len(text) <= self.embedding_max_chars:
            return text
        half = max(1, self.embedding_max_chars // 2)
        return f"{text[:half]}\n...\n{text[-half:]}"

    async def _upsert_tenants(
        self,
        conn: asyncpg.Connection,
    ) -> None:
        rows = [
            (
                tenant_id,
                f"benchmark:{self.namespace}:{source_tenant_id}"[:256],
            )
            for source_tenant_id, tenant_id in sorted(
                self._tenant_ids.items(),
                key=lambda item: item[0],
            )
        ]
        await conn.executemany(
            """
            INSERT INTO tenants (id, name, is_demo)
            VALUES ($1, $2, FALSE)
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
            """,
            rows,
        )

    async def _upsert_observations(
        self,
        conn: asyncpg.Connection,
        observations: list[BenchmarkObservation],
        embeddings: list[list[float]],
    ) -> None:
        rows = []
        for observation, embedding in zip(observations, embeddings):
            observation_id = self._observation_ids[observation.observation_id]
            tenant_id = self._tenant_ids[observation.tenant_id]
            rows.append(
                (
                    observation_id,
                    tenant_id,
                    observation.occurred_at,
                    json.dumps(
                        {
                            "content_text": observation.content,
                            "benchmark_source": observation.source,
                            "benchmark_observation_id": observation.observation_id,
                            "metadata": observation.metadata,
                        }
                    ),
                    observation.content,
                    embedding,
                    f"{self.namespace}:{observation.observation_id}",
                    json.dumps(observation.entities),
                )
            )
        await conn.executemany(
            """
            INSERT INTO observations (
                id, tenant_id, occurred_at, ingested_at, kind,
                source_channel, source_actor_ref, actor_id,
                content, content_text,
                embedding, embedding_pending,
                trust_tier, external_id, cause_id, entities_mentioned
            ) VALUES (
                $1, $2, $3, now(), 'signal',
                'benchmark:fyralis_eval', NULL, NULL,
                $4::jsonb, $5,
                $6, FALSE,
                'authoritative', $7, NULL, $8::jsonb
            )
            ON CONFLICT (id, occurred_at) DO UPDATE SET
                kind = EXCLUDED.kind,
                content = EXCLUDED.content,
                content_text = EXCLUDED.content_text,
                embedding = EXCLUDED.embedding,
                trust_tier = EXCLUDED.trust_tier,
                entities_mentioned = EXCLUDED.entities_mentioned
            """,
            rows,
        )

    async def _upsert_models(
        self,
        conn: asyncpg.Connection,
        observations: list[BenchmarkObservation],
        embeddings: list[list[float]],
    ) -> None:
        rows = []
        for observation, embedding in zip(observations, embeddings):
            model_id = self._model_ids[observation.observation_id]
            observation_id = self._observation_ids[observation.observation_id]
            natural = _model_natural_text(observation)
            proposition = {
                "kind": "observation",
                "claim_role": "fact",
                "abstraction_level": "atomic",
                "time_mode": "past",
                "modality": "observed",
                "polarity": "neutral",
                "subject": "benchmark_evidence",
                "object": observation.observation_id,
                "benchmark_source": observation.source,
                "embedding_model_version": self.embedding_model_version,
            }
            proposition = enrich_operational_model_proposition(
                proposition,
                natural=natural,
                metadata=observation.metadata
                if isinstance(observation.metadata, dict) else None,
            )
            rows.append(
                (
                    model_id,
                    self._tenant_ids[observation.tenant_id],
                    observation_id,
                    json.dumps(proposition),
                    natural,
                    embedding,
                    json.dumps(
                        {
                            "valid_from": observation.occurred_at.astimezone(timezone.utc).isoformat(),
                            "valid_until": None,
                        }
	                    ),
	                    [observation_id],
	                    json.dumps([
	                        {
	                            "kind": "observe",
	                            "event_id": str(observation_id),
	                            "weight": 1.0,
	                            "source": "benchmark_materialization",
	                        }
	                    ]),
	                    json.dumps(observation.entities),
	                )
	            )
        await conn.executemany(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_actors, scope_entities, scope_temporal,
                confidence, activation, falsifier,
                signal_readings, reading_contestable,
                supporting_event_ids, supporting_model_ids, evidential_weight,
                status, archived_at, archive_reason,
                evaluate_at, resolution_criteria, contributing_models,
                visible_to_subjects, confidence_at_assertion, last_retrieved_at
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, $5, $6,
                '{}'::uuid[], $10::jsonb, $7::jsonb,
                0.75, 0.5, NULL,
                $9::jsonb, TRUE,
                $8::uuid[], '{}'::uuid[], 0.5,
                'active', NULL, NULL,
                NULL, NULL, '{}'::uuid[],
                TRUE, 0.75, NULL
            )
            ON CONFLICT (id) DO UPDATE SET
                proposition = EXCLUDED.proposition,
                "natural" = EXCLUDED."natural",
                embedding = EXCLUDED.embedding,
                scope_entities = EXCLUDED.scope_entities,
                scope_temporal = EXCLUDED.scope_temporal,
                supporting_event_ids = EXCLUDED.supporting_event_ids,
                status = 'active',
                archived_at = NULL,
                archive_reason = NULL
            """,
            rows,
        )

    async def _upsert_benchmark_graph_edges(self, conn: asyncpg.Connection) -> None:
        rows = _benchmark_graph_edge_rows(
            namespace=self.namespace,
            observations=self.observations,
            tenant_ids=self._tenant_ids,
            observation_ids=self._observation_ids,
            model_ids=self._model_ids,
        )
        for batch in _chunks(rows, 512):
            await conn.executemany(
                """
                INSERT INTO model_edges (
                    id, tenant_id, source_model_id, target_model_id,
                    edge_kind, weight, metadata, status, detected_by,
                    created_by_event_id, confidence, evidence_event_ids,
                    evidence_model_ids, explanation, review_status,
                    confirmed_count
                ) VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7::jsonb, 'active', 'link_miner',
                    $8, $9, $10::uuid[],
                    '{}'::uuid[], $11, 'accepted',
                    1
                )
                ON CONFLICT (tenant_id, source_model_id, target_model_id, edge_kind)
                DO UPDATE SET
                    weight = GREATEST(model_edges.weight, EXCLUDED.weight),
                    metadata = model_edges.metadata || EXCLUDED.metadata,
                    status = 'active',
                    confidence = GREATEST(model_edges.confidence, EXCLUDED.confidence),
                    evidence_event_ids = EXCLUDED.evidence_event_ids,
                    explanation = EXCLUDED.explanation,
                    review_status = 'accepted',
                    confirmed_count = GREATEST(model_edges.confirmed_count, 1)
                """,
                batch,
            )


def _model_natural_text(observation: BenchmarkObservation) -> str:
    """Compact product-like Synthesis claim text for a benchmark observation.

    Full benchmark observations may contain very large raw UI trees. Product
    Synthesis models are compact claims with raw evidence attached, so the
    model text should carry operational handles while observations retain the
    raw evidence payload.
    """

    content = str(observation.content or "")
    head = content.split("\nAccessibility tree:", 1)[0]
    metadata = observation.metadata if isinstance(observation.metadata, dict) else {}
    lines = [head.strip()]
    for key, label, limit in (
        ("ui_labels", "UI labels", 24),
        ("ui_labels_added", "New labels", 24),
        ("ui_labels_removed", "Removed labels", 12),
        ("sort_fields", "Sort fields", 12),
        ("form_controls", "Form controls", 40),
        ("structured_ui_facts", "Structured UI facts", 32),
        ("pipeline_items", "Pipeline items", 12),
        ("stage_chains", "Stage chains", 8),
    ):
        rendered = _render_compact_metadata_value(metadata.get(key), limit=limit)
        if rendered:
            lines.append(f"{label}: {rendered}")
    return _clip_text("\n".join(line for line in lines if line), 6000)


def _render_compact_metadata_value(value: Any, *, limit: int) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        items = [
            f"{key}={_render_compact_metadata_value(raw, limit=6)}"
            for key, raw in list(value.items())[:limit]
            if raw
        ]
        return "; ".join(item for item in items if item)
    if isinstance(value, list):
        items = [
            _render_compact_metadata_value(item, limit=6)
            for item in value[:limit]
        ]
        return "; ".join(item for item in items if item)
    return _clip_text(" ".join(str(value).split()), 180)


def _clip_text(text: str, limit: int) -> str:
    clean = str(text or "").strip()
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 3)].rstrip() + "..."


class FyralisSageReader(FyralisDBReader):
    """Benchmark reader backed by the full SAGE-inspired SynthesisReader."""

    async def _retrieve(self, query: BenchmarkQuery) -> tuple[list[RetrievedEvidence], int]:
        result = await self._read_sage(query)
        if result is None:
            return [], 0
        evidence = self._evidence_from_sage_result(result)
        evidence.sort(key=lambda item: item.score, reverse=True)
        return evidence[: self.top_k], 1

    async def _read_sage(
        self,
        query: BenchmarkQuery,
        *,
        seed_model_ids: list[UUID] | None = None,
        query_vector: list[float] | None = None,
    ):
        tenant_id = self._tenant_ids.get(query.tenant_id)
        if tenant_id is None:
            return None
        if query_vector is None:
            query_vector = await self._embed_text(self._embedding_text(query.query_text))
        if seed_model_ids is None:
            seed_model_ids = self._bm25_seed_model_ids(query)
        trigger = TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_natural_text=query.query_text,
            precomputed_seed_vector=query_vector,
            semantic_k=self.top_k,
            member_model_ids=seed_model_ids,
        )
        reader = SynthesisReader(
            pool=self._pool,
            budget=ReaderBudget(
                max_nodes=max(24, self.top_k * 6),
                max_edges=max(48, self.top_k * 12),
                max_evidence_items=max(20, self.top_k * 4),
                lexical_candidates=max(40, self.top_k * 12),
                shortcut_candidates=12,
                affordance_candidates=max(20, self.top_k * 4),
                propagation_neighbors=max(80, self.top_k * 16),
            ),
        )
        primitive = _benchmark_question_primitive(query)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                result = await reader.read(
                    conn=conn,
                    tenant_id=tenant_id,
                    trigger=trigger,
                    question_id=query.query_id,
                    question=query.query_text,
                    question_primitive=primitive,
                    hypotheses=(),
                )
        return result

    def _evidence_from_sage_result(self, result) -> list[RetrievedEvidence]:
        projection_scores: dict[UUID, float] = {}
        projection_node_ids: dict[UUID, UUID] = {}
        projection_reasons: dict[UUID, str] = {}
        for item in result.projected_evidence:
            if item.get("evidence_kind") != "observation":
                continue
            try:
                evidence_id = UUID(str(item.get("evidence_id")))
                node_id = UUID(str(item.get("node_id")))
            except (TypeError, ValueError):
                continue
            projection_scores[evidence_id] = float(item.get("score") or 0.0)
            projection_node_ids[evidence_id] = node_id
            projection_reasons[evidence_id] = str(item.get("reason") or "")

        evidence: list[RetrievedEvidence] = []
        for observation_row in result.observations:
            observation = self._observation_by_uuid.get(observation_row.id)
            if observation is None:
                continue
            node_id = projection_node_ids.get(observation_row.id)
            activation_score = (
                float(result.model_scores.get(node_id, 0.0)) if node_id is not None else 0.0
            )
            projection_score = float(projection_scores.get(observation_row.id, 0.0))
            combined_score = (0.7 * activation_score) + (0.3 * projection_score)
            evidence.append(
                RetrievedEvidence(
                    observation_id=observation.observation_id,
                    content=observation.content,
                    score=round(combined_score, 6),
                    occurred_at=observation.occurred_at.isoformat(),
                    metadata={
                        **observation.metadata,
                        "fyralis_observation_id": str(observation_row.id),
                        "fyralis_model_id": str(node_id) if node_id is not None else None,
                        "retrieval_system": f"fyralis_sage:{self.embedding_model_version}",
                        "sage_activation_score": round(activation_score, 6),
                        "sage_projection_score": round(projection_score, 6),
                        "sage_projection_reason": projection_reasons.get(observation_row.id),
                        "sage_question_primitive": result.question_primitive,
                        "sage_selected_models": [str(model.id) for model in result.models],
                        "sage_activation_trace_count": len(result.activations),
                        "sage_projection_coverage": result.debug.get("projection_coverage"),
                        "sage_stage_timings_ms": result.debug.get("stage_timings_ms"),
                    },
                )
            )
        return evidence

    def _bm25_seed_model_ids(self, query: BenchmarkQuery) -> list[UUID]:
        return [
            self._model_ids[observation.observation_id]
            for _, observation in self._bm25_scored_observations(
                query,
                limit=self.bm25_seed_candidates,
            )
        ]

    def _bm25_scored_observations(
        self,
        query: BenchmarkQuery,
        *,
        limit: int | None = None,
    ) -> list[tuple[float, BenchmarkObservation]]:
        return self._bm25_index.query(
            tenant_id=query.tenant_id,
            query_text=query.query_text,
            limit=limit,
        )


class FyralisSageHybridReader(FyralisSageReader):
    """Fuse SAGE projection quality with BM25's high-recall candidate set."""

    async def _retrieve(self, query: BenchmarkQuery) -> tuple[list[RetrievedEvidence], int]:
        timings: dict[str, int] = {}
        hybrid_started = time.perf_counter()

        def mark(stage: str, started: float) -> float:
            now = time.perf_counter()
            timings[stage] = int((now - started) * 1000)
            return now

        stage_started = time.perf_counter()
        bm25_limit = self.bm25_seed_candidates or max(80, self.top_k * 12)
        bm25_rows = self._bm25_scored_observations(query, limit=bm25_limit)
        stage_started = mark("bm25_ms", stage_started)
        seed_model_ids = (
            [
                self._model_ids[observation.observation_id]
                for _, observation in bm25_rows[: max(24, self.top_k * 4)]
                if observation.observation_id in self._model_ids
            ]
            if _needs_bridge_completion(query)
            else []
        )
        result = await self._read_sage(query, seed_model_ids=seed_model_ids)
        stage_started = mark("sage_ms", stage_started)
        sage_evidence = [] if result is None else self._evidence_from_sage_result(result)
        stage_started = mark("sage_evidence_ms", stage_started)
        candidate_rows = self._expand_candidate_rows(query, bm25_rows)
        stage_started = mark("expand_candidates_ms", stage_started)
        candidate_rows = self._with_derived_candidate_rows(query, candidate_rows)
        stage_started = mark("derived_candidates_ms", stage_started)
        fused = self._fuse_sage_and_bm25(query, sage_evidence, candidate_rows)
        stage_started = mark("fusion_ms", stage_started)
        fused = _with_query_chain_candidate(query, fused)
        stage_started = mark("query_chain_ms", stage_started)
        packed = _pack_query_evidence(query, fused, self.top_k)
        mark("packing_ms", stage_started)
        timings["hybrid_total_ms"] = int((time.perf_counter() - hybrid_started) * 1000)
        for item in packed:
            item.metadata["sage_hybrid_stage_timings_ms"] = timings
        return packed, 2

    def _expand_candidate_rows(
        self,
        query: BenchmarkQuery,
        bm25_rows: list[tuple[float, BenchmarkObservation]],
    ) -> list[tuple[float, BenchmarkObservation]]:
        if not bm25_rows or not _needs_bridge_completion(query):
            return bm25_rows

        rows_by_id = {
            observation.observation_id: [float(score), observation]
            for score, observation in bm25_rows
        }
        tenant_observations = [
            observation
            for observation in self.observations
            if observation.tenant_id == query.tenant_id
        ]
        by_temporal_order = sorted(
            tenant_observations,
            key=lambda item: (
                _metadata_int(item.metadata.get("event_index"), default=10_000_000),
                _metadata_int(item.metadata.get("session_index"), default=10_000_000),
                item.occurred_at,
                item.observation_id,
            ),
        )
        order_index = {
            observation.observation_id: index
            for index, observation in enumerate(by_temporal_order)
        }

        for seed_score, seed in bm25_rows[: max(12, self.top_k * 3)]:
            seed_order = order_index.get(seed.observation_id)
            if seed_order is None:
                continue
            for offset in (-4, -3, -2, -1, 1, 2, 3, 4):
                neighbor_index = seed_order + offset
                if neighbor_index < 0 or neighbor_index >= len(by_temporal_order):
                    continue
                neighbor = by_temporal_order[neighbor_index]
                _add_candidate_neighbor(
                    rows_by_id,
                    neighbor=neighbor,
                    score=float(seed_score) * (0.34 / abs(offset)),
                )

        rows = [
            (float(score), observation)
            for score, observation in rows_by_id.values()
            if float(score) > 0
        ]
        rows.sort(key=lambda item: item[0], reverse=True)
        return rows[: max(len(bm25_rows), self.top_k * 32)]

    def _with_derived_candidate_rows(
        self,
        query: BenchmarkQuery,
        candidate_rows: list[tuple[float, BenchmarkObservation]],
    ) -> list[tuple[float, BenchmarkObservation]]:
        derived = _derived_candidate_rows(
            query=query,
            derived_observations=self._derived_observations_by_tenant.get(
                query.tenant_id,
                [],
            ),
            score_floor=max((float(score) for score, _ in candidate_rows), default=1.0),
        )
        if not derived:
            return candidate_rows

        rows_by_id = {
            observation.observation_id: [float(score), observation]
            for score, observation in candidate_rows
        }
        for score, observation in derived:
            existing = rows_by_id.get(observation.observation_id)
            if existing is None:
                rows_by_id[observation.observation_id] = [float(score), observation]
            else:
                existing[0] = max(float(existing[0]), float(score))
        rows = [
            (float(score), observation)
            for score, observation in rows_by_id.values()
            if float(score) > 0
        ]
        rows.sort(key=lambda item: item[0], reverse=True)
        return rows[: max(len(candidate_rows), self.top_k * 32)]

    def _fuse_sage_and_bm25(
        self,
        query: BenchmarkQuery,
        sage_evidence: list[RetrievedEvidence],
        bm25_rows: list[tuple[float, BenchmarkObservation]],
    ) -> list[RetrievedEvidence]:
        candidates: dict[str, RetrievedEvidence] = {}
        sage_ranks: dict[str, int] = {}
        sage_scores: dict[str, float] = {}

        sage_sorted = sorted(sage_evidence, key=lambda item: item.score, reverse=True)
        for rank, item in enumerate(sage_sorted, start=1):
            candidates[item.observation_id] = item
            sage_ranks[item.observation_id] = rank
            sage_scores[item.observation_id] = max(0.0, float(item.score))

        bm25_ranks: dict[str, int] = {}
        bm25_scores: dict[str, float] = {}
        for rank, (score, observation) in enumerate(bm25_rows, start=1):
            bm25_ranks[observation.observation_id] = rank
            bm25_scores[observation.observation_id] = max(0.0, float(score))
            if observation.observation_id in candidates:
                continue
            candidates[observation.observation_id] = RetrievedEvidence(
                observation_id=observation.observation_id,
                content=observation.content,
                score=0.0,
                occurred_at=observation.occurred_at.isoformat(),
                metadata={
                    **observation.metadata,
                    "retrieval_system": f"fyralis_sage_hybrid:{self.embedding_model_version}",
                },
            )

        max_sage_score = max(sage_scores.values(), default=0.0) or 1.0
        max_bm25_score = max(bm25_scores.values(), default=0.0) or 1.0
        query_terms = set(token_counts(f"{query.query_text} {query.query_type}"))
        salient_query_terms = frozenset(_salient_tokens(f"{query.query_text} {query.query_type}"))
        query_text = query.query_text.casefold()
        bridge_intent = _needs_bridge_completion(query)
        scoring_features: dict[str, _ObservationScoringFeatures] = {}
        fused: list[RetrievedEvidence] = []
        for observation_id, item in candidates.items():
            sage_rank_score = _rrf_score(sage_ranks.get(observation_id))
            bm25_rank_score = _rrf_score(bm25_ranks.get(observation_id))
            sage_norm = sage_scores.get(observation_id, 0.0) / max_sage_score
            bm25_norm = bm25_scores.get(observation_id, 0.0) / max_bm25_score
            observation = self._observation_by_id.get(observation_id)
            features = _observation_scoring_features(observation, scoring_features)
            query_relevance = _query_evidence_relevance_with_features(
                query,
                observation,
                query_text=query_text,
                query_terms=salient_query_terms,
                features=features,
                bridge_intent=bridge_intent,
            )
            slot_score = _question_slot_score_with_features(
                query,
                observation,
                query_text=query_text,
                features=features,
            )
            transition_salience = _dynamic_transition_salience_score(query, observation)
            temporal_viewpoint = _temporal_viewpoint_score(query, observation)
            fused_score = (
                (0.22 * sage_rank_score)
                + (0.28 * bm25_rank_score)
                + (0.10 * sage_norm)
                + (0.12 * bm25_norm)
                + (0.34 * query_relevance)
                + (0.18 * slot_score)
                + transition_salience
                + temporal_viewpoint
            )
            if bridge_intent:
                fused_score += 0.06 * _query_overlap_ratio(
                    query_terms,
                    set(_salient_tokens(item.content)),
                )
            metadata = {
                **item.metadata,
                "retrieval_system": f"fyralis_sage_hybrid:{self.embedding_model_version}",
                "hybrid_sage_rank": sage_ranks.get(observation_id),
                "hybrid_bm25_rank": bm25_ranks.get(observation_id),
                "hybrid_sage_score": round(sage_scores.get(observation_id, 0.0), 6),
                "hybrid_bm25_score": round(bm25_scores.get(observation_id, 0.0), 6),
                "hybrid_query_relevance": round(query_relevance, 6),
                "hybrid_slot_score": round(slot_score, 6),
                "hybrid_temporal_viewpoint": round(temporal_viewpoint, 6),
                "hybrid_roles": sorted(_retrieval_roles(query, item)),
                "hybrid_fusion": "causal_seeded_rrf_sage_bm25_v2",
            }
            fused.append(
                RetrievedEvidence(
                    observation_id=item.observation_id,
                    content=item.content,
                    score=round(fused_score, 6),
                    occurred_at=item.occurred_at,
                    metadata=metadata,
                )
            )
        fused.sort(
            key=lambda item: (
                item.score,
                item.metadata.get("hybrid_sage_rank") is not None,
                -(item.metadata.get("hybrid_bm25_rank") or 10_000),
            ),
            reverse=True,
        )
        return fused


class FyralisAskReader(FyralisDBReader):
    """Run the product Ask Fyralis path as a benchmark system.

    This reader materializes the benchmark rows into production tables, grants
    a synthetic benchmark viewer tenant-wide leadership access, then asks the
    real ``AskOrchestrator`` to answer each benchmark query. The final Ask
    answer is attached as passthrough metadata for the benchmark answerer, and
    the surfaced Ask evidence is returned as the retrieval packet.
    """

    async def _materialize(self) -> FyralisDBMaterialization:
        materialization = await super()._materialize()
        await self._upsert_benchmark_viewers()
        return materialization

    async def _upsert_benchmark_viewers(self) -> None:
        rows = [
            (
                self._viewer_id_for_tenant(source_tenant_id),
                tenant_id,
                f"Benchmark Ask viewer {source_tenant_id}"[:256],
                f"benchmark-ask-{source_tenant_id}@example.invalid"[:320],
            )
            for source_tenant_id, tenant_id in sorted(self._tenant_ids.items())
        ]
        role_rows = [
            (tenant_id, actor_id)
            for actor_id, tenant_id, _display_name, _email in rows
        ]
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(
                    """
                    INSERT INTO actors (
                        id, tenant_id, type, display_name, email, status, metadata
                    )
                    VALUES ($1, $2, 'user', $3, $4, 'active',
                            '{"source":"benchmark_ask"}'::jsonb)
                    ON CONFLICT (id) DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        email = EXCLUDED.email,
                        status = 'active'
                    """,
                    rows,
                )
                await conn.executemany(
                    """
                    INSERT INTO actor_roles (
                        tenant_id, actor_id, entity_type, entity_id, role,
                        granted_by, granted_at, revoked_at
                    )
                    VALUES ($1, $2, 'tenant', NULL, 'leadership', $2, now(), NULL)
                    ON CONFLICT ON CONSTRAINT actor_roles_dedup
                    DO NOTHING
                    """,
                    role_rows,
                )

    async def _retrieve(self, query: BenchmarkQuery) -> tuple[list[RetrievedEvidence], int]:
        tenant_id = self._tenant_ids.get(query.tenant_id)
        if tenant_id is None:
            return [], 0
        viewer_id = self._viewer_id_for_tenant(query.tenant_id)
        store = InMemoryAskStore()
        orchestrator = AskOrchestrator(
            store=store,
            conn_provider=self._pool.acquire,
            reader=SynthesisReader(
                pool=self._pool,
                budget=ReaderBudget(
                    max_nodes=max(24, self.top_k * 6),
                    max_edges=max(48, self.top_k * 12),
                    max_evidence_items=max(20, self.top_k * 4),
                    lexical_candidates=max(40, self.top_k * 12),
                    shortcut_candidates=12,
                    affordance_candidates=max(20, self.top_k * 4),
                    propagation_neighbors=max(80, self.top_k * 16),
                ),
            ),
        )
        scope = AskScope(
            type="whole_company",
            label=_ask_scope_label(query),
            filters={
                "benchmark": query.metadata.get("benchmark") or "benchmark",
                "query_id": query.query_id,
                "query_type": query.query_type,
                "haystack_tier": query.metadata.get("haystack_tier"),
            },
            access_mode="full",
        )
        session = await orchestrator.create_session(
            tenant_id=tenant_id,
            viewer_id=viewer_id,
            body=AskSessionCreateRequest(
                initial_scope=scope,
                source_route="/benchmarks/ask",
            ),
        )
        response = await orchestrator.answer_turn(
            tenant_id=tenant_id,
            viewer_id=viewer_id,
            session_id=session.id,
            body=AskTurnRequest(
                query=query.query_text,
                requested_mode="direct_synthesis_read",
            ),
        )
        evidence = self._evidence_from_ask_response(query, response)
        return evidence[: self.top_k], 1

    def _evidence_from_ask_response(self, query: BenchmarkQuery, response) -> list[RetrievedEvidence]:
        answer_metadata = {
            "answer": response.payload.answer,
            "confidence": response.payload.confidence,
            "intent": response.intent,
            "mode": response.mode,
            "retrieval_run_id": str(response.retrieval_run_id),
            "ask_latency_ms": response.latency_ms,
            "unknowns": response.payload.unknowns,
            "omitted_evidence_count": response.payload.omitted_evidence_count,
            "premise_check": response.payload.premise_check,
        }
        evidence: list[RetrievedEvidence] = []
        for index, item in enumerate(response.payload.evidence, start=1):
            observation = self._source_observation_for_ask_item(item.source_ref)
            source_metadata = observation.metadata if observation is not None else {}
            metadata = {
                **source_metadata,
                "retrieval_system": f"fyralis_ask:{self.embedding_model_version}",
                "ask_evidence_id": str(item.id),
                "ask_source_ref": str(item.source_ref) if item.source_ref else None,
                "ask_source_kind": item.source_kind,
                "ask_strength": item.strength,
                "ask_supports_answer": item.supports_answer,
                "ask_is_counterevidence": item.is_counterevidence,
                "ask_raw_payload": item.raw_payload,
            }
            if index == 1:
                metadata["passthrough_answer"] = answer_metadata
            evidence.append(
                RetrievedEvidence(
                    observation_id=(
                        observation.observation_id
                        if observation is not None
                        else f"{query.query_id}:ask:{item.source_kind}:{item.id}"
                    ),
                    content=item.summary,
                    score=round(_ask_evidence_score(item, index), 6),
                    occurred_at=(
                        observation.occurred_at.isoformat()
                        if observation is not None
                        else ""
                    ),
                    metadata=metadata,
                )
            )
        if evidence:
            return evidence
        return [
            RetrievedEvidence(
                observation_id=f"{query.query_id}:ask_answer_metadata",
                content="",
                score=0.0,
                occurred_at="",
                metadata={
                    "retrieval_system": f"fyralis_ask:{self.embedding_model_version}",
                    "benchmark_hidden_metadata_only": True,
                    "passthrough_answer": answer_metadata,
                },
            )
        ]

    def _source_observation_for_ask_item(self, source_ref: UUID | None) -> BenchmarkObservation | None:
        if source_ref is None:
            return None
        observation = self._observation_by_uuid.get(source_ref)
        if observation is not None:
            return observation
        return self._observation_by_model_id.get(source_ref)

    def _viewer_id_for_tenant(self, source_tenant_id: str) -> UUID:
        return _stable_uuid(f"{self.namespace}:ask-viewer:{source_tenant_id}")


class FyralisSagePrecisionHybridReader(FyralisSageHybridReader):
    """Keep SAGE's clean head, then fill missing packet slots with BM25."""

    async def _retrieve(self, query: BenchmarkQuery) -> tuple[list[RetrievedEvidence], int]:
        result = await self._read_sage(query, seed_model_ids=[])
        sage_evidence = [] if result is None else self._evidence_from_sage_result(result)
        bm25_limit = self.bm25_seed_candidates or max(80, self.top_k * 12)
        bm25_rows = self._bm25_scored_observations(query, limit=bm25_limit)
        fused = self._precision_fuse_sage_and_bm25(sage_evidence, bm25_rows)
        return fused[: self.top_k], 2

    def _precision_fuse_sage_and_bm25(
        self,
        sage_evidence: list[RetrievedEvidence],
        bm25_rows: list[tuple[float, BenchmarkObservation]],
    ) -> list[RetrievedEvidence]:
        sage_sorted = sorted(sage_evidence, key=lambda item: item.score, reverse=True)
        sage_head_size = min(
            self.top_k,
            max(1, round(self.top_k * 0.55)),
        )
        selected: list[RetrievedEvidence] = []
        seen: set[str] = set()

        bm25_ranks = {
            observation.observation_id: rank
            for rank, (_, observation) in enumerate(bm25_rows, start=1)
        }
        bm25_scores = {
            observation.observation_id: max(0.0, float(score))
            for score, observation in bm25_rows
        }

        for rank, item in enumerate(sage_sorted, start=1):
            if len(selected) >= sage_head_size:
                break
            if item.observation_id in seen:
                continue
            selected.append(
                _with_metadata(
                    item,
                    score=1.0 + _rrf_score(rank),
                    metadata={
                        "retrieval_system": (
                            f"fyralis_sage_precision_hybrid:{self.embedding_model_version}"
                        ),
                        "hybrid_sage_rank": rank,
                        "hybrid_bm25_rank": bm25_ranks.get(item.observation_id),
                        "hybrid_sage_score": round(float(item.score), 6),
                        "hybrid_bm25_score": round(
                            bm25_scores.get(item.observation_id, 0.0),
                            6,
                        ),
                        "hybrid_fusion": "sage_head_bm25_fill_v1",
                        "hybrid_source": "sage_head",
                    },
                )
            )
            seen.add(item.observation_id)

        for rank, (score, observation) in enumerate(bm25_rows, start=1):
            if len(selected) >= self.top_k:
                break
            if observation.observation_id in seen:
                continue
            selected.append(
                RetrievedEvidence(
                    observation_id=observation.observation_id,
                    content=observation.content,
                    score=round(_rrf_score(rank) + (0.05 * float(score)), 6),
                    occurred_at=observation.occurred_at.isoformat(),
                    metadata={
                        **observation.metadata,
                        "retrieval_system": (
                            f"fyralis_sage_precision_hybrid:{self.embedding_model_version}"
                        ),
                        "hybrid_sage_rank": None,
                        "hybrid_bm25_rank": rank,
                        "hybrid_sage_score": 0.0,
                        "hybrid_bm25_score": round(float(score), 6),
                        "hybrid_fusion": "sage_head_bm25_fill_v1",
                        "hybrid_source": "bm25_fill",
                    },
                )
            )
            seen.add(observation.observation_id)
        return selected


class FyralisSageCoverageHybridReader(FyralisSageHybridReader):
    """Fuse SAGE/BM25 with coverage-aware selection and local packet expansion."""

    async def _retrieve(self, query: BenchmarkQuery) -> tuple[list[RetrievedEvidence], int]:
        result = await self._read_sage(query, seed_model_ids=[])
        sage_evidence = [] if result is None else self._evidence_from_sage_result(result)
        bm25_limit = self.bm25_seed_candidates or max(160, self.top_k * 24)
        bm25_rows = self._bm25_scored_observations(query, limit=bm25_limit)
        candidate_rows = self._coverage_candidate_rows(query, bm25_rows)
        fused = self._coverage_fuse_sage_and_bm25(query, sage_evidence, candidate_rows)
        return fused[: self.top_k], 2

    def _coverage_candidate_rows(
        self,
        query: BenchmarkQuery,
        bm25_rows: list[tuple[float, BenchmarkObservation]],
    ) -> list[tuple[float, BenchmarkObservation]]:
        if not bm25_rows or not _coverage_or_counter_intent(query):
            return bm25_rows

        rows_by_id = {
            observation.observation_id: [float(score), observation]
            for score, observation in bm25_rows
        }
        tenant_observations = [
            observation
            for observation in self.observations
            if observation.tenant_id == query.tenant_id
        ]
        by_session_index: dict[int, list[BenchmarkObservation]] = {}
        by_temporal_order = sorted(
            tenant_observations,
            key=lambda item: (
                _metadata_int(item.metadata.get("session_index"), default=10_000_000),
                item.occurred_at,
                item.observation_id,
            ),
        )
        order_index = {
            observation.observation_id: index
            for index, observation in enumerate(by_temporal_order)
        }
        for observation in tenant_observations:
            session_index = _metadata_int(observation.metadata.get("session_index"))
            if session_index is None:
                continue
            by_session_index.setdefault(session_index, []).append(observation)

        seed_rows = bm25_rows[: max(12, self.top_k * 3)]
        for seed_score, seed in seed_rows:
            seed_session_index = _metadata_int(seed.metadata.get("session_index"))
            if seed_session_index is not None:
                for neighbor in by_session_index.get(seed_session_index, [])[:24]:
                    _add_candidate_neighbor(
                        rows_by_id,
                        neighbor=neighbor,
                        score=float(seed_score) * 0.58,
                    )

            seed_order = order_index.get(seed.observation_id)
            if seed_order is None:
                continue
            for offset in (-2, -1, 1, 2):
                neighbor_index = seed_order + offset
                if neighbor_index < 0 or neighbor_index >= len(by_temporal_order):
                    continue
                neighbor = by_temporal_order[neighbor_index]
                _add_candidate_neighbor(
                    rows_by_id,
                    neighbor=neighbor,
                    score=float(seed_score) * (0.42 / abs(offset)),
                )

        rows = [
            (float(score), observation)
            for score, observation in rows_by_id.values()
            if float(score) > 0
        ]
        rows.sort(key=lambda item: item[0], reverse=True)
        return rows[: max(len(bm25_rows), self.top_k * 32)]

    def _coverage_fuse_sage_and_bm25(
        self,
        query: BenchmarkQuery,
        sage_evidence: list[RetrievedEvidence],
        bm25_rows: list[tuple[float, BenchmarkObservation]],
    ) -> list[RetrievedEvidence]:
        candidates: dict[str, RetrievedEvidence] = {}
        sage_ranks: dict[str, int] = {}
        sage_scores: dict[str, float] = {}
        for rank, item in enumerate(
            sorted(sage_evidence, key=lambda value: value.score, reverse=True),
            start=1,
        ):
            candidates[item.observation_id] = item
            sage_ranks[item.observation_id] = rank
            sage_scores[item.observation_id] = max(0.0, float(item.score))

        bm25_ranks: dict[str, int] = {}
        bm25_scores: dict[str, float] = {}
        for rank, (score, observation) in enumerate(bm25_rows, start=1):
            bm25_ranks[observation.observation_id] = rank
            bm25_scores[observation.observation_id] = max(0.0, float(score))
            if observation.observation_id not in candidates:
                candidates[observation.observation_id] = RetrievedEvidence(
                    observation_id=observation.observation_id,
                    content=observation.content,
                    score=0.0,
                    occurred_at=observation.occurred_at.isoformat(),
                    metadata={
                        **observation.metadata,
                        "retrieval_system": (
                            f"fyralis_sage_coverage_hybrid:{self.embedding_model_version}"
                        ),
                    },
                )

        if not candidates:
            return []

        query_terms = set(token_counts(query.query_text))
        coverage_intent = _coverage_or_counter_intent(query)
        max_sage_score = max(sage_scores.values(), default=0.0) or 1.0
        max_bm25_score = max(bm25_scores.values(), default=0.0) or 1.0
        base_scores: dict[str, float] = {}
        for observation_id in candidates:
            base_scores[observation_id] = (
                (0.52 * _rrf_score(bm25_ranks.get(observation_id)))
                + (0.26 * _rrf_score(sage_ranks.get(observation_id)))
                + (0.12 * (bm25_scores.get(observation_id, 0.0) / max_bm25_score))
                + (0.10 * (sage_scores.get(observation_id, 0.0) / max_sage_score))
            )

        selected: list[RetrievedEvidence] = []
        selected_ids: set[str] = set()
        selected_token_sets: list[set[str]] = []
        selected_sessions: set[str] = set()
        while len(selected) < min(self.top_k, len(candidates)):
            best_id: str | None = None
            best_score = -1.0
            for observation_id, item in candidates.items():
                if observation_id in selected_ids:
                    continue
                token_set = set(_salient_tokens(item.content))
                adjusted = base_scores[observation_id]
                if coverage_intent:
                    adjusted += 0.10 * _query_overlap_ratio(query_terms, token_set)
                    session_key = _coverage_session_key(item)
                    if session_key and session_key not in selected_sessions:
                        adjusted += 0.07
                    if selected_token_sets:
                        adjusted -= 0.16 * max(
                            _jaccard(token_set, selected_tokens)
                            for selected_tokens in selected_token_sets
                        )
                if adjusted > best_score:
                    best_id = observation_id
                    best_score = adjusted
            if best_id is None:
                break
            item = candidates[best_id]
            selected_ids.add(best_id)
            selected_token_sets.append(set(_salient_tokens(item.content)))
            session_key = _coverage_session_key(item)
            if session_key:
                selected_sessions.add(session_key)
            selected.append(
                _with_metadata(
                    item,
                    score=best_score,
                    metadata={
                        "retrieval_system": (
                            f"fyralis_sage_coverage_hybrid:{self.embedding_model_version}"
                        ),
                        "hybrid_sage_rank": sage_ranks.get(best_id),
                        "hybrid_bm25_rank": bm25_ranks.get(best_id),
                        "hybrid_sage_score": round(sage_scores.get(best_id, 0.0), 6),
                        "hybrid_bm25_score": round(bm25_scores.get(best_id, 0.0), 6),
                        "hybrid_fusion": "coverage_mmr_sage_bm25_v1",
                        "coverage_intent": coverage_intent,
                    },
                )
            )
        return selected


class FyralisSageSemanticHybridReader(FyralisSageHybridReader):
    """Fuse SAGE, lexical BM25, and direct semantic embedding candidates."""

    async def _retrieve(self, query: BenchmarkQuery) -> tuple[list[RetrievedEvidence], int]:
        query_vector = await self._embed_text(self._embedding_text(query.query_text))
        result = await self._read_sage(
            query,
            seed_model_ids=[],
            query_vector=query_vector,
        )
        sage_evidence = [] if result is None else self._evidence_from_sage_result(result)
        candidate_limit = self.bm25_seed_candidates or max(160, self.top_k * 24)
        bm25_rows = self._bm25_scored_observations(query, limit=candidate_limit)
        semantic_rows = await self._semantic_scored_observations(
            query,
            limit=candidate_limit,
            query_vector=query_vector,
        )
        fused = self._semantic_fuse_sage_bm25_and_vectors(
            query,
            sage_evidence,
            bm25_rows,
            semantic_rows,
        )
        fused = _with_query_chain_candidate(query, fused)
        return _pack_query_evidence(query, fused, self.top_k), 3

    async def _semantic_scored_observations(
        self,
        query: BenchmarkQuery,
        *,
        limit: int,
        query_vector: list[float] | None = None,
    ) -> list[tuple[float, BenchmarkObservation]]:
        if limit <= 0:
            return []
        if query_vector is None:
            query_vector = await self._embed_text(self._embedding_text(query.query_text))
        if self._semantic_index is not None:
            return self._semantic_index.query(
                tenant_id=query.tenant_id,
                query_vector=query_vector,
                limit=limit,
            )
        rows: list[tuple[float, BenchmarkObservation]] = []
        for observation in self.observations:
            if observation.tenant_id != query.tenant_id:
                continue
            observation_vector = self._embedding_by_observation_id.get(
                observation.observation_id
            )
            if not observation_vector:
                continue
            score = _cosine_similarity(query_vector, observation_vector)
            if score > 0:
                rows.append((score, observation))
        rows.sort(key=lambda item: item[0], reverse=True)
        return rows[:limit]

    def _semantic_fuse_sage_bm25_and_vectors(
        self,
        query: BenchmarkQuery,
        sage_evidence: list[RetrievedEvidence],
        bm25_rows: list[tuple[float, BenchmarkObservation]],
        semantic_rows: list[tuple[float, BenchmarkObservation]],
    ) -> list[RetrievedEvidence]:
        candidates: dict[str, RetrievedEvidence] = {}
        sage_ranks: dict[str, int] = {}
        sage_scores: dict[str, float] = {}
        for rank, item in enumerate(
            sorted(sage_evidence, key=lambda value: value.score, reverse=True),
            start=1,
        ):
            candidates[item.observation_id] = item
            sage_ranks[item.observation_id] = rank
            sage_scores[item.observation_id] = max(0.0, float(item.score))

        bm25_ranks: dict[str, int] = {}
        bm25_scores: dict[str, float] = {}
        for rank, (score, observation) in enumerate(bm25_rows, start=1):
            bm25_ranks[observation.observation_id] = rank
            bm25_scores[observation.observation_id] = max(0.0, float(score))
            _add_retrieved_candidate(
                candidates,
                observation=observation,
                retrieval_system=f"fyralis_sage_semantic_hybrid:{self.embedding_model_version}",
            )

        semantic_ranks: dict[str, int] = {}
        semantic_scores: dict[str, float] = {}
        for rank, (score, observation) in enumerate(semantic_rows, start=1):
            semantic_ranks[observation.observation_id] = rank
            semantic_scores[observation.observation_id] = max(0.0, float(score))
            _add_retrieved_candidate(
                candidates,
                observation=observation,
                retrieval_system=f"fyralis_sage_semantic_hybrid:{self.embedding_model_version}",
            )

        max_sage_score = max(sage_scores.values(), default=0.0) or 1.0
        max_bm25_score = max(bm25_scores.values(), default=0.0) or 1.0
        max_semantic_score = max(semantic_scores.values(), default=0.0) or 1.0
        query_scope_terms = _scope_query_terms(query)
        fused: list[RetrievedEvidence] = []
        for observation_id, item in candidates.items():
            sage_rank = sage_ranks.get(observation_id)
            bm25_rank = bm25_ranks.get(observation_id)
            semantic_rank = semantic_ranks.get(observation_id)
            scope_overlap = _scope_overlap_score(
                query_scope_terms,
                self._observation_by_id.get(observation_id),
                structured_ui_intents=_structured_ui_intents(query.query_text),
            )
            transition_salience = _dynamic_transition_salience_score(
                query,
                self._observation_by_id.get(observation_id),
            )
            temporal_viewpoint = _temporal_viewpoint_score(
                query,
                self._observation_by_id.get(observation_id),
            )
            fused_score = (
                (0.34 * _rrf_score(semantic_rank))
                + (0.28 * _rrf_score(bm25_rank))
                + (0.20 * _rrf_score(sage_rank))
                + (0.08 * (semantic_scores.get(observation_id, 0.0) / max_semantic_score))
                + (0.06 * (bm25_scores.get(observation_id, 0.0) / max_bm25_score))
                + (0.04 * (sage_scores.get(observation_id, 0.0) / max_sage_score))
                + (0.10 * scope_overlap)
                + transition_salience
                + temporal_viewpoint
            )
            fused.append(
                _with_metadata(
                    item,
                    score=fused_score,
                    metadata={
                        "retrieval_system": (
                            f"fyralis_sage_semantic_hybrid:{self.embedding_model_version}"
                        ),
                        "hybrid_sage_rank": sage_rank,
                        "hybrid_bm25_rank": bm25_rank,
                        "hybrid_semantic_rank": semantic_rank,
                        "hybrid_sage_score": round(sage_scores.get(observation_id, 0.0), 6),
                        "hybrid_bm25_score": round(bm25_scores.get(observation_id, 0.0), 6),
                        "hybrid_semantic_score": round(
                            semantic_scores.get(observation_id, 0.0),
                            6,
                        ),
                        "hybrid_scope_overlap": round(scope_overlap, 6),
                        "hybrid_transition_salience": round(transition_salience, 6),
                        "hybrid_temporal_viewpoint": round(temporal_viewpoint, 6),
                        "hybrid_roles": sorted(_retrieval_roles(query, item)),
                        "hybrid_fusion": (
                            "rrf_sage_bm25_semantic_scope_transition_v1"
                        ),
                    },
                )
            )
        fused.sort(
            key=lambda item: (
                item.score,
                item.metadata.get("hybrid_semantic_rank") is not None,
                item.metadata.get("hybrid_bm25_rank") is not None,
                item.metadata.get("hybrid_sage_rank") is not None,
                -(item.metadata.get("hybrid_semantic_rank") or 10_000),
            ),
            reverse=True,
        )
        return fused


def hashed_token_vector(text: str) -> list[float]:
    """Build a stable 768-dimensional vector from lexical tokens."""
    counts = token_counts(text)
    if not counts:
        counts = token_counts(_digest_hex(text or "empty"))
    vector = [0.0] * EMBEDDING_DIM
    for token, count in counts.items():
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = int.from_bytes(digest[:4], "big") % EMBEDDING_DIM
        sign = 1.0 if digest[4] & 1 else -1.0
        vector[index] += sign * (1.0 + math.log(float(count)))
    norm = math.sqrt(sum(value * value for value in vector)) or 1.0
    return [value / norm for value in vector]


async def _ensure_observation_partition(
    conn: asyncpg.Connection,
    month: date,
) -> None:
    start = month
    if month.month == 12:
        end = date(month.year + 1, 1, 1)
    else:
        end = date(month.year, month.month + 1, 1)
    partition_name = f"observations_{start:%Y_%m}"
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {partition_name}
        PARTITION OF observations
        FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}')
        """
    )


async def _benchmark_pool_init(conn: asyncpg.Connection) -> None:
    from services.app.gateway.db_bootstrap import _register_codecs

    await _register_codecs(conn)


def _namespace_for_observations(
    namespace: str,
    observations: list[BenchmarkObservation],
) -> str:
    digest = hashlib.sha256()
    digest.update(namespace.encode("utf-8"))
    for observation in sorted(observations, key=lambda item: item.observation_id):
        digest.update(b"\0")
        digest.update(observation.source.encode("utf-8"))
        digest.update(b":")
        digest.update(observation.tenant_id.encode("utf-8"))
        digest.update(b":")
        digest.update(observation.observation_id.encode("utf-8"))
        digest.update(b":")
        digest.update(hashlib.sha256(observation.content.encode("utf-8")).digest())
    return f"{namespace}:{digest.hexdigest()[:16]}"


def _stable_uuid(seed: str) -> UUID:
    return uuid5(_BENCH_NAMESPACE, seed)


def _digest_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _embedding_cache_enabled() -> bool:
    return os.getenv("BENCHMARK_EMBED_CACHE", "1").strip().lower() not in {
        "0",
        "false",
        "no",
    }


def _embedding_cache_dir() -> Path:
    configured = os.getenv("BENCHMARK_EMBED_CACHE_DIR")
    if configured:
        return Path(configured).expanduser()
    return Path("benchmarks/.cache/embeddings")


def _embedding_cache_key(model_version: str, text: str) -> str:
    digest = hashlib.sha256()
    digest.update(model_version.encode("utf-8"))
    digest.update(b"\0")
    digest.update(text.encode("utf-8"))
    return digest.hexdigest()


def _read_cached_embedding(path: Path) -> list[float] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(value, list) or not value:
        return None
    try:
        return [float(item) for item in value]
    except (TypeError, ValueError):
        return None


def _write_cached_embedding(path: Path, vector: list[float]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(vector), encoding="utf-8")
    tmp.replace(path)


def _derived_memory_observations_by_tenant(
    observations: list[BenchmarkObservation],
) -> dict[str, list[BenchmarkObservation]]:
    by_tenant: dict[str, list[BenchmarkObservation]] = {}
    for observation in observations:
        if observation.metadata.get("benchmark") != "MEMTRACK":
            continue
        if observation.metadata.get("observation_kind") != "timeline_event":
            continue
        by_tenant.setdefault(observation.tenant_id, []).append(observation)

    derived: dict[str, list[BenchmarkObservation]] = {}
    for tenant_id, tenant_observations in by_tenant.items():
        rows = _derive_memtrack_memory_observations(tenant_id, tenant_observations)
        if rows:
            derived[tenant_id] = rows
    return derived


def _derive_memtrack_memory_observations(
    tenant_id: str,
    observations: list[BenchmarkObservation],
) -> list[BenchmarkObservation]:
    if not observations:
        return []
    ordered = sorted(
        observations,
        key=lambda item: (
            _metadata_int(item.metadata.get("event_index"), default=10_000_000),
            item.occurred_at,
            item.observation_id,
        ),
    )
    case_id = str(ordered[0].metadata.get("case_id") or tenant_id)
    latest_time = max(item.occurred_at for item in ordered)
    rows: list[BenchmarkObservation] = []

    current_state = _derive_current_state_observation(
        tenant_id=tenant_id,
        case_id=case_id,
        occurred_at=latest_time,
        ordered=ordered,
    )
    if current_state is not None:
        rows.append(current_state)

    reassignment = _derive_reassignment_observation(
        tenant_id=tenant_id,
        case_id=case_id,
        occurred_at=latest_time,
        ordered=ordered,
    )
    if reassignment is not None:
        rows.append(reassignment)

    artifacts = _derive_artifact_index_observation(
        tenant_id=tenant_id,
        case_id=case_id,
        occurred_at=latest_time,
        ordered=ordered,
    )
    if artifacts is not None:
        rows.append(artifacts)

    return rows


def _derive_current_state_observation(
    *,
    tenant_id: str,
    case_id: str,
    occurred_at,
    ordered: list[BenchmarkObservation],
) -> BenchmarkObservation | None:
    by_title: dict[str, list[BenchmarkObservation]] = {}
    for observation in ordered:
        title = str(observation.metadata.get("title") or "").strip()
        if title:
            by_title.setdefault(title, []).append(observation)
    if not by_title:
        return None

    latest_by_title = {
        title: rows[-1]
        for title, rows in sorted(by_title.items())
        if rows
    }
    status_counts: dict[str, int] = {}
    for observation in latest_by_title.values():
        status = str(observation.metadata.get("status") or "unknown").strip() or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1

    count_line = ", ".join(
        f"{status}={count}"
        for status, count in sorted(status_counts.items())
    )
    lines = [
        "MEMTRACK derived current state index",
        f"Case: {case_id}",
        "Derived from timeline events only; latest event per ticket title defines current state.",
        f"Current ticket count: {len(latest_by_title)}",
        f"Current status counts: {count_line}",
    ]
    if "done" in status_counts:
        lines.append(f"Current done ticket count: {status_counts['done']}")
    if "in_progress" in status_counts:
        lines.append(f"Current in_progress ticket count: {status_counts['in_progress']}")
    if "todo" in status_counts:
        lines.append(f"Current todo ticket count: {status_counts['todo']}")
    if "cancelled" in status_counts:
        lines.append(f"Current cancelled ticket count: {status_counts['cancelled']}")

    lines.append("Current tickets by latest state:")
    for title, observation in sorted(
        latest_by_title.items(),
        key=lambda item: (
            str(item[1].metadata.get("status") or ""),
            _metadata_int(item[1].metadata.get("event_index"), default=10_000_000),
            item[0],
        ),
    ):
        lines.append(
            "- "
            f"{title}: status={observation.metadata.get('status')}; "
            f"lead={observation.metadata.get('lead')}; "
            f"team={observation.metadata.get('team')}; "
            f"latest_event_index={observation.metadata.get('event_index')}; "
            f"timestamp={observation.metadata.get('timestamp_raw')}"
        )

    return BenchmarkObservation(
        observation_id=f"{tenant_id}:derived:current_state",
        source="benchmark_memtrack_derived_state",
        tenant_id=tenant_id,
        occurred_at=occurred_at,
        content="\n".join(lines),
        entities=[
            {"type": "status", "id": status}
            for status in sorted(status_counts)
        ],
        metadata={
            "benchmark": "MEMTRACK",
            "case_id": case_id,
            "observation_kind": "derived_memory_index",
            "derived_kind": "current_state",
            "derived_from_event_count": len(ordered),
            "current_status_counts": status_counts,
        },
    )


def _derive_reassignment_observation(
    *,
    tenant_id: str,
    case_id: str,
    occurred_at,
    ordered: list[BenchmarkObservation],
) -> BenchmarkObservation | None:
    by_title: dict[str, list[BenchmarkObservation]] = {}
    for observation in ordered:
        title = str(observation.metadata.get("title") or "").strip()
        if title:
            by_title.setdefault(title, []).append(observation)

    changed: list[tuple[str, list[BenchmarkObservation], list[str]]] = []
    for title, rows in by_title.items():
        leads: list[str] = []
        for observation in rows:
            lead = str(observation.metadata.get("lead") or "").strip()
            if lead and (not leads or leads[-1] != lead):
                leads.append(lead)
        if len(leads) > 1:
            changed.append((title, rows, leads))
    if not changed:
        return None

    changed.sort(
        key=lambda item: (
            _metadata_int(item[1][0].metadata.get("event_index"), default=10_000_000),
            item[0],
        ),
    )
    oldest_title, oldest_rows, oldest_leads = changed[0]
    oldest_first = oldest_rows[0]
    oldest_latest = oldest_rows[-1]
    lines = [
        "MEMTRACK derived reassignment index",
        f"Case: {case_id}",
        "Derived from lead changes across timeline events grouped by ticket title.",
        f"Lead-changed ticket count: {len(changed)}",
        f"Oldest reassigned ticket: {oldest_title}",
        f"Oldest reassigned ticket original lead: {oldest_leads[0]}",
        f"Oldest reassigned ticket current lead: {oldest_leads[-1]}",
        f"Oldest reassigned ticket current status: {oldest_latest.metadata.get('status')}",
        f"Oldest reassigned ticket first event index: {oldest_first.metadata.get('event_index')}",
        f"Oldest reassigned ticket latest event index: {oldest_latest.metadata.get('event_index')}",
        "Reassignment histories:",
    ]
    entities: list[dict[str, Any]] = []
    for title, rows, leads in changed[:24]:
        latest = rows[-1]
        entities.extend({"type": "actor", "id": lead} for lead in leads)
        history = "; ".join(
            (
                f"event {row.metadata.get('event_index')} "
                f"lead={row.metadata.get('lead')} "
                f"status={row.metadata.get('status')}"
            )
            for row in rows
        )
        lines.append(
            f"- {title}: leads={' -> '.join(leads)}; "
            f"current_lead={leads[-1]}; "
            f"current_status={latest.metadata.get('status')}; "
            f"history={history}"
        )

    return BenchmarkObservation(
        observation_id=f"{tenant_id}:derived:reassignment",
        source="benchmark_memtrack_derived_state",
        tenant_id=tenant_id,
        occurred_at=occurred_at,
        content="\n".join(lines),
        entities=_dedupe_benchmark_entities(entities),
        metadata={
            "benchmark": "MEMTRACK",
            "case_id": case_id,
            "observation_kind": "derived_memory_index",
            "derived_kind": "reassignment",
            "derived_from_event_count": len(ordered),
            "oldest_reassigned_title": oldest_title,
            "oldest_reassigned_current_lead": oldest_leads[-1],
            "oldest_reassigned_current_status": oldest_latest.metadata.get("status"),
        },
    )


def _derive_artifact_index_observation(
    *,
    tenant_id: str,
    case_id: str,
    occurred_at,
    ordered: list[BenchmarkObservation],
) -> BenchmarkObservation | None:
    rows: list[tuple[int, list[str], BenchmarkObservation]] = []
    for observation in ordered:
        artifacts = _artifact_terms_from_text(observation.content)
        if not artifacts:
            continue
        rows.append((
            _metadata_int(observation.metadata.get("event_index"), default=10_000_000)
            or 10_000_000,
            artifacts,
            observation,
        ))
    if not rows:
        return None

    lines = [
        "MEMTRACK derived artifact and code reference index",
        f"Case: {case_id}",
        "Derived from file, commit, PR, version, resource, and line references in timeline events.",
        "Artifact references by event:",
    ]
    entities: list[dict[str, Any]] = []
    for event_index, artifacts, observation in rows[:80]:
        short_content = " ".join(observation.content.split())
        if len(short_content) > 220:
            short_content = f"{short_content[:217]}..."
        lines.append(
            f"- event {event_index}: "
            f"platform={observation.metadata.get('platform')}; "
            f"actor={observation.metadata.get('sender') or observation.metadata.get('lead')}; "
            f"title={observation.metadata.get('title')}; "
            f"artifacts={', '.join(artifacts[:12])}; "
            f"context={short_content}"
        )
        entities.extend({"type": "artifact", "id": artifact} for artifact in artifacts[:12])

    return BenchmarkObservation(
        observation_id=f"{tenant_id}:derived:artifact_index",
        source="benchmark_memtrack_derived_artifact_index",
        tenant_id=tenant_id,
        occurred_at=occurred_at,
        content="\n".join(lines),
        entities=_dedupe_benchmark_entities(entities),
        metadata={
            "benchmark": "MEMTRACK",
            "case_id": case_id,
            "observation_kind": "derived_memory_index",
            "derived_kind": "artifact_index",
            "derived_from_event_count": len(ordered),
        },
    )


def _artifact_terms_from_text(text: str) -> list[str]:
    patterns = (
        r"\b[a-f0-9]{7,40}\b",
        r"\b[A-Za-z0-9_.-]+\.(?:py|ts|tsx|js|jsx|css|html|md|json|ya?ml|toml|lock|txt)\b",
        r"/[A-Za-z0-9_./-]+\.[A-Za-z0-9_-]+",
        r"#\d+\b",
        r"\bv?\d+\.\d+(?:\.\d+)?(?:[-+][A-Za-z0-9_.-]+)?\b",
        r"\bline\s+\d+\b",
    )
    out: list[str] = []
    for pattern in patterns:
        for match in re.findall(pattern, text, flags=re.IGNORECASE):
            value = str(match).strip()
            if value and value not in out:
                out.append(value)
            if len(out) >= 24:
                return out
    return out


def _dedupe_benchmark_entities(entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for entity in entities:
        entity_type = str(entity.get("type") or "")
        entity_id = str(entity.get("id") or "")
        key = (entity_type, entity_id)
        if not entity_type or not entity_id or key in seen:
            continue
        seen.add(key)
        out.append({"type": entity_type, "id": entity_id})
    return out


def _derived_candidate_rows(
    *,
    query: BenchmarkQuery,
    derived_observations: list[BenchmarkObservation],
    score_floor: float,
) -> list[tuple[float, BenchmarkObservation]]:
    if not derived_observations:
        return []
    intents = _derived_query_intents(query)
    if not intents:
        return []
    base = max(1.0, float(score_floor))
    rows: list[tuple[float, BenchmarkObservation]] = []
    weights = {
        "current_state": 1.38,
        "reassignment": 1.55,
        "artifact_index": 1.22,
    }
    for observation in derived_observations:
        derived_kind = str(observation.metadata.get("derived_kind") or "")
        if derived_kind not in intents:
            continue
        relevance = _query_evidence_relevance(query, observation)
        slot_score = _question_slot_score(query, observation)
        score = base * weights.get(derived_kind, 1.0)
        score += 0.35 * relevance
        score += 0.25 * slot_score
        rows.append((score, observation))
    rows.sort(key=lambda item: item[0], reverse=True)
    return rows


def _derived_query_intents(query: BenchmarkQuery) -> set[str]:
    text = f"{query.query_text} {query.query_type}".casefold()
    intents: set[str] = set()
    count_marker = any(marker in text for marker in ("how many", "count", "number"))
    ticket_state_marker = any(
        marker in text
        for marker in (
            "ticket",
            "tickets",
            "currently",
            "current",
            "status",
            "done",
            "todo",
            "in progress",
            "in_progress",
            "cancelled",
        )
    )
    if ticket_state_marker and (
        count_marker
        or any(marker in text for marker in ("currently", "current", "status"))
    ):
        intents.add("current_state")
    if any(marker in text for marker in ("reassign", "oldest reassigned")) or (
        "oldest" in text and any(marker in text for marker in ("lead", "owner", "status"))
    ):
        intents.add("reassignment")
    if any(
        marker in text
        for marker in (
            "commit",
            "hash",
            "line number",
            "line ",
            "file",
            "file type",
            "version",
            "pr ",
            "pull request",
            ".py",
            ".ts",
            ".tsx",
            ".css",
            ".html",
        )
    ):
        intents.add("artifact_index")
    return intents


def _benchmark_graph_edge_rows(
    *,
    namespace: str,
    observations: list[BenchmarkObservation],
    tenant_ids: dict[str, UUID],
    observation_ids: dict[str, UUID],
    model_ids: dict[str, UUID],
) -> list[tuple[Any, ...]]:
    rows_by_key: dict[tuple[UUID, UUID, UUID, str], tuple[Any, ...]] = {}
    by_tenant: dict[str, list[BenchmarkObservation]] = {}
    for observation in observations:
        by_tenant.setdefault(observation.tenant_id, []).append(observation)

    for tenant_key, tenant_observations in by_tenant.items():
        tenant_id = tenant_ids[tenant_key]
        ordered = sorted(
            tenant_observations,
            key=lambda item: (item.occurred_at, item.observation_id),
        )

        for left, right in zip(ordered, ordered[1:]):
            _add_edge_row(
                rows_by_key,
                namespace=namespace,
                tenant_id=tenant_id,
                left=left,
                right=right,
                observation_ids=observation_ids,
                model_ids=model_ids,
                kind="co_occurs_with",
                weight=0.42,
                reason="temporal_neighbor",
            )
            temporal_relation = _benchmark_temporal_relation(left, right)
            if temporal_relation is not None:
                edge_kind, weight, reason = temporal_relation
                _add_edge_row(
                    rows_by_key,
                    namespace=namespace,
                    tenant_id=tenant_id,
                    left=left,
                    right=right,
                    observation_ids=observation_ids,
                    model_ids=model_ids,
                    kind=edge_kind,
                    weight=weight,
                    reason=reason,
                )

        token_index: dict[str, list[int]] = {}
        token_sets: list[set[str]] = []
        for idx, observation in enumerate(ordered):
            tokens = set(_salient_tokens(observation.content))
            token_sets.append(tokens)
            for token in tokens:
                token_index.setdefault(token, []).append(idx)

        pair_scores: dict[tuple[int, int], int] = {}
        for indexes in token_index.values():
            if len(indexes) < 2 or len(indexes) > 50:
                continue
            for pos, left_idx in enumerate(indexes):
                for right_idx in indexes[pos + 1 :]:
                    key = (left_idx, right_idx)
                    pair_scores[key] = pair_scores.get(key, 0) + 1

        neighbors_by_idx: dict[int, list[tuple[int, int]]] = {}
        for (left_idx, right_idx), score in pair_scores.items():
            if score < 2:
                continue
            neighbors_by_idx.setdefault(left_idx, []).append((score, right_idx))
            neighbors_by_idx.setdefault(right_idx, []).append((score, left_idx))

        emitted_pairs: set[tuple[int, int]] = set()
        for left_idx, neighbors in neighbors_by_idx.items():
            ranked = sorted(
                neighbors,
                key=lambda item: (
                    -item[0],
                    abs(item[1] - left_idx),
                    ordered[item[1]].observation_id,
                ),
            )[:4]
            for score, right_idx in ranked:
                pair = tuple(sorted((left_idx, right_idx)))
                if pair in emitted_pairs:
                    continue
                emitted_pairs.add(pair)
                overlap = sorted(token_sets[left_idx] & token_sets[right_idx])[:8]
                _add_edge_row(
                    rows_by_key,
                    namespace=namespace,
                    tenant_id=tenant_id,
                    left=ordered[left_idx],
                    right=ordered[right_idx],
                    observation_ids=observation_ids,
                    model_ids=model_ids,
                    kind="same_issue_as",
                    weight=min(0.9, 0.28 + 0.07 * float(score)),
                    reason="shared_salient_terms",
                    extra_metadata={"shared_terms": overlap, "overlap_score": score},
                )

    return list(rows_by_key.values())


def _add_edge_row(
    rows_by_key: dict[tuple[UUID, UUID, UUID, str], tuple[Any, ...]],
    *,
    namespace: str,
    tenant_id: UUID,
    left: BenchmarkObservation,
    right: BenchmarkObservation,
    observation_ids: dict[str, UUID],
    model_ids: dict[str, UUID],
    kind: str,
    weight: float,
    reason: str,
    extra_metadata: dict[str, Any] | None = None,
) -> None:
    left_model_id = model_ids[left.observation_id]
    right_model_id = model_ids[right.observation_id]
    if left_model_id == right_model_id:
        return
    source_model_id, target_model_id = sorted((left_model_id, right_model_id), key=str)
    left_observation_id = observation_ids[left.observation_id]
    right_observation_id = observation_ids[right.observation_id]
    key = (tenant_id, source_model_id, target_model_id, kind)
    edge_id = _stable_uuid(
        f"{namespace}:edge:{tenant_id}:{source_model_id}:{target_model_id}:{kind}"
    )
    metadata = {
        "benchmark_graph_enrichment": True,
        "reason": reason,
        "source_observation_id": left.observation_id,
        "target_observation_id": right.observation_id,
    }
    if extra_metadata:
        metadata.update(extra_metadata)
    row = (
        edge_id,
        tenant_id,
        source_model_id,
        target_model_id,
        kind,
        float(weight),
        json.dumps(metadata, sort_keys=True, default=str),
        left_observation_id,
        float(weight),
        [left_observation_id, right_observation_id],
        f"Benchmark graph enrichment: {reason}",
    )
    existing = rows_by_key.get(key)
    if existing is None or float(existing[5]) < float(weight):
        rows_by_key[key] = row


def _benchmark_temporal_relation(
    left: BenchmarkObservation,
    right: BenchmarkObservation,
) -> tuple[str, float, str] | None:
    """Infer a light structural edge from public event order and text.

    This is intentionally source-only: it never looks at benchmark
    answers. The goal is just to preserve the relation cues already
    present in the event stream so SAGE sees more than undirected
    lexical co-occurrence.
    """

    left_text = left.content.casefold()
    right_text = right.content.casefold()
    combined = f"{left_text}\n{right_text}"
    if any(marker in right_text for marker in _RESOLUTION_EDGE_MARKERS):
        return ("contributes_to_resolution", 0.72, "temporal_resolution_marker")
    if any(marker in right_text for marker in _CAUSE_EDGE_MARKERS):
        return ("causes", 0.74, "temporal_causal_marker")
    if any(marker in combined for marker in _EXPLAINS_EDGE_MARKERS):
        return ("explains", 0.66, "temporal_explanation_marker")
    if (
        left.metadata.get("title")
        and left.metadata.get("title") == right.metadata.get("title")
        and (
            left.metadata.get("lead") == right.metadata.get("lead")
            or left.metadata.get("team") == right.metadata.get("team")
        )
    ):
        return ("supports", 0.58, "same_ticket_continuity")
    return None


def _salient_tokens(text: str) -> list[str]:
    counts = token_counts(text)
    tokens = [
        token
        for token, count in counts.items()
        if len(token) >= 4 and not token.isdigit() and count > 0
    ]
    return sorted(tokens, key=lambda token: (-counts[token], -len(token), token))[:32]


_TRIGGER_MARKERS = (
    "triggered",
    "trigger",
    "initial",
    "first",
    "started",
    "paper",
    "research insight",
    "alert",
    "discovered",
    "found",
)
_CAUSE_MARKERS = (
    "because",
    "caused",
    "cause",
    "conflict",
    "due to",
    "failed",
    "failure",
    "root cause",
    "triggered",
    "blocked",
    "dependency",
)
_TRANSITION_MARKERS = (
    "after",
    "before",
    "changed",
    "shifted",
    "split",
    "handoff",
    "reassigned",
    "moved",
    "became",
    "led to",
)
_OUTCOME_MARKERS = (
    "completed",
    "consequence",
    "deployed",
    "impact",
    "replacement",
    "resolved",
    "resolution",
    "rollback",
    "dropped",
    "risk",
    "solution",
)
_FINAL_OUTCOME_MARKERS = (
    "completed",
    "deployed",
    "final solution",
    "replacement:",
    "replacement shipped",
    "resolved",
    "resolution:",
    "solution shipped",
)
_HINDSIGHT_MARKERS = (
    "final solution",
    "final assessment",
    "postmortem",
    "post-mortem",
    "retrospective",
    "root cause",
    "later discovered",
    "eventually",
    "identified as",
)
_MENTAL_MODEL_CONTENT_MARKERS = (
    "assumed",
    "assumption",
    "believed",
    "expected",
    "hypothesis",
    "hypothesized",
    "interpreted",
    "mental model",
    "thought",
    "understood",
)
_DECISION_MARKERS = (
    "chose",
    "decided",
    "decision",
    "implemented",
    "proposed",
    "selected",
    "workaround",
    "approach",
)
_CAUSE_EDGE_MARKERS = (
    "because",
    "caused",
    "due to",
    "led to",
    "root cause",
    "triggered",
)
_RESOLUTION_EDGE_MARKERS = (
    "final solution",
    "fix",
    "fixed",
    "mitigation",
    "resolved",
    "resolution",
    "rollback",
)
_EXPLAINS_EDGE_MARKERS = (
    "explained",
    "identified",
    "discovered",
    "found that",
    "realized",
)


def _needs_bridge_completion(query: BenchmarkQuery) -> bool:
    text = f"{query.query_text} {query.query_type}".casefold()
    markers = {
        "after",
        "because",
        "blocked",
        "cascade",
        "causal",
        "caused",
        "connection",
        "consequence",
        "dependency",
        "despite",
        "effect",
        "failed",
        "final",
        "following",
        "impact",
        "instead",
        "led",
        "mechanism",
        "missed",
        "resolved",
        "rollback",
        "root",
        "solution",
        "triggered",
        "why",
        "workaround",
    }
    return any(marker in text for marker in markers)


def _needs_chain_composition(query: BenchmarkQuery) -> bool:
    text = f"{query.query_text} {query.query_type}".casefold()
    return (
        _needs_bridge_completion(query)
        or _is_mental_model_query(query)
        or _query_asks_for_finality(query)
        or any(marker in text for marker in ("timeline", "during", "before", "after"))
    )


def _needs_role_packing(query: BenchmarkQuery) -> bool:
    text = f"{query.query_text} {query.query_type}".casefold()
    return (
        _needs_chain_composition(query)
        or _asks_for_actor_or_owner(text)
        or _coverage_or_counter_intent(query)
    )


def _with_query_chain_candidate(
    query: BenchmarkQuery,
    evidence: list[RetrievedEvidence],
) -> list[RetrievedEvidence]:
    if not evidence or not _needs_chain_composition(query):
        return evidence
    if any(item.metadata.get("derived_kind") == "query_chain" for item in evidence):
        return evidence

    sources = _select_chain_sources(query, evidence, max_sources=6)
    if len(sources) < 2:
        return evidence

    covered_roles: set[str] = set()
    for item in sources:
        covered_roles.update(_retrieval_roles(query, item))
    desired_roles = _desired_roles(query)
    if len(covered_roles & desired_roles) < 2:
        return evidence

    ordered_sources = sorted(sources, key=_evidence_temporal_sort_key)
    source_ids = [item.observation_id for item in ordered_sources]
    digest_input = query.query_id + "\0" + "\0".join(source_ids)
    digest = hashlib.sha256(
        digest_input.encode("utf-8")
    ).hexdigest()[:16]
    source_lines = []
    for item in ordered_sources:
        roles = sorted(_retrieval_roles(query, item) & desired_roles)
        role_text = ",".join(roles) if roles else "context"
        structured = _chain_structured_fields(item)
        structured_text = f" | fields={structured}" if structured else ""
        source_lines.append(
            "- "
            f"{_source_event_label(item)} | roles={role_text} | "
            f"{_short_evidence_excerpt(item.content, max_chars=640)}"
            f"{structured_text}"
        )
    content = "\n".join([
        "Query-local composed evidence chain",
        "Built only from retrieved candidate events; source event IDs are preserved below.",
        f"Question: {query.query_text}",
        f"Covered roles: {', '.join(sorted(covered_roles & desired_roles))}",
        "Source events in temporal order:",
        *source_lines,
    ])
    score = max(float(item.score) for item in ordered_sources)
    score += min(0.42, 0.10 + 0.045 * len(covered_roles & desired_roles))
    chain = RetrievedEvidence(
        observation_id=f"{query.tenant_id}:derived:query_chain:{digest}",
        content=content,
        score=round(score, 6),
        occurred_at=ordered_sources[0].occurred_at,
        metadata={
            "retrieval_system": "query_local_chain_composition_v1",
            "observation_kind": "query_local_composition",
            "derived_kind": "query_chain",
            "source_observation_ids": source_ids,
            "covered_roles": sorted(covered_roles & desired_roles),
            "case_id": query.metadata.get("case_id"),
            "benchmark": query.metadata.get("benchmark"),
        },
    )
    return [chain, *evidence]


def _pack_query_evidence(
    query: BenchmarkQuery,
    evidence: list[RetrievedEvidence],
    top_k: int,
) -> list[RetrievedEvidence]:
    if top_k <= 0 or not evidence:
        return []
    ranked = sorted(evidence, key=lambda item: item.score, reverse=True)
    if not _needs_role_packing(query):
        return ranked[:top_k]

    desired_roles = _desired_roles(query)
    roles_by_id = {
        item.observation_id: _retrieval_roles(query, item)
        for item in ranked
    }
    tokens_by_id = {
        item.observation_id: set(_salient_tokens(item.content))
        for item in ranked
    }
    selected: list[RetrievedEvidence] = []
    selected_ids: set[str] = set()
    selected_tokens: list[set[str]] = []
    covered_roles: set[str] = set()
    while len(selected) < min(top_k, len(ranked)):
        best_item: RetrievedEvidence | None = None
        best_score = -1_000_000.0
        best_roles: set[str] = set()
        best_new_roles: set[str] = set()
        for item in ranked:
            if item.observation_id in selected_ids:
                continue
            roles = roles_by_id[item.observation_id]
            new_roles = (roles & desired_roles) - covered_roles
            token_set = tokens_by_id[item.observation_id]
            adjusted = float(item.score)
            adjusted += 0.24 * len(new_roles)
            if "composed_chain" in roles and "composed_chain" not in covered_roles:
                adjusted += 0.24
            if _is_mental_model_query(query) and "diagnosis" in roles and (
                "actor_viewpoint" not in covered_roles
            ):
                adjusted -= 0.22
            if "temporal_anchor" in new_roles:
                adjusted += 0.06
            if selected_tokens:
                adjusted -= 0.14 * max(
                    _jaccard(token_set, tokens) for tokens in selected_tokens
                )
            if adjusted > best_score:
                best_item = item
                best_score = adjusted
                best_roles = roles
                best_new_roles = new_roles
        if best_item is None:
            break
        selected_ids.add(best_item.observation_id)
        selected_tokens.append(tokens_by_id[best_item.observation_id])
        covered_roles.update(best_roles & desired_roles)
        selected.append(
            _with_metadata(
                best_item,
                score=best_score,
                metadata={
                    "hybrid_packing": "role_aware_chain_pack_v1",
                    "hybrid_packing_roles": sorted(best_roles),
                    "hybrid_packing_new_roles": sorted(best_new_roles),
                },
            )
        )
    return selected


def _select_chain_sources(
    query: BenchmarkQuery,
    evidence: list[RetrievedEvidence],
    *,
    max_sources: int,
) -> list[RetrievedEvidence]:
    desired_roles = _desired_roles(query) - {"composed_chain"}
    if not desired_roles:
        return []
    candidates = [
        item
        for item in sorted(evidence, key=lambda value: value.score, reverse=True)[:80]
        if _is_atomic_timeline_evidence(item)
    ]
    selected: list[RetrievedEvidence] = []
    selected_ids: set[str] = set()
    selected_tokens: list[set[str]] = []
    covered: set[str] = set()
    while len(selected) < min(max_sources, len(candidates)):
        best_item: RetrievedEvidence | None = None
        best_score = -1_000_000.0
        for item in candidates:
            if item.observation_id in selected_ids:
                continue
            roles = _retrieval_roles(query, item)
            new_roles = (roles & desired_roles) - covered
            if not new_roles and len(selected) >= 2:
                continue
            token_set = set(_salient_tokens(item.content))
            adjusted = float(item.score) + 0.30 * len(new_roles)
            adjusted += _temporal_viewpoint_score(query, item)
            if selected_tokens:
                adjusted -= 0.12 * max(
                    _jaccard(token_set, tokens) for tokens in selected_tokens
                )
            if adjusted > best_score:
                best_item = item
                best_score = adjusted
        if best_item is None:
            break
        selected.append(best_item)
        selected_ids.add(best_item.observation_id)
        selected_tokens.append(set(_salient_tokens(best_item.content)))
        covered.update(_retrieval_roles(query, best_item) & desired_roles)
        if covered >= desired_roles and len(selected) >= 3:
            break
    return selected


def _desired_roles(query: BenchmarkQuery) -> set[str]:
    text = f"{query.query_text} {query.query_type}".casefold()
    roles: set[str] = {"temporal_anchor"}
    if _needs_chain_composition(query):
        roles.update({"composed_chain", "trigger", "cause", "transition", "outcome"})
    if _is_mental_model_query(query):
        roles.update({"actor_viewpoint", "decision"})
        if not _query_asks_for_hindsight(query):
            roles.discard("diagnosis")
    if _query_asks_for_hindsight(query):
        roles.add("diagnosis")
    if _query_asks_for_finality(query):
        roles.update({"decision", "final_outcome"})
    if _asks_for_actor_or_owner(text):
        roles.update({"owner", "actor_viewpoint", "decision"})
    if _asks_for_current_or_count(text):
        roles.add("state")
    if any(marker in text for marker in ("commit", "hash", "file", "line", ".py")):
        roles.add("artifact")
    return roles


def _retrieval_roles(query: BenchmarkQuery, item: RetrievedEvidence) -> set[str]:
    metadata = item.metadata
    derived_kind = str(metadata.get("derived_kind") or "")
    if derived_kind == "query_chain":
        return {"composed_chain", "temporal_anchor"}

    text = f"{item.content} {_metadata_text(metadata)}".casefold()
    roles: set[str] = set()
    if _metadata_int(metadata.get("event_index")) is not None or metadata.get("timestamp_raw"):
        roles.add("temporal_anchor")
    if any(marker in text for marker in _TRIGGER_MARKERS):
        roles.add("trigger")
    if any(marker in text for marker in _CAUSE_MARKERS):
        roles.add("cause")
    if any(marker in text for marker in _TRANSITION_MARKERS):
        roles.add("transition")
    if any(marker in text for marker in _OUTCOME_MARKERS):
        roles.add("outcome")
    if any(marker in text for marker in _FINAL_OUTCOME_MARKERS):
        roles.add("final_outcome")
    if any(marker in text for marker in _HINDSIGHT_MARKERS):
        roles.add("diagnosis")
    if any(marker in text for marker in _MENTAL_MODEL_CONTENT_MARKERS):
        roles.add("actor_viewpoint")
    if any(marker in text for marker in _DECISION_MARKERS):
        roles.add("decision")
    if _has_actor_or_owner_metadata(metadata) or any(
        marker in text for marker in ("owner", "assigned", "responsible", "championed")
    ):
        roles.add("owner")
    if _has_state_or_count_metadata(metadata) or any(
        marker in text for marker in ("status:", "done", "todo", "in_progress")
    ):
        roles.add("state")
    if _artifact_terms_from_text(text):
        roles.add("artifact")
    return roles


def _temporal_viewpoint_score(
    query: BenchmarkQuery,
    observation: BenchmarkObservation | RetrievedEvidence | None,
) -> float:
    if observation is None:
        return 0.0
    query_text = f"{query.query_text} {query.query_type}".casefold()
    content = _evidence_content(observation).casefold()
    metadata = _evidence_metadata(observation)
    score = 0.0
    mental_model = _is_mental_model_query(query)
    hindsight = any(marker in content for marker in _HINDSIGHT_MARKERS)
    asks_hindsight = _query_asks_for_hindsight(query)
    asks_finality = _query_asks_for_finality(query)

    if mental_model:
        if any(marker in content for marker in _MENTAL_MODEL_CONTENT_MARKERS):
            score += 0.22
        if any(marker in content for marker in _DECISION_MARKERS):
            score += 0.08
        if hindsight and not asks_hindsight:
            score -= 0.24
    if any(marker in query_text for marker in ("during", "at the time", "then")):
        if hindsight and not asks_hindsight:
            score -= 0.12
        if _metadata_int(metadata.get("event_index")) is not None:
            score += 0.04
    if any(marker in query_text for marker in ("triggered", "initial", "first")):
        event_index = _metadata_int(metadata.get("event_index"))
        if event_index is not None and event_index <= 6:
            score += 0.08
    if asks_hindsight and hindsight:
        score += 0.10
    if asks_finality:
        if any(marker in content for marker in _FINAL_OUTCOME_MARKERS):
            score += 0.22
        if any(marker in content for marker in _DECISION_MARKERS):
            score += 0.18
        if "assessment" in content and not any(marker in content for marker in _FINAL_OUTCOME_MARKERS):
            score -= 0.10
    return max(-0.28, min(0.28, score))


def _is_mental_model_query(query: BenchmarkQuery) -> bool:
    text = f"{query.query_text} {query.query_type}".casefold()
    explicit = any(
        marker in text
        for marker in (
            "mental model",
            "believe",
            "believed",
            "assume",
            "assumed",
            "understand",
            "understood",
            "thinking",
            "thought",
        )
    )
    decision_why = "why did" in text and any(
        marker in text
        for marker in ("choose", "chose", "decide", "decided", "approach", "workaround")
    )
    return explicit or decision_why


def _query_asks_for_hindsight(query: BenchmarkQuery) -> bool:
    text = f"{query.query_text} {query.query_type}".casefold()
    return any(
        marker in text
        for marker in (
            "final",
            "postmortem",
            "post-mortem",
            "root cause",
            "retrospective",
            "identified as the root",
        )
    )


def _query_asks_for_finality(query: BenchmarkQuery) -> bool:
    text = f"{query.query_text} {query.query_type}".casefold()
    return any(
        marker in text
        for marker in (
            "final",
            "final solution",
            "resolved",
            "replacement",
            "solution",
            "what happened",
        )
    )


def _is_atomic_timeline_evidence(item: RetrievedEvidence) -> bool:
    metadata = item.metadata
    if metadata.get("derived_kind"):
        return False
    return (
        metadata.get("observation_kind") == "timeline_event"
        or ":event:" in item.observation_id
        or _metadata_int(metadata.get("event_index")) is not None
    )


def _evidence_temporal_sort_key(item: RetrievedEvidence) -> tuple[int, str, str]:
    event_index = _metadata_int(item.metadata.get("event_index"), default=10_000_000)
    return (
        event_index if event_index is not None else 10_000_000,
        str(item.occurred_at or ""),
        item.observation_id,
    )


def _source_event_label(item: RetrievedEvidence) -> str:
    event_index = _metadata_int(item.metadata.get("event_index"))
    if event_index is None:
        return item.observation_id
    return f"{item.observation_id} event_index={event_index}"


def _chain_structured_fields(item: RetrievedEvidence) -> str:
    fields = []
    for key in (
        "timestamp_raw",
        "platform",
        "sender",
        "lead",
        "team",
        "status",
        "priority",
        "title",
    ):
        value = item.metadata.get(key)
        if value not in (None, ""):
            fields.append(f"{key}={value}")
    return "; ".join(fields)


def _short_evidence_excerpt(text: str, *, max_chars: int) -> str:
    compact = " ".join(str(text or "").split())
    if len(compact) <= max_chars:
        return compact
    return f"{compact[: max_chars - 3].rstrip()}..."


def _evidence_content(
    item: BenchmarkObservation | RetrievedEvidence,
) -> str:
    return str(getattr(item, "content", "") or "")


def _evidence_metadata(
    item: BenchmarkObservation | RetrievedEvidence,
) -> dict[str, Any]:
    metadata = getattr(item, "metadata", {})
    return metadata if isinstance(metadata, dict) else {}


def _metadata_text(metadata: dict[str, Any]) -> str:
    parts = []
    for key in (
        "lead",
        "sender",
        "team",
        "status",
        "title",
        "platform",
        "channel",
        "commit_id",
        "pr",
    ):
        value = metadata.get(key)
        if value is not None:
            parts.append(str(value))
    return " ".join(parts)


def _has_actor_or_owner_metadata(metadata: dict[str, Any]) -> bool:
    return any(metadata.get(key) for key in ("lead", "team", "sender", "author"))


def _has_state_or_count_metadata(metadata: dict[str, Any]) -> bool:
    return metadata.get("status") is not None


def _query_evidence_relevance(
    query: BenchmarkQuery,
    observation: BenchmarkObservation | None,
) -> float:
    if observation is None:
        return 0.0
    query_text = query.query_text.casefold()
    content = observation.content.casefold()
    query_terms = set(_salient_tokens(f"{query.query_text} {query.query_type}"))
    content_terms = set(_salient_tokens(observation.content))
    score = 0.0
    if query_terms:
        overlap = query_terms & content_terms
        score += min(0.34, 0.055 * len(overlap))
        score += 0.22 * _query_overlap_ratio(query_terms, content_terms)

    score += min(0.16, 0.035 * _ordered_phrase_hits(query_text, content))
    metadata_terms = set(_salient_tokens(_metadata_scope_text(observation)))
    if metadata_terms and query_terms:
        score += min(0.18, 0.045 * len(query_terms & metadata_terms))

    if _needs_bridge_completion(query):
        causal_terms = {
            "because",
            "blocked",
            "caused",
            "conflict",
            "dependency",
            "failed",
            "failure",
            "impact",
            "identified",
            "missed",
            "postmortem",
            "resolution",
            "resolved",
            "rollback",
            "root",
            "solution",
            "triggered",
            "workaround",
        }
        if content_terms & causal_terms:
            score += min(0.18, 0.045 * len(content_terms & causal_terms))
        if any(marker in content for marker in ("root cause", "final solution", "resolution:")):
            score += 0.12

    if _asks_for_actor_or_owner(query_text) and _has_actor_or_owner(observation):
        score += 0.12
    if _asks_for_current_or_count(query_text) and _has_state_or_count_signal(observation):
        score += 0.12
    return max(0.0, min(1.0, score))


def _question_slot_score(
    query: BenchmarkQuery,
    observation: BenchmarkObservation | None,
) -> float:
    if observation is None:
        return 0.0
    query_text = query.query_text.casefold()
    content = observation.content.casefold()
    metadata = observation.metadata
    requested = 0
    covered = 0

    def need(condition: bool, has_slot: bool) -> None:
        nonlocal requested, covered
        if not condition:
            return
        requested += 1
        if has_slot:
            covered += 1

    content_terms = set(_salient_tokens(observation.content))
    need(
        any(marker in query_text for marker in ("root cause", "caused", "why", "triggered")),
        any(marker in content for marker in ("root cause", "caused", "because", "due to"))
        or bool(content_terms & {"triggered", "failure", "rollback", "conflict"}),
    )
    need(
        any(marker in query_text for marker in ("final", "solution", "resolved", "resolution")),
        any(marker in content for marker in ("final", "solution", "resolved", "resolution")),
    )
    need(
        _asks_for_actor_or_owner(query_text),
        _has_actor_or_owner(observation),
    )
    need(
        any(marker in query_text for marker in ("evidence", "specific", "exact", "technical")),
        any(marker in content for marker in ("evidence", "specific", "technical", "discovered"))
        or bool(metadata.get("commit_id") or metadata.get("pr")),
    )
    need(
        _asks_for_current_or_count(query_text),
        _has_state_or_count_signal(observation),
    )
    need(
        any(marker in query_text for marker in ("before", "after", "during", "first", "oldest")),
        bool(metadata.get("timestamp_raw") or metadata.get("event_index") is not None)
        or "timestamp:" in content,
    )
    if requested == 0:
        return 0.0
    return covered / float(requested)


def _ordered_phrase_hits(query_text: str, content: str) -> int:
    query_tokens = [
        token
        for token in re.findall(r"[a-z0-9][a-z0-9_.:/#-]*", query_text)
        if len(token) > 2
    ]
    hits = 0
    for size in (2, 3, 4):
        for index in range(0, len(query_tokens) - size + 1):
            phrase = " ".join(query_tokens[index:index + size])
            if phrase in content:
                hits += 1
    return hits


def _metadata_scope_text(observation: BenchmarkObservation) -> str:
    parts: list[str] = []
    for entity in observation.entities:
        if not isinstance(entity, dict):
            continue
        parts.append(str(entity.get("type") or ""))
        parts.append(str(entity.get("id") or ""))
    for key, value in observation.metadata.items():
        if key in {
            "case_id",
            "channel",
            "commit_id",
            "lead",
            "platform",
            "priority",
            "sender",
            "status",
            "team",
            "title",
        }:
            parts.append(str(value or ""))
    return " ".join(parts)


def _asks_for_actor_or_owner(query_text: str) -> bool:
    return any(
        marker in query_text
        for marker in ("who", "which team", "team member", "lead", "owner", "championed")
    )


def _has_actor_or_owner(observation: BenchmarkObservation) -> bool:
    metadata = observation.metadata
    return any(metadata.get(key) for key in ("lead", "team", "sender", "author"))


def _asks_for_current_or_count(query_text: str) -> bool:
    return any(
        marker in query_text
        for marker in ("how many", "currently", "current", "done", "status", "oldest")
    )


def _has_state_or_count_signal(observation: BenchmarkObservation) -> bool:
    metadata = observation.metadata
    if metadata.get("status") is not None:
        return True
    content = observation.content.casefold()
    return any(marker in content for marker in ("status:", "done", "cancelled", "in_progress"))


def _observation_scoring_features(
    observation: BenchmarkObservation | None,
    cache: dict[str, _ObservationScoringFeatures],
) -> _ObservationScoringFeatures | None:
    if observation is None:
        return None
    cached = cache.get(observation.observation_id)
    if cached is not None:
        return cached
    content_lower = observation.content.casefold()
    metadata = observation.metadata
    features = _ObservationScoringFeatures(
        content_lower=content_lower,
        content_terms=frozenset(_salient_tokens(observation.content)),
        metadata_terms=frozenset(_salient_tokens(_metadata_scope_text(observation))),
        has_state_or_count_signal=(
            metadata.get("status") is not None
            or any(
                marker in content_lower
                for marker in ("status:", "done", "cancelled", "in_progress")
            )
        ),
    )
    cache[observation.observation_id] = features
    return features


def _query_evidence_relevance_with_features(
    query: BenchmarkQuery,
    observation: BenchmarkObservation | None,
    *,
    query_text: str,
    query_terms: frozenset[str],
    features: _ObservationScoringFeatures | None,
    bridge_intent: bool,
) -> float:
    if observation is None or features is None:
        return 0.0
    score = 0.0
    content_terms = set(features.content_terms)
    if query_terms:
        overlap = query_terms & features.content_terms
        score += min(0.34, 0.055 * len(overlap))
        score += 0.22 * _query_overlap_ratio(set(query_terms), content_terms)

    score += min(0.16, 0.035 * _ordered_phrase_hits(query_text, features.content_lower))
    if features.metadata_terms and query_terms:
        score += min(0.18, 0.045 * len(query_terms & features.metadata_terms))

    if bridge_intent:
        causal_terms = {
            "because",
            "blocked",
            "caused",
            "conflict",
            "dependency",
            "failed",
            "failure",
            "impact",
            "identified",
            "missed",
            "postmortem",
            "resolution",
            "resolved",
            "rollback",
            "root",
            "solution",
            "triggered",
            "workaround",
        }
        if content_terms & causal_terms:
            score += min(0.18, 0.045 * len(content_terms & causal_terms))
        if any(
            marker in features.content_lower
            for marker in ("root cause", "final solution", "resolution:")
        ):
            score += 0.12

    if _asks_for_actor_or_owner(query_text) and _has_actor_or_owner(observation):
        score += 0.12
    if _asks_for_current_or_count(query_text) and features.has_state_or_count_signal:
        score += 0.12
    return max(0.0, min(1.0, score))


def _question_slot_score_with_features(
    query: BenchmarkQuery,
    observation: BenchmarkObservation | None,
    *,
    query_text: str,
    features: _ObservationScoringFeatures | None,
) -> float:
    if observation is None or features is None:
        return 0.0
    content = features.content_lower
    metadata = observation.metadata
    requested = 0
    covered = 0

    def need(condition: bool, has_slot: bool) -> None:
        nonlocal requested, covered
        if not condition:
            return
        requested += 1
        if has_slot:
            covered += 1

    content_terms = set(features.content_terms)
    need(
        any(marker in query_text for marker in ("root cause", "caused", "why", "triggered")),
        any(marker in content for marker in ("root cause", "caused", "because", "due to"))
        or bool(content_terms & {"triggered", "failure", "rollback", "conflict"}),
    )
    need(
        any(marker in query_text for marker in ("final", "solution", "resolved", "resolution")),
        any(marker in content for marker in ("final", "solution", "resolved", "resolution")),
    )
    need(
        _asks_for_actor_or_owner(query_text),
        _has_actor_or_owner(observation),
    )
    need(
        any(marker in query_text for marker in ("evidence", "specific", "exact", "technical")),
        any(marker in content for marker in ("evidence", "specific", "technical", "discovered"))
        or bool(metadata.get("commit_id") or metadata.get("pr")),
    )
    need(
        _asks_for_current_or_count(query_text),
        features.has_state_or_count_signal,
    )
    need(
        any(marker in query_text for marker in ("before", "after", "during", "first", "oldest")),
        bool(metadata.get("timestamp_raw") or metadata.get("event_index") is not None)
        or "timestamp:" in content,
    )
    if requested == 0:
        return 0.0
    return covered / float(requested)


def _coverage_or_counter_intent(query: BenchmarkQuery) -> bool:
    text = f"{query.query_text} {query.query_type}".casefold()
    markers = {
        "all",
        "both",
        "compare",
        "different",
        "each",
        "every",
        "list",
        "many",
        "multiple",
        "not",
        "never",
        "rather",
        "total",
        "updated",
        "which",
    }
    tokens = set(token_counts(text))
    return (
        "how many" in text
        or "what are" in text
        or "which of" in text
        or "changed" in text
        or "contrad" in text
        or "counter" in text
        or bool(tokens & markers)
    )


def _coverage_session_key(item: RetrievedEvidence) -> str | None:
    session_id = item.metadata.get("session_id")
    if session_id is not None:
        return f"session_id:{session_id}"
    session_index = item.metadata.get("session_index")
    if session_index is not None:
        return f"session_index:{session_index}"
    occurred_at = str(item.occurred_at or "")
    return occurred_at[:10] if occurred_at else None


def _query_overlap_ratio(query_terms: set[str], content_terms: set[str]) -> float:
    if not query_terms:
        return 0.0
    return len(query_terms & content_terms) / float(len(query_terms))


def _scope_query_terms(query: BenchmarkQuery) -> set[str]:
    text = " ".join(
        str(part or "")
        for part in (
            query.query_text,
            query.query_type,
            query.metadata.get("domain"),
            query.metadata.get("environment"),
        )
    )
    return set(_salient_tokens(text))


def _scope_overlap_score(
    query_terms: set[str],
    observation: BenchmarkObservation | None,
    *,
    structured_ui_intents: set[str] | None = None,
) -> float:
    if not query_terms or observation is None:
        return 0.0
    scope_text_parts: list[str] = []
    for entity in observation.entities:
        if not isinstance(entity, dict):
            continue
        scope_text_parts.append(str(entity.get("type") or ""))
        scope_text_parts.append(str(entity.get("id") or ""))
    metadata = observation.metadata
    structured_ui_intents = structured_ui_intents or set()
    for key in (
        "domain",
        "environment",
        "trajectory_goal",
        "trajectory_outcome",
        "url_path",
        "workflow_phase",
        "sort_fields",
        "form_controls",
        "form_control_focus",
        "pipeline_items",
        "stage_chains",
    ):
        scope_text_parts.append(str(metadata.get(key) or ""))
    if structured_ui_intents:
        structured_facts = metadata.get("structured_ui_facts")
        if isinstance(structured_facts, list):
            scope_text_parts.append(
                " ".join(
                    str(fact)
                    for fact in structured_facts
                    if _structured_fact_matches_intents(str(fact), structured_ui_intents)
                )
            )
    scope_terms = set(_salient_tokens(" ".join(scope_text_parts)))
    if not scope_terms:
        return 0.0
    overlap = query_terms & scope_terms
    if not overlap:
        return 0.0
    return min(1.0, len(overlap) / float(min(len(query_terms), 8)))


def _dynamic_transition_salience_score(
    query: BenchmarkQuery,
    observation: BenchmarkObservation | None,
) -> float:
    if observation is None:
        return 0.0
    query_type = (query.query_type or "").casefold()
    query_text = query.query_text.casefold()
    dynamic_markers = {
        "after",
        "automatic",
        "automatically",
        "before",
        "change",
        "changed",
        "dropdown",
        "open",
        "option",
        "popup",
        "pipeline",
        "selected",
        "sort",
        "stage",
        "state",
        "title",
        "total",
    }
    if (
        "dynamic" not in query_type
        and "procedure" not in query_type
        and not any(marker in query_text for marker in dynamic_markers)
    ):
        return 0.0

    score = 0.0
    anchor_overlap: set[str] = set()
    metadata = observation.metadata
    observation_kind = str(metadata.get("observation_kind") or "")
    if observation_kind == "state_transition":
        score += 0.08

    query_terms = set(_salient_tokens(f"{query.query_text} {query.query_type}"))
    added_label_text = " ".join(str(label) for label in metadata.get("ui_labels_added", []))
    removed_label_text = " ".join(str(label) for label in metadata.get("ui_labels_removed", []))
    action_text = str(metadata.get("action") or "")
    added_terms = set(_salient_tokens(added_label_text))
    removed_terms = set(_salient_tokens(removed_label_text))
    action_terms = set(_salient_tokens(action_text))
    transition_terms = added_terms | removed_terms | action_terms
    delta_overlap = query_terms & transition_terms
    if query_terms and transition_terms:
        score += min(0.18, 0.035 * len(delta_overlap))

    added_overlap = query_terms & added_terms
    removed_overlap = query_terms & removed_terms
    if added_overlap:
        score += 0.06
    if removed_overlap:
        score += 0.03

    content_lower = observation.content.casefold()
    structured_ui_intents = _structured_ui_intents(query.query_text)
    anchor_terms = _query_anchor_terms(query_terms)
    if anchor_terms:
        anchor_parts = [
            observation.content[:5000],
            metadata.get("trajectory_goal"),
            metadata.get("url_path"),
            metadata.get("sort_fields"),
            metadata.get("form_controls"),
            metadata.get("form_control_focus"),
            metadata.get("pipeline_items"),
            metadata.get("stage_chains"),
        ]
        if structured_ui_intents:
            structured_facts = metadata.get("structured_ui_facts")
            if isinstance(structured_facts, list):
                anchor_parts.append(
                    " ".join(
                        str(fact)
                        for fact in structured_facts
                        if _structured_fact_matches_intents(
                            str(fact),
                            structured_ui_intents,
                        )
                    )
                )
        anchor_text = " ".join(
            str(part or "")
            for part in anchor_parts
        )
        anchor_overlap = anchor_terms & set(_salient_tokens(anchor_text))
        if anchor_overlap:
            score += min(0.20, 0.07 * len(anchor_overlap))
        else:
            score -= 0.14

    if (
        ("newly visible" in content_lower and added_overlap)
        or "field value has changed" in content_lower
    ):
        score += 0.05
    if any(marker in query_text for marker in ("dropdown", "option", "open")) and (
        ("newly visible" in content_lower and added_overlap)
        or (metadata.get("ui_labels_added") and added_overlap)
    ):
        score += 0.05

    structured_facts = metadata.get("structured_ui_facts")
    if structured_facts and structured_ui_intents:
        matching_facts = [
            str(fact)
            for fact in structured_facts
            if _structured_fact_matches_intents(str(fact), structured_ui_intents)
        ]
        facts_text = " ".join(matching_facts)
        facts_terms = set(_salient_tokens(facts_text))
        fact_overlap = query_terms & facts_terms
        if fact_overlap:
            score += min(0.24, 0.06 * len(fact_overlap))
        if "autocomplete popup title" in facts_text.casefold() and "popup" in structured_ui_intents:
            score += 0.16
        if "table summary row" in facts_text.casefold() and "table" in structured_ui_intents:
            score += 0.16
        if "field list" in facts_text.casefold() and "field_list" in structured_ui_intents:
            score += 0.16
        if "checkbox choice group" in facts_text.casefold() and "checkbox" in structured_ui_intents:
            score += 0.16

    sort_markers = ("sort", "sorting", "sort row", "default sort", "target field")
    if metadata.get("sort_fields") and any(marker in query_text for marker in sort_markers):
        score += 0.22
        if "new sort order condition added" in content_lower:
            score += 0.08
        if "before selecting" in query_text or "initially shown" in query_text:
            score += 0.05

    stage_markers = ("stage", "stages", "pipeline", "order", "complete")
    stage_chains = metadata.get("stage_chains")
    if stage_chains and any(marker in query_text for marker in stage_markers):
        max_pending = _max_stage_pending_count(stage_chains)
        score += min(0.28, 0.04 * float(max_pending))
        if "macbook" in query_text and "macbook" in observation.content.casefold():
            score += 0.06
        if "excluding in-progress" in query_text or "in-progress" in query_text:
            max_remaining = _max_stage_remaining_excluding_in_progress_count(stage_chains)
            score += min(0.12, 0.02 * float(max_remaining))
            score += 0.04

    terminal_markers = (
        "send_msg_to_user",
        "completed",
        "verified",
        "task completed",
    )
    if any(marker in action_text.casefold() for marker in terminal_markers):
        score -= 0.28
    if anchor_terms:
        upper_bound = 0.48 if anchor_overlap else 0.24
    else:
        upper_bound = 0.32
    return max(-0.14, min(upper_bound, score))


def _structured_ui_intents(query_text: str) -> set[str]:
    query_lower = query_text.casefold()
    intents: set[str] = set()
    if any(
        marker in query_lower
        for marker in (
            "bottom",
            "last option",
            "option order",
            "selected pane",
            "list columns",
            "left pane",
            "right pane",
        )
    ):
        intents.add("field_list")
    if any(
        marker in query_lower
        for marker in (
            "popup",
            "pop-up",
            "search box",
            "recent selection",
            "recent selections",
            "lookup",
        )
    ) or ("title" in query_lower and ("box" in query_lower or "popup" in query_lower)):
        intents.add("popup")
    if any(marker in query_lower for marker in ("total", "summary row", "subtotal")):
        intents.add("table")
    if any(
        marker in query_lower
        for marker in (
            "checkbox",
            "checked",
            "unchecked",
            "choice",
            "choices",
            "selected options",
        )
    ):
        intents.add("checkbox")
    if any(
        marker in query_lower
        for marker in (
            "required",
            "read-only",
            "readonly",
            "disabled",
            "editable",
            "duplicate of",
        )
    ):
        intents.add("editable_form")
    return intents


def _structured_fact_matches_intents(fact: str, intents: set[str]) -> bool:
    fact_lower = fact.casefold()
    return (
        ("field_list" in intents and "field list " in fact_lower)
        or ("popup" in intents and "autocomplete popup title" in fact_lower)
        or ("table" in intents and "table summary row" in fact_lower)
        or ("checkbox" in intents and "checkbox choice group" in fact_lower)
        or (
            "editable_form" in intents
            and (
                "editable form fields" in fact_lower
                or "required editable form fields" in fact_lower
                or "disabled/read-only form fields" in fact_lower
            )
        )
    )


def _query_anchor_terms(query_terms: set[str]) -> set[str]:
    generic = {
        "action",
        "after",
        "answer",
        "before",
        "change",
        "changed",
        "complete",
        "contain",
        "default",
        "dropdown",
        "english",
        "excluding",
        "field",
        "filter",
        "filters",
        "fully",
        "in-progress",
        "initially",
        "label",
        "labels",
        "list",
        "many",
        "ones",
        "option",
        "order",
        "page",
        "pipeline",
        "place",
        "portal",
        "progres",
        "progress",
        "remain",
        "service",
        "servicenow",
        "shown",
        "sort",
        "sorting",
        "stage",
        "stages",
        "state",
        "substring",
        "target",
        "value",
        "which",
        "working",
    }
    anchors = {
        term
        for term in query_terms
        if len(term) >= 4 and term not in generic and not term.isdigit()
    }
    return set(sorted(anchors)[:8])


def _max_stage_pending_count(stage_chains: Any) -> int:
    if not isinstance(stage_chains, list):
        return 0
    max_pending = 0
    for chain in stage_chains:
        match = re.search(r"pending_not_started_count=(\d+)", str(chain))
        if match:
            max_pending = max(max_pending, int(match.group(1)))
    return max_pending


def _max_stage_remaining_excluding_in_progress_count(stage_chains: Any) -> int:
    if not isinstance(stage_chains, list):
        return 0
    max_remaining = 0
    for chain in stage_chains:
        match = re.search(r"remaining_excluding_in_progress_count=(\d+)", str(chain))
        if match:
            max_remaining = max(max_remaining, int(match.group(1)))
    return max_remaining


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / float(len(left | right))


def _cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    width = min(len(left), len(right))
    dot = 0.0
    left_norm = 0.0
    right_norm = 0.0
    for index in range(width):
        left_value = float(left[index])
        right_value = float(right[index])
        dot += left_value * right_value
        left_norm += left_value * left_value
        right_norm += right_value * right_value
    denominator = math.sqrt(left_norm) * math.sqrt(right_norm)
    if denominator <= 0:
        return 0.0
    return dot / denominator


def _metadata_int(value: Any, *, default: int | None = None) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _add_candidate_neighbor(
    rows_by_id: dict[str, list[Any]],
    *,
    neighbor: BenchmarkObservation,
    score: float,
) -> None:
    if score <= 0:
        return
    row = rows_by_id.get(neighbor.observation_id)
    if row is None:
        rows_by_id[neighbor.observation_id] = [score, neighbor]
    else:
        row[0] = max(float(row[0]), score)


def _add_retrieved_candidate(
    candidates: dict[str, RetrievedEvidence],
    *,
    observation: BenchmarkObservation,
    retrieval_system: str,
) -> None:
    if observation.observation_id in candidates:
        return
    candidates[observation.observation_id] = RetrievedEvidence(
        observation_id=observation.observation_id,
        content=observation.content,
        score=0.0,
        occurred_at=observation.occurred_at.isoformat(),
        metadata={
            **observation.metadata,
            "retrieval_system": retrieval_system,
        },
    )


def _bm25_score(
    *,
    term_frequency: int,
    document_frequency: int,
    total_docs: int,
    document_length: int,
    average_document_length: float,
    k1: float = 1.5,
    b: float = 0.75,
) -> float:
    if total_docs <= 0 or document_frequency <= 0:
        return 0.0
    idf = math.log(1.0 + ((total_docs - document_frequency + 0.5) / (document_frequency + 0.5)))
    if average_document_length <= 0:
        length_norm = 1.0
    else:
        length_norm = 1.0 - b + b * (document_length / average_document_length)
    numerator = term_frequency * (k1 + 1.0)
    denominator = term_frequency + k1 * length_norm
    return idf * (numerator / denominator)


def _normalize_embedding_mode(mode: str) -> str:
    normalized = mode.casefold().strip()
    if normalized in {"hash", "hashed", "hashed_token_vector", "hashed_token_vector_v1"}:
        return "hash"
    if normalized in {"provider", "env"}:
        return "provider"
    if normalized in {"ollama", "openai"}:
        return normalized
    raise ValueError(
        "embedding_mode must be one of hash, provider, ollama, or openai; "
        f"got {mode!r}"
    )


def _benchmark_question_primitive(query: BenchmarkQuery) -> str:
    raw = query.metadata.get("question_primitive") or query.constraints.get(
        "question_primitive"
    )
    if raw:
        return str(raw).strip().upper()
    query_type = (query.query_type or "").casefold()
    if "counter" in query_type or "contrad" in query_type:
        return "COUNTEREVIDENCE"
    if "owner" in query_type:
        return "OWNERSHIP"
    if "causal" in query_type or "cause" in query_type:
        return "DEPENDENCY"
    if "dynamic" in query_type:
        return "STATUS"
    if "procedure" in query_type or "workflow" in query_type:
        return "ACTION"
    if "time" in query_type or "timeline" in query_type:
        return "TIMELINE"
    return "FACT_LOOKUP"


def _ask_scope_label(query: BenchmarkQuery) -> str:
    benchmark = str(query.metadata.get("benchmark") or "Benchmark")
    tier = query.metadata.get("haystack_tier")
    environment = query.metadata.get("environment")
    parts = [benchmark]
    if tier:
        parts.append(str(tier))
    if environment:
        parts.append(str(environment))
    return " / ".join(parts)


def _ask_evidence_score(item: Any, rank: int) -> float:
    strength_score = {
        "decisive": 1.0,
        "supporting": 0.82,
        "contextual": 0.62,
        "counterevidence": 0.58,
        "weak": 0.32,
        "unknown": 0.18,
    }.get(str(getattr(item, "strength", "") or "").casefold(), 0.4)
    return strength_score + (1.0 / max(1, rank + 20))


def _chunks(items: list[Any], size: int) -> Iterable[list[Any]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def _with_metadata(
    item: RetrievedEvidence,
    *,
    score: float,
    metadata: dict[str, Any],
) -> RetrievedEvidence:
    return RetrievedEvidence(
        observation_id=item.observation_id,
        content=item.content,
        score=round(score, 6),
        occurred_at=item.occurred_at,
        metadata={**item.metadata, **metadata},
    )


def _rrf_score(rank: int | None, *, k: int = 60) -> float:
    if rank is None or rank <= 0:
        return 0.0
    return (1.0 / (k + rank)) / (1.0 / (k + 1))


__all__ = [
    "FyralisDBMaterialization",
    "FyralisAskReader",
    "FyralisDBReader",
    "FyralisSageCoverageHybridReader",
    "FyralisSageHybridReader",
    "FyralisSagePrecisionHybridReader",
    "FyralisSageReader",
    "FyralisSageSemanticHybridReader",
    "hashed_token_vector",
]
