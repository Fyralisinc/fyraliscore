"""Unit tests for SemanticPathwayEvaluator with hand-crafted corpora."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from lsob_contracts import (
    Corpus,
    CorpusMeta,
    EvaluationContext,
    GroundTruth,
)

from lsob_evaluator_l1.semantic import SemanticPathwayEvaluator


class _OracleSUT:
    """Returns gold-relevant ids first, in order."""

    name = "oracle"
    max_concurrent_ingestion = 1

    async def retrieval_semantic(self, query: str, k: int) -> list[str]:
        # The probe asks about commitment C-x: return the matching model first.
        if "C-x" in query:
            return ["model:commitment:C-x", "model:owner:alice"][:k]
        if "C-y" in query:
            return ["model:commitment:C-y", "model:owner:bob"][:k]
        if "customer" in query:
            return ["model:customer:acme"][:k]
        return []

    async def retrieval_entity_resolve(self, phrase, author_id):  # pragma: no cover
        return None

    async def retrieval_rerank(self, items, query):  # pragma: no cover
        return items


class _EmptySUT:
    """Returns nothing — forces recall/MRR/nDCG = 0."""

    name = "empty"
    max_concurrent_ingestion = 1

    async def retrieval_semantic(self, query, k):
        return []

    async def retrieval_entity_resolve(self, phrase, author_id):  # pragma: no cover
        return None

    async def retrieval_rerank(self, items, query):  # pragma: no cover
        return items


def _tiny_corpus() -> Corpus:
    ts = datetime(2026, 1, 31, tzinfo=timezone.utc)
    meta = CorpusMeta(
        corpus_id="tiny",
        company_id="t",
        months_simulated=1,
        seed=1,
        config_hash="h",
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_date=ts,
    )
    gt = GroundTruth(
        timestamp=ts,
        commitments=[
            {"id": "C-x", "owner": "alice"},
            {"id": "C-y", "owner": "bob"},
        ],
        customers=[{"id": "acme"}],
    )
    return Corpus(meta=meta, signals=[], ground_truth=[gt])


@pytest.mark.asyncio
async def test_semantic_oracle_is_perfect():
    corpus = _tiny_corpus()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=_OracleSUT(),
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="t",
    )
    results = await SemanticPathwayEvaluator().evaluate(ctx)
    by_name = {r.metric_name: r for r in results}
    # Oracle lands the gold item at rank 1 for every probe.
    assert by_name["semantic_mrr"].value == 1.0
    assert by_name["semantic_recall_at_5"].value > 0.0
    assert by_name["semantic_ndcg_at_10"].value > 0.0
    # Breakdown should include per-month and per-proposition-kind keys.
    br = by_name["semantic_recall_at_10"].breakdown_by
    assert "by_month" in br and "by_proposition_kind" in br


@pytest.mark.asyncio
async def test_semantic_empty_sut_scores_zero():
    corpus = _tiny_corpus()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=_EmptySUT(),
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="t",
    )
    results = await SemanticPathwayEvaluator().evaluate(ctx)
    for r in results:
        assert r.value == 0.0
        # CI should be populated (deterministic bootstrap of zeros).
        assert r.confidence_interval == (0.0, 0.0)


@pytest.mark.asyncio
async def test_semantic_not_applicable_sut_returns_empty():
    class NoRetrieval:
        name = "no"
        max_concurrent_ingestion = 1

    corpus = _tiny_corpus()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=NoRetrieval(),
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="t",
    )
    results = await SemanticPathwayEvaluator().evaluate(ctx)
    assert results == []
