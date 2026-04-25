"""Unit tests for RerankerEvaluator."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from lsob_contracts import Corpus, CorpusMeta, EvaluationContext, GroundTruth

from lsob_evaluator_l1.reranker import RerankerEvaluator


def _corpus_two_items() -> Corpus:
    ts = datetime(2026, 1, 31, tzinfo=timezone.utc)
    meta = CorpusMeta(
        corpus_id="rr",
        company_id="c",
        months_simulated=1,
        seed=1,
        config_hash="h",
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_date=ts,
    )
    gt = GroundTruth(
        timestamp=ts,
        commitments=[{"id": "C-a", "owner": "x"}, {"id": "C-b", "owner": "y"}],
        customers=[{"id": "acme"}],
    )
    return Corpus(meta=meta, signals=[], ground_truth=[gt])


class _OracleReranker:
    """Returns items sorted back into gold order (commitment before customer)."""

    name = "oracle"
    max_concurrent_ingestion = 1

    async def retrieval_semantic(self, q, k):  # pragma: no cover
        return []

    async def retrieval_entity_resolve(self, p, a):  # pragma: no cover
        return None

    async def retrieval_rerank(self, items, query):
        # Gold order is commitment ids first in declaration order, then customers.
        priority = {"commitment": 0, "customer": 1, "owner": 2}
        return sorted(items, key=lambda x: (priority.get(x.split(":")[1], 3), x))


class _ReverseReranker:
    name = "reverse"
    max_concurrent_ingestion = 1

    async def retrieval_semantic(self, q, k):  # pragma: no cover
        return []

    async def retrieval_entity_resolve(self, p, a):  # pragma: no cover
        return None

    async def retrieval_rerank(self, items, query):
        return list(reversed(items))


@pytest.mark.asyncio
async def test_reranker_oracle_hits_ceiling():
    corpus = _corpus_two_items()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=_OracleReranker(),
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="t",
    )
    results = {r.metric_name: r.value for r in await RerankerEvaluator().evaluate(ctx)}
    assert results["reranker_ndcg_at_10"] == pytest.approx(1.0)
    assert results["reranker_kendall_tau"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_reranker_reverse_recovers_input_order():
    # Our probe's candidates are already the reversed gold order, so reversing
    # them *again* recovers gold order → tau == 1.0.
    corpus = _corpus_two_items()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=_ReverseReranker(),
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="t",
    )
    results = {r.metric_name: r.value for r in await RerankerEvaluator().evaluate(ctx)}
    assert results["reranker_kendall_tau"] == pytest.approx(1.0)
    assert results["reranker_ndcg_at_10"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_reranker_identity_returns_reversed_tau():
    # Identity leaves the candidates reversed vs gold → tau == -1.0.
    class _Identity:
        name = "id"
        max_concurrent_ingestion = 1

        async def retrieval_semantic(self, q, k):  # pragma: no cover
            return []

        async def retrieval_entity_resolve(self, p, a):  # pragma: no cover
            return None

        async def retrieval_rerank(self, items, query):
            return list(items)

    corpus = _corpus_two_items()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=_Identity(),
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="t",
    )
    results = {r.metric_name: r.value for r in await RerankerEvaluator().evaluate(ctx)}
    assert results["reranker_kendall_tau"] == pytest.approx(-1.0)
