"""Unit tests for EntityResolutionEvaluator."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from lsob_contracts import (
    Corpus,
    CorpusMeta,
    EvaluationContext,
    Signal,
    SourceChannel,
)

from lsob_evaluator_l1.entity_resolution import EntityResolutionEvaluator


def _corpus_with_signals(signals: list[Signal]) -> Corpus:
    ts = datetime(2026, 1, 31, tzinfo=timezone.utc)
    meta = CorpusMeta(
        corpus_id="er",
        company_id="c",
        months_simulated=1,
        seed=1,
        config_hash="h",
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_date=ts,
    )
    return Corpus(meta=meta, signals=signals, ground_truth=[])


def _sig(sid: str, text: str, meta: dict, ts_day: int = 2) -> Signal:
    return Signal(
        signal_id=sid,
        source_channel=SourceChannel.slack,
        author_id="alice",
        content_text=text,
        timestamp=datetime(2026, 1, ts_day, tzinfo=timezone.utc),
        metadata=meta,
    )


class _OracleResolver:
    """Returns whatever the signal metadata says."""

    name = "oracle"
    max_concurrent_ingestion = 1

    def __init__(self, lookup: dict[str, str | None]) -> None:
        self._lookup = lookup

    async def retrieval_semantic(self, query, k):  # pragma: no cover
        return []

    async def retrieval_entity_resolve(self, phrase, author_id):
        return self._lookup.get(phrase)

    async def retrieval_rerank(self, items, query):  # pragma: no cover
        return items


class _AlwaysWrongResolver:
    name = "wrong"
    max_concurrent_ingestion = 1

    async def retrieval_semantic(self, query, k):  # pragma: no cover
        return []

    async def retrieval_entity_resolve(self, phrase, author_id):
        return "WRONG"

    async def retrieval_rerank(self, items, query):  # pragma: no cover
        return items


@pytest.mark.asyncio
async def test_entity_oracle_is_perfect():
    # 2 resolvable + 1 unresolvable => accuracy 1.0, precision 1.0, recall 1.0.
    sigs = [
        _sig("a", "about commitment foo", {"commitment_ref": "C-foo"}),
        _sig("b", "about customer acme", {"customer_ref": "acme"}),
        _sig("c", "random chatter", {}),
    ]
    corpus = _corpus_with_signals(sigs)
    lookup = {
        "about commitment foo": "C-foo",
        "about customer acme": "acme",
        "random chatter": None,
    }
    ctx = EvaluationContext(
        corpus=corpus,
        sut=_OracleResolver(lookup),
        ground_truth_checkpoint=datetime(2026, 1, 31, tzinfo=timezone.utc),
        run_id="t",
    )
    results = await EntityResolutionEvaluator().evaluate(ctx)
    by_name = {r.metric_name: r.value for r in results}
    assert by_name["entity_resolution_accuracy"] == pytest.approx(1.0)
    assert by_name["entity_resolution_precision"] == pytest.approx(1.0)
    assert by_name["entity_resolution_recall"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_entity_always_wrong_drops_all_metrics():
    sigs = [
        _sig("a", "about commitment foo", {"commitment_ref": "C-foo"}),
        _sig("b", "about customer acme", {"customer_ref": "acme"}),
    ]
    corpus = _corpus_with_signals(sigs)
    ctx = EvaluationContext(
        corpus=corpus,
        sut=_AlwaysWrongResolver(),
        ground_truth_checkpoint=datetime(2026, 1, 31, tzinfo=timezone.utc),
        run_id="t",
    )
    results = await EntityResolutionEvaluator().evaluate(ctx)
    for r in results:
        assert r.value == 0.0


@pytest.mark.asyncio
async def test_entity_precision_recall_mixed_case():
    # 4 probes: 2 correct positives, 1 false positive, 1 false negative.
    # precision = 2 / (2+1) = 2/3
    # recall    = 2 / (2+1) = 2/3
    # accuracy  = 2 / 4    = 0.5
    sigs = [
        _sig("tp1", "about C-a", {"commitment_ref": "C-a"}),
        _sig("tp2", "about C-b", {"commitment_ref": "C-b"}),
        _sig("fp", "ambiguous talk", {}),          # gold None, guess non-None
        _sig("fn", "about C-c", {"commitment_ref": "C-c"}),  # gold non-None, guess None
    ]
    corpus = _corpus_with_signals(sigs)
    lookup = {
        "about C-a": "C-a",
        "about C-b": "C-b",
        "ambiguous talk": "C-wrong",
        "about C-c": None,
    }
    ctx = EvaluationContext(
        corpus=corpus,
        sut=_OracleResolver(lookup),
        ground_truth_checkpoint=datetime(2026, 1, 31, tzinfo=timezone.utc),
        run_id="t",
    )
    results = {r.metric_name: r.value for r in await EntityResolutionEvaluator().evaluate(ctx)}
    assert results["entity_resolution_precision"] == pytest.approx(2 / 3)
    assert results["entity_resolution_recall"] == pytest.approx(2 / 3)
    assert results["entity_resolution_accuracy"] == pytest.approx(0.5)
