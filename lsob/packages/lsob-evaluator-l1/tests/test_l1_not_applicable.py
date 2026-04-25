"""Verifies the composite evaluator's fallback when the SUT lacks retrieval."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from lsob_contracts import (
    Corpus,
    CorpusMeta,
    EvaluationContext,
    GroundTruth,
)

from lsob_evaluator_l1 import LayerOneEvaluator, MockNonRetrievalSUT


def _tiny_corpus() -> Corpus:
    ts = datetime(2026, 1, 31, tzinfo=timezone.utc)
    meta = CorpusMeta(
        corpus_id="na",
        company_id="c",
        months_simulated=1,
        seed=1,
        config_hash="h",
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_date=ts,
    )
    gt = GroundTruth(
        timestamp=ts,
        commitments=[{"id": "C-a", "owner": "x"}],
        customers=[],
    )
    return Corpus(meta=meta, signals=[], ground_truth=[gt])


@pytest.mark.asyncio
async def test_non_retrieval_sut_emits_layer_not_applicable():
    corpus = _tiny_corpus()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=MockNonRetrievalSUT(),
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="na-1",
    )
    results = await LayerOneEvaluator().evaluate(ctx)
    assert len(results) == 1
    (only,) = results
    assert only.layer_id == 1
    assert only.metric_name == "layer_not_applicable"
    assert only.value == 0.0
    assert only.confidence_interval is None
    assert only.breakdown_by["sut_type"] == "MockNonRetrievalSUT"
