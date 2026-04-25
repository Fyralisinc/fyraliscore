"""Integration test — Layer 4 on fixtures/mini_corpus_a.json."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lsob_contracts import Corpus, EvaluationContext

from lsob_evaluator_l4.evaluator import LayerFourEvaluator
from lsob_evaluator_l4.mock_sut import (
    MockSurfacingSUT,
    make_commitment_at_risk,
    make_customer_at_risk,
)

_FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "fixtures"
    / "mini_corpus_a.json"
)


def _load_corpus() -> Corpus:
    return Corpus.model_validate(json.loads(_FIXTURE.read_text()))


@pytest.mark.asyncio
async def test_mini_corpus_a_perfect_surfacing() -> None:
    corpus = _load_corpus()
    checkpoint = corpus.ground_truth[0].timestamp
    sut = MockSurfacingSUT(
        canned_at_risk={
            checkpoint: [
                make_commitment_at_risk("C-ingest"),
                make_customer_at_risk("acme"),
            ]
        },
    )
    evaluator = LayerFourEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=checkpoint,
        run_id="mini-a-run",
    )
    results = {r.metric_name: r for r in await evaluator.evaluate(ctx)}
    assert results["at_risk_commitment_precision"].value == pytest.approx(1.0)
    assert results["at_risk_commitment_recall"].value == pytest.approx(1.0)
    assert results["at_risk_commitment_f1"].value == pytest.approx(1.0)
    assert results["customer_risk_precision"].value == pytest.approx(1.0)
    assert results["customer_risk_recall"].value == pytest.approx(1.0)
    assert results["customer_risk_f1"].value == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_mini_corpus_a_spurious_surfacing_hurts_precision() -> None:
    corpus = _load_corpus()
    checkpoint = corpus.ground_truth[0].timestamp
    sut = MockSurfacingSUT(
        canned_at_risk={
            checkpoint: [
                make_commitment_at_risk("C-ingest"),
                make_commitment_at_risk("C-ghost"),  # spurious
                make_customer_at_risk("acme"),
            ]
        },
    )
    evaluator = LayerFourEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=checkpoint,
        run_id="mini-a-run",
    )
    results = {r.metric_name: r for r in await evaluator.evaluate(ctx)}
    # 1 tp, 1 fp, 0 fn → P=0.5, R=1.0, F1=2/3
    assert results["at_risk_commitment_precision"].value == pytest.approx(0.5)
    assert results["at_risk_commitment_recall"].value == pytest.approx(1.0)
    assert results["at_risk_commitment_f1"].value == pytest.approx(2 / 3)
