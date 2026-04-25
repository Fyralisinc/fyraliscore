"""Layer 4 sub-evaluation 4 — alert-fatigue ratio."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lsob_contracts import Corpus, CorpusMeta, EvaluationContext, GroundTruth

from lsob_evaluator_l4.evaluator import LayerFourEvaluator
from lsob_evaluator_l4.mock_sut import MockSurfacingSUT

UTC = timezone.utc


def _two_month_corpus() -> Corpus:
    return Corpus(
        meta=CorpusMeta(
            corpus_id="t",
            company_id="t",
            months_simulated=2,
            seed=1,
            config_hash="h",
            start_date=datetime(2026, 1, 1, tzinfo=UTC),
            end_date=datetime(2026, 2, 28, tzinfo=UTC),
        ),
        signals=[],
        ground_truth=[
            GroundTruth(
                timestamp=datetime(2026, 2, 28, tzinfo=UTC),
                patterns=[
                    {
                        "id": "P-jan",
                        "description": "x",
                        "detection_eligible_after": "2026-01-10T00:00:00Z",
                    },
                    {
                        "id": "P-feb",
                        "description": "x",
                        "detection_eligible_after": "2026-02-15T00:00:00Z",
                    },
                ],
            )
        ],
    )


@pytest.mark.asyncio
async def test_fatigue_one_to_one() -> None:
    corpus = _two_month_corpus()
    sut = MockSurfacingSUT(
        canned_anomalies=[
            {"timestamp": datetime(2026, 1, 10, tzinfo=UTC), "kind": "k"},
            {"timestamp": datetime(2026, 2, 15, tzinfo=UTC), "kind": "k"},
        ],
    )
    evaluator = LayerFourEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.meta.end_date,
        run_id="run",
    )
    results = {r.metric_name: r for r in await evaluator.evaluate(ctx)}
    fatigue = results["alert_fatigue_ratio"]
    assert fatigue.value == pytest.approx(1.0)
    by_month = fatigue.breakdown_by["by_month"]
    assert by_month["2026-01"]["emitted"] == 1
    assert by_month["2026-01"]["genuine"] == 1
    assert by_month["2026-01"]["ratio"] == pytest.approx(1.0)
    assert by_month["2026-02"]["ratio"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_fatigue_noisy_month() -> None:
    corpus = _two_month_corpus()
    sut = MockSurfacingSUT(
        canned_anomalies=[
            {"timestamp": datetime(2026, 1, 2, tzinfo=UTC), "kind": "k"},
            {"timestamp": datetime(2026, 1, 5, tzinfo=UTC), "kind": "k"},
            {"timestamp": datetime(2026, 1, 20, tzinfo=UTC), "kind": "k"},
            {"timestamp": datetime(2026, 2, 15, tzinfo=UTC), "kind": "k"},
        ],
    )
    evaluator = LayerFourEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.meta.end_date,
        run_id="run",
    )
    results = {r.metric_name: r for r in await evaluator.evaluate(ctx)}
    fatigue = results["alert_fatigue_ratio"]
    # 4 emitted / 2 genuine = 2.0
    assert fatigue.value == pytest.approx(2.0)
    by_month = fatigue.breakdown_by["by_month"]
    assert by_month["2026-01"]["ratio"] == pytest.approx(3.0)
    assert by_month["2026-02"]["ratio"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_fatigue_no_emissions_returns_zero() -> None:
    corpus = _two_month_corpus()
    sut = MockSurfacingSUT(canned_anomalies=[])
    evaluator = LayerFourEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.meta.end_date,
        run_id="run",
    )
    results = {r.metric_name: r for r in await evaluator.evaluate(ctx)}
    assert results["alert_fatigue_ratio"].value == pytest.approx(0.0)
