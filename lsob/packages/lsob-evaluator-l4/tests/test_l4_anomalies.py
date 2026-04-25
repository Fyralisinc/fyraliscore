"""Layer 4 sub-evaluation 3 — anomaly precision."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lsob_contracts import Corpus, CorpusMeta, EvaluationContext, GroundTruth

from lsob_evaluator_l4.evaluator import LayerFourEvaluator
from lsob_evaluator_l4.mock_sut import MockSurfacingSUT

UTC = timezone.utc


def _corpus_with_pattern(pattern_at: datetime) -> Corpus:
    return Corpus(
        meta=CorpusMeta(
            corpus_id="t",
            company_id="t",
            months_simulated=1,
            seed=1,
            config_hash="h",
            start_date=datetime(2026, 1, 1, tzinfo=UTC),
            end_date=datetime(2026, 1, 31, tzinfo=UTC),
        ),
        signals=[],
        ground_truth=[
            GroundTruth(
                timestamp=datetime(2026, 1, 31, tzinfo=UTC),
                patterns=[
                    {
                        "id": "P1",
                        "description": "x",
                        "detection_eligible_after": pattern_at.isoformat(),
                    }
                ],
            )
        ],
    )


@pytest.mark.asyncio
async def test_all_anomalies_align_with_truth() -> None:
    pattern_at = datetime(2026, 1, 16, tzinfo=UTC)
    corpus = _corpus_with_pattern(pattern_at)
    sut = MockSurfacingSUT(
        canned_anomalies=[
            {"timestamp": datetime(2026, 1, 17, tzinfo=UTC), "kind": "k"},
            {"timestamp": datetime(2026, 1, 20, tzinfo=UTC), "kind": "k"},
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
    assert results["anomaly_precision"].value == pytest.approx(1.0)
    assert results["anomaly_precision"].breakdown_by["tp"] == 2
    assert results["anomaly_precision"].breakdown_by["fp"] == 0


@pytest.mark.asyncio
async def test_far_away_anomalies_are_false_positives() -> None:
    pattern_at = datetime(2026, 1, 16, tzinfo=UTC)
    corpus = _corpus_with_pattern(pattern_at)
    sut = MockSurfacingSUT(
        canned_anomalies=[
            {"timestamp": datetime(2026, 1, 17, tzinfo=UTC), "kind": "k"},  # TP
            {"timestamp": datetime(2026, 1, 1, tzinfo=UTC), "kind": "k"},  # FP
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
    assert results["anomaly_precision"].value == pytest.approx(0.5)
    assert results["anomaly_precision"].breakdown_by["tp"] == 1
    assert results["anomaly_precision"].breakdown_by["fp"] == 1


@pytest.mark.asyncio
async def test_no_emissions_produces_zero_precision() -> None:
    corpus = _corpus_with_pattern(datetime(2026, 1, 16, tzinfo=UTC))
    sut = MockSurfacingSUT(canned_anomalies=[])
    evaluator = LayerFourEvaluator()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.meta.end_date,
        run_id="run",
    )
    results = {r.metric_name: r for r in await evaluator.evaluate(ctx)}
    assert results["anomaly_precision"].value == 0.0


@pytest.mark.asyncio
async def test_anomalies_accept_iso_string_timestamps() -> None:
    corpus = _corpus_with_pattern(datetime(2026, 1, 16, tzinfo=UTC))
    sut = MockSurfacingSUT(
        canned_anomalies=[
            {"timestamp": datetime(2026, 1, 17, tzinfo=UTC), "kind": "k"},
        ],
    )

    # Swap timestamps to plain strings to make sure coercion works.
    sut.canned_anomalies = [
        {"timestamp": "2026-01-17T00:00:00+00:00", "kind": "k"}
    ]
    # Widen `emitted_anomalies` window by relaxing filter — the mock only
    # returns items with `start <= ts < end`, so we bypass with direct call.
    evaluator = LayerFourEvaluator()

    # Patch MockSurfacingSUT to return the anomaly ignoring string vs dt type.
    class _Pass(MockSurfacingSUT):
        async def emitted_anomalies(self, start, end):  # type: ignore[override]
            return list(self.canned_anomalies)

    sut2 = _Pass(
        canned_anomalies=[
            {"timestamp": "2026-01-17T00:00:00+00:00", "kind": "k"}
        ]
    )
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut2,
        ground_truth_checkpoint=corpus.meta.end_date,
        run_id="run",
    )
    results = {r.metric_name: r for r in await evaluator.evaluate(ctx)}
    assert results["anomaly_precision"].value == pytest.approx(1.0)
