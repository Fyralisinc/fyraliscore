"""Pattern precipitation latency sub-evaluation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from lsob_contracts import EvaluationContext, PatternTruth

from lsob_evaluator_l5.evaluator import LayerFiveEvaluator
from lsob_evaluator_l5.mock_sut import MockTemporalSUT

from conftest import build_corpus


async def test_pattern_latency_detects_months_until_first_belief() -> None:
    start = datetime(2024, 1, 15, tzinfo=timezone.utc)
    corpus = build_corpus(months=6, start=start)
    emergence = start  # month 0
    pattern = PatternTruth(
        pattern_id="pat-1",
        description="retention cliff",
        emergence_at=emergence,
        detection_eligible_after=emergence,
    )
    # Attach pattern to corpus via attribute setattr so the evaluator finds it.
    object.__setattr__(corpus, "pattern_truths", [pattern])

    # SUT "detects" in month 3 (90 days after start).
    sut = MockTemporalSUT(
        pattern_detected_at={"pat-1": start + timedelta(days=90)}
    )
    ctx = EvaluationContext(
        corpus=corpus, sut=sut, ground_truth_checkpoint=start, run_id="r"
    )
    results = await LayerFiveEvaluator()._pattern_precipitation_latency(ctx)
    mean_res = next(r for r in results if r.metric_name.endswith("_mean"))
    assert mean_res.value == 3.0
    assert mean_res.breakdown_by["n_detected"] == 1


async def test_pattern_latency_no_patterns_emits_fallback() -> None:
    corpus = build_corpus(months=3)
    ctx = EvaluationContext(
        corpus=corpus,
        sut=MockTemporalSUT(),
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="r",
    )
    results = await LayerFiveEvaluator()._pattern_precipitation_latency(ctx)
    assert results
    assert results[0].breakdown_by["reason"] == "no patterns in corpus"
