"""End-to-end Layer 3 test against fixtures/mini_corpus_a.json with MockCalibratedSUT."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from lsob_contracts import Corpus, EvalResult, EvaluationContext

from lsob_evaluator_l3.evaluator import LayerThreeEvaluator
from lsob_evaluator_l3.mock_sut import MockCalibratedSUT


FIXTURE = (
    Path(__file__).resolve().parents[3] / "fixtures" / "mini_corpus_a.json"
)


def _load_fixture() -> Corpus:
    with FIXTURE.open() as f:
        raw = json.load(f)
    return Corpus.model_validate(raw)


def _metric(results: list[EvalResult], name: str) -> EvalResult:
    for r in results:
        if r.metric_name == name:
            return r
    raise AssertionError(f"metric {name!r} missing from results: {[r.metric_name for r in results]}")


async def test_integration_mini_corpus_a_brier_is_0_49(tmp_path: Path):
    corpus = _load_fixture()
    preds = [p for gt in corpus.ground_truth for p in gt.predictions_that_will_resolve]
    sut = MockCalibratedSUT.from_predictions(preds, actor_id="alice")

    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="itest",
        extras={"output_dir": str(tmp_path)},
    )
    results = await LayerThreeEvaluator().evaluate(ctx)

    brier = _metric(results, "brier")
    assert brier.value == pytest.approx(0.49, abs=1e-9)

    # Reliability diagram PNG should be emitted to the configured output dir.
    png = tmp_path / "reliability_itest.png"
    assert png.exists()
    assert png.stat().st_size > 0

    not_made = _metric(results, "predictions_not_made")
    assert not_made.value == 0.0


async def test_integration_predictions_not_made_counted(tmp_path: Path):
    corpus = _load_fixture()
    # Empty SUT: no beliefs => every prediction is "not made".
    sut = MockCalibratedSUT()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="itest-empty",
        extras={"output_dir": str(tmp_path)},
    )
    results = await LayerThreeEvaluator().evaluate(ctx)
    assert _metric(results, "predictions_not_made").value == 1.0
    # With no resolved predictions, Brier defaults to 0.
    assert _metric(results, "brier").value == 0.0


async def test_integration_per_actor_ece_emitted(tmp_path: Path):
    corpus = _load_fixture()
    preds = [p for gt in corpus.ground_truth for p in gt.predictions_that_will_resolve]
    sut = MockCalibratedSUT.from_predictions(preds, actor_id="alice")
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="itest-actor",
        extras={"output_dir": str(tmp_path)},
    )
    results = await LayerThreeEvaluator().evaluate(ctx)
    actor_results = [r for r in results if r.metric_name == "ece_by_actor"]
    assert actor_results, "expected at least one per-actor ECE result"
    assert any(r.breakdown_by.get("actor_id") == "alice" for r in actor_results)
