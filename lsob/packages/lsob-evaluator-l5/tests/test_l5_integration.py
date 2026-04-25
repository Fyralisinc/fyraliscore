"""End-to-end: full LayerFiveEvaluator against the hand-crafted 6-month fixture."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from lsob_contracts import Corpus, EvaluationContext

from lsob_evaluator_l5.evaluator import LayerFiveEvaluator
from lsob_evaluator_l5.mock_sut import MockTemporalSUT

FIXTURE = Path(__file__).parent / "fixtures" / "mini_6mo.json"


def _load_fixture() -> Corpus:
    return Corpus.model_validate(json.loads(FIXTURE.read_text()))


async def test_full_evaluator_runs_on_fixture() -> None:
    corpus = _load_fixture()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=MockTemporalSUT(),
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="integration",
    )
    ev = LayerFiveEvaluator()
    results = await ev.evaluate(ctx)
    assert results, "evaluator produced no results"

    metric_names = {r.metric_name for r in results}
    # All sub-evaluators should have produced at least one row.
    assert "calibration_trajectory_slope" in metric_names
    assert any(
        name.startswith("pattern_precipitation_latency_")
        or name == "pattern_precipitation_latency_mean"
        for name in metric_names
    )
    assert "belief_churn_per_month" in metric_names
    assert "retrieval_recall_at_10_trajectory" in metric_names
    # No shocks in fixture → recovery_na.
    assert "recovery_na" in metric_names

    # Every result carries layer_id=5 and the integration run_id.
    assert all(r.layer_id == 5 for r in results)
    assert all(r.run_id == "integration" for r in results)


def test_cli_run_on_fixture(tmp_path) -> None:
    from typer.testing import CliRunner

    from lsob_evaluator_l5.cli import app

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "--corpus", str(FIXTURE), "--sut", "mock"],
    )
    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert isinstance(parsed, list)
    assert parsed
    assert all(row["layer_id"] == 5 for row in parsed)
