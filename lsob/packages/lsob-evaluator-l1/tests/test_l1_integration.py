"""End-to-end integration test of LayerOneEvaluator vs the mini corpus."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from lsob_contracts import Corpus, EvaluationContext

from lsob_evaluator_l1 import LayerOneEvaluator, MockRetrievalSUT

FIXTURE = (
    Path(__file__).resolve().parents[3] / "fixtures" / "mini_corpus_a.json"
)


@pytest.mark.asyncio
async def test_integration_mini_corpus_a():
    assert FIXTURE.exists(), f"fixture missing: {FIXTURE}"
    corpus = Corpus.model_validate_json(FIXTURE.read_text())
    sut = MockRetrievalSUT(corpus)
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=corpus.ground_truth[-1].timestamp,
        run_id="integration-a",
    )
    results = await LayerOneEvaluator().evaluate(ctx)
    names = {r.metric_name for r in results}

    # All three sub-evaluators should have reported.
    assert "semantic_recall_at_10" in names
    assert "entity_resolution_accuracy" in names
    assert "reranker_ndcg_at_10" in names
    # "not applicable" must NOT appear when SUT implements the protocol.
    assert "layer_not_applicable" not in names
    # Every result carries the right layer and run id.
    for r in results:
        assert r.layer_id == 1
        assert r.run_id == "integration-a"
        assert 0.0 <= r.value <= 1.0 or r.metric_name.endswith("kendall_tau")


def test_cli_emits_json(tmp_path: Path):
    """Invoke the console script end-to-end and parse its stdout as JSON."""
    # Prefer running via `uv run` so the console script entry point is found
    # regardless of how pytest was launched.
    uv = "/opt/homebrew/bin/uv"
    cmd = [
        uv,
        "run",
        "lsob-eval-l1",
        "run",
        "--corpus",
        str(FIXTURE),
        "--sut",
        "mock",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert isinstance(payload, list) and payload
    assert all(item["layer_id"] == 1 for item in payload)


def test_cli_none_sut_reports_not_applicable():
    uv = "/opt/homebrew/bin/uv"
    cmd = [
        uv,
        "run",
        "lsob-eval-l1",
        "run",
        "--corpus",
        str(FIXTURE),
        "--sut",
        "none",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert len(payload) == 1
    assert payload[0]["metric_name"] == "layer_not_applicable"
