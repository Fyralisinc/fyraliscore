"""End-to-end: ``run_once`` drives MockSUT + NoopEvaluators against a fixture corpus."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lsob_contracts import AblationConfig

from lsob_harness import db as dbmod
from lsob_harness.mocks import MockEvaluatorRegistry, MockSUT
from lsob_harness.runner import RunRequest, run_once


@pytest.mark.asyncio
async def test_run_once_writes_db_and_summary(tmp_path: Path, mini_corpus_a: Path) -> None:
    sut = MockSUT(name="mock")
    evaluators = MockEvaluatorRegistry.construct_for_layers([1, 2, 3, 4, 5, 6])

    req = RunRequest(
        corpus_path=mini_corpus_a,
        sut_name="mock",
        layers=[1, 2, 3, 4, 5, 6],
        ablation=AblationConfig(name="none"),
        runs_root=tmp_path / "runs",
        sut_override=sut,
        evaluators_override=evaluators,
    )

    outcome = await run_once(req)

    # summary.json looks right
    assert outcome.summary_path.exists()
    summary = json.loads(outcome.summary_path.read_text())
    assert summary["run_id"] == outcome.run_id
    assert summary["sut"] == "mock"
    assert summary["ablation"] == "none"
    assert summary["num_eval_results"] == len(outcome.results)
    # per-month evaluators (layers 1,2,4) + final (3,5,6): one-month corpus
    # has a single checkpoint, so we expect 3 per-month + 3 final = 6.
    assert len(outcome.results) == 6

    # results.db has both tables populated
    with dbmod.open_db(outcome.results_db) as conn:
        mf = dbmod.read_manifest(conn, outcome.run_id)
        assert mf is not None
        assert mf.baseline == "mock"
        results = dbmod.read_eval_results(conn, outcome.run_id)
    assert len(results) == 6
    assert {r.layer_id for r in results} == {1, 2, 3, 4, 5, 6}

    # index.db mirrors the run
    with dbmod.open_db(outcome.index_db) as conn:
        rows = dbmod.list_runs(conn)
    assert len(rows) == 1
    assert rows[0]["run_id"] == outcome.run_id
    assert rows[0]["layer_count"] == 6

    # SUT actually received signals
    assert sut.ingested_count > 0


@pytest.mark.asyncio
async def test_run_id_format_and_fallback_sha(tmp_path: Path, mini_corpus_a: Path) -> None:
    req = RunRequest(
        corpus_path=mini_corpus_a,
        sut_name="mock",
        layers=[1],
        ablation=AblationConfig(name="none"),
        runs_root=tmp_path / "runs",
        sut_override=MockSUT(),
        evaluators_override=MockEvaluatorRegistry.construct_for_layers([1]),
    )
    outcome = await run_once(req)

    # run_id = {sut}-{corpus_id}-{ablation}-{timestamp}-{sha}
    parts = outcome.run_id.split("-")
    assert parts[0] == "mock"
    # Corpus id 'mini-a' contains a dash, so we only sanity-check the tail.
    assert parts[-1] in (outcome.manifest.git_sha, "nogit") or len(parts[-1]) >= 3
    assert outcome.manifest.git_sha  # always populated (possibly 'nogit')
