"""The ``timings`` table should be populated after a successful run."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lsob_contracts import AblationConfig

from lsob_harness import db as dbmod
from lsob_harness.mocks import MockEvaluatorRegistry, MockSUT
from lsob_harness.runner import RunRequest, run_once


@pytest.mark.asyncio
async def test_timings_table_has_expected_phases(
    tmp_path: Path, mini_corpus_a: Path
) -> None:
    sut = MockSUT(name="mock")
    evaluators = MockEvaluatorRegistry.construct_for_layers([1, 2, 3])
    req = RunRequest(
        corpus_path=mini_corpus_a,
        sut_name="mock",
        layers=[1, 2, 3],
        ablation=AblationConfig(name="none"),
        runs_root=tmp_path / "runs",
        sut_override=sut,
        evaluators_override=evaluators,
    )
    outcome = await run_once(req)

    with dbmod.open_db(outcome.results_db) as conn:
        rows = dbmod.read_timings(conn, outcome.run_id)

    phases = {r["phase"] for r in rows}
    assert "total_wall_clock" in phases
    assert "ingest_signal" in phases
    assert "ingest_bucket" in phases
    # Per-evaluator phases
    assert any(p.startswith("evaluator_") for p in phases)

    # total_wall_clock should be positive
    totals = [r for r in rows if r["phase"] == "total_wall_clock"]
    assert len(totals) == 1
    assert totals[0]["duration_ms"] > 0

    # Per-signal count matches corpus size
    per_signal = [r for r in rows if r["phase"] == "ingest_signal"]
    assert len(per_signal) == 10


@pytest.mark.asyncio
async def test_summary_contains_timings_extras(
    tmp_path: Path, mini_corpus_a: Path
) -> None:
    sut = MockSUT(name="mock")
    evaluators = MockEvaluatorRegistry.construct_for_layers([1])
    req = RunRequest(
        corpus_path=mini_corpus_a,
        sut_name="mock",
        layers=[1],
        ablation=AblationConfig(name="none"),
        runs_root=tmp_path / "runs",
        sut_override=sut,
        evaluators_override=evaluators,
    )
    outcome = await run_once(req)

    summary = json.loads(outcome.summary_path.read_text())
    assert "extras" in summary
    assert "timings" in summary["extras"]
    timings_extras = summary["extras"]["timings"]
    assert "total_wall_clock_ms" in timings_extras
    assert "by_phase_ms" in timings_extras
    assert "throughput_signals_per_sec" in timings_extras
    assert timings_extras["total_wall_clock_ms"] > 0
