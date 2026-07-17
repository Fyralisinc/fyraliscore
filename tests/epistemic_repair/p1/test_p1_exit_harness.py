from __future__ import annotations

import json
from pathlib import Path

import pytest

from lib.evaluation.epistemic_repair.p1_exit import (
    ARTIFACT_SCHEMA_VERSION,
    run_p1_exit_evaluation,
    write_p1_exit_artifact,
)


ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.asyncio
async def test_sealed_two_batch_exit_run_reconciles_without_real_provider() -> None:
    report = await run_p1_exit_evaluation(repository_root=ROOT)

    assert report["schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert report["execution_mode"] == "deterministic_no_real_provider"
    assert report["input_contract"] == {
        "batch_count": 2,
        "signals_per_batch": [10, 10],
        "individual_signal_calls": 0,
    }
    assert report["counts"] == {
        "think_runs": 2,
        "logical_calls": 2,
        "physical_attempts": 4,
        "cost_rows": 4,
        "counts_reconciled": True,
    }
    assert [batch["attempt_outcomes"] for batch in report["batches"]] == [
        ["timeout", "success"],
        ["parse_failure", "success"],
    ]
    assert all(batch["timing_reconciled"] for batch in report["batches"])
    assert report["latency"]["failed_attempts_included"]
    assert report["cost_reconciliation"]["basis"].endswith("never_actual")
    assert report["hook_scan"]["hook_blind"]
    assert report["hard_gates"] == {
        "HG-01_benchmark_blindness": True,
        "HG-13_observability_integrity": True,
    }
    assert all(report["deterministic_success_criteria"].values())
    assert report["deterministic_success_criteria"]["context_digest_coverage"]
    assert report["deterministic_passed"]
    assert not report["phase_exit_ready"]
    assert "durable PostgreSQL receipt write and recovery behavior" in report[
        "unverified_phase_criteria"
    ]
    assert "clean_batch_t1_p95_ms" not in report["latency"]


@pytest.mark.asyncio
async def test_exit_artifact_round_trips_as_json(tmp_path: Path) -> None:
    report = await run_p1_exit_evaluation(repository_root=ROOT)
    path = write_p1_exit_artifact(report, tmp_path / "p1.json")

    assert json.loads(path.read_text()) == report
