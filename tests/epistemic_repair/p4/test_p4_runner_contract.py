from pathlib import Path
import subprocess
import sys


def test_cli_requires_dsn_and_exposes_output() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/run_epistemic_repair_p4_online_loop.py", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "--dsn" in result.stdout
    assert "--output" in result.stdout


def test_runner_uses_production_barrier_and_rollback_script() -> None:
    runner = Path("services/evaluation/epistemic_repair/p4_runner.py").read_text()
    script = Path("scripts/run_epistemic_repair_p4_online_loop.py").read_text()
    assert "CompanyLearningBarrierService" in runner
    assert "company_learning_context_decisions" in runner
    assert "company_learning_outcome_links" in runner
    assert "projection_refresh_jobs" in runner
    assert "transaction.rollback()" in script


def test_late_historical_use_metric_is_database_derived() -> None:
    runner = Path("services/evaluation/epistemic_repair/p4_runner.py").read_text()
    assert "late_unnecessary_historical / late_historical" in runner
    assert "late_unnecessary_historical_observation_count" in runner
    assert '"late_unnecessary_historical_observation_use": 0.0' not in runner


def test_refresh_evidence_uses_canonical_job_primary_key() -> None:
    runner = Path("services/evaluation/epistemic_repair/p4_runner.py").read_text()
    assert "SELECT id AS job_id,projection_name" in runner
    assert "ORDER BY id" in runner
    assert "SELECT job_id,projection_name" not in runner
