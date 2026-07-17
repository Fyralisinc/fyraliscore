from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import asyncpg
import pytest

from lib.evaluation.epistemic_repair.p2_runner import run_p2_truth_kernel


def test_p2_runner_cli_exposes_dsn_and_output() -> None:
    script = Path("scripts/run_epistemic_repair_p2_truth_kernel.py")
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        check=False, capture_output=True, text=True,
    )
    assert result.returncode == 0
    assert "--dsn" in result.stdout
    assert "--output" in result.stdout


@pytest.mark.asyncio
async def test_full_p2_runner_reports_missing_as_missing_on_postgres() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for the PostgreSQL P2 evaluator")
    conn = await asyncpg.connect(dsn)
    transaction = conn.transaction()
    await transaction.start()
    try:
        report = await run_p2_truth_kernel(conn)
        assert report["schema_version"] == "epistemic-repair-p2-truth-kernel-v1"
        assert len(report["case_results"]) == report["population"]["case_count"]
        assert all(item["status"] != "pass" for item in report["hard_gates"].values() if item["coverage"] < 1.0)
        assert report["missing_evidence"]
        assert not report["phase_exit_ready"]
    finally:
        await transaction.rollback()
        await conn.close()
