from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import asyncpg
import pytest

from services.evaluation.epistemic_repair.p2_runner import run_p2_truth_kernel


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
async def test_p2_runner_normalizes_gates_and_preserves_missing_race_on_postgres() -> None:
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
        # The P9 member-evidence normalizer is the public exit boundary. It
        # deliberately replaces internal per-gate diagnostic objects with one
        # boolean per preregistered gate while preserving raw denominators in
        # ``p9_member_contributions``.
        assert set(report["hard_gates"]) == {
            f"HG-{index:02d}" for index in range(4, 11)
        }
        assert all(value is True for value in report["hard_gates"].values())
        assert set(report["p9_member_contributions"]["gate_members"]) == set(
            report["hard_gates"]
        )
        # This direct-connection test intentionally supplies no independent
        # concurrency DSN, so the race probe must remain explicit missing
        # evidence even though every executed gate member passed.
        assert report["missing_evidence"] == [
            "race:p2-race-confirm-vs-falsify"
        ]
        assert report["phase_exit_ready"] is False
    finally:
        await transaction.rollback()
        await conn.close()
