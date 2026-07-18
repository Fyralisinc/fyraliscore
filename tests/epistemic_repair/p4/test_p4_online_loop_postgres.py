from __future__ import annotations

import os

import asyncpg
import pytest

from services.evaluation.epistemic_repair.p4_runner import run_p4_online_loop


@pytest.mark.asyncio
async def test_six_batch_online_loop_on_postgres() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for P4 evaluator")
    conn = await asyncpg.connect(dsn)
    outer = conn.transaction()
    await outer.start()
    try:
        report = await run_p4_online_loop(conn)
        assert report["execution_status"] == "complete"
        assert len(report["batch_results"]) == 6
        assert all(item["signal_count"] == 20 for item in report["batch_results"])
        assert all(report["hard_gates"].values())
        assert set(report["hard_gates"]) == {"HG-10", "HG-11", "HG-12", "HG-13"}
        assert all(
            item["refresh_queue_evidence"]["coalesced_row_count"] == 1
            and item["refresh_queue_evidence"]["pending_after_drain"] == 0
            for item in report["batch_results"]
        )
        assert report["continuous_metrics"]["selected_context_utilization"] >= 0.80
        assert report["continuous_metrics"]["delayed_attribution_coverage"] >= 0.90
        assert report["continuous_metrics"]["late_historical_observation_selected_count"] == 1
        assert report["continuous_metrics"]["late_unnecessary_historical_observation_count"] == 0
        assert report["continuous_metrics"]["late_unnecessary_historical_observation_use"] == 0.0
        assert report["phase_exit_ready"]
    finally:
        await outer.rollback()
        await conn.close()
