from __future__ import annotations

import os

import pytest

from lib.evaluation.epistemic_repair.p8_population import ScaleCell
from lib.evaluation.epistemic_repair.p8_scale_runner import (
    ScaleExecution,
    evaluate_scale_execution,
    run_scale_cell,
)


pytestmark = pytest.mark.asyncio


async def test_measured_postgres_scale_cell_uses_truth_retrieval_and_barriers() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required")
    cell = await run_scale_cell(dsn, ScaleCell("p8-test-bs10-h3-t2", 10, 3, 2))
    assert len(cell.tenant_receipts) == 2
    assert cell.observation_rows == 60
    assert cell.canonical_rows == 10  # two tenants x (model + version + three barriers)
    assert cell.semantic_quality == 1.0
    assert cell.cross_tenant_leakage == 0
    assert cell.queue_depth_slope_final_half <= 0
    assert cell.rollback_isolated is True
    assert cell.physically_isolated_database is False
    assert all(row.barriers == 3 for row in cell.tenant_receipts)
    assert all(row.accepted_model_hits == 3 for row in cell.tenant_receipts)


async def test_scale_evaluator_does_not_equate_rollback_with_database_isolation() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required")
    cell = await run_scale_cell(dsn, ScaleCell("p8-test-bs10-h3-t1", 10, 3, 1))
    evaluation = evaluate_scale_execution(ScaleExecution((cell,), None, False, False, "a" * 64))
    assert evaluation["scale_execution_ready"] is False
    assert evaluation["gates"]["physically_isolated_database_per_cell"] is False
