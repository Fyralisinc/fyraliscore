from __future__ import annotations

import os

import pytest

from lib.evaluation.epistemic_repair.p8_postgres_runner import (
    P8_DB_COVERED_BOUNDARIES,
    P8_DB_UNCOVERED_BOUNDARIES,
    run_postgres_fault_slice,
)


pytestmark = pytest.mark.asyncio


async def test_postgres_fault_slice_restarts_queries_and_replays_without_overclaim() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required")
    result = await run_postgres_fault_slice(dsn)
    assert len(result.receipts) == 18
    assert len(P8_DB_COVERED_BOUNDARIES) == 9
    assert len(P8_DB_UNCOVERED_BOUNDARIES) == 3
    assert result.exact_required_fault_coverage is False
    assert len(result.evidence_digest) == 64
    assert all(row.post_restart_barrier_count == 1 for row in result.receipts)
    assert all(row.post_restart_model_count == 1 for row in result.receipts)
    assert all(row.post_restart_pending_count == 0 for row in result.receipts)
    assert all(len(row.replay_receipt_digest) == 64 for row in result.receipts)
    assert len({row.queried_state_digest for row in result.receipts}) == 18
