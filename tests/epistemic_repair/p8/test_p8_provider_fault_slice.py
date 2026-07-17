from __future__ import annotations

import os

import pytest

from lib.evaluation.epistemic_repair.p8_provider_runner import (
    PROVIDER_BOUNDARIES,
    run_provider_fault_slice,
)


pytestmark = pytest.mark.asyncio


@pytest.mark.real_llm
async def test_real_codex_cli_faults_have_durable_attempt_receipts() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required")
    result = await run_provider_fault_slice(dsn)
    assert result.provider == "codex-cli"
    assert result.model == "gpt-5.4"
    assert len(result.receipts) == 6
    assert {row.boundary for row in result.receipts} == set(PROVIDER_BOUNDARIES)
    assert all(row.persisted_logical_receipts == 1 for row in result.receipts)
    assert all(row.persisted_attempt_receipts == 1 for row in result.receipts)
    assert len({row.physical_attempt_id for row in result.receipts}) == 3
    assert {row.observed_outcome for row in result.receipts} == {"timeout", "parse_failure"}
    partial = [row for row in result.receipts if row.boundary == "provider_timeout_after_partial_work"]
    assert all(row.partial_events > 0 for row in partial)
