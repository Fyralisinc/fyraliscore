from __future__ import annotations

import os

import pytest

from lib.evaluation.epistemic_repair.p8_provider_runner import (
    PROVIDER_BOUNDARIES,
    _reported_usage,
    run_provider_fault_slice,
)


pytestmark = pytest.mark.asyncio


def test_codex_turn_completed_usage_is_exactly_reported_not_estimated() -> None:
    stdout = (
        b'{"type":"item.completed"}\n'
        b'{"type":"turn.completed","usage":{"input_tokens":16711,'
        b'"cached_input_tokens":16256,"output_tokens":21}}\n'
    )
    assert _reported_usage(stdout) == {
        "input_tokens": 16711, "cached_input_tokens": 16256,
        "output_tokens": 21,
    }
    assert _reported_usage(b'{"type":"turn.started"}\n') == {}


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
