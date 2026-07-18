from __future__ import annotations

import json
import os
import asyncio

import asyncpg
import pytest

from services.evaluation.epistemic_repair.p7_real_runner import run_p7_real_provider
from lib.llm.provider import LLMConfig, LLMProvider


pytestmark = pytest.mark.asyncio


class _EmptySemanticProvider(LLMProvider):
    def __init__(self) -> None:
        super().__init__(
            LLMConfig(
                provider="deterministic-test",
                api_key="not-used",
                model="p7-empty-semantic-v1",
                max_retries=0,
            )
        )
        self.active = 0
        self.max_active = 0

    async def _raw_call(self, **_: object) -> str:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0.01)
            return json.dumps({"theses": []})
        finally:
            self.active -= 1


async def test_real_lane_binds_all_matched_units_calls_and_receipts() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for P7 PostgreSQL proof")
    conn = await asyncpg.connect(dsn)
    tx = conn.transaction()
    await tx.start()
    try:
        provider = _EmptySemanticProvider()
        artifact = await run_p7_real_provider(
            conn,
            provider=provider,
            commit_sha="test-commit",
            transport="deterministic-test",
            parallel_arms=5,
        )
    finally:
        await tx.rollback()
        await conn.close()
    assert len(artifact.unit_evidence) == 15
    assert len(artifact.call_receipts) == 45
    assert len(artifact.endpoints) == 45
    assert all(row.logical_receipt_count == 3 for row in artifact.unit_evidence)
    assert all(row.physical_receipt_count == 3 for row in artifact.unit_evidence)
    assert artifact.hard_gates["exact_paired_population"] is True
    assert artifact.hard_gates["durable_attempt_receipts"] is True
    assert artifact.strategic_verdict == "insufficient_evidence"
    assert artifact.phase_exit_ready is False
    assert len(artifact.paired_mature_comparisons) == 4
    assert len(artifact.paired_facet_intervals) == 4
    assert artifact.economics_status == "token_usage_unavailable"
    assert provider.max_active == 5
    for world_id in {row.world_id for row in artifact.call_receipts}:
        for arm_id in {row.arm_id for row in artifact.call_receipts}:
            stages = [
                row.stage_batch
                for row in artifact.call_receipts
                if row.world_id == world_id and row.arm_id == arm_id
            ]
            assert stages == [3, 6, 12]
