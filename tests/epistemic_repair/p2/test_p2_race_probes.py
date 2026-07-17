from __future__ import annotations

from datetime import datetime, timezone
import os
from uuid import uuid4

import pytest

from lib.contracts.truth_admission import ModelHead, ModelTruthLifecycle, ModelVersion
from lib.evaluation.epistemic_repair.p2_race_probes import (
    FiveProjectionFence,
    InjectedFenceFailure,
    probe_concurrent_transitions,
)
from services.domain.truth_kernel.service import FenceContext
from services.domain.truth_kernel.repository import render_model_head_cas_sql


class _RecordingTx:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    async def execute(self, _sql: str, *args: object) -> str:
        self.calls.append(args)
        return "INSERT 0 1"


def _context() -> FenceContext:
    now = datetime.now(timezone.utc)
    tenant_id, model_id, old_version_id = uuid4(), uuid4(), uuid4()
    prior = ModelHead(
        tenant_id=tenant_id, model_id=model_id, version_id=old_version_id,
        version=1, semantic_digest="a" * 64,
        lifecycle=ModelTruthLifecycle.ACTIVE, advanced_at=now,
    )
    # Construction validation is irrelevant to this fence unit: it only reads
    # lifecycle/created_at. model_construct keeps the fixture intentionally tiny.
    successor = ModelVersion.model_construct(
        tenant_id=tenant_id, model_id=model_id, version_id=uuid4(), version=2,
        lifecycle=ModelTruthLifecycle.FALSIFIED, created_at=now,
    )
    return FenceContext(tenant_id, model_id, prior, successor, uuid4(), "b" * 64)


@pytest.mark.asyncio
async def test_five_projection_fence_injects_after_exactly_third_write() -> None:
    tx = _RecordingTx()
    with pytest.raises(InjectedFenceFailure):
        await FiveProjectionFence(fail_after=3).apply(tx=tx, context=_context())
    assert len(tx.calls) == 3


@pytest.mark.asyncio
async def test_five_projection_fence_completes_five_unique_targets() -> None:
    tx = _RecordingTx()
    await FiveProjectionFence().apply(tx=tx, context=_context())
    assert len(tx.calls) == 5
    assert len({call[2] for call in tx.calls}) == 5


def test_disposable_probe_uses_exact_production_cas_template() -> None:
    production = render_model_head_cas_sql()
    disposable = render_model_head_cas_sql("p2_race_contract.head")
    assert disposable == production.replace("model_truth_heads", "p2_race_contract.head")


@pytest.mark.asyncio
async def test_two_connection_cas_has_one_winner_and_drops_probe_schema() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is not configured")
    result = await probe_concurrent_transitions(
        dsn, tenant_id=uuid4(), model_id=uuid4()
    )
    assert result.conforms
    assert result.winner_count == 1
    assert result.lifecycle_event_count == 1
    assert result.final_version == 2
