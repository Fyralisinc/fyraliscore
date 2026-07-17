from __future__ import annotations

from datetime import datetime, timezone
import os
from uuid import uuid4

import asyncpg
import pytest

from lib.evaluation.epistemic_repair.p2_runner import _admission
from lib.shared.errors import InvariantViolation
from services.domain.company_learning.barrier import (
    CompanyLearningBarrierService,
    ContextDecision,
)
from services.domain.truth_kernel.repository import AsyncpgTruthKernelStorage
from services.domain.truth_kernel.service import TruthKernelService


pytestmark = pytest.mark.asyncio


async def test_barrier_proves_visibility_credit_and_idempotent_versioning():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required")
    conn = await asyncpg.connect(dsn)
    outer = conn.transaction()
    await outer.start()
    try:
        tenant_id = uuid4()
        await conn.execute("INSERT INTO tenants (id,name) VALUES ($1,'p4-barrier')", tenant_id)
        truth = TruthKernelService(storage=AsyncpgTruthKernelStorage())
        admitted = await truth.admit(tx=conn, command=_admission(tenant_id, 1))
        service = CompanyLearningBarrierService()
        decision_id = uuid4()
        now = datetime.now(timezone.utc)
        await service.record_context_decision(
            tx=conn,
            item=ContextDecision(
                decision_id=decision_id, tenant_id=tenant_id, batch_id="batch-1",
                route_id="accepted-memory-first", context_item_kind="accepted_model",
                context_item_id=str(admitted.model_id), context_item_version="1",
                retrieved=True, selected=True, included=True, referenced=True,
                counterevidence_retained=False, confidence_affecting=True,
                necessary_background=False, historical_reopen_reason=None,
                decision_fate="mutation", result_object_kind="model_version",
                result_object_id=admitted.version_id,
                evidence_lineage=({"kind": "model_version", "id": str(admitted.version_id)},),
                decided_at=now,
            ),
        )
        barrier_id = uuid4()
        first = await service.complete(
            tx=conn, barrier_id=barrier_id, tenant_id=tenant_id,
            batch_id="batch-1", expected_model_version_ids=(admitted.version_id,),
            truth_critical_pending_count=0, completed_at=now,
        )
        replay = await service.complete(
            tx=conn, barrier_id=barrier_id, tenant_id=tenant_id,
            batch_id="batch-1", expected_model_version_ids=(admitted.version_id,),
            truth_critical_pending_count=0, completed_at=now,
        )
        assert first == replay
        assert first.barrier_version == 1
        assert await conn.fetchval(
            "SELECT count(*) FROM company_learning_barriers WHERE tenant_id=$1",
            tenant_id,
        ) == 1
        assert await conn.fetchval(
            "SELECT count(*) FROM company_learning_context_decisions WHERE tenant_id=$1 AND referenced",
            tenant_id,
        ) == 1
        with pytest.raises(InvariantViolation, match="truth-critical work"):
            await service.complete(
                tx=conn, barrier_id=uuid4(), tenant_id=tenant_id,
                batch_id="batch-2", truth_critical_pending_count=1,
                completed_at=now,
            )
    finally:
        await outer.rollback()
        await conn.close()
