"""One genuine DB-backed P8 replay slice; not full P8 qualification."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from uuid import uuid4

import asyncpg
import pytest

from services.evaluation.epistemic_repair.p2_runner import _admission
from services.domain.company_learning.barrier import CompanyLearningBarrierService
from services.domain.truth_kernel.repository import AsyncpgTruthKernelStorage
from services.domain.truth_kernel.service import TruthKernelService


pytestmark = pytest.mark.asyncio


async def test_duplicate_delivery_replays_one_durable_barrier_receipt() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required")
    conn = await asyncpg.connect(dsn)
    tx = conn.transaction()
    await tx.start()
    try:
        tenant_id = uuid4()
        await conn.execute("INSERT INTO tenants (id,name) VALUES ($1,'p8-replay-slice')", tenant_id)
        admitted = await TruthKernelService(storage=AsyncpgTruthKernelStorage()).admit(
            tx=conn, command=_admission(tenant_id, 1),
        )
        service = CompanyLearningBarrierService()
        now, barrier_id = datetime.now(timezone.utc), uuid4()
        first = await service.complete(
            tx=conn, barrier_id=barrier_id, tenant_id=tenant_id, batch_id="p8-db-replay",
            expected_model_version_ids=(admitted.version_id,),
            truth_critical_pending_count=0, completed_at=now,
        )
        duplicate = await service.complete(
            tx=conn, barrier_id=uuid4(), tenant_id=tenant_id, batch_id="p8-db-replay",
            expected_model_version_ids=(admitted.version_id,),
            truth_critical_pending_count=0, completed_at=now,
        )
        assert duplicate == first
        assert await conn.fetchval(
            "SELECT count(*) FROM company_learning_barriers WHERE tenant_id=$1 AND batch_id=$2",
            tenant_id, "p8-db-replay",
        ) == 1
        assert await conn.fetchval(
            "SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1 AND truth_version_id=$2",
            tenant_id, admitted.version_id,
        ) == 1
    finally:
        await tx.rollback()
        await conn.close()
