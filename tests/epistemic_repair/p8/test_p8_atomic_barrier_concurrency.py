from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import os
from uuid import uuid4

import asyncpg
import pytest

from lib.shared.errors import InvariantViolation
from services.domain.company_learning.barrier import CompanyLearningBarrierService


pytestmark = pytest.mark.asyncio


async def test_atomic_common_path_replay_and_version_chain_after_blocked_lock() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required")
    tenant, service = uuid4(), CompanyLearningBarrierService()
    setup = await asyncpg.connect(dsn)
    await setup.execute("INSERT INTO tenants(id,name) VALUES($1,$2)", tenant, f"p8-atomic-{tenant}")
    one, two = await asyncpg.connect(dsn), await asyncpg.connect(dsn)
    now = datetime.now(timezone.utc)
    try:
        missing_tx = one.transaction()
        await missing_tx.start()
        with pytest.raises(InvariantViolation, match="expected Models are not current"):
            await service.complete(
                tx=one, barrier_id=uuid4(), tenant_id=tenant, batch_id="missing",
                expected_model_version_ids=(uuid4(),),
                truth_critical_pending_count=0, completed_at=now,
            )
        await missing_tx.rollback()

        tx1, tx2 = one.transaction(), two.transaction()
        await tx1.start(); await tx2.start()
        first = await service.complete(
            tx=one, barrier_id=uuid4(), tenant_id=tenant, batch_id="first",
            truth_critical_pending_count=0, completed_at=now,
        )
        blocked = asyncio.create_task(service.complete(
            tx=two, barrier_id=uuid4(), tenant_id=tenant, batch_id="second",
            truth_critical_pending_count=0, completed_at=now + timedelta(seconds=1),
        ))
        await asyncio.sleep(.05)
        assert not blocked.done()
        await tx1.commit()
        second = await blocked
        await tx2.commit()
        assert second.barrier_version == first.barrier_version + 1
        assert second.prior_barrier_id == first.barrier_id

        tx3, tx4 = one.transaction(), two.transaction()
        await tx3.start(); await tx4.start()
        original = await service.complete(
            tx=one, barrier_id=uuid4(), tenant_id=tenant, batch_id="duplicate",
            truth_critical_pending_count=0, completed_at=now + timedelta(seconds=2),
        )
        duplicate_task = asyncio.create_task(service.complete(
            tx=two, barrier_id=uuid4(), tenant_id=tenant, batch_id="duplicate",
            truth_critical_pending_count=0, completed_at=now + timedelta(days=1),
        ))
        await asyncio.sleep(.05)
        assert not duplicate_task.done()
        await tx3.commit()
        duplicate = await duplicate_task
        await tx4.commit()
        assert duplicate == original
    finally:
        await one.close(); await two.close()
        await setup.execute("DELETE FROM tenants WHERE id=$1", tenant)
        await setup.close()
