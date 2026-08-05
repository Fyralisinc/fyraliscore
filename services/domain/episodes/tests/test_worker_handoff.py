from __future__ import annotations

from datetime import timedelta

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.episodes.handoff import EpisodeSnapshotOutboxRepository
from services.domain.episodes.reasoning import EpisodeReasoningInputService
from services.domain.episodes.worker import EpisodeConstructorWorker, EpisodeSettlementWorker
from services.domain.identity.intake import IdentityIntakeRepository
from services.domain.identity.worker import IdentityResolutionWorker

from .test_routing_repo import _seed


pytestmark = pytest.mark.integration


async def test_durable_workers_complete_intake_and_handoff_structured_snapshot(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id=uuid7()
    async with fresh_db.acquire() as conn:
        notion=await _seed(
            conn,tenant_id=tenant_id,source="notion",scope="notion:alpen",
            object_id="audit-plan",anchor_id="security-audit",
            text="Security audit covers authentication.",
        )
        jira=await _seed(
            conn,tenant_id=tenant_id,source="jira",scope="jira:alpen",
            object_id="SEC-412",anchor_id="security-audit",
            text="Security audit task is in progress.",
        )
        for observation in (notion,jira):
            await IdentityIntakeRepository().enqueue_observation_ready(observation,conn=conn)
    await IdentityResolutionWorker(fresh_db).run_once(worker_id="identity",batch_size=10)
    assert await EpisodeConstructorWorker(fresh_db).run_once(
        worker_id="constructor",batch_size=10
    ) == 2
    async with fresh_db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT count(*) FROM perception_outbox WHERE tenant_id=$1 AND status='completed'",
            tenant_id,
        ) == 2
        assert await conn.fetchval(
            "SELECT count(*) FROM episode_lifecycle_events WHERE tenant_id=$1 AND event_kind='opened'",
            tenant_id,
        ) == 1
    assert await EpisodeSettlementWorker(
        fresh_db,quiet_period=timedelta(0)
    ).run_once(batch_size=10) == 1
    outbox=EpisodeSnapshotOutboxRepository()
    async with fresh_db.acquire() as conn:
        items=await outbox.claim(
            worker_id="reasoning",batch_size=10,lease_seconds=60,conn=conn
        )
        assert len(items) == 1
        batch=await EpisodeReasoningInputService().load(items[0],conn=conn)
        assert batch.reasoning_input.mode == "automatic_update"
        assert len(batch.observations) == 2
        assert {row["source"] for row in batch.observations} == {"notion","jira"}
        completed=await outbox.complete(
            items[0].id,tenant_id=tenant_id,worker_id="reasoning",conn=conn
        )
        assert completed.status == "completed"
        assert await EpisodeSettlementWorker(
            fresh_db,quiet_period=timedelta(0)
        ).run_once(batch_size=10) == 0
