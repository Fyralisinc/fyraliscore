from __future__ import annotations

from datetime import timedelta

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.episodes.handoff_worker import EpisodeReasoningHandoffWorker
from services.domain.episodes.worker import EpisodeConstructorWorker, EpisodeSettlementWorker
from services.domain.identity.intake import IdentityIntakeRepository
from services.domain.identity.worker import IdentityResolutionWorker
from services.domain.perception.knowledge import PerceptionKnowledgeWorker

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
    await PerceptionKnowledgeWorker(fresh_db).run_once(
        worker_id="knowledge",batch_size=10
    )
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
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "INSERT INTO reasoning_ingress_policies (tenant_id,mode,reason,updated_by) "
            "VALUES ($1,'episode','integration_test','pytest')",
            tenant_id,
        )
    assert await EpisodeReasoningHandoffWorker(fresh_db).run_once(
        worker_id="reasoning",batch_size=10
    ) == 1
    async with fresh_db.acquire() as conn:
        items=await conn.fetch(
            "SELECT * FROM episode_snapshot_outbox WHERE tenant_id=$1",tenant_id
        )
        assert len(items) == 1
        assert items[0]["status"] == "completed", items[0]["last_error"]
        trigger=await conn.fetchrow(
            "SELECT id,trigger_kind,trigger_subkind,payload FROM think_trigger_queue "
            "WHERE id=$1",items[0]["id"],
        )
        assert trigger is not None
        assert trigger["trigger_kind"] == "T1"
        assert trigger["trigger_subkind"] == "episode_snapshot"
        payload=trigger["payload"]
        if isinstance(payload,str):
            import json
            payload=json.loads(payload)
        assert set(payload["observation_ids"]) == {str(notion.id),str(jira.id)}
        assert payload["input_contract"] == "episode-reasoning-v1"
        assert await EpisodeSettlementWorker(
            fresh_db,quiet_period=timedelta(0)
        ).run_once(batch_size=10) == 0
