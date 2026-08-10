from __future__ import annotations

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.episodes.query import QueryEpisodeService
from services.domain.identity.intake import IdentityIntakeRepository

from .test_lifecycle_snapshot import _route_all
from .test_routing_repo import _seed


pytestmark = pytest.mark.integration


async def test_query_episode_reuses_topic_by_recorded_equivalence_and_filters_acl(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    ceo_id = uuid7()
    legal_id = uuid7()
    async with fresh_db.acquire() as conn:
        await conn.executemany(
            "INSERT INTO actors (id,tenant_id,type,display_name,status,metadata) "
            "VALUES ($1,$2,'human_internal',$3,'active','{}'::jsonb)",
            [(ceo_id, tenant_id, "CEO"), (legal_id, tenant_id, "Legal")],
        )
        notion = await _seed(
            conn, tenant_id=tenant_id, source="notion", scope="notion:alpen",
            object_id="audit-plan", anchor_id="security-audit",
            text="Security audit covers authentication and payments.",
        )
        jira = await _seed(
            conn, tenant_id=tenant_id, source="jira", scope="jira:alpen",
            object_id="SEC-412", anchor_id="security-audit",
            text="Security audit session revocation is in progress.",
        )
        restricted = await _seed(
            conn, tenant_id=tenant_id, source="slack", scope="slack:alpen",
            object_id="legal-note", anchor_id="security-audit",
            text="Privileged security audit legal note.",
            access_policy={
                "visibility": "restricted",
                "audience": [{"type": "actor", "id": str(legal_id)}],
                "source_acl_version": "slack-v1",
            },
        )
        for observation in (notion, jira, restricted):
            await IdentityIntakeRepository().enqueue_observation_ready(
                observation, conn=conn
            )
    await _route_all(fresh_db)
    async with fresh_db.acquire() as conn:
        result = await QueryEpisodeService().construct(
            "What is the current state of the security audit?",
            tenant_id=tenant_id,
            requester_actor_id=ceo_id,
            seed_anchor_refs=({"type": "workstream", "id": "security-audit"},),
            conn=conn,
        )
        assert result.equivalent_topic_ids
        assert result.snapshot.lifecycle_state == "settled"
        assert result.snapshot.settlement.reason == "query_scope_satisfied"
        assert set(result.snapshot.observation_ids) == {notion.id, jira.id}
        assert restricted.id not in result.snapshot.observation_ids
        assert await conn.fetchval(
            "SELECT count(*) FROM episode_topic_equivalences WHERE tenant_id=$1",
            tenant_id,
        ) == 1
        replay = await QueryEpisodeService().construct(
            "What is the current state of the security audit?",
            tenant_id=tenant_id,
            requester_actor_id=ceo_id,
            seed_anchor_refs=({"type": "workstream", "id": "security-audit"},),
            conn=conn,
        )
        assert replay.snapshot.id == result.snapshot.id
