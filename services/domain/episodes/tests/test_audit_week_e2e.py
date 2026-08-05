from __future__ import annotations

from datetime import timedelta

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.episodes.handoff import EpisodeSnapshotOutboxRepository
from services.domain.episodes.read import EpisodeReadService
from services.domain.episodes.reasoning import EpisodeReasoningInputService
from services.domain.episodes.worker import EpisodeConstructorWorker, EpisodeSettlementWorker
from services.domain.identity.intake import IdentityIntakeRepository
from services.domain.identity.worker import IdentityResolutionWorker

from .test_lifecycle_snapshot import _claim
from .test_routing_repo import _seed


pytestmark=pytest.mark.integration


async def test_alpen_audit_week_cross_source_episode_end_to_end(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id=uuid7()
    sam_one,sam_two=uuid7(),uuid7()
    async with fresh_db.acquire() as conn:
        await conn.executemany(
            "INSERT INTO actors (id,tenant_id,type,display_name,status,metadata) "
            "VALUES ($1,$2,'human_internal','Sam','active','{}'::jsonb)",
            [(sam_one,tenant_id),(sam_two,tenant_id)],
        )
        notion=await _seed(
            conn,tenant_id=tenant_id,source="notion",scope="notion:alpen",
            object_id="audit-map",anchor_id="security-audit",
            text="Audit map: authentication in progress and payments scheduled.",
        )
        slack=await _seed(
            conn,tenant_id=tenant_id,source="slack",scope="slack:alpen",
            object_id="C-AUDIT:101",anchor_id="security-audit",
            text="Authentication audit is complete.",
        )
        meeting=await _seed(
            conn,tenant_id=tenant_id,source="fireflies",scope="fireflies:alpen",
            object_id="audit-sync",anchor_id="security-audit",
            text="Authentication audit is not complete.",
        )
        jira=await _seed(
            conn,tenant_id=tenant_id,source="jira",scope="jira:alpen",
            object_id="SEC-412",anchor_id="security-audit",
            text="Sam moved session revocation audit to in progress.",
            extra_entities=[{"type":"person_name","id":"Sam"}],
            unresolved_phrases=["Sam"],
        )
        marketing=await _seed(
            conn,tenant_id=tenant_id,source="notion",scope="notion:alpen",
            object_id="marketing-audit",anchor_id="marketing-content-audit",
            text="The marketing content audit is complete.",
        )
        await _claim(conn,slack,value="complete",polarity="positive")
        await _claim(conn,meeting,value="incomplete",polarity="negative")
        for observation in (notion,slack,meeting,jira,marketing):
            await IdentityIntakeRepository().enqueue_observation_ready(observation,conn=conn)

    assert await IdentityResolutionWorker(fresh_db).run_once(
        worker_id="identity",batch_size=20
    ) == 5
    assert await EpisodeConstructorWorker(fresh_db).run_once(
        worker_id="constructor",batch_size=20
    ) == 5
    assert await EpisodeSettlementWorker(
        fresh_db,quiet_period=timedelta(0)
    ).run_once(batch_size=20) == 2

    handoff=EpisodeSnapshotOutboxRepository()
    read=EpisodeReadService()
    async with fresh_db.acquire() as conn:
        audit_snapshot_row=await conn.fetchrow(
            """
            SELECT s.id,s.episode_id FROM episode_snapshots s
            JOIN episode_topics t ON t.tenant_id=s.tenant_id AND t.id=s.topic_id
            WHERE s.tenant_id=$1 AND t.primary_anchor=$2::jsonb
            """,
            tenant_id,'{"type":"workstream","id":"security-audit"}',
        )
        assert audit_snapshot_row is not None
        snapshot=await read.snapshot(
            audit_snapshot_row["id"],tenant_id=tenant_id,conn=conn
        )
        assert snapshot is not None
        assert set(snapshot.observation_ids) == {notion.id,slack.id,meeting.id,jira.id}
        assert marketing.id not in snapshot.observation_ids
        assert len(snapshot.contradictions) == 1
        assert snapshot.contradictions[0].status == "unresolved"
        assert snapshot.coverage.citation_completeness == 1
        assert snapshot.access.visibility == "tenant"
        assert await conn.fetchval(
            "SELECT identity_resolution_status FROM perception_outbox "
            "WHERE tenant_id=$1 AND observation_id=$2",tenant_id,jira.id,
        ) == "partial"
        citations=await read.citations(snapshot.id,tenant_id=tenant_id,conn=conn)
        assert {row["source"] for row in citations} == {
            "notion","slack","fireflies","jira"
        }
        explanations=await read.memberships(
            snapshot.episode_id,tenant_id=tenant_id,conn=conn
        )
        assert all(row["reasons"] and row["feature_snapshot"] for row in explanations)
        contradictions=await read.contradictions(
            snapshot.episode_id,tenant_id=tenant_id,conn=conn
        )
        assert len(contradictions) == 1

        items=await handoff.claim(
            worker_id="reasoning",batch_size=10,lease_seconds=60,conn=conn
        )
        audit_item=next(item for item in items if item.episode_snapshot_id==snapshot.id)
        batch=await EpisodeReasoningInputService().load(audit_item,conn=conn)
        assert batch.reasoning_input.authorized_evidence_ids == snapshot.evidence_ids
        assert len(batch.observations) == 4
        assert len(batch.claims) == 2
