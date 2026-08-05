from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ObservationRow, SourceEvidenceCreate
from services.domain.episodes.intake import EpisodeIntakeRepository
from services.domain.episodes.service import EpisodeRoutingService
from services.domain.evidence.repo import SourceEvidenceRepository
from services.domain.identity.intake import IdentityIntakeRepository
from services.domain.identity.worker import IdentityResolutionWorker
from services.domain.perception.knowledge import PerceptionKnowledgeWorker


pytestmark = pytest.mark.integration


async def _seed(
    conn: asyncpg.Connection,
    *,
    tenant_id,
    source: str,
    scope: str,
    object_id: str,
    anchor_id: str,
    text: str,
    access_policy: dict | None = None,
    extra_entities: list[dict] | None = None,
    unresolved_phrases: list[str] | None = None,
) -> ObservationRow:
    now = datetime.now(UTC)
    evidence = await SourceEvidenceRepository().insert(
        SourceEvidenceCreate(
            tenant_id=tenant_id, source=source, installation_scope=scope,
            source_channel=f"{source}:object", source_object_type="record",
            source_object_id=object_id, source_revision_id="v1", operation="create",
            source_recorded_at=now, content_hash=hashlib.sha256(text.encode()).hexdigest(),
            raw_ingested_at=now, normalized_at=now, ingress_kind="poll",
            raw_retention_state="not_stored",
            access_policy=access_policy or {"visibility": "tenant", "audience": [],
                                            "source_acl_version": f"{source}-v1"},
        ),
        conn=conn,
    )
    row = await conn.fetchrow(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel, content, content_text,
          embedding_pending, trust_tier, entities_mentioned, evidence_id
        ) VALUES ($1,$2,$3,'signal',$4,$5::jsonb,$6,TRUE,'authoritative',$7::jsonb,$8)
        RETURNING id, tenant_id, occurred_at, ingested_at, kind, source_channel,
          source_actor_ref, actor_id, content, content_text, embedding,
          embedding_pending, trust_tier, external_id, cause_id, sequence_num,
          entities_mentioned, evidence_id
        """,
        uuid7(), tenant_id, now, f"{source}:object",
        json.dumps({
            "_episode_topics": ["Security Audit"],
            "_unresolved_phrases": unresolved_phrases or [],
        }), text,
        json.dumps([
            {"type": "workstream", "id": anchor_id},
            *(extra_entities or []),
        ]),
        evidence.evidence.id,
    )
    value = dict(row)
    for name in ("content", "entities_mentioned"):
        if isinstance(value[name], str):
            value[name] = json.loads(value[name])
    return ObservationRow.model_validate(value)


async def test_cross_source_observations_route_to_one_episode_and_replay(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    async with fresh_db.acquire() as conn:
        notion = await _seed(
            conn, tenant_id=tenant_id, source="notion", scope="notion:alpen",
            object_id="audit-plan", anchor_id="security-audit",
            text="Authentication and payments are in scope for security audit.",
        )
        jira = await _seed(
            conn, tenant_id=tenant_id, source="jira", scope="jira:alpen",
            object_id="SEC-412", anchor_id="security-audit",
            text="Session revocation audit moved to in progress.",
        )
        await IdentityIntakeRepository().enqueue_observation_ready(notion, conn=conn)
        await IdentityIntakeRepository().enqueue_observation_ready(jira, conn=conn)

    assert await IdentityResolutionWorker(fresh_db).run_once(
        worker_id="identity", batch_size=10
    ) == 2
    assert await PerceptionKnowledgeWorker(fresh_db).run_once(
        worker_id="knowledge", batch_size=10
    ) >= 2
    intake = EpisodeIntakeRepository()
    async with fresh_db.acquire() as conn:
        items = await intake.claim(
            worker_id="constructor", batch_size=10, lease_seconds=60, conn=conn
        )
        assert len(items) == 2
        service = EpisodeRoutingService()
        first = await service.route(items[0], conn=conn)
        second = await service.route(items[1], conn=conn)
        replay = await service.route(items[1], conn=conn)

        included = [row for row in (*first, *second) if row.decision == "include"]
        assert len(included) == 2
        assert len({row.episode_id for row in included}) == 1
        assert [row.id for row in replay] == [row.id for row in second]
        assert await conn.fetchval(
            "SELECT count(*) FROM episode_topics WHERE tenant_id=$1", tenant_id
        ) == 1
        assert await conn.fetchval(
            "SELECT count(*) FROM episodes WHERE tenant_id=$1", tenant_id
        ) == 1


async def test_conflicting_stable_anchor_creates_distinct_episode(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    async with fresh_db.acquire() as conn:
        security = await _seed(
            conn, tenant_id=tenant_id, source="notion", scope="notion:alpen",
            object_id="security", anchor_id="security-audit",
            text="The quarterly audit is complete.",
        )
        marketing = await _seed(
            conn, tenant_id=tenant_id, source="slack", scope="slack:alpen",
            object_id="marketing", anchor_id="marketing-content-audit",
            text="The quarterly audit is complete.",
        )
        await IdentityIntakeRepository().enqueue_observation_ready(security, conn=conn)
        await IdentityIntakeRepository().enqueue_observation_ready(marketing, conn=conn)
    await IdentityResolutionWorker(fresh_db).run_once(worker_id="identity", batch_size=10)
    await PerceptionKnowledgeWorker(fresh_db).run_once(
        worker_id="knowledge", batch_size=10
    )
    async with fresh_db.acquire() as conn:
        items = await EpisodeIntakeRepository().claim(
            worker_id="constructor", batch_size=10, lease_seconds=60, conn=conn
        )
        service = EpisodeRoutingService()
        await service.route(items[0], conn=conn)
        memberships = await service.route(items[1], conn=conn)
        assert await conn.fetchval(
            "SELECT count(*) FROM episodes WHERE tenant_id=$1", tenant_id
        ) == 2
        assert any(row.decision == "exclude" for row in memberships), [
            (row.decision, row.score, row.feature_snapshot) for row in memberships
        ]
