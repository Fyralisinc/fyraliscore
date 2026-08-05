from __future__ import annotations

from datetime import UTC, datetime

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.episodes.construction import EpisodeConstructionService
from services.domain.episodes.intake import EpisodeIntakeRepository
from services.domain.episodes.read import EpisodeReadService
from services.domain.episodes.service import EpisodeRoutingService
from services.domain.identity.intake import IdentityIntakeRepository
from services.domain.identity.worker import IdentityResolutionWorker
from services.domain.perception.claims import (
    PerceptionClaimCreate,
    PerceptionClaimRepository,
    span_for_text,
)

from .test_routing_repo import _seed


pytestmark = pytest.mark.integration


async def _claim(conn, observation, *, value: str, polarity: str):
    return await PerceptionClaimRepository().insert(
        PerceptionClaimCreate(
            tenant_id=observation.tenant_id,
            evidence_id=observation.evidence_id,
            observation_id=observation.id,
            subject_ref={"type": "service", "id": "authentication"},
            predicate="audit_status",
            object_value=value,
            polarity=polarity,
            confidence=1,
            evidence_span=span_for_text(observation.content_text),
            extractor_kind="deterministic",
            extractor_name="episode-test-claims",
            extractor_version="1.0.0",
        ),
        conn=conn,
    )


async def _route_all(pool: asyncpg.Pool):
    await IdentityResolutionWorker(pool).run_once(worker_id="identity", batch_size=20)
    async with pool.acquire() as conn:
        items = await EpisodeIntakeRepository().claim(
            worker_id="constructor", batch_size=20, lease_seconds=60, conn=conn
        )
        rows = []
        for item in items:
            rows.extend(await EpisodeRoutingService().route(item, conn=conn))
        return rows


async def test_settlement_preserves_contradictions_and_late_evidence_reopens(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    async with fresh_db.acquire() as conn:
        slack = await _seed(
            conn, tenant_id=tenant_id, source="slack", scope="slack:alpen",
            object_id="auth-complete", anchor_id="security-audit",
            text="Authentication audit is complete.",
        )
        meeting = await _seed(
            conn, tenant_id=tenant_id, source="fireflies", scope="fireflies:alpen",
            object_id="audit-sync", anchor_id="security-audit",
            text="Authentication audit is not complete.",
        )
        await _claim(conn, slack, value="complete", polarity="positive")
        await _claim(conn, meeting, value="incomplete", polarity="negative")
        await IdentityIntakeRepository().enqueue_observation_ready(slack, conn=conn)
        await IdentityIntakeRepository().enqueue_observation_ready(meeting, conn=conn)
    memberships = await _route_all(fresh_db)
    episode_id = next(row.episode_id for row in memberships if row.decision == "include")
    construction = EpisodeConstructionService()
    async with fresh_db.acquire() as conn:
        first = await construction.settle(
            episode_id, tenant_id=tenant_id, reason="explicit_close", conn=conn
        )
        replay = await construction.settle(
            episode_id, tenant_id=tenant_id, reason="explicit_close", conn=conn
        )
        assert replay.id == first.id
        assert first.lifecycle_state == "settled"
        assert len(first.observation_ids) == 2
        assert len(first.contradictions) == 1
        assert first.contradictions[0].kind == "opposite_polarity"
        assert first.access.visibility == "tenant"
        with pytest.raises(asyncpg.RaiseError, match="episode history is immutable"):
            await conn.execute(
                "UPDATE episode_snapshots SET version=2 WHERE id=$1", first.id
            )

    async with fresh_db.acquire() as conn:
        jira = await _seed(
            conn, tenant_id=tenant_id, source="jira", scope="jira:alpen",
            object_id="SEC-412", anchor_id="security-audit",
            text="Session revocation audit moved to in progress.",
        )
        await IdentityIntakeRepository().enqueue_observation_ready(jira, conn=conn)
    late_memberships = await _route_all(fresh_db)
    late = next(row for row in late_memberships if row.decision == "include")
    async with fresh_db.acquire() as conn:
        assert await construction.reopen_for_late_evidence(
            episode_id, tenant_id=tenant_id, membership_id=late.id, conn=conn
        )
        second = await construction.settle(
            episode_id, tenant_id=tenant_id, reason="quiet_period", conn=conn
        )
        assert second.version == 2
        assert second.prior_snapshot_id == first.id
        assert len(second.observation_ids) == 3
        diff = await EpisodeReadService().diff(
            first.id, second.id, tenant_id=tenant_id, conn=conn
        )
        assert diff.added_observation_ids == (jira.id,)
        assert diff.removed_observation_ids == ()
        assert await conn.fetchval(
            "SELECT count(*) FROM episode_snapshots WHERE tenant_id=$1 AND episode_id=$2",
            tenant_id, episode_id,
        ) == 2
