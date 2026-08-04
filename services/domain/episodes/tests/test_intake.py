from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import asyncpg
import pytest

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ObservationRow, SourceEvidenceCreate
from services.domain.episodes.intake import EpisodeIntakeRepository
from services.domain.evidence.repo import SourceEvidenceRepository
from services.domain.identity import (
    IdentityResolutionRepository,
    IdentityResolutionSnapshot,
    ResolutionRunCreate,
    ResolutionRunRepository,
    capability_snapshot,
)


pytestmark = pytest.mark.integration


async def _seed_observation(conn: asyncpg.Connection, tenant_id) -> ObservationRow:
    now = datetime.now(tz=timezone.utc)
    evidence = await SourceEvidenceRepository().insert(
        SourceEvidenceCreate(
            tenant_id=tenant_id,
            source="notion",
            installation_scope="stateless:notion",
            source_channel="notion:page",
            source_object_type="page",
            source_object_id="audit-map",
            source_revision_id="r1",
            source_recorded_at=now,
            raw_object_key="raw/audit-map.json",
            content_hash=hashlib.sha256(b"audit state").hexdigest(),
            raw_ingested_at=now,
            normalized_at=now,
            ingress_kind="poll",
            access_policy={
                "visibility": "tenant",
                "audience": [],
                "source_acl_version": "test-v1",
            },
        ),
        conn=conn,
    )
    row = await conn.fetchrow(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel,
          content, content_text, embedding_pending, trust_tier,
          external_id, entities_mentioned, evidence_id
        ) VALUES (
          $1, $2, $3, 'signal', 'notion:page', '{}'::jsonb,
          'audit state', TRUE, 'authoritative', 'audit-map:r1', '[]'::jsonb, $4
        )
        RETURNING id, tenant_id, occurred_at, ingested_at, kind,
          source_channel, source_actor_ref, actor_id, content, content_text,
          embedding, embedding_pending, trust_tier, external_id, cause_id,
          sequence_num, entities_mentioned, evidence_id
        """,
        uuid7(),
        tenant_id,
        now,
        evidence.evidence.id,
    )
    assert row is not None
    value = dict(row)
    if isinstance(value["content"], str):
        import json

        value["content"] = json.loads(value["content"])
        value["entities_mentioned"] = json.loads(value["entities_mentioned"])
    return ObservationRow.model_validate(value)


async def _identity_snapshot(
    conn: asyncpg.Connection, observation: ObservationRow
) -> IdentityResolutionSnapshot:
    run = await ResolutionRunRepository().start(
        ResolutionRunCreate(
            tenant_id=observation.tenant_id,
            input_kind="observation",
            observation_id=observation.id,
            observation_occurred_at=observation.occurred_at,
            input_hash=hashlib.sha256(f"episode:{observation.id}".encode()).hexdigest(),
            resolver_name="fyralis-identity",
            resolver_version="1.0.0",
            policy_version="source-grounded-v1",
            capability_snapshot=capability_snapshot(),
        ),
        conn=conn,
    )
    snapshot = IdentityResolutionSnapshot.seal(
        id=uuid7(),
        tenant_id=observation.tenant_id,
        resolver_run_id=run.id,
        input_kind="observation",
        observation_id=observation.id,
        observation_occurred_at=observation.occurred_at,
        resolution_status="complete",
        items=(),
        resolver_name="fyralis-identity",
        resolver_version="1.0.0",
        policy_version="source-grounded-v1",
        created_at=observation.occurred_at,
    )
    snapshot = await IdentityResolutionRepository().persist_snapshot(
        snapshot, assertion_ids=[], conn=conn
    )
    await ResolutionRunRepository().finish(
        run.id,
        tenant_id=observation.tenant_id,
        status="completed",
        result_hash=snapshot.snapshot_hash,
        conn=conn,
    )
    return snapshot


async def test_outbox_is_idempotent_leased_and_retryable(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    repo = EpisodeIntakeRepository()
    async with fresh_db.acquire() as conn:
        observation = await _seed_observation(conn, tenant_id)
        snapshot = await _identity_snapshot(conn, observation)
        first = await repo.enqueue_identity_resolved(observation, snapshot, conn=conn)
        replay = await repo.enqueue_identity_resolved(observation, snapshot, conn=conn)
        assert replay.id == first.id

        claimed = await repo.claim(
            worker_id="constructor-1",
            batch_size=10,
            lease_seconds=30,
            conn=conn,
        )
        assert [item.id for item in claimed] == [first.id]
        assert await repo.claim(
            worker_id="constructor-2",
            batch_size=10,
            lease_seconds=30,
            conn=conn,
        ) == []

        retried = await repo.retry(
            first.id,
            tenant_id=tenant_id,
            worker_id="constructor-1",
            error="transient router failure",
            delay_seconds=0,
            max_attempts=3,
            conn=conn,
        )
        assert retried.status == "pending"
        second_claim = await repo.claim(
            worker_id="constructor-2",
            batch_size=1,
            lease_seconds=30,
            conn=conn,
        )
        assert second_claim[0].attempt_count == 2
        completed = await repo.complete(
            first.id,
            tenant_id=tenant_id,
            worker_id="constructor-2",
            conn=conn,
        )
        assert completed.status == "completed"
        assert completed.completed_at is not None


async def test_outbox_enqueue_rolls_back_with_observation_transaction(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    async with fresh_db.acquire() as conn:
        observation = await _seed_observation(conn, tenant_id)
        snapshot = await _identity_snapshot(conn, observation)
        transaction = conn.transaction()
        await transaction.start()
        await EpisodeIntakeRepository().enqueue_identity_resolved(
            observation, snapshot, conn=conn
        )
        await transaction.rollback()
        count = await conn.fetchval(
            "SELECT count(*) FROM perception_outbox WHERE tenant_id = $1",
            tenant_id,
        )
    assert count == 0


async def test_outbox_completion_requires_lease_owner(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    repo = EpisodeIntakeRepository()
    async with fresh_db.acquire() as conn:
        observation = await _seed_observation(conn, tenant_id)
        snapshot = await _identity_snapshot(conn, observation)
        item = await repo.enqueue_identity_resolved(observation, snapshot, conn=conn)
        await repo.claim(
            worker_id="constructor-1",
            batch_size=1,
            lease_seconds=30,
            conn=conn,
        )
        with pytest.raises(ValidationError, match="not leased"):
            await repo.complete(
                item.id,
                tenant_id=tenant_id,
                worker_id="constructor-2",
                conn=conn,
            )
        dead = await repo.retry(
            item.id,
            tenant_id=tenant_id,
            worker_id="constructor-1",
            error="non-retryable contract failure",
            delay_seconds=0,
            max_attempts=1,
            conn=conn,
        )
    assert dead.status == "dead_letter"


async def test_outbox_rejects_cross_tenant_evidence(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    async with fresh_db.acquire() as conn:
        observation = await _seed_observation(conn, tenant_id)
        snapshot = await _identity_snapshot(conn, observation)
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await EpisodeIntakeRepository().enqueue_ready(
                tenant_id=uuid7(),
                observation_id=observation.id,
                observation_occurred_at=observation.occurred_at,
                evidence_id=observation.evidence_id,
                source_channel=observation.source_channel,
                kind=observation.kind,
                trust_tier=observation.trust_tier,
                actor_id=observation.actor_id,
                identity_snapshot_id=snapshot.id,
                identity_snapshot_hash=snapshot.snapshot_hash,
                identity_resolution_status=snapshot.resolution_status,
                conn=conn,
            )
