from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ObservationRow, SourceEvidenceCreate
from services.domain.evidence.repo import SourceEvidenceRepository
from services.domain.identity import (
    IdentityIntakeRepository,
    IdentityLifecycleService,
    QueryIdentityResolutionService,
    extract_query_mentions,
)
from services.domain.identity.worker import IdentityResolutionWorker
from services.domain.perception.knowledge import PerceptionKnowledgeWorker


pytestmark = pytest.mark.integration


async def _seed_audit_observation(
    conn: asyncpg.Connection, tenant_id, actor_id
) -> ObservationRow:
    fixture = json.loads(
        (Path(__file__).parent / "fixtures" / "alpen_audit_week.json").read_text()
    )
    now = datetime.now(UTC)
    evidence = await SourceEvidenceRepository().insert(
        SourceEvidenceCreate(
            tenant_id=tenant_id,
            source="slack",
            installation_scope=fixture["installation_scope"],
            source_channel="slack:message",
            source_object_type="message",
            source_object_id="C-AUDIT:100",
            source_revision_id="100",
            operation="create",
            source_recorded_at=now,
            content_hash=hashlib.sha256(fixture["observation"].encode()).hexdigest(),
            raw_ingested_at=now,
            normalized_at=now,
            ingress_kind="webhook",
            raw_retention_state="not_stored",
            access_policy={
                "visibility": "tenant",
                "audience": [],
                "source_acl_version": "slack-test-v1",
            },
        ),
        conn=conn,
    )
    row = await conn.fetchrow(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel, source_actor_ref,
          actor_id, content, content_text, embedding_pending, trust_tier,
          entities_mentioned, evidence_id
        ) VALUES (
          $1, $2, $3, 'signal', 'slack:message', 'U42', $4, $5::jsonb,
          $6, TRUE, 'attested_agent', '[]'::jsonb, $7
        ) RETURNING id, tenant_id, occurred_at, ingested_at, kind,
          source_channel, source_actor_ref, actor_id, content, content_text,
          embedding, embedding_pending, trust_tier, external_id, cause_id,
          sequence_num, entities_mentioned, evidence_id
        """,
        uuid7(),
        tenant_id,
        now,
        actor_id,
        json.dumps({"_unresolved_phrases": fixture["unresolved_phrases"]}),
        fixture["observation"],
        evidence.evidence.id,
    )
    assert row is not None
    value = dict(row)
    for key in ("content", "entities_mentioned"):
        if isinstance(value[key], str):
            value[key] = json.loads(value[key])
    return ObservationRow.model_validate(value)


async def test_audit_week_flows_through_identity_before_episode_and_can_reprocess(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status, metadata) "
            "VALUES ($1, $2, 'human_internal', 'Simanta', 'active', '{}'::jsonb)",
            actor_id,
            tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO actor_identity_mappings (
              actor_id, tenant_id, installation_scope, source_channel,
              source_actor_ref, confidence
            ) VALUES ($1, $2, 'slack:alpen-workspace', 'slack', 'U42', 1.0)
            """,
            actor_id,
            tenant_id,
        )
        observation = await _seed_audit_observation(conn, tenant_id, actor_id)
        item = await IdentityIntakeRepository().enqueue_observation_ready(
            observation, conn=conn
        )
        assert await conn.fetchval(
            "SELECT count(*) FROM perception_outbox WHERE tenant_id = $1", tenant_id
        ) == 0

    assert await IdentityResolutionWorker(fresh_db).run_once(
        worker_id="identity-1", batch_size=10
    ) == 1

    assert await PerceptionKnowledgeWorker(fresh_db).run_once(
        worker_id="knowledge-1", batch_size=10
    ) >= 1

    async with fresh_db.acquire() as conn:
        completed = await conn.fetchrow(
            "SELECT status FROM identity_resolution_outbox WHERE id = $1", item.id
        )
        episode_handoff = await conn.fetchrow(
            """
            SELECT contract_version, identity_snapshot_id,
                   identity_snapshot_hash, identity_resolution_status
              FROM perception_outbox
             WHERE tenant_id = $1 AND observation_id = $2
            """,
            tenant_id,
            observation.id,
        )
        assert completed["status"] == "completed"
        assert episode_handoff is not None
        assert episode_handoff["contract_version"] == 3
        assert episode_handoff["identity_snapshot_id"] is not None
        assert episode_handoff["identity_resolution_status"] == "partial"
        assert len(episode_handoff["identity_snapshot_hash"]) == 64

        assertion_id = await conn.fetchval(
            """
            SELECT a.id FROM identity_assertions a
            JOIN identity_dependents d
              ON d.tenant_id = a.tenant_id AND d.identity_assertion_id = a.id
             WHERE a.tenant_id = $1 AND a.status = 'accepted'
               AND d.dependent_kind = 'observation' AND d.dependent_id = $2
             ORDER BY a.created_at LIMIT 1
            """,
            tenant_id,
            observation.id,
        )
        assert assertion_id is not None
        reruns = await IdentityLifecycleService(fresh_db).request_reresolution(
            [assertion_id],
            tenant_id=tenant_id,
            reason="human_corrected_principal",
            conn=conn,
        )
        assert len(reruns) == 1
        assert reruns[0].event_kind == "identity.reresolution_requested"

    assert await IdentityResolutionWorker(fresh_db).run_once(
        worker_id="identity-2", batch_size=10
    ) == 1
    assert await PerceptionKnowledgeWorker(fresh_db).run_once(
        worker_id="knowledge-2", batch_size=10
    ) >= 1
    async with fresh_db.acquire() as conn:
        deliveries = await conn.fetchval(
            "SELECT count(*) FROM perception_outbox WHERE tenant_id = $1 "
            "AND observation_id = $2",
            tenant_id,
            observation.id,
        )
        assert deliveries == 2


async def test_query_resolution_is_auditable_but_does_not_create_assertions(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    requester_id = uuid7()
    simanta_id = uuid7()
    async with fresh_db.acquire() as conn:
        await conn.executemany(
            "INSERT INTO actors (id, tenant_id, type, display_name, status, metadata) "
            "VALUES ($1, $2, 'human_internal', $3, 'active', '{}'::jsonb)",
            [
                (requester_id, tenant_id, "CEO"),
                (simanta_id, tenant_id, "Simanta"),
            ],
        )
        before = await conn.fetchval(
            "SELECT count(*) FROM identity_assertions WHERE tenant_id = $1", tenant_id
        )
        result = await QueryIdentityResolutionService().resolve_query(
            "What did Simanta say about Audit Week?",
            tenant_id=tenant_id,
            requester_actor_id=requester_id,
            mention_texts=("Simanta", "Audit Week"),
            conn=conn,
        )
        after = await conn.fetchval(
            "SELECT count(*) FROM identity_assertions WHERE tenant_id = $1", tenant_id
        )
        assert before == after == 0
        assert result.topic_seed == "What did Simanta say about Audit Week?"
        assert result.snapshot.input_kind == "query"
        assert result.snapshot.requester_actor_id == requester_id
        assert result.snapshot.items[0].selected_ref == {
            "type": "actor",
            "id": str(simanta_id),
        }
        assert result.snapshot.items[1].outcome == "unresolved"


def test_query_mention_extraction_is_conservative() -> None:
    assert extract_query_mentions('What is "Audit Week" status according to Simanta?') == (
        "Audit Week",
        "Simanta",
    )
