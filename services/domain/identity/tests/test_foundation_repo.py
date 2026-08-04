from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.identity import (
    EntityMentionCreate,
    EntityMentionRepository,
    ResolutionRunCreate,
    ResolutionRunRepository,
    SourceReferenceCreate,
    SourceReferenceRepository,
    capability_snapshot,
)


pytestmark = pytest.mark.integration


async def _seed_observation(conn: asyncpg.Connection, tenant_id):
    evidence_id = uuid7()
    observation_id = uuid7()
    occurred_at = datetime.now(UTC)
    await conn.execute(
        """
        INSERT INTO source_evidence (
          id, tenant_id, source, installation_scope, source_channel,
          source_object_type, source_object_id, source_revision_id, operation,
          source_recorded_at, content_hash, raw_ingested_at, normalized_at,
          ingress_kind, contract_version, connector_version, parser_version,
          normalizer_version, raw_retention_state
        ) VALUES (
          $1, $2, 'slack', 'slack:alpen', 'slack:message', 'message',
          'C1:100', '100', 'create', $3, $4, $3, $3, 'webhook',
          1, '1.0.0', 'slack-v1', 'normalizer-v1', 'not_stored'
        )
        """,
        evidence_id,
        tenant_id,
        occurred_at,
        hashlib.sha256(b"identity-foundation").hexdigest(),
    )
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel, content,
          content_text, trust_tier, evidence_id
        ) VALUES ($1, $2, $3, 'signal', 'slack:message', '{}'::jsonb,
                  'Simanta updated the audit', 'attested_agent', $4)
        """,
        observation_id,
        tenant_id,
        occurred_at,
        evidence_id,
    )
    return observation_id, evidence_id, occurred_at


async def test_foundation_repositories_are_idempotent_and_versioned(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    source_repo = SourceReferenceRepository()
    mention_repo = EntityMentionRepository()
    run_repo = ResolutionRunRepository()

    async with fresh_db.acquire() as conn:
        observation_id, evidence_id, occurred_at = await _seed_observation(
            conn, tenant_id
        )
        value = SourceReferenceCreate(
            tenant_id=tenant_id,
            installation_scope="slack:alpen",
            source="slack",
            native_type="user",
            native_id="U42",
            reference_kind="principal",
            attributes={"display_name": "Simanta"},
            evidence_id=evidence_id,
        )
        first = await source_repo.register(value, conn=conn)
        replay = await source_repo.register(value, conn=conn)
        assert first.id == replay.id
        assert replay.version == 1

        mention_value = EntityMentionCreate(
            tenant_id=tenant_id,
            observation_id=observation_id,
            observation_occurred_at=occurred_at,
            evidence_id=evidence_id,
            source_reference_id=first.id,
            mention_kind="source_actor",
            text="Simanta",
            expected_types=("person",),
        )
        mention = await mention_repo.register(mention_value, conn=conn)
        duplicate = await mention_repo.register(mention_value, conn=conn)
        assert duplicate.id == mention.id

        run_value = ResolutionRunCreate(
            tenant_id=tenant_id,
            input_kind="observation",
            observation_id=observation_id,
            observation_occurred_at=occurred_at,
            input_hash=hashlib.sha256(b"obs-run").hexdigest(),
            resolver_name="fyralis-identity",
            resolver_version="1",
            policy_version="1",
            capability_snapshot=capability_snapshot(),
        )
        run = await run_repo.start(run_value, conn=conn)
        duplicate_run = await run_repo.start(run_value, conn=conn)
        assert duplicate_run.id == run.id
        completed = await run_repo.finish(
            run.id,
            tenant_id=tenant_id,
            status="completed",
            result_hash=hashlib.sha256(b"result").hexdigest(),
            conn=conn,
        )
        assert completed.status == "completed"

        mentions = await mention_repo.for_observation(
            observation_id, tenant_id=tenant_id, conn=conn
        )
        assert [item.id for item in mentions] == [mention.id]
