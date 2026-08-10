from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.identity import (
    EntityMentionCreate,
    EntityMentionRepository,
    IdentityResolutionService,
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
    content_hash = hashlib.sha256(str(observation_id).encode()).hexdigest()
    await conn.execute(
        """
        INSERT INTO source_evidence (
          id, tenant_id, source, installation_scope, source_channel,
          source_object_type, source_object_id, source_revision_id, operation,
          source_recorded_at, content_hash, raw_ingested_at, normalized_at,
          ingress_kind, contract_version, connector_version, parser_version,
          normalizer_version, raw_retention_state, access_policy,
          access_policy_hash
        ) VALUES (
          $1, $2, 'slack', 'slack:alpen', 'slack:message', 'message',
          'C1:200', '200', 'create', $3, $4, $3, $3, 'webhook',
          1, '1.0.0', 'slack-v1', 'normalizer-v1', 'not_stored',
          '{"visibility":"tenant","audience":[],"source_acl_version":"v1"}'::jsonb,
          $5
        )
        """,
        evidence_id,
        tenant_id,
        occurred_at,
        content_hash,
        hashlib.sha256(b"tenant-policy").hexdigest(),
    )
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel, source_actor_ref,
          content, content_text, trust_tier, evidence_id
        ) VALUES ($1, $2, $3, 'signal', 'slack:message', 'slack:U42',
                  '{}'::jsonb, 'Simanta updated the audit', 'attested_agent', $4)
        """,
        observation_id,
        tenant_id,
        occurred_at,
        evidence_id,
    )
    return observation_id, evidence_id, occurred_at


async def test_mapped_source_principal_seals_accepted_identity_snapshot(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    policy_hash = hashlib.sha256(b"tenant-policy").hexdigest()
    async with fresh_db.acquire() as conn:
        observation_id, evidence_id, occurred_at = await _seed_observation(
            conn, tenant_id
        )
        await conn.execute(
            """
            INSERT INTO actors (
              id, tenant_id, type, display_name, status, metadata
            ) VALUES ($1, $2, 'human_internal', 'Simanta', 'active', '{}'::jsonb)
            """,
            actor_id,
            tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO actor_identity_mappings (
              actor_id, tenant_id, installation_scope, source_channel,
              source_actor_ref, confidence
            ) VALUES ($1, $2, 'slack:alpen', 'slack', 'slack:U42', 1.0)
            """,
            actor_id,
            tenant_id,
        )
        source_ref = await SourceReferenceRepository().register(
            SourceReferenceCreate(
                tenant_id=tenant_id,
                installation_scope="slack:alpen",
                source="slack",
                native_type="user",
                native_id="U42",
                reference_kind="principal",
                attributes={
                    "source_channel": "slack",
                    "source_actor_ref": "slack:U42",
                },
                evidence_id=evidence_id,
            ),
            conn=conn,
        )
        mention = await EntityMentionRepository().register(
            EntityMentionCreate(
                tenant_id=tenant_id,
                observation_id=observation_id,
                observation_occurred_at=occurred_at,
                evidence_id=evidence_id,
                source_reference_id=source_ref.id,
                mention_kind="source_actor",
                text="Simanta",
                expected_types=("person",),
            ),
            conn=conn,
        )
        run = await ResolutionRunRepository().start(
            ResolutionRunCreate(
                tenant_id=tenant_id,
                input_kind="observation",
                observation_id=observation_id,
                observation_occurred_at=occurred_at,
                input_hash=hashlib.sha256(b"mapped-principal-run").hexdigest(),
                resolver_name="fyralis-identity",
                resolver_version="1.0.0",
                policy_version="source-grounded-v1",
                capability_snapshot=capability_snapshot(),
            ),
            conn=conn,
        )
        snapshot = await IdentityResolutionService().resolve(
            run=run,
            mentions=[mention],
            access_policy_hash=policy_hash,
            conn=conn,
            evaluated_at=occurred_at,
        )
        completed = await ResolutionRunRepository().finish(
            run.id,
            tenant_id=tenant_id,
            status="completed",
            result_hash=snapshot.snapshot_hash,
            conn=conn,
        )

        assert snapshot.resolution_status == "complete"
        assert snapshot.items[0].outcome == "resolved"
        assert snapshot.items[0].selected_ref == {
            "type": "actor",
            "id": str(actor_id),
        }
        assert completed.result_hash == snapshot.snapshot_hash
        assert await conn.fetchval(
            "SELECT status FROM identity_assertions WHERE id = $1",
            snapshot.items[0].assertion_id,
        ) == "accepted"
        assert await conn.fetchval(
            "SELECT count(*) FROM identity_resolution_candidates WHERE resolver_run_id = $1",
            run.id,
        ) >= 1

        with pytest.raises(asyncpg.RaiseError, match="snapshots are immutable"):
            await conn.execute(
                "UPDATE identity_resolution_snapshots SET resolution_status = 'partial' "
                "WHERE id = $1",
                snapshot.id,
            )
