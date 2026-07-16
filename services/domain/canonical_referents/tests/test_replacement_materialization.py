from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.errors import InvariantViolation
from services.domain.canonical_referents.replacement import (
    CanonicalResourceReplacementOrchestrator,
)
from services.domain.canonical_referents.service import (
    CanonicalReferentRegistryService,
)
from services.domain.canonical_referents.types import (
    CanonicalReferentReplacementCommand,
    CanonicalReferentVersionRef,
)
from services.domain.entity_aliases.repo import (
    EntityAliasRepo,
    insert_alias_with_connection,
)
from services.domain.source_identity_bindings.repo import (
    SourceIdentityBindingRepo,
)


pytestmark = pytest.mark.integration

TENANT_ID = UUID("71111111-1111-1111-1111-111111111111")
OTHER_TENANT_ID = UUID("72222222-2222-2222-2222-222222222222")


async def _install_json_codecs(conn: asyncpg.Connection) -> None:
    for type_name in ("json", "jsonb"):
        await conn.set_type_codec(
            type_name,
            encoder=lambda value: (
                value if isinstance(value, str) else json.dumps(value)
            ),
            decoder=json.loads,
            schema="pg_catalog",
        )


async def _prepare_pool(pool: asyncpg.Pool) -> None:
    # The top-level integration fixture starts with one idle connection.
    # Installing codecs on that connection is sufficient because this proof
    # deliberately runs one transaction at a time.
    async with pool.acquire() as conn:
        await _install_json_codecs(conn)


def _ref(resource_id: UUID) -> CanonicalReferentVersionRef:
    return CanonicalReferentVersionRef(
        type="resource",
        id=str(resource_id),
        version=1,
    )


def _command(
    *,
    predecessor_id: UUID,
    successor_id: UUID,
    effective_at: datetime,
    cause_event_id: UUID,
    operation_ref: str = "replace:legacy-billing:v1",
    tenant_id: UUID = TENANT_ID,
) -> CanonicalReferentReplacementCommand:
    return CanonicalReferentReplacementCommand(
        tenant_id=tenant_id,
        operation_ref=operation_ref,
        predecessor=_ref(predecessor_id),
        successor=_ref(successor_id),
        expected_predecessor_version=1,
        effective_at=effective_at,
        authority_ref="review:canonical-resource-replacement:1",
        reason="The governed billing platform replaced the legacy system.",
        evidence_refs=("observation:replacement", "review:approved"),
        cause_event_id=cause_event_id,
    )


@pytest.mark.asyncio
async def test_replacement_materializes_every_current_surface_and_preserves_history(
    fresh_db: asyncpg.Pool,
) -> None:
    await _prepare_pool(fresh_db)
    now = datetime.now(timezone.utc)
    created_at = now - timedelta(days=3)
    effective_at = now - timedelta(minutes=5)
    historical_at = effective_at - timedelta(hours=1)
    predecessor_id = uuid7()
    successor_id = uuid7()
    alternative_successor_id = uuid7()
    cause_event_id = uuid7()
    delayed_observation_id = uuid7()
    model_id = uuid7()
    source_repo = SourceIdentityBindingRepo(fresh_db)

    async with fresh_db.acquire() as conn, conn.transaction():
        await _seed_tenant(conn, TENANT_ID)
        await _seed_tenant(conn, OTHER_TENANT_ID)
        await _seed_observation(
            conn,
            observation_id=cause_event_id,
            occurred_at=now,
            source_channel="review:canonical-replacement",
        )
        await _seed_observation(
            conn,
            observation_id=delayed_observation_id,
            occurred_at=historical_at,
            source_channel="jira:project",
        )
        await _seed_resource(
            conn,
            resource_id=predecessor_id,
            identity="Legacy Billing",
            created_at=created_at,
        )
        await _seed_resource(
            conn,
            resource_id=successor_id,
            identity="Billing Platform",
            created_at=created_at + timedelta(hours=1),
        )
        await _seed_resource(
            conn,
            resource_id=alternative_successor_id,
            identity="Alternative Billing Platform",
            created_at=created_at + timedelta(hours=2),
        )
        await insert_alias_with_connection(
            conn,
            phrase="legacy billing",
            resolved_entity_ref=_ref(predecessor_id).model_dump(mode="json"),
            source="resource_lifecycle",
            confidence=1.0,
            tenant_id=TENANT_ID,
            is_canonical=True,
            valid_from=created_at,
            source_event_id=cause_event_id,
        )
        await insert_alias_with_connection(
            conn,
            phrase="billing platform",
            resolved_entity_ref=_ref(successor_id).model_dump(mode="json"),
            source="resource_lifecycle",
            confidence=1.0,
            tenant_id=TENANT_ID,
            is_canonical=True,
            valid_from=created_at + timedelta(hours=1),
            source_event_id=cause_event_id,
        )
        binding = await source_repo.bind(
            tenant_id=TENANT_ID,
            source_system="jira",
            source_native_identifier="jira:project:10042",
            source_identity_authority_ref="jira:installation:test",
            canonical_ref=_ref(predecessor_id).model_dump(mode="json"),
            evidence_refs=("jira:project:10042",),
            valid_from=created_at,
            transaction_from=created_at,
            conn=conn,
        )
        await source_repo.attach_to_observation(
            tenant_id=TENANT_ID,
            observation_id=delayed_observation_id,
            binding=binding,
            source_surface="Legacy Billing",
            attachment_authority_ref="jira:installation:test",
            conn=conn,
        )
        await _seed_model_and_projection(
            conn,
            model_id=model_id,
            observation_id=cause_event_id,
            predecessor_id=predecessor_id,
        )

    report = await CanonicalResourceReplacementOrchestrator(fresh_db).apply(
        _command(
            predecessor_id=predecessor_id,
            successor_id=successor_id,
            effective_at=effective_at,
            cause_event_id=cause_event_id,
        )
    )

    assert report.transition.applied is True
    assert report.predecessor_retired is True
    assert report.closed_alias_count == 1
    assert report.superseded_binding_lineages == (binding.binding_lineage_id,)
    assert len(report.projection_fence.invalidated_subjects) == 1
    assert len(report.projection_fence.refresh_job_ids) == 1
    assert report.lineage.members == (
        _ref(predecessor_id),
        _ref(successor_id),
    )

    aliases = EntityAliasRepo(fresh_db)
    assert (
        await aliases.fast_path_resolve(
            "legacy billing",
            TENANT_ID,
        )
        is None
    )
    assert await aliases.fast_path_resolve(
        "legacy billing",
        TENANT_ID,
        as_of=historical_at,
    ) == _ref(predecessor_id).model_dump(mode="json")
    assert await aliases.fast_path_resolve(
        "billing platform",
        TENANT_ID,
    ) == _ref(successor_id).model_dump(mode="json")

    current_binding = await source_repo.find_current_binding(
        tenant_id=TENANT_ID,
        source_system="jira",
        source_native_identifier="jira:project:10042",
    )
    assert current_binding is not None
    assert current_binding.canonical_referent_id == str(successor_id)
    historical_binding = await source_repo.find_visible_binding(
        tenant_id=TENANT_ID,
        source_system="jira",
        source_native_identifier="jira:project:10042",
        valid_at=historical_at,
        known_at=datetime.now(timezone.utc),
    )
    assert historical_binding is not None
    assert historical_binding.canonical_referent_id == str(predecessor_id)
    boundary_binding = await source_repo.find_visible_binding(
        tenant_id=TENANT_ID,
        source_system="jira",
        source_native_identifier="jira:project:10042",
        valid_at=effective_at,
        known_at=datetime.now(timezone.utc),
    )
    assert boundary_binding is not None
    assert boundary_binding.canonical_referent_id == str(successor_id)
    assert await source_repo.resolve_observation_source(
        tenant_id=TENANT_ID,
        observation_id=delayed_observation_id,
        phrase="Legacy Billing",
        valid_at=historical_at,
        known_at=datetime.now(timezone.utc),
    ) is None

    registry = CanonicalReferentRegistryService(fresh_db)
    before_lineage = await registry.lineage_at(
        tenant_id=TENANT_ID,
        referent=_ref(predecessor_id),
        valid_at=effective_at - timedelta(microseconds=1),
        known_at=datetime.now(timezone.utc),
    )
    assert before_lineage.members == (_ref(predecessor_id),)

    async with fresh_db.acquire() as conn:
        predecessor_archived_at = await conn.fetchval(
            "SELECT archived_at FROM resources WHERE id=$1",
            predecessor_id,
        )
        successor_archived_at = await conn.fetchval(
            "SELECT archived_at FROM resources WHERE id=$1",
            successor_id,
        )
        attachment = await conn.fetchrow(
            """
            SELECT binding_id, binding_version
            FROM observation_source_identity_bindings
            WHERE tenant_id=$1 AND observation_id=$2
            """,
            TENANT_ID,
            delayed_observation_id,
        )
        scoped_model_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM model_scope_entities
            WHERE tenant_id=$1 AND model_id=$2
              AND entity_type='resource' AND entity_id=$3
            """,
            TENANT_ID,
            model_id,
            predecessor_id,
        )
        snapshot_count = await conn.fetchval(
            """
            SELECT count(*) FROM projection_snapshots
            WHERE tenant_id=$1 AND subject_key=$2
            """,
            TENANT_ID,
            f"resource:{predecessor_id}",
        )
        dependency_count = await conn.fetchval(
            """
            SELECT count(*) FROM projection_dependencies
            WHERE tenant_id=$1 AND subject_key=$2
            """,
            TENANT_ID,
            f"resource:{predecessor_id}",
        )
        refresh_jobs = await conn.fetch(
            """
            SELECT payload, status
            FROM projection_refresh_jobs
            WHERE tenant_id=$1 AND subject_key=$2
            """,
            TENANT_ID,
            f"resource:{predecessor_id}",
        )
        transition_reason = await conn.fetchval(
            """
            SELECT reason FROM canonical_referent_transitions
            WHERE tenant_id=$1 AND operation_ref=$2
            """,
            TENANT_ID,
            "replace:legacy-billing:v1",
        )
        source_content = await conn.fetchval(
            """
            SELECT content_text FROM observations
            WHERE tenant_id=$1 AND id=$2
            """,
            TENANT_ID,
            delayed_observation_id,
        )
        model_state = await conn.fetchrow(
            """
            SELECT status, "natural", scope_entities
            FROM models WHERE tenant_id=$1 AND id=$2
            """,
            TENANT_ID,
            model_id,
        )

    assert predecessor_archived_at == effective_at
    assert successor_archived_at is None
    assert attachment["binding_id"] == UUID(binding.binding_id)
    assert attachment["binding_version"] == binding.binding_version
    assert scoped_model_count == 1
    assert snapshot_count == 0
    assert dependency_count == 0
    assert len(refresh_jobs) == 1
    assert refresh_jobs[0]["status"] == "pending"
    assert refresh_jobs[0]["payload"]["correction_kind"] == (
        "canonical_referent_replaced"
    )
    assert transition_reason == (
        "The governed billing platform replaced the legacy system."
    )
    assert source_content == "canonical replacement evidence"
    assert model_state["status"] == "active"
    assert model_state["natural"] == (
        "The legacy billing system is operational."
    )
    assert model_state["scope_entities"] == [
        {"type": "resource", "id": str(predecessor_id)}
    ]

    replay = await CanonicalResourceReplacementOrchestrator(fresh_db).apply(
        _command(
            predecessor_id=predecessor_id,
            successor_id=successor_id,
            effective_at=effective_at,
            cause_event_id=cause_event_id,
        )
    )
    assert replay.transition.applied is False
    assert replay.predecessor_retired is False
    assert replay.closed_alias_count == 0
    assert replay.superseded_binding_lineages == ()
    assert replay.projection_fence.invalidated_subjects == ()

    async with fresh_db.acquire() as conn:
        assert await conn.fetchval(
            """
            SELECT count(*) FROM canonical_referent_transitions
            WHERE tenant_id=$1
            """,
            TENANT_ID,
        ) == 1
        assert await conn.fetchval(
            """
            SELECT count(*) FROM projection_refresh_jobs
            WHERE tenant_id=$1 AND subject_key=$2
            """,
            TENANT_ID,
            f"resource:{predecessor_id}",
        ) == 1

    with pytest.raises(
        InvariantViolation,
        match="operation_ref already names a different replacement request",
    ):
        await CanonicalResourceReplacementOrchestrator(fresh_db).apply(
            _command(
                predecessor_id=predecessor_id,
                successor_id=alternative_successor_id,
                effective_at=effective_at,
                cause_event_id=cause_event_id,
            )
        )
    with pytest.raises(
        InvariantViolation,
        match="replacement predecessor is no longer a lineage head",
    ):
        await CanonicalResourceReplacementOrchestrator(fresh_db).apply(
            _command(
                predecessor_id=predecessor_id,
                successor_id=alternative_successor_id,
                effective_at=effective_at + timedelta(microseconds=1),
                cause_event_id=cause_event_id,
                operation_ref="replace:stale-head:v1",
            )
        )
    with pytest.raises(
        InvariantViolation,
        match="tenant-local physical resources",
    ):
        await CanonicalResourceReplacementOrchestrator(fresh_db).apply(
            _command(
                predecessor_id=predecessor_id,
                successor_id=successor_id,
                effective_at=effective_at,
                cause_event_id=cause_event_id,
                operation_ref="replace:foreign-tenant:v1",
                tenant_id=OTHER_TENANT_ID,
            )
        )
    async with fresh_db.acquire() as conn:
        assert await conn.fetchval(
            """
            SELECT count(*) FROM canonical_referent_transitions
            WHERE tenant_id=$1
            """,
            OTHER_TENANT_ID,
        ) == 0


@pytest.mark.asyncio
async def test_downstream_failure_rolls_back_transition_and_every_repair(
    fresh_db: asyncpg.Pool,
) -> None:
    await _prepare_pool(fresh_db)
    now = datetime.now(timezone.utc)
    created_at = now - timedelta(days=2)
    effective_at = now - timedelta(minutes=2)
    predecessor_id = uuid7()
    successor_id = uuid7()
    cause_event_id = uuid7()

    async with fresh_db.acquire() as conn, conn.transaction():
        await _seed_tenant(conn, TENANT_ID)
        await _seed_observation(
            conn,
            observation_id=cause_event_id,
            occurred_at=now,
            source_channel="review:canonical-replacement",
        )
        await _seed_resource(
            conn,
            resource_id=predecessor_id,
            identity="Rollback Legacy",
            created_at=created_at,
        )
        await _seed_resource(
            conn,
            resource_id=successor_id,
            identity="Rollback Successor",
            created_at=created_at + timedelta(hours=1),
        )
        await insert_alias_with_connection(
            conn,
            phrase="rollback legacy",
            resolved_entity_ref=_ref(predecessor_id).model_dump(mode="json"),
            source="resource_lifecycle",
            confidence=1.0,
            tenant_id=TENANT_ID,
            valid_from=created_at,
            source_event_id=cause_event_id,
        )

    class _FailingProjectionAdapter:
        async def invalidate_for_canonical_referent(self, *_args, **_kwargs):
            raise RuntimeError("forced projection failure")

    orchestrator = CanonicalResourceReplacementOrchestrator(
        fresh_db,
        projection_adapter=_FailingProjectionAdapter(),  # type: ignore[arg-type]
    )
    with pytest.raises(RuntimeError, match="forced projection failure"):
        await orchestrator.apply(
            _command(
                predecessor_id=predecessor_id,
                successor_id=successor_id,
                effective_at=effective_at,
                cause_event_id=cause_event_id,
                operation_ref="replace:rollback:v1",
            )
        )

    async with fresh_db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT count(*) FROM canonical_referent_transitions"
        ) == 0
        assert await conn.fetchval(
            "SELECT archived_at FROM resources WHERE id=$1",
            predecessor_id,
        ) is None
        alias = await conn.fetchrow(
            """
            SELECT valid_until, validity_reason
            FROM entity_aliases
            WHERE tenant_id=$1 AND alias_text='rollback legacy'
            """,
            TENANT_ID,
        )
    assert alias["valid_until"] is None
    assert alias["validity_reason"] is None


async def _seed_tenant(conn: asyncpg.Connection, tenant_id: UUID) -> None:
    await conn.execute(
        """
        INSERT INTO tenants (id, name)
        VALUES ($1, $2)
        ON CONFLICT (id) DO NOTHING
        """,
        tenant_id,
        f"Replacement tenant {str(tenant_id)[:8]}",
    )


async def _seed_observation(
    conn: asyncpg.Connection,
    *,
    observation_id: UUID,
    occurred_at: datetime,
    source_channel: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel,
          content, content_text, trust_tier
        ) VALUES (
          $1, $2, $3, 'signal', $4, '{}'::jsonb,
          'canonical replacement evidence', 'authoritative'
        )
        """,
        observation_id,
        TENANT_ID,
        occurred_at,
        source_channel,
    )


async def _seed_resource(
    conn: asyncpg.Connection,
    *,
    resource_id: UUID,
    identity: str,
    created_at: datetime,
) -> None:
    await conn.execute(
        """
        INSERT INTO resources (
          id, tenant_id, kind, identity, description, current_value,
          valuation_confidence, utilization_state, controllability,
          temporal_character, metadata, created_at, last_updated_at
        ) VALUES (
          $1, $2, 'infrastructure', $3, $4, '{}'::jsonb,
          1.0, 'available', 'owned', 'permanent',
          jsonb_build_object('semantic_kind', 'system'),
          $5, $5
        )
        """,
        resource_id,
        TENANT_ID,
        identity,
        f"{identity} system resource",
        created_at,
    )


async def _seed_model_and_projection(
    conn: asyncpg.Connection,
    *,
    model_id: UUID,
    observation_id: UUID,
    predecessor_id: UUID,
) -> None:
    vector_literal = "[" + ",".join(["0"] * 768) + "]"
    proposition = {
        "kind": "belief",
        "claim_role": "fact",
        "abstraction_level": "atomic",
        "time_mode": "current",
        "modality": "observed",
        "polarity": "positive",
        "subject": f"resource:{predecessor_id}",
        "predicate": "is_operational",
        "object": True,
    }
    await conn.execute(
        """
        INSERT INTO models (
          id, tenant_id, born_from_event_id, proposition, "natural",
          embedding, scope_entities, scope_temporal, confidence,
          confidence_at_assertion
        ) VALUES (
          $1, $2, $3, $4::jsonb,
          'The legacy billing system is operational.',
          $5::vector, $6::jsonb, '{}'::jsonb, 0.6, 0.6
        )
        """,
        model_id,
        TENANT_ID,
        observation_id,
        proposition,
        vector_literal,
        [{"type": "resource", "id": str(predecessor_id)}],
    )
    await conn.execute(
        """
        INSERT INTO model_scope_entities (
          model_id, tenant_id, entity_type, entity_id, source, confidence
        ) VALUES ($1, $2, 'resource', $3, 'pytest', 1.0)
        ON CONFLICT DO NOTHING
        """,
        model_id,
        TENANT_ID,
        predecessor_id,
    )
    subject_key = f"resource:{predecessor_id}"
    await conn.execute(
        """
        INSERT INTO projection_snapshots (
          tenant_id, projection_name, projection_version, subject_key,
          payload, confidence, source_model_ids, source_event_ids
        ) VALUES (
          $1, 'resources', 'v1', $2, '{}'::jsonb, 0.8, $3::uuid[], $4::uuid[]
        )
        """,
        TENANT_ID,
        subject_key,
        [model_id],
        [observation_id],
    )
    await conn.execute(
        """
        INSERT INTO projection_dependencies (
          tenant_id, projection_name, projection_version, subject_key,
          ref_kind, ref_value, reason
        ) VALUES (
          $1, 'resources', 'v1', $2, 'model', $3, 'pytest'
        )
        """,
        TENANT_ID,
        subject_key,
        str(model_id),
    )
