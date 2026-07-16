from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.source_identity_bindings import SourceIdentityBindingRepo


pytestmark = pytest.mark.integration


async def _seed_source_object(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    source_channel: str,
    occurred_at: datetime,
) -> tuple[UUID, UUID]:
    observation_id = uuid7()
    resource_id = uuid7()
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute("INSERT INTO tenants (id) VALUES ($1)", tenant_id)
        await conn.execute(
            """
            INSERT INTO resources (
                id, tenant_id, kind, identity, current_value, metadata
            ) VALUES (
                $1, $2, 'infrastructure', 'Bound source object',
                '{"name":"Bound source object"}'::jsonb,
                '{"semantic_kind":"system"}'::jsonb
            )
            """,
            resource_id,
            tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO observations (
                id, tenant_id, occurred_at, kind, source_channel,
                content, content_text, trust_tier, external_id,
                entities_mentioned
            ) VALUES (
                $1, $2, $3, 'signal', $4, '{}'::jsonb,
                'Mercury is degraded', 'authoritative',
                'source-event-version-42', '[]'::jsonb
            )
            """,
            observation_id,
            tenant_id,
            occurred_at,
            source_channel,
        )
    return observation_id, resource_id


async def test_binding_requires_explicit_observation_attachment(
    resolver_db: asyncpg.Pool,
) -> None:
    now = datetime.now(timezone.utc)
    tenant_id = uuid7()
    observation_id, resource_id = await _seed_source_object(
        resolver_db,
        tenant_id=tenant_id,
        source_channel="pagerduty:webhook",
        occurred_at=now,
    )
    repo = SourceIdentityBindingRepo(resolver_db)
    binding = await repo.bind(
        tenant_id=tenant_id,
        source_system="pagerduty",
        source_native_identifier="pagerduty:service:mercury-msg",
        source_identity_authority_ref="pagerduty-service-object-contract-v1",
        canonical_ref={"type": "resource", "id": str(resource_id)},
        evidence_refs=("pagerduty-service:mercury-msg",),
        valid_from=now - timedelta(days=1),
    )
    duplicate = await repo.bind(
        tenant_id=tenant_id,
        source_system="pagerduty",
        source_native_identifier="pagerduty:service:mercury-msg",
        source_identity_authority_ref="pagerduty-service-object-contract-v1",
        canonical_ref={"type": "resource", "id": str(resource_id)},
        evidence_refs=("pagerduty-service:mercury-msg",),
        valid_from=now - timedelta(days=1),
    )
    assert duplicate == binding

    assert await repo.resolve_observation_source(
        tenant_id=tenant_id,
        observation_id=observation_id,
        valid_at=now,
    ) is None
    assert await repo.resolve_observation_source(
        tenant_id=tenant_id,
        observation_id=observation_id,
        valid_at=now - timedelta(days=2),
    ) is None

    wrong_system_binding = await repo.bind(
        tenant_id=tenant_id,
        source_system="jira",
        source_native_identifier="jira:service:mercury-msg",
        source_identity_authority_ref="jira-service-object-contract-v1",
        canonical_ref={"type": "resource", "id": str(resource_id)},
        evidence_refs=("jira-service:mercury-msg",),
        valid_from=now - timedelta(days=1),
    )
    with pytest.raises(
        ValueError,
        match="system does not match observation source",
    ):
        await repo.attach_to_observation(
            tenant_id=tenant_id,
            observation_id=observation_id,
            binding=wrong_system_binding,
            attachment_authority_ref="jira-ingestion-envelope-v1",
        )

    await repo.attach_to_observation(
        tenant_id=tenant_id,
        observation_id=observation_id,
        binding=binding,
        attachment_authority_ref="pagerduty-ingestion-envelope-v1",
    )
    resolved = await repo.resolve_observation_source(
        tenant_id=tenant_id,
        observation_id=observation_id,
        valid_at=now,
    )
    assert resolved is not None
    assert resolved.binding == binding
    assert (
        resolved.attachment_authority_ref
        == "pagerduty-ingestion-envelope-v1"
    )
    assert resolved.canonical_ref == {
        "type": "resource",
        "id": str(resource_id),
        "version": 1,
    }


async def test_multiple_attached_bindings_fail_closed(
    resolver_db: asyncpg.Pool,
) -> None:
    now = datetime.now(timezone.utc)
    tenant_id = uuid7()
    observation_id, first_resource = await _seed_source_object(
        resolver_db,
        tenant_id=tenant_id,
        source_channel="jira:webhook",
        occurred_at=now,
    )
    second_resource = uuid7()
    async with resolver_db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO resources (
                id, tenant_id, kind, identity, current_value, metadata
            ) VALUES (
                $1, $2, 'relational', 'Second source object',
                '{"name":"Second source object"}'::jsonb,
                '{"semantic_kind":"team"}'::jsonb
            )
            """,
            second_resource,
            tenant_id,
        )
    repo = SourceIdentityBindingRepo(resolver_db)
    first = await repo.bind(
        tenant_id=tenant_id,
        source_system="jira",
        source_native_identifier="jira:team:first",
        source_identity_authority_ref="jira-team-contract-v1",
        canonical_ref={"type": "resource", "id": str(first_resource)},
        evidence_refs=("jira-team:first",),
        valid_from=now - timedelta(days=1),
    )
    second = await repo.bind(
        tenant_id=tenant_id,
        source_system="jira",
        source_native_identifier="jira:team:second",
        source_identity_authority_ref="jira-team-contract-v1",
        canonical_ref={"type": "resource", "id": str(second_resource)},
        evidence_refs=("jira-team:second",),
        valid_from=now - timedelta(days=1),
    )
    for binding in (first, second):
        await repo.attach_to_observation(
            tenant_id=tenant_id,
            observation_id=observation_id,
            binding=binding,
            attachment_authority_ref="jira-ingestion-envelope-v1",
        )

    assert await repo.resolve_observation_source(
        tenant_id=tenant_id,
        observation_id=observation_id,
        valid_at=now,
    ) is None
