from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.domain.source_identity_bindings import SourceIdentityBindingRepo
from services.workers.entity_resolver.context import build_context
from services.workers.entity_resolver.worker import EntityResolverWorker


pytestmark = pytest.mark.integration


class _ForbiddenProvider:
    def __init__(self) -> None:
        self.calls = 0

    async def structured(self, **_kwargs):
        self.calls += 1
        raise AssertionError("authenticated source identity must not invoke an LLM")


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
        phrase="Mercury",
        valid_at=now,
    ) is None

    assert await repo.resolve_observation_source(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="Mercury",
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
            source_surface="Mercury",
            attachment_authority_ref="jira-ingestion-envelope-v1",
        )

    await repo.attach_to_observation(
        tenant_id=tenant_id,
        observation_id=observation_id,
        binding=binding,
        source_surface="Mercury",
        attachment_authority_ref="pagerduty-ingestion-envelope-v1",
    )
    resolved = await repo.resolve_observation_source(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="  mercury  ",
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
    assert resolved.source_surface == "Mercury"
    assert await repo.resolve_observation_source(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="Mercury Billing",
        valid_at=now,
    ) is None

    matching_context = await build_context(
        pool=resolver_db,
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="Mercury",
    )
    nonmatching_context = await build_context(
        pool=resolver_db,
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="degraded",
    )
    assert matching_context.source_identity_binding is not None
    assert nonmatching_context.source_identity_binding is None
    source_inputs = EntityResolverWorker._candidate_inputs(
        matching_context
    )
    decisive = tuple(
        item
        for item in source_inputs
        if item.genuine_source_binding is not None
    )
    assert len(decisive) == 1
    assert decisive[0].canonical_ref == resolved.canonical_ref
    assert decisive[0].exact_mention_match is True

    provider = _ForbiddenProvider()
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,  # type: ignore[arg-type]
        alias_repo=EntityAliasRepo(resolver_db),
    )
    assert await worker._process_phrase(
        phrase="Mercury",
        observation_id=observation_id,
        tenant_id=tenant_id,
        conn=None,
    ) == "resolved"
    assert provider.calls == 0
    async with resolver_db.acquire() as conn:
        trace = await conn.fetchrow(
            """
            SELECT trace.current_fate, trace.selected_referent,
                   admission.decision AS admission_decision,
                   assessment.model_output,
                   assessment.scorer_and_calibration_version
            FROM grounding_traces trace
            JOIN resolution_assessments assessment
              ON assessment.tenant_id=trace.tenant_id
             AND assessment.id=trace.resolution_assessment_id
            JOIN grounding_admission_decisions admission
              ON admission.tenant_id=trace.tenant_id
             AND admission.id=trace.grounding_admission_id
            WHERE trace.tenant_id=$1
              AND trace.source_observation_id=$2
            """,
            tenant_id,
            observation_id,
        )
        alias_count = await conn.fetchval(
            "SELECT count(*) FROM entity_aliases WHERE tenant_id=$1",
            tenant_id,
        )
    assert trace is not None
    assert trace["current_fate"] == "resolved_for_consumer"
    assert trace["selected_referent"]["id"] == str(resource_id)
    admitted_binding = trace["admission_decision"]["genuine_source_binding"]
    assert admitted_binding["binding_id"] == binding.binding_id
    assert admitted_binding["binding_version"] == binding.binding_version
    assert trace["model_output"]["decision_source"] == (
        "authenticated_source_identity_binding"
    )
    assert trace["model_output"]["llm_invoked"] is False
    assert trace["scorer_and_calibration_version"] == (
        "authenticated-source-identity-binding-v1"
    )
    assert alias_count == 0

    matching_context.phrase = "Mercury Billing"
    assert all(
        item.genuine_source_binding is None
        for item in EntityResolverWorker._candidate_inputs(
            matching_context
        )
    )


async def test_surface_specific_bindings_resolve_independently_and_fail_closed(
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
    await repo.attach_to_observation(
        tenant_id=tenant_id,
        observation_id=observation_id,
        binding=first,
        source_surface="Orion Reliability",
        attachment_authority_ref="jira-ingestion-envelope-v1",
    )
    await repo.attach_to_observation(
        tenant_id=tenant_id,
        observation_id=observation_id,
        binding=second,
        source_surface="Orion Sales",
        attachment_authority_ref="jira-ingestion-envelope-v1",
    )

    first_resolved = await repo.resolve_observation_source(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="orion reliability",
        valid_at=now,
    )
    second_resolved = await repo.resolve_observation_source(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="ORION SALES",
        valid_at=now,
    )
    assert first_resolved is not None
    assert first_resolved.binding == first
    assert second_resolved is not None
    assert second_resolved.binding == second

    await repo.attach_to_observation(
        tenant_id=tenant_id,
        observation_id=observation_id,
        binding=second,
        source_surface="Orion Reliability",
        attachment_authority_ref="jira-ingestion-envelope-v1",
    )

    assert await repo.resolve_observation_source(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="Orion Reliability",
        valid_at=now,
    ) is None
