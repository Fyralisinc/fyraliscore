from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.source_identity_bindings import SourceIdentityBindingRepo


pytestmark = pytest.mark.integration


async def _seed_tenant_and_resources(
    pool: asyncpg.Pool,
) -> tuple[UUID, UUID, UUID]:
    tenant_id = uuid7()
    old_resource = uuid7()
    new_resource = uuid7()
    await pool.execute("INSERT INTO tenants (id) VALUES ($1)", tenant_id)
    for resource_id, identity in (
        (old_resource, "Old project"),
        (new_resource, "New project"),
    ):
        await pool.execute(
            """
            INSERT INTO resources (
                id, tenant_id, kind, identity, current_value, metadata
            ) VALUES (
                $1, $2, 'capacity', $3,
                jsonb_build_object('name', $3::text),
                '{"semantic_kind":"project"}'::jsonb
            )
            """,
            resource_id,
            tenant_id,
            identity,
        )
    return tenant_id, old_resource, new_resource


async def _seed_observation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    occurred_at: datetime,
    external_id: str,
) -> UUID:
    observation_id = uuid7()
    await pool.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            content, content_text, trust_tier, external_id,
            entities_mentioned
        ) VALUES (
            $1, $2, $3, 'signal', 'jira:webhook', '{}'::jsonb,
            'ENG changed', 'authoritative', $4, '[]'::jsonb
        )
        """,
        observation_id,
        tenant_id,
        occurred_at,
        external_id,
    )
    return observation_id


async def test_supersede_preserves_as_of_history_and_exact_attachment(
    resolver_db: asyncpg.Pool,
) -> None:
    tenant_id, old_resource, new_resource = (
        await _seed_tenant_and_resources(resolver_db)
    )
    repo = SourceIdentityBindingRepo(resolver_db)
    valid_from = datetime(2026, 1, 1, tzinfo=timezone.utc)
    effective_at = datetime(2026, 3, 1, tzinfo=timezone.utc)
    old_event_at = datetime(2026, 2, 1, tzinfo=timezone.utc)
    new_event_at = datetime(2026, 4, 1, tzinfo=timezone.utc)
    original = await repo.bind(
        tenant_id=tenant_id,
        source_system="jira",
        source_native_identifier="jira:project:eng",
        source_identity_authority_ref="jira-project-contract-v1",
        canonical_ref={"type": "resource", "id": str(old_resource)},
        evidence_refs=("jira-project:eng:v1",),
        valid_from=valid_from,
        transaction_from=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    observation_id = await _seed_observation(
        resolver_db,
        tenant_id=tenant_id,
        occurred_at=old_event_at,
        external_id="old-observation",
    )
    await repo.attach_to_observation(
        tenant_id=tenant_id,
        observation_id=observation_id,
        binding=original,
        source_surface="ENG",
        attachment_authority_ref="jira-envelope-v1",
    )

    transition = await repo.supersede(
        tenant_id=tenant_id,
        binding_lineage_id=original.binding_lineage_id or "",
        expected_binding_version=1,
        effective_at=effective_at,
        operation_ref="jira-project-eng-rebind-v2",
        reason="source project was remapped",
        evidence_refs=("admin-review:rebind-1",),
        new_canonical_ref={
            "type": "resource",
            "id": str(new_resource),
        },
        new_source_identity_authority_ref="jira-project-contract-v2",
        new_evidence_refs=("jira-project:eng:v2",),
    )
    assert transition.applied is True
    assert [item.binding_version for item in transition.result_bindings] == [
        2,
        3,
    ]
    assert all(
        item.binding_lineage_id == original.binding_lineage_id
        for item in transition.result_bindings
    )

    replay = await repo.supersede(
        tenant_id=tenant_id,
        binding_lineage_id=original.binding_lineage_id or "",
        expected_binding_version=1,
        effective_at=effective_at,
        operation_ref="jira-project-eng-rebind-v2",
        reason="source project was remapped",
        evidence_refs=("admin-review:rebind-1",),
        new_canonical_ref={
            "type": "resource",
            "id": str(new_resource),
        },
        new_source_identity_authority_ref="jira-project-contract-v2",
        new_evidence_refs=("jira-project:eng:v2",),
    )
    assert replay.applied is False
    assert replay.result_bindings == transition.result_bindings

    with pytest.raises(ValueError, match="stale binding version"):
        await repo.close(
            tenant_id=tenant_id,
            binding_lineage_id=original.binding_lineage_id or "",
            expected_binding_version=1,
            effective_at=new_event_at,
            operation_ref="stale-close",
            reason="stale caller",
            evidence_refs=("stale-command",),
        )

    known_before = transition.transaction_at - timedelta(microseconds=1)
    known_after = transition.transaction_at + timedelta(microseconds=1)
    old_as_known_before = await repo.find_as_of_binding(
        tenant_id=tenant_id,
        source_system="jira",
        source_native_identifier="jira:project:eng",
        valid_at=old_event_at,
        known_at=known_before,
    )
    old_as_known_after = await repo.find_as_of_binding(
        tenant_id=tenant_id,
        source_system="jira",
        source_native_identifier="jira:project:eng",
        valid_at=old_event_at,
        known_at=known_after,
    )
    new_as_known_after = await repo.find_as_of_binding(
        tenant_id=tenant_id,
        source_system="jira",
        source_native_identifier="jira:project:eng",
        valid_at=new_event_at,
        known_at=known_after,
    )
    current = await repo.find_current_binding(
        tenant_id=tenant_id,
        source_system="jira",
        source_native_identifier="jira:project:eng",
    )
    assert old_as_known_before is not None
    assert old_as_known_before.binding_id == original.binding_id
    assert old_as_known_before.binding_version == 1
    assert old_as_known_before.canonical_referent_id == str(old_resource)
    assert (
        old_as_known_before.temporal_scope.transaction_to
        == transition.transaction_at
    )
    assert old_as_known_after == transition.result_bindings[0]
    assert new_as_known_after == transition.result_bindings[1]
    assert current == transition.result_bindings[1]

    assert await repo.resolve_observation_source(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="ENG",
        valid_at=old_event_at,
        known_at=known_before,
    ) is not None
    assert await repo.resolve_observation_source(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="ENG",
        valid_at=old_event_at,
        known_at=known_after,
    ) is None
    attachment = await resolver_db.fetchrow(
        """
        SELECT binding_id, binding_version
        FROM observation_source_identity_bindings
        WHERE tenant_id=$1 AND observation_id=$2
        """,
        tenant_id,
        observation_id,
    )
    assert str(attachment["binding_id"]) == original.binding_id
    assert attachment["binding_version"] == 1
    with pytest.raises(ValueError, match="different binding version"):
        await repo.attach_to_observation(
            tenant_id=tenant_id,
            observation_id=observation_id,
            binding=transition.result_bindings[0],
            source_surface="ENG",
            attachment_authority_ref="jira-envelope-v1",
        )

    delayed_observation = await _seed_observation(
        resolver_db,
        tenant_id=tenant_id,
        occurred_at=old_event_at,
        external_id="delayed-observation",
    )
    await repo.attach_to_observation(
        tenant_id=tenant_id,
        observation_id=delayed_observation,
        binding=old_as_known_after,
        source_surface="ENG",
        attachment_authority_ref="jira-envelope-v1",
    )
    delayed_known_at = datetime.now(timezone.utc)
    delayed = await repo.resolve_observation_source(
        tenant_id=tenant_id,
        observation_id=delayed_observation,
        phrase="ENG",
        valid_at=old_event_at,
        known_at=delayed_known_at,
    )
    assert delayed is not None
    assert delayed.binding.binding_version == 2


@pytest.mark.parametrize("operation_kind", ["close", "revoke"])
async def test_terminal_operations_are_idempotent_and_tenant_scoped(
    resolver_db: asyncpg.Pool,
    operation_kind: str,
) -> None:
    tenant_id, old_resource, _ = await _seed_tenant_and_resources(
        resolver_db
    )
    other_tenant = uuid7()
    await resolver_db.execute(
        "INSERT INTO tenants (id) VALUES ($1)",
        other_tenant,
    )
    repo = SourceIdentityBindingRepo(resolver_db)
    binding = await repo.bind(
        tenant_id=tenant_id,
        source_system="linear",
        source_native_identifier=f"linear:project:{operation_kind}",
        source_identity_authority_ref="linear-project-contract-v1",
        canonical_ref={"type": "resource", "id": str(old_resource)},
        evidence_refs=(f"linear-project:{operation_kind}",),
        valid_from=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    operation = getattr(repo, operation_kind)
    kwargs = {
        "tenant_id": tenant_id,
        "binding_lineage_id": binding.binding_lineage_id or "",
        "expected_binding_version": 1,
        "effective_at": datetime(2026, 3, 1, tzinfo=timezone.utc),
        "operation_ref": f"{operation_kind}-linear-project",
        "reason": f"{operation_kind} source identity",
        "evidence_refs": (f"review:{operation_kind}",),
    }
    first = await operation(**kwargs)
    replay = await operation(**kwargs)

    assert first.applied is True
    assert replay.applied is False
    assert replay.result_bindings == first.result_bindings
    assert await repo.find_current_binding(
        tenant_id=tenant_id,
        source_system="linear",
        source_native_identifier=f"linear:project:{operation_kind}",
    ) is None
    assert await repo.find_as_of_binding(
        tenant_id=tenant_id,
        source_system="linear",
        source_native_identifier=f"linear:project:{operation_kind}",
        valid_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        known_at=first.transaction_at + timedelta(microseconds=1),
    ) == first.result_bindings[0]
    assert await repo.find_as_of_binding(
        tenant_id=tenant_id,
        source_system="linear",
        source_native_identifier=f"linear:project:{operation_kind}",
        valid_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
        known_at=first.transaction_at + timedelta(microseconds=1),
    ) is None
    assert await repo.find_current_binding(
        tenant_id=other_tenant,
        source_system="linear",
        source_native_identifier=f"linear:project:{operation_kind}",
    ) is None
    with pytest.raises(ValueError, match="no current binding"):
        await operation(
            **{
                **kwargs,
                "tenant_id": other_tenant,
                "operation_ref": f"other-tenant-{operation_kind}",
            }
        )


async def test_future_close_rejects_overlapping_rebind_but_allows_boundary_successor(
    resolver_db: asyncpg.Pool,
) -> None:
    tenant_id, old_resource, new_resource = (
        await _seed_tenant_and_resources(resolver_db)
    )
    repo = SourceIdentityBindingRepo(resolver_db)
    identifier = "jira:project:scheduled-close"
    original = await repo.bind(
        tenant_id=tenant_id,
        source_system="jira",
        source_native_identifier=identifier,
        source_identity_authority_ref="jira-project-contract-v1",
        canonical_ref={"type": "resource", "id": str(old_resource)},
        evidence_refs=("jira-project:scheduled-close:v1",),
        valid_from=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    boundary = datetime(2026, 12, 1, tzinfo=timezone.utc)
    await repo.close(
        tenant_id=tenant_id,
        binding_lineage_id=original.binding_lineage_id or "",
        expected_binding_version=1,
        effective_at=boundary,
        operation_ref="scheduled-close-v1",
        reason="source identity retires at year end",
        evidence_refs=("admin-review:scheduled-close",),
    )

    with pytest.raises(ValueError, match="valid-time interval overlaps"):
        await repo.bind(
            tenant_id=tenant_id,
            source_system="jira",
            source_native_identifier=identifier,
            source_identity_authority_ref="jira-project-contract-v2",
            canonical_ref={"type": "resource", "id": str(new_resource)},
            evidence_refs=("jira-project:scheduled-close:v2",),
            valid_from=datetime(2026, 8, 1, tzinfo=timezone.utc),
        )

    before_boundary = await repo.find_current_binding(
        tenant_id=tenant_id,
        source_system="jira",
        source_native_identifier=identifier,
        at=datetime(2026, 9, 1, tzinfo=timezone.utc),
    )
    assert before_boundary is not None
    assert before_boundary.canonical_referent_id == str(old_resource)

    successor = await repo.bind(
        tenant_id=tenant_id,
        source_system="jira",
        source_native_identifier=identifier,
        source_identity_authority_ref="jira-project-contract-v2",
        canonical_ref={"type": "resource", "id": str(new_resource)},
        evidence_refs=("jira-project:scheduled-close:v2",),
        valid_from=boundary,
    )
    after_boundary = await repo.find_current_binding(
        tenant_id=tenant_id,
        source_system="jira",
        source_native_identifier=identifier,
        at=datetime(2026, 12, 2, tzinfo=timezone.utc),
    )
    assert after_boundary == successor
