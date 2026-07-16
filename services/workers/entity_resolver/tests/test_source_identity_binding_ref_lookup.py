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
) -> tuple[UUID, UUID, UUID, UUID]:
    tenant_id = uuid7()
    other_tenant = uuid7()
    old_resource = uuid7()
    new_resource = uuid7()
    await pool.executemany(
        "INSERT INTO tenants (id) VALUES ($1)",
        ((tenant_id,), (other_tenant,)),
    )
    for resource_id, identity in (
        (old_resource, "Old system"),
        (new_resource, "New system"),
    ):
        await pool.execute(
            """
            INSERT INTO resources (
                id, tenant_id, kind, identity, current_value, metadata
            ) VALUES (
                $1, $2, 'infrastructure', $3,
                jsonb_build_object('name', $3::text),
                '{"semantic_kind":"system"}'::jsonb
            )
            """,
            resource_id,
            tenant_id,
            identity,
        )
    return tenant_id, other_tenant, old_resource, new_resource


async def test_exact_ref_listing_is_current_tenant_scoped_and_deterministic(
    resolver_db: asyncpg.Pool,
) -> None:
    tenant_id, other_tenant, resource_id, _ = (
        await _seed_tenant_and_resources(resolver_db)
    )
    repo = SourceIdentityBindingRepo(resolver_db)
    valid_from = datetime(2026, 1, 1, tzinfo=timezone.utc)
    expected = []
    for source_system, source_identifier in (
        ("jira", "jira:system:z"),
        ("github", "github:system:a"),
        ("linear", "linear:system:m"),
    ):
        expected.append(
            await repo.bind(
                tenant_id=tenant_id,
                source_system=source_system,
                source_native_identifier=source_identifier,
                source_identity_authority_ref=f"{source_system}-system-v1",
                canonical_ref={
                    "type": "resource",
                    "id": str(resource_id),
                    "version": 1,
                },
                evidence_refs=(source_identifier,),
                valid_from=valid_from,
            )
        )
    await repo.bind(
        tenant_id=tenant_id,
        source_system="slack",
        source_native_identifier="slack:system:version-two",
        source_identity_authority_ref="slack-system-v2",
        canonical_ref={
            "type": "resource",
            "id": str(resource_id),
            "version": 2,
        },
        evidence_refs=("slack:system:version-two",),
        valid_from=valid_from,
    )
    await repo.bind(
        tenant_id=other_tenant,
        source_system="jira",
        source_native_identifier="jira:system:other-tenant",
        source_identity_authority_ref="jira-system-v1",
        canonical_ref={
            "type": "resource",
            "id": str(resource_id),
            "version": 1,
        },
        evidence_refs=("jira:system:other-tenant",),
        valid_from=valid_from,
    )
    before_count = await resolver_db.fetchval(
        "SELECT count(*) FROM source_identity_bindings"
    )

    matches = await repo.list_bindings_for_canonical_ref(
        tenant_id=tenant_id,
        canonical_referent_type="resource",
        canonical_referent_id=str(resource_id),
        canonical_referent_version=1,
    )
    version_two = await repo.list_bindings_for_canonical_ref(
        tenant_id=tenant_id,
        canonical_referent_type="resource",
        canonical_referent_id=str(resource_id),
        canonical_referent_version=2,
    )
    other_tenant_matches = await repo.list_bindings_for_canonical_ref(
        tenant_id=other_tenant,
        canonical_referent_type="resource",
        canonical_referent_id=str(resource_id),
        canonical_referent_version=1,
    )

    assert [item.source_system for item in matches] == [
        "github",
        "jira",
        "linear",
    ]
    assert {item.binding_id for item in matches} == {
        item.binding_id for item in expected
    }
    assert len(version_two) == 1
    assert version_two[0].canonical_referent_version == 2
    assert len(other_tenant_matches) == 1
    assert other_tenant_matches[0].tenant_id == other_tenant
    assert await resolver_db.fetchval(
        "SELECT count(*) FROM source_identity_bindings"
    ) == before_count

    await resolver_db.execute(
        "UPDATE resources SET archived_at=now() WHERE id=$1",
        resource_id,
    )
    assert await repo.list_bindings_for_canonical_ref(
        tenant_id=tenant_id,
        canonical_referent_type="resource",
        canonical_referent_id=str(resource_id),
        canonical_referent_version=1,
    ) == matches


async def test_exact_ref_listing_preserves_bitemporal_binding_history(
    resolver_db: asyncpg.Pool,
) -> None:
    tenant_id, _, old_resource, new_resource = (
        await _seed_tenant_and_resources(resolver_db)
    )
    repo = SourceIdentityBindingRepo(resolver_db)
    original = await repo.bind(
        tenant_id=tenant_id,
        source_system="jira",
        source_native_identifier="jira:system:mercury",
        source_identity_authority_ref="jira-system-v1",
        canonical_ref={
            "type": "resource",
            "id": str(old_resource),
            "version": 1,
        },
        evidence_refs=("jira:system:mercury:v1",),
        valid_from=datetime(2026, 1, 1, tzinfo=timezone.utc),
        transaction_from=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    transition = await repo.supersede(
        tenant_id=tenant_id,
        binding_lineage_id=original.binding_lineage_id or "",
        expected_binding_version=1,
        effective_at=datetime(2026, 3, 1, tzinfo=timezone.utc),
        operation_ref="replace:jira-system-mercury",
        reason="Canonical system referent was replaced.",
        evidence_refs=("review:mercury",),
        new_canonical_ref={
            "type": "resource",
            "id": str(new_resource),
            "version": 1,
        },
        new_source_identity_authority_ref="jira-system-v2",
        new_evidence_refs=("jira:system:mercury:v2",),
    )
    known_before = transition.transaction_at - timedelta(microseconds=1)
    known_after = transition.transaction_at + timedelta(microseconds=1)

    old_before_transition_known = await repo.list_bindings_for_canonical_ref(
        tenant_id=tenant_id,
        canonical_referent_type="resource",
        canonical_referent_id=str(old_resource),
        canonical_referent_version=1,
        valid_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        known_at=known_before,
    )
    old_after_transition_known = await repo.list_bindings_for_canonical_ref(
        tenant_id=tenant_id,
        canonical_referent_type="resource",
        canonical_referent_id=str(old_resource),
        canonical_referent_version=1,
        valid_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
        known_at=known_after,
    )
    old_after_effective = await repo.list_bindings_for_canonical_ref(
        tenant_id=tenant_id,
        canonical_referent_type="resource",
        canonical_referent_id=str(old_resource),
        canonical_referent_version=1,
        valid_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
        known_at=known_after,
    )
    new_after_effective = await repo.list_bindings_for_canonical_ref(
        tenant_id=tenant_id,
        canonical_referent_type="resource",
        canonical_referent_id=str(new_resource),
        canonical_referent_version=1,
        valid_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
        known_at=known_after,
    )

    assert len(old_before_transition_known) == 1
    assert old_before_transition_known[0].binding_id == original.binding_id
    assert old_before_transition_known[0].binding_version == 1
    assert (
        old_before_transition_known[0].temporal_scope.transaction_to
        == transition.transaction_at
    )
    assert old_after_transition_known == (transition.result_bindings[0],)
    assert old_after_effective == ()
    assert new_after_effective == (transition.result_bindings[1],)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"canonical_referent_type": "", "canonical_referent_id": "x"},
        {"canonical_referent_type": "resource", "canonical_referent_id": ""},
        {
            "canonical_referent_type": "resource",
            "canonical_referent_id": "x",
            "canonical_referent_version": 0,
        },
        {
            "canonical_referent_type": "resource",
            "canonical_referent_id": "x",
            "valid_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        },
        {
            "canonical_referent_type": "resource",
            "canonical_referent_id": "x",
            "valid_at": datetime(2026, 1, 1),
            "known_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
        },
    ],
)
async def test_exact_ref_listing_rejects_incomplete_cutoffs_and_refs(
    resolver_db: asyncpg.Pool,
    kwargs,
) -> None:
    repo = SourceIdentityBindingRepo(resolver_db)
    base = {
        "tenant_id": uuid7(),
        "canonical_referent_type": "resource",
        "canonical_referent_id": "resource:one",
        "canonical_referent_version": 1,
    }
    base.update(kwargs)
    with pytest.raises(ValueError):
        await repo.list_bindings_for_canonical_ref(**base)
