from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from services.domain.canonical_referents.service import (
    CanonicalReferentRegistryService,
)
from services.domain.canonical_referents.types import (
    CanonicalReferentReplacementCommand,
    CanonicalReferentVersionRef,
)
from services.domain.resources import repo as resources_repo


pytestmark = pytest.mark.integration

TENANT_A = UUID("11111111-1111-1111-1111-111111111111")
TENANT_B = UUID("22222222-2222-2222-2222-222222222222")
CREATED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)
FIRST_EFFECTIVE = datetime(2026, 1, 10, tzinfo=timezone.utc)
SECOND_EFFECTIVE = datetime(2026, 1, 20, tzinfo=timezone.utc)


def _ref(resource_id: UUID) -> CanonicalReferentVersionRef:
    return CanonicalReferentVersionRef(
        type="resource",
        id=str(resource_id),
        version=1,
    )


def _command(
    *,
    operation_ref: str,
    predecessor: CanonicalReferentVersionRef,
    successor: CanonicalReferentVersionRef,
    effective_at: datetime,
) -> CanonicalReferentReplacementCommand:
    return CanonicalReferentReplacementCommand(
        tenant_id=TENANT_A,
        operation_ref=operation_ref,
        predecessor=predecessor,
        successor=successor,
        expected_predecessor_version=1,
        effective_at=effective_at,
        authority_ref="authority:lineage-read-test",
        reason="Test canonical resource replacement.",
        evidence_refs=("test:evidence",),
    )


async def _seed_resource(
    pool: asyncpg.Pool,
    *,
    resource_id: UUID,
    identity: str,
    archived_at: datetime | None,
    created_at: datetime = CREATED_AT,
) -> None:
    await pool.execute(
        """
        INSERT INTO resources (
            id, tenant_id, kind, identity, current_value, metadata,
            created_at, last_updated_at, archived_at
        ) VALUES (
            $1, $2, 'infrastructure', $3,
            jsonb_build_object('name', $3::text),
            '{"semantic_kind":"system"}'::jsonb,
            $4, $4, $5
        )
        """,
        resource_id,
        TENANT_A,
        identity,
        created_at,
        archived_at,
    )


@pytest.mark.asyncio
async def test_lineage_aware_resource_read_preserves_history_and_resolves_head(
    fresh_db: asyncpg.Pool,
) -> None:
    root_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1")
    middle_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2")
    head_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3")
    await _seed_resource(
        fresh_db,
        resource_id=root_id,
        identity="Mercury legacy",
        archived_at=FIRST_EFFECTIVE,
    )
    await _seed_resource(
        fresh_db,
        resource_id=middle_id,
        identity="Mercury intermediate",
        archived_at=SECOND_EFFECTIVE,
    )
    await _seed_resource(
        fresh_db,
        resource_id=head_id,
        identity="Mercury current",
        archived_at=None,
        created_at=SECOND_EFFECTIVE + timedelta(days=5),
    )
    registry = CanonicalReferentRegistryService(fresh_db)
    first = await registry.apply_replacement(
        _command(
            operation_ref="replace:mercury:first",
            predecessor=_ref(root_id),
            successor=_ref(middle_id),
            effective_at=FIRST_EFFECTIVE,
        )
    )
    await asyncio.sleep(0.01)
    second = await registry.apply_replacement(
        _command(
            operation_ref="replace:mercury:second",
            predecessor=_ref(middle_id),
            successor=_ref(head_id),
            effective_at=SECOND_EFFECTIVE,
        )
    )

    async with fresh_db.acquire() as conn:
        before_first = await resources_repo.get_by_canonical_ref(
            tenant_id=TENANT_A,
            canonical_ref=_ref(root_id),
            valid_at=FIRST_EFFECTIVE - timedelta(seconds=1),
            known_at=second.transaction_at,
            registry=registry,
            conn=conn,
        )
        between = await resources_repo.get_by_canonical_ref(
            tenant_id=TENANT_A,
            canonical_ref=_ref(root_id),
            valid_at=SECOND_EFFECTIVE - timedelta(seconds=1),
            known_at=second.transaction_at,
            registry=registry,
            conn=conn,
        )
        current = await resources_repo.get_by_canonical_ref(
            tenant_id=TENANT_A,
            canonical_ref=_ref(root_id),
            valid_at=SECOND_EFFECTIVE + timedelta(seconds=1),
            known_at=second.transaction_at,
            registry=registry,
            conn=conn,
        )
        second_not_yet_known = await resources_repo.get_by_canonical_ref(
            tenant_id=TENANT_A,
            canonical_ref=_ref(root_id),
            valid_at=SECOND_EFFECTIVE + timedelta(seconds=1),
            known_at=first.transaction_at,
            registry=registry,
            conn=conn,
        )
        now = datetime.now(timezone.utc)
        current_now = await resources_repo.get_by_canonical_ref(
            tenant_id=TENANT_A,
            canonical_ref=_ref(root_id),
            valid_at=now,
            known_at=now,
            registry=registry,
            conn=conn,
        )

    assert before_first.effective_ref == _ref(root_id)
    assert before_first.resource is not None
    assert before_first.resource.identity == "Mercury legacy"
    assert between.effective_ref == _ref(middle_id)
    assert between.resource is not None
    assert between.resource.identity == "Mercury intermediate"
    assert current.effective_ref == _ref(head_id)
    assert current.resource is not None
    assert current.resource.identity == "Mercury current"
    assert current.resource.created_at > current.resolution.lineage.valid_at
    assert current.resolution.lineage.members == (
        _ref(root_id),
        _ref(middle_id),
        _ref(head_id),
    )
    assert second_not_yet_known.effective_ref == _ref(middle_id)
    assert second_not_yet_known.resource is not None
    assert second_not_yet_known.resource.archived_at == SECOND_EFFECTIVE
    assert current_now.effective_ref == _ref(head_id)
    assert current_now.resource is not None
    assert current_now.resource.identity == "Mercury current"


@pytest.mark.asyncio
async def test_lineage_aware_resource_read_is_tenant_scoped(
    fresh_db: asyncpg.Pool,
) -> None:
    resource_id = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1")
    await _seed_resource(
        fresh_db,
        resource_id=resource_id,
        identity="Tenant A only",
        archived_at=None,
    )
    registry = CanonicalReferentRegistryService(fresh_db)
    cutoff = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        tenant_a = await resources_repo.get_by_canonical_ref(
            tenant_id=TENANT_A,
            canonical_ref=_ref(resource_id),
            valid_at=cutoff,
            known_at=cutoff,
            registry=registry,
            conn=conn,
        )
        tenant_b = await resources_repo.get_by_canonical_ref(
            tenant_id=TENANT_B,
            canonical_ref=_ref(resource_id),
            valid_at=cutoff,
            known_at=cutoff,
            registry=registry,
            conn=conn,
        )

    assert tenant_a.resource is not None
    assert tenant_a.resource.identity == "Tenant A only"
    assert tenant_b.resource is None
    assert tenant_b.resolution.lineage.members == (_ref(resource_id),)
