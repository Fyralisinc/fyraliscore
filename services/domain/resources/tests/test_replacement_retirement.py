from __future__ import annotations

from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from lib.shared.errors import InvariantViolation, ValidationError
from services.domain.entity_aliases.repo import insert_alias_with_connection
from services.domain.resources import repo
from services.domain.resources.tests.conftest import (
    TENANT_A,
    TENANT_B,
    make_observation,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _create_system(
    *,
    event_id,
    metadata: dict | None = None,
):
    return await repo.create(
        kind="infrastructure",
        identity="system:mercury",
        current_value={"name": "Mercury"},
        metadata=metadata or {"semantic_kind": "system"},
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
    )


async def _database_now(pool: asyncpg.Pool) -> datetime:
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT clock_timestamp()")


async def _event(pool: asyncpg.Pool):
    await pool.executemany(
        """
        INSERT INTO tenants (id, name, is_demo)
        VALUES ($1, $2, FALSE)
        ON CONFLICT (id) DO NOTHING
        """,
        (
            (TENANT_A, "replacement-retirement-a"),
            (TENANT_B, "replacement-retirement-b"),
        ),
    )
    return await make_observation(pool, tenant_id=TENANT_A)


async def test_retirement_uses_explicit_boundary_emits_once_and_replays(
    resources_db: asyncpg.Pool,
) -> None:
    event_id = await _event(resources_db)
    resource = await _create_system(event_id=event_id)
    boundary = await _database_now(resources_db)

    retired = await repo.retire_non_customer_at(
        resource.id,
        tenant_id=TENANT_A,
        canonical_referent_type="resource",
        effective_at=boundary,
        reason="Canonical system identity was replaced.",
        cause_event_id=event_id,
    )
    replayed = await repo.retire_non_customer_at(
        resource.id,
        tenant_id=TENANT_A,
        canonical_referent_type="resource",
        effective_at=boundary,
        reason="Canonical system identity was replaced.",
        cause_event_id=event_id,
    )

    assert retired.archived_at == boundary
    assert retired.last_updated_by_event_id == event_id
    assert replayed == retired
    row = await resources_db.fetchrow(
        """
        SELECT occurred_at, content
        FROM observations
        WHERE tenant_id=$1
          AND kind='state_change'
          AND content ->> 'state_change_kind'='resource_archived'
          AND content ->> 'entity_id'=$2
        """,
        TENANT_A,
        str(resource.id),
    )
    assert row is not None
    assert row["occurred_at"] == boundary
    assert row["content"]["metadata"] == {
        "reason": "Canonical system identity was replaced.",
        "resource_kind": "infrastructure",
        "semantic_kind": "system",
        "effective_at": boundary.isoformat(),
        "retirement_mode": "canonical_replacement",
    }
    assert await resources_db.fetchval(
        """
        SELECT count(*)
        FROM observations
        WHERE tenant_id=$1
          AND kind='state_change'
          AND content ->> 'state_change_kind'='resource_archived'
          AND content ->> 'entity_id'=$2
        """,
        TENANT_A,
        str(resource.id),
    ) == 1


async def test_retirement_does_not_manage_alias_lifecycle(
    resources_db: asyncpg.Pool,
) -> None:
    event_id = await _event(resources_db)
    resource = await _create_system(event_id=event_id)
    async with resources_db.acquire() as conn, conn.transaction():
        await insert_alias_with_connection(
            conn,
            phrase="Mercury",
            resolved_entity_ref={
                "type": "resource",
                "id": str(resource.id),
                "version": 1,
            },
            source="resource_lifecycle",
            confidence=1.0,
            tenant_id=TENANT_A,
            source_event_id=event_id,
            valid_from=resource.created_at,
        )
        boundary = await conn.fetchval("SELECT clock_timestamp()")
        await repo.retire_non_customer_at(
            resource.id,
            tenant_id=TENANT_A,
            canonical_referent_type="resource",
            effective_at=boundary,
            reason="Replacement retirement; alias transfer is separate.",
            cause_event_id=event_id,
            conn=conn,
        )
        alias = await conn.fetchrow(
            """
            SELECT valid_until, validity_event_id, validity_reason
            FROM entity_aliases
            WHERE tenant_id=$1 AND alias_text='Mercury'
            """,
            TENANT_A,
        )

    assert alias is not None
    assert alias["valid_until"] is None
    assert alias["validity_event_id"] is None
    assert alias["validity_reason"] is None


async def test_retirement_rejects_customer_and_actor_protocols(
    resources_db: asyncpg.Pool,
) -> None:
    event_id = await _event(resources_db)
    system = await _create_system(event_id=event_id)
    customer = await repo.create(
        kind="relational",
        identity="Acme",
        current_value={"arr_usd": 100_000},
        metadata={"semantic_kind": "customer"},
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
    )
    actor_like = await _create_system(
        event_id=event_id,
        metadata={"semantic_kind": "human_internal"},
    )
    boundary = await _database_now(resources_db)

    with pytest.raises(InvariantViolation) as typed_actor:
        await repo.retire_non_customer_at(
            system.id,
            tenant_id=TENANT_A,
            canonical_referent_type="actor",
            effective_at=boundary,
            reason="wrong protocol",
        )
    assert typed_actor.value.invariant == "RESOURCE_RETIREMENT_REFERENT_TYPE"

    with pytest.raises(InvariantViolation) as customer_error:
        await repo.retire_non_customer_at(
            customer.id,
            tenant_id=TENANT_A,
            canonical_referent_type="resource",
            effective_at=boundary,
            reason="wrong protocol",
        )
    assert customer_error.value.invariant == "RESOURCE_RETIREMENT_CUSTOMER"

    with pytest.raises(InvariantViolation) as actor_error:
        await repo.retire_non_customer_at(
            actor_like.id,
            tenant_id=TENANT_A,
            canonical_referent_type="resource",
            effective_at=boundary,
            reason="wrong protocol",
        )
    assert actor_error.value.invariant == "RESOURCE_RETIREMENT_ACTOR"


async def test_retirement_rejects_invalid_future_and_conflicting_boundaries(
    resources_db: asyncpg.Pool,
) -> None:
    event_id = await _event(resources_db)
    resource = await _create_system(event_id=event_id)

    with pytest.raises(ValidationError, match="timezone-aware"):
        await repo.retire_non_customer_at(
            resource.id,
            tenant_id=TENANT_A,
            canonical_referent_type="resource",
            effective_at=datetime(2026, 1, 1),
            reason="invalid",
        )
    with pytest.raises(ValidationError, match="later than resource creation"):
        await repo.retire_non_customer_at(
            resource.id,
            tenant_id=TENANT_A,
            canonical_referent_type="resource",
            effective_at=resource.created_at,
            reason="invalid",
        )
    with pytest.raises(ValidationError, match="future-effective"):
        await repo.retire_non_customer_at(
            resource.id,
            tenant_id=TENANT_A,
            canonical_referent_type="resource",
            effective_at=datetime.now(timezone.utc) + timedelta(days=1),
            reason="invalid",
        )

    updated = await repo.update_attributes(
        resource.id,
        patch={"name": "Mercury v2"},
        last_updated_by_event_id=event_id,
    )
    invalid_historical_boundary = resource.created_at + (
        updated.last_updated_at - resource.created_at
    ) / 2
    with pytest.raises(ValidationError, match="latest resource update"):
        await repo.retire_non_customer_at(
            resource.id,
            tenant_id=TENANT_A,
            canonical_referent_type="resource",
            effective_at=invalid_historical_boundary,
            reason="temporally inconsistent",
        )

    boundary = await _database_now(resources_db)
    await repo.retire_non_customer_at(
        resource.id,
        tenant_id=TENANT_A,
        canonical_referent_type="resource",
        effective_at=boundary,
        reason="valid",
    )
    with pytest.raises(InvariantViolation) as conflict:
        await repo.retire_non_customer_at(
            resource.id,
            tenant_id=TENANT_A,
            canonical_referent_type="resource",
            effective_at=boundary - timedelta(microseconds=1),
            reason="conflicting replay",
        )
    assert conflict.value.invariant == "RESOURCE_RETIREMENT_BOUNDARY_CONFLICT"

    with pytest.raises(ValidationError, match="resource not found"):
        await repo.retire_non_customer_at(
            resource.id,
            tenant_id=TENANT_B,
            canonical_referent_type="resource",
            effective_at=boundary,
            reason="cross-tenant",
        )
