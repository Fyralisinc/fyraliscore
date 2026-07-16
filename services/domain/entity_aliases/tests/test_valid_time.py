"""Valid-time behavior for historical customer aliases."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.entity_aliases.repo import (
    EntityAliasRepo,
    close_aliases_for_entity_with_connection,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _customer(
    pool: asyncpg.Pool,
    *,
    tenant_id,
    identity: str,
    archived_at=None,
):
    customer_id = uuid7()
    await pool.execute(
        """
        INSERT INTO resources (
            id, tenant_id, kind, identity, current_value, metadata, archived_at
        ) VALUES (
            $1, $2, 'relational', $3, '{}'::jsonb,
            '{"semantic_kind":"customer"}'::jsonb, $4
        )
        """,
        customer_id,
        tenant_id,
        identity,
        archived_at,
    )
    return customer_id


async def test_historical_name_reuse_resolves_by_event_time(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    await fresh_db.execute(
        "INSERT INTO tenants (id) VALUES ($1)",
        tenant_id,
    )
    old_customer_id = await _customer(
        fresh_db,
        tenant_id=tenant_id,
        identity="Acme",
    )
    new_customer_id = await _customer(
        fresh_db,
        tenant_id=tenant_id,
        identity="Acme",
    )
    old_ref = {"type": "customer", "id": str(old_customer_id)}
    new_ref = {"type": "customer", "id": str(new_customer_id)}
    repo = EntityAliasRepo(fresh_db)
    now = datetime.now(timezone.utc)
    old_start = now - timedelta(days=30)
    reuse_at = now - timedelta(days=10)

    await repo.insert_alias(
        phrase="Acme",
        resolved_entity_ref=old_ref,
        source="resource_lifecycle",
        confidence=1.0,
        tenant_id=tenant_id,
        valid_from=old_start,
        is_canonical=True,
    )
    async with fresh_db.acquire() as conn, conn.transaction():
        closed = await close_aliases_for_entity_with_connection(
            conn,
            tenant_id=tenant_id,
            resolved_entity_ref=old_ref,
            valid_until=reuse_at,
            validity_event_id=None,
            validity_reason="customer_renamed",
            phrases=["Acme"],
        )
    assert closed == 1

    await repo.insert_alias(
        phrase="Acme",
        resolved_entity_ref=new_ref,
        source="resource_lifecycle",
        confidence=1.0,
        tenant_id=tenant_id,
        valid_from=reuse_at,
        is_canonical=True,
    )

    assert await repo.fast_path_resolve(
        "ACME",
        tenant_id,
        as_of=old_start + timedelta(days=1),
    ) == old_ref
    assert await repo.fast_path_resolve(
        "Acme",
        tenant_id,
        as_of=reuse_at,
    ) == new_ref
    assert await repo.fast_path_resolve("Acme", tenant_id) == new_ref
    history = await repo.list_history(" acme ", tenant_id)
    assert [item["resolved_entity_ref"] for item in history] == [
        old_ref,
        new_ref,
    ]
    assert history[0]["valid_until"] == reuse_at
    assert history[1]["valid_from"] == reuse_at
    assert history[1]["valid_until"] is None


async def test_archived_customer_resolves_only_before_archive_time(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    await fresh_db.execute(
        "INSERT INTO tenants (id) VALUES ($1)",
        tenant_id,
    )
    now = datetime.now(timezone.utc)
    alias_start = now - timedelta(days=30)
    archived_at = now - timedelta(days=5)
    customer_id = await _customer(
        fresh_db,
        tenant_id=tenant_id,
        identity="Nimbus",
        archived_at=archived_at,
    )
    customer_ref = {"type": "customer", "id": str(customer_id)}
    alias_id = uuid7()
    await fresh_db.execute(
        """
        INSERT INTO entity_aliases (
            id, tenant_id, alias_text, resolved_entity_ref,
            entity_metadata, confidence, first_seen_at, last_used_at,
            valid_from, valid_until, validity_reason
        ) VALUES (
            $1, $2, 'Nimbus', $3::jsonb,
            '{"source":"resource_lifecycle"}'::jsonb, 1.0, $4, $4,
            $4, $5, 'customer_archived'
        )
        """,
        alias_id,
        tenant_id,
        json.dumps(customer_ref),
        alias_start,
        archived_at,
    )
    repo = EntityAliasRepo(fresh_db)

    assert await repo.fast_path_resolve(
        "Nimbus",
        tenant_id,
        as_of=archived_at - timedelta(seconds=1),
    ) == customer_ref
    assert await repo.fast_path_resolve(
        "Nimbus",
        tenant_id,
        as_of=archived_at,
    ) is None
    assert await repo.fast_path_resolve("Nimbus", tenant_id) is None
