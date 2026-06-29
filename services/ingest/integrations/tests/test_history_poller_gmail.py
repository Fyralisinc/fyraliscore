"""Tests for the Gmail history.list fallback poller."""
from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction
from services.ingest.integrations.gmail import history_poller


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def _seeded_tenants(fresh_db: asyncpg.Pool):
    tenants: list[UUID] = []
    try:
        yield tenants
    finally:
        for tenant in tenants:
            async with tenant_transaction(tenant, pool=fresh_db) as tctx:
                await tctx.execute(
                    "DELETE FROM gmail_mailbox_watches WHERE tenant_id = $1",
                    tenant,
                )
                await tctx.execute(
                    "DELETE FROM gmail_installations WHERE tenant_id = $1",
                    tenant,
                )
            await fresh_db.execute("DELETE FROM tenants WHERE id = $1", tenant)


async def _seed_mailbox(
    pool: asyncpg.Pool,
    seeded_tenants: list[UUID],
    *,
    email: str,
    history_id: str = "1000",
) -> UUID:
    tenant = uuid4()
    seeded_tenants.append(tenant)
    await pool.execute("INSERT INTO tenants (id, name) VALUES ($1, 'gmail-history')", tenant)
    install = uuid7()
    watch = uuid7()
    async with tenant_transaction(tenant, pool=pool) as tctx:
        await tctx.execute(
            """
            INSERT INTO gmail_installations (
              id, tenant_id, workspace_domain, service_account_email,
              scope, resolved_user_count, resolved_at
            )
            VALUES (
              $1, $2, 'acme.com', 'svc@acme.iam',
              'gmail.metadata', 1, now()
            )
            """,
            install,
            tenant,
        )
        await tctx.execute(
            """
            INSERT INTO gmail_mailbox_watches (
              id, tenant_id, gmail_installation_id, email_address,
              state, history_id, watch_expiration
            )
            VALUES (
              $1, $2, $3, $4, 'active', $5, now() + interval '6 days'
            )
            """,
            watch,
            tenant,
            install,
            email,
            history_id,
        )
    return watch


async def test_concurrent_replicas_lease_distinct_mailboxes(
    fresh_db: asyncpg.Pool,
    _seeded_tenants: list[UUID],
) -> None:
    watch_a = await _seed_mailbox(
        fresh_db,
        _seeded_tenants,
        email="alice@acme.com",
    )
    watch_b = await _seed_mailbox(
        fresh_db,
        _seeded_tenants,
        email="bob@acme.com",
    )
    first_ready = asyncio.Event()
    release = asyncio.Event()

    async def _lease_one() -> UUID:
        async with fresh_db.acquire() as conn:
            async with conn.transaction():
                rows = await history_poller._lease_due_mailboxes(conn, limit=1)
                assert len(rows) == 1
                first_ready.set()
                await release.wait()
                return rows[0]["id"]

    first = asyncio.create_task(_lease_one())
    try:
        await asyncio.wait_for(first_ready.wait(), timeout=5.0)
        async with fresh_db.acquire() as conn:
            async with conn.transaction():
                rows = await history_poller._lease_due_mailboxes(conn, limit=1)
                assert len(rows) == 1
                second_id = rows[0]["id"]
        release.set()
        first_id = await asyncio.wait_for(first, timeout=5.0)
    finally:
        release.set()

    assert {first_id, second_id} == {watch_a, watch_b}
