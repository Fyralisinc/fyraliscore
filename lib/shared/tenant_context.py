"""
lib/shared/tenant_context.py — bind an asyncpg connection to a tenant.

Today the codebase carries `tenant_id` as an explicit argument on every
repo call. Every query then has `WHERE tenant_id = $1` written by hand.
This works but pushes the tenancy invariant out of the schema and into
the discipline of every author of every query.

A TenantContext makes the tenancy invariant *structural*:

  1. Every connection acquired via `tenant_transaction(tenant_id)` runs
     inside a transaction with `app.current_tenant` set to the tenant's
     UUID, accessible from any SQL via `current_setting()`.

  2. The Phase 4 RLS migration installs a policy on every tenant-scoped
     table:
         USING (tenant_id = current_setting('app.current_tenant')::uuid)
     This means even a buggy query that *forgets* the tenant_id filter
     can never return another tenant's rows. Defense in depth.

  3. Repos that accept `TenantContext` instead of `asyncpg.Connection +
     UUID` will eventually drop their explicit tenant_id parameter
     from public methods. (Out of scope for this stage — call sites
     change incrementally.)

Why a transaction is mandatory
------------------------------
SET LOCAL only takes effect inside an explicit transaction. We use
`set_config(.., is_local=true)` which is the function form of SET
LOCAL — its scope is bounded by the surrounding transaction so it
can't leak into the next pool acquire. SET (without LOCAL) would
risk a tenant context bleeding into another caller via connection
reuse, which is exactly the disaster RLS is supposed to prevent.

Backward compatibility
----------------------
TenantContext quacks like an asyncpg.Connection for the methods repos
actually use (execute / fetch / fetchrow / fetchval / transaction /
prepare). A repo written today that takes `conn` works unchanged when
handed a TenantContext. New repos should type their parameter as
`TenantContext` to make the requirement explicit at the type level.
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.db import get_pool
from lib.shared.errors import CompanyOSError


class TenantContextError(CompanyOSError):
    default_code = "tenant_context_error"


class TenantContext:
    """An asyncpg connection bound to a tenant. Use as `conn` in repos.

    Created only via `tenant_transaction()` — the transaction is what
    makes `SET LOCAL app.current_tenant` durable for the life of the
    context. Manually constructing a TenantContext does NOT set the
    Postgres-side current_tenant; the helper is the chokepoint.
    """

    __slots__ = ("conn", "tenant_id")

    def __init__(self, conn: asyncpg.Connection, tenant_id: UUID) -> None:
        self.conn = conn
        self.tenant_id = tenant_id

    # -----------------------------------------------------------------
    # asyncpg.Connection delegation — the surface repos actually use.
    # -----------------------------------------------------------------
    async def execute(self, query: str, *args: Any, **kwargs: Any) -> str:
        return await self.conn.execute(query, *args, **kwargs)

    async def executemany(self, query: str, args: Any, **kwargs: Any) -> None:
        return await self.conn.executemany(query, args, **kwargs)

    async def fetch(self, query: str, *args: Any, **kwargs: Any) -> list[asyncpg.Record]:
        return await self.conn.fetch(query, *args, **kwargs)

    async def fetchrow(
        self, query: str, *args: Any, **kwargs: Any
    ) -> asyncpg.Record | None:
        return await self.conn.fetchrow(query, *args, **kwargs)

    async def fetchval(self, query: str, *args: Any, **kwargs: Any) -> Any:
        return await self.conn.fetchval(query, *args, **kwargs)

    async def prepare(self, query: str, **kwargs: Any) -> asyncpg.PreparedStatement:
        return await self.conn.prepare(query, **kwargs)

    def transaction(self, **kwargs: Any) -> Any:
        """Returns an asyncpg.Transaction. The tenant setting is
        already set on the parent transaction; nested transactions
        inherit it via the same connection's session state."""
        return self.conn.transaction(**kwargs)

    def cursor(self, query: str, *args: Any, **kwargs: Any) -> Any:
        return self.conn.cursor(query, *args, **kwargs)


async def _set_tenant(conn: asyncpg.Connection, tenant_id: UUID) -> None:
    """Set app.current_tenant for the life of the surrounding transaction.
    Uses set_config(.., is_local=true) — same semantics as SET LOCAL."""
    await conn.execute(
        "SELECT set_config('app.current_tenant', $1::text, true)",
        str(tenant_id),
    )


@asynccontextmanager
async def tenant_transaction(
    tenant_id: UUID,
    *,
    pool: asyncpg.Pool | None = None,
    isolation: str | None = None,
) -> AsyncIterator[TenantContext]:
    """Yield a TenantContext inside a transaction with the tenant bound.

    The transaction is committed if the body exits cleanly, rolled back
    on exception. The Postgres-side `app.current_tenant` setting
    vanishes when the transaction ends — no leak into the pool.

    Usage:

        async with tenant_transaction(tenant_id) as tctx:
            await tctx.execute("INSERT INTO models (...) VALUES ($1, ...)", ...)
            # RLS policies see app.current_tenant == tenant_id
    """
    if not isinstance(tenant_id, UUID):
        raise TenantContextError(
            f"tenant_id must be UUID, got {type(tenant_id).__name__}",
        )
    actual_pool = pool or get_pool()
    async with actual_pool.acquire() as conn:
        async with conn.transaction(isolation=isolation):
            await _set_tenant(conn, tenant_id)
            yield TenantContext(conn, tenant_id)


@asynccontextmanager
async def bind_tenant(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> AsyncIterator[TenantContext]:
    """Bind a tenant to an *existing* connection that already has a
    transaction open. Useful in tests and in code paths that already
    hold a connection (e.g. a worker that opens its own transaction).

    The caller owns the surrounding transaction and the
    set_config(is_local=true) is scoped to it.
    """
    if not isinstance(tenant_id, UUID):
        raise TenantContextError(
            f"tenant_id must be UUID, got {type(tenant_id).__name__}",
        )
    await _set_tenant(conn, tenant_id)
    yield TenantContext(conn, tenant_id)


async def current_tenant(conn: asyncpg.Connection) -> UUID | None:
    """Read app.current_tenant from the connection's current transaction.

    Returns None if unset (which RLS policies treat as 'no rows visible').
    Useful for assertions in repos that want to verify a TenantContext
    was actually used."""
    raw = await conn.fetchval(
        "SELECT current_setting('app.current_tenant', true)"
    )
    if not raw:
        return None
    try:
        return UUID(raw)
    except (ValueError, TypeError):
        return None


__all__ = [
    "TenantContext",
    "TenantContextError",
    "tenant_transaction",
    "bind_tenant",
    "current_tenant",
]
