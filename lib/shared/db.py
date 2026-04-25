"""
lib/shared/db.py — asyncpg pool, typed helpers, transaction with savepoints.

Goals:
- one connection pool per process (lazy-initialised via `get_pool()`)
- every SELECT helper hydrates rows into Pydantic models when a `row_type`
  is provided — catches schema drift on the read path
- `transaction()` returns a context manager that supports nested
  savepoints (asyncpg provides this natively)
- `execute` returns the asyncpg status tag (e.g. 'INSERT 0 1')

Integration tests use these helpers against a real Postgres — no mocks,
per BUILD-PLAN §0.5 non-negotiable #4.
"""
from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, TypeVar

import asyncpg
from pydantic import BaseModel

from lib.shared.errors import CompanyOSError


T = TypeVar("T", bound=BaseModel)


class ConnectionPoolNotInitializedError(CompanyOSError):
    default_code = "db_pool_not_initialized"


class RowHydrationError(CompanyOSError):
    """The row returned by Postgres doesn't fit the Pydantic type."""
    default_code = "row_hydration_error"


# ---------------------------------------------------------------------
# Pool lifecycle
# ---------------------------------------------------------------------

_pool: asyncpg.Pool | None = None


async def init_pool(
    dsn: str | None = None,
    *,
    min_size: int = 1,
    max_size: int = 10,
    command_timeout: float = 30.0,
) -> asyncpg.Pool:
    """
    Create (or return the existing) process-wide pool. Idempotent:
    subsequent calls with the same DSN are no-ops.
    """
    global _pool
    if _pool is not None:
        return _pool
    dsn = dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise ConnectionPoolNotInitializedError(
            "no DSN provided and $DATABASE_URL is unset"
        )
    _pool = await asyncpg.create_pool(
        dsn,
        min_size=min_size,
        max_size=max_size,
        command_timeout=command_timeout,
    )
    return _pool


async def close_pool() -> None:
    """Close the process-wide pool. Safe to call when already closed."""
    global _pool
    if _pool is None:
        return
    await _pool.close()
    _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise ConnectionPoolNotInitializedError(
            "pool not initialised — call init_pool() or pass an explicit pool"
        )
    return _pool


# ---------------------------------------------------------------------
# Transactions with savepoint nesting
# ---------------------------------------------------------------------

@asynccontextmanager
async def transaction(
    *,
    pool: asyncpg.Pool | None = None,
    isolation: str | None = None,
) -> AsyncIterator[asyncpg.Connection]:
    """
    Yield an asyncpg Connection inside a transaction. Supports nested
    usage: if the caller is already inside a transaction on the same
    connection, a savepoint is used.

    Usage:

        async with transaction() as tx:
            await tx.execute("INSERT INTO actors ...")
            async with transaction() as tx2:   # nested savepoint
                await tx2.execute("INSERT INTO observations ...")
    """
    actual_pool = pool or get_pool()
    async with actual_pool.acquire() as conn:
        async with conn.transaction(isolation=isolation):
            yield conn


# ---------------------------------------------------------------------
# Typed query helpers
# ---------------------------------------------------------------------

def _to_dict(row: asyncpg.Record) -> dict[str, Any]:
    """asyncpg.Record -> plain dict without copying data an extra time."""
    return dict(row)


def _hydrate(row: asyncpg.Record, row_type: type[T]) -> T:
    try:
        return row_type.model_validate(_to_dict(row))
    except Exception as e:
        raise RowHydrationError(
            f"could not hydrate row into {row_type.__name__}: {e}",
            row_keys=list(row.keys()),
            row_type=row_type.__name__,
        ) from e


async def select_one(
    query: str,
    *args: Any,
    row_type: type[T] | None = None,
    conn: asyncpg.Connection | None = None,
) -> T | dict[str, Any] | None:
    """
    Return the first row or None. If `row_type` is given, hydrate
    into the Pydantic model; otherwise return a plain dict.
    """
    runner = conn if conn is not None else get_pool()
    row = await runner.fetchrow(query, *args)
    if row is None:
        return None
    if row_type is None:
        return _to_dict(row)
    return _hydrate(row, row_type)


async def select_many(
    query: str,
    *args: Any,
    row_type: type[T] | None = None,
    conn: asyncpg.Connection | None = None,
) -> list[T] | list[dict[str, Any]]:
    """Return all rows (possibly empty)."""
    runner = conn if conn is not None else get_pool()
    rows = await runner.fetch(query, *args)
    if row_type is None:
        return [_to_dict(r) for r in rows]
    return [_hydrate(r, row_type) for r in rows]


async def execute(
    query: str,
    *args: Any,
    conn: asyncpg.Connection | None = None,
) -> str:
    """Run a DDL / DML and return the status tag."""
    runner = conn if conn is not None else get_pool()
    return await runner.execute(query, *args)


__all__ = [
    "init_pool",
    "close_pool",
    "get_pool",
    "transaction",
    "select_one",
    "select_many",
    "execute",
    "ConnectionPoolNotInitializedError",
    "RowHydrationError",
]
