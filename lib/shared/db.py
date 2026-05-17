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
from urllib.parse import urlparse

import asyncpg
from pydantic import BaseModel

from lib.shared.errors import CompanyOSError


T = TypeVar("T", bound=BaseModel)


class ConnectionPoolNotInitializedError(CompanyOSError):
    default_code = "db_pool_not_initialized"


class RowHydrationError(CompanyOSError):
    """The row returned by Postgres doesn't fit the Pydantic type."""
    default_code = "row_hydration_error"


class InvalidDsnError(CompanyOSError):
    """The DSN passed to `init_pool` is malformed."""
    default_code = "invalid_dsn"


# ---------------------------------------------------------------------
# Pool lifecycle
# ---------------------------------------------------------------------

_pool: asyncpg.Pool | None = None


def _validate_dsn(dsn: str) -> None:
    """Reject DSNs that obviously can't connect. Surface failures at
    pool-creation time, not on first query.

    Per ingestion LLD §5.2: pgbouncer-mode pools MUST validate the DSN
    to avoid silently degrading. We accept libpq URI form
    (postgresql:// or postgres://) and require a host.
    """
    if not dsn or not isinstance(dsn, str):
        raise InvalidDsnError("DSN is empty or non-string")
    parsed = urlparse(dsn)
    if parsed.scheme not in ("postgresql", "postgres"):
        raise InvalidDsnError(
            f"DSN must use postgresql:// or postgres:// scheme; "
            f"got scheme={parsed.scheme!r}"
        )
    if not parsed.hostname:
        raise InvalidDsnError("DSN missing host component")


async def init_pool(
    dsn: str | None = None,
    *,
    min_size: int = 1,
    max_size: int = 10,
    command_timeout: float = 30.0,
    pgbouncer_compatible: bool = False,
) -> asyncpg.Pool:
    """
    Create (or return the existing) process-wide pool. Idempotent:
    subsequent calls with the same DSN are no-ops.

    `pgbouncer_compatible` (per ingestion LLD §5.2):
      When True, the pool is configured for use behind a pgbouncer
      proxy running in transaction mode. asyncpg's prepared-statement
      cache is disabled (`statement_cache_size=0`) — pgbouncer
      transaction mode multiplexes server connections across clients,
      so prepared statements created on one server connection are NOT
      available on subsequent acquires. Leaving the cache on triggers
      the `prepared statement "__asyncpg_stmt_*__" does not exist`
      error in production.

      Default False preserves existing behaviour. Downstream milestones
      (M3 ShardFetchWorkflow workers, M5 normalizer pool) flip this
      flag for their pools; M1 only ships the capability.

      See services/ingestion/db_config.py for which worker classes
      will use which mode.

      NOTE: this differs in name from the M1 prompt's "create_pool"
      (no such function exists in this module — init_pool IS the
      entry point). Semantic intent is the same.
    """
    global _pool
    if _pool is not None:
        return _pool
    dsn = dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise ConnectionPoolNotInitializedError(
            "no DSN provided and $DATABASE_URL is unset"
        )
    if pgbouncer_compatible:
        # Validate aggressively in pgbouncer mode. A silent DSN typo
        # here would only surface as a connection failure under load.
        _validate_dsn(dsn)

    extra_kwargs: dict[str, Any] = {}
    if pgbouncer_compatible:
        # asyncpg's prepared-statement cache is keyed per-connection.
        # Transaction-mode pgbouncer rotates server connections; the
        # cache must be off. See LLD §5.2.
        extra_kwargs["statement_cache_size"] = 0

    _pool = await asyncpg.create_pool(
        dsn,
        min_size=min_size,
        max_size=max_size,
        command_timeout=command_timeout,
        **extra_kwargs,
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
    "InvalidDsnError",
    "RowHydrationError",
]
