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
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, TypeVar
from urllib.parse import urlparse

import asyncpg
from pydantic import BaseModel

from lib.observability.pools import (
    observe_acquire_wait,
    register_pool,
    unregister_pool,
)
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


class UnsafeDatabaseRoleError(CompanyOSError):
    """The connected DB role can bypass production tenant isolation."""
    default_code = "unsafe_database_role"


class RlsPolicyShapeError(CompanyOSError):
    """Tenant-scoped tables do not have strict RLS policy shape."""
    default_code = "rls_policy_shape_error"


# ---------------------------------------------------------------------
# Pool lifecycle
# ---------------------------------------------------------------------

_pool: asyncpg.Pool | None = None


@dataclass(frozen=True)
class DatabaseSessionTimeouts:
    statement_timeout_ms: int = 30_000
    lock_timeout_ms: int = 5_000
    idle_in_transaction_session_timeout_ms: int = 60_000


@dataclass(frozen=True)
class RlsPolicyViolation:
    schema_name: str
    table_name: str
    reason: str

    def render(self) -> str:
        return f"{self.schema_name}.{self.table_name}: {self.reason}"


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


def _positive_int_env(name: str, *, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer number of milliseconds") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero milliseconds")
    return value


def positive_int_env(name: str, *, default: int) -> int:
    return _positive_int_env(name, default=default)


_TRUE_ENV_VALUES = frozenset({"1", "true", "yes", "on", "y"})
_FALSE_ENV_VALUES = frozenset({"0", "false", "no", "off", "n"})
_GLOBAL_PGBOUNCER_ENV = "POSTGRES_PGBOUNCER_COMPATIBLE"


def _bool_env_value(name: str, raw: str) -> bool:
    value = raw.strip().lower()
    if value in _TRUE_ENV_VALUES:
        return True
    if value in _FALSE_ENV_VALUES:
        return False
    raise ValueError(
        f"{name} must be one of "
        f"{sorted(_TRUE_ENV_VALUES | _FALSE_ENV_VALUES)}"
    )


def pgbouncer_compatible_from_env(
    *,
    process_env_var: str | None = None,
    default: bool = False,
) -> bool:
    """Return whether asyncpg pools should disable prepared caching.

    `POSTGRES_PGBOUNCER_COMPATIBLE=1` enables transaction-pooling
    compatibility process-wide. A process-specific env var can override it
    when a mixed deployment routes only some processes through pgbouncer.
    """

    env_names = (
        (process_env_var,) if process_env_var else ()
    ) + (_GLOBAL_PGBOUNCER_ENV,)
    for name in env_names:
        raw = os.environ.get(name)
        if raw is None or not raw.strip():
            continue
        return _bool_env_value(name, raw)
    return default


def asyncpg_pool_runtime_kwargs(
    *,
    dsn: str | None = None,
    pgbouncer_compatible: bool | None = None,
    process_env_var: str | None = None,
    default_pgbouncer_compatible: bool = False,
) -> dict[str, Any]:
    """Extra asyncpg.create_pool kwargs for the configured DB routing mode."""

    enabled = (
        pgbouncer_compatible
        if pgbouncer_compatible is not None
        else pgbouncer_compatible_from_env(
            process_env_var=process_env_var,
            default=default_pgbouncer_compatible,
        )
    )
    if not enabled:
        return {}
    if dsn is not None:
        _validate_dsn(dsn)
    return {"statement_cache_size": 0}


def session_timeout_settings_from_env() -> DatabaseSessionTimeouts:
    return DatabaseSessionTimeouts(
        statement_timeout_ms=_positive_int_env(
            "DB_STATEMENT_TIMEOUT_MS",
            default=DatabaseSessionTimeouts.statement_timeout_ms,
        ),
        lock_timeout_ms=_positive_int_env(
            "DB_LOCK_TIMEOUT_MS",
            default=DatabaseSessionTimeouts.lock_timeout_ms,
        ),
        idle_in_transaction_session_timeout_ms=_positive_int_env(
            "DB_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS",
            default=(
                DatabaseSessionTimeouts.idle_in_transaction_session_timeout_ms
            ),
        ),
    )


async def configure_connection_timeouts(
    conn: asyncpg.Connection,
    *,
    timeouts: DatabaseSessionTimeouts | None = None,
) -> None:
    """Apply per-session DB timeout guardrails to a newly opened connection."""

    timeouts = timeouts or session_timeout_settings_from_env()
    values = {
        "statement_timeout": timeouts.statement_timeout_ms,
        "lock_timeout": timeouts.lock_timeout_ms,
        "idle_in_transaction_session_timeout": (
            timeouts.idle_in_transaction_session_timeout_ms
        ),
    }
    for setting, value_ms in values.items():
        await conn.execute(
            "SELECT set_config($1, $2, false)",
            setting,
            f"{value_ms}ms",
        )


async def assert_database_role_is_production_safe(
    conn: asyncpg.Connection,
) -> None:
    """Fail if the current DB role can bypass row-level security."""

    row = await conn.fetchrow(
        """
        SELECT current_user AS role_name,
               rolsuper AS is_superuser,
               rolbypassrls AS bypass_rls
        FROM pg_roles
        WHERE rolname = current_user
        """
    )
    if row is None:
        raise UnsafeDatabaseRoleError("could not inspect current database role")
    role_name = str(row["role_name"])
    if bool(row["is_superuser"]):
        raise UnsafeDatabaseRoleError(
            f"database role {role_name!r} is SUPERUSER; production app roles "
            "must not bypass tenant RLS"
        )
    if bool(row["bypass_rls"]):
        raise UnsafeDatabaseRoleError(
            f"database role {role_name!r} has BYPASSRLS; production app roles "
            "must not bypass tenant RLS"
        )


async def find_strict_tenant_rls_policy_violations(
    conn: asyncpg.Connection,
    *,
    schema_name: str = "public",
) -> list[RlsPolicyViolation]:
    """Return tenant tables with disabled, unforced, missing, or bypass RLS."""

    rows = await conn.fetch(
        """
        WITH tenant_tables AS (
            SELECT c.oid,
                   n.nspname AS schema_name,
                   c.relname AS table_name,
                   c.relrowsecurity,
                   c.relforcerowsecurity
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a
              ON a.attrelid = c.oid
             AND a.attname = 'tenant_id'
             AND NOT a.attisdropped
            WHERE n.nspname = $1
              AND c.relkind IN ('r', 'p')
              AND NOT c.relispartition
        ),
        policy_summary AS (
            SELECT p.polrelid,
                   COUNT(*) AS policy_count,
                   BOOL_OR(
                       COALESCE(pg_get_expr(p.polqual, p.polrelid), '')
                           ~* 'current_setting\\(''app\\.current_tenant''[^)]*\\)\\s+IS\\s+NULL'
                       OR COALESCE(pg_get_expr(p.polwithcheck, p.polrelid), '')
                           ~* 'current_setting\\(''app\\.current_tenant''[^)]*\\)\\s+IS\\s+NULL'
                       OR COALESCE(pg_get_expr(p.polqual, p.polrelid), '')
                           ~* 'NULLIF\\s*\\(\\s*current_setting\\(''app\\.current_tenant'''
                       OR COALESCE(pg_get_expr(p.polwithcheck, p.polrelid), '')
                           ~* 'NULLIF\\s*\\(\\s*current_setting\\(''app\\.current_tenant'''
                   ) AS has_no_tenant_bypass
            FROM pg_policy p
            GROUP BY p.polrelid
        )
        SELECT tt.schema_name,
               tt.table_name,
               CASE
                   WHEN NOT tt.relrowsecurity THEN 'rls_disabled'
                   WHEN NOT tt.relforcerowsecurity THEN 'rls_not_forced'
                   WHEN COALESCE(ps.policy_count, 0) = 0 THEN 'missing_policy'
                   WHEN COALESCE(ps.has_no_tenant_bypass, FALSE)
                        THEN 'permits_unbound_current_tenant'
                   ELSE NULL
               END AS reason
        FROM tenant_tables tt
        LEFT JOIN policy_summary ps ON ps.polrelid = tt.oid
        WHERE NOT tt.relrowsecurity
           OR NOT tt.relforcerowsecurity
           OR COALESCE(ps.policy_count, 0) = 0
           OR COALESCE(ps.has_no_tenant_bypass, FALSE)
        ORDER BY tt.schema_name, tt.table_name
        """,
        schema_name,
    )
    return [
        RlsPolicyViolation(
            schema_name=str(row["schema_name"]),
            table_name=str(row["table_name"]),
            reason=str(row["reason"]),
        )
        for row in rows
    ]


async def assert_strict_tenant_rls_policies(
    conn: asyncpg.Connection,
    *,
    schema_name: str = "public",
) -> None:
    violations = await find_strict_tenant_rls_policy_violations(
        conn,
        schema_name=schema_name,
    )
    if not violations:
        return
    rendered = "; ".join(v.render() for v in violations[:12])
    suffix = "" if len(violations) <= 12 else f"; +{len(violations) - 12} more"
    raise RlsPolicyShapeError(
        "strict tenant RLS policy check failed: " + rendered + suffix
    )


async def assert_database_startup_safety(
    conn: asyncpg.Connection,
    *,
    require_strict_tenant_rls: bool = True,
) -> None:
    await assert_database_role_is_production_safe(conn)
    if require_strict_tenant_rls:
        await assert_strict_tenant_rls_policies(conn)


async def assert_pool_database_startup_safety(
    pool: asyncpg.Pool,
    *,
    require_strict_tenant_rls: bool = True,
) -> None:
    async with pool.acquire() as conn:
        await assert_database_startup_safety(
            conn,
            require_strict_tenant_rls=require_strict_tenant_rls,
        )


async def init_pool(
    dsn: str | None = None,
    *,
    min_size: int = 1,
    max_size: int = 10,
    command_timeout: float = 30.0,
    pgbouncer_compatible: bool = False,
    enforce_production_guards: bool = False,
    init: Callable[[asyncpg.Connection], Awaitable[None]] | None = None,
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

      See services/ingest/ingestion/db_config.py for which worker classes
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
    # asyncpg's prepared-statement cache is keyed per-connection.
    # Transaction-mode pgbouncer rotates server connections; the cache
    # must be off. See LLD §5.2.
    extra_kwargs = asyncpg_pool_runtime_kwargs(
        dsn=dsn,
        pgbouncer_compatible=pgbouncer_compatible,
    )

    async def _init_connection(conn: asyncpg.Connection) -> None:
        await configure_connection_timeouts(conn)
        if init is not None:
            await init(conn)

    _pool = await asyncpg.create_pool(
        dsn,
        min_size=min_size,
        max_size=max_size,
        command_timeout=command_timeout,
        init=_init_connection,
        **extra_kwargs,
    )
    # Scrape-time db_pool_* gauges (docs/architecture/observability_architecture.md §3).
    register_pool("shared", _pool)
    if enforce_production_guards:
        try:
            await assert_pool_database_startup_safety(_pool)
        except Exception:
            await close_pool()
            raise
    return _pool


async def close_pool() -> None:
    """Close the process-wide pool. Safe to call when already closed."""
    global _pool
    if _pool is None:
        return
    pool = _pool
    _pool = None
    unregister_pool("shared")
    try:
        await pool.close()
    except RuntimeError as exc:
        if "Event loop is closed" not in str(exc):
            raise


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
    pool_label = "shared" if actual_pool is _pool else "external"
    acquire_started = time.monotonic()
    async with actual_pool.acquire() as conn:
        observe_acquire_wait(pool_label, time.monotonic() - acquire_started)
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
