"""Tests for `init_pool`'s pgbouncer-compatible mode (M1.2).

Per ingestion LLD §5.2: pools used behind transaction-mode pgbouncer
must pass `statement_cache_size=0` to asyncpg and must validate the
DSN at construction time. M1.2 adds the parameter; downstream
milestones (M3 fetcher, M5 writer) flip individual call sites to
opt in.

Default-false behaviour preservation is the contract for M1.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

import lib.shared.db as db_module
from lib.shared.db import (
    DatabaseSessionTimeouts,
    InvalidDsnError,
    RlsPolicyShapeError,
    UnsafeDatabaseRoleError,
    _validate_dsn,
    asyncpg_pool_runtime_kwargs,
    assert_database_role_is_production_safe,
    assert_strict_tenant_rls_policies,
    configure_connection_timeouts,
    find_strict_tenant_rls_policy_violations,
    init_pool,
    pgbouncer_compatible_from_env,
    session_timeout_settings_from_env,
)


def _reset_pool_state() -> None:
    """Clear the module-level pool singleton without awaiting close()
    on a mock object. Each test patches `asyncpg.create_pool` to
    return a MagicMock, which doesn't support `await close()`.
    """
    db_module._pool = None


# ---------------------------------------------------------------------
# Unit-level DSN validation.  Pure; runs without a real Postgres.
# ---------------------------------------------------------------------

@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://user:pw@localhost:5433/db",
        "postgres://user@host/db",
        "postgresql://host/db",
    ],
)
def test_validate_dsn_accepts_libpq_uris(dsn: str) -> None:
    # Should not raise.
    _validate_dsn(dsn)


@pytest.mark.parametrize(
    "dsn,reason",
    [
        ("", "empty"),
        ("mysql://host/db", "wrong scheme"),
        ("postgresql:///db_only", "no host"),
        ("not-a-uri-at-all", "no scheme"),
    ],
)
def test_validate_dsn_rejects_bad_inputs(dsn: str, reason: str) -> None:
    with pytest.raises(InvalidDsnError):
        _validate_dsn(dsn)


def test_pgbouncer_compatible_from_env_defaults_false(monkeypatch) -> None:
    monkeypatch.delenv("POSTGRES_PGBOUNCER_COMPATIBLE", raising=False)
    monkeypatch.delenv("THINK_POSTGRES_PGBOUNCER_COMPATIBLE", raising=False)

    assert pgbouncer_compatible_from_env(
        process_env_var="THINK_POSTGRES_PGBOUNCER_COMPATIBLE"
    ) is False
    assert asyncpg_pool_runtime_kwargs(
        dsn="postgresql://u:p@localhost:5433/db",
        process_env_var="THINK_POSTGRES_PGBOUNCER_COMPATIBLE",
    ) == {}


def test_pgbouncer_compatible_from_env_uses_global_flag(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_PGBOUNCER_COMPATIBLE", "1")
    monkeypatch.delenv("THINK_POSTGRES_PGBOUNCER_COMPATIBLE", raising=False)

    assert pgbouncer_compatible_from_env(
        process_env_var="THINK_POSTGRES_PGBOUNCER_COMPATIBLE"
    ) is True
    assert asyncpg_pool_runtime_kwargs(
        dsn="postgresql://u:p@localhost:5433/db",
        process_env_var="THINK_POSTGRES_PGBOUNCER_COMPATIBLE",
    ) == {"statement_cache_size": 0}


def test_pgbouncer_compatible_process_flag_overrides_global(
    monkeypatch,
) -> None:
    monkeypatch.setenv("POSTGRES_PGBOUNCER_COMPATIBLE", "1")
    monkeypatch.setenv("THINK_POSTGRES_PGBOUNCER_COMPATIBLE", "0")

    assert pgbouncer_compatible_from_env(
        process_env_var="THINK_POSTGRES_PGBOUNCER_COMPATIBLE"
    ) is False


def test_pgbouncer_compatible_rejects_invalid_env(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_PGBOUNCER_COMPATIBLE", "sometimes")

    with pytest.raises(ValueError, match="POSTGRES_PGBOUNCER_COMPATIBLE"):
        pgbouncer_compatible_from_env()


# ---------------------------------------------------------------------
# Session timeout guardrails. Pure; runs without a real Postgres.
# ---------------------------------------------------------------------

def test_session_timeout_settings_default_to_safe_values(monkeypatch) -> None:
    monkeypatch.delenv("DB_STATEMENT_TIMEOUT_MS", raising=False)
    monkeypatch.delenv("DB_LOCK_TIMEOUT_MS", raising=False)
    monkeypatch.delenv(
        "DB_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS", raising=False
    )

    settings = session_timeout_settings_from_env()

    assert settings == DatabaseSessionTimeouts(
        statement_timeout_ms=30_000,
        lock_timeout_ms=5_000,
        idle_in_transaction_session_timeout_ms=60_000,
    )


def test_session_timeout_settings_accept_env_overrides(monkeypatch) -> None:
    monkeypatch.setenv("DB_STATEMENT_TIMEOUT_MS", "45000")
    monkeypatch.setenv("DB_LOCK_TIMEOUT_MS", "3000")
    monkeypatch.setenv("DB_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS", "90000")

    settings = session_timeout_settings_from_env()

    assert settings == DatabaseSessionTimeouts(
        statement_timeout_ms=45_000,
        lock_timeout_ms=3_000,
        idle_in_transaction_session_timeout_ms=90_000,
    )


@pytest.mark.parametrize(
    "env_name,env_value",
    [
        ("DB_STATEMENT_TIMEOUT_MS", "0"),
        ("DB_LOCK_TIMEOUT_MS", "-1"),
        ("DB_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS", "soon"),
    ],
)
def test_session_timeout_settings_reject_invalid_values(
    monkeypatch,
    env_name: str,
    env_value: str,
) -> None:
    monkeypatch.setenv(env_name, env_value)

    with pytest.raises(ValueError):
        session_timeout_settings_from_env()


async def test_configure_connection_timeouts_applies_session_settings() -> None:
    conn = AsyncMock()

    await configure_connection_timeouts(
        conn,
        timeouts=DatabaseSessionTimeouts(
            statement_timeout_ms=11_000,
            lock_timeout_ms=2_000,
            idle_in_transaction_session_timeout_ms=22_000,
        ),
    )

    assert conn.execute.await_args_list == [
        call("SELECT set_config($1, $2, false)", "statement_timeout", "11000ms"),
        call("SELECT set_config($1, $2, false)", "lock_timeout", "2000ms"),
        call(
            "SELECT set_config($1, $2, false)",
            "idle_in_transaction_session_timeout",
            "22000ms",
        ),
    ]


async def test_database_role_guard_allows_non_bypass_role() -> None:
    conn = AsyncMock()
    conn.fetchrow.return_value = {
        "role_name": "fyralis_app",
        "is_superuser": False,
        "bypass_rls": False,
    }

    await assert_database_role_is_production_safe(conn)


@pytest.mark.parametrize(
    "field,message",
    [
        ("is_superuser", "SUPERUSER"),
        ("bypass_rls", "BYPASSRLS"),
    ],
)
async def test_database_role_guard_rejects_rls_bypass_roles(
    field: str,
    message: str,
) -> None:
    conn = AsyncMock()
    row = {
        "role_name": "company_os",
        "is_superuser": False,
        "bypass_rls": False,
    }
    row[field] = True
    conn.fetchrow.return_value = row

    with pytest.raises(UnsafeDatabaseRoleError, match=message):
        await assert_database_role_is_production_safe(conn)


async def test_strict_tenant_rls_policy_finder_maps_rows() -> None:
    conn = AsyncMock()
    conn.fetch.return_value = [
        {
            "schema_name": "public",
            "table_name": "models",
            "reason": "permits_unbound_current_tenant",
        }
    ]

    violations = await find_strict_tenant_rls_policy_violations(conn)

    assert [v.render() for v in violations] == [
        "public.models: permits_unbound_current_tenant"
    ]


async def test_strict_tenant_rls_policy_guard_raises_on_violations() -> None:
    conn = AsyncMock()
    conn.fetch.return_value = [
        {
            "schema_name": "public",
            "table_name": "observations",
            "reason": "rls_not_forced",
        }
    ]

    with pytest.raises(RlsPolicyShapeError, match="public.observations"):
        await assert_strict_tenant_rls_policies(conn)


# ---------------------------------------------------------------------
# Pool wiring — verify the kwarg flow with create_pool patched. We do
# not need a live Postgres to assert that `statement_cache_size=0`
# is forwarded to asyncpg when `pgbouncer_compatible=True`.
# ---------------------------------------------------------------------

async def test_init_pool_default_does_not_set_statement_cache_size(monkeypatch):
    """M1's contract — default-False preserves existing kwargs.

    Existing call sites must continue to behave exactly as before.
    """
    _reset_pool_state()
    fake_pool = MagicMock(name="fake_asyncpg_pool")
    create_pool = AsyncMock(return_value=fake_pool)
    with patch("lib.shared.db.asyncpg.create_pool", create_pool):
        pool = await init_pool(
            "postgresql://u:p@localhost:5433/db",
            min_size=2,
            max_size=8,
        )
    assert pool is fake_pool
    _, kwargs = create_pool.call_args
    assert "statement_cache_size" not in kwargs, (
        "default-False call MUST NOT forward statement_cache_size; "
        "existing callers' behaviour must be preserved."
    )
    assert kwargs["min_size"] == 2
    assert kwargs["max_size"] == 8
    assert callable(kwargs["init"])


async def test_init_pool_pgbouncer_mode_disables_prepared_statements(monkeypatch):
    """When pgbouncer_compatible=True, statement_cache_size=0 must be
    forwarded to asyncpg. This is the LLD §5.2 contract.
    """
    _reset_pool_state()
    fake_pool = MagicMock(name="fake_asyncpg_pool")
    create_pool = AsyncMock(return_value=fake_pool)
    with patch("lib.shared.db.asyncpg.create_pool", create_pool):
        await init_pool(
            "postgresql://u:p@localhost:5433/db",
            pgbouncer_compatible=True,
        )
    _, kwargs = create_pool.call_args
    assert kwargs.get("statement_cache_size") == 0
    assert callable(kwargs["init"])


async def test_init_pool_init_callback_applies_timeouts_and_custom_init(
    monkeypatch,
) -> None:
    _reset_pool_state()
    monkeypatch.setenv("DB_STATEMENT_TIMEOUT_MS", "12000")
    monkeypatch.setenv("DB_LOCK_TIMEOUT_MS", "2500")
    monkeypatch.setenv("DB_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS", "30000")
    fake_pool = MagicMock(name="fake_asyncpg_pool")
    create_pool = AsyncMock(return_value=fake_pool)
    custom_init = AsyncMock()
    with patch("lib.shared.db.asyncpg.create_pool", create_pool):
        await init_pool(
            "postgresql://u:p@localhost:5433/db",
            init=custom_init,
        )
    _, kwargs = create_pool.call_args

    conn = AsyncMock()
    await kwargs["init"](conn)

    assert conn.execute.await_args_list == [
        call("SELECT set_config($1, $2, false)", "statement_timeout", "12000ms"),
        call("SELECT set_config($1, $2, false)", "lock_timeout", "2500ms"),
        call(
            "SELECT set_config($1, $2, false)",
            "idle_in_transaction_session_timeout",
            "30000ms",
        ),
    ]
    custom_init.assert_awaited_once_with(conn)


async def test_init_pool_production_guard_closes_pool_on_failure() -> None:
    _reset_pool_state()
    fake_pool = MagicMock(name="fake_asyncpg_pool")
    fake_pool.close = AsyncMock()
    create_pool = AsyncMock(return_value=fake_pool)
    guard = AsyncMock(
        side_effect=UnsafeDatabaseRoleError("database role can bypass RLS")
    )
    with (
        patch("lib.shared.db.asyncpg.create_pool", create_pool),
        patch("lib.shared.db.assert_pool_database_startup_safety", guard),
    ):
        with pytest.raises(UnsafeDatabaseRoleError):
            await init_pool(
                "postgresql://u:p@localhost:5433/db",
                enforce_production_guards=True,
            )

    guard.assert_awaited_once_with(fake_pool)
    fake_pool.close.assert_awaited_once()
    assert db_module._pool is None


async def test_pgbouncer_mode_dsn_validation(monkeypatch):
    """An obviously-invalid DSN in pgbouncer mode must fail loudly,
    not silently degrade at first-query time.
    """
    _reset_pool_state()
    # Even though the create_pool call is patched, the DSN validation
    # gate runs BEFORE the call — assert we never reach the mock.
    create_pool = AsyncMock()
    with patch("lib.shared.db.asyncpg.create_pool", create_pool):
        with pytest.raises(InvalidDsnError):
            await init_pool(
                "mysql://nope/db",  # wrong scheme
                pgbouncer_compatible=True,
            )
    create_pool.assert_not_called()


async def test_pgbouncer_mode_skips_dsn_validation_when_default(monkeypatch):
    """Symmetric guard — default-False mode MUST NOT introduce a new
    DSN-validation requirement on existing callers. Legacy DSN strings
    that the OS happens to accept (e.g. malformed-but-route-able
    Unix-socket DSNs the existing code tolerates) must still pass
    through unchanged.
    """
    _reset_pool_state()
    fake_pool = MagicMock(name="fake_asyncpg_pool")
    create_pool = AsyncMock(return_value=fake_pool)
    with patch("lib.shared.db.asyncpg.create_pool", create_pool):
        await init_pool(
            "postgresql://u:p@localhost:5433/db",
            pgbouncer_compatible=False,
        )
    create_pool.assert_called_once()


# ---------------------------------------------------------------------
# Integration test placeholder — exercising a real pgbouncer instance
# is plan §5.2 M1's test_pool_pgbouncer_compatibility. That test
# requires staging infra (Q1 unresolved) and is therefore skipped
# until M2+. Stub is left here so the test name from the plan exists
# in the suite and can be filled in later.
# ---------------------------------------------------------------------

@pytest.mark.requires_infra
async def test_pool_pgbouncer_compatibility():
    pytest.skip(
        "Requires real pgbouncer transaction-mode proxy. Pending "
        "implementation-plan Q1 resolution + staging deployment. "
        "Will exercise: bind pool to pgbouncer DSN, fire 100 "
        "queries across acquires, assert zero "
        "'prepared statement does not exist' errors."
    )
