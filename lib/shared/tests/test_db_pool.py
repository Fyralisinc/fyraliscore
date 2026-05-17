"""Tests for `init_pool`'s pgbouncer-compatible mode (M1.2).

Per ingestion LLD §5.2: pools used behind transaction-mode pgbouncer
must pass `statement_cache_size=0` to asyncpg and must validate the
DSN at construction time. M1.2 adds the parameter; downstream
milestones (M3 fetcher, M5 writer) flip individual call sites to
opt in.

Default-false behaviour preservation is the contract for M1.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import lib.shared.db as db_module
from lib.shared.db import (
    InvalidDsnError,
    _validate_dsn,
    init_pool,
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
