from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.app.gateway import db_bootstrap


async def test_create_gateway_pool_reads_pool_size_env(monkeypatch) -> None:
    monkeypatch.setenv("GATEWAY_POSTGRES_POOL_SIZE", "12")
    fake_pool = MagicMock(name="gateway_pool")
    create_pool = AsyncMock(return_value=fake_pool)

    with patch("services.app.gateway.db_bootstrap.asyncpg.create_pool", create_pool):
        pool = await db_bootstrap.create_gateway_pool(
            "postgresql://u:p@localhost:5433/db"
        )

    assert pool is fake_pool
    _, kwargs = create_pool.call_args
    assert kwargs["max_size"] == 12
    assert kwargs["init"] is db_bootstrap._register_codecs
    db_bootstrap._db_module._pool = None


async def test_create_gateway_pool_explicit_max_overrides_env(monkeypatch) -> None:
    monkeypatch.setenv("GATEWAY_POSTGRES_POOL_SIZE", "12")
    fake_pool = MagicMock(name="gateway_pool")
    create_pool = AsyncMock(return_value=fake_pool)

    with patch("services.app.gateway.db_bootstrap.asyncpg.create_pool", create_pool):
        await db_bootstrap.create_gateway_pool(
            "postgresql://u:p@localhost:5433/db",
            max_size=4,
        )

    _, kwargs = create_pool.call_args
    assert kwargs["max_size"] == 4
    db_bootstrap._db_module._pool = None


async def test_create_gateway_pool_pgbouncer_env_disables_statement_cache(
    monkeypatch,
) -> None:
    monkeypatch.setenv("POSTGRES_PGBOUNCER_COMPATIBLE", "1")
    fake_pool = MagicMock(name="gateway_pool")
    create_pool = AsyncMock(return_value=fake_pool)

    with patch("services.app.gateway.db_bootstrap.asyncpg.create_pool", create_pool):
        await db_bootstrap.create_gateway_pool(
            "postgresql://u:p@localhost:5433/db",
        )

    _, kwargs = create_pool.call_args
    assert kwargs["statement_cache_size"] == 0
    db_bootstrap._db_module._pool = None


async def test_create_gateway_pool_rejects_pool_size_below_min(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GATEWAY_POSTGRES_POOL_SIZE", "1")

    with pytest.raises(ValueError):
        await db_bootstrap.create_gateway_pool(
            "postgresql://u:p@localhost:5433/db",
            min_size=2,
        )
