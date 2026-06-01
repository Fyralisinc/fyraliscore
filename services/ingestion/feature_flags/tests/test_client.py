"""Unit tests for services.ingestion.feature_flags.client (M2.1).

Verifies the 30s TTL cache contract, default-on semantics for missing
rows, and graceful handling of DB read failures (per M2's prime
directive: a flag-read error must not break the inline path).
"""
from __future__ import annotations

import time
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from services.ingestion.feature_flags import (
    SHADOW_WRITE_ENABLED,
    FlagCache,
    TenantFlags,
)


_TENANT = UUID("33333333-3333-3333-3333-333333333333")


def _pool_returning(row: dict | None) -> AsyncMock:
    pool = AsyncMock()
    pool.fetchrow = AsyncMock(return_value=row)
    return pool


# ---------------------------------------------------------------------
# Default-on semantics
# ---------------------------------------------------------------------

async def test_get_bool_returns_default_when_no_row() -> None:
    pool = _pool_returning(None)
    flags = TenantFlags(pool)

    val = await flags.get_bool(_TENANT, SHADOW_WRITE_ENABLED, default=True)
    assert val is True


async def test_get_bool_returns_row_value_when_present() -> None:
    pool = _pool_returning({"flag_value": False})
    flags = TenantFlags(pool)

    val = await flags.get_bool(_TENANT, SHADOW_WRITE_ENABLED, default=True)
    assert val is False


# ---------------------------------------------------------------------
# TTL cache behaviour. The TTL is 30s by default; we override to a
# tiny value for fast tests.
# ---------------------------------------------------------------------

async def test_cache_hit_avoids_pool_read_within_ttl() -> None:
    pool = _pool_returning({"flag_value": True})
    flags = TenantFlags(pool, cache=FlagCache(ttl_seconds=30.0))

    for _ in range(5):
        v = await flags.get_bool(_TENANT, SHADOW_WRITE_ENABLED, default=False)
        assert v is True
    # Only the first call hit the pool; subsequent reads came from cache.
    assert pool.fetchrow.await_count == 1


async def test_cache_expiry_triggers_re_read() -> None:
    pool = _pool_returning({"flag_value": True})
    cache = FlagCache(ttl_seconds=0.05)  # 50 ms
    flags = TenantFlags(pool, cache=cache)

    await flags.get_bool(_TENANT, SHADOW_WRITE_ENABLED, default=False)
    assert pool.fetchrow.await_count == 1

    time.sleep(0.08)  # past TTL

    await flags.get_bool(_TENANT, SHADOW_WRITE_ENABLED, default=False)
    assert pool.fetchrow.await_count == 2


async def test_invalidate_drops_entry() -> None:
    pool = _pool_returning({"flag_value": True})
    flags = TenantFlags(pool)
    await flags.get_bool(_TENANT, SHADOW_WRITE_ENABLED, default=False)
    flags.cache.invalidate(_TENANT, SHADOW_WRITE_ENABLED)
    await flags.get_bool(_TENANT, SHADOW_WRITE_ENABLED, default=False)
    assert pool.fetchrow.await_count == 2


# ---------------------------------------------------------------------
# Failure handling — DB read raises. Per M2 prime directive, this
# MUST NOT propagate; the reader returns the default.
# ---------------------------------------------------------------------

async def test_get_bool_returns_default_when_pool_raises() -> None:
    pool = AsyncMock()
    pool.fetchrow = AsyncMock(side_effect=RuntimeError("pool down"))
    flags = TenantFlags(pool)

    val = await flags.get_bool(_TENANT, SHADOW_WRITE_ENABLED, default=True)
    assert val is True  # default returned; no exception propagated


# ---------------------------------------------------------------------
# Per-tenant isolation — cache keyed by (tenant_id, flag_name).
# ---------------------------------------------------------------------

async def test_cache_keys_isolate_tenants() -> None:
    other = uuid4()
    rows = {_TENANT: {"flag_value": True}, other: {"flag_value": False}}
    pool = AsyncMock()
    async def _fetchrow(_sql, tenant_id, _flag):
        return rows.get(tenant_id)
    pool.fetchrow = AsyncMock(side_effect=_fetchrow)
    flags = TenantFlags(pool)

    assert await flags.get_bool(_TENANT, SHADOW_WRITE_ENABLED, default=False) is True
    assert await flags.get_bool(other, SHADOW_WRITE_ENABLED, default=False) is False
