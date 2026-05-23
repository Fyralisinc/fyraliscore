"""Tests for services.greeting.cache.

Phase-1 exit gate: cache table created and working.
"""
from __future__ import annotations

import asyncio

import pytest

from services.greeting.cache import CACHE_KEYS, ViewCeoCacheRepo
from services.greeting.tests.conftest import TENANT_A, TENANT_B


pytestmark = pytest.mark.integration


async def test_migration_creates_table(greeting_db):
    """Phase 1 gate: the migration applied cleanly and the table
    accepts writes."""
    async with greeting_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS n FROM view_ceo_cache"
        )
        assert row["n"] == 0


async def test_set_and_get(greeting_db):
    cache = ViewCeoCacheRepo(greeting_db)
    await cache.set_cached(TENANT_A, "greeting", {"body_html": "hi"})
    got = await cache.get_cached(TENANT_A, "greeting")
    assert got is not None
    assert got.content == {"body_html": "hi"}
    assert got.tenant_id == TENANT_A
    assert got.cache_key == "greeting"
    assert got.recomputed_reason == "scheduled"
    assert got.staleness_seconds >= 0
    assert got.staleness_seconds < 5  # write just happened


async def test_set_overwrites(greeting_db):
    cache = ViewCeoCacheRepo(greeting_db)
    await cache.set_cached(TENANT_A, "greeting", {"body_html": "v1"})
    await cache.set_cached(
        TENANT_A, "greeting", {"body_html": "v2"}, reason="manual"
    )
    got = await cache.get_cached(TENANT_A, "greeting")
    assert got is not None
    assert got.content == {"body_html": "v2"}
    assert got.recomputed_reason == "manual"


async def test_get_missing(greeting_db):
    cache = ViewCeoCacheRepo(greeting_db)
    assert await cache.get_cached(TENANT_A, "greeting") is None


async def test_tenant_isolation(greeting_db):
    cache = ViewCeoCacheRepo(greeting_db)
    await cache.set_cached(TENANT_A, "greeting", {"for": "A"})
    await cache.set_cached(TENANT_B, "greeting", {"for": "B"})
    a = await cache.get_cached(TENANT_A, "greeting")
    b = await cache.get_cached(TENANT_B, "greeting")
    assert a is not None and a.content == {"for": "A"}
    assert b is not None and b.content == {"for": "B"}


async def test_invalidate(greeting_db):
    cache = ViewCeoCacheRepo(greeting_db)
    await cache.set_cached(TENANT_A, "greeting", {"v": 1})
    assert await cache.invalidate(TENANT_A, "greeting") is True
    assert await cache.get_cached(TENANT_A, "greeting") is None


async def test_get_all(greeting_db):
    cache = ViewCeoCacheRepo(greeting_db)
    for key in CACHE_KEYS:
        await cache.set_cached(TENANT_A, key, {"key": key})
    all_rows = await cache.get_all(TENANT_A)
    assert set(all_rows.keys()) == set(CACHE_KEYS)


async def test_staleness_grows(greeting_db):
    cache = ViewCeoCacheRepo(greeting_db)
    await cache.set_cached(TENANT_A, "greeting", {"v": 1})
    await asyncio.sleep(1.1)
    got = await cache.get_cached(TENANT_A, "greeting")
    assert got is not None
    assert got.staleness_seconds >= 1.0
