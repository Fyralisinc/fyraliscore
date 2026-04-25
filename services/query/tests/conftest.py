"""Shared fixtures for services/query tests. Most tests are
unit-level — they mock retrieval + rendering so they run fast and
don't require a live DB."""
from __future__ import annotations

import pytest_asyncio


@pytest_asyncio.fixture(autouse=True)
async def _isolate_caches():
    """Every test gets a fresh classifier cache + default cache adapter
    so state from one test doesn't bleed into another."""
    from services.query.classifier import get_default_cache
    from services.query.adapters import get_default_cache_adapter
    await get_default_cache().clear()
    await get_default_cache_adapter().clear_all()
    yield
    await get_default_cache().clear()
    await get_default_cache_adapter().clear_all()
