"""services/demo/tests/test_notifications_and_routing.py
Suppression + per-tenant model routing helpers."""
from __future__ import annotations

import os

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.demo.model_routing import (
    determinism_seed_for_tenant,
    resolve_model,
)
from services.demo.notifications import should_suppress
from services.demo.repo import get_demo_config_by_company, upsert_tenant


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_should_suppress_is_false_for_unknown_tenant(
    fresh_db: asyncpg.Pool,
):
    assert (await should_suppress(fresh_db, uuid7())) is False


@pytest.mark.asyncio
async def test_should_suppress_is_true_for_demo_tenant(
    fresh_db: asyncpg.Pool,
):
    cfg = await get_demo_config_by_company(fresh_db, "northwind")
    assert cfg is not None
    tid = uuid7()
    await upsert_tenant(
        fresh_db, tenant_id=tid, name="northwind-suppress",
        is_demo=True, demo_config_id=cfg.id,
    )
    assert (await should_suppress(fresh_db, tid)) is True


@pytest.mark.asyncio
async def test_should_suppress_is_false_for_non_demo_tenant(
    fresh_db: asyncpg.Pool,
):
    tid = uuid7()
    await upsert_tenant(
        fresh_db, tenant_id=tid, name="prod-tenant",
        is_demo=False, demo_config_id=None,
    )
    assert (await should_suppress(fresh_db, tid)) is False


@pytest.mark.asyncio
async def test_resolve_model_returns_fallback_for_non_demo(
    fresh_db: asyncpg.Pool,
):
    tid = uuid7()
    await upsert_tenant(
        fresh_db, tenant_id=tid, name="prod-tenant",
        is_demo=False, demo_config_id=None,
    )
    out = await resolve_model(
        fresh_db, tenant_id=tid, call_kind="think",
        fallback_model="claude-opus-4-7",
    )
    assert out == "claude-opus-4-7"


@pytest.mark.asyncio
async def test_resolve_model_returns_haiku_for_demo_tenant(
    fresh_db: asyncpg.Pool, monkeypatch
):
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    cfg = await get_demo_config_by_company(fresh_db, "truss")
    assert cfg is not None
    tid = uuid7()
    await upsert_tenant(
        fresh_db, tenant_id=tid, name="truss-routing",
        is_demo=True, demo_config_id=cfg.id,
    )
    out = await resolve_model(
        fresh_db, tenant_id=tid, call_kind="think",
        fallback_model="claude-opus-4-7",
    )
    # Truss config routes "think" → "haiku" (short-name expansion to
    # the canonical Anthropic model id).
    assert "haiku" in out


@pytest.mark.asyncio
async def test_determinism_seed_resolves_for_demo(fresh_db: asyncpg.Pool):
    cfg = await get_demo_config_by_company(fresh_db, "truss")
    assert cfg is not None
    tid = uuid7()
    await upsert_tenant(
        fresh_db, tenant_id=tid, name="truss-seed",
        is_demo=True, demo_config_id=cfg.id,
    )
    seed = await determinism_seed_for_tenant(fresh_db, tid)
    assert seed == 42


@pytest.mark.asyncio
async def test_determinism_seed_none_for_non_demo(fresh_db: asyncpg.Pool):
    tid = uuid7()
    await upsert_tenant(
        fresh_db, tenant_id=tid, name="prod-tenant",
        is_demo=False, demo_config_id=None,
    )
    assert (await determinism_seed_for_tenant(fresh_db, tid)) is None
