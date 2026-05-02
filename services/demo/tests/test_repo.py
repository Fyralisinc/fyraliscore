"""services/demo/tests/test_repo.py — DB-backed tests for demo repo.

Exercises the migrations + the repo helpers against a real Postgres,
matching the convention used by services/recommendations/tests.
"""
from __future__ import annotations

from decimal import Decimal
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.demo.repo import (
    end_demo_session,
    get_active_session_for_tenant,
    get_demo_config_by_company,
    get_demo_config_by_id,
    get_demo_session,
    get_tenant,
    increment_signal_count,
    insert_demo_session,
    list_demo_configs,
    record_demo_session_cost,
    upsert_tenant,
)


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_demo_configs_seeded_by_migration(fresh_db: asyncpg.Pool):
    rows = await list_demo_configs(fresh_db)
    company_ids = {r.company_id for r in rows}
    assert company_ids == {"truss", "northwind", "meridian"}
    truss = await get_demo_config_by_company(fresh_db, "truss")
    assert truss is not None
    assert truss.cost_cap_usd_per_session == Decimal("5.0000")
    assert truss.notifications_suppressed is True
    assert truss.determinism_seed == 42


@pytest.mark.asyncio
async def test_tenant_upsert_marks_is_demo(fresh_db: asyncpg.Pool):
    tid = uuid7()
    cfg = await get_demo_config_by_company(fresh_db, "northwind")
    assert cfg is not None
    await upsert_tenant(
        fresh_db, tenant_id=tid, name="northwind-demo",
        is_demo=True, demo_config_id=cfg.id,
    )
    tenant = await get_tenant(fresh_db, tid)
    assert tenant is not None
    assert tenant.is_demo is True
    assert tenant.demo_config_id == cfg.id


@pytest.mark.asyncio
async def test_tenant_absent_returns_none(fresh_db: asyncpg.Pool):
    tenant = await get_tenant(fresh_db, uuid7())
    assert tenant is None


@pytest.mark.asyncio
async def test_session_lifecycle_active_then_ended(fresh_db: asyncpg.Pool):
    cfg = await get_demo_config_by_company(fresh_db, "truss")
    assert cfg is not None
    tid = uuid7()
    await upsert_tenant(
        fresh_db, tenant_id=tid, name="truss-test",
        is_demo=True, demo_config_id=cfg.id,
    )
    session = await insert_demo_session(
        fresh_db, tenant_id=tid, demo_config_id=cfg.id, ceo_actor_id=None,
    )
    assert session.ended_at is None
    assert session.total_cost_usd == Decimal("0")

    active = await get_active_session_for_tenant(fresh_db, tid)
    assert active is not None
    assert active.id == session.id

    await increment_signal_count(fresh_db, session.id)
    refetched = await get_demo_session(fresh_db, session.id)
    assert refetched is not None
    assert refetched.signals_injected == 1

    ended = await end_demo_session(fresh_db, session.id, end_reason="user_ended")
    assert ended is True
    final = await get_demo_session(fresh_db, session.id)
    assert final is not None
    assert final.ended_at is not None
    assert final.end_reason == "user_ended"

    no_active = await get_active_session_for_tenant(fresh_db, tid)
    assert no_active is None


@pytest.mark.asyncio
async def test_record_cost_updates_session_total(fresh_db: asyncpg.Pool):
    cfg = await get_demo_config_by_company(fresh_db, "northwind")
    assert cfg is not None
    tid = uuid7()
    await upsert_tenant(
        fresh_db, tenant_id=tid, name="northwind-cost-test",
        is_demo=True, demo_config_id=cfg.id,
    )
    session = await insert_demo_session(
        fresh_db, tenant_id=tid, demo_config_id=cfg.id, ceo_actor_id=None,
    )
    await record_demo_session_cost(
        fresh_db,
        demo_session_id=session.id,
        call_kind="think",
        model_name="claude-haiku-4-5",
        input_tokens=1000,
        output_tokens=200,
        cost_usd=0.0123,
    )
    await record_demo_session_cost(
        fresh_db,
        demo_session_id=session.id,
        call_kind="render",
        model_name="claude-haiku-4-5",
        input_tokens=500,
        output_tokens=100,
        cost_usd=0.0064,
    )
    refetched = await get_demo_session(fresh_db, session.id)
    assert refetched is not None
    # 0.0123 + 0.0064 = 0.0187 (Decimal addition is exact)
    assert refetched.total_cost_usd == Decimal("0.018700")
