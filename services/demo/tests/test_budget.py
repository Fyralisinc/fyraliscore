"""services/demo/tests/test_budget.py — DemoBudget cost-cap behavior."""
from __future__ import annotations

from decimal import Decimal

import asyncpg
import pytest

from lib.llm.provider import LLMUsage
from lib.shared.ids import uuid7
from services.demo.budget import DemoBudget, mark_session_cost_capped
from services.demo.repo import (
    get_demo_config_by_company,
    get_demo_session,
    insert_demo_session,
    upsert_tenant,
)


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_budget_for_unknown_session_returns_none(
    fresh_db: asyncpg.Pool,
):
    budget = await DemoBudget.for_session(fresh_db, uuid7())
    assert budget is None


@pytest.mark.asyncio
async def test_budget_starts_under_cap(fresh_db: asyncpg.Pool):
    cfg = await get_demo_config_by_company(fresh_db, "truss")
    assert cfg is not None
    tid = uuid7()
    await upsert_tenant(
        fresh_db, tenant_id=tid, name="truss-budget",
        is_demo=True, demo_config_id=cfg.id,
    )
    session = await insert_demo_session(
        fresh_db, tenant_id=tid, demo_config_id=cfg.id, ceo_actor_id=None,
    )
    budget = await DemoBudget.for_session(fresh_db, session.id)
    assert budget is not None
    assert budget.tripped is False
    assert budget.cap_usd == Decimal("5.0000")
    assert budget.spent_usd == Decimal("0")


@pytest.mark.asyncio
async def test_budget_flush_persists_costs_and_advances_spent(
    fresh_db: asyncpg.Pool,
):
    cfg = await get_demo_config_by_company(fresh_db, "truss")
    assert cfg is not None
    tid = uuid7()
    await upsert_tenant(
        fresh_db, tenant_id=tid, name="truss-flush",
        is_demo=True, demo_config_id=cfg.id,
    )
    session = await insert_demo_session(
        fresh_db, tenant_id=tid, demo_config_id=cfg.id, ceo_actor_id=None,
    )
    budget = await DemoBudget.for_session(fresh_db, session.id)
    assert budget is not None

    budget.aggregator.record(
        LLMUsage(
            input_tokens=1000, output_tokens=500,
            model_name="claude-haiku-4-5", cost_usd=0.50,
        )
    )
    await budget.flush(fresh_db, call_kind="think")

    assert budget.spent_usd == Decimal("0.5")
    assert budget.aggregator.calls == []  # reset after flush

    refetched = await get_demo_session(fresh_db, session.id)
    assert refetched is not None
    assert refetched.total_cost_usd == Decimal("0.500000")


@pytest.mark.asyncio
async def test_budget_trips_when_cumulative_spend_exceeds_cap(
    fresh_db: asyncpg.Pool,
):
    cfg = await get_demo_config_by_company(fresh_db, "truss")  # cap=5.0
    assert cfg is not None
    tid = uuid7()
    await upsert_tenant(
        fresh_db, tenant_id=tid, name="truss-cap",
        is_demo=True, demo_config_id=cfg.id,
    )
    session = await insert_demo_session(
        fresh_db, tenant_id=tid, demo_config_id=cfg.id, ceo_actor_id=None,
    )
    budget = await DemoBudget.for_session(fresh_db, session.id)
    assert budget is not None

    budget.aggregator.record(
        LLMUsage(
            input_tokens=10_000, output_tokens=5_000,
            model_name="claude-opus-4-7", cost_usd=4.99,
        )
    )
    assert budget.tripped_after_call is False

    budget.aggregator.record(
        LLMUsage(
            input_tokens=10_000, output_tokens=5_000,
            model_name="claude-opus-4-7", cost_usd=0.05,
        )
    )
    assert budget.tripped_after_call is True

    await budget.flush(fresh_db, call_kind="think")
    assert budget.tripped is True

    await mark_session_cost_capped(fresh_db, session.id)
    refetched = await get_demo_session(fresh_db, session.id)
    assert refetched is not None
    assert refetched.cost_cap_breached_at is not None
