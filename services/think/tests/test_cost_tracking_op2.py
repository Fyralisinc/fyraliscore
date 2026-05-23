"""services/think/tests/test_cost_tracking_op2.py — OP-2 tests.

THINK-DESIGN-AUDIT §9.3 — per-trigger cost attribution. Verifies:
  * `compute_cost_usd` math matches published pricing
  * `get_pricing_for_model` exact + substring + default fallthrough
  * `LLMUsageAggregator` accumulates multiple calls correctly
  * provider `_record_usage` bridges token counts into the aggregator
  * `record_think_run_cost` inserts the row with correct columns
  * per-tenant aggregation query returns expected totals
"""
from __future__ import annotations

import uuid

import asyncpg
import pytest
import pytest_asyncio

from lib.llm.provider import (
    LLMConfig,
    LLMProvider,
    LLMUsage,
    LLMUsageAggregator,
    compute_cost_usd,
    get_pricing_for_model,
    MODEL_PRICING,
)

from services.think.observability import (
    aggregate_costs_for_tenant,
    record_think_run_cost,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Pure unit tests — pricing + cost calc + aggregator
# ---------------------------------------------------------------------


def test_get_pricing_exact_match():
    p = get_pricing_for_model("deepseek-reasoner")
    assert p["input_per_mtok"] == 0.55
    assert p["output_per_mtok"] == 2.19


def test_get_pricing_substring_match():
    p = get_pricing_for_model("deepseek-reasoner-v2")
    assert p["input_per_mtok"] == 0.55


def test_get_pricing_default_fallthrough():
    p = get_pricing_for_model("some-unknown-model")
    assert p == MODEL_PRICING["default"]


def test_get_pricing_none_falls_through():
    assert get_pricing_for_model(None) == MODEL_PRICING["default"]


def test_compute_cost_usd_deepseek_reasoner():
    # 1M input / 1M output at reasoner prices = 0.55 + 2.19 = 2.74
    cost = compute_cost_usd(
        input_tokens=1_000_000,
        output_tokens=1_000_000,
        model_name="deepseek-reasoner",
    )
    assert abs(cost - 2.74) < 1e-9


def test_compute_cost_usd_scales_linearly():
    # 100K in / 100K out = (0.55 + 2.19) / 10 = 0.274
    cost = compute_cost_usd(
        input_tokens=100_000,
        output_tokens=100_000,
        model_name="deepseek-reasoner",
    )
    assert abs(cost - 0.274) < 1e-9


def test_aggregator_accumulates():
    agg = LLMUsageAggregator()
    agg.record(LLMUsage(
        input_tokens=1000, output_tokens=500,
        model_name="deepseek-reasoner",
        cost_usd=compute_cost_usd(
            input_tokens=1000, output_tokens=500,
            model_name="deepseek-reasoner",
        ),
    ))
    agg.record(LLMUsage(
        input_tokens=500, output_tokens=200,
        model_name="deepseek-reasoner",
        cost_usd=compute_cost_usd(
            input_tokens=500, output_tokens=200,
            model_name="deepseek-reasoner",
        ),
    ))
    assert agg.call_count == 2
    assert agg.total_input_tokens == 1500
    assert agg.total_output_tokens == 700
    assert abs(
        agg.total_cost_usd
        - compute_cost_usd(
            input_tokens=1500, output_tokens=700,
            model_name="deepseek-reasoner",
        )
    ) < 1e-9


def test_aggregator_reset():
    agg = LLMUsageAggregator()
    agg.record(LLMUsage(input_tokens=1, output_tokens=1, cost_usd=0.0))
    assert agg.call_count == 1
    agg.reset()
    assert agg.call_count == 0
    assert agg.total_cost_usd == 0.0


# ---------------------------------------------------------------------
# Provider bridge — _record_usage calls into the installed aggregator.
# ---------------------------------------------------------------------


class _MockProvider(LLMProvider):
    """A provider that bypasses _raw_call but records synthetic usage
    on every `structured`-like call. Exercises the aggregator bridge."""
    async def _raw_call(self, **kwargs):
        raise RuntimeError("not used")


def test_provider_bridge_records_to_aggregator():
    provider = _MockProvider(LLMConfig(
        provider="deepseek", api_key="x", model="deepseek-reasoner",
    ))
    agg = LLMUsageAggregator()
    provider.set_usage_aggregator(agg)

    # Directly exercise the bridge as the SDK paths would.
    provider._record_usage(100, 50)
    provider._record_usage(200, 80)

    assert agg.call_count == 2
    assert agg.total_input_tokens == 300
    assert agg.total_output_tokens == 130
    expected = compute_cost_usd(
        input_tokens=300, output_tokens=130,
        model_name="deepseek-reasoner",
    )
    assert abs(agg.total_cost_usd - expected) < 1e-9


def test_provider_bridge_noop_without_aggregator():
    """Calling `_record_usage` with no aggregator installed must not
    crash — it's a hot-path helper and exceptions here would kill real
    LLM calls."""
    provider = _MockProvider(LLMConfig(
        provider="deepseek", api_key="x", model="deepseek-reasoner",
    ))
    # No aggregator installed.
    provider._record_usage(100, 50)  # must not raise


# ---------------------------------------------------------------------
# record_think_run_cost — DB integration
# ---------------------------------------------------------------------


@pytest_asyncio.fixture
async def clean_costs(db_pool: asyncpg.Pool, tenant):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM think_run_costs WHERE tenant_id = $1", tenant,
        )
    yield
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM think_run_costs WHERE tenant_id = $1", tenant,
        )


async def test_record_think_run_cost_inserts_row(
    db_pool: asyncpg.Pool, tenant, clean_costs,
):
    trigger_id = uuid.uuid4()
    cost = compute_cost_usd(
        input_tokens=1234, output_tokens=567,
        model_name="deepseek-reasoner",
    )
    await record_think_run_cost(
        db_pool,
        trigger_id=trigger_id,
        tenant_id=tenant,
        trigger_kind="T2",
        outcome="success",
        llm_calls_count=2,
        llm_input_tokens_total=1234,
        llm_output_tokens_total=567,
        llm_cost_usd=cost,
        latency_total_ms=8_500,
        retry_count=0,
        model_name="deepseek-reasoner",
    )
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM think_run_costs WHERE trigger_id = $1",
            trigger_id,
        )
    assert row is not None
    assert row["tenant_id"] == tenant
    assert row["trigger_kind"] == "T2"
    assert row["outcome"] == "success"
    assert row["llm_calls_count"] == 2
    assert row["llm_input_tokens_total"] == 1234
    assert row["llm_output_tokens_total"] == 567
    # NUMERIC(12,6) truncates to 6 decimals, so we allow up to 1e-6.
    assert float(row["llm_cost_usd"]) == pytest.approx(cost, abs=1e-6)
    assert row["latency_total_ms"] == 8_500
    assert row["retry_count"] == 0
    assert row["model_name"] == "deepseek-reasoner"


async def test_record_think_run_cost_unknown_outcome_coerced(
    db_pool: asyncpg.Pool, tenant, clean_costs,
):
    """An outcome not in the CHECK constraint set is coerced to
    'failed' (with a warning log) rather than raising."""
    trigger_id = uuid.uuid4()
    await record_think_run_cost(
        db_pool,
        trigger_id=trigger_id,
        tenant_id=tenant,
        trigger_kind="T3",
        outcome="totally_made_up",
        llm_calls_count=0,
        llm_input_tokens_total=0,
        llm_output_tokens_total=0,
        llm_cost_usd=0.0,
        latency_total_ms=100,
    )
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT outcome FROM think_run_costs WHERE trigger_id = $1",
            trigger_id,
        )
    assert row["outcome"] == "failed"


async def test_per_tenant_aggregation(
    db_pool: asyncpg.Pool, tenant, clean_costs,
):
    """Insert multiple rows for a tenant, verify the aggregation query
    sums correctly per trigger kind."""
    for i in range(3):
        await record_think_run_cost(
            db_pool,
            trigger_id=uuid.uuid4(),
            tenant_id=tenant,
            trigger_kind="T2",
            outcome="success",
            llm_calls_count=1,
            llm_input_tokens_total=1000,
            llm_output_tokens_total=200,
            llm_cost_usd=0.001,
            latency_total_ms=500,
        )
    # One T3 row.
    await record_think_run_cost(
        db_pool,
        trigger_id=uuid.uuid4(),
        tenant_id=tenant,
        trigger_kind="T3",
        outcome="success",
        llm_calls_count=2,
        llm_input_tokens_total=5000,
        llm_output_tokens_total=1000,
        llm_cost_usd=0.01,
        latency_total_ms=2000,
    )
    agg = await aggregate_costs_for_tenant(
        db_pool, tenant_id=tenant, window_hours=24,
    )
    by_kind = {r["trigger_kind"]: r for r in agg["rows"]}
    assert by_kind["T2"]["runs"] == 3
    assert by_kind["T2"]["input_tokens"] == 3000
    assert by_kind["T2"]["output_tokens"] == 600
    assert by_kind["T2"]["total_cost_usd"] == pytest.approx(0.003, abs=1e-6)
    assert by_kind["T3"]["runs"] == 1
    assert by_kind["T3"]["input_tokens"] == 5000


async def test_aggregation_empty_tenant(db_pool: asyncpg.Pool):
    """Empty tenant returns an empty rows list (graceful)."""
    agg = await aggregate_costs_for_tenant(
        db_pool, tenant_id=uuid.uuid4(), window_hours=24,
    )
    assert agg["rows"] == []
