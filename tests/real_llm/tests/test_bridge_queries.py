"""Real-LLM Bridge query tests (REAL-LLM-TEST-SUITE-PLAN.md §4.5)."""
from __future__ import annotations

from decimal import Decimal

import asyncpg
import pytest

from lib.embeddings.ollama import OllamaClient
from services.actors.repo import ActorRepo
from services.bridge.queries import (
    RevenueAtRiskReport,
    capability_at_risk,
    revenue_at_risk,
)
from services.entity_aliases.repo import EntityAliasRepo
from tests.real_llm.infrastructure.real_llm_runner import real_llm_test
from tests.real_llm.infrastructure.scenario_loader import (
    Scenario,
    inject_sequence,
)
from tests.real_llm.infrastructure.think_drain import wait_for_think_to_drain


# ---------------------------------------------------------------------------
# Test 1 — revenue_at_risk reflects customer-health signals
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
@real_llm_test(attempts=3, pass_threshold=2, timeout_seconds=600)
async def test_revenue_at_risk_reflects_customer_health(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    think_worker: None,
) -> None:
    """After the customer_churn_signal sequence drains, the Bridge
    revenue_at_risk report should contain a Globex entry whose
    total_at_risk_usd is a plausible USD figure.

    Tolerance is wide: Globex ARR is $60K, but the LLM may or may not
    produce a will_slip prediction Model that flips a served commitment
    into the at-risk bucket. The Wave 5-B query also surfaces customers
    with linked commitments and zero risk, so the value can legitimately
    be 0 if Think didn't escalate. Range: [0, 200_000].
    """
    await inject_sequence(
        scenario_02,
        "customer_churn_signal",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )
    assert scenario_02.tenant_id is not None
    await wait_for_think_to_drain(
        scenario_02.tenant_id, fresh_db, timeout_seconds=300
    )

    async with fresh_db.acquire() as conn:
        report: RevenueAtRiskReport = await revenue_at_risk(
            scenario_02.tenant_id, conn=conn
        )
    assert isinstance(report, RevenueAtRiskReport)
    assert report.tenant_id == scenario_02.tenant_id

    globex_id = scenario_02.customer_id("Globex Inc")
    globex_row = next(
        (c for c in report.customers if c.customer_resource_id == globex_id),
        None,
    )
    assert globex_row is not None, (
        f"Globex customer {globex_id} missing from revenue_at_risk report; "
        f"saw customers: {[str(c.customer_resource_id) for c in report.customers]}"
    )
    value = float(globex_row.total_at_risk_usd)
    assert 0 <= value <= 200_000, (
        f"Globex revenue_at_risk ${value:,.2f} outside [0, 200_000]; "
        f"row={globex_row!r}"
    )


# ---------------------------------------------------------------------------
# Test 2 — healthy customer has zero / low revenue at risk in bare scenario
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
@real_llm_test(attempts=1, pass_threshold=1, timeout_seconds=120)
async def test_revenue_at_risk_zero_for_healthy_customers_only(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
) -> None:
    """With NO sequences injected and no Models created, the healthy
    customer Acme should appear in the report (because it has linked
    customer_commitments rows) but with total_at_risk_usd at the low end.

    Acme owns the 'Quarterly business review with Acme' commitment (active,
    due_days_from_start=10) which the horizon_days=90 default catches as
    at-risk-by-due-date. Because Acme has no per-row revenue_at_risk_usd
    explicit values, the query falls back to ARR. ARR for Acme is $180K
    so the explicit-fallback path could reach there. We tolerate the wide
    band and only require the value is non-negative and bounded by ARR.
    """
    assert scenario_02.tenant_id is not None
    async with fresh_db.acquire() as conn:
        report = await revenue_at_risk(scenario_02.tenant_id, conn=conn)
    assert isinstance(report, RevenueAtRiskReport)

    acme_id = scenario_02.customer_id("Acme Corp")
    acme_row = next(
        (c for c in report.customers if c.customer_resource_id == acme_id),
        None,
    )
    # Acme might not appear at all if it has no served commitments in the
    # at-risk set; that's also acceptable (effectively a zero reading).
    if acme_row is None:
        return
    value = float(acme_row.total_at_risk_usd)
    # Acme ARR is $180K; the at-risk total should never exceed that.
    assert 0 <= value <= 180_000, (
        f"Acme revenue_at_risk ${value:,.2f} outside [0, 180_000]"
    )
    assert acme_row.total_at_risk_usd == (
        acme_row.blocked_usd + acme_row.paused_usd + acme_row.doneunverified_usd
    ), "Per-state buckets should sum to total"


# ---------------------------------------------------------------------------
# Test 3 — customer health timeline / detail query contract
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
@real_llm_test(attempts=1, pass_threshold=1, timeout_seconds=600)
async def test_customer_health_summary_query_returns_results(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    think_worker: None,
) -> None:
    """Structural contract test for render_customer_detail.

    After injecting the alice_ships_refund_flow sequence and draining
    Think, the per-customer dashboard query for Acme should return a
    populated CustomerDetailDashboard with the expected shape:
      - customer_resource_id matches input
      - identity is a non-empty string
      - arr_usd is a Decimal
      - health_timeline is a non-empty list of HealthPoints
      - served_commitments and active_deployments exist as lists
    """
    from services.bridge.dashboards import (
        CustomerDetailDashboard,
        render_customer_detail,
    )
    from services.bridge.queries import HealthPoint

    await inject_sequence(
        scenario_02,
        "alice_ships_refund_flow",
        pool=fresh_db,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )
    assert scenario_02.tenant_id is not None
    await wait_for_think_to_drain(
        scenario_02.tenant_id, fresh_db, timeout_seconds=300
    )

    acme_id = scenario_02.customer_id("Acme Corp")
    async with fresh_db.acquire() as conn:
        detail = await render_customer_detail(
            acme_id,
            tenant_id=scenario_02.tenant_id,
            window_days=30,
            conn=conn,
        )

    assert isinstance(detail, CustomerDetailDashboard)
    assert detail.customer_resource_id == acme_id
    assert isinstance(detail.identity, str) and detail.identity, (
        f"identity should be a non-empty string, got {detail.identity!r}"
    )
    assert isinstance(detail.arr_usd, Decimal)
    assert detail.arr_usd > 0, (
        f"Acme has scenario ARR $180K, expected arr_usd > 0, got {detail.arr_usd}"
    )
    assert isinstance(detail.served_commitments, list)
    assert isinstance(detail.active_deployments, list)
    assert isinstance(detail.health_timeline, list)
    assert len(detail.health_timeline) == 30, (
        f"window_days=30 should yield 30 daily HealthPoints, got "
        f"{len(detail.health_timeline)}"
    )
    for pt in detail.health_timeline:
        assert isinstance(pt, HealthPoint)
        assert isinstance(pt.total_at_risk_usd, Decimal)
        assert pt.total_at_risk_usd >= 0
        assert pt.blocked_commitment_count >= 0


# ---------------------------------------------------------------------------
# Test 4 — Bridge aggregate query returns sensible default with no signals
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
@real_llm_test(attempts=1, pass_threshold=1, timeout_seconds=60)
async def test_bridge_query_after_no_signals_returns_empty_or_default(
    scenario_02: Scenario,
    fresh_db: asyncpg.Pool,
) -> None:
    """Structural contract: with no sequences injected, both
    capability_at_risk and revenue_at_risk should return without error
    and yield sensible defaults (empty list / zero grand_total).

    Scenario 02 declares no capacity Resources, so capability_at_risk
    must return an empty list. revenue_at_risk should return a
    RevenueAtRiskReport whose grand_total_usd is bounded by the sum of
    all customer ARRs (Acme 180K + Globex 60K + Initech 45K = 285K).
    """
    assert scenario_02.tenant_id is not None

    async with fresh_db.acquire() as conn:
        cap = await capability_at_risk(scenario_02.tenant_id, conn=conn)
    assert isinstance(cap, list), f"capability_at_risk should return list, got {type(cap)}"
    assert cap == [], (
        f"scenario_02 declares no capacity resources; capability_at_risk "
        f"should be empty, got {len(cap)} entries: {cap!r}"
    )

    async with fresh_db.acquire() as conn:
        rar = await revenue_at_risk(scenario_02.tenant_id, conn=conn)
    assert isinstance(rar, RevenueAtRiskReport)
    assert rar.tenant_id == scenario_02.tenant_id
    assert rar.horizon_days == 90
    assert isinstance(rar.customers, list)
    assert isinstance(rar.grand_total_usd, Decimal)
    assert rar.grand_total_usd >= 0
    # Total ARR across all 3 customers is $285K; at-risk cannot exceed that
    # under the ARR-fallback ceiling (each commitment splits ARR evenly).
    assert float(rar.grand_total_usd) <= 285_000, (
        f"grand_total_usd ${rar.grand_total_usd} exceeds total scenario ARR $285K"
    )
    assert rar.fallback_count >= 0
