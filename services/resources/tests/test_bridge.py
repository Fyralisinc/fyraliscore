"""Tests for services/resources/bridge.py — primitives."""
from __future__ import annotations

import time
from decimal import Decimal
from datetime import datetime, timezone

import pytest

from lib.shared.db import transaction

from services.resources import bridge, customer_commitments as cc, deployments, repo
from services.resources.tests.conftest import (
    TENANT_A,
    make_commitment,
    make_decision,
    make_observation,
    set_commitment_state,
)


pytestmark = pytest.mark.asyncio


async def _make_customer(pool, event_id, ident="customer:acme", arr_cents=50_000_00, tenant=TENANT_A):
    return await repo.create(
        kind="relational",
        identity=ident,
        current_value={
            "counterparty_id": ident.split(":")[-1],
            "arr_cents": arr_cents,
            "strength": "strong",
        },
        tenant_id=tenant,
        created_by_event_id=event_id,
    )


# ---------------------------------------------------------------------
# revenue_at_risk_for_customer
# ---------------------------------------------------------------------

async def test_revenue_at_risk_blocked_commitment_returns_arr(resources_db, event_id):
    customer = await _make_customer(resources_db, event_id, arr_cents=100_000_00)
    cmt = await make_commitment(resources_db)
    await cc.link_commitment(customer.id, cmt, tenant_id=TENANT_A, served_description="critical work")
    await set_commitment_state(resources_db, cmt, "blocked")
    rar = await bridge.revenue_at_risk_for_customer(customer.id)
    assert rar == Decimal("100000.00")


async def test_revenue_at_risk_paused_returns_arr(resources_db, event_id):
    customer = await _make_customer(resources_db, event_id, arr_cents=2_500_00)
    cmt = await make_commitment(resources_db)
    await cc.link_commitment(customer.id, cmt, tenant_id=TENANT_A)
    await set_commitment_state(resources_db, cmt, "paused")
    assert await bridge.revenue_at_risk_for_customer(customer.id) == Decimal("2500.00")


async def test_revenue_at_risk_doneunverified_returns_arr(resources_db, event_id):
    customer = await _make_customer(resources_db, event_id, arr_cents=1_000_00)
    cmt = await make_commitment(resources_db)
    await cc.link_commitment(customer.id, cmt, tenant_id=TENANT_A)
    await set_commitment_state(resources_db, cmt, "doneunverified")
    assert await bridge.revenue_at_risk_for_customer(customer.id) == Decimal("1000.00")


async def test_revenue_at_risk_all_doneverified_zero(resources_db, event_id):
    customer = await _make_customer(resources_db, event_id, arr_cents=100_000_00)
    # Directly insert commitment with resolved event + doneverified to bypass
    # our repo's full invariant chain; set state via raw SQL after.
    c1 = await make_commitment(resources_db)
    await cc.link_commitment(customer.id, c1, tenant_id=TENANT_A)
    await set_commitment_state(resources_db, c1, "doneverified")
    assert await bridge.revenue_at_risk_for_customer(customer.id) == Decimal("0")


async def test_revenue_at_risk_zero_commitments(resources_db, event_id):
    customer = await _make_customer(resources_db, event_id, arr_cents=99_000_00)
    assert await bridge.revenue_at_risk_for_customer(customer.id) == Decimal("0")


async def test_revenue_at_risk_missing_customer(resources_db):
    import uuid
    # Random UUID that doesn't exist.
    assert await bridge.revenue_at_risk_for_customer(uuid.uuid4()) == Decimal("0")


async def test_revenue_at_risk_non_relational_returns_zero(resources_db, event_id):
    # Feeding a financial resource id to the customer function returns 0.
    r = await repo.create(
        kind="financial", identity="cash",
        current_value={"amount_cents": 1_000_00},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    assert await bridge.revenue_at_risk_for_customer(r.id) == Decimal("0")


# ---------------------------------------------------------------------
# capability_at_risk
# ---------------------------------------------------------------------

async def test_capability_at_risk_includes_depleted(resources_db, event_id):
    r = await repo.create(
        kind="capacity", identity="eng",
        current_value={"total_units": 2, "deployed_units": 0, "available_units": 2},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    c1 = await make_commitment(resources_db)
    c2 = await make_commitment(resources_db)
    ev = await make_observation(resources_db)
    await deployments.deploy(r.id, c1, quantity={"units": 1}, source_event_id=ev)
    await deployments.deploy(r.id, c2, quantity={"units": 1}, source_event_id=ev)
    at_risk = await bridge.capability_at_risk(TENANT_A)
    assert any(item["resource"].id == r.id for item in at_risk)
    entry = next(item for item in at_risk if item["resource"].id == r.id)
    assert entry["utilization"] == 1.0
    assert len(entry["deploying_commitments"]) == 2


async def test_capability_at_risk_excludes_under_utilized(resources_db, event_id):
    r = await repo.create(
        kind="capacity", identity="spacious",
        current_value={"total_units": 100, "deployed_units": 10, "available_units": 90},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    at_risk = await bridge.capability_at_risk(TENANT_A)
    assert all(item["resource"].id != r.id for item in at_risk)


# ---------------------------------------------------------------------
# feasibility_check
# ---------------------------------------------------------------------

async def test_feasibility_needs_capacity_insufficient(resources_db, event_id):
    r = await repo.create(
        kind="capacity", identity="eng",
        current_value={"total_units": 0, "deployed_units": 0, "available_units": 0},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    async with transaction() as tx:
        out = await bridge.feasibility_check(
            {
                "estimated_capacity": {
                    "deploys": [{"resource_id": r.id, "units": 2}]
                }
            },
            tx,
        )
    assert out["feasible"] is False
    assert any("available units" in s for s in out["reasons"])


async def test_feasibility_decision_revisited_is_warning(resources_db, event_id):
    dec = await make_decision(resources_db, state="revisited")
    async with transaction() as tx:
        out = await bridge.feasibility_check(
            {"constrained_by_decision_ids": [dec]}, tx,
        )
    assert out["feasible"] is True
    assert any("revisited" in w for w in out["warnings"])


async def test_feasibility_owner_overload_is_warning(resources_db, event_id):
    from services.resources.tests.conftest import make_actor
    owner = await make_actor(resources_db)
    # Give owner 6 active commitments (> threshold).
    for i in range(6):
        await make_commitment(resources_db, owner_id=owner, title=f"c{i}")
    async with transaction() as tx:
        out = await bridge.feasibility_check({"owner_id": owner}, tx)
    assert out["feasible"] is True
    assert any("active commitments" in w for w in out["warnings"])


async def test_feasibility_happy_path(resources_db, event_id):
    # Enough capacity, active decision, no overload.
    r = await repo.create(
        kind="capacity", identity="eng",
        current_value={"total_units": 10, "deployed_units": 0, "available_units": 10},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    dec = await make_decision(resources_db, state="active")
    async with transaction() as tx:
        out = await bridge.feasibility_check(
            {
                "estimated_capacity": {"deploys": [{"resource_id": r.id, "units": 3}]},
                "constrained_by_decision_ids": [dec],
            },
            tx,
        )
    assert out["feasible"] is True
    assert out["reasons"] == []
    assert out["warnings"] == []


# ---------------------------------------------------------------------
# External counterparty traversal
# ---------------------------------------------------------------------

async def test_external_counterparty_traversal(resources_db, event_id):
    """
    Commitment carries external_counterparty_ref pointing at the customer;
    we link it via customer_commitments and assert the bridge picks up risk.
    """
    customer = await _make_customer(resources_db, event_id, arr_cents=25_000_00)
    cmt = await make_commitment(resources_db)
    # Stamp external_counterparty_ref on the commitment (audit hint).
    async with resources_db.acquire() as c:
        await c.execute(
            """
            UPDATE commitments SET external_counterparty_ref = $2::jsonb
            WHERE id = $1
            """,
            cmt,
            '{"kind": "customer", "id": "' + str(customer.id) + '"}',
        )
    await cc.link_commitment(customer.id, cmt, tenant_id=TENANT_A, served_description="launch")
    await set_commitment_state(resources_db, cmt, "blocked")
    assert await bridge.revenue_at_risk_for_customer(customer.id) == Decimal("25000.00")


# ---------------------------------------------------------------------
# Hot-path benchmark (lite)
# ---------------------------------------------------------------------

async def test_revenue_at_risk_bulk_benchmark(resources_db, event_id):
    """
    Build N_CUSTOMERS customer resources with M_COMMITMENTS each; assert
    revenue_at_risk_all completes in < 800ms per BUILD-PLAN 5-B target.
    Lite scale to stay within unit-test time budget.
    """
    N_CUSTOMERS = 200
    M_COMMITMENTS = 5
    # Pre-create a single owner to avoid the per-commitment actor insert churn.
    from services.resources.tests.conftest import make_actor
    owner = await make_actor(resources_db)

    async with resources_db.acquire() as c:
        # Bulk-insert customers.
        rows: list[tuple] = []
        customer_ids = []
        for i in range(N_CUSTOMERS):
            from lib.shared.ids import uuid7
            rid = uuid7()
            customer_ids.append(rid)
            rows.append((
                rid, TENANT_A, "relational", f"customer:{i}",
                None,
                '{"arr_cents": 10000, "strength": "moderate"}',
                1.0, "available", "owned", "permanent", None,
            ))
        await c.executemany(
            """
            INSERT INTO resources (
              id, tenant_id, kind, identity, description, current_value,
              valuation_confidence, utilization_state, controllability,
              temporal_character, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10, $11::jsonb)
            """,
            rows,
        )
        # One observation for all commitments.
        obs_id = await make_observation(resources_db)
        # Build commitments.
        cmt_rows: list[tuple] = []
        link_rows: list[tuple] = []
        cmt_ids = []
        from lib.shared.ids import uuid7 as _u7
        from datetime import timedelta
        due = datetime.now(timezone.utc) + timedelta(days=30)
        for cust_id in customer_ids:
            for j in range(M_COMMITMENTS):
                cid = _u7()
                cmt_ids.append(cid)
                # Half of commitments blocked, half doneverified to create
                # heterogeneous at-risk pattern.
                state = "blocked" if j == 0 else "doneverified"
                cmt_rows.append((
                    cid, TENANT_A, f"c{j}", state, owner, due, obs_id
                ))
                link_rows.append((cust_id, cid, "served"))
        await c.executemany(
            """
            INSERT INTO commitments (
              id, tenant_id, title, state, owner_id, due_date,
              created_by_event_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            cmt_rows,
        )
        await c.executemany(
            """
            INSERT INTO customer_commitments (
              customer_resource_id, commitment_id, served_description
            ) VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            """,
            link_rows,
        )

    t0 = time.monotonic()
    total = await bridge.revenue_at_risk_all(TENANT_A)
    elapsed_ms = (time.monotonic() - t0) * 1000
    # Each customer's one `blocked` commitment makes them at-risk.
    assert total == (Decimal(10_000) * Decimal(N_CUSTOMERS) / Decimal(100)).quantize(Decimal("0.01"))
    assert elapsed_ms < 800, f"bulk at-risk scan too slow: {elapsed_ms:.0f}ms"
