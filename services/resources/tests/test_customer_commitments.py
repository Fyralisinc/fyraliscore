"""Tests for services/resources/customer_commitments.py."""
from __future__ import annotations

from decimal import Decimal

import pytest

from lib.shared.errors import ValidationError

from services.resources import repo, customer_commitments as cc
from services.resources.tests.conftest import (
    TENANT_A,
    TENANT_B,
    make_commitment,
)


pytestmark = pytest.mark.asyncio


async def _make_customer(pool, event_id, ident="customer:acme", arr_cents=50_000_00, tenant=TENANT_A):
    return await repo.create(
        kind="relational",
        identity=ident,
        current_value={
            "counterparty_id": "acme",
            "arr_cents": arr_cents,
            "contract_state": "active",
            "strength": "strong",
        },
        tenant_id=tenant,
        created_by_event_id=event_id,
    )


async def test_link_and_lookup(resources_db, event_id):
    customer = await _make_customer(resources_db, event_id)
    cmt = await make_commitment(resources_db)
    link = await cc.link_commitment(
        customer.id,
        cmt,
        tenant_id=TENANT_A,
        served_description="launch feature X",
        revenue_at_risk_usd=Decimal("42000.00"),
        relationship_kind="delivers",
        criticality="high",
    )
    assert link.tenant_id == TENANT_A
    assert link.revenue_at_risk_usd == Decimal("42000.00")
    assert link.criticality == "high"
    assert link.relationship_kind == "delivers"

    found = await cc.commitments_for_customer(customer.id, tenant_id=TENANT_A)
    assert len(found) == 1
    assert found[0][0].id == cmt
    assert found[0][1].served_description == "launch feature X"
    assert found[0][1].revenue_at_risk_usd == Decimal("42000.00")


async def test_link_is_idempotent_and_updates_all_mutable_fields(resources_db, event_id):
    customer = await _make_customer(resources_db, event_id)
    cmt = await make_commitment(resources_db)
    await cc.link_commitment(
        customer.id, cmt, tenant_id=TENANT_A,
        served_description="v1", criticality="medium",
    )
    await cc.link_commitment(
        customer.id, cmt, tenant_id=TENANT_A,
        served_description="v2", criticality="must_have",
        revenue_at_risk_usd=Decimal("9999.99"),
        relationship_kind="supports",
    )
    found = await cc.commitments_for_customer(customer.id, tenant_id=TENANT_A)
    assert len(found) == 1
    link = found[0][1]
    assert link.served_description == "v2"
    assert link.criticality == "must_have"
    assert link.revenue_at_risk_usd == Decimal("9999.99")
    assert link.relationship_kind == "supports"


async def test_unlink(resources_db, event_id):
    customer = await _make_customer(resources_db, event_id)
    cmt = await make_commitment(resources_db)
    await cc.link_commitment(customer.id, cmt, tenant_id=TENANT_A)
    removed = await cc.unlink(customer.id, cmt, tenant_id=TENANT_A)
    assert removed is True
    assert await cc.commitments_for_customer(customer.id, tenant_id=TENANT_A) == []
    # Second unlink returns False.
    assert await cc.unlink(customer.id, cmt, tenant_id=TENANT_A) is False


async def test_unlink_tenant_isolation(resources_db, event_id):
    """A tenant-B unlink MUST NOT delete a tenant-A linkage."""
    customer = await _make_customer(resources_db, event_id)
    cmt = await make_commitment(resources_db)
    await cc.link_commitment(customer.id, cmt, tenant_id=TENANT_A)
    # Wrong tenant — no match.
    removed = await cc.unlink(customer.id, cmt, tenant_id=TENANT_B)
    assert removed is False
    # Row still present under A.
    assert (
        await cc.commitments_for_customer(customer.id, tenant_id=TENANT_A)
        != []
    )


async def test_link_rejects_non_relational_resource(resources_db, event_id):
    bad = await repo.create(
        kind="financial", identity="c", current_value={"amount_cents": 0},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    cmt = await make_commitment(resources_db)
    with pytest.raises(ValidationError):
        await cc.link_commitment(bad.id, cmt, tenant_id=TENANT_A)


async def test_link_rejects_wrong_tenant_resource(resources_db, event_id):
    customer = await _make_customer(resources_db, event_id, tenant=TENANT_A)
    cmt = await make_commitment(resources_db, tenant_id=TENANT_A)
    with pytest.raises(ValidationError) as exc:
        await cc.link_commitment(customer.id, cmt, tenant_id=TENANT_B)
    assert "tenant mismatch" in exc.value.message


async def test_customers_for_commitment_reverse(resources_db, event_id):
    c1 = await _make_customer(resources_db, event_id, ident="customer:acme")
    c2 = await _make_customer(resources_db, event_id, ident="customer:globex")
    cmt = await make_commitment(resources_db)
    await cc.link_commitment(c1.id, cmt, tenant_id=TENANT_A)
    await cc.link_commitment(c2.id, cmt, tenant_id=TENANT_A)
    customers = await cc.customers_for_commitment(cmt, tenant_id=TENANT_A)
    assert set(customers) == {c1.id, c2.id}
