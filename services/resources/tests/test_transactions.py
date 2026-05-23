"""Tests for services/resources/transactions.py."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from lib.shared.errors import InvariantViolation, ValidationError

from services.resources import repo
from services.resources.transactions import record_transaction
from services.resources.tests.conftest import TENANT_A, make_observation


pytestmark = pytest.mark.asyncio


# ---------- integration: record_transaction ----------

async def test_record_transaction_acquire_financial(resources_db, event_id):
    r = await repo.create(
        kind="financial", identity="cash", current_value={"amount_cents": 0},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    tx_evt = await make_observation(resources_db)
    tx_row = await record_transaction(
        r.id,
        kind="acquire",
        delta={"amount_cents": 1000_00},
        occurred_at=datetime.now(timezone.utc),
        source_event_id=tx_evt,
    )
    assert tx_row.transaction_type == "acquire"
    reloaded = await repo.get(r.id)
    assert reloaded.current_value["amount_cents"] == 1000_00


async def test_record_transaction_spend_reduces(resources_db, event_id):
    r = await repo.create(
        kind="financial", identity="cash", current_value={"amount_cents": 500},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    tx_evt = await make_observation(resources_db)
    await record_transaction(
        r.id, kind="spend", delta={"amount_cents": 200},
        occurred_at=datetime.now(timezone.utc), source_event_id=tx_evt,
    )
    reloaded = await repo.get(r.id)
    assert reloaded.current_value["amount_cents"] == 300


async def test_record_transaction_capacity_deploy_updates_utilization(resources_db, event_id):
    r = await repo.create(
        kind="capacity", identity="eng",
        current_value={"total_units": 3, "deployed_units": 0, "available_units": 3},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    tx_evt = await make_observation(resources_db)
    await record_transaction(
        r.id, kind="deploy", delta={"deployed_units": 3},
        occurred_at=datetime.now(timezone.utc), source_event_id=tx_evt,
    )
    reloaded = await repo.get(r.id)
    assert reloaded.utilization_state == "depleted"
    assert reloaded.current_value["deployed_units"] == 3
    assert reloaded.current_value["available_units"] == 0


async def test_record_transaction_capacity_partial_deploy(resources_db, event_id):
    r = await repo.create(
        kind="capacity", identity="eng",
        current_value={"total_units": 5, "deployed_units": 0, "available_units": 5},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    tx_evt = await make_observation(resources_db)
    await record_transaction(
        r.id, kind="deploy", delta={"deployed_units": 2},
        occurred_at=datetime.now(timezone.utc), source_event_id=tx_evt,
    )
    reloaded = await repo.get(r.id)
    assert reloaded.utilization_state == "deployed"


async def test_record_transaction_insufficient_capacity_raises_r1(resources_db, event_id):
    r = await repo.create(
        kind="capacity", identity="eng",
        current_value={"total_units": 1, "deployed_units": 0, "available_units": 1},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    tx_evt = await make_observation(resources_db)
    with pytest.raises(InvariantViolation) as ei:
        await record_transaction(
            r.id, kind="deploy", delta={"deployed_units": 5},
            occurred_at=datetime.now(timezone.utc), source_event_id=tx_evt,
        )
    assert ei.value.invariant == "R1"


async def test_record_transaction_relational_weaken(resources_db, event_id):
    r = await repo.create(
        kind="relational", identity="customer:acme",
        current_value={"counterparty_id": "acme", "arr_cents": 50_000_00, "strength": "strong"},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    tx_evt = await make_observation(resources_db)
    await record_transaction(
        r.id, kind="weaken", delta={"strength_delta": -2},
        occurred_at=datetime.now(timezone.utc), source_event_id=tx_evt,
    )
    reloaded = await repo.get(r.id)
    assert reloaded.current_value["strength"] == "weakening"


async def test_record_transaction_ip_expire(resources_db, event_id):
    r = await repo.create(
        kind="ip", identity="patent_1",
        current_value={"type": "patent", "registration_id": "US1"},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    tx_evt = await make_observation(resources_db)
    await record_transaction(
        r.id, kind="expire", delta={"reason": "term_end"},
        occurred_at=datetime.now(timezone.utc), source_event_id=tx_evt,
    )
    reloaded = await repo.get(r.id)
    assert reloaded.utilization_state == "expired"
    assert reloaded.current_value.get("expired") is True


async def test_record_transaction_invalid_type_rejected(resources_db, event_id):
    r = await repo.create(
        kind="financial", identity="cash", current_value={"amount_cents": 0},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    tx_evt = await make_observation(resources_db)
    with pytest.raises(ValidationError):
        await record_transaction(
            r.id, kind="explode",  # type: ignore[arg-type]
            delta={}, occurred_at=datetime.now(timezone.utc),
            source_event_id=tx_evt,
        )


async def test_record_transaction_ensures_partition(resources_db, event_id):
    """A far-future occurred_at triggers partition creation."""
    r = await repo.create(
        kind="financial", identity="cash", current_value={"amount_cents": 0},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    tx_evt = await make_observation(resources_db)
    future = datetime.now(timezone.utc).replace(microsecond=0)
    # Add ~1 year to step outside the Wave-0 window.
    future = future.replace(year=future.year + 1)
    tx_row = await record_transaction(
        r.id, kind="acquire", delta={"amount_cents": 100},
        occurred_at=future, source_event_id=tx_evt,
    )
    assert tx_row.occurred_at.year == future.year


async def test_record_transaction_emits_state_change(resources_db, event_id):
    r = await repo.create(
        kind="financial", identity="cash", current_value={"amount_cents": 0},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    tx_evt = await make_observation(resources_db)
    await record_transaction(
        r.id, kind="acquire", delta={"amount_cents": 100},
        occurred_at=datetime.now(timezone.utc), source_event_id=tx_evt,
    )
    async with resources_db.acquire() as c:
        rows = await c.fetch(
            """
            SELECT content->>'state_change_kind' AS sc
            FROM observations
            WHERE kind = 'state_change'
              AND content->>'entity_id' = $1
            ORDER BY occurred_at DESC
            """,
            str(r.id),
        )
    sc_kinds = {r["sc"] for r in rows}
    assert "resource_acquire" in sc_kinds
    assert "resource_created" in sc_kinds


async def test_record_transaction_archived_rejected(resources_db, event_id):
    r = await repo.create(
        kind="financial", identity="cash", current_value={"amount_cents": 0},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    await repo.archive(r.id, reason="r", cause_event_id=event_id)
    tx_evt = await make_observation(resources_db)
    with pytest.raises(InvariantViolation):
        await record_transaction(
            r.id, kind="acquire", delta={"amount_cents": 1},
            occurred_at=datetime.now(timezone.utc), source_event_id=tx_evt,
        )


async def test_record_transaction_missing_resource(resources_db):
    tx_evt = await make_observation(resources_db)
    with pytest.raises(ValidationError):
        await record_transaction(
            uuid4(),  # random non-existent
            kind="acquire", delta={"amount_cents": 1},
            occurred_at=datetime.now(timezone.utc), source_event_id=tx_evt,
        )


# ---------- Property: capacity invariant ----------

async def test_capacity_invariant_over_random_sequence(resources_db, event_id):
    """
    For a capacity resource, after any legal sequence of acquire/deploy/release:
      total_units == available_units + deployed_units
    """
    import random
    random.seed(42)
    r = await repo.create(
        kind="capacity", identity="eng",
        current_value={"total_units": 10, "deployed_units": 0, "available_units": 10},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    tx_evt = await make_observation(resources_db)
    for _ in range(20):
        roll = random.random()
        if roll < 0.33:
            # acquire
            n = random.randint(1, 3)
            await record_transaction(
                r.id, kind="acquire", delta={"deployed_units": n},
                occurred_at=datetime.now(timezone.utc), source_event_id=tx_evt,
            )
        elif roll < 0.66:
            reloaded = await repo.get(r.id)
            avail = int(reloaded.current_value.get("available_units", 0))
            if avail <= 0:
                continue
            n = random.randint(1, max(1, avail))
            await record_transaction(
                r.id, kind="deploy", delta={"deployed_units": n},
                occurred_at=datetime.now(timezone.utc), source_event_id=tx_evt,
            )
        else:
            reloaded = await repo.get(r.id)
            deployed = int(reloaded.current_value.get("deployed_units", 0))
            if deployed <= 0:
                continue
            n = random.randint(1, deployed)
            await record_transaction(
                r.id, kind="release", delta={"deployed_units": n},
                occurred_at=datetime.now(timezone.utc), source_event_id=tx_evt,
            )
    reloaded = await repo.get(r.id)
    cv = reloaded.current_value
    assert int(cv["total_units"]) == int(cv["available_units"]) + int(cv["deployed_units"])
