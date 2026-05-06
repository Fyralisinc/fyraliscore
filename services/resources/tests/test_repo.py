"""Tests for services/resources/repo.py — create/update/archive/search."""
from __future__ import annotations


import pytest

from lib.shared.errors import InvariantViolation, ValidationError

from services.resources import repo
from services.resources.tests.conftest import (
    TENANT_A,
    TENANT_B,
    make_observation,
)


pytestmark = pytest.mark.asyncio


# -----------------------------------------------------------------
# create — six kinds
# -----------------------------------------------------------------

async def test_create_financial(resources_db, event_id):
    row = await repo.create(
        kind="financial",
        identity="operating_cash",
        description="Main checking account",
        current_value={"amount_cents": 500_000_00, "currency": "USD", "account": "bofa"},
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
    )
    assert row.kind == "financial"
    assert row.current_value["amount_cents"] == 500_000_00
    assert row.utilization_state == "available"


async def test_create_ip(resources_db, event_id):
    row = await repo.create(
        kind="ip",
        identity="patent_US11234567",
        current_value={
            "type": "patent",
            "registration_id": "US11234567",
            "jurisdiction": "US",
            "filing_date": "2024-01-01",
            "expiration": "2044-01-01",
        },
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
    )
    assert row.kind == "ip"
    assert row.current_value["registration_id"] == "US11234567"


async def test_create_relational_customer(resources_db, event_id):
    row = await repo.create(
        kind="relational",
        identity="customer:acme",
        current_value={
            "counterparty_id": "acme",
            "arr_cents": 50_000_00,
            "contract_state": "active",
            "strength": "strong",
        },
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
    )
    assert row.current_value["arr_cents"] == 50_000_00
    assert row.current_value["strength"] == "strong"


async def test_create_capacity(resources_db, event_id):
    row = await repo.create(
        kind="capacity",
        identity="eng_team",
        current_value={"total_units": 10, "unit_type": "engineer", "deployed_units": 0, "available_units": 10},
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
    )
    assert row.current_value["total_units"] == 10
    assert row.current_value["available_units"] == 10


async def test_create_infrastructure(resources_db, event_id):
    row = await repo.create(
        kind="infrastructure",
        identity="office:sf",
        current_value={"capacity_spec": "50 seats", "renewal_terms": "annual", "expiration": "2027-01-01"},
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
        temporal_character="time_limited",
    )
    assert row.kind == "infrastructure"
    assert row.temporal_character == "time_limited"


async def test_create_regulatory(resources_db, event_id):
    row = await repo.create(
        kind="regulatory",
        identity="soc2:type2",
        current_value={"jurisdiction": "US", "scope": "platform", "expiration": "2026-12-31", "conditions": []},
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
    )
    assert row.kind == "regulatory"


async def test_create_invalid_kind_rejected(resources_db, event_id):
    with pytest.raises(ValidationError):
        await repo.create(
            kind="bogus",  # type: ignore[arg-type]
            identity="x",
            current_value={},
            tenant_id=TENANT_A,
            created_by_event_id=event_id,
        )


async def test_create_empty_identity_rejected(resources_db, event_id):
    with pytest.raises(ValidationError):
        await repo.create(
            kind="financial",
            identity="   ",
            current_value={"amount_cents": 0},
            tenant_id=TENANT_A,
            created_by_event_id=event_id,
        )


async def test_create_invalid_utilization_rejected(resources_db, event_id):
    with pytest.raises(ValidationError):
        await repo.create(
            kind="capacity",
            identity="eng_team",
            current_value={"total_units": 5, "available_units": 5, "deployed_units": 0},
            utilization_state="exploded",  # type: ignore[arg-type]
            tenant_id=TENANT_A,
            created_by_event_id=event_id,
        )


# -----------------------------------------------------------------
# create — state_change emission
# -----------------------------------------------------------------

async def test_create_emits_state_change(resources_db, event_id):
    row = await repo.create(
        kind="financial",
        identity="checking",
        current_value={"amount_cents": 100_00},
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
    )
    async with resources_db.acquire() as c:
        obs = await c.fetchrow(
            """
            SELECT kind, source_channel, content, cause_id
            FROM observations
            WHERE kind = 'state_change'
              AND content->>'entity_id' = $1
            ORDER BY occurred_at DESC LIMIT 1
            """,
            str(row.id),
        )
    assert obs is not None
    assert obs["source_channel"] == "internal:state_change"
    assert obs["content"]["state_change_kind"] == "resource_created"
    assert obs["cause_id"] == event_id


# -----------------------------------------------------------------
# update_attributes
# -----------------------------------------------------------------

async def test_update_attributes_merges_patch(resources_db, event_id):
    row = await repo.create(
        kind="financial",
        identity="checking",
        current_value={"amount_cents": 100, "currency": "USD"},
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
    )
    upd_event = await make_observation(resources_db)
    new = await repo.update_attributes(
        row.id,
        patch={"amount_cents": 200, "account": "chase"},
        last_updated_by_event_id=upd_event,
    )
    assert new.current_value["amount_cents"] == 200
    assert new.current_value["currency"] == "USD"  # preserved
    assert new.current_value["account"] == "chase"
    assert new.last_updated_by_event_id == upd_event


async def test_update_attributes_requires_some_patch(resources_db, event_id):
    row = await repo.create(
        kind="financial", identity="c", current_value={"amount_cents": 0},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    with pytest.raises(ValidationError):
        await repo.update_attributes(
            row.id, last_updated_by_event_id=event_id,
        )


async def test_update_attributes_on_missing_resource(resources_db, event_id):
    import uuid as _u
    with pytest.raises(ValidationError):
        await repo.update_attributes(
            _u.uuid4(),
            patch={"x": 1},
            last_updated_by_event_id=event_id,
        )


# -----------------------------------------------------------------
# archive
# -----------------------------------------------------------------

async def test_archive_sets_archived_at(resources_db, event_id):
    row = await repo.create(
        kind="ip", identity="p1", current_value={"type": "patent"},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    archived = await repo.archive(row.id, reason="superseded", cause_event_id=event_id)
    assert archived.archived_at is not None


async def test_archive_is_idempotent(resources_db, event_id):
    row = await repo.create(
        kind="ip", identity="p1", current_value={"type": "patent"},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    a1 = await repo.archive(row.id, reason="r", cause_event_id=event_id)
    a2 = await repo.archive(row.id, reason="r", cause_event_id=event_id)
    assert a1.archived_at == a2.archived_at


async def test_update_archived_rejected(resources_db, event_id):
    row = await repo.create(
        kind="financial", identity="c", current_value={"amount_cents": 0},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    await repo.archive(row.id, reason="r", cause_event_id=event_id)
    with pytest.raises(InvariantViolation):
        await repo.update_attributes(
            row.id, patch={"amount_cents": 99},
            last_updated_by_event_id=event_id,
        )


# -----------------------------------------------------------------
# search
# -----------------------------------------------------------------

async def test_search_by_kind_filters_archived(resources_db, event_id):
    r1 = await repo.create(
        kind="ip", identity="alpha_patent",
        current_value={"type": "patent"},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    r2 = await repo.create(
        kind="ip", identity="beta_patent",
        current_value={"type": "patent"},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    await repo.archive(r1.id, reason="r", cause_event_id=event_id)
    active = await repo.search_by_kind("ip", TENANT_A)
    ids = {r.id for r in active}
    assert r2.id in ids
    assert r1.id not in ids
    # With include_archived, r1 reappears.
    all_ = await repo.search_by_kind("ip", TENANT_A, include_archived=True)
    ids_all = {r.id for r in all_}
    assert r1.id in ids_all and r2.id in ids_all


async def test_archived_accessible_by_id(resources_db, event_id):
    row = await repo.create(
        kind="ip", identity="x", current_value={"type": "patent"},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    await repo.archive(row.id, reason="r", cause_event_id=event_id)
    fetched = await repo.get(row.id)
    assert fetched is not None and fetched.archived_at is not None


async def test_search_by_name_fuzzy(resources_db, event_id):
    await repo.create(
        kind="relational", identity="customer:acme_corp",
        description="Acme Corp — enterprise SaaS",
        current_value={"counterparty_id": "acme", "arr_cents": 10000},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    await repo.create(
        kind="relational", identity="customer:globex",
        description="Globex Industries",
        current_value={"counterparty_id": "globex", "arr_cents": 5000},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    # "acme" should match via identity trigram.
    hits = await repo.search_by_name_fuzzy("acme", TENANT_A)
    assert len(hits) >= 1
    assert any("acme" in h.identity for h in hits)


async def test_search_by_name_fuzzy_empty(resources_db, event_id):
    assert await repo.search_by_name_fuzzy("  ", TENANT_A) == []


# -----------------------------------------------------------------
# tenant isolation
# -----------------------------------------------------------------

async def test_tenant_isolation(resources_db, event_id):
    await repo.create(
        kind="financial", identity="a_cash", current_value={"amount_cents": 1},
        tenant_id=TENANT_A, created_by_event_id=event_id,
    )
    ev_b = await make_observation(resources_db, tenant_id=TENANT_B)
    await repo.create(
        kind="financial", identity="b_cash", current_value={"amount_cents": 2},
        tenant_id=TENANT_B, created_by_event_id=ev_b,
    )
    a = await repo.search_by_kind("financial", TENANT_A)
    b = await repo.search_by_kind("financial", TENANT_B)
    assert {r.identity for r in a} == {"a_cash"}
    assert {r.identity for r in b} == {"b_cash"}
