"""Integration tests for services/acts/decisions.py."""
from __future__ import annotations

import pytest

from lib.shared.errors import InvariantViolation, ValidationError
from services.acts import decisions
from services.acts.tests.conftest import TENANT_A, TENANT_B, make_observation


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_decision_create_drafted(acts_db, event_id):
    d = await decisions.create(
        title="adopt kubernetes",
        decision_text="we migrate to k8s",
        rationale="scalability",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    assert d.state == "drafted"
    assert d.decision_text == "we migrate to k8s"


async def test_decision_create_invalid_initial_state(acts_db, event_id):
    with pytest.raises(ValidationError):
        await decisions.create(
            title="bad",
            decision_text="nope",
            state="archived",   # can't create into archived
            created_by_event_id=event_id,
            tenant_id=TENANT_A,
        )


async def test_decision_create_requires_fields(acts_db, event_id):
    with pytest.raises(ValidationError):
        await decisions.create(
            title="",
            decision_text="nonempty",
            created_by_event_id=event_id,
            tenant_id=TENANT_A,
        )
    with pytest.raises(ValidationError):
        await decisions.create(
            title="ok",
            decision_text="",
            created_by_event_id=event_id,
            tenant_id=TENANT_A,
        )


async def test_decision_full_path(acts_db, event_id):
    d = await decisions.create(
        title="d",
        decision_text="do X",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    # drafted -> active -> revisited -> active -> archived
    d = await decisions.transition(d.id, "active", cause_event_id=event_id)
    assert d.state == "active"
    d = await decisions.transition(d.id, "revisited", cause_event_id=event_id)
    assert d.state == "revisited"
    d = await decisions.transition(d.id, "active", cause_event_id=event_id)
    assert d.state == "active"
    d = await decisions.transition(d.id, "archived", cause_event_id=event_id)
    assert d.state == "archived"
    assert d.archived_at is not None


async def test_decision_drafted_cannot_archive_directly(acts_db, event_id):
    d = await decisions.create(
        title="d",
        decision_text="x",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    with pytest.raises(InvariantViolation) as exc:
        await decisions.transition(d.id, "archived", cause_event_id=event_id)
    assert exc.value.invariant == "D_STATE"


async def test_decision_archived_is_terminal(acts_db, event_id):
    d = await decisions.create(
        title="d",
        decision_text="x",
        state="active",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    await decisions.transition(d.id, "archived", cause_event_id=event_id)
    with pytest.raises(InvariantViolation) as exc:
        await decisions.transition(d.id, "active", cause_event_id=event_id)
    assert exc.value.invariant == "D_STATE"


async def test_decision_tenant_isolation(acts_db, event_id):
    ev_b = await make_observation(acts_db, tenant_id=TENANT_B)
    d_a = await decisions.create(
        title="A",
        decision_text="a",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    d_b = await decisions.create(
        title="B",
        decision_text="b",
        created_by_event_id=ev_b,
        tenant_id=TENANT_B,
    )
    async with acts_db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id FROM decisions WHERE tenant_id = $1", TENANT_A
        )
    ids = {r["id"] for r in rows}
    assert d_a.id in ids
    assert d_b.id not in ids
