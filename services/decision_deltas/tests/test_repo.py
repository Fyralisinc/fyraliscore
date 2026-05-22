"""
services/decision_deltas/tests/test_repo.py — direct repo tests.

Covers create / list / get / status transitions / accept happy path.
Uses the gateway_pool fixture (per-test fresh DB).
"""
from __future__ import annotations

from datetime import datetime, timezone

import asyncpg
import pytest

from lib.shared.ids import uuid7

from services.decision_deltas import repo as dd_repo
from services.decision_deltas import apply as apply_mod

from .conftest import _ensure_tenant, seed_decision_delta


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_create_then_get_roundtrip(
    gateway_pool: asyncpg.Pool, tenant_id, seeded_actor,
):
    await _ensure_tenant(gateway_pool, tenant_id)
    async with gateway_pool.acquire() as conn:
        delta_id = await dd_repo.create_delta(
            conn,
            tenant_id=tenant_id,
            main_assertion="Engineering capacity will exceed 90%.",
            label="needs_review",
            current_state={"label": "Utilization", "value": "78%"},
            suggested_update={"label": "Utilization", "value": "92%"},
            target_node_kind="resource",
            confidence=0.66,
            confidence_basis="6 signals across 3 sources",
            category="capacity",
            impact={"teams_affected": ["platform", "infra"]},
            evidence=[
                {
                    "source": "linear",
                    "title": "Sprint velocity dipped 18%",
                    "ts": datetime.now(timezone.utc),
                    "trust_tier": "reputable",
                },
                {
                    "source": "github",
                    "title": "Open PR count +44%",
                    "ts": datetime.now(timezone.utc),
                    "trust_tier": "attested",
                    "weight": 0.7,
                },
            ],
        )

        loaded = await dd_repo.get_delta(
            conn, tenant_id=tenant_id, delta_id=delta_id,
        )
    assert loaded is not None
    assert loaded.status == "proposed"
    assert loaded.label == "needs_review"
    assert loaded.confidence == pytest.approx(0.66)
    assert loaded.category == "capacity"
    assert len(loaded.evidence) == 2
    assert {e.source for e in loaded.evidence} == {"linear", "github"}


@pytest.mark.asyncio
async def test_create_rejects_empty_assertion(
    gateway_pool: asyncpg.Pool, tenant_id,
):
    await _ensure_tenant(gateway_pool, tenant_id)
    async with gateway_pool.acquire() as conn:
        with pytest.raises(Exception) as exc:
            await dd_repo.create_delta(
                conn,
                tenant_id=tenant_id,
                main_assertion="   ",
            )
    assert "main_assertion" in str(exc.value)


@pytest.mark.asyncio
async def test_create_high_confidence_requires_falsifier(
    gateway_pool: asyncpg.Pool, tenant_id,
):
    await _ensure_tenant(gateway_pool, tenant_id)
    async with gateway_pool.acquire() as conn:
        with pytest.raises(Exception) as exc:
            await dd_repo.create_delta(
                conn,
                tenant_id=tenant_id,
                main_assertion="Highly confident claim.",
                confidence=0.95,
            )
    assert "falsification_condition" in str(exc.value)


@pytest.mark.asyncio
async def test_list_filters_by_status_and_category(
    gateway_pool: asyncpg.Pool, tenant_id,
):
    a = await seed_decision_delta(
        gateway_pool, tenant=tenant_id,
        main_assertion="A: customer risk shift",
        status="proposed",
        category="customer_risk",
    )
    b = await seed_decision_delta(
        gateway_pool, tenant=tenant_id,
        main_assertion="B: capacity",
        status="proposed",
        category="capacity",
    )
    c = await seed_decision_delta(
        gateway_pool, tenant=tenant_id,
        main_assertion="C: accepted already",
        status="accepted",
        category="customer_risk",
    )
    async with gateway_pool.acquire() as conn:
        proposed = await dd_repo.list_deltas(
            conn, tenant_id=tenant_id, status="proposed",
        )
        cust = await dd_repo.list_deltas(
            conn, tenant_id=tenant_id,
            status="proposed",
            category="customer_risk",
        )

    proposed_ids = {v.id for v in proposed}
    assert a in proposed_ids and b in proposed_ids
    assert c not in proposed_ids

    cust_ids = {v.id for v in cust}
    assert cust_ids == {a}


@pytest.mark.asyncio
async def test_list_filters_by_target(
    gateway_pool: asyncpg.Pool, tenant_id,
):
    target = uuid7()
    a = await seed_decision_delta(
        gateway_pool, tenant=tenant_id,
        main_assertion="targeted",
        target_node_kind="customer",
        target_node_id=target,
    )
    await seed_decision_delta(
        gateway_pool, tenant=tenant_id,
        main_assertion="elsewhere",
        target_node_kind="customer",
        target_node_id=uuid7(),
    )
    async with gateway_pool.acquire() as conn:
        rows = await dd_repo.list_deltas(
            conn, tenant_id=tenant_id,
            target_kind="customer", target_id=target,
        )
    assert {v.id for v in rows} == {a}


@pytest.mark.asyncio
async def test_update_status_enforces_transition_rules(
    gateway_pool: asyncpg.Pool, tenant_id, seeded_actor,
):
    delta_id = await seed_decision_delta(
        gateway_pool, tenant=tenant_id, status="proposed",
    )
    async with gateway_pool.acquire() as conn:
        # proposed -> delegated is allowed
        view = await dd_repo.update_status(
            conn, tenant_id=tenant_id, delta_id=delta_id,
            status="delegated", user_id=seeded_actor,
        )
        assert view.status == "delegated"

        # delegated -> proposed is NOT allowed
        with pytest.raises(dd_repo.InvalidStatusTransitionError):
            await dd_repo.update_status(
                conn, tenant_id=tenant_id, delta_id=delta_id,
                status="proposed", user_id=seeded_actor,
            )


@pytest.mark.asyncio
async def test_accept_and_apply_marks_accepted_and_emits_event(
    gateway_pool: asyncpg.Pool, tenant_id, seeded_actor,
):
    delta_id = await seed_decision_delta(
        gateway_pool, tenant=tenant_id, status="proposed",
        target_node_kind=None, target_node_id=None,
    )
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            view, triggered = await apply_mod.apply_acceptance(
                conn=conn,
                tenant_id=tenant_id,
                delta_id=delta_id,
                user_id=seeded_actor,
            )
    assert view.status == "accepted"
    assert view.accepted_by == seeded_actor
    assert view.accepted_at is not None
    # No target node so target_updated should be False.
    assert triggered["target_updated"] is False
    # Acceptance event written to topology_events.
    assert triggered["target_event_id"] is not None
    async with gateway_pool.acquire() as conn:
        evt = await conn.fetchrow(
            "SELECT kind, payload FROM topology_events "
            "WHERE id = $1",
            triggered["target_event_id"],
        )
    assert evt is not None
    assert evt["kind"] == "drift"
    payload = evt["payload"]
    if isinstance(payload, str):
        import json
        payload = json.loads(payload)
    assert payload["event_kind"] == "decision_delta_accepted"
    assert payload["delta_id"] == str(delta_id)


@pytest.mark.asyncio
async def test_accept_twice_is_idempotent(
    gateway_pool: asyncpg.Pool, tenant_id, seeded_actor,
):
    delta_id = await seed_decision_delta(
        gateway_pool, tenant=tenant_id, status="proposed",
    )
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            await apply_mod.apply_acceptance(
                conn=conn, tenant_id=tenant_id,
                delta_id=delta_id, user_id=seeded_actor,
            )
        # Second accept should short-circuit without raising.
        async with conn.transaction():
            view, triggered = await apply_mod.apply_acceptance(
                conn=conn, tenant_id=tenant_id,
                delta_id=delta_id, user_id=seeded_actor,
            )
    assert view.status == "accepted"
    assert "already_accepted" in triggered["notes"]


@pytest.mark.asyncio
async def test_get_returns_none_for_other_tenant(
    gateway_pool: asyncpg.Pool, tenant_id, tenant_id_b,
):
    delta_id = await seed_decision_delta(
        gateway_pool, tenant=tenant_id,
    )
    async with gateway_pool.acquire() as conn:
        view_a = await dd_repo.get_delta(
            conn, tenant_id=tenant_id, delta_id=delta_id,
        )
        view_b = await dd_repo.get_delta(
            conn, tenant_id=tenant_id_b, delta_id=delta_id,
        )
    assert view_a is not None
    assert view_b is None
