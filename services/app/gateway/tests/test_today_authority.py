from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lib.shared.ids import uuid7
from services.app.gateway import today_routes
from services.platform.access_control.authority import (
    AuthorityDecision,
    ObjectRef,
    Principal,
)
from services.product.decision_deltas.repo import DecisionDeltaView


pytestmark = pytest.mark.asyncio


class _FakeConn:
    pass


def _delta(
    *,
    tenant_id,
    source_recommendation_id=None,
    target_node_kind=None,
    target_node_id=None,
) -> DecisionDeltaView:
    now = datetime.now(timezone.utc)
    return DecisionDeltaView(
        id=uuid7(),
        tenant_id=tenant_id,
        status="proposed",
        label="needs_review",
        main_assertion="Review the proposed change.",
        current_state=None,
        suggested_update=None,
        target_node_kind=target_node_kind,
        target_node_id=target_node_id,
        confidence=0.7,
        confidence_basis=None,
        falsification_condition=None,
        consequence_preview=None,
        impact=None,
        category="customer_risk",
        source_recommendation_id=source_recommendation_id,
        created_at=now,
        updated_at=now,
        accepted_at=None,
        accepted_by=None,
        resolution_target_at=None,
        evidence=[],
    )


async def test_decision_delta_filter_drops_denied_source_model(monkeypatch) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    denied_model = uuid7()
    allowed_model = uuid7()
    views = [
        _delta(tenant_id=tenant_id, source_recommendation_id=denied_model),
        _delta(tenant_id=tenant_id, source_recommendation_id=allowed_model),
    ]

    async def fake_authorize_read(
        principal: Principal,
        purpose: str,
        object_ref: ObjectRef,
        *,
        conn,
    ) -> AuthorityDecision:
        if object_ref.object_id == denied_model:
            return AuthorityDecision(False, "restricted_model")
        return AuthorityDecision(True, "ok")

    monkeypatch.setattr(today_routes, "authorize_read", fake_authorize_read)

    filtered = await today_routes._filter_authorized_delta_views(
        views,
        principal=Principal(tenant_id=tenant_id, actor_id=actor_id),
        conn=_FakeConn(),  # type: ignore[arg-type]
    )

    assert [view.source_recommendation_id for view in filtered] == [allowed_model]


async def test_decision_delta_filter_drops_denied_target_ref(monkeypatch) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    denied_customer = uuid7()
    allowed_customer = uuid7()
    views = [
        _delta(
            tenant_id=tenant_id,
            target_node_kind="customer",
            target_node_id=denied_customer,
        ),
        _delta(
            tenant_id=tenant_id,
            target_node_kind="customer",
            target_node_id=allowed_customer,
        ),
    ]

    async def fake_authorize_read(
        principal: Principal,
        purpose: str,
        object_ref: ObjectRef,
        *,
        conn,
    ) -> AuthorityDecision:
        assert object_ref.object_kind == "resource"
        if object_ref.object_id == denied_customer:
            return AuthorityDecision(False, "restricted_customer")
        return AuthorityDecision(True, "ok")

    monkeypatch.setattr(today_routes, "authorize_read", fake_authorize_read)

    filtered = await today_routes._filter_authorized_delta_views(
        views,
        principal=Principal(tenant_id=tenant_id, actor_id=actor_id),
        conn=_FakeConn(),  # type: ignore[arg-type]
    )

    assert [view.target_node_id for view in filtered] == [allowed_customer]


async def test_unbacked_legacy_delta_remains_visible_until_provenance_exists() -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    view = _delta(tenant_id=tenant_id)

    assert await today_routes._authorized_delta_view(
        view,
        principal=Principal(tenant_id=tenant_id, actor_id=actor_id),
        conn=_FakeConn(),  # type: ignore[arg-type]
    )


async def test_topology_event_refs_map_customer_targets_to_resource_refs() -> None:
    tenant_id = uuid7()
    customer_id = uuid7()

    refs = today_routes._topology_event_refs(
        tenant_id=tenant_id,
        payload={
            "target_kind": "customer",
            "target_id": str(customer_id),
        },
    )

    assert refs == (
        ObjectRef(
            tenant_id=tenant_id,
            object_kind="resource",
            object_id=customer_id,
        ),
    )
