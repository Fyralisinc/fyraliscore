from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lib.shared.ids import uuid7
from services.platform.access_control.authority import (
    AuthorityDecision,
    ObjectRef,
    Principal,
)
from services.product.recommendations import repo as recommendations_repo


pytestmark = pytest.mark.asyncio


class _FakeConn:
    def __init__(self, *, model_rows: list[dict], target_rows: list[dict]) -> None:
        self.model_rows = model_rows
        self.target_rows = target_rows

    async def fetch(self, query: str, *args):
        if "FROM models m" in query:
            return self.model_rows
        if "FROM commitments WHERE id = ANY" in query:
            return self.target_rows
        if "FROM recommendation_feedback_stats" in query:
            return []
        raise AssertionError(f"unexpected query: {query}")


def _model_row(model_id, tenant_id, actor_id, commitment_id) -> dict:
    return {
        "id": model_id,
        "proposition": {
            "target_actor_id": str(actor_id),
            "target_act_ref": {"type": "commitment", "id": str(commitment_id)},
            "is_system_hypothesis": True,
        },
        "natural": f"Review commitment {model_id}",
        "confidence": 0.55,
        "proposition_kind": "hypothesis",
        "claim_role": "hypothesis",
        "target_actor_id": actor_id,
        "supporting_event_ids": [],
        "supporting_model_ids": [],
        "created_at": datetime.now(timezone.utc),
        "scope_entities": [],
    }


def _commitment_row(commitment_id, *, title: str = "Restricted commitment") -> dict:
    return {
        "id": commitment_id,
        "title": title,
        "state": "active",
        "archived_at": None,
    }


async def test_list_for_actor_drops_recommendation_model_denied_by_authority(
    monkeypatch,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    allowed_model = uuid7()
    denied_model = uuid7()
    commitment_id = uuid7()
    conn = _FakeConn(
        model_rows=[
            _model_row(denied_model, tenant_id, actor_id, commitment_id),
            _model_row(allowed_model, tenant_id, actor_id, commitment_id),
        ],
        target_rows=[_commitment_row(commitment_id)],
    )

    async def fake_authorize_read(
        principal: Principal,
        purpose: str,
        object_ref: ObjectRef,
        *,
        conn,
    ) -> AuthorityDecision:
        if object_ref.object_kind == "model" and object_ref.object_id == denied_model:
            return AuthorityDecision(False, "restricted_model")
        return AuthorityDecision(True, "ok")

    monkeypatch.setattr(recommendations_repo, "authorize_read", fake_authorize_read)

    views = await recommendations_repo.list_for_actor(
        tenant_id=tenant_id,
        target_actor_id=actor_id,
        conn=conn,  # type: ignore[arg-type]
        principal=Principal(tenant_id=tenant_id, actor_id=actor_id),
    )

    assert [view.id for view in views] == [allowed_model]


async def test_list_for_actor_drops_recommendation_when_target_is_denied(
    monkeypatch,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    model_id = uuid7()
    commitment_id = uuid7()
    conn = _FakeConn(
        model_rows=[_model_row(model_id, tenant_id, actor_id, commitment_id)],
        target_rows=[_commitment_row(commitment_id)],
    )

    async def fake_authorize_read(
        principal: Principal,
        purpose: str,
        object_ref: ObjectRef,
        *,
        conn,
    ) -> AuthorityDecision:
        if object_ref.object_kind == "commitment":
            return AuthorityDecision(False, "restricted_target")
        return AuthorityDecision(True, "ok")

    monkeypatch.setattr(recommendations_repo, "authorize_read", fake_authorize_read)

    views = await recommendations_repo.list_for_actor(
        tenant_id=tenant_id,
        target_actor_id=actor_id,
        conn=conn,  # type: ignore[arg-type]
        principal=Principal(tenant_id=tenant_id, actor_id=actor_id),
    )

    assert views == []
