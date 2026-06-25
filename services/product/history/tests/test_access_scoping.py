"""Actor-scoped History payload and summary tests."""
from __future__ import annotations

import pytest

from services.platform.access_control.roles import grant_role
from services.product.history import build_history
from services.product.history.summary import build_summary


pytestmark = pytest.mark.integration


async def _make_private_for_actor(tx_conn, model_id, actor_id=None) -> None:
    await tx_conn.execute(
        """
        UPDATE models
        SET visible_to_subjects = FALSE,
            scope_actors = $2::uuid[]
        WHERE id = $1
        """,
        model_id,
        [actor_id] if actor_id is not None else [],
    )


@pytest.mark.asyncio
async def test_build_history_filters_hidden_prediction_models(
    tx_conn,
    tenant,
    actor_id,
    make_prediction_model,
) -> None:
    visible = await make_prediction_model(
        "visible prediction",
        created_at_offset_days=1,
    )
    hidden = await make_prediction_model(
        "hidden prediction",
        created_at_offset_days=1,
    )
    await _make_private_for_actor(tx_conn, visible, actor_id)
    await _make_private_for_actor(tx_conn, hidden)

    payload = await build_history(
        tenant_id=tenant,
        actor_id=actor_id,
        period="30d",
        conn=tx_conn,
    )

    assert [p["prediction_text"] for p in payload.predictions] == [
        "visible prediction"
    ]
    assert {event["links"][0]["id"] for event in payload.events} == {
        str(visible)
    }


@pytest.mark.asyncio
async def test_build_history_records_admin_override_for_private_models(
    tx_conn,
    tenant,
    actor_id,
    make_prediction_model,
) -> None:
    hidden = await make_prediction_model(
        "admin-visible private prediction",
        created_at_offset_days=1,
    )
    await _make_private_for_actor(tx_conn, hidden)
    await grant_role(
        actor_id,
        "tenant",
        None,
        "admin",
        actor_id,
        conn=tx_conn,
        tenant_id=tenant,
    )

    payload = await build_history(
        tenant_id=tenant,
        actor_id=actor_id,
        period="30d",
        conn=tx_conn,
    )

    assert any(
        p["prediction_text"] == "admin-visible private prediction"
        for p in payload.predictions
    )
    row = await tx_conn.fetchrow(
        """
        SELECT override_kind, reason
        FROM access_override_log
        WHERE tenant_id = $1
          AND actor_id = $2
          AND entity_type = 'model'
          AND entity_id = $3
        ORDER BY occurred_at DESC
        LIMIT 1
        """,
        tenant,
        actor_id,
        hidden,
    )
    assert row is not None
    assert row["override_kind"] == "admin"
    assert row["reason"] == "admin_override"


@pytest.mark.asyncio
async def test_build_summary_counts_only_visible_prediction_models(
    tx_conn,
    tenant,
    actor_id,
    make_prediction_model,
) -> None:
    visible = await make_prediction_model(
        "visible summary prediction",
        created_at_offset_days=1,
    )
    hidden = await make_prediction_model(
        "hidden summary prediction",
        created_at_offset_days=1,
    )
    await _make_private_for_actor(tx_conn, visible, actor_id)
    await _make_private_for_actor(tx_conn, hidden)

    summary = await build_summary(
        tenant_id=tenant,
        actor_id=actor_id,
        range_days=30,
        conn=tx_conn,
    )

    assert summary["predictions_made"] == {
        "value": 1,
        "split": "0 resolved \u00b7 1 active",
    }
    assert summary["model_updates"]["value"] == 1
