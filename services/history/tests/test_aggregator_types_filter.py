"""services/history/tests/test_aggregator_types_filter.py —
canonical-types filter on build_history.
"""
from __future__ import annotations

import pytest

from services.history import build_history
from services.history.aggregator import (
    CANONICAL_LEDGER_TYPES,
    _canonical_type,
)


def test_canonical_ledger_types_are_six():
    """Spec §6.1: exactly these six canonical types in the Ledger."""
    assert set(CANONICAL_LEDGER_TYPES) == {
        "action_taken",
        "model_update",
        "prediction_made",
        "prediction_resolved",
        "observation_ingested",
        "contestation",
    }


def test_canonical_type_mapping_default_to_model_update():
    """Unknown internal type defaults to model_update."""
    assert _canonical_type("unknown-event") == "model_update"
    assert _canonical_type("") == "model_update"


def test_canonical_type_mapping_prediction_made_and_resolved():
    assert _canonical_type("prediction-made") == "prediction_made"
    assert _canonical_type("prediction-resolved") == "prediction_resolved"


def test_canonical_type_mapping_actions_and_contestation():
    assert _canonical_type("commitment-completed") == "action_taken"
    assert _canonical_type("commitment-blocked") == "action_taken"
    assert _canonical_type("decision-contested") == "contestation"
    assert _canonical_type("decision-superseded") == "model_update"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_build_history_types_filter_keeps_predictions_made(
    tx_conn, tenant, make_prediction_model,
):
    """types=['prediction_made'] returns only prediction-made events."""
    await make_prediction_model("a prediction", created_at_offset_days=1)
    payload = await build_history(
        tenant_id=tenant, period="30d", conn=tx_conn,
        types=["prediction_made"],
    )
    assert payload.events  # at least the prediction-made event
    assert all(
        e["type"] == "prediction-made"
        for e in payload.events
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_build_history_types_filter_drops_unwanted(
    tx_conn, tenant, make_prediction_model, insert_state_change,
):
    """types=['action_taken'] drops prediction-made events."""
    await make_prediction_model("a prediction", created_at_offset_days=1)
    await insert_state_change(
        entity_kind="commitment",
        new_state="doneverified",
        occurred_offset_days=1,
    )
    payload = await build_history(
        tenant_id=tenant, period="30d", conn=tx_conn,
        types=["action_taken"],
    )
    # No prediction-made should survive the filter.
    assert all(
        e["type"] != "prediction-made" for e in payload.events
    )
    # All surviving events must canonicalize to action_taken.
    assert all(
        _canonical_type(e["type"]) == "action_taken"
        for e in payload.events
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_build_history_no_types_arg_unchanged(
    tx_conn, tenant, make_prediction_model,
):
    """Backwards compat: omitting `types` returns all events."""
    await make_prediction_model("p1", created_at_offset_days=1)
    payload = await build_history(
        tenant_id=tenant, period="30d", conn=tx_conn,
    )
    types_seen = {e["type"] for e in payload.events}
    # Must include prediction-made even though we filtered nothing.
    assert "prediction-made" in types_seen


@pytest.mark.integration
@pytest.mark.asyncio
async def test_build_history_unknown_types_are_dropped(
    tx_conn, tenant, make_prediction_model,
):
    """Unknown canonical names in the filter are silently dropped.
    When that leaves an empty filter, the filter is disabled (all
    events returned)."""
    await make_prediction_model("p1", created_at_offset_days=1)
    payload = await build_history(
        tenant_id=tenant, period="30d", conn=tx_conn,
        types=["bogus_value"],
    )
    # Filter collapsed to disabled, so we get the prediction-made back.
    assert any(e["type"] == "prediction-made" for e in payload.events)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_build_history_empty_types_list_is_disabled(
    tx_conn, tenant, make_prediction_model,
):
    """Passing types=[] disables the filter (same as None)."""
    await make_prediction_model("p1", created_at_offset_days=1)
    payload = await build_history(
        tenant_id=tenant, period="30d", conn=tx_conn,
        types=[],
    )
    assert any(e["type"] == "prediction-made" for e in payload.events)
