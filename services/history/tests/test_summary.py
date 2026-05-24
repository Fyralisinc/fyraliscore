"""services/history/tests/test_summary.py — Ledger summary counters.

Covers shape + key behaviors:
  - Returns the six counters with the expected keys.
  - WoW delta math is correct for non-zero windows.
  - Predictions counters tolerate the `predictions` table being
    absent (Phase 4 migration 0041 may not have run yet).
"""
from __future__ import annotations

import pytest

from services.history.summary import (
    _counter,
    _counter_pp,
    _counter_split,
    _fmt_pct,
    _pct_change,
    build_summary,
)


# ---------------------------------------------------------------------
# Pure helpers — unit tests, no DB.
# ---------------------------------------------------------------------


def test_pct_change_zero_previous_returns_zero():
    assert _pct_change(10, 0) == 0.0


def test_pct_change_increase():
    assert _pct_change(15, 10) == pytest.approx(0.5)


def test_pct_change_decrease():
    assert _pct_change(5, 10) == pytest.approx(-0.5)


def test_fmt_pct_positive_includes_plus():
    assert _fmt_pct(0.18).startswith("+")
    assert "%" in _fmt_pct(0.18)


def test_fmt_pct_negative_no_plus():
    assert not _fmt_pct(-0.1).startswith("+")


def test_counter_shape():
    c = _counter(10, 0.2, "+20%")
    assert c == {"value": 10, "delta_pct": 0.2, "delta_label": "+20%"}


def test_counter_split_shape():
    c = _counter_split(28, "7 resolved · 21 active")
    assert c["value"] == 28
    assert "resolved" in c["split"]


def test_counter_pp_shape():
    c = _counter_pp(0.71, 0.06, "+6pp last 30 days")
    assert c["value"] == pytest.approx(0.71)
    assert c["delta_pp"] == pytest.approx(0.06)


# ---------------------------------------------------------------------
# Integration — runs against a real Postgres but the predictions table
# may legitimately be missing, which the endpoint must tolerate.
# ---------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_summary_returns_all_six_counters_for_empty_tenant(
    tx_conn, tenant,
):
    """Fresh tenant → all six counters present, all zero values."""
    payload = await build_summary(
        tenant_id=tenant, range_days=30, conn=tx_conn,
    )
    for key in (
        "events", "model_updates", "predictions_made",
        "predictions_accuracy", "actions_taken", "contestations",
    ):
        assert key in payload, f"missing counter: {key}"
    assert payload["events"]["value"] == 0
    assert payload["model_updates"]["value"] == 0
    assert payload["actions_taken"]["value"] == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_summary_counts_state_change_actions(
    tx_conn, tenant, insert_state_change,
):
    """A commitment doneverified observation lands in actions_taken."""
    await insert_state_change(
        entity_kind="commitment",
        new_state="doneverified",
        occurred_offset_days=2,
    )
    payload = await build_summary(
        tenant_id=tenant, range_days=30, conn=tx_conn,
    )
    assert payload["actions_taken"]["value"] >= 1
    # And it should also show up in raw events (state_change observations).
    assert payload["events"]["value"] >= 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_summary_predictions_resilient_when_table_missing(
    tx_conn, tenant,
):
    """Spec: predictions counters must return zeros (not raise) when
    the predictions table is absent.

    We simulate absence by querying inside a SAVEPOINT and dropping
    the table if it exists. The outer test transaction rolls back at
    teardown so this is non-destructive.
    """
    # If the table exists, drop it inside a savepoint so the rest of
    # this test sees it as missing.
    await tx_conn.execute("SAVEPOINT no_predictions")
    try:
        await tx_conn.execute("DROP TABLE IF EXISTS predictions CASCADE")
        payload = await build_summary(
            tenant_id=tenant, range_days=30, conn=tx_conn,
        )
        assert payload["predictions_made"]["value"] == 0
        assert payload["predictions_accuracy"]["value"] == 0.0
    finally:
        await tx_conn.execute("ROLLBACK TO SAVEPOINT no_predictions")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_summary_range_days_clamped(
    tx_conn, tenant,
):
    """range_days <= 0 is clamped to 30 (the default)."""
    payload = await build_summary(
        tenant_id=tenant, range_days=0, conn=tx_conn,
    )
    assert payload["range_days"] == 30
