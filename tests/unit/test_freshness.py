"""Unit tests for services.today.freshness.

Covers `truth_freshness_seconds` against synthetic focus dicts:
  * Model state_changed_at,
  * state_changes `at` field,
  * evidence `t` field,
  * Mix of all three (max wins),
  * Missing / malformed values,
  * Future-dated events (clamps to 0).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.today.freshness import truth_freshness_seconds


_NOW = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)


def _ago(seconds: int) -> datetime:
    return _NOW - timedelta(seconds=seconds)


# ---------------------------------------------------------------------
# Single source happy paths
# ---------------------------------------------------------------------


def test_freshness_from_model_state_changed_at():
    focus = {"model": {"state_changed_at": _ago(3600)}}
    assert truth_freshness_seconds(focus, _NOW) == 3600


def test_freshness_from_model_last_state_change_at():
    # Snapshot uses `last_state_change_at`; helper accepts both.
    focus = {"model": {"last_state_change_at": _ago(120)}}
    assert truth_freshness_seconds(focus, _NOW) == 120


def test_freshness_from_state_change_at():
    focus = {"state_changes": [{"at": _ago(60)}]}
    assert truth_freshness_seconds(focus, _NOW) == 60


def test_freshness_from_state_change_occurred_at():
    focus = {"state_changes": [{"occurred_at": _ago(45)}]}
    assert truth_freshness_seconds(focus, _NOW) == 45


def test_freshness_from_evidence_t():
    focus = {"evidence": [{"t": _ago(900)}]}
    assert truth_freshness_seconds(focus, _NOW) == 900


# ---------------------------------------------------------------------
# Maximum wins (most recent event)
# ---------------------------------------------------------------------


def test_freshness_picks_most_recent_across_sources():
    focus = {
        "model": {"state_changed_at": _ago(7200)},   # 2h ago
        "state_changes": [
            {"at": _ago(3600)},
            {"at": _ago(300)},                       # 5m ago - most recent
        ],
        "evidence": [{"t": _ago(1800)}],
    }
    # Most recent is 5m ago.
    assert truth_freshness_seconds(focus, _NOW) == 300


def test_freshness_iso_string_accepted():
    iso = _ago(120).isoformat()
    focus = {"model": {"state_changed_at": iso}}
    assert truth_freshness_seconds(focus, _NOW) == 120


def test_freshness_iso_string_with_z_suffix():
    iso = _ago(60).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    focus = {"evidence": [{"t": iso}]}
    assert truth_freshness_seconds(focus, _NOW) == 60


# ---------------------------------------------------------------------
# Missing / malformed
# ---------------------------------------------------------------------


def test_freshness_returns_none_when_no_events():
    assert truth_freshness_seconds({}, _NOW) is None
    assert truth_freshness_seconds({"unrelated": "data"}, _NOW) is None
    assert truth_freshness_seconds(None, _NOW) is None


def test_freshness_ignores_malformed_fields():
    focus = {
        "model": {"state_changed_at": "not-a-date"},
        "state_changes": [{"at": "garbage"}, "not-a-dict"],
        "evidence": [{"t": None}, {"t": ""}],
    }
    assert truth_freshness_seconds(focus, _NOW) is None


def test_freshness_handles_partial_mix():
    # One dict has a bad value, another has a good one — pick the good.
    focus = {
        "state_changes": [
            {"at": "bogus"},
            {"at": _ago(42)},
        ],
    }
    assert truth_freshness_seconds(focus, _NOW) == 42


# ---------------------------------------------------------------------
# Future-dated events clamp to 0
# ---------------------------------------------------------------------


def test_freshness_future_event_clamps_to_zero():
    focus = {"model": {"state_changed_at": _NOW + timedelta(hours=1)}}
    assert truth_freshness_seconds(focus, _NOW) == 0


# ---------------------------------------------------------------------
# Naive datetime treated as UTC
# ---------------------------------------------------------------------


def test_freshness_naive_datetime_treated_as_utc():
    naive = (_NOW - timedelta(seconds=30)).replace(tzinfo=None)
    focus = {"model": {"state_changed_at": naive}}
    assert truth_freshness_seconds(focus, _NOW) == 30
