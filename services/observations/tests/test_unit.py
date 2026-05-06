"""Unit tests for services/observations — pure-Python helpers.

No DB required. Cover:
- partitions.compute_partitions — year boundary, month math, spec count.
- partitions.partition_name — canonical YYYY_MM format.
- events.NewObservationEvent — JSON payload shape.
- events.notify_scope — buffering, exception-safe discard, no-scope
  no-op for schedule_notify.
"""
from __future__ import annotations

import json
from datetime import date

from lib.shared.ids import uuid7
from services.observations import events, partitions


# =====================================================================
# partitions
# =====================================================================

def test_compute_partitions_default_count():
    specs = partitions.compute_partitions(as_of=date(2026, 2, 15))
    assert len(specs) == 4  # current month + next 3


def test_compute_partitions_respects_months_ahead_zero():
    specs = partitions.compute_partitions(as_of=date(2026, 7, 1), months_ahead=0)
    assert len(specs) == 1
    assert specs[0].month_start == date(2026, 7, 1)
    assert specs[0].month_end == date(2026, 8, 1)


def test_compute_partitions_crosses_year_boundary():
    specs = partitions.compute_partitions(
        as_of=date(2026, 11, 3), months_ahead=3,
    )
    names = [s.name for s in specs]
    assert names == [
        "observations_2026_11",
        "observations_2026_12",
        "observations_2027_01",
        "observations_2027_02",
    ]


def test_compute_partitions_negative_months_raises():
    import pytest
    with pytest.raises(ValueError):
        partitions.compute_partitions(months_ahead=-1)


def test_partition_name_format():
    assert partitions.partition_name("observations", date(2026, 4, 1)) == (
        "observations_2026_04"
    )
    assert partitions.partition_name("observations", date(2026, 12, 1)) == (
        "observations_2026_12"
    )


# =====================================================================
# events — payload + scope
# =====================================================================

def test_new_observation_event_payload_is_stable_json():
    oid = uuid7()
    tid = uuid7()
    e = events.NewObservationEvent(
        id=oid, kind="signal", tenant_id=tid, source_channel="slack:message",
    )
    payload = e.to_payload()
    decoded = json.loads(payload)
    # Keys are sorted.
    assert list(decoded.keys()) == ["id", "kind", "source_channel", "tenant_id"]
    assert decoded["id"] == str(oid)
    assert decoded["tenant_id"] == str(tid)
    assert decoded["kind"] == "signal"
    assert decoded["source_channel"] == "slack:message"


def test_schedule_notify_outside_scope_is_noop():
    # No scope active.
    e = events.NewObservationEvent(
        id=uuid7(), kind="signal", tenant_id=uuid7(),
        source_channel="slack:message",
    )
    assert events.schedule_notify(e) is False


def test_schedule_notify_inside_scope_buffers():
    e1 = events.NewObservationEvent(
        id=uuid7(), kind="signal", tenant_id=uuid7(), source_channel="c",
    )
    e2 = events.NewObservationEvent(
        id=uuid7(), kind="state_change", tenant_id=uuid7(),
        source_channel="internal:state_change",
    )
    with events.notify_scope() as scope:
        assert events.schedule_notify(e1) is True
        assert events.schedule_notify(e2) is True
    assert scope.events == [e1, e2]


def test_notify_scope_discards_on_exception():
    import pytest
    scope = events.notify_scope()
    e = events.NewObservationEvent(
        id=uuid7(), kind="signal", tenant_id=uuid7(), source_channel="c",
    )
    with pytest.raises(ValueError):
        with scope:
            events.schedule_notify(e)
            raise ValueError("abort")
    assert scope.events == []


def test_event_from_row_hydrates_uuids_from_strings():
    oid = uuid7()
    tid = uuid7()
    row = {
        "id": str(oid),
        "kind": "signal",
        "tenant_id": str(tid),
        "source_channel": "slack:message",
    }
    e = events.event_from_row(row)
    assert e.id == oid
    assert e.tenant_id == tid
    assert e.kind == "signal"
    assert e.source_channel == "slack:message"


def test_event_from_row_passes_through_uuids():
    oid = uuid7()
    tid = uuid7()
    row = {
        "id": oid,
        "kind": "state_change",
        "tenant_id": tid,
        "source_channel": "internal:state_change",
    }
    e = events.event_from_row(row)
    assert e.id == oid
    assert e.tenant_id == tid


def test_observations_channel_is_exported_and_stable():
    assert events.OBSERVATIONS_CHANNEL == "observations_new"
