"""Tests for services/ingest/ingestion/planners/google_calendar.py (IN-15)."""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from services.ingest.source_contract.runtime import resolve_planner
from services.ingest.ingestion.planners.context import PlannerContext
from services.ingest.ingestion.planners.google_calendar import (
    SHARD_KIND_EVENTS,
    plan_shards_google_calendar,
)


pytestmark = pytest.mark.asyncio


class _FakeRecord:
    def __init__(self, data):
        self._d = data

    def __getitem__(self, k):
        return self._d[k]

    def __contains__(self, k):
        return k in self._d


def _ctx(calendars):
    install = _FakeRecord({
        "id": uuid4(),
        "calendars": json.dumps(calendars),
    })
    return PlannerContext(
        tenant_id=uuid4(), install=install, conn=None, source_client=None,
    )


async def test_one_shard_per_calendar():
    ctx = _ctx([
        {"calendar_id": "alice@acme.com", "owner_email": "alice@acme.com", "sync_token": None},
        {"calendar_id": "bob@acme.com", "owner_email": "bob@acme.com", "sync_token": "tok"},
    ])
    shards = await plan_shards_google_calendar(ctx)
    assert len(shards) == 2
    assert all(s.shard_kind == SHARD_KIND_EVENTS for s in shards)
    by_cal = {s.shard_identifier["calendar_id"]: s for s in shards}
    assert by_cal["alice@acme.com"].shard_identifier["owner_email"] == "alice@acme.com"
    # a warm sync_token from the registry is threaded into the shard.
    assert by_cal["bob@acme.com"].shard_identifier["sync_token"] == "tok"


async def test_empty_calendars_yields_no_shards():
    shards = await plan_shards_google_calendar(_ctx([]))
    assert shards == []


async def test_calendars_as_native_list():
    """The loader may hand a decoded list rather than a JSON string."""
    install = _FakeRecord({
        "id": uuid4(),
        "calendars": [{"calendar_id": "c@acme.com", "owner_email": "c@acme.com"}],
    })
    ctx = PlannerContext(
        tenant_id=uuid4(), install=install, conn=None, source_client=None,
    )
    shards = await plan_shards_google_calendar(ctx)
    assert [s.shard_identifier["calendar_id"] for s in shards] == ["c@acme.com"]


async def test_dispatch_wired():
    assert resolve_planner("google_calendar") is plan_shards_google_calendar
