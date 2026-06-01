"""End-to-end Google Calendar pipeline (IN-15): planner -> fetcher -> channel
route -> handler -> ObservationDraft. Exercises the full normalization
contract (minus Kafka/S3), proving a workspace install yields well-formed
observations for events with stable external_ids and correct trust/kind.
"""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from services.ingest.ingestion.fetchers import google_calendar as gc
from services.ingest.ingestion.fetchers.google_calendar import fetch_page_google_calendar
from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.ingestion.planners.context import PlannerContext
from services.ingest.ingestion.planners.google_calendar import plan_shards_google_calendar


pytestmark = pytest.mark.asyncio


class _Inst:
    def __init__(self, calendars):
        self._d = {"id": uuid4(), "scope": "calendar.readonly",
                   "calendars": json.dumps(calendars)}

    def __getitem__(self, k):
        return self._d[k]

    def __contains__(self, k):
        return k in self._d


class _FakeCalClient:
    """Two calendars; each returns one confirmed event + one cancelled event,
    terminal in a single page with a nextSyncToken."""

    async def list_events(self, *, calendar_id, user_email, **kw):
        return {
            "items": [
                {
                    "kind": "calendar#event", "id": f"{calendar_id}-evt1",
                    "status": "confirmed", "summary": "Standup",
                    "start": {"dateTime": "2026-04-22T14:00:00Z"},
                    "end": {"dateTime": "2026-04-22T14:15:00Z"},
                    "organizer": {"email": user_email},
                    "attendees": [{"email": user_email}, {"email": "ext@vc.com"}],
                    "updated": "2026-04-20T10:00:00.000Z",
                },
                {
                    "kind": "calendar#event", "id": f"{calendar_id}-evt2",
                    "status": "cancelled",
                    "updated": "2026-04-21T10:00:00.000Z",
                },
            ],
            "nextSyncToken": "tok-1",
        }


async def test_workspace_install_yields_observations(monkeypatch):
    client = _FakeCalClient()

    async def fake_open(install):
        async def close(): return None
        return client, close
    monkeypatch.setattr(gc, "_open_calendar_client", fake_open)

    inst = _Inst([
        {"calendar_id": "alice@acme.com", "owner_email": "alice@acme.com", "sync_token": None},
        {"calendar_id": "bob@acme.com", "owner_email": "bob@acme.com", "sync_token": None},
    ])

    # 1. Plan shards from the workspace install.
    ctx = PlannerContext(tenant_id=uuid4(), install=inst, conn=None, source_client=None)
    shards = await plan_shards_google_calendar(ctx)
    assert len(shards) == 2

    # 2. Drain each shard's fetcher; 3. route + handle each record.
    handler = get_handler(resolve_channel("google_calendar", "backfill"))
    drafts = []
    for shard in shards:
        cursor, guard = None, 0
        while True:
            guard += 1
            assert guard < 50
            r = await fetch_page_google_calendar(inst, shard.shard_identifier, cursor)
            for record in r.records:
                drafts.append(await handler(record, {}))
            cursor = r.next_cursor
            if r.end_of_data:
                break

    # Two calendars x (1 confirmed + 1 cancelled) = 4 observations.
    assert len(drafts) == 4
    assert {d.source_channel for d in drafts} == {"google_calendar:event"}
    assert {d.trust_tier for d in drafts} == {"authoritative"}

    by_event = {d.content["event_id"]: d for d in drafts}
    assert by_event["alice@acme.com-evt1"].kind == "signal"
    # the cancelled event is a state_change with a versioned external_id.
    assert by_event["bob@acme.com-evt2"].kind == "state_change"
    assert by_event["bob@acme.com-evt2"].external_id.endswith(":cancelled:none")
    # external attendee detected against the calendar owner's domain.
    confirmed = by_event["alice@acme.com-evt1"]
    ext = {e["id"]: e for e in confirmed.entities_hint if e["type"] == "email_address"}
    assert ext["ext@vc.com"]["external"] is True


async def test_backfill_and_poll_twins_dedup_identically(monkeypatch):
    """external_id parity: the same event via backfill and via the poll
    re-run derives the SAME (channel, external_id) so the dedup index
    collapses them."""
    assert resolve_channel("google_calendar", "backfill") == resolve_channel(
        "google_calendar", "poll",
    )
    client = _FakeCalClient()

    async def fake_open(install):
        async def close(): return None
        return client, close
    monkeypatch.setattr(gc, "_open_calendar_client", fake_open)

    inst = _Inst([])
    handler = get_handler("google_calendar:event")
    sid = {"shard_kind": "google_calendar_events",
           "calendar_id": "alice@acme.com", "owner_email": "alice@acme.com"}
    r = await fetch_page_google_calendar(inst, sid, None)
    event = r.records[0]
    d1 = await handler(event, {})
    d2 = await handler(event, {})
    assert (d1.source_channel, d1.external_id) == (d2.source_channel, d2.external_id)
