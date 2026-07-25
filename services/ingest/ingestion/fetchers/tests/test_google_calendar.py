"""Tests for services/ingest/ingestion/fetchers/google_calendar.py (IN-15)."""
from __future__ import annotations

import pytest

from lib.shared.errors import CompanyOSError
from lib.shared.provider_transport import (
    RequestContext,
    RetryLater,
    RetryReason,
)
from services.ingest.ingestion.fetchers import google_calendar as gc
from services.ingest.ingestion.fetchers.google_calendar import (
    SHARD_KIND_EVENTS,
    GoogleCalendarCursor,
    fetch_page_google_calendar,
)
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.source_contract.runtime import resolve_fetcher


pytestmark = pytest.mark.asyncio


class _FakeInst:
    def __getitem__(self, k):
        return "calendar.readonly" if k == "scope" else "row"


def _ev(eid, updated="2026-04-20T10:00:00.000Z"):
    return {
        "kind": "calendar#event", "id": eid, "status": "confirmed",
        "summary": eid, "updated": updated,
        "start": {"dateTime": "2026-04-22T14:00:00Z"},
        "end": {"dateTime": "2026-04-22T15:00:00Z"},
        "organizer": {"email": "alice@acme.com"},
    }


class _FakeCalClient:
    """Deterministic fake. Drives a 2-page full sync ending in a syncToken,
    plus optional 429 / 410 injection."""

    def __init__(self, *, rate_limit_once=False, expired_sync=False):
        self.rate_limit_once = rate_limit_once
        self.expired_sync = expired_sync
        self.calls: list[dict] = []

    async def list_events(self, **kw):
        self.calls.append(kw)
        from services.ingest.integrations.gmail.client import GoogleRateLimited

        if self.rate_limit_once:
            self.rate_limit_once = False
            raise GoogleRateLimited("429", status=429)
        if self.expired_sync and kw.get("sync_token"):
            raise CompanyOSError("410 gone", status=410)
        # Incremental (syncToken) -> one delta page, terminal w/ new token.
        if kw.get("sync_token"):
            return {"items": [_ev("evt-delta")], "nextSyncToken": "tok-2"}
        # Full sync: page 1 -> page 2 -> terminal with nextSyncToken.
        if kw.get("page_token") is None:
            return {"items": [_ev("evt-1")], "nextPageToken": "pg-2"}
        return {"items": [_ev("evt-2")], "nextSyncToken": "tok-1"}


def _patch(monkeypatch, fake):
    async def fake_open(install):
        async def close(): return None
        return fake, close
    monkeypatch.setattr(gc, "_open_calendar_client", fake_open)


def _shard(**over):
    base = {
        "shard_kind": SHARD_KIND_EVENTS,
        "calendar_id": "alice@acme.com",
        "owner_email": "alice@acme.com",
        "installation_id": "inst-1",
    }
    base.update(over)
    return base


async def _drain(monkeypatch, fake, shard, cursor=None):
    _patch(monkeypatch, fake)
    records, guard = [], 0
    while True:
        guard += 1
        assert guard < 50, "fetch loop did not terminate"
        r = await fetch_page_google_calendar(_FakeInst(), shard, cursor)
        records.extend(r.records)
        cursor = r.next_cursor
        if r.end_of_data:
            break
    return records, cursor


async def test_full_sync_pages_then_captures_sync_token(monkeypatch):
    fake = _FakeCalClient()
    records, cursor = await _drain(monkeypatch, fake, _shard())
    ids = [r["id"] for r in records]
    assert ids == ["evt-1", "evt-2"]
    # every record carries the injected calendar/owner context.
    assert all(r["_fyralis_calendar_id"] == "alice@acme.com" for r in records)
    # nextSyncToken from the final page is stamped for the next run (D2).
    assert cursor["next_sync_token"] == "tok-1"
    # high-water captured for the reconciler.
    assert cursor["high_water_updated"] == "2026-04-20T10:00:00.000Z"
    # full sync used timeMin + orderBy, never a syncToken.
    assert fake.calls[0]["time_min"] is not None
    assert fake.calls[0]["order_by"] == "startTime"


async def test_incremental_mode_when_shard_warm_started(monkeypatch):
    fake = _FakeCalClient()
    records, cursor = await _drain(
        monkeypatch, fake, _shard(sync_token="tok-warm"),
    )
    assert [r["id"] for r in records] == ["evt-delta"]
    # incremental path passed the syncToken + showDeleted, no timeMin.
    assert fake.calls[0]["sync_token"] == "tok-warm"
    assert fake.calls[0]["show_deleted"] is True
    assert cursor["next_sync_token"] == "tok-2"


async def test_retry_later_propagates_without_returning_an_advanced_cursor(monkeypatch):
    class _Deferred:
        async def list_events(self, **kw):
            raise RetryLater.after(
                request_context=RequestContext(
                    source="google_calendar",
                    operation="events.list",
                    tenant_id="tenant-1",
                    installation_id="install-1",
                ),
                delay_seconds=60,
                reason=RetryReason.RATE_LIMIT,
            )

    cursor = GoogleCalendarCursor(
        seeded=True,
        sync_token="sync-0",
        page_token="page-before",
    ).model_dump(mode="json")
    original = dict(cursor)
    _patch(monkeypatch, _Deferred())
    with pytest.raises(RetryLater):
        await fetch_page_google_calendar(_FakeInst(), _shard(), cursor)
    assert cursor == original


async def test_expired_sync_token_reseeds_full_sync(monkeypatch):
    fake = _FakeCalClient(expired_sync=True)
    _patch(monkeypatch, fake)
    r = await fetch_page_google_calendar(
        _FakeInst(), _shard(sync_token="tok-old"), None,
    )
    # 410 -> empty page, cursor reset to a full windowed sync, not terminal.
    assert r.records == []
    assert r.end_of_data is False
    assert r.next_cursor["sync_token"] is None
    assert r.next_cursor["time_min"] is not None


async def test_cursor_strict():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        GoogleCalendarCursor.model_validate({"bogus": 1})


async def test_dispatch_and_routing_wired():
    assert resolve_fetcher("google_calendar") is fetch_page_google_calendar
    assert resolve_channel("google_calendar", "backfill") == "google_calendar:event"
    assert resolve_channel("google_calendar", "poll") == "google_calendar:event"
