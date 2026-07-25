"""Tests for services/ingest/integrations/google_calendar/client.py (IN-15)."""
from __future__ import annotations

import pytest

from services.ingest.integrations.google_calendar.client import (
    CALENDAR_READONLY_SCOPE,
    GoogleCalendarClient,
    resolve_scope,
)


pytestmark = pytest.mark.asyncio


class _FakeHttp:
    """Records request(...) kwargs and returns canned bodies in sequence."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.requests: list[dict] = []

    async def request(
        self,
        method,
        url,
        *,
        user_email,
        scopes,
        params=None,
        json_body=None,
        operation_id,
    ):
        self.requests.append({
            "method": method, "url": url, "user_email": user_email,
            "scopes": tuple(scopes), "params": params or {},
            "json_body": json_body,
            "operation_id": operation_id,
        })
        return self._responses.pop(0)


def _client(responses):
    http = _FakeHttp(responses)
    return GoogleCalendarClient(http, base_url="https://cal.test/v3"), http


async def test_list_calendars_operation_id():
    client, http = _client([{"items": []}])
    await client.list_calendars(user_email="alice@acme.com")
    assert http.requests[0]["operation_id"] == "calendarList.list"


async def test_full_sync_request_shape():
    client, http = _client([{"items": [{"id": "e1"}], "nextSyncToken": "tok"}])
    body = await client.list_events(
        calendar_id="alice@acme.com", user_email="alice@acme.com",
        time_min="2026-01-01T00:00:00Z", order_by="startTime",
    )
    assert body["nextSyncToken"] == "tok"
    req = http.requests[0]
    assert req["url"] == "https://cal.test/v3/calendars/alice@acme.com/events"
    assert req["scopes"] == (CALENDAR_READONLY_SCOPE,)
    assert req["params"]["timeMin"] == "2026-01-01T00:00:00Z"
    assert req["params"]["orderBy"] == "startTime"
    assert req["params"]["singleEvents"] == "true"
    assert req["operation_id"] == "events.list"
    # full sync never sends a syncToken.
    assert "syncToken" not in req["params"]


async def test_incremental_request_shape_excludes_time_min():
    client, http = _client([{"items": [], "nextSyncToken": "tok2"}])
    await client.list_events(
        calendar_id="primary", user_email="alice@acme.com",
        sync_token="tok1", show_deleted=True,
    )
    req = http.requests[0]
    assert req["params"]["syncToken"] == "tok1"
    assert req["params"]["showDeleted"] == "true"
    # syncToken mode must NOT carry timeMin / orderBy (Google rejects it).
    assert "timeMin" not in req["params"]
    assert "orderBy" not in req["params"]


async def test_has_updates_since_true_when_items_returned():
    client, _ = _client([{"items": [{"id": "e9"}]}])
    assert await client.has_updates_since(
        calendar_id="alice@acme.com", user_email="alice@acme.com",
        updated_min="2026-04-20T10:00:00Z",
    ) is True


async def test_has_updates_since_false_when_empty():
    client, http = _client([{"items": []}])
    assert await client.has_updates_since(
        calendar_id="alice@acme.com", user_email="alice@acme.com",
        updated_min="2026-04-20T10:00:00Z",
    ) is False
    # the probe is a cheap 1-row updatedMin query.
    assert http.requests[0]["params"]["updatedMin"] == "2026-04-20T10:00:00Z"
    assert http.requests[0]["params"]["maxResults"] == 1


async def test_resolve_scope():
    assert resolve_scope("calendar.readonly") == CALENDAR_READONLY_SCOPE
    with pytest.raises(ValueError):
        resolve_scope("calendar.write")


async def test_watch_events_request_shape():
    client, http = _client([{"id": "ch1", "resourceId": "res1", "expiration": "123"}])
    body = await client.watch_events(
        calendar_id="alice@acme.com", user_email="alice@acme.com",
        channel_id="ch1", address="https://app.test/webhooks/google_calendar/push",
        token="secret-tok", ttl_seconds=604800,
    )
    assert body["resourceId"] == "res1"
    req = http.requests[0]
    assert req["method"] == "POST"
    assert req["url"] == "https://cal.test/v3/calendars/alice@acme.com/events/watch"
    jb = req["json_body"]
    assert jb["id"] == "ch1"
    assert jb["type"] == "web_hook"
    assert jb["address"] == "https://app.test/webhooks/google_calendar/push"
    assert jb["token"] == "secret-tok"
    assert jb["params"]["ttl"] == "604800"
    assert req["operation_id"] == "events.watch"


async def test_stop_channel_request_shape():
    client, http = _client([{}])
    await client.stop_channel(
        user_email="alice@acme.com", channel_id="ch1", resource_id="res1",
    )
    req = http.requests[0]
    assert req["method"] == "POST"
    assert req["url"] == "https://cal.test/v3/channels/stop"
    assert req["json_body"] == {"id": "ch1", "resourceId": "res1"}
    assert req["operation_id"] == "channels.stop"
