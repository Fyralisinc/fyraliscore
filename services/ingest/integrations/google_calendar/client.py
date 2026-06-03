"""services/ingest/integrations/google_calendar/client.py — outbound Calendar client.

A thin wrapper over the SHARED `GoogleHttpClient` (services/ingest/integrations/
gmail/client.py), which owns DWD token minting, the `Authorization: Bearer`
header, 401-retry, and 429/403-quota -> `GoogleRateLimited` mapping. This
class adds only the Calendar v3 request shapes.

Auth model (D1): the service account impersonates the calendar's OWNER
(`owner_email`) and addresses the calendar by `calendar_id` (a user's
primary calendar is addressed by their email). Every call therefore takes
both — `user_email` is who we impersonate, `calendar_id` is what we read.

Incremental sync (D2): `events.list` returns a `nextSyncToken` on the last
page of a full sync. Passing it on the next run returns only deltas. Google
rejects `syncToken` combined with `timeMin`/`orderBy`, so the two modes are
mutually exclusive in `list_events`. An expired token yields HTTP 410, which
the fetcher catches to reseed a full sync.

Base URL is resolved via `lib.integrations.endpoints.endpoint("google_calendar_api")`
so backfill can be pointed at a local spammer for tests — pure config.
"""
from __future__ import annotations

from typing import Any

from services.ingest.integrations.gmail.client import GoogleHttpClient


CALENDAR_READONLY_SCOPE = "https://www.googleapis.com/auth/calendar.readonly"

# Scope alias stored on google_calendar_installations.scope -> long URL.
_SCOPE_ALIAS = {
    "calendar.readonly": CALENDAR_READONLY_SCOPE,
}

# events.list page size. Calendar caps at 2500; keep conservative so a
# single page stays bounded under the per-fetch wall budget.
_DEFAULT_PAGE_SIZE = 250


def resolve_scope(alias: str) -> str:
    """Map an install scope alias to the long Calendar scope URL."""
    long_scope = _SCOPE_ALIAS.get(alias)
    if long_scope is None:
        raise ValueError(
            f"google_calendar install carries unknown scope alias: {alias!r}",
        )
    return long_scope


class GoogleCalendarClient:
    """Operations against the Calendar v3 REST API."""

    def __init__(
        self,
        http: GoogleHttpClient,
        *,
        scope: str = CALENDAR_READONLY_SCOPE,
        base_url: str | None = None,
    ) -> None:
        from lib.integrations.endpoints import endpoint
        self._http = http
        self._scope = scope
        self._base = (base_url or endpoint("google_calendar_api")).rstrip("/")

    async def list_calendars(
        self, *, user_email: str, page_token: str | None = None,
    ) -> dict[str, Any]:
        """`GET /users/me/calendarList` impersonating `user_email`. Returns
        the raw body (`items`, `nextPageToken`). Used by onboarding to
        confirm access; v1 shards on the primary calendar (= the email)."""
        params: dict[str, Any] = {"maxResults": 250}
        if page_token:
            params["pageToken"] = page_token
        return await self._http.request(
            "GET",
            f"{self._base}/users/me/calendarList",
            user_email=user_email,
            scopes=(self._scope,),
            params=params,
        )

    async def list_events(
        self,
        *,
        calendar_id: str,
        user_email: str,
        page_token: str | None = None,
        sync_token: str | None = None,
        time_min: str | None = None,
        updated_min: str | None = None,
        max_results: int = _DEFAULT_PAGE_SIZE,
        show_deleted: bool = False,
        single_events: bool = True,
        order_by: str | None = None,
    ) -> dict[str, Any]:
        """`GET /calendars/{calendarId}/events`.

        Two mutually-exclusive modes:
          - FULL sync: pass `time_min` (+ optional `order_by="startTime"`).
          - INCREMENTAL: pass `sync_token` (no time_min / order_by allowed).

        Returns the raw API body: `{items, nextPageToken, nextSyncToken}`.
        On an expired sync token Google returns HTTP 410 (surfaced as
        `GoogleApiError(status=410)` by the shared client).
        """
        from urllib.parse import quote

        params: dict[str, Any] = {
            "maxResults": max_results,
            "singleEvents": "true" if single_events else "false",
        }
        if show_deleted:
            params["showDeleted"] = "true"
        if sync_token is not None:
            params["syncToken"] = sync_token
        else:
            # Full / windowed sync — time bounds + ordering are only valid
            # without a syncToken.
            if time_min is not None:
                params["timeMin"] = time_min
            if updated_min is not None:
                params["updatedMin"] = updated_min
            if order_by is not None:
                params["orderBy"] = order_by
        if page_token is not None:
            params["pageToken"] = page_token

        return await self._http.request(
            "GET",
            f"{self._base}/calendars/{quote(calendar_id, safe='@')}/events",
            user_email=user_email,
            scopes=(self._scope,),
            params=params,
        )

    async def has_updates_since(
        self, *, calendar_id: str, user_email: str, updated_min: str,
    ) -> bool:
        """Reconciler gap probe (D2-adjacent): one cheap 1-row query asking
        "did anything change on this calendar since `updated_min`?". Uses
        `updatedMin` + `maxResults=1` + `showDeleted` so a cancellation also
        counts as a change."""
        body = await self.list_events(
            calendar_id=calendar_id,
            user_email=user_email,
            updated_min=updated_min,
            max_results=1,
            show_deleted=True,
        )
        items = body.get("items")
        return isinstance(items, list) and len(items) > 0

    async def watch_events(
        self,
        *,
        calendar_id: str,
        user_email: str,
        channel_id: str,
        address: str,
        token: str,
        ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        """`POST /calendars/{calendarId}/events/watch` — open a push channel.

        Calendar pushes directly to a `web_hook` `address` (no Pub/Sub). The
        notification is a content-less ping carrying `X-Goog-*` headers; the
        receiver verifies `X-Goog-Channel-Token == token` and drains the delta
        via `syncToken`. Returns the raw channel resource
        (`{id, resourceId, resourceUri, expiration}`)."""
        from urllib.parse import quote

        body: dict[str, Any] = {
            "id": channel_id,
            "type": "web_hook",
            "address": address,
            "token": token,
        }
        if ttl_seconds:
            body["params"] = {"ttl": str(ttl_seconds)}
        return await self._http.request(
            "POST",
            f"{self._base}/calendars/{quote(calendar_id, safe='@')}/events/watch",
            user_email=user_email,
            scopes=(self._scope,),
            json_body=body,
        )

    async def stop_channel(
        self, *, user_email: str, channel_id: str, resource_id: str,
    ) -> None:
        """`POST /channels/stop` — tear down a push channel (idempotent)."""
        await self._http.request(
            "POST",
            f"{self._base}/channels/stop",
            user_email=user_email,
            scopes=(self._scope,),
            json_body={"id": channel_id, "resourceId": resource_id},
        )


__all__ = [
    "CALENDAR_READONLY_SCOPE",
    "GoogleCalendarClient",
    "resolve_scope",
]
