"""MockGoogleCalendarClient — Calendar v3 surface used by IN-15 backfill.

Implements only the methods the M6 google_calendar fetcher + reconciler call:
  - list_events(calendar_id, user_email, page_token, sync_token, time_min,
                updated_min, max_results, show_deleted, single_events, order_by)
  - has_updates_since(calendar_id, user_email, updated_min)

Stateful over a fixture produced by
`services.ingest.synthetic.fixtures.google_calendar_generator.make_google_calendar`.
Returns dicts with the literal Calendar API field names (`items`,
`nextPageToken`, `nextSyncToken`) so the REAL fetcher code path runs exactly
as it would against Google. Injected at the `_open_calendar_client` seam by
the X3 harness helper (so the DWD token mint + httpx layer are bypassed in
mock mode — the harness tests the M6 chain, not Google's transport).
"""
from __future__ import annotations

from typing import Any, NoReturn

from services.ingest.integrations.gmail.client import GoogleApiError, GoogleRateLimited
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockGoogleCalendarClient(_MockBase):
    """Stateful in-process replacement for `GoogleCalendarClient`."""

    def __init__(
        self,
        *,
        fixture: dict[str, Any],
        profile: FaultProfile = HAPPY_PATH,
    ) -> None:
        super().__init__(profile=profile)
        self._fixture = fixture
        self._page_size = int(fixture.get("page_size", 250))

    def _events(self, calendar_id: str) -> list[dict[str, Any]]:
        return list(self._fixture.get("events", {}).get(calendar_id, []))

    def _delta(self, calendar_id: str) -> list[dict[str, Any]]:
        return list(self._fixture.get("delta", {}).get(calendar_id, []))

    async def list_events(
        self,
        *,
        calendar_id: str,
        user_email: str,
        page_token: str | None = None,
        sync_token: str | None = None,
        time_min: str | None = None,
        updated_min: str | None = None,
        max_results: int = 250,
        show_deleted: bool = False,
        single_events: bool = True,
        order_by: str | None = None,
    ) -> dict[str, Any]:
        self._check_fault()

        # Incremental: a syncToken returns the calendar's delta, terminal.
        if sync_token is not None:
            return {"items": self._delta(calendar_id), "nextSyncToken": "sync-2"}

        events = self._events(calendar_id)

        # Reconciler probe: events updated strictly after the bound.
        if updated_min is not None:
            hit = [e for e in events if str(e.get("updated", "")) > updated_min]
            return {"items": hit[:max_results]}

        # Full windowed sync: paginate by page_token (an integer offset),
        # terminal page carries the nextSyncToken for the warm incremental start.
        start = int(page_token) if page_token else 0
        end = start + self._page_size
        page = events[start:end]
        result: dict[str, Any] = {"items": page}
        if end < len(events):
            result["nextPageToken"] = str(end)
        else:
            result["nextSyncToken"] = "sync-1"
        return result

    async def has_updates_since(
        self, *, calendar_id: str, user_email: str, updated_min: str,
    ) -> bool:
        self._check_fault()
        return any(
            str(e.get("updated", "")) > updated_min
            for e in self._events(calendar_id) + self._delta(calendar_id)
        )

    # ---- Fault raisers (reuse the shared Google error types) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise GoogleRateLimited("MockGoogleCalendarClient: rate limit (X2 fault)")

    def _raise_5xx(self) -> NoReturn:
        raise GoogleApiError("MockGoogleCalendarClient: 503 (X2 fault)")

    def _raise_auth_error(self) -> NoReturn:
        raise GoogleApiError("MockGoogleCalendarClient: 401 (X2 fault)")

    def _raise_transient(self) -> NoReturn:
        raise GoogleApiError("MockGoogleCalendarClient: transient (X2 fault)")
