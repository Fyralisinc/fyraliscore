"""services/ingest/synthetic/mock_servers/google_calendar.py — local Calendar mock.

A real, threaded HTTP server that mimics the two endpoints the Google Calendar
DWD ingestion path touches, so a sandbox can drive the REAL minter + httpx
client + fetcher against it with no Google credentials:

  POST /token
      The DWD JWT->access-token exchange. Returns a canned bearer token
      regardless of the (correctly RS256-signed) assertion. The fake
      service-account JSON's `token_uri` points here.

  GET /calendars/{calendarId}/events
      Calendar v3 events.list, with the three modes the fetcher/reconciler use:
        - full sync   (timeMin, no syncToken)  -> all events + nextSyncToken
        - incremental (syncToken)              -> the calendar's delta events
                                                  (new + cancelled) + a new
                                                  nextSyncToken
        - probe       (updatedMin, maxResults) -> events updated since the bound
                                                  (the reconciler gap probe)

Fixtures are a dict: {calendar_id: {"events": [...], "delta": [...]}}. Events
are raw Calendar v3 event objects (the same shape the real API returns); the
mock does not synthesize them so the sandbox controls exactly what lands.

Usage:
    server, base_url, token_url = start_mock_calendar(fixtures)
    try:
        ...  # base_url -> GOOGLE_CALENDAR_API_BASE_URL ; token_url -> SA token_uri
    finally:
        server.shutdown()
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse


CalendarFixtures = dict[str, dict[str, list[dict[str, Any]]]]


def _make_handler(fixtures: CalendarFixtures, hits: dict[str, int]):
    class _Handler(BaseHTTPRequestHandler):
        # Silence the default stderr request logging.
        def log_message(self, *args: Any) -> None:  # noqa: D401
            return

        def _json(self, status: int, body: dict[str, Any]) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.rstrip("/").endswith("/token") or parsed.path == "/token":
                # Drain the body (the signed JWT assertion); we don't verify it.
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length:
                    self.rfile.read(length)
                hits["token"] = hits.get("token", 0) + 1
                self._json(200, {
                    "access_token": "sandbox-access-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                })
                return
            self._json(404, {"error": {"message": f"no POST route {parsed.path}"}})

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            # Path: /calendars/{calendarId}/events  (calendarId may be url-encoded).
            parts = [p for p in parsed.path.split("/") if p]
            if len(parts) >= 3 and parts[-3] == "calendars" and parts[-1] == "events":
                calendar_id = unquote(parts[-2])
                self._handle_events(calendar_id, params)
                return
            self._json(404, {"error": {"message": f"no GET route {parsed.path}"}})

        def _handle_events(self, calendar_id: str, params: dict[str, str]) -> None:
            hits[f"events:{calendar_id}"] = hits.get(f"events:{calendar_id}", 0) + 1
            fx = fixtures.get(calendar_id)
            if fx is None:
                # Unknown calendar: empty page, terminal sync.
                self._json(200, {"items": [], "nextSyncToken": "sync-empty"})
                return

            events = list(fx.get("events", []))
            delta = list(fx.get("delta", []))

            if "syncToken" in params:
                # Incremental: return the delta (new + cancelled), terminal.
                token = params["syncToken"]
                if token == "EXPIRED":
                    # Demonstrate the 410 GONE -> full-resync fallback.
                    self._json(410, {"error": {
                        "code": 410, "message": "Sync token is no longer valid.",
                    }})
                    return
                self._json(200, {"items": delta, "nextSyncToken": "sync-2"})
                return

            if "updatedMin" in params:
                # Reconciler probe: events updated since the bound.
                bound = params["updatedMin"]
                hit = [e for e in (events + delta) if str(e.get("updated", "")) > bound]
                max_results = int(params.get("maxResults", "250"))
                self._json(200, {"items": hit[:max_results]})
                return

            # Full windowed sync: all events for the calendar, terminal, with a
            # syncToken the next (incremental) run warm-starts from.
            self._json(200, {"items": events, "nextSyncToken": "sync-1"})

    return _Handler


def start_mock_calendar(
    fixtures: CalendarFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str, str]:
    """Start the mock on a background daemon thread.

    Returns `(server, base_url, token_url)`:
      - base_url  -> set as GOOGLE_CALENDAR_API_BASE_URL (the client appends
        `/calendars/{id}/events`).
      - token_url -> put in the fake service-account JSON's `token_uri`.
    Call `server.shutdown()` to stop.
    """
    hits: dict[str, int] = {}
    handler = _make_handler(fixtures, hits)
    server = ThreadingHTTPServer((host, port), handler)
    server.request_hits = hits  # type: ignore[attr-defined]  # for sandbox assertions
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://{bound_host}:{bound_port}"
    return server, base_url, f"{base_url}/token"


__all__ = ["CalendarFixtures", "start_mock_calendar"]
