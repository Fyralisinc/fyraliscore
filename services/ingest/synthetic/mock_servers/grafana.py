"""services/ingest/synthetic/mock_servers/grafana.py — local Grafana HTTP API mock (IN-GRAFANA).

A real, threaded HTTP server that mimics the Grafana endpoints the ingestion path
touches, so a sandbox can drive the REAL GrafanaClient + fetcher + reconciler
against it with no Grafana instance:

  GET /api/annotations?from=&to=&limit=
      Annotations in the [from, to] window (epoch MILLISECONDS), newest-first,
      capped at `limit`. Returns a bare JSON ARRAY (Grafana's shape). Both the
      backfill walk and the reconciler's 1-row gap probe use this.

  GET /api/org
      Org/connectivity + credential probe.

Fixtures: a flat list of annotation objects (id, time, text, tags, alertId,
newState, prevState, userId, userName, dashboardUID, panelId, timeEnd). The mock
does not synthesize them so the sandbox controls exactly what lands. The list is
mutable — append to it between calls to simulate an incremental delta.

The client may be pointed here either via `GRAFANA_API_BASE_URL=<base>` or via the
spammer single-host base (`SYNTHETIC_SOURCE_API_BASE=<base>` -> `<base>/grafana`);
the handler matches on the `/api/...` path SUFFIX so either prefix works.

Usage:
    server, base_url = start_mock_grafana(fixtures)
    try:
        ...  # base_url -> GRAFANA_API_BASE_URL (or SYNTHETIC_SOURCE_API_BASE)
    finally:
        server.shutdown()
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


GrafanaFixtures = list[dict[str, Any]]


def _make_handler(fixtures: GrafanaFixtures, hits: dict[str, int]):
    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:  # noqa: D401
            return

        def _json(self, status: int, body: Any) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            if path.endswith("/api/annotations"):
                params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
                self._handle_annotations(params)
                return
            if path.endswith("/api/org"):
                hits["org"] = hits.get("org", 0) + 1
                self._json(200, {"id": 1, "name": "Sandbox Org"})
                return
            self._json(404, {"message": f"no GET route {path}"})

        def _handle_annotations(self, params: dict[str, str]) -> None:
            hits["annotations"] = hits.get("annotations", 0) + 1

            def _int(name: str) -> int | None:
                v = params.get(name)
                if v is None or v == "":
                    return None
                try:
                    return int(v)
                except ValueError:
                    return None

            from_ms = _int("from")
            to_ms = _int("to")
            limit = _int("limit") or 100

            rows = [
                a for a in fixtures
                if (from_ms is None or int(a.get("time", 0)) >= from_ms)
                and (to_ms is None or int(a.get("time", 0)) <= to_ms)
            ]
            # Grafana returns annotations newest-first.
            rows.sort(key=lambda a: int(a.get("time", 0)), reverse=True)
            self._json(200, rows[:limit])

    return _Handler


def start_mock_grafana(
    fixtures: GrafanaFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str]:
    """Start the mock on a background daemon thread.

    Returns `(server, base_url)`. Point the client at `base_url` via
    `GRAFANA_API_BASE_URL` (direct) or `SYNTHETIC_SOURCE_API_BASE` (spammer mode,
    served under `/grafana`). Call `server.shutdown()` to stop. The `fixtures`
    list is held by reference, so appending to it between calls is visible to
    subsequent requests (used to simulate an incremental delta)."""
    hits: dict[str, int] = {}
    handler = _make_handler(fixtures, hits)
    server = ThreadingHTTPServer((host, port), handler)
    server.request_hits = hits  # type: ignore[attr-defined]
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://{bound_host}:{bound_port}"


__all__ = ["GrafanaFixtures", "start_mock_grafana"]
