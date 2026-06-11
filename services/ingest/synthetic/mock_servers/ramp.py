"""services/ingest/synthetic/mock_servers/ramp.py — local Ramp mock.

A real, threaded HTTP server that mimics the VERIFIED Ramp Developer API wire
contract (docs.ramp.com), so a sandbox can drive the REAL RampClient + fetcher
+ reconciler against it with no Ramp credentials:

  POST <prefix>/token
      OAuth client-credentials mint. Accepts ANY client creds (Basic header or
      none) and returns {"access_token", "token_type": "Bearer",
      "expires_in": 3600, "scope"}.
  GET <prefix>/transactions | /reimbursements | /cards | /users
      REST collections with the real KEYSET envelope:
      {"data": [...], "page": {"next": <absolute URL embedding
      start=<last entity id>, or null at EOF>}}. `page.next` URLs are built
      from the INCOMING request's Host header + path so the real client can
      follow them. Honours `page_size` (clamped 2..100), `start`, and the
      incremental window params `from_date` (transactions) / `updated_after`
      (reimbursements): when a window param is present the `delta` pool is
      served instead of `rows`.
  GET <prefix>/business
      Connectivity probe ({"id": <business_id>, "business_name_legal", ...}).

Fixtures: {resource: {"rows": [...], "delta": [...]}} where `resource` is the
plural URL segment ("transactions", "reimbursements", "cards", "users") and
each row is a real-shaped Ramp object (id, state, amount, …). `delta` is the
pool returned for incremental (window-param) queries.

Pointed at via `SYNTHETIC_SOURCE_API_BASE` (spammer mode, served under
`/ramp`); the handler matches the collection path SUFFIX so the prefix doesn't
matter.

Usage:
    server, base_url = start_mock_ramp(fixtures, business_id="bus-…")
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit


RampFixtures = dict[str, dict[str, list[dict[str, Any]]]]

_RESOURCES = ("transactions", "reimbursements", "cards", "users")
# resource -> the query param that switches the pool to `delta`.
_WINDOW_PARAM = {
    "transactions": "from_date",
    "reimbursements": "updated_after",
}


def _make_handler(fixtures: RampFixtures, hits: dict[str, int], business_id: str):
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

        def _incoming_base(self, path: str) -> str:
            host = self.headers.get("Host") or "127.0.0.1"
            return f"http://{host}{path}"

        def do_POST(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            if path.endswith("/token"):
                # Accept any client creds (Basic or none) — spammer parity.
                hits["token"] = hits.get("token", 0) + 1
                self._json(200, {
                    "access_token": "mock-ramp-access-token",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "scope": "transactions:read reimbursements:read "
                             "cards:read users:read business:read",
                })
                return
            self._json(404, {"error": f"no POST route {path}"})

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlsplit(self.path)
            path = parsed.path
            if path.endswith("/business"):
                hits["business"] = hits.get("business", 0) + 1
                self._json(200, {
                    "id": business_id,
                    "business_name_legal": "Sandbox Co",
                    "business_name_on_card": "Sandbox Co",
                    "active": True,
                })
                return
            segment = path.rstrip("/").rsplit("/", 1)[-1]
            if segment in _RESOURCES:
                self._handle_list(segment, path, parsed.query)
                return
            self._json(404, {"error": f"no GET route {path}"})

        def _handle_list(self, resource: str, path: str, query: str) -> None:
            hits[f"list:{resource}"] = hits.get(f"list:{resource}", 0) + 1
            params = {k: v[0] for k, v in parse_qs(query).items()}
            fx = fixtures.get(resource) or {"rows": [], "delta": []}

            window_param = _WINDOW_PARAM.get(resource)
            incremental = bool(window_param and params.get(window_param))
            pool = list(fx.get("delta", [])) if incremental else list(fx.get("rows", []))

            try:
                page_size = int(params.get("page_size", "20"))
            except ValueError:
                page_size = 20
            page_size = max(2, min(100, page_size))

            pos = 0
            start = params.get("start")
            if start:
                for i, row in enumerate(pool):
                    if str(row.get("id")) == start:
                        pos = i + 1
                        break
            page = pool[pos:pos + page_size]

            # Keyset `page.next`: absolute URL on the INCOMING host so the real
            # client can follow it (re-rooted by the client when overridden).
            next_url = None
            if page and len(page) == page_size and (pos + page_size) < len(pool):
                next_params = dict(params)
                next_params["start"] = str(page[-1].get("id"))
                next_params["page_size"] = page_size
                next_url = (
                    f"{self._incoming_base(path)}?{urlencode(next_params)}"
                )

            self._json(200, {"data": page, "page": {"next": next_url}})

    return _Handler


def start_mock_ramp(
    fixtures: RampFixtures,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    business_id: str = "mock-ramp-business",
) -> tuple[ThreadingHTTPServer, str]:
    """Start the mock on a background daemon thread. Returns `(server, base_url)`."""
    hits: dict[str, int] = {}
    handler = _make_handler(fixtures, hits, business_id)
    server = ThreadingHTTPServer((host, port), handler)
    server.request_hits = hits  # type: ignore[attr-defined]
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://{bound_host}:{bound_port}"


__all__ = ["RampFixtures", "start_mock_ramp"]
