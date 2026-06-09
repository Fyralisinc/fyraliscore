"""services/ingest/synthetic/mock_servers/figma.py — local Figma REST mock.

A real, threaded HTTP server that mimics the Figma v1 endpoints the ingestion
path touches, so a sandbox can drive the REAL FigmaClient + fetcher +
reconciler against it with no Figma credentials:

  GET /v1/files
      All files visible to the token (seed-time enumeration).
  GET /v1/files/{key}/meta
      One file's metadata (the fetcher's recency probe).
  GET /v1/files/{key}/events?limit&offset&start
      Paginated events. Two modes:
        - full  (no `start`)              -> all events for the file.
        - incremental (`start=<date>`)    -> the file's delta events.

Fixtures: {file_key: {"file": {...}, "events": [...], "delta": [...]}}
where each event is a raw Figma event object. The mock does not synthesize them
so the sandbox controls exactly what lands.

The client is pointed here via the spammer single-host base
(`SYNTHETIC_SOURCE_API_BASE=<base>` -> `<base>/figma`); the handler matches on
the `/v1/files...` path SUFFIX so the prefix doesn't matter.

Usage:
    server, base_url = start_mock_figma(fixtures)
    try:
        ...  # base_url -> SYNTHETIC_SOURCE_API_BASE
    finally:
        server.shutdown()
"""
from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


FigmaFixtures = dict[str, dict[str, Any]]

_FILE_EVENTS_RE = re.compile(r"/v1/files/([^/]+)/events$")
_FILE_META_RE = re.compile(r"/v1/files/([^/]+)/meta$")


def _make_handler(fixtures: FigmaFixtures, hits: dict[str, int]):
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
            params = {k: v[0] for k, v in parse_qs(parsed.query).items()}

            m = _FILE_EVENTS_RE.search(path)
            if m:
                self._handle_events(m.group(1), params)
                return
            if path.endswith("/v1/files"):
                hits["files"] = hits.get("files", 0) + 1
                files = [
                    fx.get("file", {"key": key})
                    for key, fx in fixtures.items()
                ]
                self._json(200, {"files": files, "total": len(files)})
                return
            m = _FILE_META_RE.search(path)
            if m:
                key = m.group(1)
                hits[f"file:{key}"] = hits.get(f"file:{key}", 0) + 1
                fx = fixtures.get(key)
                if fx is None:
                    self._json(404, {"error": f"no file {key}"})
                    return
                self._json(200, fx.get("file", {"key": key}))
                return
            self._json(404, {"error": f"no GET route {path}"})

        def _handle_events(self, file_key: str, params: dict[str, str]) -> None:
            hits[f"events:{file_key}"] = hits.get(f"events:{file_key}", 0) + 1
            fx = fixtures.get(file_key)
            if fx is None:
                self._json(200, {"events": [], "total": 0})
                return
            incremental = bool(params.get("start"))
            pool = list(fx.get("delta", [])) if incremental else list(fx.get("events", []))
            try:
                limit = int(params.get("limit", "100") or "100")
            except ValueError:
                limit = 100
            try:
                offset = int(params.get("offset", "0") or "0")
            except ValueError:
                offset = 0
            page = pool[offset:offset + limit]
            self._json(200, {"events": page, "total": len(pool)})

    return _Handler


def start_mock_figma(
    fixtures: FigmaFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str]:
    """Start the mock on a background daemon thread.

    Returns `(server, base_url)`. Point the client at `base_url` via
    `SYNTHETIC_SOURCE_API_BASE` (spammer mode, served under `/figma`). Call
    `server.shutdown()` to stop.
    """
    hits: dict[str, int] = {}
    handler = _make_handler(fixtures, hits)
    server = ThreadingHTTPServer((host, port), handler)
    server.request_hits = hits  # type: ignore[attr-defined]
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://{bound_host}:{bound_port}"


__all__ = ["FigmaFixtures", "start_mock_figma"]
