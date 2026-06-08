"""services/ingest/synthetic/mock_servers/fireflies.py — local Fireflies REST mock.

A real, threaded HTTP server that mimics the Fireflies endpoints the ingestion
path touches, so a sandbox can drive the REAL FirefliesClient + fetcher +
reconciler against it with no Fireflies credentials:

  GET /workspace
      The workspace the token is scoped to (seed-time probe).
  GET /transcript/{id}
      One transcript (hydrate probe).
  GET /transcripts?limit&offset&start
      Paginated transcripts. Two modes:
        - full  (no `start`)              -> all transcripts for the workspace.
        - incremental (`start=<date>`)    -> the workspace's delta transcripts.

Fixtures: {"workspace": {...}, "transcripts": [...], "delta": [...]} where each
transcript is a raw Fireflies transcript object. The mock does not synthesize
them so the sandbox controls exactly what lands.

The client is pointed here via the spammer single-host base
(`SYNTHETIC_SOURCE_API_BASE=<base>` -> `<base>/fireflies`); the handler matches
on the `/workspace` / `/transcripts` / `/transcript/...` path SUFFIX so the
prefix doesn't matter.

Usage:
    server, base_url = start_mock_fireflies(fixtures)
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


FirefliesFixtures = dict[str, Any]

_TRANSCRIPT_RE = re.compile(r"/transcript/([^/]+)$")


def _transcript_id(t: dict[str, Any]) -> str:
    return str(t.get("id") or t.get("transcript_id") or t.get("transcriptId") or "")


def _make_handler(fixtures: FirefliesFixtures, hits: dict[str, int]):
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

            if path.endswith("/transcripts"):
                self._handle_transcripts(params)
                return
            if path.endswith("/workspace"):
                hits["workspace"] = hits.get("workspace", 0) + 1
                self._json(200, fixtures.get("workspace", {"id": "ws-mock"}))
                return
            m = _TRANSCRIPT_RE.search(path)
            if m:
                tid = m.group(1)
                hits[f"transcript:{tid}"] = hits.get(f"transcript:{tid}", 0) + 1
                for t in fixtures.get("transcripts", []):
                    if _transcript_id(t) == tid:
                        self._json(200, t)
                        return
                self._json(404, {"error": f"no transcript {tid}"})
                return
            self._json(404, {"error": f"no GET route {path}"})

        def _handle_transcripts(self, params: dict[str, str]) -> None:
            hits["transcripts"] = hits.get("transcripts", 0) + 1
            incremental = bool(params.get("start"))
            pool = list(fixtures.get("delta", [])) if incremental else list(fixtures.get("transcripts", []))
            try:
                limit = int(params.get("limit", "50") or "50")
            except ValueError:
                limit = 50
            try:
                offset = int(params.get("offset", "0") or "0")
            except ValueError:
                offset = 0
            page = pool[offset:offset + limit]
            self._json(200, {"transcripts": page, "total": len(pool)})

    return _Handler


def start_mock_fireflies(
    fixtures: FirefliesFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str]:
    """Start the mock on a background daemon thread.

    Returns `(server, base_url)`. Point the client at `base_url` via
    `SYNTHETIC_SOURCE_API_BASE` (spammer mode, served under `/fireflies`). Call
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


__all__ = ["FirefliesFixtures", "start_mock_fireflies"]
