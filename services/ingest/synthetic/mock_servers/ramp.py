"""services/ingest/synthetic/mock_servers/ramp.py — local Ramp mock.

Cloned from the QuickBooks archetype mock server. A real, threaded HTTP server
that mimics the archetype query endpoint the ingestion path touches, so a sandbox
can drive the REAL RampClient + fetcher + reconciler against it with no Ramp
credentials. (The endpoint shape is the archetype's; the verified Ramp read
surface is UNVERIFIED — see the client/fetcher TODOs.):

  GET /v3/company/{business}/query?query=<SQL>&minorversion=75
      SQL-like entity query. The mock parses the entity name, optional
      `Metadata.LastUpdatedTime > '<ts>'` WHERE filter, and
      `STARTPOSITION n MAXRESULTS m` paging out of the SQL. Returns
      {"QueryResponse": {"<Entity>": [...], "startPosition", "maxResults"}}.
  GET /v3/company/{business}/companyinfo/{business}
      Connectivity probe.

Fixtures: {entity_name: {"rows": [...], "delta": [...]}} where each row is a raw
RAMP entity object (Id, SyncToken, MetaData.LastUpdatedTime, ...). `delta` is the
pool returned for incremental (`Metadata.LastUpdatedTime > ...`) queries.

Pointed at via `SYNTHETIC_SOURCE_API_BASE` (spammer mode, served under
`/ramp`); the handler matches the `/v3/company/.../query` path SUFFIX so
the prefix doesn't matter.

Usage:
    server, base_url = start_mock_ramp(fixtures)
"""
from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


RampFixtures = dict[str, dict[str, list[dict[str, Any]]]]

_FROM_RE = re.compile(r"\bFROM\s+(\w+)", re.IGNORECASE)
_START_RE = re.compile(r"STARTPOSITION\s+(\d+)", re.IGNORECASE)
_MAX_RE = re.compile(r"MAXRESULTS\s+(\d+)", re.IGNORECASE)
_INCR_RE = re.compile(r"LastUpdatedTime\s*>", re.IGNORECASE)


def _make_handler(fixtures: RampFixtures, hits: dict[str, int]):
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
            if path.endswith("/query"):
                params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
                self._handle_query(params.get("query", ""))
                return
            if "/companyinfo/" in path:
                hits["companyinfo"] = hits.get("companyinfo", 0) + 1
                self._json(200, {"CompanyInfo": {"CompanyName": "Sandbox Co"}})
                return
            self._json(404, {"error": f"no GET route {path}"})

        def _handle_query(self, sql: str) -> None:
            m = _FROM_RE.search(sql)
            entity = m.group(1) if m else None
            hits[f"query:{entity}"] = hits.get(f"query:{entity}", 0) + 1
            fx = fixtures.get(entity) if entity else None
            if fx is None:
                self._json(200, {"QueryResponse": {"startPosition": 1, "maxResults": 0}})
                return
            incremental = bool(_INCR_RE.search(sql))
            pool = list(fx.get("delta", [])) if incremental else list(fx.get("rows", []))
            start = int(_START_RE.search(sql).group(1)) if _START_RE.search(sql) else 1
            max_results = int(_MAX_RE.search(sql).group(1)) if _MAX_RE.search(sql) else 100
            page = pool[start - 1: start - 1 + max_results]
            self._json(200, {
                "QueryResponse": {
                    entity: page,
                    "startPosition": start,
                    "maxResults": len(page),
                },
                "time": "2026-01-01T00:00:00.000-08:00",
            })

    return _Handler


def start_mock_ramp(
    fixtures: RampFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str]:
    """Start the mock on a background daemon thread. Returns `(server, base_url)`."""
    hits: dict[str, int] = {}
    handler = _make_handler(fixtures, hits)
    server = ThreadingHTTPServer((host, port), handler)
    server.request_hits = hits  # type: ignore[attr-defined]
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://{bound_host}:{bound_port}"


__all__ = ["RampFixtures", "start_mock_ramp"]
