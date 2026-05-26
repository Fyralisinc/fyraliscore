"""services/synthetic/mock_servers/jira.py — local Jira Cloud REST mock (IN-17).

A real, threaded HTTP server that mimics the Jira Cloud v3 endpoints the
ingestion path touches, so a sandbox can drive the REAL JiraClient + fetcher +
reconciler against it with no Atlassian credentials:

  POST /rest/api/3/search/jql
      JQL issue search (the post-2025 token-paginated endpoint; the classic
      /rest/api/3/search was removed → 410). Returns {issues, nextPageToken,
      isLast} — the page token encodes the offset. Two JQL modes:
        - full  (`project = "KEY" ORDER BY updated ASC`)          -> all issues.
        - incremental (`... AND updated >= "<floor>" ...`)        -> the
          project's delta issues.
  POST /rest/api/3/search/approximate-count
      -> {count} for the reconciler gap probe (replaces the removed `total`).

  GET /rest/api/3/project/search
      Projects visible to the token (seed-time enumeration).

  GET /rest/api/3/myself
      Credential/connectivity probe.

Fixtures: {project_key: {"issues": [...], "delta": [...]}} where each issue is
a raw Jira v3 issue object (id, key, fields{...}, optional changelog{histories}).
The mock does not synthesize them so the sandbox controls exactly what lands.

The client may be pointed here either via `JIRA_API_BASE_URL=<base>` or via the
spammer single-host base (`SYNTHETIC_SOURCE_API_BASE=<base>` -> `<base>/jira`);
the handler matches on the `/rest/api/3/...` path SUFFIX so either prefix works.

Usage:
    server, base_url = start_mock_jira(fixtures)
    try:
        ...  # base_url -> JIRA_API_BASE_URL (or SYNTHETIC_SOURCE_API_BASE)
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


JiraFixtures = dict[str, dict[str, list[dict[str, Any]]]]

_PROJECT_RE = re.compile(r'project\s*=\s*"([^"]+)"', re.IGNORECASE)


def _make_handler(fixtures: JiraFixtures, hits: dict[str, int]):
    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:  # noqa: D401
            return

        def _json(self, status: int, body: dict[str, Any]) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _read_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if not length:
                return {}
            try:
                return json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError:
                return {}

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            body = self._read_body()
            if path.endswith("/rest/api/3/search/jql"):
                self._handle_search(body)
                return
            if path.endswith("/rest/api/3/search/approximate-count"):
                self._handle_count(body)
                return
            self._json(404, {"errorMessages": [f"no POST route {path}"]})

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            if path.endswith("/rest/api/3/project/search"):
                params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
                self._handle_project_search(params)
                return
            if path.endswith("/rest/api/3/myself"):
                hits["myself"] = hits.get("myself", 0) + 1
                self._json(200, {
                    "accountId": "sandbox-account",
                    "emailAddress": "sandbox@acme.com",
                    "displayName": "Sandbox Bot",
                })
                return
            self._json(404, {"errorMessages": [f"no GET route {path}"]})

        def _pool_for(self, jql: str) -> list[dict[str, Any]]:
            m = _PROJECT_RE.search(jql)
            project_key = m.group(1) if m else None
            hits[f"search:{project_key}"] = hits.get(f"search:{project_key}", 0) + 1
            fx = fixtures.get(project_key) if project_key else None
            if fx is None:
                return []
            # Matches both the walk floor (`updated >=`) and the reconciler
            # probe (`updated >`), since "updated >" is a prefix of "updated >=".
            incremental = "updated >" in jql.lower() or "updated>" in jql.lower()
            return list(fx.get("delta", [])) if incremental else list(fx.get("issues", []))

        def _handle_search(self, body: dict[str, Any]) -> None:
            """New token-paginated `/rest/api/3/search/jql`: the page token
            encodes the offset; the response carries `nextPageToken` + `isLast`
            (NO startAt/total — matching Jira Cloud 2025)."""
            jql = str(body.get("jql", ""))
            max_results = int(body.get("maxResults", 50) or 50)
            try:
                offset = int(body.get("nextPageToken") or 0)
            except (TypeError, ValueError):
                offset = 0
            pool = self._pool_for(jql)
            page = pool[offset:offset + max_results]
            next_offset = offset + len(page)
            is_last = next_offset >= len(pool)
            resp: dict[str, Any] = {"issues": page, "isLast": is_last}
            if not is_last:
                resp["nextPageToken"] = str(next_offset)
            self._json(200, resp)

        def _handle_count(self, body: dict[str, Any]) -> None:
            """`/rest/api/3/search/approximate-count` -> {count}."""
            pool = self._pool_for(str(body.get("jql", "")))
            self._json(200, {"count": len(pool)})

        def _handle_project_search(self, params: dict[str, str]) -> None:
            hits["project_search"] = hits.get("project_search", 0) + 1
            start_at = int(params.get("startAt", "0") or "0")
            max_results = int(params.get("maxResults", "50") or "50")
            projects = [
                {"id": str(1000 + i), "key": key, "name": f"{key} project"}
                for i, key in enumerate(sorted(fixtures.keys()))
            ]
            total = len(projects)
            page = projects[start_at:start_at + max_results]
            self._json(200, {
                "startAt": start_at,
                "maxResults": max_results,
                "total": total,
                "isLast": start_at + len(page) >= total,
                "values": page,
            })

    return _Handler


def start_mock_jira(
    fixtures: JiraFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str]:
    """Start the mock on a background daemon thread.

    Returns `(server, base_url)`. Point the client at `base_url` via
    `JIRA_API_BASE_URL` (direct) or `SYNTHETIC_SOURCE_API_BASE` (spammer mode,
    served under `/jira`). Call `server.shutdown()` to stop.
    """
    hits: dict[str, int] = {}
    handler = _make_handler(fixtures, hits)
    server = ThreadingHTTPServer((host, port), handler)
    server.request_hits = hits  # type: ignore[attr-defined]
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://{bound_host}:{bound_port}"


__all__ = ["JiraFixtures", "start_mock_jira"]
