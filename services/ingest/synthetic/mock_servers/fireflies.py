"""Local Fireflies GraphQL mock for running the real FirefliesClient."""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse


FirefliesFixtures = dict[str, Any]


def _make_handler(fixtures: FirefliesFixtures, hits: dict[str, int]):
    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:
            return

        def _json(self, status: int, body: Any) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if not path.endswith("/graphql"):
                self._json(404, {"error": f"no POST route {path}"})
                return
            hits["graphql"] = hits.get("graphql", 0) + 1
            try:
                size = int(self.headers.get("Content-Length", "0") or "0")
                body = json.loads(self.rfile.read(size) or b"{}")
            except json.JSONDecodeError:
                self._json(400, {"errors": [{"message": "invalid json"}]})
                return
            query = str(body.get("query") or "")
            variables = body.get("variables") if isinstance(body.get("variables"), dict) else {}
            if "user" in query and "transcripts" not in query and "transcript(" not in query:
                self._json(200, {"data": {"user": _user()}})
                return
            if "transcript(" in query:
                self._json(200, {"data": {"transcript": _transcript(str(variables.get("id") or ""))}})
                return
            if "transcripts" in query:
                self._json(200, {"data": {"transcripts": _transcripts(variables)}})
                return
            self._json(200, {"data": {}})

    def _user() -> dict[str, Any]:
        workspace = fixtures.get("workspace")
        if isinstance(workspace, dict):
            return {
                "id": workspace.get("id") or workspace.get("workspace_id") or fixtures.get("workspace_id") or "ws-mock",
                "email": workspace.get("email") or "mock-fireflies@example.com",
                "name": workspace.get("name") or workspace.get("workspace_name") or "Synthetic Workspace",
            }
        return {
            "id": fixtures.get("workspace_id") or "ws-mock",
            "email": "mock-fireflies@example.com",
            "name": fixtures.get("workspace_name") or "Synthetic Workspace",
        }

    def _transcript(transcript_id: str) -> dict[str, Any] | None:
        for item in fixtures.get("transcripts", []):
            if isinstance(item, dict) and _transcript_id(item) == transcript_id:
                return item
        return None

    def _transcripts(variables: dict[str, Any]) -> list[dict[str, Any]]:
        floor = variables.get("fromDate")
        source_key = "delta" if isinstance(floor, str) and floor and isinstance(fixtures.get("delta"), list) else "transcripts"
        items = [t for t in fixtures.get(source_key, []) if isinstance(t, dict)]
        if isinstance(floor, str) and floor:
            items = [t for t in items if _transcript_date(t) >= floor[:10]]
        try:
            skip = int(variables.get("skip") or 0)
        except (TypeError, ValueError):
            skip = 0
        try:
            limit = int(variables.get("limit") or 50)
        except (TypeError, ValueError):
            limit = 50
        return items[skip:skip + limit]

    return _Handler


def _transcript_id(item: dict[str, Any]) -> str:
    return str(item.get("id") or item.get("transcript_id") or item.get("transcriptId") or "")


def _transcript_date(item: dict[str, Any]) -> str:
    value = item.get("dateTime") or item.get("date") or item.get("createdAt") or ""
    if isinstance(value, (int, float)):
        from datetime import datetime, timezone
        return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).date().isoformat()
    return value[:10] if isinstance(value, str) else ""


def start_mock_fireflies(
    fixtures: FirefliesFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str]:
    hits: dict[str, int] = {}
    handler = _make_handler(fixtures, hits)
    server = ThreadingHTTPServer((host, port), handler)
    server.request_hits = hits  # type: ignore[attr-defined]
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://{bound_host}:{bound_port}"


__all__ = ["FirefliesFixtures", "start_mock_fireflies"]
