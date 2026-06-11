"""Local Figma REST mock for running the real FigmaClient in sandboxes."""
from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse


FigmaFixtures = dict[str, dict[str, Any]]

_PROJECT_FILES_RE = re.compile(r"/v1/projects/([^/]+)/files$")
_FILE_RE = re.compile(r"/v1/files/([^/]+)$")
_VERSIONS_RE = re.compile(r"/v1/files/([^/]+)/versions$")
_COMMENTS_RE = re.compile(r"/v1/files/([^/]+)/comments$")


def _make_handler(fixtures: FigmaFixtures, hits: dict[str, int]):
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

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if "/v1/teams/" in path and path.endswith("/projects"):
                hits["projects"] = hits.get("projects", 0) + 1
                self._json(200, {"projects": [{"id": "mock-project", "name": "Synthetic"}]})
                return
            m = _PROJECT_FILES_RE.search(path)
            if m:
                hits[f"project:{m.group(1)}:files"] = hits.get(f"project:{m.group(1)}:files", 0) + 1
                self._json(200, {"files": _files()})
                return
            m = _VERSIONS_RE.search(path)
            if m:
                key = f"versions:{m.group(1)}"
                hits[key] = hits.get(key, 0) + 1
                self._json(200, {"versions": _versions(m.group(1)), "pagination": {"next_page": None}})
                return
            m = _COMMENTS_RE.search(path)
            if m:
                key = f"comments:{m.group(1)}"
                hits[key] = hits.get(key, 0) + 1
                self._json(200, {"comments": _comments(m.group(1))})
                return
            m = _FILE_RE.search(path)
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

    def _files() -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for key, fx in fixtures.items():
            item = dict(fx.get("file", {"key": key}))
            item.setdefault("key", key)
            out.append(item)
        return out

    def _events(file_key: str) -> list[dict[str, Any]]:
        fx = fixtures.get(file_key)
        if not isinstance(fx, dict):
            return []
        use_delta = (
            (hits.get(f"versions:{file_key}", 0) > 1)
            or (hits.get(f"comments:{file_key}", 0) > 1)
        )
        source_key = "delta" if use_delta and isinstance(fx.get("delta"), list) else "events"
        return [e for e in fx.get(source_key, []) if isinstance(e, dict)]

    def _versions(file_key: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for event in _events(file_key):
            if str(event.get("event_type") or event.get("type") or "").upper() == "FILE_COMMENT":
                continue
            out.append({
                "id": event.get("version") or event.get("id"),
                "label": event.get("label"),
                "description": event.get("description"),
                "user": event.get("triggered_by") or event.get("user"),
                "created_at": event.get("created_at") or event.get("createdAt"),
            })
        return out

    def _comments(file_key: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for event in _events(file_key):
            if str(event.get("event_type") or event.get("type") or "").upper() != "FILE_COMMENT":
                continue
            out.append({
                "id": event.get("id"),
                "message": event.get("message") or event.get("label"),
                "user": event.get("triggered_by") or event.get("user"),
                "created_at": event.get("created_at") or event.get("createdAt"),
                "updated_at": event.get("updated_at") or event.get("created_at") or event.get("createdAt"),
            })
        return out

    return _Handler


def start_mock_figma(
    fixtures: FigmaFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str]:
    hits: dict[str, int] = {}
    handler = _make_handler(fixtures, hits)
    server = ThreadingHTTPServer((host, port), handler)
    server.request_hits = hits  # type: ignore[attr-defined]
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://{bound_host}:{bound_port}"


__all__ = ["FigmaFixtures", "start_mock_figma"]
