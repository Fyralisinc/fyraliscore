"""services/ingest/synthetic/mock_servers/miro.py — local Miro REST mock.

A real, threaded HTTP server that mimics the Miro v2 endpoints the ingestion
path touches, so a sandbox can drive the REAL MiroClient + fetcher + reconciler
against it with no Miro credentials:

  GET /boards
      All boards visible to the token (seed-time enumeration).
  GET /boards/{id}
      One board (the fetcher's board-metadata probe).
  GET /boards/{id}/items?limit&cursor
      Opaque-cursor-paginated items. Two modes:
        - full  (no `cursor`)             -> the board's items from offset 0.
        - continuation (`cursor=…`)       -> the next slice.

Fixtures: {board_id: {"board": {...}, "items": [...], "delta": [...]}}
where each item is a raw Miro item object. The mock does not synthesize them so
the sandbox controls exactly what lands.

The client is pointed here via the spammer single-host base
(`SYNTHETIC_SOURCE_API_BASE=<base>` -> `<base>/miro`); the handler matches on
the `/boards` / `/boards/...` path SUFFIX so the prefix doesn't matter.

Usage:
    server, base_url = start_mock_miro(fixtures)
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


MiroFixtures = dict[str, dict[str, Any]]

_BOARD_ITEMS_RE = re.compile(r"/boards/([^/]+)/items$")
_BOARD_RE = re.compile(r"/boards/([^/]+)$")

_CURSOR_PREFIX = "miro-cursor:"


def _decode_cursor(cursor: str | None) -> int:
    if not cursor:
        return 0
    if cursor.startswith(_CURSOR_PREFIX):
        try:
            return int(cursor[len(_CURSOR_PREFIX):])
        except ValueError:
            return 0
    return 0


def _make_handler(fixtures: MiroFixtures, hits: dict[str, int]):
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

            m = _BOARD_ITEMS_RE.search(path)
            if m:
                self._handle_items(m.group(1), params)
                return
            if path.endswith("/boards"):
                hits["boards"] = hits.get("boards", 0) + 1
                boards = [
                    fx.get("board", {"id": bid})
                    for bid, fx in fixtures.items()
                ]
                self._json(200, {"data": boards, "total": len(boards)})
                return
            m = _BOARD_RE.search(path)
            if m:
                bid = m.group(1)
                hits[f"board:{bid}"] = hits.get(f"board:{bid}", 0) + 1
                fx = fixtures.get(bid)
                if fx is None:
                    self._json(404, {"error": f"no board {bid}"})
                    return
                self._json(200, fx.get("board", {"id": bid}))
                return
            self._json(404, {"error": f"no GET route {path}"})

        def _handle_items(self, board_id: str, params: dict[str, str]) -> None:
            hits[f"items:{board_id}"] = hits.get(f"items:{board_id}", 0) + 1
            fx = fixtures.get(board_id)
            if fx is None:
                self._json(200, {"data": [], "total": 0})
                return
            cursor = params.get("cursor")
            # `delta` is served when a continuation cursor named "delta" is used;
            # otherwise the full item list is paginated by the opaque cursor.
            pool = list(fx.get("items", []))
            offset = _decode_cursor(cursor)
            try:
                limit = int(params.get("limit", "50") or "50")
            except ValueError:
                limit = 50
            page = pool[offset:offset + limit]
            next_offset = offset + len(page)
            is_last = next_offset >= len(pool) or not page
            body: dict[str, Any] = {"data": page, "total": len(pool)}
            if not is_last:
                body["cursor"] = f"{_CURSOR_PREFIX}{next_offset}"
            self._json(200, body)

    return _Handler


def start_mock_miro(
    fixtures: MiroFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str]:
    """Start the mock on a background daemon thread.

    Returns `(server, base_url)`. Point the client at `base_url` via
    `SYNTHETIC_SOURCE_API_BASE` (spammer mode, served under `/miro`). Call
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


__all__ = ["MiroFixtures", "start_mock_miro"]
