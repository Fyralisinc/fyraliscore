"""services/ingest/synthetic/mock_servers/google_drive.py — local Drive v3 mock.

A real, threaded HTTP server that mimics the Drive v3 endpoints the Google Drive
DWD ingestion path touches, so a sandbox can drive the REAL minter + httpx
client + fetcher against it with no Google credentials:

  POST /token
      The DWD JWT->access-token exchange. Returns a canned bearer token.

  GET /changes/startPageToken            -> {"startPageToken": ...}
  GET /files                             -> full backfill walk {"files": [...]}
  GET /changes?pageToken=…               -> incremental delta {"changes": [...],
                                            "newStartPageToken": ...}
                                            (pageToken=="EXPIRED" -> 410)
  GET /drives?useDomainAdminAccess       -> shared-drive enumeration
  GET /files/{id}/export?mimeType=…      -> Doc/Sheet/Slide text body
  GET /files/{id}?alt=media              -> plain-text / PDF / binary body
  GET /files/{id}/comments               -> comments (+ replies)
  GET /files/{id}/revisions              -> revision history

Fixtures (a dict) let the sandbox control exactly what lands:
    {
      "files":   [ <drive v3 file objects> ],
      "changes": [ {"fileId","removed","time","file": {...}}, ... ],
      "exports": { file_id: "extracted text body" | b"<raw bytes for PDF>" },
      "comments":  { file_id: [ <comment objects> ] },
      "revisions": { file_id: [ <revision objects> ] },
      "shared_drives": [ {"id","name"}, ... ],
      "start_page_token": "spt-1",
      "new_start_page_token": "spt-2",
    }

Usage:
    server, base_url, token_url = start_mock_drive(fixtures)
    try:
        ...  # base_url -> GOOGLE_DRIVE_API_BASE_URL ; token_url -> SA token_uri
    finally:
        server.shutdown()
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse


DriveFixtures = dict[str, Any]


def _make_handler(fixtures: DriveFixtures, hits: dict[str, int]):
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

        def _raw(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.rstrip("/").endswith("/token") or parsed.path == "/token":
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
            parts = [p for p in parsed.path.split("/") if p]

            # /changes/startPageToken
            if parts[-2:] == ["changes", "startPageToken"]:
                hits["startPageToken"] = hits.get("startPageToken", 0) + 1
                self._json(200, {
                    "startPageToken": fixtures.get("start_page_token", "spt-1"),
                })
                return
            # /changes
            if parts[-1] == "changes":
                self._handle_changes(params)
                return
            # /drives
            if parts[-1] == "drives":
                hits["drives"] = hits.get("drives", 0) + 1
                self._json(200, {"drives": fixtures.get("shared_drives", [])})
                return
            # /files/{id}/export
            if len(parts) >= 3 and parts[-3] == "files" and parts[-1] == "export":
                self._handle_export(unquote(parts[-2]))
                return
            # /files/{id}/comments
            if len(parts) >= 3 and parts[-3] == "files" and parts[-1] == "comments":
                fid = unquote(parts[-2])
                hits[f"comments:{fid}"] = hits.get(f"comments:{fid}", 0) + 1
                self._json(200, {"comments": fixtures.get("comments", {}).get(fid, [])})
                return
            # /files/{id}/revisions
            if len(parts) >= 3 and parts[-3] == "files" and parts[-1] == "revisions":
                fid = unquote(parts[-2])
                hits[f"revisions:{fid}"] = hits.get(f"revisions:{fid}", 0) + 1
                self._json(200, {"revisions": fixtures.get("revisions", {}).get(fid, [])})
                return
            # /files/{id}  (alt=media)
            if len(parts) >= 2 and parts[-2] == "files" and params.get("alt") == "media":
                self._handle_media(unquote(parts[-1]))
                return
            # /files  (list)
            if parts[-1] == "files":
                self._handle_files(params)
                return
            self._json(404, {"error": {"message": f"no GET route {parsed.path}"}})

        def _handle_files(self, params: dict[str, str]) -> None:
            hits["files"] = hits.get("files", 0) + 1
            # Single page: all files, terminal (no nextPageToken).
            self._json(200, {"files": list(fixtures.get("files", []))})

        def _handle_changes(self, params: dict[str, str]) -> None:
            token = params.get("pageToken", "")
            hits["changes"] = hits.get("changes", 0) + 1
            if token == "EXPIRED":
                self._json(410, {"error": {
                    "code": 410, "message": "Page token expired.",
                }})
                return
            changes = list(fixtures.get("changes", []))
            page_size = int(params.get("pageSize", "200"))
            self._json(200, {
                "changes": changes[:page_size],
                "newStartPageToken": fixtures.get("new_start_page_token", "spt-2"),
            })

        def _serve_body(self, file_id: str) -> None:
            body = fixtures.get("exports", {}).get(file_id, "")
            if isinstance(body, bytes):
                # Raw bytes (e.g. a real PDF) -> served as application/pdf.
                self._raw(200, body, "application/pdf")
            else:
                self._raw(200, body.encode("utf-8"), "text/plain; charset=UTF-8")

        def _handle_export(self, file_id: str) -> None:
            hits[f"export:{file_id}"] = hits.get(f"export:{file_id}", 0) + 1
            self._serve_body(file_id)

        def _handle_media(self, file_id: str) -> None:
            hits[f"media:{file_id}"] = hits.get(f"media:{file_id}", 0) + 1
            self._serve_body(file_id)

    return _Handler


def start_mock_drive(
    fixtures: DriveFixtures, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, str, str]:
    """Start the mock on a background daemon thread.

    Returns `(server, base_url, token_url)`:
      - base_url  -> set as GOOGLE_DRIVE_API_BASE_URL (client appends /files, …).
      - token_url -> put in the fake service-account JSON's `token_uri`.
    Call `server.shutdown()` to stop.
    """
    hits: dict[str, int] = {}
    handler = _make_handler(fixtures, hits)
    server = ThreadingHTTPServer((host, port), handler)
    server.request_hits = hits  # type: ignore[attr-defined]
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://{bound_host}:{bound_port}"
    return server, base_url, f"{base_url}/token"


__all__ = ["DriveFixtures", "start_mock_drive"]
