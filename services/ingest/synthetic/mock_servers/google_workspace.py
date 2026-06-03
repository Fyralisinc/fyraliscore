"""services/ingest/synthetic/mock_servers/google_workspace.py — one mock for a whole
Google Workspace organization (Gmail + Calendar + Drive over Domain-Wide
Delegation).

Gmail/Calendar/Drive each had a single-source mock (or, for Gmail, none — it
used respx). But the three sources are *not* independent installs: in a real
Workspace they are one organization. A super-admin grants ONE service account
domain-wide delegation; ingestion then enumerates the domain through the Admin
SDK Directory API and impersonates each user, per-source, with the SAME service
account. To exercise that org-level path end-to-end we need a single mock that
behaves like one Workspace domain across every API the pipeline touches.

This server models that. Under one host it serves, faithfully enough to drive
the REAL DWD minter + httpx client + every Google fetcher with no Google creds:

  POST /token
      The DWD JWT->access-token exchange. UNLIKE the per-source mocks (which
      return a canned token), this decodes the signed assertion's `sub` claim
      (the impersonated user) and returns a token BOUND to that user:
      `wstok:<user-email>`. That is what makes one mock able to serve every
      mailbox/drive correctly — Gmail and Drive address the caller as `me`, so
      the only way to know *which* user is the bearer token, exactly as in
      production. The assertion signature is not verified (the fake SA key is
      self-issued); only the `sub`/`scope` claims are read.

  Admin SDK Directory v1  (/admin/directory/v1/...)
      GET /users?domain=…                  -> all domain users
      GET /users?query=orgUnitPath=/Sales  -> users in an org unit
      GET /groups?domain=…                 -> all domain groups
      GET /groups/{key}/members            -> a group's members
      GET /customer/{cid}/orgunits         -> org units
      Directory calls impersonate the admin; routing is by query param, not by
      the bearer.

  Gmail v1  (/gmail/v1/users/me/...)        -> routed by the bearer's user
      GET /messages                        -> message id/threadId stubs
      GET /messages/{id}                   -> a full message resource
      GET /profile                         -> {emailAddress, historyId}
      GET /history?startHistoryId=…        -> messagesAdded events (gap fill)
      POST /watch | /stop                  -> watch lifecycle (push path)

  Calendar v3  (/calendar/v3/calendars/{calendarId}/events)  -> by calendarId
      full sync (no syncToken)             -> events + nextSyncToken
      incremental (syncToken)              -> delta + new nextSyncToken
      probe (updatedMin)                   -> events updated since the bound
      syncToken=="EXPIRED"                 -> 410 (full-resync fallback)

  Drive v3  (/drive/v3/...)
      My Drive is routed by the bearer's user (corpora=user); a Shared Drive by
      the `driveId` query param (corpora=drive).
      GET /changes/startPageToken          -> {startPageToken}
      GET /files                           -> backfill walk {files}
      GET /changes?pageToken=…             -> delta {changes, newStartPageToken}
                                              (pageToken=="EXPIRED" -> 410)
      GET /drives?useDomainAdminAccess     -> shared-drive enumeration
      GET /files/{id}/export?mimeType=…    -> Doc/Sheet/Slide text body
      GET /files/{id}?alt=media            -> plain-text / PDF / binary body
      GET /files/{id}/comments             -> comments (+ replies)
      GET /files/{id}/revisions            -> revision history

The org is a single fixture object (see `WorkspaceOrg`). The mock synthesizes
nothing it isn't given, so a test controls exactly what lands.

Usage:
    org = WorkspaceOrg(domain="acme.com", users=[...], ...)
    server, env = start_mock_workspace(org)
    try:
        os.environ.update(env)          # GOOGLE_*_BASE_URL + token uri wiring
        # (point the fake SA's token_uri at env["GOOGLE_TOKEN_URI"] too)
        ...
    finally:
        server.shutdown()
"""
from __future__ import annotations

import base64
import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse


# ---------------------------------------------------------------------
# The org fixture.
# ---------------------------------------------------------------------
@dataclass
class WorkspaceOrg:
    """One Google Workspace domain, as the mock sees it.

    Directory shape mirrors the Admin SDK; per-source content is the raw API
    object shape each fetcher expects (so nothing is synthesized server-side).
    """

    domain: str

    # Admin SDK Directory.
    users: list[dict[str, Any]] = field(default_factory=list)
    groups: list[dict[str, Any]] = field(default_factory=list)
    # group email/key -> member resources [{type, email, ...}].
    group_members: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    org_units: list[dict[str, Any]] = field(default_factory=list)

    # Gmail: per-user mailbox. user_email -> {
    #   "messages": [<full Gmail message resource with id+payload.headers>],
    #   "history_id": "<str>",
    #   "history": [<users.history.list entries>],   # optional, gap fill
    # }
    gmail: dict[str, dict[str, Any]] = field(default_factory=dict)

    # Calendar: calendar_id (== owner email) -> {"events": [...], "delta": [...]}
    calendar: dict[str, dict[str, list[dict[str, Any]]]] = field(default_factory=dict)

    # Drive — My Drive per user. user_email -> drive fixture (see DriveFixtures):
    #   {"files":[...], "changes":[...], "exports":{id:str|bytes},
    #    "comments":{id:[...]}, "revisions":{id:[...]}}
    drive_my: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Shared drives enumerated org-wide, + per-drive content.
    shared_drives: list[dict[str, Any]] = field(default_factory=list)
    drive_shared: dict[str, dict[str, Any]] = field(default_factory=dict)

    start_page_token: str = "spt-1"
    new_start_page_token: str = "spt-2"


# ---------------------------------------------------------------------
# Token binding — decode the DWD assertion's `sub`.
# ---------------------------------------------------------------------
_TOKEN_PREFIX = "wstok:"


def _decode_jwt_sub(assertion: str) -> str | None:
    """Return the `sub` (impersonated user) from a JWT assertion WITHOUT
    verifying the signature. The middle segment is base64url JSON."""
    try:
        payload_b64 = assertion.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload_b64))
        sub = claims.get("sub")
        return sub if isinstance(sub, str) and sub else None
    except (IndexError, ValueError, TypeError):
        return None


def _bearer_user(headers: Any) -> str | None:
    """Extract the impersonated user from `Authorization: Bearer wstok:<user>`."""
    auth = headers.get("Authorization", "") or ""
    if not auth.startswith("Bearer "):
        return None
    token = auth[len("Bearer "):].strip()
    if token.startswith(_TOKEN_PREFIX):
        return token[len(_TOKEN_PREFIX):]
    return None


# ---------------------------------------------------------------------
# Request handler.
# ---------------------------------------------------------------------
def _make_handler(org: WorkspaceOrg, hits: dict[str, int]):
    def bump(key: str, n: int = 1) -> None:
        hits[key] = hits.get(key, 0) + n

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:  # noqa: D401 — silence stderr
            return

        # -- response helpers -----------------------------------------
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

        def _err(self, status: int, message: str) -> None:
            self._json(status, {"error": {"code": status, "message": message}})

        # -- POST ------------------------------------------------------
        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            if path.rstrip("/").endswith("/token") or path == "/token":
                self._handle_token()
                return
            # Gmail watch/stop (push lifecycle). Not exercised by backfill but
            # supported so the watch path can drive this mock too.
            if "/gmail/v1/" in path and path.rstrip("/").endswith("/watch"):
                bump("gmail.watch")
                self._drain_body()
                user = _bearer_user(self.headers) or ""
                hid = (org.gmail.get(user, {}) or {}).get("history_id", "1")
                self._json(200, {"historyId": hid, "expiration": "9999999999999"})
                return
            if "/gmail/v1/" in path and path.rstrip("/").endswith("/stop"):
                bump("gmail.stop")
                self._drain_body()
                self._json(204, {})
                return
            self._err(404, f"no POST route {path}")

        def _drain_body(self) -> bytes:
            length = int(self.headers.get("Content-Length", "0") or "0")
            return self.rfile.read(length) if length else b""

        def _handle_token(self) -> None:
            body = self._drain_body().decode("utf-8", errors="replace")
            params = {k: v[0] for k, v in parse_qs(body).items()}
            sub = _decode_jwt_sub(params.get("assertion", ""))
            bump("token")
            if sub:
                bump(f"token:{sub}")
            # Bind the access token to the impersonated user so data endpoints
            # that address the caller as `me` (Gmail, Drive My Drive) can route.
            self._json(200, {
                "access_token": f"{_TOKEN_PREFIX}{sub or 'unknown'}",
                "expires_in": 3600,
                "token_type": "Bearer",
            })

        # -- GET -------------------------------------------------------
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            try:
                if "/admin/directory/v1/" in path:
                    self._route_directory(path, params)
                elif "/gmail/v1/" in path:
                    self._route_gmail(path, params)
                elif "/calendar/v3/" in path:
                    self._route_calendar(path, params)
                elif "/drive/v3/" in path:
                    self._route_drive(path, params)
                else:
                    self._err(404, f"no GET route {path}")
            except BrokenPipeError:  # pragma: no cover — client gave up
                pass

        # -- Admin SDK Directory --------------------------------------
        def _route_directory(self, path: str, params: dict[str, str]) -> None:
            parts = [p for p in path.split("/") if p]
            # /customer/{cid}/orgunits
            if "orgunits" in parts:
                bump("dir.orgunits")
                self._json(200, {"organizationUnits": org.org_units})
                return
            # /groups/{key}/members
            if len(parts) >= 2 and parts[-1] == "members" and "groups" in parts:
                group_key = unquote(parts[-2])
                bump(f"dir.members:{group_key}")
                members = org.group_members.get(group_key, [])
                self._json(200, {"members": members})
                return
            # /groups
            if parts[-1] == "groups":
                bump("dir.groups")
                self._json(200, {"groups": org.groups})
                return
            # /users (by domain, or filtered by orgUnitPath query)
            if parts[-1] == "users":
                query = params.get("query", "")
                if query.startswith("orgUnitPath="):
                    ou = query[len("orgUnitPath="):]
                    bump(f"dir.users.ou:{ou}")
                    matched = [u for u in org.users if u.get("orgUnitPath") == ou]
                    self._json(200, {"users": matched})
                    return
                bump("dir.users")
                self._json(200, {"users": org.users})
                return
            self._err(404, f"no directory route {path}")

        # -- Gmail v1 --------------------------------------------------
        def _route_gmail(self, path: str, params: dict[str, str]) -> None:
            user = _bearer_user(self.headers)
            if not user:
                self._err(401, "gmail: missing/!bound bearer")
                return
            mailbox = org.gmail.get(user, {})
            parts = [p for p in path.split("/") if p]

            # /users/me/profile
            if parts[-1] == "profile":
                bump(f"gmail.profile:{user}")
                self._json(200, {
                    "emailAddress": user,
                    "historyId": str(mailbox.get("history_id", "1")),
                    "messagesTotal": len(mailbox.get("messages", [])),
                })
                return
            # /users/me/history
            if parts[-1] == "history":
                bump(f"gmail.history:{user}")
                self._json(200, {
                    "history": mailbox.get("history", []),
                    "historyId": str(mailbox.get("history_id", "1")),
                })
                return
            # /users/me/messages/{id}
            if len(parts) >= 2 and parts[-2] == "messages":
                msg_id = unquote(parts[-1])
                bump(f"gmail.get:{user}")
                for m in mailbox.get("messages", []):
                    if str(m.get("id")) == msg_id:
                        self._json(200, m)
                        return
                self._err(404, f"gmail message not found: {msg_id}")
                return
            # /users/me/messages  (list -> id/threadId stubs, single page)
            if parts[-1] == "messages":
                bump(f"gmail.list:{user}")
                stubs = [
                    {"id": str(m.get("id")), "threadId": m.get("threadId")}
                    for m in mailbox.get("messages", [])
                ]
                self._json(200, {
                    "messages": stubs,
                    "resultSizeEstimate": len(stubs),
                })
                return
            self._err(404, f"no gmail route {path}")

        # -- Calendar v3 ----------------------------------------------
        def _route_calendar(self, path: str, params: dict[str, str]) -> None:
            parts = [p for p in path.split("/") if p]
            if len(parts) >= 3 and parts[-3] == "calendars" and parts[-1] == "events":
                calendar_id = unquote(parts[-2])
                self._calendar_events(calendar_id, params)
                return
            self._err(404, f"no calendar route {path}")

        def _calendar_events(self, calendar_id: str, params: dict[str, str]) -> None:
            bump(f"cal.events:{calendar_id}")
            fx = org.calendar.get(calendar_id)
            if fx is None:
                self._json(200, {"items": [], "nextSyncToken": "sync-empty"})
                return
            events = list(fx.get("events", []))
            delta = list(fx.get("delta", []))
            if "syncToken" in params:
                if params["syncToken"] == "EXPIRED":
                    self._err(410, "Sync token is no longer valid.")
                    return
                self._json(200, {"items": delta, "nextSyncToken": "sync-2"})
                return
            if "updatedMin" in params:
                bound = params["updatedMin"]
                hit = [e for e in (events + delta) if str(e.get("updated", "")) > bound]
                self._json(200, {"items": hit[: int(params.get("maxResults", "250"))]})
                return
            self._json(200, {"items": events, "nextSyncToken": "sync-1"})

        # -- Drive v3 --------------------------------------------------
        def _drive_fixture(self, params: dict[str, str]) -> dict[str, Any]:
            """Pick the drive fixture: a Shared Drive (driveId param) or the
            bearer user's My Drive."""
            drive_id = params.get("driveId")
            if drive_id and drive_id in org.drive_shared:
                return org.drive_shared[drive_id]
            user = _bearer_user(self.headers) or ""
            return org.drive_my.get(user, {})

        def _route_drive(self, path: str, params: dict[str, str]) -> None:
            parts = [p for p in path.split("/") if p]
            # /changes/startPageToken
            if parts[-2:] == ["changes", "startPageToken"]:
                bump("drive.startPageToken")
                self._json(200, {"startPageToken": org.start_page_token})
                return
            # /changes
            if parts[-1] == "changes":
                self._drive_changes(params)
                return
            # /drives
            if parts[-1] == "drives":
                bump("drive.drives")
                self._json(200, {"drives": org.shared_drives})
                return
            # /files/{id}/export
            if len(parts) >= 3 and parts[-3] == "files" and parts[-1] == "export":
                self._drive_body(unquote(parts[-2]), params, "export")
                return
            # /files/{id}/comments
            if len(parts) >= 3 and parts[-3] == "files" and parts[-1] == "comments":
                fid = unquote(parts[-2])
                bump(f"drive.comments:{fid}")
                self._json(200, {"comments": self._drive_fixture(params).get("comments", {}).get(fid, [])})
                return
            # /files/{id}/revisions
            if len(parts) >= 3 and parts[-3] == "files" and parts[-1] == "revisions":
                fid = unquote(parts[-2])
                bump(f"drive.revisions:{fid}")
                self._json(200, {"revisions": self._drive_fixture(params).get("revisions", {}).get(fid, [])})
                return
            # /files/{id}?alt=media
            if len(parts) >= 2 and parts[-2] == "files" and params.get("alt") == "media":
                self._drive_body(unquote(parts[-1]), params, "media")
                return
            # /files  (list)
            if parts[-1] == "files":
                bump("drive.files")
                self._json(200, {"files": list(self._drive_fixture(params).get("files", []))})
                return
            self._err(404, f"no drive route {path}")

        def _drive_changes(self, params: dict[str, str]) -> None:
            bump("drive.changes")
            if params.get("pageToken") == "EXPIRED":
                self._err(410, "Page token expired.")
                return
            fx = self._drive_fixture(params)
            self._json(200, {
                "changes": list(fx.get("changes", [])),
                "newStartPageToken": org.new_start_page_token,
            })

        def _drive_body(self, file_id: str, params: dict[str, str], kind: str) -> None:
            bump(f"drive.{kind}:{file_id}")
            body = self._drive_fixture(params).get("exports", {}).get(file_id, "")
            if isinstance(body, bytes):
                self._raw(200, body, "application/pdf")
            else:
                self._raw(200, body.encode("utf-8"), "text/plain; charset=UTF-8")

    return _Handler


def start_mock_workspace(
    org: WorkspaceOrg, *, host: str = "127.0.0.1", port: int = 0,
) -> tuple[ThreadingHTTPServer, dict[str, str]]:
    """Start the org mock on a background daemon thread.

    Returns `(server, env)` where `env` is the set of environment overrides
    that point every Google client at this mock (and a `GOOGLE_TOKEN_URI` for
    the fake service-account JSON's `token_uri`). `server.request_hits` is a
    live dict of per-endpoint hit counters for assertions. Call
    `server.shutdown()` to stop.
    """
    hits: dict[str, int] = {}
    server = ThreadingHTTPServer((host, port), _make_handler(org, hits))
    server.request_hits = hits  # type: ignore[attr-defined]
    bound_host, bound_port = server.server_address[0], server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://{bound_host}:{bound_port}"
    env = {
        "GOOGLE_TOKEN_URI": f"{base}/token",
        "GOOGLE_DIRECTORY_BASE_URL": f"{base}/admin/directory/v1",
        "GMAIL_API_BASE_URL": f"{base}/gmail/v1",
        "GOOGLE_CALENDAR_API_BASE_URL": f"{base}/calendar/v3",
        "GOOGLE_DRIVE_API_BASE_URL": f"{base}/drive/v3",
    }
    return server, env


__all__ = ["WorkspaceOrg", "start_mock_workspace"]
