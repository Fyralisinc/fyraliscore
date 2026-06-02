"""services/ingest/integrations/google_drive/client.py — outbound Drive v3 client.

A thin wrapper over the SHARED `GoogleHttpClient` (services/ingest/integrations/
gmail/client.py), which owns DWD token minting, the `Authorization: Bearer`
header, 401-retry, and 429/403-quota -> `GoogleRateLimited` mapping. This
class adds only the Drive v3 request shapes + document text export.

Auth model (D1): the service account impersonates a user (`user_email`). For a
user's My Drive we read that user's corpus; for a Shared Drive we impersonate a
user/admin who can see it and address it by `drive_id`.

Incremental sync (D2): `changes.getStartPageToken` returns the token to start
watching from; `changes.list?pageToken=…` returns deltas and a
`newStartPageToken` on the last page. This is the exact analog of Calendar's
syncToken. An invalid/expired page token yields HTTP 410/400, which the fetcher
catches to reseed a full sync.

Content extraction (D8): Google-native docs are exported to text via
`files.export`; plain-text files via `alt=media`. Both return non-JSON bodies,
so they use the shared client's `request_bytes`. Binary types are metadata-only.

Base URL is resolved via `lib.integrations.endpoints.endpoint("google_drive_api")`
so backfill can be pointed at a local spammer for tests — pure config.
"""
from __future__ import annotations

import io
import logging
from typing import Any

from services.ingest.integrations.gmail.client import GoogleHttpClient


log = logging.getLogger(__name__)


DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"

# Scope alias stored on google_drive_installations.scope -> long URL.
_SCOPE_ALIAS = {
    "drive.readonly": DRIVE_READONLY_SCOPE,
}

# files.list / changes.list page size. Drive caps at 1000.
_DEFAULT_PAGE_SIZE = 200

# Sentinel drive_id for a user's personal corpus (My Drive).
MY_DRIVE_SENTINEL = "my-drive"

# The metadata fields we request on every file (kept in sync between
# files.list and changes.list so the handler sees a uniform shape).
_FILE_FIELDS = (
    "id,name,mimeType,version,trashed,explicitlyTrashed,createdTime,"
    "modifiedTime,webViewLink,size,owners(emailAddress,displayName),"
    "lastModifyingUser(emailAddress,displayName),"
    "permissions(emailAddress,role,type),driveId,parents,shared,starred"
)

# mimeType -> export mimeType for Google-native docs.
_EXPORT_MIME = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.spreadsheet": "text/csv",
    "application/vnd.google-apps.presentation": "text/plain",
}

PDF_MIME = "application/pdf"
FOLDER_MIME = "application/vnd.google-apps.folder"


def resolve_scope(alias: str) -> str:
    """Map an install scope alias to the long Drive scope URL."""
    long_scope = _SCOPE_ALIAS.get(alias)
    if long_scope is None:
        raise ValueError(
            f"google_drive install carries unknown scope alias: {alias!r}",
        )
    return long_scope


def is_extractable(mime_type: str | None) -> bool:
    """True if we can extract text for this mimeType: a Google-native doc, a
    PDF, or a text/* file. Other binary types (images, video, archives) are
    metadata-only."""
    if not mime_type:
        return False
    return (
        mime_type in _EXPORT_MIME
        or mime_type == PDF_MIME
        or mime_type.startswith("text/")
    )


def _extract_pdf_text(raw: bytes, *, max_pages: int, max_bytes: int) -> str | None:
    """Extract text from PDF bytes with pypdf (pure-Python). Best-effort:
    encrypted/corrupt PDFs return None. Bounded by page + byte caps so a huge
    deck can't blow the CPU/observation budget."""
    from pypdf import PdfReader
    from pypdf.errors import PdfReadError

    try:
        reader = PdfReader(io.BytesIO(raw))
        if reader.is_encrypted:
            # Try the empty-password path; many "encrypted" PDFs use it.
            try:
                reader.decrypt("")
            except Exception:  # noqa: BLE001
                return None
        parts: list[str] = []
        total = 0
        for page in reader.pages[:max_pages]:
            text = page.extract_text() or ""
            if not text:
                continue
            parts.append(text)
            total += len(text)
            if total >= max_bytes:
                break
        joined = "\n".join(parts).strip()
        return joined[:max_bytes] or None
    except (PdfReadError, Exception) as exc:  # noqa: BLE001 — best-effort
        log.info("google_drive_pdf_extract_failed", extra={"error": str(exc)[:200]})
        return None


class GoogleDriveClient:
    """Operations against the Drive v3 REST API."""

    def __init__(
        self,
        http: GoogleHttpClient,
        *,
        scope: str = DRIVE_READONLY_SCOPE,
        base_url: str | None = None,
    ) -> None:
        from lib.integrations.endpoints import endpoint
        self._http = http
        self._scope = scope
        self._base = (base_url or endpoint("google_drive_api")).rstrip("/")

    async def list_shared_drives(
        self, *, user_email: str, page_token: str | None = None,
    ) -> dict[str, Any]:
        """`GET /drives?useDomainAdminAccess=true` impersonating an admin —
        enumerate the org's Shared Drives at onboarding. Returns the raw body
        (`drives`, `nextPageToken`)."""
        params: dict[str, Any] = {
            "useDomainAdminAccess": "true",
            "pageSize": 100,
            "fields": "drives(id,name),nextPageToken",
        }
        if page_token:
            params["pageToken"] = page_token
        return await self._http.request(
            "GET",
            f"{self._base}/drives",
            user_email=user_email,
            scopes=(self._scope,),
            params=params,
        )

    async def get_start_page_token(
        self, *, user_email: str, drive_id: str | None = None,
    ) -> str:
        """`GET /changes/startPageToken` — the warm-start token for incremental
        sync. For a Shared Drive pass its `drive_id`."""
        params: dict[str, Any] = {"supportsAllDrives": "true"}
        if drive_id and drive_id != MY_DRIVE_SENTINEL:
            params["driveId"] = drive_id
        body = await self._http.request(
            "GET",
            f"{self._base}/changes/startPageToken",
            user_email=user_email,
            scopes=(self._scope,),
            params=params,
        )
        token = body.get("startPageToken")
        if not isinstance(token, str) or not token:
            raise ValueError("drive changes.getStartPageToken returned no token")
        return token

    async def list_files(
        self,
        *,
        user_email: str,
        drive_id: str | None = None,
        modified_after: str | None = None,
        page_token: str | None = None,
        max_results: int = _DEFAULT_PAGE_SIZE,
    ) -> dict[str, Any]:
        """`GET /files` — FULL backfill walk, windowed by `modifiedTime`.

        My Drive: `corpora=user`. Shared Drive: `corpora=drive&driveId=…`
        with `includeItemsFromAllDrives`. Returns `{files, nextPageToken}`.
        """
        q_parts = ["trashed = false"]
        if modified_after:
            q_parts.append(f"modifiedTime > '{modified_after}'")
        params: dict[str, Any] = {
            "q": " and ".join(q_parts),
            "orderBy": "modifiedTime",
            "pageSize": max_results,
            "fields": f"files({_FILE_FIELDS}),nextPageToken",
            "supportsAllDrives": "true",
        }
        if drive_id and drive_id != MY_DRIVE_SENTINEL:
            params["corpora"] = "drive"
            params["driveId"] = drive_id
            params["includeItemsFromAllDrives"] = "true"
        else:
            params["corpora"] = "user"
        if page_token:
            params["pageToken"] = page_token
        return await self._http.request(
            "GET",
            f"{self._base}/files",
            user_email=user_email,
            scopes=(self._scope,),
            params=params,
        )

    async def list_changes(
        self,
        *,
        user_email: str,
        page_token: str,
        drive_id: str | None = None,
        max_results: int = _DEFAULT_PAGE_SIZE,
    ) -> dict[str, Any]:
        """`GET /changes?pageToken=…` — INCREMENTAL delta. `includeRemoved` so a
        trashed/removed file produces a change. Returns
        `{changes, nextPageToken, newStartPageToken}`. Each change carries
        `{fileId, removed, time, file?}`."""
        params: dict[str, Any] = {
            "pageToken": page_token,
            "includeRemoved": "true",
            "pageSize": max_results,
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
            "fields": (
                f"changes(fileId,removed,time,changeType,driveId,"
                f"file({_FILE_FIELDS})),nextPageToken,newStartPageToken"
            ),
        }
        if drive_id and drive_id != MY_DRIVE_SENTINEL:
            params["driveId"] = drive_id
        return await self._http.request(
            "GET",
            f"{self._base}/changes",
            user_email=user_email,
            scopes=(self._scope,),
            params=params,
        )

    async def export_text(
        self,
        *,
        user_email: str,
        file_id: str,
        mime_type: str,
        max_bytes: int,
        pdf_max_pages: int = 50,
    ) -> str | None:
        """Extract a file's text. Google-native docs via `files.export`, PDFs
        via `alt=media` + pypdf, text/* files via `alt=media`. Returns decoded
        text truncated to `max_bytes`, or None for non-extractable types.
        Best-effort: any error is swallowed by the caller (the metadata
        observation still lands)."""
        from urllib.parse import quote

        if mime_type in _EXPORT_MIME:
            params = {"mimeType": _EXPORT_MIME[mime_type]}
            url = f"{self._base}/files/{quote(file_id, safe='')}/export"
            decode_pdf = False
        elif mime_type == PDF_MIME or mime_type.startswith("text/"):
            params = {"alt": "media", "supportsAllDrives": "true"}
            url = f"{self._base}/files/{quote(file_id, safe='')}"
            decode_pdf = mime_type == PDF_MIME
        else:
            return None

        raw = await self._http.request_bytes(
            "GET", url, user_email=user_email, scopes=(self._scope,), params=params,
        )
        if not raw:
            return None
        if decode_pdf:
            return _extract_pdf_text(raw, max_pages=pdf_max_pages, max_bytes=max_bytes)
        return raw[:max_bytes].decode("utf-8", errors="replace")

    async def list_comments(
        self, *, user_email: str, file_id: str, page_token: str | None = None,
        page_size: int = 100,
    ) -> dict[str, Any]:
        """`GET /files/{fileId}/comments` — comments + nested replies. The
        Drive API requires an explicit `fields` selector. Author `emailAddress`
        is often omitted by Drive for privacy; `displayName` is the fallback."""
        from urllib.parse import quote

        params: dict[str, Any] = {
            "pageSize": page_size,
            "fields": (
                "comments(id,content,createdTime,modifiedTime,resolved,"
                "author(displayName,emailAddress),quotedFileContent(value),"
                "replies(id,content,createdTime,author(displayName,emailAddress))),"
                "nextPageToken"
            ),
        }
        if page_token:
            params["pageToken"] = page_token
        return await self._http.request(
            "GET",
            f"{self._base}/files/{quote(file_id, safe='')}/comments",
            user_email=user_email,
            scopes=(self._scope,),
            params=params,
        )

    async def list_revisions(
        self, *, user_email: str, file_id: str, page_token: str | None = None,
        page_size: int = 100,
    ) -> dict[str, Any]:
        """`GET /files/{fileId}/revisions` — the edit timeline (who saved
        when). Returns oldest-first."""
        from urllib.parse import quote

        params: dict[str, Any] = {
            "pageSize": page_size,
            "fields": (
                "revisions(id,modifiedTime,keepForever,published,size,"
                "lastModifyingUser(displayName,emailAddress)),nextPageToken"
            ),
        }
        if page_token:
            params["pageToken"] = page_token
        return await self._http.request(
            "GET",
            f"{self._base}/files/{quote(file_id, safe='')}/revisions",
            user_email=user_email,
            scopes=(self._scope,),
            params=params,
        )

    async def has_changes_since(
        self, *, user_email: str, page_token: str, drive_id: str | None = None,
    ) -> bool:
        """Reconciler gap probe (D2-adjacent): does `changes.list` from
        `page_token` yield any change? One cheap small-page query.
        `includeRemoved` so a trash also counts."""
        body = await self.list_changes(
            user_email=user_email,
            page_token=page_token,
            drive_id=drive_id,
            max_results=1,
        )
        changes = body.get("changes")
        return isinstance(changes, list) and len(changes) > 0

    async def watch_changes(
        self,
        *,
        user_email: str,
        page_token: str,
        channel_id: str,
        address: str,
        token: str,
        drive_id: str | None = None,
        ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        """`POST /changes/watch?pageToken=…` — open a push channel on the
        changes feed. Drive pushes a content-less ping to the `web_hook`
        `address` carrying `X-Goog-*` headers; the receiver verifies
        `X-Goog-Channel-Token == token` and drains the delta from the same
        `pageToken`. Returns the raw channel resource
        (`{id, resourceId, resourceUri, expiration}`)."""
        params: dict[str, Any] = {
            "pageToken": page_token,
            "supportsAllDrives": "true",
            "includeRemoved": "true",
        }
        if drive_id and drive_id != MY_DRIVE_SENTINEL:
            params["driveId"] = drive_id
        body: dict[str, Any] = {
            "id": channel_id,
            "type": "web_hook",
            "address": address,
            "token": token,
        }
        if ttl_seconds:
            body["params"] = {"ttl": str(ttl_seconds)}
        return await self._http.request(
            "POST",
            f"{self._base}/changes/watch",
            user_email=user_email,
            scopes=(self._scope,),
            params=params,
            json_body=body,
        )

    async def stop_channel(
        self, *, user_email: str, channel_id: str, resource_id: str,
    ) -> None:
        """`POST /channels/stop` — tear down a push channel (idempotent)."""
        await self._http.request(
            "POST",
            f"{self._base}/channels/stop",
            user_email=user_email,
            scopes=(self._scope,),
            json_body={"id": channel_id, "resourceId": resource_id},
        )


__all__ = [
    "DRIVE_READONLY_SCOPE",
    "FOLDER_MIME",
    "GoogleDriveClient",
    "MY_DRIVE_SENTINEL",
    "PDF_MIME",
    "is_extractable",
    "resolve_scope",
]
