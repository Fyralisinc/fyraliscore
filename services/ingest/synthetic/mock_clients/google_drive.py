"""MockGoogleDriveClient — Drive v3 surface used by IN-16 backfill.

Implements only the methods the REAL `google_drive` fetcher + reconciler call:
  - get_start_page_token(user_email, drive_id) -> str
  - list_files(user_email, drive_id, modified_after, page_token, max_results)
        -> {"files": [...], "nextPageToken": str|None}
  - list_changes(user_email, page_token, drive_id, max_results)
        -> {"changes": [...], "nextPageToken": ..., "newStartPageToken": ...}
  - export_text(user_email, file_id, mime_type, max_bytes, pdf_max_pages)
        -> str|None
  - list_comments(user_email, file_id, page_token, page_size)
        -> {"comments": [...], "nextPageToken": ...}
  - list_revisions(user_email, file_id, page_token, page_size)
        -> {"revisions": [...], "nextPageToken": ...}
  - has_changes_since(user_email, page_token, drive_id) -> bool

Stateful over a fixture from `services.ingest.synthetic.fixtures.
google_drive_generator.make_google_drive`. Returns dicts with the literal Drive
v3 field names (`files`, `changes`, `nextPageToken`, `newStartPageToken`) so the
REAL fetcher code path runs exactly as it would against Google. Injected at the
`_open_drive_client` seam by the harness (so the DWD token mint + httpx layer
are bypassed in mock mode — the harness tests the M6 chain, not Google's
transport).

Each public method calls `self._check_fault()` first so a FaultProfile surfaces
the SHARED Google error types (`GoogleRateLimited` / `GoogleApiError`) the real
client raises.
"""
from __future__ import annotations

from typing import Any, NoReturn

from services.ingest.integrations.gmail.client import (
    GoogleApiError, GoogleRateLimited,
)
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


# Mirror the production client's sentinel for a user's personal corpus so a
# shard with drive_id="my-drive" resolves to the My Drive target.
_MY_DRIVE_SENTINEL = "my-drive"


class MockGoogleDriveClient(_MockBase):
    """Stateful in-process replacement for `GoogleDriveClient`."""

    def __init__(
        self,
        *,
        fixture: dict[str, Any],
        profile: FaultProfile = HAPPY_PATH,
    ) -> None:
        super().__init__(profile=profile)
        self._fixture = fixture
        self._page_size = int(fixture.get("page_size", 200))
        # Index targets by drive_id so a shard's drive_id selects the corpus.
        self._by_drive: dict[str, dict[str, Any]] = {
            t["drive_id"]: t for t in fixture.get("targets", [])
        }

    def _target(self, drive_id: str | None) -> dict[str, Any]:
        key = drive_id or _MY_DRIVE_SENTINEL
        tgt = self._by_drive.get(key)
        if tgt is None:
            # Unknown drive -> empty corpus (mirrors a drive with no files).
            return {
                "files": [], "changes": [], "comments": {}, "revisions": {},
                "extracted_text": {}, "start_page_token": f"start-{key}",
            }
        return tgt

    # ---- Drive v3 surface ----
    async def get_start_page_token(
        self, *, user_email: str, drive_id: str | None = None,
    ) -> str:
        self._check_fault()
        return self._target(drive_id).get("start_page_token", "start-token")

    async def list_files(
        self,
        *,
        user_email: str,
        drive_id: str | None = None,
        modified_after: str | None = None,
        page_token: str | None = None,
        max_results: int = 200,
    ) -> dict[str, Any]:
        self._check_fault()
        tgt = self._target(drive_id)
        files = list(tgt.get("files", []))
        if modified_after is not None:
            files = [
                f for f in files
                if str(f.get("modifiedTime", "")) > modified_after
            ]
        page_size = min(self._page_size, max_results)
        start = int(page_token) if page_token else 0
        end = start + page_size
        page = files[start:end]
        result: dict[str, Any] = {"files": page}
        if end < len(files):
            result["nextPageToken"] = str(end)
        return result

    async def list_changes(
        self,
        *,
        user_email: str,
        page_token: str,
        drive_id: str | None = None,
        max_results: int = 200,
    ) -> dict[str, Any]:
        self._check_fault()
        tgt = self._target(drive_id)
        changes = list(tgt.get("changes", []))
        page_size = min(self._page_size, max_results)
        # The incremental walk starts from the warm-start token, then advances
        # via integer offsets encoded in `page_token`.
        start_token = tgt.get("start_page_token", "start-token")
        start = 0 if page_token == start_token else (
            int(page_token) if page_token and page_token.isdigit() else 0
        )
        end = start + page_size
        page = changes[start:end]
        result: dict[str, Any] = {"changes": page}
        if end < len(changes):
            result["nextPageToken"] = str(end)
        else:
            # Terminal page carries the warm start for the next poll.
            result["newStartPageToken"] = f"{start_token}-next"
        return result

    async def export_text(
        self,
        *,
        user_email: str,
        file_id: str,
        mime_type: str,
        max_bytes: int,
        pdf_max_pages: int = 50,
    ) -> str | None:
        self._check_fault()
        # Search every target for the file's extracted body.
        for tgt in self._by_drive.values():
            text = tgt.get("extracted_text", {}).get(file_id)
            if text is not None:
                return text[:max_bytes]
        return None

    async def list_comments(
        self, *, user_email: str, file_id: str, page_token: str | None = None,
        page_size: int = 100,
    ) -> dict[str, Any]:
        self._check_fault()
        for tgt in self._by_drive.values():
            comments = tgt.get("comments", {}).get(file_id)
            if comments is not None:
                return {"comments": list(comments)}
        return {"comments": []}

    async def list_revisions(
        self, *, user_email: str, file_id: str, page_token: str | None = None,
        page_size: int = 100,
    ) -> dict[str, Any]:
        self._check_fault()
        for tgt in self._by_drive.values():
            revisions = tgt.get("revisions", {}).get(file_id)
            if revisions is not None:
                return {"revisions": list(revisions)}
        return {"revisions": []}

    async def has_changes_since(
        self, *, user_email: str, page_token: str, drive_id: str | None = None,
    ) -> bool:
        self._check_fault()
        return len(self._target(drive_id).get("changes", [])) > 0

    # ---- Fault raisers (reuse the shared Google error types) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise GoogleRateLimited("MockGoogleDriveClient: rate limit (X2 fault)")

    def _raise_5xx(self) -> NoReturn:
        raise GoogleApiError("MockGoogleDriveClient: 503 (X2 fault)")

    def _raise_auth_error(self) -> NoReturn:
        raise GoogleApiError("MockGoogleDriveClient: 401 (X2 fault)")

    def _raise_transient(self) -> NoReturn:
        raise GoogleApiError("MockGoogleDriveClient: transient (X2 fault)")


__all__ = ["MockGoogleDriveClient"]
