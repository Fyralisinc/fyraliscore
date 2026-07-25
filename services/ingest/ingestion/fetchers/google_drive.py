"""services/ingest/ingestion/fetchers/google_drive.py — Drive fetcher (IN-16).

Per A18 (per-source backfill = net-new code) + A16/N1 (cursor advanced by
ShardFetch, opaque to it) + A27.3 (records shaped for the handler).

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `google_drive_files` shard streams one drive's files (a user's My Drive or a
Shared Drive). ShardFetch calls this fetcher in a loop, persisting the cursor
between calls. Two modes share the cursor:

  - FULL (initial backfill): on the first seeded call we capture the incremental
    warm-start token via `changes.getStartPageToken` and stash it into the
    cursor IMMEDIATELY (so edits made *during* the backfill window are caught by
    the first poll — the canonical Drive ordering). Then `files.list` windowed
    by `modifiedTime > now-N days`, paged via `pageToken`.
  - INCREMENTAL (poll): when the cursor (or the shard, warm-started by the
    planner) carries a `start_page_token`, `changes.list?pageToken=…
    &includeRemoved=true` returns ONLY changed/removed files since the token.
    The last page returns `newStartPageToken` — the warm start for next run.

`end_of_data=True` when a page returns no `nextPageToken`.

============================================================
STALE PAGE TOKEN (HTTP 410/400 -> full reseed; Risk #1)
============================================================
An aged-out page token yields HTTP 410 (or 400 invalid). The fetcher catches
it, clears the token, switches to FULL mode, and returns an empty cursor-reset
page so ShardFetch re-enters and runs a fresh windowed full sync. Dedup makes
the re-walk idempotent.

============================================================
CONTENT EXTRACTION (D8) + HANDLER CONFORMANCE (A27.3)
============================================================
Each record is the RAW Drive v3 file object plus injected private keys:
`_fyralis_drive_id`, `_fyralis_owner_email`, `_fyralis_drive_kind`,
`_fyralis_removed` (bool), `_fyralis_change_time` (for removed changes), and
`_fyralis_extracted_text` (Doc/Sheet/Slide/text bodies exported here so the
handler stays a pure function). The `google_drive:file` handler derives a
VERSIONED `external_id = gdrive:{file_id}:{version}` so an edit lands a new
observation while identical re-fetches dedup.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import CompanyOSError
from services.ingest.ingestion.fetchers import FetchResult
from services.ingest.integrations.google_drive import metrics
from services.ingest.integrations.google_drive.client import (
    GoogleDriveClient,
    is_extractable,
    resolve_scope,
)
from lib.shared.provider_transport import ProviderTransportError


log = logging.getLogger(__name__)


SHARD_KIND_FILES = "google_drive_files"


def _backfill_days() -> int:
    """Windowed backfill horizon (env-overridable)."""
    try:
        return int(os.environ.get("GOOGLE_DRIVE_BACKFILL_DAYS", "180"))
    except ValueError:
        return 180


def _extract_max_bytes() -> int:
    """Cap on extracted text per file (env-overridable)."""
    try:
        return int(os.environ.get("GOOGLE_DRIVE_EXTRACT_MAX_BYTES", "524288"))
    except ValueError:
        return 524288


def _pdf_max_pages() -> int:
    try:
        return int(os.environ.get("GOOGLE_DRIVE_PDF_MAX_PAGES", "250"))
    except ValueError:
        return 250


def _max_extract_file_bytes() -> int:
    """Skip extraction for files larger than this (avoid downloading huge
    binaries just to extract text). Env-overridable; default 50 MB."""
    try:
        return int(os.environ.get("GOOGLE_DRIVE_MAX_EXTRACT_FILE_BYTES", str(50 * 1024 * 1024)))
    except ValueError:
        return 50 * 1024 * 1024


def _flag(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off", "")


class GoogleDriveCursor(BaseModel):
    """Cursor for one drive shard. Round-trips through the opaque dict in
    workflow_states.state_data per the M6.2a contract.

    - page_token            : Drive's nextPageToken within the current run.
    - start_page_token      : the ACTIVE incremental token (incremental mode
                              when set).
    - next_start_page_token : captured at the start of a full sync, or from the
                              last page of an incremental sync; the warm start
                              for the next run (D2) + the reconciler's gap
                              reference point.
    - time_min              : the windowed-backfill lower bound, frozen on first
                              call so paging stays stable across ticks.
    - files_seen            : diagnostic.
    - seeded                : whether the first-call setup has run.
    """

    model_config = ConfigDict(extra="forbid")

    page_token: str | None = None
    start_page_token: str | None = None
    next_start_page_token: str | None = None
    time_min: str | None = None
    files_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> GoogleDriveCursor:
    if c is None:
        return GoogleDriveCursor()
    return GoogleDriveCursor.model_validate(c)


def _encode_cursor(c: GoogleDriveCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


async def _open_drive_client(install: asyncpg.Record):  # noqa: ANN202
    """Test seam — monkeypatched by tests. Production builds a real
    GoogleDriveClient over the shared Gmail DWD minter + GoogleHttpClient."""
    from services.ingest.integrations.gmail.client import build_google_http_client
    from services.ingest.integrations.gmail.dwd import get_minter

    scope = resolve_scope(install["scope"])
    http = build_google_http_client(
        get_minter(),
        source="google_drive",
        tenant_id=str(install["tenant_id"]),
        installation_id=str(install["id"]),
    )
    await http.__aenter__()
    client = GoogleDriveClient(http, scope=scope)

    async def close() -> None:
        await http.__aexit__(None, None, None)

    return client, close


async def _maybe_extract(
    client: Any, *, owner_email: str, file: dict[str, Any],
) -> None:
    """Best-effort text extraction (Doc/Sheet/Slide/PDF/text); injects
    `_fyralis_extracted_text`. Any failure is logged + counted and leaves the
    metadata observation intact."""
    mime = file.get("mimeType")
    if not is_extractable(mime) or file.get("trashed"):
        file["_fyralis_text_yield"] = "none"
        file["_fyralis_needs_multimodal"] = bool(mime) and not is_extractable(mime)
        return
    file_id = file.get("id")
    if not isinstance(file_id, str) or not file_id:
        return
    # Size guard: don't download a huge binary just to extract text. `size` is
    # a string of bytes (absent for Google-native docs, which export cheaply).
    size_raw = file.get("size")
    if size_raw is not None:
        try:
            if int(size_raw) > _max_extract_file_bytes():
                file["_fyralis_text_yield"] = "skipped_size"
                file["_fyralis_needs_multimodal"] = True
                return
        except (TypeError, ValueError):
            pass
    try:
        text = await client.export_text(
            user_email=owner_email,
            file_id=file_id,
            mime_type=mime,
            max_bytes=_extract_max_bytes(),
            pdf_max_pages=_pdf_max_pages(),
        )
    except ProviderTransportError:
        # Child hydration is part of this page's correctness boundary. Preserve
        # transport outcomes so RetryLater is durably scheduled and binding /
        # policy defects fail closed instead of advancing past missing content.
        raise
    except Exception as exc:  # noqa: BLE001 — extraction is best-effort
        metrics.record_fetch_event("extract_failed")
        log.info(
            "google_drive_extract_failed",
            extra={"file_id": file_id, "error": str(exc)[:200]},
        )
        return
    if text:
        file["_fyralis_extracted_text"] = text
        file["_fyralis_text_yield"] = "text"
        file["_fyralis_needs_multimodal"] = False
        metrics.record_fetch_event("extracted")
    else:
        file["_fyralis_text_yield"] = "empty"
        file["_fyralis_needs_multimodal"] = bool(mime == "application/pdf")


async def _collab_records(
    client: Any, *, owner_email: str, drive_id: str, drive_kind: str,
    file: dict[str, Any],
) -> list[dict[str, Any]]:
    """Fetch a file's comments + revision history and return them as separate
    records (tagged `_fyralis_record_type`) so the handler emits distinct
    `google_drive:comment` / `google_drive:revision` observations. Gated by
    env; skipped for folders / removed files. Best-effort per call."""
    from services.ingest.integrations.google_drive.client import FOLDER_MIME

    file_id = file.get("id")
    if not isinstance(file_id, str) or not file_id:
        return []
    if file.get("mimeType") == FOLDER_MIME or file.get("trashed"):
        return []

    ctx = {
        "_fyralis_file_id": file_id,
        "_fyralis_file_name": file.get("name"),
        "_fyralis_owner_email": owner_email,
        "_fyralis_drive_id": drive_id,
        "_fyralis_drive_kind": drive_kind,
    }
    out: list[dict[str, Any]] = []

    if _flag("GOOGLE_DRIVE_FETCH_COMMENTS"):
        try:
            body = await client.list_comments(user_email=owner_email, file_id=file_id)
            for c in body.get("comments") or []:
                if isinstance(c, dict):
                    rec = dict(c)
                    rec.update(ctx)
                    rec["_fyralis_record_type"] = "comment"
                    out.append(rec)
            if out:
                metrics.record_fetch_event("comments", by=len(out))
        except ProviderTransportError:
            raise
        except Exception as exc:  # noqa: BLE001 — best-effort
            log.info("google_drive_comments_failed",
                     extra={"file_id": file_id, "error": str(exc)[:200]})

    if _flag("GOOGLE_DRIVE_FETCH_REVISIONS"):
        try:
            body = await client.list_revisions(user_email=owner_email, file_id=file_id)
            revs = body.get("revisions") or []
            for r in revs:
                if isinstance(r, dict):
                    rec = dict(r)
                    rec.update(ctx)
                    rec["_fyralis_record_type"] = "revision"
                    out.append(rec)
            if revs:
                metrics.record_fetch_event("revisions", by=len(revs))
        except ProviderTransportError:
            raise
        except Exception as exc:  # noqa: BLE001 — best-effort
            log.info("google_drive_revisions_failed",
                     extra={"file_id": file_id, "error": str(exc)[:200]})

    return out


def _stamp(
    file: dict[str, Any], *, drive_id: str, drive_kind: str, owner_email: str,
    removed: bool, change_time: str | None,
) -> dict[str, Any]:
    file["_fyralis_record_type"] = "file"
    file["_fyralis_drive_id"] = drive_id
    file["_fyralis_drive_kind"] = drive_kind
    file["_fyralis_owner_email"] = owner_email
    file["_fyralis_removed"] = removed
    if change_time is not None:
        file["_fyralis_change_time"] = change_time
    return file


async def fetch_page_google_drive(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of files + next cursor for a drive shard."""
    drive_id = shard_identifier.get("drive_id") or "my-drive"
    drive_kind = shard_identifier.get("drive_kind") or "my_drive"
    owner_email = shard_identifier.get("owner_email")
    if not isinstance(owner_email, str) or not owner_email:
        # Misconfigured shard — nothing to walk.
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    cur = _decode_cursor(cursor)
    client, close = await _open_drive_client(install)
    try:
        # First-call setup: choose the sync mode + freeze the backfill window.
        # For a FULL backfill, capture the start-page-token UP FRONT so changes
        # made during the backfill window aren't lost.
        if not cur.seeded:
            warm_token = shard_identifier.get("start_page_token")
            if isinstance(warm_token, str) and warm_token:
                cur.start_page_token = warm_token  # warm start -> incremental
            else:
                cur.time_min = (
                    datetime.now(timezone.utc) - timedelta(days=_backfill_days())
                ).isoformat().replace("+00:00", "Z")
                cur.next_start_page_token = await client.get_start_page_token(
                    user_email=owner_email,
                    drive_id=drive_id,
                )
            cur.seeded = True

        incremental = cur.start_page_token is not None

        try:
            if incremental:
                body = await client.list_changes(
                    user_email=owner_email,
                    page_token=cur.page_token or cur.start_page_token,
                    drive_id=drive_id,
                )
            else:
                body = await client.list_files(
                    user_email=owner_email,
                    drive_id=drive_id,
                    modified_after=cur.time_min,
                    page_token=cur.page_token,
                )
        except CompanyOSError as exc:
            status = (exc.context or {}).get("status")
            if status in (400, 410) and incremental:
                # Page token expired/invalid — reseed a windowed full sync.
                metrics.record_fetch_event("start_token_expired")
                log.info(
                    "google_drive_start_token_expired",
                    extra={"drive_id": drive_id},
                )
                reseed = GoogleDriveCursor(
                    seeded=False,
                    next_start_page_token=cur.next_start_page_token,
                )
                return FetchResult(
                    records=[], next_cursor=_encode_cursor(reseed),
                    end_of_data=False,
                )
            raise

        records: list[dict[str, Any]] = []
        if incremental:
            changes = body.get("changes")
            change_list = (
                [c for c in changes if isinstance(c, dict)]
                if isinstance(changes, list) else []
            )
            for ch in change_list:
                removed = bool(ch.get("removed"))
                file = ch.get("file")
                if isinstance(file, dict):
                    if file.get("trashed"):
                        removed = True
                    await _maybe_extract(client, owner_email=owner_email, file=file)
                else:
                    # A removed/lost-access change carries only fileId.
                    file = {"id": ch.get("fileId")}
                    removed = True
                _stamp(
                    file, drive_id=drive_id, drive_kind=drive_kind,
                    owner_email=owner_email, removed=removed,
                    change_time=ch.get("time"),
                )
                records.append(file)
                if not removed:
                    records.extend(await _collab_records(
                        client, owner_email=owner_email, drive_id=drive_id,
                        drive_kind=drive_kind, file=file,
                    ))
            next_page_token = body.get("nextPageToken")
            new_start = body.get("newStartPageToken")
            if isinstance(new_start, str) and new_start:
                cur.next_start_page_token = new_start
            # Within an incremental walk, advance the page token via the cursor.
            # `start_page_token` stays set (the incremental-mode marker); the
            # next tick resumes from `page_token` until the walk finishes.
            cur.page_token = next_page_token if next_page_token else None
        else:
            files = body.get("files")
            file_list = (
                [f for f in files if isinstance(f, dict)]
                if isinstance(files, list) else []
            )
            for file in file_list:
                await _maybe_extract(client, owner_email=owner_email, file=file)
                _stamp(
                    file, drive_id=drive_id, drive_kind=drive_kind,
                    owner_email=owner_email, removed=bool(file.get("trashed")),
                    change_time=None,
                )
                records.append(file)
                if not file.get("trashed"):
                    records.extend(await _collab_records(
                        client, owner_email=owner_email, drive_id=drive_id,
                        drive_kind=drive_kind, file=file,
                    ))
            next_page_token = body.get("nextPageToken")
            cur.page_token = next_page_token if next_page_token else None

        is_last_page = not next_page_token
        cur.files_seen += len(records)
        if records:
            metrics.record_fetch_event("files", by=len(records))

        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=is_last_page,
        )
    finally:
        await close()




__all__ = [
    "SHARD_KIND_FILES",
    "GoogleDriveCursor",
    "fetch_page_google_drive",
]
