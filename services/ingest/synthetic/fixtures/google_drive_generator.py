"""Deterministic Google Drive fixtures for the X2/X3 harness (IN-16).

`make_google_drive(...)` builds a fixture the `MockGoogleDriveClient` serves and
the X3 harness's install-seeding reads. It models one or more drive *targets*
(a user's My Drive or a Shared Drive) — each becomes one
`google_drive_files` shard at plan time.

Output shape (everything `MockGoogleDriveClient` consumes):

    {
      "targets": [
        {
          "owner_email": "alice@acme.example",
          "drive_id": "my-drive",          # MY_DRIVE_SENTINEL for a personal corpus
          "drive_kind": "my_drive",        # or "shared_drive"
          "start_page_token": "drive-start-1",  # warm-start token this drive serves
          "files": [<Drive v3 file objects>],   # FULL-backfill corpus (files.list)
          "changes": [<Drive v3 change objects>],  # INCREMENTAL deltas (changes.list)
          "comments": {file_id: [<comment objects>]},   # per-file comment threads
          "revisions": {file_id: [<revision objects>]},  # per-file edit timeline
          "extracted_text": {file_id: "..."},   # export_text body per file
        },
        ...
      ],
      "page_size": 200,
    }

Files / comments / revisions are RAW Drive v3 objects (the same shape the real
API returns), so the REAL fetcher + handler code is exercised exactly. Same
input params -> identical fixture (no randomness; ids/timestamps are derived
deterministically from the index).

By DEFAULT a file fans out to EXACTLY ONE record (comments_per_file=0,
revisions_per_file=0) so observation counts are trivially `files_per_target`.
Set `comments_per_file>0` / `revisions_per_file>0` to exercise the fetcher's
`_collab_records` fan-out (gated in production by GOOGLE_DRIVE_FETCH_COMMENTS /
GOOGLE_DRIVE_FETCH_REVISIONS — both default-on).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


# A Google-native Doc so `is_extractable` is True and the file is NOT a folder
# (folders are skipped by `_collab_records`), keeping the fan-out predictable.
_DOC_MIME = "application/vnd.google-apps.document"


def _iso(base: datetime, minutes: int) -> str:
    dt = base + timedelta(minutes=minutes)
    return dt.isoformat().replace("+00:00", "Z")


def _file(
    *, owner_email: str, drive_id: str, idx: int, base: datetime,
) -> dict[str, Any]:
    """One raw Drive v3 file object. `version` is a monotonic counter so the
    handler's versioned external_id is stable + unique per file."""
    file_id = f"file-{drive_id}-{idx}"
    created = _iso(base, idx)
    modified = _iso(base, idx + 1)
    return {
        "id": file_id,
        "name": f"Doc {idx} ({drive_id})",
        "mimeType": _DOC_MIME,
        # Monotonic version string -> deterministic gdrive:{file_id}:{version}.
        "version": str(100 + idx),
        "trashed": False,
        "explicitlyTrashed": False,
        "createdTime": created,
        "modifiedTime": modified,
        "webViewLink": f"https://docs.google.com/document/d/{file_id}",
        "owners": [{"emailAddress": owner_email, "displayName": owner_email}],
        "lastModifyingUser": {
            "emailAddress": owner_email, "displayName": owner_email,
        },
        "driveId": None if drive_id == "my-drive" else drive_id,
        "parents": [drive_id],
        "shared": False,
        "starred": False,
    }


def _comment(*, file_id: str, owner_email: str, idx: int, base: datetime) -> dict[str, Any]:
    return {
        "id": f"comment-{file_id}-{idx}",
        "content": f"Comment {idx} on {file_id}",
        "createdTime": _iso(base, idx),
        "modifiedTime": _iso(base, idx),
        "resolved": False,
        "author": {"displayName": owner_email, "emailAddress": owner_email},
        "replies": [],
    }


def _revision(*, file_id: str, owner_email: str, idx: int, base: datetime) -> dict[str, Any]:
    return {
        "id": f"revision-{file_id}-{idx}",
        "modifiedTime": _iso(base, idx),
        "keepForever": False,
        "published": False,
        "size": str(1024 * (idx + 1)),
        "lastModifyingUser": {"displayName": owner_email, "emailAddress": owner_email},
    }


def _target(
    *,
    owner_email: str,
    drive_id: str,
    drive_kind: str,
    files_per_target: int,
    comments_per_file: int,
    revisions_per_file: int,
    base: datetime,
) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    comments: dict[str, list[dict[str, Any]]] = {}
    revisions: dict[str, list[dict[str, Any]]] = {}
    extracted: dict[str, str] = {}

    for i in range(files_per_target):
        f = _file(owner_email=owner_email, drive_id=drive_id, idx=i, base=base)
        files.append(f)
        fid = f["id"]
        extracted[fid] = f"Extracted body of {f['name']}"
        comments[fid] = [
            _comment(file_id=fid, owner_email=owner_email, idx=c, base=base)
            for c in range(comments_per_file)
        ]
        revisions[fid] = [
            _revision(file_id=fid, owner_email=owner_email, idx=r, base=base)
            for r in range(revisions_per_file)
        ]

    return {
        "owner_email": owner_email,
        "drive_id": drive_id,
        "drive_kind": drive_kind,
        "start_page_token": f"start-{drive_id}",
        "files": files,
        # Clean-backfill scenario: no incremental deltas by default.
        "changes": [],
        "comments": comments,
        "revisions": revisions,
        "extracted_text": extracted,
    }


def make_google_drive(
    *,
    targets: list[dict[str, Any]] | None = None,
    files_per_target: int = 3,
    comments_per_file: int = 0,
    revisions_per_file: int = 0,
    page_size: int = 200,
    base_iso: str = "2026-01-05T00:00:00Z",
) -> dict[str, Any]:
    """Build a Drive backfill fixture.

    Args:
      targets: list of `{owner_email, drive_id?, drive_kind?}` dicts — one per
        drive shard. Defaults to a single My Drive target for
        `alice@acme.example`.
      files_per_target: files seeded per drive (the FULL-backfill corpus).
      comments_per_file: comment threads attached to each non-folder file
        (fan-out; default 0 so a file -> exactly 1 record).
      revisions_per_file: revisions attached to each non-folder file (fan-out;
        default 0).
      page_size: how many files `list_files` / changes `list_changes` return
        per page.
      base_iso: ISO-Z anchor for all generated timestamps (kept in 2026-01 so
        occurred_at lands inside the observations partition window).

    Returns:
      Fixture dict consumable by `MockGoogleDriveClient(fixture=...)`.
    """
    base = datetime.fromisoformat(base_iso.replace("Z", "+00:00"))
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)

    if targets is None:
        targets = [{"owner_email": "alice@acme.example"}]

    built: list[dict[str, Any]] = []
    for t in targets:
        owner_email = t.get("owner_email")
        if not isinstance(owner_email, str) or not owner_email:
            raise ValueError("make_google_drive target needs an owner_email")
        drive_id = t.get("drive_id") or "my-drive"
        drive_kind = t.get("drive_kind") or (
            "my_drive" if drive_id == "my-drive" else "shared_drive"
        )
        built.append(_target(
            owner_email=owner_email,
            drive_id=drive_id,
            drive_kind=drive_kind,
            files_per_target=files_per_target,
            comments_per_file=comments_per_file,
            revisions_per_file=revisions_per_file,
            base=base,
        ))

    return {"targets": built, "page_size": page_size}


__all__ = ["make_google_drive"]
