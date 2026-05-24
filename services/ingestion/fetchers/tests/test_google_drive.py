"""Tests for services/ingestion/fetchers/google_drive.py (IN-16)."""
from __future__ import annotations

import pytest

from lib.shared.errors import CompanyOSError
from services.ingestion.fetchers import FETCHER_DISPATCH
from services.ingestion.fetchers import google_drive as gd
from services.ingestion.fetchers.google_drive import (
    SHARD_KIND_FILES,
    fetch_page_google_drive,
)
from services.ingestion.normalizer.channel_mapping import resolve_channel


pytestmark = pytest.mark.asyncio


class _FakeInst:
    def __getitem__(self, k):
        return "drive.readonly" if k == "scope" else "row"


def _file(fid, mime="application/vnd.google-apps.document", **over):
    base = {
        "id": fid, "name": fid, "mimeType": mime, "version": "1",
        "trashed": False, "modifiedTime": "2026-04-20T10:00:00.000Z",
        "owners": [{"emailAddress": "alice@acme.com"}],
    }
    base.update(over)
    return base


class _FakeDriveClient:
    """Deterministic fake covering full backfill + incremental + injections."""

    def __init__(self, *, rate_limit_always=False, expired_token=False):
        self.rate_limit_always = rate_limit_always
        self.expired_token = expired_token
        self.calls: list[str] = []

    async def get_start_page_token(self, **kw):
        self.calls.append("start_token")
        return "spt-captured"

    async def list_files(self, **kw):
        self.calls.append("files")
        from services.integrations.gmail.client import GoogleRateLimited
        if self.rate_limit_always:
            raise GoogleRateLimited("429", status=429)
        if kw.get("page_token") is None:
            return {"files": [_file("f1")], "nextPageToken": "pg-2"}
        return {"files": [_file("f2")]}  # terminal

    async def list_changes(self, **kw):
        self.calls.append("changes")
        if self.expired_token:
            raise CompanyOSError("410", status=410)
        return {
            "changes": [
                {"fileId": "f3", "removed": False, "time": "2026-04-21T00:00:00Z",
                 "file": _file("f3", version="2")},
                {"fileId": "f4", "removed": True, "time": "2026-04-21T01:00:00Z"},
            ],
            "newStartPageToken": "spt-next",
        }

    async def export_text(self, *, user_email, file_id, mime_type, max_bytes, pdf_max_pages=50):
        self.calls.append(f"export:{file_id}")
        return f"body of {file_id}"

    async def list_comments(self, *, user_email, file_id, page_token=None):
        self.calls.append(f"comments:{file_id}")
        return {"comments": [
            {"id": f"c-{file_id}", "content": "looks good",
             "author": {"displayName": "Reviewer", "emailAddress": "rev@acme.com"},
             "createdTime": "2026-04-21T00:00:00Z",
             "modifiedTime": "2026-04-21T00:00:00Z", "resolved": False},
        ]}

    async def list_revisions(self, *, user_email, file_id, page_token=None):
        self.calls.append(f"revisions:{file_id}")
        return {"revisions": [
            {"id": f"r-{file_id}", "modifiedTime": "2026-04-20T10:00:00Z",
             "lastModifyingUser": {"emailAddress": "bob@acme.com"}},
        ]}


def _patch(monkeypatch, fake):
    async def fake_open(install):
        async def close():
            return None
        return fake, close
    monkeypatch.setattr(gd, "_open_drive_client", fake_open)


def _shard(**over):
    base = {
        "shard_kind": SHARD_KIND_FILES,
        "drive_kind": "my_drive",
        "drive_id": "my-drive",
        "owner_email": "alice@acme.com",
        "installation_id": "inst-1",
    }
    base.update(over)
    return base


async def _drain(monkeypatch, fake, shard, cursor=None):
    _patch(monkeypatch, fake)
    records, guard = [], 0
    while True:
        guard += 1
        assert guard < 50, "fetch loop did not terminate"
        r = await fetch_page_google_drive(_FakeInst(), shard, cursor)
        records.extend(r.records)
        cursor = r.next_cursor
        if r.end_of_data:
            break
    return records, cursor


async def test_dispatch_registered():
    assert FETCHER_DISPATCH["google_drive"] is fetch_page_google_drive
    assert resolve_channel("google_drive", "backfill") == "google_drive:file"


def _by_type(records, t):
    return [r for r in records if r.get("_fyralis_record_type") == t]


async def test_full_backfill_pages_and_captures_start_token(monkeypatch):
    fake = _FakeDriveClient()
    records, cursor = await _drain(monkeypatch, fake, _shard())
    files = _by_type(records, "file")
    assert [r["id"] for r in files] == ["f1", "f2"]
    # start-page-token captured up-front for the future poll.
    assert cursor["next_start_page_token"] == "spt-captured"
    # extracted text injected by the fetcher on the file records.
    assert all(r.get("_fyralis_extracted_text") for r in files)
    assert all(r["_fyralis_drive_id"] == "my-drive" for r in files)
    # comment + revision records emitted per non-folder file.
    comments = _by_type(records, "comment")
    revisions = _by_type(records, "revision")
    assert {c["_fyralis_file_id"] for c in comments} == {"f1", "f2"}
    assert {r["_fyralis_file_id"] for r in revisions} == {"f1", "f2"}


async def test_incremental_mode_when_warm_started(monkeypatch):
    fake = _FakeDriveClient()
    records, cursor = await _drain(
        monkeypatch, fake, _shard(start_page_token="spt-warm"),
    )
    files = _by_type(records, "file")
    assert [r["id"] for r in files] == ["f3", "f4"]
    # removed change carries the removed flag, no extraction, no collab records.
    f4 = [r for r in files if r["id"] == "f4"][0]
    assert f4["_fyralis_removed"] is True
    f3 = [r for r in files if r["id"] == "f3"][0]
    assert f3["_fyralis_removed"] is False
    assert f3.get("_fyralis_extracted_text") == "body of f3"
    # collab records only for the non-removed file (f3), not the removed f4.
    assert {c["_fyralis_file_id"] for c in _by_type(records, "comment")} == {"f3"}
    assert {r["_fyralis_file_id"] for r in _by_type(records, "revision")} == {"f3"}
    assert cursor["next_start_page_token"] == "spt-next"
    assert "start_token" not in fake.calls  # no full-sync setup on warm start


async def test_expired_token_reseeds_full_sync(monkeypatch):
    fake = _FakeDriveClient(expired_token=True)
    _patch(monkeypatch, fake)
    r = await fetch_page_google_drive(
        _FakeInst(), _shard(start_page_token="spt-warm"), None,
    )
    # Reseed: empty page, not end-of-data, seeded reset so ShardFetch re-enters.
    assert r.records == []
    assert r.end_of_data is False
    assert r.next_cursor["seeded"] is False


async def test_rate_limit_preserves_cursor(monkeypatch):
    # Patch the retry helper to a no-retry passthrough so the test doesn't sit
    # through real backoff sleeps (same approach as the calendar fetcher test).
    async def _no_retry(fn, retry_on, **kw):
        return await fn()
    monkeypatch.setattr(gd, "retry_with_backoff_on_429", _no_retry)

    fake = _FakeDriveClient(rate_limit_always=True)
    _patch(monkeypatch, fake)
    # First call seeds (captures token) then hits a persistent 429 on list_files.
    r = await fetch_page_google_drive(_FakeInst(), _shard(), None)
    assert r.records == []
    assert r.end_of_data is False
