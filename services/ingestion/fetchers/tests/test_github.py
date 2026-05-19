"""Tests for services/ingestion/fetchers/github.py (M6.4)."""
from __future__ import annotations

import pytest

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingestion.fetchers import github as gh_fetcher
from services.ingestion.fetchers.github import (
    GithubCursor,
    SHARD_KIND_REPO_EVENTS,
    fetch_page_github,
)


pytestmark = pytest.mark.asyncio


class _FakeGithubClient:
    """Fake GithubClient surface for the fetcher's seam."""

    def __init__(self, pages, etag="W/etag-1", final_etag="W/etag-2"):
        self.pages = list(pages)
        self.etag = etag
        self.final_etag = final_etag
        self.calls = 0

    async def list_repo_events(
        self, *, owner, repo, event_type, page, per_page, etag,
    ):
        self.calls += 1
        if self.calls > len(self.pages):
            return [], self.final_etag, None
        page_records = self.pages[self.calls - 1]
        next_page = (self.calls + 1) if self.calls < len(self.pages) else None
        return page_records, self.etag, next_page


class _FakeInstall:
    def __init__(self):
        self._fields = {
            "id": "instrow", "tenant_id": "t", "provider": "github",
            "installation_id": "42", "enabled": True,
        }

    def __getitem__(self, k):
        return self._fields[k]


def _patch_client(monkeypatch, fake):
    async def fake_open(install):
        async def close(): return None
        return fake, close
    monkeypatch.setattr(gh_fetcher, "_open_github_client", fake_open)


async def test_first_page_advances_cursor(monkeypatch):
    fake = _FakeGithubClient(pages=[
        [{"id": 1, "updated_at": "2025-01-01T00:00:00Z"},
         {"id": 2, "updated_at": "2025-01-02T00:00:00Z"}],
    ])
    _patch_client(monkeypatch, fake)
    result = await fetch_page_github(
        install=_FakeInstall(),
        shard_identifier={
            "shard_kind": SHARD_KIND_REPO_EVENTS,
            "owner": "acme", "repo": "api",
            "event_type": "issues",
            "installation_id": "42",
            "repo_full_name": "acme/api",
        },
        cursor=None,
    )
    assert isinstance(result, FetchResult)
    assert len(result.records) == 2
    assert result.end_of_data is True  # only 1 page < per_page = end
    assert result.next_cursor["etag"] == "W/etag-1"
    assert result.next_cursor["last_seen_updated_at"] == "2025-01-02T00:00:00Z"


async def test_multi_page_paginates(monkeypatch):
    fake = _FakeGithubClient(pages=[
        [{"id": i, "updated_at": f"2025-01-{i:02d}T00:00:00Z"}
         for i in range(1, 101)],  # 100 records exactly = per_page → continue
        [{"id": 999, "updated_at": "2025-02-01T00:00:00Z"}],
    ])
    _patch_client(monkeypatch, fake)
    # First page:
    r1 = await fetch_page_github(
        _FakeInstall(),
        {"event_type": "issues", "owner": "a", "repo": "b",
         "installation_id": "42"},
        cursor=None,
    )
    assert len(r1.records) == 100
    assert r1.end_of_data is False
    # Second page (uses next_cursor):
    r2 = await fetch_page_github(
        _FakeInstall(),
        {"event_type": "issues", "owner": "a", "repo": "b",
         "installation_id": "42"},
        cursor=r1.next_cursor,
    )
    assert len(r2.records) == 1
    assert r2.end_of_data is True


async def test_record_envelope_shape(monkeypatch):
    fake = _FakeGithubClient(pages=[
        [{"id": 1, "title": "Bug", "updated_at": "2025-01-01T00:00:00Z"}],
    ])
    _patch_client(monkeypatch, fake)
    result = await fetch_page_github(
        _FakeInstall(),
        {"event_type": "pull_requests", "owner": "a", "repo": "b",
         "installation_id": "42", "repo_full_name": "a/b"},
        cursor=None,
    )
    rec = result.records[0]
    assert set(rec.keys()) == {
        "event_type", "repo_full_name", "installation_id",
        "payload", "read_path",
    }
    assert rec["read_path"] == "backfill"
    assert rec["event_type"] == "pull_requests"
    assert rec["payload"]["title"] == "Bug"


async def test_unknown_event_type_raises():
    with pytest.raises(ValueError, match="unknown event_type"):
        await fetch_page_github(
            _FakeInstall(),
            {"event_type": "bogus", "owner": "a", "repo": "b"},
            cursor=None,
        )


async def test_cursor_strict_pydantic():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        GithubCursor.model_validate({"page": 1, "extra_field": True})


async def test_dispatch_wired():
    assert FETCHER_DISPATCH["github"] is fetch_page_github
