"""Tests for services/ingest/ingestion/fetchers/github.py (M6.4)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingest.ingestion.fetchers import github as gh_fetcher
from services.ingest.ingestion.fetchers.github import (
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


async def test_issues_stream_filters_out_pull_requests(monkeypatch):
    # GitHub's /issues list returns PRs too (each with a `pull_request` key);
    # they must NOT become `issues` observations (the dedicated pull_requests
    # shard already covers them, and their issue node_id won't dedup against
    # the PR node_id). The cursor must still advance over the PR's timestamp.
    fake = _FakeGithubClient(pages=[
        [{"id": 1, "node_id": "Issue_1", "updated_at": "2025-01-01T00:00:00Z"},
         {"id": 2, "node_id": "Issue_2", "updated_at": "2025-01-03T00:00:00Z",
          "pull_request": {"url": "https://api.github.com/.../pulls/2"}}],
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
    # Only the real issue produced a record; the PR was dropped.
    assert len(result.records) == 1
    # ...but the cursor still advanced past the PR's (newer) timestamp.
    assert result.next_cursor["last_seen_updated_at"] == "2025-01-03T00:00:00Z"


async def test_pull_requests_stream_keeps_all_items(monkeypatch):
    # On the pull_requests shard the filter is scoped to event_type=="issues",
    # so nothing is dropped there.
    fake = _FakeGithubClient(pages=[
        [{"id": 1, "node_id": "PullRequest_1", "updated_at": "2025-01-01T00:00:00Z"},
         {"id": 2, "node_id": "PullRequest_2", "updated_at": "2025-01-02T00:00:00Z"}],
    ])
    _patch_client(monkeypatch, fake)
    result = await fetch_page_github(
        install=_FakeInstall(),
        shard_identifier={
            "shard_kind": SHARD_KIND_REPO_EVENTS,
            "owner": "acme", "repo": "api",
            "event_type": "pull_requests",
            "installation_id": "42",
            "repo_full_name": "acme/api",
        },
        cursor=None,
    )
    assert len(result.records) == 2


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
    """A27.3 — records are emitted in the github:webhook event-body
    shape (`{action, pull_request|issue, repository, sender}`) plus a
    reserved `webhook_metadata` carrying X-GitHub-Event. The bare REST
    item is preserved verbatim under the pull_request/issue key so the
    handler derives the same node_id-based external_id."""
    fake = _FakeGithubClient(pages=[
        [{"id": 1, "node_id": "PR_node", "title": "Bug", "state": "open",
          "user": {"login": "octocat"},
          "updated_at": "2025-01-01T00:00:00Z"}],
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
        "action", "pull_request", "repository", "sender",
        "webhook_metadata",
    }
    assert rec["webhook_metadata"] == {"X-GitHub-Event": "pull_request"}
    assert rec["pull_request"]["title"] == "Bug"
    assert rec["pull_request"]["node_id"] == "PR_node"
    assert rec["repository"]["full_name"] == "a/b"
    assert rec["sender"]["login"] == "octocat"
    assert rec["action"] == "opened"


async def test_issue_comments_reshape(monkeypatch):
    """issue_comments → issue_comment webhook body. external_id parity =
    comment.node_id; the parent issue number is parsed from issue_url
    (the repo-level endpoint omits the issue object)."""
    fake = _FakeGithubClient(pages=[
        [{"id": 5, "node_id": "IC_node", "body": "looks good",
          "user": {"login": "octocat"},
          "issue_url": "https://api.github.com/repos/a/b/issues/42",
          "updated_at": "2025-01-01T00:00:00Z"}],
    ])
    _patch_client(monkeypatch, fake)
    result = await fetch_page_github(
        _FakeInstall(),
        {"event_type": "issue_comments", "owner": "a", "repo": "b",
         "installation_id": "42", "repo_full_name": "a/b"},
        cursor=None,
    )
    rec = result.records[0]
    assert set(rec.keys()) == {
        "action", "comment", "issue", "repository", "sender",
        "webhook_metadata",
    }
    assert rec["webhook_metadata"] == {"X-GitHub-Event": "issue_comment"}
    assert rec["comment"]["node_id"] == "IC_node"
    assert rec["issue"]["number"] == 42
    assert rec["sender"]["login"] == "octocat"


async def test_commits_reshape(monkeypatch):
    """commits → single-commit push body. external_id parity = {repo}@{sha}
    (after=sha); head_commit.timestamp carries the real author date so the
    backfilled observation keeps its true occurred_at."""
    fake = _FakeGithubClient(pages=[
        [{"sha": "abc123", "node_id": "C_node",
          "commit": {"author": {"date": "2025-01-01T00:00:00Z"},
                     "message": "fix the bug"},
          "author": {"login": "octocat"}}],
    ])
    _patch_client(monkeypatch, fake)
    result = await fetch_page_github(
        _FakeInstall(),
        {"event_type": "commits", "owner": "a", "repo": "b",
         "installation_id": "42", "repo_full_name": "a/b"},
        cursor=None,
    )
    rec = result.records[0]
    assert set(rec.keys()) == {
        "ref", "after", "commits", "head_commit", "repository", "sender",
        "webhook_metadata",
    }
    assert rec["webhook_metadata"] == {"X-GitHub-Event": "push"}
    assert rec["after"] == "abc123"
    assert rec["head_commit"]["timestamp"] == "2025-01-01T00:00:00Z"
    assert rec["sender"]["login"] == "octocat"
    # last_seen advances off the commit author date (not updated_at).
    assert result.next_cursor["last_seen_updated_at"] == "2025-01-01T00:00:00Z"


async def test_commit_reshape_flows_through_push_handler(monkeypatch):
    """End-to-end parity: the reshaped commit record, fed to the live
    push shaper, yields external_id={repo}@{sha} and the real commit time
    — so a backfilled commit dedups with its live push twin."""
    from services.ingest.ingestion.handlers.github import _EVENT_SHAPERS

    fake = _FakeGithubClient(pages=[
        [{"sha": "deadbeef", "node_id": "C_node",
          "commit": {"author": {"date": "2024-06-01T12:00:00Z"},
                     "message": "ship it"},
          "author": {"login": "octocat"}}],
    ])
    _patch_client(monkeypatch, fake)
    result = await fetch_page_github(
        _FakeInstall(),
        {"event_type": "commits", "owner": "a", "repo": "b",
         "installation_id": "42", "repo_full_name": "a/b"},
        cursor=None,
    )
    draft = _EVENT_SHAPERS["push"](result.records[0])
    assert draft.external_id == "a/b@deadbeef"
    assert draft.occurred_at.year == 2024 and draft.occurred_at.month == 6


class _FakeFanoutClient:
    """PR enumeration + per-parent child paging for the fan-out fetcher."""

    def __init__(self, prs, *, review_pages=None, check_pages=None):
        self.prs = prs
        self.review_pages = review_pages or {}   # {pr_number: [[p1], [p2]]}
        self.check_pages = check_pages or {}      # {sha: [[p1], ...]}

    async def list_repo_events(
        self, *, owner, repo, event_type, page, per_page, etag,
    ):
        assert event_type == "pull_requests"  # parents are always PRs
        return (self.prs, "etag", None) if page == 1 else ([], "etag", None)

    @staticmethod
    def _page(pages, page):
        idx = page - 1
        if idx >= len(pages):
            return [], "etag", None
        nxt = page + 1 if (idx + 1) < len(pages) else None
        return pages[idx], "etag", nxt

    async def list_pr_reviews(
        self, *, owner, repo, pull_number, page, per_page, etag,
    ):
        return self._page(self.review_pages.get(pull_number, []), page)

    async def list_check_runs(self, *, owner, repo, ref, page, per_page, etag):
        return self._page(self.check_pages.get(ref, []), page)


async def _drain_fanout(monkeypatch, fake, shard_identifier):
    """Drive fetch_page_github call-by-call (round-tripping the opaque
    cursor) until end_of_data, accumulating records — the N1 loop."""
    _patch_client(monkeypatch, fake)
    cursor, records = None, []
    for _ in range(50):  # safety bound
        r = await fetch_page_github(_FakeInstall(), shard_identifier, cursor)
        records.extend(r.records)
        cursor = r.next_cursor
        if r.end_of_data:
            break
    else:  # pragma: no cover
        raise AssertionError("fan-out did not terminate")
    return records


async def test_pr_reviews_fanout_parity(monkeypatch):
    """pr_reviews enumerates PRs then drains each PR's reviews; the reshape
    flows through the live pull_request_review shaper to review.node_id."""
    from services.ingest.ingestion.handlers.github import _EVENT_SHAPERS

    prs = [{"number": 1, "node_id": "PR_1"}, {"number": 2, "node_id": "PR_2"}]
    review_pages = {
        1: [[{"node_id": "PRR_1", "state": "approved",
              "user": {"login": "rev"}, "submitted_at": "2025-01-01T00:00:00Z"}]],
        2: [[{"node_id": "PRR_2", "state": "commented",
              "user": {"login": "rev"}, "submitted_at": "2025-01-02T00:00:00Z"}]],
    }
    fake = _FakeFanoutClient(prs, review_pages=review_pages)
    records = await _drain_fanout(
        monkeypatch, fake,
        {"event_type": "pr_reviews", "owner": "a", "repo": "b",
         "installation_id": "42", "repo_full_name": "a/b"},
    )
    assert len(records) == 2
    rec = records[0]
    assert set(rec.keys()) == {
        "action", "review", "pull_request", "repository", "sender",
        "webhook_metadata",
    }
    assert rec["webhook_metadata"] == {"X-GitHub-Event": "pull_request_review"}
    assert rec["pull_request"]["number"] == 1
    draft = _EVENT_SHAPERS["pull_request_review"](rec)
    assert draft.external_id == "PRR_1"


async def test_pr_reviews_fanout_resumes_inner_pages(monkeypatch):
    """A single PR with two review pages is drained across calls — the inner
    child_page advances and resumes purely from the cursor dict."""
    prs = [{"number": 7, "node_id": "PR_7"}]
    review_pages = {7: [
        [{"node_id": "R_a", "user": {"login": "x"}}],
        [{"node_id": "R_b", "user": {"login": "x"}}],
    ]}
    fake = _FakeFanoutClient(prs, review_pages=review_pages)
    records = await _drain_fanout(
        monkeypatch, fake,
        {"event_type": "pr_reviews", "owner": "a", "repo": "b",
         "installation_id": "42", "repo_full_name": "a/b"},
    )
    assert [r["review"]["node_id"] for r in records] == ["R_a", "R_b"]


async def test_check_runs_fanout_parity(monkeypatch):
    """check_runs fans out over PR head SHAs; reshape flows through the live
    check_run shaper to check.node_id."""
    from services.ingest.ingestion.handlers.github import _EVENT_SHAPERS

    prs = [{"number": 1, "node_id": "PR_1", "head": {"sha": "sha1"}}]
    check_pages = {"sha1": [[{"node_id": "CR_1", "name": "ci",
                              "status": "completed", "conclusion": "success",
                              "head_sha": "sha1",
                              "completed_at": "2025-01-01T00:00:00Z"}]]}
    fake = _FakeFanoutClient(prs, check_pages=check_pages)
    records = await _drain_fanout(
        monkeypatch, fake,
        {"event_type": "check_runs", "owner": "a", "repo": "b",
         "installation_id": "42", "repo_full_name": "a/b"},
    )
    assert len(records) == 1
    rec = records[0]
    assert rec["webhook_metadata"] == {"X-GitHub-Event": "check_run"}
    draft = _EVENT_SHAPERS["check_run"](rec)
    assert draft.external_id == "CR_1"


async def test_check_runs_skips_prs_without_head_sha(monkeypatch):
    """A PR with no head SHA is skipped (no ref to query) — yields no
    records and terminates cleanly."""
    prs = [{"number": 1, "node_id": "PR_1"}]  # no head
    fake = _FakeFanoutClient(prs, check_pages={})
    records = await _drain_fanout(
        monkeypatch, fake,
        {"event_type": "check_runs", "owner": "a", "repo": "b",
         "installation_id": "42", "repo_full_name": "a/b"},
    )
    assert records == []


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
