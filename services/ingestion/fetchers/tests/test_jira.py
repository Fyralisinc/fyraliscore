"""Tests for services/ingestion/fetchers/jira.py (IN-17)."""
from __future__ import annotations

import pytest

from services.ingestion.fetchers import FETCHER_DISPATCH
from services.ingestion.fetchers import jira as jf
from services.ingestion.fetchers.jira import (
    SHARD_KIND_PROJECT_ISSUES,
    JiraCursor,
    fetch_page_jira,
)
from services.ingestion.normalizer.channel_mapping import resolve_channel


pytestmark = pytest.mark.asyncio


class _FakeInst:
    _d = {"base_url": "https://acme.atlassian.net", "cloud_id": "cloud-xyz"}

    def __getitem__(self, k):
        return self._d[k]

    def __contains__(self, k):
        return k in self._d


def _issue(iid, key, updated, *, with_changelog=True, with_comment=True):
    issue = {
        "id": iid,
        "key": key,
        "self": f"https://acme.atlassian.net/rest/api/2/issue/{iid}",
        "fields": {
            "summary": f"{key} summary",
            "status": {"name": "Open"},
            "updated": updated,
            "project": {"key": "ENG"},
        },
    }
    if with_comment:
        issue["fields"]["comment"] = {
            "comments": [
                {"id": f"c{iid}", "updated": updated, "body": "hi", "author": {"emailAddress": "a@x.com"}},
            ],
        }
    if with_changelog:
        issue["changelog"] = {
            "histories": [
                {"id": f"h{iid}", "created": updated,
                 "items": [{"field": "status", "fromString": "To Do", "toString": "Open"}]},
            ],
        }
    return issue


class _FakeJiraClient:
    """Two-page full sync via the token-paginated /search/jql shape; records
    the JQL it was called with."""

    def __init__(self):
        self.calls: list[dict] = []

    async def search_issues(self, *, jql, next_page_token=None, max_results=100,
                            fields=None, expand="changelog"):
        self.calls.append({"jql": jql, "next_page_token": next_page_token})
        if next_page_token is None:
            issues = [
                _issue("10001", "ENG-1", "2026-05-01T10:00:00.000+0000"),
                _issue("10002", "ENG-2", "2026-05-02T10:00:00.000+0000"),
            ]
            return issues, "tok-2", False  # more pages
        issues = [_issue("10003", "ENG-3", "2026-05-03T10:00:00.000+0000")]
        return issues, None, True  # terminal


def _patch_client(monkeypatch, client):
    async def _open(_install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(jf, "_open_jira_client", _open)


async def test_dispatch_and_channel_wired():
    assert FETCHER_DISPATCH["jira"] is fetch_page_jira
    assert resolve_channel("jira", "backfill") == "jira:issue"
    assert resolve_channel("jira", "webhook") == "jira:issue"
    assert resolve_channel("jira", "poll") == "jira:issue"


async def test_full_backfill_fans_out_and_advances_cursor(monkeypatch):
    client = _FakeJiraClient()
    _patch_client(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_PROJECT_ISSUES, "project_key": "ENG"}

    # Page 1.
    res1 = await fetch_page_jira(_FakeInst(), shard, None)
    assert res1.end_of_data is False
    # 2 issues -> each fans out into issue + transition + comment = 3 records.
    assert len(res1.records) == 6
    types = {r.get("_fyralis_record_type") for r in res1.records}
    assert types == {"issue", "transition", "comment"}
    # site stamped from the base_url host (parity with the webhook path).
    assert all(r["_fyralis_site"] == "acme.atlassian.net" for r in res1.records)
    cur = JiraCursor.model_validate(res1.next_cursor)
    assert cur.next_page_token == "tok-2"
    assert cur.high_water_updated == "2026-05-02T10:00:00.000+0000"
    # FULL mode -> JQL has no `updated >=` floor.
    assert "updated >=" not in client.calls[0]["jql"]

    # Page 2 (terminal) — the cursor's token is threaded back to the client.
    res2 = await fetch_page_jira(_FakeInst(), shard, res1.next_cursor)
    assert client.calls[1]["next_page_token"] == "tok-2"
    assert res2.end_of_data is True
    assert len(res2.records) == 3
    cur2 = JiraCursor.model_validate(res2.next_cursor)
    assert cur2.high_water_updated == "2026-05-03T10:00:00.000+0000"
    assert cur2.issues_seen == 3
    assert cur2.next_page_token is None


async def test_warm_start_uses_incremental_jql(monkeypatch):
    client = _FakeJiraClient()
    _patch_client(monkeypatch, client)
    shard = {
        "shard_kind": SHARD_KIND_PROJECT_ISSUES,
        "project_key": "ENG",
        "updated_cursor": "2026-05-02T10:00:00.000+0000",
    }
    await fetch_page_jira(_FakeInst(), shard, None)
    assert 'updated >= "2026/05/02 10:00"' in client.calls[0]["jql"]


async def test_to_jql_datetime_keeps_user_tz_wall_clock():
    # UTC / Z stay as-is.
    assert jf._to_jql_datetime("2026-05-20T12:30:00.000+0000") == "2026/05/20 12:30"
    assert jf._to_jql_datetime("2026-05-20T12:30:00Z") == "2026/05/20 12:30"
    # CRITICAL: a non-UTC offset (e.g. +0545 Nepal) must keep its OWN wall
    # clock, NOT convert to UTC. Jira interprets the bare JQL literal in the
    # user's tz, so converting to UTC ("03:39") would shift the floor ~6h into
    # the past and make `updated >` match everything → infinite reshard.
    assert jf._to_jql_datetime("2026-05-26T09:24:29.224+0545") == "2026/05/26 09:24"
    assert jf._to_jql_datetime(None) is None
    assert jf._to_jql_datetime("garbage") is None


async def test_to_jql_minute_after_is_exclusive_floor():
    # Rounds UP to the next minute so the reconciler probe (`updated >=`)
    # excludes the high-water's own minute -> converges (no infinite reshard).
    # 09:24:29 must become 09:25 (NOT 09:24, which `>=`/`>` both re-match).
    assert jf._to_jql_minute_after("2026-05-26T09:24:29.224+0545") == "2026/05/26 09:25"
    assert jf._to_jql_minute_after("2026-05-20T12:30:00.000+0000") == "2026/05/20 12:31"
    # minute rollover into the next hour/day.
    assert jf._to_jql_minute_after("2026-05-20T12:59:30.000+0000") == "2026/05/20 13:00"
    assert jf._to_jql_minute_after("2026-05-20T23:59:30Z") == "2026/05/21 00:00"
    assert jf._to_jql_minute_after(None) is None


async def test_missing_project_key_ends_cleanly(monkeypatch):
    _patch_client(monkeypatch, _FakeJiraClient())
    res = await fetch_page_jira(_FakeInst(), {"shard_kind": SHARD_KIND_PROJECT_ISSUES}, None)
    assert res.end_of_data is True
    assert res.records == []
