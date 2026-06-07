"""Self-verifying synthetic Jira backfill test (IN-17, X2/X3 infra).

Drives the REAL `fetch_page_jira` fetcher against `MockJiraClient` (a fixture
from `make_jira`) through the `_open_jira_client` seam, then runs EVERY emitted
record through the REAL `jira:issue` handler. No database / network — the mock
+ fixture are the only test doubles; the fetcher, cursor logic, fan-out, and
handler are all production code.

Asserted invariants:
  - fan-out count == issues * (1 + transitions_per_issue + comments_per_issue),
  - every record yields a draft with a non-null external_id + an occurred_at
    in 2026 (the observations partition window),
  - the three fanned record types (issue/transition/comment) all appear,
  - pagination: issues_per_project > page_size triggers multi-page fetch,
  - faults: a rate-limit FaultProfile surfaces the production fallback.
"""
from __future__ import annotations

import asyncio

import pytest

from services.ingest.ingestion.fetchers import jira as jira_fetcher
from services.ingest.ingestion.fetchers.jira import fetch_page_jira
from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.fixtures.jira_generator import make_jira
from services.ingest.synthetic.mock_clients.jira import MockJiraClient
from lib.shared.errors import JiraApiError


# The fetcher reads `install["base_url"]` (via `_site_of`) and the shard's
# `project_key` / `updated_cursor`. Model an install record as a plain dict
# (the fetcher only does `install["base_url"]` + `"base_url" in install`).
def _install(site_host: str) -> dict[str, object]:
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "base_url": f"https://{site_host}",
        "cloud_id": "cloud-mock",
    }


def _shard(project_key: str, *, updated_cursor: str | None = None) -> dict[str, object]:
    return {
        "shard_kind": "jira_project_issues",
        "project_key": project_key,
        "project_id": "10000",
        "installation_id": "00000000-0000-0000-0000-000000000001",
        "updated_cursor": updated_cursor,
    }


def _patch_client(monkeypatch, client: MockJiraClient) -> None:
    """Rebind the fetcher's `_open_jira_client` seam to yield the mock."""
    async def _open(_install):  # noqa: ANN001, ANN202
        async def _close() -> None:
            return None
        return client, _close

    monkeypatch.setattr(jira_fetcher, "_open_jira_client", _open)


async def _drive_backfill(
    install: dict[str, object], shard: dict[str, object],
) -> list[dict[str, object]]:
    """Run the real fetch loop to completion, collecting all records.
    Threads `next_cursor` back each iteration exactly like ShardFetch."""
    records: list[dict[str, object]] = []
    cursor: dict[str, object] | None = None
    for _ in range(1000):  # generous guard against a runaway loop
        result = await fetch_page_jira(install, shard, cursor)
        records.extend(result.records)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    else:  # pragma: no cover - only on a genuine non-terminating fetcher bug
        raise AssertionError("fetch loop did not reach end_of_data")
    return records


def test_synthetic_jira_backfill_drives_real_fetcher_and_handler(monkeypatch):
    site_host = "acme.atlassian.net"
    issues_per_project = 3
    transitions_per_issue = 2
    comments_per_issue = 1

    fixture = make_jira(
        site_host=site_host,
        projects=1,
        issues_per_project=issues_per_project,
        transitions_per_issue=transitions_per_issue,
        comments_per_issue=comments_per_issue,
    )
    project_key = fixture["projects"][0]["project_key"]

    client = MockJiraClient(fixture=fixture, profile=HAPPY_PATH)
    _patch_client(monkeypatch, client)

    records = asyncio.run(
        _drive_backfill(_install(site_host), _shard(project_key))
    )

    # Fan-out count.
    expected = issues_per_project * (
        1 + transitions_per_issue + comments_per_issue
    )
    assert expected == 12
    assert len(records) == expected

    # All three fanned record types present.
    types = {r.get("_fyralis_record_type") for r in records}
    assert types == {"issue", "transition", "comment"}

    # Drive each record through the REAL handler.
    channel = resolve_channel("jira", "backfill")
    assert channel == "jira:issue"
    handler = get_handler(channel)

    async def _run_handler(rec: dict[str, object]):
        body = dict(rec)
        headers = body.pop("webhook_metadata", {}) or {}
        return await handler(body, headers)

    drafts = asyncio.run(_gather([_run_handler(r) for r in records]))

    assert len(drafts) == expected
    external_ids = set()
    for draft in drafts:
        assert draft.external_id is not None and draft.external_id != ""
        assert draft.source_channel == "jira:issue"
        assert draft.occurred_at is not None
        assert draft.occurred_at.year == 2026
        external_ids.add(draft.external_id)
    # external_ids are unique across the fan-out (no accidental collapse).
    assert len(external_ids) == expected

    # At least one transition is a STATUS change -> kind="state_change".
    kinds = [d.kind for d in drafts]
    assert "state_change" in kinds


def test_synthetic_jira_pagination_multi_page(monkeypatch):
    """issues_per_project > page_size must trigger >1 fetch page yet still
    yield every issue's full fan-out."""
    site_host = "acme.atlassian.net"
    page_size = 2
    issues_per_project = 5  # > page_size -> 3 pages
    fixture = make_jira(
        site_host=site_host,
        projects=1,
        issues_per_project=issues_per_project,
        transitions_per_issue=1,
        comments_per_issue=1,
        page_size=page_size,
    )
    project_key = fixture["projects"][0]["project_key"]

    # Count the actual fetch calls to prove multi-page paging happened.
    client = MockJiraClient(fixture=fixture, profile=HAPPY_PATH)
    call_count = {"n": 0}
    orig_search = client.search_issues

    async def _counting_search(**kwargs):
        call_count["n"] += 1
        return await orig_search(**kwargs)

    client.search_issues = _counting_search  # type: ignore[method-assign]
    _patch_client(monkeypatch, client)

    records = asyncio.run(
        _drive_backfill(_install(site_host), _shard(project_key))
    )

    assert call_count["n"] >= 3  # ceil(5/2) = 3 pages
    expected = issues_per_project * (1 + 1 + 1)
    assert len(records) == expected
    issue_records = [r for r in records if r.get("_fyralis_record_type") == "issue"]
    assert len(issue_records) == issues_per_project


def test_synthetic_jira_rate_limit_fault(monkeypatch):
    """A rate-limit FaultProfile makes `search_issues` raise
    JiraApiError(jira_api_rate_limited); the fetcher catches it and ends the
    round empty WITHOUT advancing (end_of_data False)."""
    site_host = "acme.atlassian.net"
    fixture = make_jira(
        site_host=site_host,
        projects=1,
        issues_per_project=3,
        transitions_per_issue=1,
        comments_per_issue=1,
    )
    project_key = fixture["projects"][0]["project_key"]

    # rate_limit_after_n_requests=0 -> the very first call raises.
    profile = FaultProfile(rate_limit_after_n_requests=0)

    # 1. Raw client surface raises the production error type + code.
    raw_client = MockJiraClient(fixture=fixture, profile=profile)
    with pytest.raises(JiraApiError) as exc_info:
        asyncio.run(raw_client.search_issues(jql=f'project = "{project_key}"'))
    err = exc_info.value
    assert getattr(err, "_code", None) == "jira_api_rate_limited"

    # 2. Through the fetcher: the rate-limit fallback returns an empty,
    #    non-terminal page (cursor unadvanced) so ShardFetch re-enters.
    fetch_client = MockJiraClient(fixture=fixture, profile=profile)
    _patch_client(monkeypatch, fetch_client)
    result = asyncio.run(
        fetch_page_jira(_install(site_host), _shard(project_key), None)
    )
    assert result.records == []
    assert result.end_of_data is False


def test_mock_jira_implements_methods_called_by_fetcher_and_reconciler():
    """Surface check: the mock implements search_issues + has_updates_since
    (+ list_projects / myself) the production fetcher/reconciler/seed call."""
    import inspect

    client = MockJiraClient(
        fixture=make_jira(projects=1, issues_per_project=1),
    )
    for name in ("search_issues", "has_updates_since", "list_projects", "myself"):
        assert hasattr(client, name)
        assert inspect.iscoroutinefunction(getattr(client, name))


# ---------------------------------------------------------------------
# Local async helper (avoids creating multiple event loops per record).
# ---------------------------------------------------------------------
async def _gather(coros):
    return await asyncio.gather(*coros)
