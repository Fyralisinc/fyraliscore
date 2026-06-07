"""Self-verifying synthetic Notion backfill test (IN-14, X2/X3 infra).

Drives the REAL `fetch_page_notion` tree-walk fetcher against
`MockNotionClient` (a fixture from `make_notion`) through the
`_open_notion_client` seam, then runs EVERY emitted record through the REAL
`notion:object` handler. No database / network — the mock + fixture are the
only test doubles; the fetcher's resumable WorkItem-stack cursor logic, the
block/comment fan-out, and the handler are all production code.

WHAT BECOMES AN OBSERVATION (verified against the real fetcher)
--------------------------------------------------------------
The fetcher pops one work item (= one Notion list call) per
`fetch_page_notion` invocation; some pops emit records, others only push more
work items. Records ARE the observations. Per page (a DB row OR a loose page)
the fetcher emits exactly:

    1 page  +  blocks_per_page blocks  +  comments_per_item comments

So:
    page observations    = databases * pages_per_database + loose_pages
    block observations   = (page count) * blocks_per_page
    comment observations = (page count) * comments_per_item

The default fixture (databases=1, pages_per_database=2, loose_pages=1,
blocks=0, comments=0) yields exactly 1*2 + 1 == 3 page observations and
nothing else.
"""
from __future__ import annotations

import asyncio

import pytest

from lib.shared.errors import NotionApiError
from services.ingest.ingestion.fetchers import notion as notion_fetcher
from services.ingest.ingestion.fetchers.notion import (
    SHARD_KIND_DATABASE,
    SHARD_KIND_PAGE_TREE,
    fetch_page_notion,
)
from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.fixtures.notion_generator import make_notion
from services.ingest.synthetic.mock_clients.notion import MockNotionClient


WORKSPACE_ID = "x3-notion-ws"


# The fetcher only passes `install` to the (monkeypatched) `_open_notion_client`
# seam, so the install record is opaque to the test — a plain dict suffices.
def _install() -> dict[str, object]:
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "tenant_id": "00000000-0000-0000-0000-0000000000aa",
        "source": "notion",
    }


def _database_shard(database_id: str) -> dict[str, object]:
    return {
        "shard_kind": SHARD_KIND_DATABASE,
        "database_id": database_id,
        "workspace_id": WORKSPACE_ID,
    }


def _page_tree_shard() -> dict[str, object]:
    return {
        "shard_kind": SHARD_KIND_PAGE_TREE,
        "workspace_id": WORKSPACE_ID,
    }


def _patch_client(monkeypatch, client: MockNotionClient) -> None:
    """Rebind the fetcher's `_open_notion_client` seam to yield the mock +
    an async close callable (the real seam returns `(client, close)`)."""
    async def _open(_install):  # noqa: ANN001, ANN202
        async def _close() -> None:
            return None
        return client, _close

    monkeypatch.setattr(notion_fetcher, "_open_notion_client", _open)


async def _drive_walk(
    install: dict[str, object], shard: dict[str, object],
) -> list[dict[str, object]]:
    """Run the real resumable tree walk to completion, threading `next_cursor`
    back each iteration exactly like ShardFetch, until `end_of_data`."""
    records: list[dict[str, object]] = []
    cursor: dict[str, object] | None = None
    for _ in range(10_000):  # generous guard against a runaway walk
        result = await fetch_page_notion(install, shard, cursor)
        records.extend(result.records)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    else:  # pragma: no cover - only on a genuine non-terminating fetcher bug
        raise AssertionError("tree walk did not reach end_of_data")
    return records


async def _gather(coros):
    return await asyncio.gather(*coros)


def _run_through_handler(records: list[dict[str, object]]):
    """Push every fetched record through the REAL notion:object handler."""
    channel = resolve_channel("notion", "backfill")
    assert channel == "notion:object"
    handler = get_handler(channel)

    async def _one(rec: dict[str, object]):
        return await handler(dict(rec), {})

    return asyncio.run(_gather([_one(r) for r in records]))


# ---------------------------------------------------------------------
# Happy path: default fixture, both shard kinds.
# ---------------------------------------------------------------------
def test_synthetic_notion_backfill_drives_real_fetcher_and_handler(monkeypatch):
    databases = 1
    pages_per_database = 2
    loose_pages = 1

    fixture = make_notion(
        workspace_id=WORKSPACE_ID,
        databases=databases,
        pages_per_database=pages_per_database,
        loose_pages=loose_pages,
        blocks_per_page=0,
        comments_per_item=0,
    )
    database_id = fixture["databases"][0]["database_id"]

    client = MockNotionClient(fixture=fixture, profile=HAPPY_PATH)
    _patch_client(monkeypatch, client)

    install = _install()
    db_records = asyncio.run(_drive_walk(install, _database_shard(database_id)))
    pt_records = asyncio.run(_drive_walk(install, _page_tree_shard()))
    records = db_records + pt_records

    # With blocks=0/comments=0 every record is a page object; total page
    # observations == databases*pages_per_database + loose_pages == 3.
    expected_pages = databases * pages_per_database + loose_pages
    assert expected_pages == 3
    assert all(r.get("object") == "page" for r in records)
    assert len(records) == expected_pages

    # Each db-row carries a database_id parent; the loose page does not.
    assert len(db_records) == databases * pages_per_database
    assert len(pt_records) == loose_pages
    assert all(
        (r.get("parent") or {}).get("type") == "database_id" for r in db_records
    )
    assert all(
        (r.get("parent") or {}).get("type") != "database_id" for r in pt_records
    )

    # The fetcher stamps the workspace id onto every record.
    assert all(r.get("_fyralis_workspace_id") == WORKSPACE_ID for r in records)

    # Drive each record through the REAL handler.
    drafts = _run_through_handler(records)
    assert len(drafts) == expected_pages
    external_ids = set()
    for draft in drafts:
        assert draft.external_id is not None and draft.external_id != ""
        assert draft.source_channel == "notion:object"
        assert draft.occurred_at is not None
        assert draft.occurred_at.year == 2026
        external_ids.add(draft.external_id)
    # No accidental collapse — each page is a distinct observation.
    assert len(external_ids) == expected_pages


# ---------------------------------------------------------------------
# Fan-out: blocks + comments per page, both shard kinds.
# ---------------------------------------------------------------------
def test_synthetic_notion_fanout_blocks_and_comments(monkeypatch):
    databases = 1
    pages_per_database = 2
    loose_pages = 1
    blocks_per_page = 1
    comments_per_item = 1

    fixture = make_notion(
        workspace_id=WORKSPACE_ID,
        databases=databases,
        pages_per_database=pages_per_database,
        loose_pages=loose_pages,
        blocks_per_page=blocks_per_page,
        comments_per_item=comments_per_item,
    )
    database_id = fixture["databases"][0]["database_id"]

    client = MockNotionClient(fixture=fixture, profile=HAPPY_PATH)
    _patch_client(monkeypatch, client)

    install = _install()
    records = asyncio.run(_drive_walk(install, _database_shard(database_id)))
    records += asyncio.run(_drive_walk(install, _page_tree_shard()))

    page_count = databases * pages_per_database + loose_pages
    by_type: dict[object, int] = {}
    for r in records:
        by_type[r.get("object")] = by_type.get(r.get("object"), 0) + 1

    # Verified against ACTUAL fetcher behavior: each page emits 1 page + its
    # blocks + its comments; blocks have has_children=False so no recursion.
    assert by_type.get("page") == page_count                       # 3
    assert by_type.get("block") == page_count * blocks_per_page    # 3
    assert by_type.get("comment") == page_count * comments_per_item  # 3
    expected_total = page_count * (1 + blocks_per_page + comments_per_item)
    assert expected_total == 9
    assert len(records) == expected_total

    # Every record (page/block/comment) yields a valid draft with a 2026 ts.
    drafts = _run_through_handler(records)
    assert len(drafts) == expected_total
    external_ids = set()
    for draft in drafts:
        assert draft.external_id is not None and draft.external_id != ""
        assert draft.source_channel == "notion:object"
        assert draft.occurred_at is not None
        assert draft.occurred_at.year == 2026
        external_ids.add(draft.external_id)
    assert len(external_ids) == expected_total

    # All three object kinds are present in the draft content.
    object_types = {d.content.get("object_type") for d in drafts}
    assert object_types == {"page", "block", "comment"}


# ---------------------------------------------------------------------
# Pagination: rows beyond the page_size cap must span >1 list call.
# ---------------------------------------------------------------------
def test_synthetic_notion_pagination_multi_page(monkeypatch):
    pages_per_database = 5
    page_size = 2  # ceil(5/2) = 3 query_database calls
    fixture = make_notion(
        workspace_id=WORKSPACE_ID,
        databases=1,
        pages_per_database=pages_per_database,
        loose_pages=0,
        page_size=page_size,
    )
    database_id = fixture["databases"][0]["database_id"]

    client = MockNotionClient(fixture=fixture, profile=HAPPY_PATH)
    calls = {"n": 0}
    orig = client.query_database

    async def _counting(database_id, **kwargs):  # noqa: ANN001
        calls["n"] += 1
        return await orig(database_id, **kwargs)

    client.query_database = _counting  # type: ignore[method-assign]
    _patch_client(monkeypatch, client)

    records = asyncio.run(_drive_walk(_install(), _database_shard(database_id)))

    assert calls["n"] >= 3  # the db_rows cursor spanned multiple pages
    assert len([r for r in records if r.get("object") == "page"]) == pages_per_database


# ---------------------------------------------------------------------
# Database enumeration: search(object_filter="database") returns the
# databases so the REAL planner can build notion_database shards.
# ---------------------------------------------------------------------
def test_search_database_filter_returns_databases_for_planner():
    fixture = make_notion(workspace_id=WORKSPACE_ID, databases=2, loose_pages=1)
    client = MockNotionClient(fixture=fixture, profile=HAPPY_PATH)

    dbs, cursor, has_more = asyncio.run(client.search(object_filter="database"))
    assert has_more is False and cursor is None
    assert len(dbs) == 2
    assert all(d.get("object") == "database" for d in dbs)
    returned_ids = {d.get("id") for d in dbs}
    fixture_ids = {d["database_id"] for d in fixture["databases"]}
    assert returned_ids == fixture_ids

    # search(object_filter="page") returns the loose pages (not databases).
    pages, _, _ = asyncio.run(client.search(object_filter="page"))
    assert len(pages) == 1
    assert all(p.get("object") == "page" for p in pages)


# ---------------------------------------------------------------------
# Fault: a rate-limit FaultProfile surfaces NotionApiError, and the fetcher's
# 429 fallback returns an empty NON-terminal page (cursor unadvanced).
# ---------------------------------------------------------------------
def test_synthetic_notion_rate_limit_fault(monkeypatch):
    fixture = make_notion(
        workspace_id=WORKSPACE_ID,
        databases=1,
        pages_per_database=2,
        loose_pages=1,
    )
    database_id = fixture["databases"][0]["database_id"]

    # rate_limit_after_n_requests=0 → the very first call raises.
    profile = FaultProfile(rate_limit_after_n_requests=0)

    # 1. Raw client surface raises the production error type + code.
    raw = MockNotionClient(fixture=fixture, profile=profile)
    with pytest.raises(NotionApiError) as exc_info:
        asyncio.run(raw.query_database(database_id))
    err = exc_info.value
    assert getattr(err, "_code", None) == "notion_api_rate_limited"
    assert (err.context or {}).get("http_status") == 429

    # 2. Through the fetcher: the 429 fallback re-pushes the same work item and
    #    ends the round empty + non-terminal so ShardFetch re-enters next tick.
    fetch_client = MockNotionClient(fixture=fixture, profile=profile)
    _patch_client(monkeypatch, fetch_client)
    result = asyncio.run(
        fetch_page_notion(_install(), _database_shard(database_id), None)
    )
    assert result.records == []
    assert result.end_of_data is False
    # The work stack is preserved (the db_rows item was re-pushed).
    assert result.next_cursor is not None
    assert result.next_cursor["stack"]  # non-empty → walk resumes


# ---------------------------------------------------------------------
# Surface check: the mock implements every method the production
# fetcher / reconciler / planner call.
# ---------------------------------------------------------------------
def test_mock_notion_implements_required_methods():
    import inspect

    client = MockNotionClient(fixture=make_notion(databases=1, pages_per_database=1))
    for name in (
        "search",
        "query_database",
        "list_block_children",
        "list_comments",
        "latest_database_edit",
        "latest_page_edit",
    ):
        assert hasattr(client, name)
        assert inspect.iscoroutinefunction(getattr(client, name))
