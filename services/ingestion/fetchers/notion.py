"""services/ingestion/fetchers/notion.py — Notion backfill + poll fetcher (IN-14).

Per A18 (per-source backfill = net-new code) + A16/N1 (cursor advanced by
ShardFetch, opaque to it) + A27.3 (records shaped for the handler).

============================================================
RESUMABLE TREE WALK (the cursor IS a work stack)
============================================================
ShardFetch calls this fetcher in a loop, persisting the returned cursor
between calls. A Notion shard is a TREE (database → rows → each row's
blocks → each row's comments), and each Notion list endpoint is itself
paginated. We model the whole walk as an explicit work stack carried in
the cursor, popping ONE work item (= one Notion API list call) per
fetcher invocation:

  - db_rows      : POST /v1/databases/{id}/query — emits page rows, then
                   pushes a page_blocks + page_comments item per row, and
                   a continuation if has_more.
  - loose_pages  : POST /v1/search?filter=page — emits pages NOT owned by
                   a database (DB rows are covered by their db shard),
                   then pushes blocks + comments per loose page.
  - page_blocks  : GET /v1/blocks/{block}/children — emits child blocks;
                   recurses into children with children up to the depth
                   cap (D2); stamps a truncation marker at the cap.
  - page_comments: GET /v1/comments?block_id={page} — emits comments.

`end_of_data=True` exactly when the stack empties. The stack lives in
`workflow_states.state_data["cursor"]`; for a typical workspace it holds
hundreds of entries (bounded by breadth) — acceptable for v1.

============================================================
HANDLER CONFORMANCE (A27.3) + external_id PARITY
============================================================
Each record is the RAW Notion object (it carries its own `object` field:
"page" | "block" | "comment"), so the `notion:object` handler branches on
`record["object"]` with no injected header. The handler derives
`external_id = notion:{object}:{id}` and `occurred_at` from the object's
own timestamp — identical whether the object arrived via backfill or the
"poll" incremental re-run, so the dedup UNIQUE index collapses the twins.
The fetcher injects two private keys only: `_fyralis_workspace_id` (entity
grounding) and `_fyralis_truncated` (depth-cap marker, D2).
"""
from __future__ import annotations

import logging
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.integrations.notion import metrics


log = logging.getLogger(__name__)


SHARD_KIND_DATABASE = "notion_database"
SHARD_KIND_PAGE_TREE = "notion_page_tree"

# D2 — block-tree recursion depth cap (env-overridable).
def _depth_cap() -> int:
    try:
        return int(os.environ.get("NOTION_BLOCK_DEPTH_CAP", "3"))
    except ValueError:
        return 3


class WorkItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str  # db_rows | loose_pages | page_blocks | page_comments
    list_cursor: str | None = None
    page_id: str | None = None   # page whose blocks/comments we walk
    block_id: str | None = None  # parent block for page_blocks (page_id at root)
    depth: int = 0


class NotionCursor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stack: list[WorkItem] = []
    items_seen: int = 0
    last_edited_at: str | None = None  # high-water for the reconciler
    seeded: bool = False


async def _open_notion_client(install: asyncpg.Record):  # noqa: ANN202
    """Test seam — monkeypatched by the test harness. Production builds a
    real NotionClient pointed at the resolver's notion_api base."""
    from services.ingestion.fetchers._clients import open_notion_client
    return await open_notion_client(install)


def _decode_cursor(c: dict[str, Any] | None) -> NotionCursor:
    if c is None:
        return NotionCursor()
    return NotionCursor.model_validate(c)


def _encode_cursor(c: NotionCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


def _seed(cur: NotionCursor, shard_kind: str) -> NotionCursor:
    """Seed the work stack from the shard kind on the first call."""
    if shard_kind == SHARD_KIND_DATABASE:
        # database_id is read off the shard at run time and threaded into
        # the db_rows item via the caller.
        cur.stack = [WorkItem(kind="db_rows")]
    elif shard_kind == SHARD_KIND_PAGE_TREE:
        cur.stack = [WorkItem(kind="loose_pages")]
    cur.seeded = True
    return cur


def _bump_high_water(cur: NotionCursor, obj: dict[str, Any]) -> None:
    edited = obj.get("last_edited_time") or obj.get("created_time")
    if isinstance(edited, str) and (
        cur.last_edited_at is None or edited > cur.last_edited_at
    ):
        cur.last_edited_at = edited


def _is_database_row(page: dict[str, Any]) -> bool:
    parent = page.get("parent") or {}
    return isinstance(parent, dict) and parent.get("type") == "database_id"


async def fetch_page_notion(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    shard_kind = shard_identifier.get("shard_kind")
    workspace_id = shard_identifier.get("workspace_id")
    database_id = shard_identifier.get("database_id")
    cur = _decode_cursor(cursor)
    if not cur.seeded:
        cur = _seed(cur, shard_kind)

    if not cur.stack:
        return FetchResult(records=[], next_cursor=_encode_cursor(cur), end_of_data=True)

    item = cur.stack.pop()
    records: list[dict[str, Any]] = []
    depth_cap = _depth_cap()

    client, close = await _open_notion_client(install)
    try:
        from lib.shared.errors import NotionApiError

        try:
            if item.kind == "db_rows":
                rows, next_cursor, has_more = await client.query_database(
                    database_id, start_cursor=item.list_cursor,
                )
                for page in rows:
                    page["_fyralis_workspace_id"] = workspace_id
                    records.append(page)
                    _bump_high_water(cur, page)
                    pid = page.get("id")
                    if isinstance(pid, str):
                        cur.stack.append(WorkItem(kind="page_comments", page_id=pid))
                        cur.stack.append(
                            WorkItem(kind="page_blocks", page_id=pid, block_id=pid, depth=0)
                        )
                if has_more and next_cursor:
                    cur.stack.append(WorkItem(kind="db_rows", list_cursor=next_cursor))

            elif item.kind == "loose_pages":
                pages, next_cursor, has_more = await client.search(
                    object_filter="page", start_cursor=item.list_cursor,
                )
                for page in pages:
                    if _is_database_row(page):
                        continue  # owned by a notion_database shard
                    page["_fyralis_workspace_id"] = workspace_id
                    records.append(page)
                    _bump_high_water(cur, page)
                    pid = page.get("id")
                    if isinstance(pid, str):
                        cur.stack.append(WorkItem(kind="page_comments", page_id=pid))
                        cur.stack.append(
                            WorkItem(kind="page_blocks", page_id=pid, block_id=pid, depth=0)
                        )
                if has_more and next_cursor:
                    cur.stack.append(WorkItem(kind="loose_pages", list_cursor=next_cursor))

            elif item.kind == "page_blocks":
                blocks, next_cursor, has_more = await client.list_block_children(
                    item.block_id, start_cursor=item.list_cursor,
                )
                for block in blocks:
                    block["_fyralis_workspace_id"] = workspace_id
                    _bump_high_water(cur, block)
                    if block.get("has_children"):
                        if item.depth + 1 < depth_cap:
                            bid = block.get("id")
                            if isinstance(bid, str):
                                cur.stack.append(WorkItem(
                                    kind="page_blocks", page_id=item.page_id,
                                    block_id=bid, depth=item.depth + 1,
                                ))
                        else:
                            block["_fyralis_truncated"] = {
                                "reason": "depth_cap", "depth": depth_cap,
                            }
                            metrics.record_fetch_event("block_truncated")
                    records.append(block)
                if has_more and next_cursor:
                    cur.stack.append(WorkItem(
                        kind="page_blocks", page_id=item.page_id,
                        block_id=item.block_id, depth=item.depth,
                        list_cursor=next_cursor,
                    ))

            elif item.kind == "page_comments":
                comments, next_cursor, has_more = await client.list_comments(
                    item.page_id, start_cursor=item.list_cursor,
                )
                for comment in comments:
                    comment["_fyralis_workspace_id"] = workspace_id
                    records.append(comment)
                    _bump_high_water(cur, comment)
                if has_more and next_cursor:
                    cur.stack.append(WorkItem(
                        kind="page_comments", page_id=item.page_id,
                        list_cursor=next_cursor,
                    ))

        except NotionApiError as e:
            # Rate-limit budget exhausted: re-push the SAME work item with
            # its cursor unadvanced and end this round empty so ShardFetch
            # re-enters next tick (cursor preserved). Other API errors
            # propagate → shard marked failed.
            if (e.context or {}).get("http_status") == 429:
                cur.stack.append(item)
                metrics.record_fetch_event("rate_limited")
                log.info("notion_backfill_rate_limited", extra={"shard_kind": shard_kind})
                return FetchResult(
                    records=[], next_cursor=_encode_cursor(cur), end_of_data=False,
                )
            # 404 on a single object (page un-shared mid-walk): skip this
            # item, keep walking the rest of the tree.
            if (e.context or {}).get("http_status") == 404:
                log.info("notion_backfill_skip_404", extra={"shard_kind": shard_kind})
                return FetchResult(
                    records=[], next_cursor=_encode_cursor(cur),
                    end_of_data=(len(cur.stack) == 0),
                )
            raise

        cur.items_seen += len(records)
        if records:
            metrics.record_fetch_event("pages", by=len(records))
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=(len(cur.stack) == 0),
        )
    finally:
        await close()


FETCHER_DISPATCH["notion"] = fetch_page_notion


__all__ = [
    "SHARD_KIND_DATABASE",
    "SHARD_KIND_PAGE_TREE",
    "NotionCursor",
    "WorkItem",
    "fetch_page_notion",
]
