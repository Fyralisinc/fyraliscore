"""Notion workspace fixture generator (IN-14, X3 synthetic infra).

`make_notion(workspace_id=..., databases=N, pages_per_database=M,
loose_pages=L, blocks_per_page=B, comments_per_item=C)` produces a
deterministic Notion workspace shaped to feed `MockNotionClient`, which in
turn drives the REAL resumable tree-walk fetcher
(`services/ingest/ingestion/fetchers/notion.py`).

WHAT THE FETCHER WALKS (and therefore what becomes an observation)
------------------------------------------------------------------
A Notion shard is a TREE the fetcher pops one list-call at a time:

  - notion_database shard → db_rows: `query_database(database_id)` emits each
    row (a `page` object) as ONE record, then schedules a page_blocks +
    page_comments walk per row.
  - notion_page_tree shard → loose_pages: `search(object_filter="page")` emits
    each page NOT owned by a database as ONE record (database rows are skipped
    via `_is_database_row` because they belong to a notion_database shard),
    then schedules a page_blocks + page_comments walk per loose page.
  - page_blocks: `list_block_children(page_id)` emits EVERY child block as a
    record (recursing into blocks with `has_children` up to the depth cap).
  - page_comments: `list_comments(page_id)` emits each comment as a record.

So per page (DB row OR loose page) the fetcher emits exactly:
    1 page  +  blocks_per_page blocks  +  comments_per_item comments
records — and each record becomes ONE observation through the
`notion:object` handler. Total page observations across a workspace:
    databases * pages_per_database + loose_pages.

To keep the default counts trivially exact, `blocks_per_page=0` and
`comments_per_item=0` by default (each page yields exactly one page
observation). Generated blocks carry `has_children=False`, so the block
walk never recurses — `blocks_per_page` blocks in, `blocks_per_page` block
records out, no depth-cap surprises.

DETERMINISM
-----------
Every id / timestamp / title is a SHA-256 of its coordinates, so a given
call yields byte-identical output. Timestamps land in the 2026-01
observations partition window, spaced minutes apart, so the handler's
`occurred_at` parses into 2026 and `last_edited_time` ordering is stable.

This dict is EXACTLY what `MockNotionClient(fixture=...)` consumes.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any


def make_notion(
    *,
    workspace_id: str = "x3-notion-ws",
    databases: int = 1,
    pages_per_database: int = 2,
    loose_pages: int = 1,
    blocks_per_page: int = 0,
    comments_per_item: int = 0,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 100,
) -> dict[str, Any]:
    """Build a Notion workspace fixture consumable by `MockNotionClient`.

    Args:
      workspace_id: Logical workspace id (the fetcher stamps it onto every
        record as `_fyralis_workspace_id`; also the shard's `workspace_id`).
      databases: Number of databases. Each becomes one `notion_database`
        shard target and is returned by `search(object_filter="database")`.
      pages_per_database: Rows per database (each is a `page` object returned
        by `query_database` whose `parent.type == "database_id"`).
      loose_pages: Pages NOT owned by a database (returned by
        `search(object_filter="page")`; the `notion_page_tree` shard target).
      blocks_per_page: Top-level child blocks per page/row (each a `block`
        record via `list_block_children`). Generated with
        `has_children=False` so the walk never recurses.
      comments_per_item: Comments per page/row (each a `comment` record via
        `list_comments`).
      base_iso: ISO-8601 anchor for the oldest object; everything is offset
        forward so it lands in the 2026-01 partition.
      page_size: The mock client's per-list-call page cap (Notion's max=100).

    Returns:
      Fixture dict:
        {
          "workspace_id": "x3-notion-ws",
          "page_size": 100,
          "databases": [
            {
              "database_id": "...",                 # notion_database shard target
              "object_summary": { <search "database" result> },
              "rows": [ <page object>, ... ],       # query_database results
            }, ...
          ],
          "loose_pages": [ <page object>, ... ],    # search "page" results
          "blocks_by_page": { page_id: [ <block>, ... ] },   # list_block_children
          "comments_by_page": { page_id: [ <comment>, ... ] }, # list_comments
        }
    """
    base = _parse_base(base_iso)

    database_list: list[dict[str, Any]] = []
    blocks_by_page: dict[str, list[dict[str, Any]]] = {}
    comments_by_page: dict[str, list[dict[str, Any]]] = {}

    # --- Databases + their rows (page objects with a database_id parent) ---
    for d in range(databases):
        database_id = _id(workspace_id, "db", d)
        db_summary = _database_object(workspace_id, database_id, d, base)
        rows: list[dict[str, Any]] = []
        for r in range(pages_per_database):
            page_id = _id(workspace_id, "dbrow", d, r)
            # Each row anchored 60 min apart (within its db); ids carry the real
            # uniqueness, so the anchor only needs to keep timestamps ordered and
            # safely inside the 2026-01 partition window.
            anchor = (d * pages_per_database + r) * 60
            row = _page_object(
                workspace_id=workspace_id,
                page_id=page_id,
                base=base,
                minute_anchor=anchor,
                parent={"type": "database_id", "database_id": database_id},
                title=f"Row {r} of db {d}",
            )
            rows.append(row)
            _attach_children(
                workspace_id, page_id, base, anchor,
                blocks_per_page, comments_per_item,
                blocks_by_page, comments_by_page,
            )
        database_list.append({
            "database_id": database_id,
            "object_summary": db_summary,
            "rows": rows,
        })

    # --- Loose pages (NOT owned by a database) ---
    loose_list: list[dict[str, Any]] = []
    # Anchor loose pages on a separate day from DB rows (day 2 of the base
    # month) so the two streams never share a timestamp, while both stay well
    # inside the 2026-01 partition window.
    loose_base = base + timedelta(days=1)
    for lp in range(loose_pages):
        page_id = _id(workspace_id, "loose", lp)
        anchor = lp * 60
        page = _page_object(
            workspace_id=workspace_id,
            page_id=page_id,
            base=loose_base,
            minute_anchor=anchor,
            parent={"type": "workspace", "workspace": True},
            title=f"Loose page {lp}",
        )
        loose_list.append(page)
        _attach_children(
            workspace_id, page_id, loose_base, anchor,
            blocks_per_page, comments_per_item,
            blocks_by_page, comments_by_page,
        )

    return {
        "workspace_id": workspace_id,
        "page_size": page_size,
        "databases": database_list,
        "loose_pages": loose_list,
        "blocks_by_page": blocks_by_page,
        "comments_by_page": comments_by_page,
    }


# ---------------------------------------------------------------------
# Per-entity builders
# ---------------------------------------------------------------------

def _attach_children(
    workspace_id: str,
    page_id: str,
    base: datetime,
    minute_anchor: int,
    blocks_per_page: int,
    comments_per_item: int,
    blocks_by_page: dict[str, list[dict[str, Any]]],
    comments_by_page: dict[str, list[dict[str, Any]]],
) -> None:
    if blocks_per_page > 0:
        blocks_by_page[page_id] = [
            _block_object(
                workspace_id, page_id, b, base, minute_anchor + 5 + b,
            )
            for b in range(blocks_per_page)
        ]
    if comments_per_item > 0:
        comments_by_page[page_id] = [
            _comment_object(
                workspace_id, page_id, c, base, minute_anchor + 20 + c,
            )
            for c in range(comments_per_item)
        ]


def _page_object(
    *,
    workspace_id: str,
    page_id: str,
    base: datetime,
    minute_anchor: int,
    parent: dict[str, Any],
    title: str,
) -> dict[str, Any]:
    created = _iso(base, minute_anchor)
    edited = _iso(base, minute_anchor + 30)
    actor = _user(workspace_id, page_id)
    return {
        "object": "page",
        "id": page_id,
        "created_time": created,
        "last_edited_time": edited,
        "created_by": actor,
        "last_edited_by": actor,
        "parent": parent,
        "url": f"https://www.notion.so/{page_id.replace('-', '')}",
        "properties": {
            "Name": {
                "id": "title",
                "type": "title",
                "title": [_text_span(title)],
            },
            # A status property makes a DB row a tracked work item; the handler
            # emits those as kind="state_change" (parity with jira's status).
            "Status": {
                "id": _digest(page_id, "status")[:8],
                "type": "status",
                "status": {"name": "In Progress", "color": "blue"},
            },
        },
    }


def _block_object(
    workspace_id: str, page_id: str, b: int, base: datetime, minute: int,
) -> dict[str, Any]:
    block_id = _id(workspace_id, "block", page_id, b)
    edited = _iso(base, minute)
    actor = _user(workspace_id, block_id)
    text = f"Block {b} on page {page_id[:8]}"
    return {
        "object": "block",
        "id": block_id,
        "type": "paragraph",
        "created_time": edited,
        "last_edited_time": edited,
        "created_by": actor,
        "last_edited_by": actor,
        # has_children=False → the fetcher's block walk never recurses, so
        # `blocks_per_page` blocks in == `blocks_per_page` block records out.
        "has_children": False,
        "parent": {"type": "page_id", "page_id": page_id},
        "paragraph": {"rich_text": [_text_span(text)]},
    }


def _comment_object(
    workspace_id: str, page_id: str, c: int, base: datetime, minute: int,
) -> dict[str, Any]:
    comment_id = _id(workspace_id, "comment", page_id, c)
    created = _iso(base, minute)
    return {
        "object": "comment",
        "id": comment_id,
        "discussion_id": _id(workspace_id, "discussion", page_id),
        "created_time": created,
        "last_edited_time": created,
        "created_by": _user(workspace_id, comment_id),
        "parent": {"type": "page_id", "page_id": page_id},
        "rich_text": [_text_span(f"Comment {c} on {page_id[:8]}")],
    }


def _database_object(
    workspace_id: str, database_id: str, d: int, base: datetime,
) -> dict[str, Any]:
    """The `database` object as returned by `search(object_filter="database")`
    — the surface the REAL planner enumerates to build notion_database shards."""
    created = _iso(base, d)
    return {
        "object": "database",
        "id": database_id,
        "created_time": created,
        "last_edited_time": created,
        "title": [_text_span(f"Database {d}")],
        "parent": {"type": "workspace", "workspace": True},
        "url": f"https://www.notion.so/{database_id.replace('-', '')}",
    }


def _user(workspace_id: str, anchor: str) -> dict[str, Any]:
    uid = _id(workspace_id, "user", anchor)
    return {
        "object": "user",
        "id": uid,
        "name": f"User {_digest(anchor)[:6]}",
        "type": "person",
    }


def _text_span(text: str) -> dict[str, Any]:
    """A Notion `text`-type rich_text span (what the handler flattens via
    `_rich_text_to_plain`)."""
    return {
        "type": "text",
        "text": {"content": text, "link": None},
        "plain_text": text,
        "annotations": {},
        "href": None,
    }


# ---------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------

def _iso(base: datetime, minutes: int) -> str:
    """ISO-8601 UTC `...Z` timestamp (the shape Notion emits + the handler's
    `_parse_iso` consumes)."""
    dt = (base + timedelta(minutes=minutes)).astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"


def _parse_base(base_iso: str) -> datetime:
    s = base_iso[:-1] + "+00:00" if base_iso.endswith("Z") else base_iso
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _id(*parts: Any) -> str:
    """A UUID-shaped, deterministic Notion object id (Notion ids are UUIDs)."""
    h = _digest(*parts)
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update(str(part).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_notion"]
