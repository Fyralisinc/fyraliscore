"""MockNotionClient — Notion REST surface used by IN-14 backfill/poll.

In-process replacement for `NotionClient`
(`services/ingest/integrations/notion/client.py`). Implements every read
method the production tree-walk fetcher (`fetchers/notion.py`), reconciler
(`reconcilers/notion.py`), and planner (`planners/notion.py`) call against
the `_open_notion_client` seam — each returning the SAME
`(results, next_cursor, has_more)` triple the real client returns from
`_unwrap_list`:

  - search(object_filter="database"|"page", start_cursor=None)
      -> databases (planner enumeration) / loose pages (page_tree walk)
  - query_database(database_id, start_cursor=None)   -> the database's rows
  - list_block_children(block_id, start_cursor=None) -> a page/block's blocks
  - list_comments(block_id, start_cursor=None)       -> a page/block's comments
  - latest_database_edit(database_id) -> str | None  (reconciler gap probe)
  - latest_page_edit()                -> str | None  (reconciler gap probe)

Pagination mirrors the real opaque-cursor contract: every list call is
capped at the fixture's `page_size`; the returned `next_cursor` is the
opaque string `"off:<n>"` (the next start offset) and is `None` iff
`has_more` is False — so the fetcher threads it back verbatim, exactly like
a real Notion `next_cursor`.

Faults: every public method calls `self._check_fault()` first (A21). The
four raisers surface `NotionApiError` with the production `code` values AND
`context["http_status"]`, because the fetcher's rate-limit fallback keys on
`(e.context or {}).get("http_status") == 429` (NOT on `code`).
"""
from __future__ import annotations

from typing import Any, NoReturn

from lib.shared.errors import NotionApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockNotionClient(_MockBase):
    """In-process replacement for `NotionClient`, driven by a `make_notion`
    fixture.

    `fixture` shape (per `make_notion`):
        {
          "workspace_id": "x3-notion-ws",
          "page_size": 100,
          "databases": [
            {"database_id": "...", "object_summary": {<search "database">},
             "rows": [<page object>, ...]},  # query_database results
            ...
          ],
          "loose_pages": [<page object>, ...],          # search "page" results
          "blocks_by_page": {page_id: [<block>, ...]},  # list_block_children
          "comments_by_page": {page_id: [<comment>, ...]},  # list_comments
        }
    """

    def __init__(
        self,
        *,
        fixture: dict[str, Any],
        profile: FaultProfile = HAPPY_PATH,
    ) -> None:
        super().__init__(profile=profile)
        self._fixture = fixture
        self._page_size = int(fixture.get("page_size", 100)) or 100

    # -----------------------------------------------------------------
    # Public read surface (mirrors NotionClient)
    # -----------------------------------------------------------------
    async def search(
        self,
        *,
        object_filter: str | None = None,
        start_cursor: str | None = None,
        page_size: int = 100,
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """`POST /v1/search`. `object_filter == "database"` returns the
        workspace's database objects (the surface the REAL planner enumerates
        to build notion_database shards); `object_filter == "page"` returns
        the LOOSE pages (database rows are reached via `query_database`, so
        they are intentionally excluded here — the page_tree walk skips them
        anyway via `_is_database_row`). `None` returns both."""
        self._check_fault()
        if object_filter == "database":
            results = [db["object_summary"] for db in self._fixture.get("databases", [])]
        elif object_filter == "page":
            results = list(self._fixture.get("loose_pages", []))
        else:
            results = [db["object_summary"] for db in self._fixture.get("databases", [])]
            results += list(self._fixture.get("loose_pages", []))
        return self._paginate(results, start_cursor, page_size)

    async def query_database(
        self,
        database_id: str,
        *,
        start_cursor: str | None = None,
        page_size: int = 100,
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """`POST /v1/databases/{id}/query` — the database's rows (page objects)."""
        self._check_fault()
        rows = self._rows_for(database_id)
        return self._paginate(rows, start_cursor, page_size)

    async def list_block_children(
        self,
        block_id: str,
        *,
        start_cursor: str | None = None,
        page_size: int = 100,
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """`GET /v1/blocks/{id}/children` — child blocks of a page/block."""
        self._check_fault()
        blocks = list(self._fixture.get("blocks_by_page", {}).get(block_id, []))
        return self._paginate(blocks, start_cursor, page_size)

    async def list_comments(
        self,
        block_id: str,
        *,
        start_cursor: str | None = None,
        page_size: int = 100,
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """`GET /v1/comments?block_id=…` — comments on a page/block."""
        self._check_fault()
        comments = list(self._fixture.get("comments_by_page", {}).get(block_id, []))
        return self._paginate(comments, start_cursor, page_size)

    async def latest_database_edit(self, database_id: str) -> str | None:
        """`last_edited_time` of the most-recently-edited row in a database,
        or None if empty — the reconciler's notion_database gap probe."""
        self._check_fault()
        rows = self._rows_for(database_id)
        return self._max_edit(rows)

    async def latest_page_edit(self) -> str | None:
        """`last_edited_time` of the most-recently-edited LOOSE page (one NOT
        owned by a database) — the reconciler's notion_page_tree gap probe.
        Mirrors the real client's database-row exclusion."""
        self._check_fault()
        loose = [
            p for p in self._fixture.get("loose_pages", [])
            if not _is_database_row(p)
        ]
        return self._max_edit(loose)

    async def aclose(self) -> None:
        """No-op (the mock holds no httpx client); present for surface parity."""
        return None

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------
    def _rows_for(self, database_id: str) -> list[dict[str, Any]]:
        for db in self._fixture.get("databases", []):
            if db.get("database_id") == database_id:
                return list(db.get("rows", []))
        return []

    @staticmethod
    def _max_edit(objs: list[dict[str, Any]]) -> str | None:
        edits = [
            e for e in (
                o.get("last_edited_time") or o.get("created_time") for o in objs
            )
            if isinstance(e, str)
        ]
        return max(edits) if edits else None

    def _paginate(
        self,
        items: list[dict[str, Any]],
        start_cursor: str | None,
        page_size: int,
    ) -> tuple[list[dict[str, Any]], str | None, bool]:
        """Opaque-cursor pagination capped at the fixture's page_size, matching
        the real `(results, next_cursor, has_more)` contract."""
        start = self._decode_cursor(start_cursor)
        per_page = min(int(page_size or self._page_size), self._page_size)
        end = start + per_page
        page = items[start:end]
        has_more = end < len(items)
        next_cursor = self._encode_cursor(end) if has_more else None
        return page, next_cursor, has_more

    @staticmethod
    def _encode_cursor(offset: int) -> str:
        return f"off:{offset}"

    @staticmethod
    def _decode_cursor(cursor: str | None) -> int:
        if not cursor:
            return 0
        try:
            return int(cursor.split(":", 1)[1])
        except (IndexError, ValueError):
            return 0

    # -----------------------------------------------------------------
    # Fault raisers (production NotionApiError codes — A21)
    # -----------------------------------------------------------------
    def _raise_rate_limit(self) -> NoReturn:
        # The fetcher's rate-limit fallback keys on context["http_status"]==429,
        # NOT on code — so the status MUST be present for the fallback to fire.
        raise NotionApiError(
            "MockNotionClient: rate limit (429), retry budget exhausted (X2 fault)",
            code="notion_api_rate_limited",
            context={"http_status": 429, "retry_after": "1"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise NotionApiError(
            "MockNotionClient: 503 (X2 fault)",
            code="notion_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise NotionApiError(
            "MockNotionClient: 401 integration token rejected (X2 fault)",
            code="notion_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise NotionApiError(
            "MockNotionClient: transient transport error (X2 fault)",
            code="notion_api_error",
            context={"error_type": "TransportError"},
        )


def _is_database_row(page: dict[str, Any]) -> bool:
    parent = page.get("parent") or {}
    return isinstance(parent, dict) and parent.get("type") == "database_id"


__all__ = ["MockNotionClient"]
