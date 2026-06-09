"""MockMiroClient — Miro whiteboard API surface used by the backfill.

In-process replacement for `MiroClient` (services/ingest/integrations/miro/
client.py). Implements the read methods the Miro backfill chain calls:

  - get_board(board_id) -> dict
      `GET /boards/{id}` body (board-metadata probe).
  - list_boards() -> list[dict]
      `GET /boards` (seed-time enumeration).
  - list_items(board_id, *, limit, cursor) -> (items, next_cursor, total)
      `GET /boards/{id}/items`, OPAQUE-CURSOR-paginated, honouring `limit`.
      `next_cursor is None` is terminal — exactly the real client's contract.
      The cursor is an opaque string the caller round-trips verbatim (here it
      encodes the next offset; the fetcher must NOT interpret it).

Every public method calls `self._check_fault()` first (A21), so the fetcher /
reconciler see real `MiroApiError` types with the right `code` on a configured
fault — same as production.

`fixture` shape: see fixtures/miro_generator.py::make_miro.
"""
from __future__ import annotations

from typing import Any, NoReturn

from lib.shared.errors import MiroApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


# The opaque cursor the mock hands back. The caller treats it as opaque; the
# mock encodes the next offset behind this prefix so the real-client cursor
# contract (round-trip an opaque token) is exercised.
_CURSOR_PREFIX = "miro-cursor:"


class MockMiroClient(_MockBase):
    """Stateful in-process replacement for `MiroClient`.

    Pagination is opaque-cursor based (like the real client): the cursor encodes
    the next slice offset into the board's item list and `limit` caps the page.
    """

    def __init__(
        self,
        *,
        fixture: dict[str, Any],
        profile: FaultProfile = HAPPY_PATH,
    ) -> None:
        super().__init__(profile=profile)
        self._fixture = fixture

    # ---- Read surface ----
    async def list_boards(self) -> list[dict[str, Any]]:
        """`GET /boards` — all boards visible to the org token."""
        self._check_fault()
        boards = self._fixture.get("boards", {})
        order = self._fixture.get("board_order") or list(boards.keys())
        out: list[dict[str, Any]] = []
        for bid in order:
            board = boards.get(bid)
            if isinstance(board, dict):
                out.append({k: v for k, v in board.items() if k != "items"})
        return out

    async def get_board(self, board_id: str) -> dict[str, Any]:
        """`GET /boards/{id}` — the board-metadata probe body."""
        self._check_fault()
        board = self._fixture.get("boards", {}).get(board_id)
        if not isinstance(board, dict):
            raise MiroApiError(
                f"MockMiroClient: board {board_id!r} not found",
                code="miro_api_not_found",
                context={"board_id": board_id},
            )
        # Return the board body WITHOUT the embedded item list (the real
        # `GET /boards/{id}` does not inline items).
        return {k: v for k, v in board.items() if k != "items"}

    async def list_items(
        self,
        board_id: str,
        *,
        limit: int = 50,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None, int]:
        """`GET /boards/{id}/items` — opaque-cursor-paginated.

        Returns `(items, next_cursor, total)`. `next_cursor is None` signals the
        last page (matching the real client's terminal contract).
        """
        self._check_fault()
        items = self._items_for(board_id)
        total = len(items)
        offset = _decode_cursor(cursor)
        # Honour the fixture's page-size cap the same way the real client bounds
        # the page (and how the github mock caps per_page).
        page_cap = int(self._fixture.get("page_size", limit) or limit)
        eff_limit = min(limit, page_cap)
        page = items[offset:offset + eff_limit]
        next_offset = offset + len(page)
        is_last = next_offset >= total or not page
        next_cursor = None if is_last else _encode_cursor(next_offset)
        return page, next_cursor, total

    async def has_items_since(self, board_id: str, since: str) -> bool:
        """Reconciler probe convenience: True if any item is modified on/after
        `since`. Mirrors reconcilers/miro.py's first-page list + compare."""
        self._check_fault()
        floor = since
        return any(_item_modified(it) >= floor for it in self._items_for(board_id))

    # ---- Helpers ----
    def _items_for(self, board_id: str) -> list[dict[str, Any]]:
        board = self._fixture.get("boards", {}).get(board_id)
        if not isinstance(board, dict):
            return []
        items = board.get("items")
        return list(items) if isinstance(items, list) else []

    # ---- Fault raisers (surface the real MiroApiError + codes) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise MiroApiError(
            "MockMiroClient: rate limit (X2 fault)",
            code="miro_api_rate_limited",
            context={"http_status": 429, "retry_after": "1"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise MiroApiError(
            "MockMiroClient: 503 (X2 fault)",
            code="miro_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise MiroApiError(
            "MockMiroClient: 401 token rejected (X2 fault)",
            code="miro_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise MiroApiError(
            "MockMiroClient: transient transport error (X2 fault)",
            code="miro_api_error",
            context={"error_type": "TransportError"},
        )


def _encode_cursor(offset: int) -> str:
    return f"{_CURSOR_PREFIX}{offset}"


def _decode_cursor(cursor: str | None) -> int:
    if not cursor:
        return 0
    if cursor.startswith(_CURSOR_PREFIX):
        try:
            return int(cursor[len(_CURSOR_PREFIX):])
        except ValueError:
            return 0
    return 0


def _item_modified(item: dict[str, Any]) -> str:
    iso = item.get("modifiedAt") or item.get("createdAt") or ""
    return iso if isinstance(iso, str) else ""


__all__ = ["MockMiroClient"]
