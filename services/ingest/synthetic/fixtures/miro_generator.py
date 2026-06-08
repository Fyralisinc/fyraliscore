"""Miro boards/items fixture generator (whiteboard / design source).

`make_miro(*, org_id, boards=1, items_per_board=4, seed=None)` produces a
deterministic Miro install fixture shaped to feed `MockMiroClient`. It mirrors
the brex/github generators: every field is derived via `hashlib` (stable across
runs), timestamps land in 2026-01, and the shape is exactly what the mock client
paginates over.

Fixture shape (consumed by `MockMiroClient(fixture=...)`):
    {
      "org_id": "<org_id>",
      "boards": {
        "<board_id>": {
          # the `GET /boards/{id}` body (board-metadata probe).
          "id": "<board_id>",
          "name": "...", "type": "board",
          "createdAt": "2026-01-05T00:00:00Z", ...,
          # the board's items, paginated by the mock client via opaque cursor.
          "items": [ {<item>}, ... ],
        },
        ...
      },
      "board_order": ["<board_id>", ...],   # planner shard order
      "page_size": 50,
    }

The fetcher (services/ingest/ingestion/fetchers/miro.py) emits ONE `item` record
per item and NO board snapshot — so the observation count per board is exactly
`items_per_board`. With the default `boards=1, items_per_board=4` a single
tenant produces exactly 4 backfill observations.
"""
from __future__ import annotations

import hashlib
from typing import Any


# Default item types cycled across a board's items. Mirrors Miro's item `type`
# field (sticky_note / shape / text / card / frame / connector …).
_ITEM_TYPES = ("sticky_note", "shape", "text", "card")


def make_miro(
    *,
    org_id: str,
    boards: int = 1,
    items_per_board: int = 4,
    item_types: list[str] | None = None,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 50,
    seed: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic Miro install fixture.

    Args:
      org_id: The Miro org id — the install-namespacing identifier that scopes
        every observation's external_id (`miro:{org_id}:item:…`). REQUIRED so
        two tenants' fixtures are distinct.
      boards: Number of boards (one shard each in the planner).
      items_per_board: Items on each board's stream (== observations/board).
      item_types: Optional override for the per-item `type` cycle.
      base_iso: Anchor timestamp (2026-01); items are spaced backwards from it
        so the list is newest-first.
      page_size: The mock client's per-page cap for `list_items`.
      seed: Optional namespacing salt mixed into the synthetic `board_id` /
        `item_id`. The org_id already namespaces the external_id; `seed`
        additionally varies the board/item ids so multiple fixtures for the
        SAME org stay distinct. Default None preserves org-derived ids.

    Returns:
      Fixture dict consumable by `MockMiroClient(fixture=...)`.
    """
    types = item_types or list(_ITEM_TYPES)
    base_date = base_iso[:10]  # YYYY-MM-DD anchor for spacing.
    salt = seed if seed else org_id

    boards_map: dict[str, dict[str, Any]] = {}
    board_order: list[str] = []
    for b in range(boards):
        board_id = f"board_{_digest(salt, 'board', b)[:16]}"
        board_order.append(board_id)
        items = [
            _item(board_id, salt, idx, base_date, types)
            for idx in range(items_per_board)
        ]
        boards_map[board_id] = {
            "id": board_id,
            "name": f"Design Board {b + 1}",
            "type": "board",
            "createdAt": f"{base_date}T00:00:00Z",
            "modifiedAt": f"{base_date}T00:00:00Z",
            # Newest-first item stream (the mock paginates this slice).
            "items": items,
        }

    return {
        "org_id": org_id,
        "boards": boards_map,
        "board_order": board_order,
        "page_size": page_size,
    }


def _item(
    board_id: str, salt: str, idx: int, base_date: str, types: list[str],
) -> dict[str, Any]:
    """One deterministic Miro board item, newest-first by `idx`.

    idx=0 is the newest; later indices are older. Both `modifiedAt` and
    `createdAt` land in 2026-01 so the handler's occurred_at is always a 2026
    timestamp. The item id mixes in the salt so two tenants' items are DISTINCT.
    """
    item_id = f"item_{_digest(salt, board_id, 'item', idx)[:20]}"
    item_type = types[idx % len(types)]
    # Space items one hour apart, newest first: idx 0 -> 23:00, etc.
    hour = 23 - (idx % 24)
    iso = f"{base_date}T{hour:02d}:00:00Z"
    author = f"user_{_digest(salt, 'author', idx % 3)[:8]}"
    return {
        "id": item_id,
        "boardId": board_id,
        "type": item_type,
        "data": {"content": f"{item_type} {idx} on {board_id[:12]}"},
        "createdBy": {"id": author, "type": "user"},
        "modifiedBy": {"id": author, "type": "user"},
        "position": {"x": float(idx * 100), "y": float(idx * 50)},
        "geometry": {"width": 200.0, "height": 120.0},
        "createdAt": iso,
        "modifiedAt": iso,
        # Monotonic-ish version discriminator (the external_id versioner).
        "version": str(idx + 1),
    }


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_miro"]
