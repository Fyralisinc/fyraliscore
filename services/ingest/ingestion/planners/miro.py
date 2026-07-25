"""services/ingest/ingestion/planners/miro.py — Miro planner.

Per the Brex/Jira loader precedent (A18.2): Miro's install record is
board-scoped, and the planner needs the 1-to-N active-board list to emit one
shard per board. The enrichment lives in `miro_boards`; the SourceOnboarding
loader JSON-aggregates it into `ctx.install["boards"]` so the planner stays
stateless (no DB I/O).

Each shard is one board's item stream. The fetcher walks
`GET /boards/{id}/items` (opaque cursor) on first run, then incrementally via
the per-board item high-water cursor.

`ctx.source_client` is None — boards are read from DB state (populated at
seed/install time by `MiroClient.list_boards`).

TODO(human): confirm Miro resource taxonomy to shard on. This clones Brex's
one-shard-per-account model keyed on `miro_boards.board_id`. If a single board's
item count is unbounded, a finer shard (per item-type or per frame) may be
needed; start with one shard per board and refine once the surface is confirmed.
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_BOARD_ITEMS = "miro_board_items"


def _decode_boards(install: Any) -> list[dict[str, Any]]:
    raw = install["boards"] if "boards" in install else None
    if raw is None:
        return []
    if isinstance(raw, (str, bytes)):
        try:
            decoded = orjson.loads(raw)
        except orjson.JSONDecodeError:
            return []
    elif isinstance(raw, list):
        decoded = raw
    else:
        return []
    return [b for b in decoded if isinstance(b, dict)]


async def plan_shards_miro(ctx: PlannerContext) -> list[Shard]:
    """One `miro_board_items` shard per active board.

    Reads DB state only (boards pre-aggregated by the loader), so
    `ctx.source_client` is None — same as Brex/Jira/Calendar/Gmail.
    """
    install_id = str(ctx.install["id"])
    boards = _decode_boards(ctx.install)
    # The org id namespaces every observation's external_id
    # (`miro:{org_id}:item:…`), keeping two tenants' identical board/item ids
    # distinct under the global UNIQUE(source_channel, external_id, occurred_at)
    # index. Falls back to the install id when the org id was not resolved at
    # seed time (still install-unique, so the namespacing invariant holds).
    org_id = (
        ctx.install["org_id"]
        if "org_id" in ctx.install and ctx.install["org_id"]
        else install_id
    )

    shards: list[Shard] = []
    for board in boards:
        board_id = board.get("board_id")
        if not isinstance(board_id, str) or not board_id:
            continue
        shards.append(Shard(
            shard_kind=SHARD_KIND_BOARD_ITEMS,
            shard_identifier={
                "shard_kind": SHARD_KIND_BOARD_ITEMS,
                "board_id": board_id,
                "board_name": board.get("board_name"),
                "org_id": str(org_id),
                "installation_id": install_id,
                # The high-water item-modifiedAt cursor — None on first sync.
                "item_cursor": board.get("item_cursor"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.miro.planned",
        extra={"board_shards": len(shards), "installation_id": install_id},
    )
    return shards




__all__ = ["SHARD_KIND_BOARD_ITEMS", "plan_shards_miro"]
