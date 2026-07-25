"""services/ingest/ingestion/planners/telegram.py — Telegram planner (IN-TELEGRAM).

Per ingestion LLD §3 + the Jira/Mercury loader precedent (A18.2): the install
record is account-scoped, but the planner needs the 1-to-N dialog list to emit
one shard per dialog. The enrichment lives in `telegram_dialogs`; the
SourceOnboarding loader JSON-aggregates it onto `ctx.install["dialogs"]` so the
planner stays stateless (no DB I/O), exactly like Jira's project aggregation.

Each shard is one dialog's message history. The fetcher pages it backwards via
`messages.getHistory` cursored on `offset_id`, warm-starting from the per-dialog
`offset_id_cursor` high-water for an incremental re-walk.

`ctx.source_client` is None — dialogs are read from DB state (populated at
install time by `TelegramClient.iter_dialogs` via `finalize_install`).
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_DIALOG_HISTORY = "telegram_dialog_history"


def _decode_dialogs(install: Any) -> list[dict[str, Any]]:
    """Decode the JSON-aggregated `dialogs` column on the install record."""
    raw = install["dialogs"] if "dialogs" in install else None
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
    return [d for d in decoded if isinstance(d, dict)]


async def plan_shards_telegram(ctx: PlannerContext) -> list[Shard]:
    """One `telegram_dialog_history` shard per active dialog.

    Reads DB state only (dialogs pre-aggregated by the loader), so
    `ctx.source_client` is None — same as Jira/Mercury.
    """
    install_id = str(ctx.install["id"])
    dialogs = _decode_dialogs(ctx.install)

    shards: list[Shard] = []
    for d in dialogs:
        dialog_id = d.get("dialog_id")
        if not isinstance(dialog_id, int):
            continue
        shards.append(Shard(
            shard_kind=SHARD_KIND_DIALOG_HISTORY,
            shard_identifier={
                "shard_kind": SHARD_KIND_DIALOG_HISTORY,
                "dialog_id": dialog_id,
                "dialog_kind": d.get("dialog_kind") or "chat",
                "access_hash": d.get("access_hash"),
                "dialog_title": d.get("title"),
                "installation_id": install_id,
                # The per-dialog offset_id high-water — None on first full sync.
                "offset_id_cursor": d.get("offset_id_cursor"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.telegram.planned",
        extra={"dialog_shards": len(shards), "installation_id": install_id},
    )
    return shards




__all__ = ["SHARD_KIND_DIALOG_HISTORY", "plan_shards_telegram"]
