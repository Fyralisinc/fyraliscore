"""services/ingest/ingestion/planners/google_drive.py — Drive planner (IN-16).

Mirrors the Calendar planner (A18.2 loader pattern): the Drive install record
is workspace-scoped, but the planner needs the 1-to-N active-target list to
emit one shard per drive. The enrichment lives in `google_drive_targets`; the
SourceOnboarding loader JSON-aggregates it into `ctx.install["targets"]` so the
planner stays stateless (no DB I/O).

Each shard is one drive's file stream — a user's My Drive (D6) or a Shared
Drive. The fetcher walks files.list windowed by modifiedTime on first run, then
incrementally via the Changes API start-page-token.
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_FILES = "google_drive_files"


def _decode_targets(install: Any) -> list[dict[str, Any]]:
    """Decode the JSON-aggregated `targets` column on the install record."""
    raw = install["targets"] if "targets" in install else None
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
    return [t for t in decoded if isinstance(t, dict)]


async def plan_shards_google_drive(ctx: PlannerContext) -> list[Shard]:
    """One `google_drive_files` shard per active target.

    Reads DB state only (targets pre-aggregated by the loader), so
    `ctx.source_client` is None — same as Gmail / Calendar.
    """
    install_id = str(ctx.install["id"])
    targets = _decode_targets(ctx.install)

    shards: list[Shard] = []
    for t in targets:
        owner_email = t.get("owner_email")
        if not isinstance(owner_email, str) or not owner_email:
            continue
        shards.append(Shard(
            shard_kind=SHARD_KIND_FILES,
            shard_identifier={
                "shard_kind": SHARD_KIND_FILES,
                "drive_kind": t.get("drive_kind") or "my_drive",
                "drive_id": t.get("drive_id") or "my-drive",
                "owner_email": owner_email,
                "installation_id": install_id,
                "start_page_token": t.get("start_page_token"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.google_drive.planned",
        extra={"drive_shards": len(shards), "installation_id": install_id},
    )
    return shards




__all__ = ["SHARD_KIND_FILES", "plan_shards_google_drive"]
