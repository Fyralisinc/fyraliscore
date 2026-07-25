"""services/ingest/ingestion/planners/figma.py — Figma planner (design).

Per the Gmail/Calendar/Jira/Brex loader precedent (A18.2): Figma's install
record is file-scoped, and the planner needs the 1-to-N active-file list to emit
one shard per file. The enrichment lives in `figma_files`; the SourceOnboarding
loader JSON-aggregates it into `ctx.install["files"]` so the planner stays
stateless (no DB I/O).

Each shard is one file's event stream (named versions + comments collapsed into
an "event" stream). The fetcher walks `GET /v1/files/{key}/events` on first run,
then incrementally via the per-file event high-water cursor.

`ctx.source_client` is None — files are read from DB state (populated at
seed/install time by `FigmaClient.list_files`).

TODO(human): confirm Figma resource taxonomy to shard on. This clones Brex's
one-shard-per-account model keyed on `figma_files.file_key`. Real Figma backfill
enumerates teams → projects → files and derives an event stream per file from
`/versions` + `/comments`; start with one shard per file and extend the
companion-call fan-out once the surface is confirmed.
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_FILE_EVENTS = "figma_file_events"
SHARD_KIND_FILE_SNAPSHOT = "figma_file_snapshot"


def _decode_files(install: Any) -> list[dict[str, Any]]:
    raw = install["files"] if "files" in install else None
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
    return [f for f in decoded if isinstance(f, dict)]


async def plan_shards_figma(ctx: PlannerContext) -> list[Shard]:
    """One event shard and one durable-document snapshot shard per file.

    Reads DB state only (files pre-aggregated by the loader), so
    `ctx.source_client` is None — same as Jira/Calendar/Gmail/Brex.
    """
    install_id = str(ctx.install["id"])
    files = _decode_files(ctx.install)
    team_id = ctx.install["team_id"] if "team_id" in ctx.install else None

    shards: list[Shard] = []
    for f in files:
        file_key = f.get("file_key")
        if not isinstance(file_key, str) or not file_key:
            continue
        shards.append(Shard(
            shard_kind=SHARD_KIND_FILE_EVENTS,
            shard_identifier={
                "shard_kind": SHARD_KIND_FILE_EVENTS,
                "file_key": file_key,
                "file_name": f.get("file_name"),
                # team_id namespaces every external_id (figma:{team_id}:event:…).
                "team_id": team_id,
                "installation_id": install_id,
                # The high-water event-createdAt cursor — None on first sync.
                "event_cursor": f.get("event_cursor"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))
        # A file with zero comments/versions is still valuable company
        # intelligence.  The snapshot shard therefore runs independently of
        # the event stream and emits a design observation on first sync.
        shards.append(Shard(
            shard_kind=SHARD_KIND_FILE_SNAPSHOT,
            shard_identifier={
                "shard_kind": SHARD_KIND_FILE_SNAPSHOT,
                "file_key": file_key,
                "file_name": f.get("file_name"),
                "project_name": f.get("project_name"),
                "team_id": team_id,
                "installation_id": install_id,
                # Populated by the artifact slice migration + loader.  Older
                # installations omit it and safely take a full snapshot.
                "snapshot_version": f.get("snapshot_version"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.figma.planned",
        extra={"file_shards": len(shards), "installation_id": install_id},
    )
    return shards




__all__ = [
    "SHARD_KIND_FILE_EVENTS",
    "SHARD_KIND_FILE_SNAPSHOT",
    "plan_shards_figma",
]
