"""services/ingest/ingestion/fetchers/figma.py — Figma backfill/poll fetcher (design).

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
TWO SHARD KINDS
============================================================
A `figma_file_events` shard streams one file's events (named versions +
comments collapsed into an "event" stream).

  - FULL (initial backfill): read Figma's real `GET /versions` and
    `GET /comments` companion endpoints, merge their derived event records,
    then page the merged list locally, newest-first.
  - INCREMENTAL (poll): when the shard is warm-started with an `event_cursor`
    (the high-water event `createdAt`), the fetcher passes `start=<date>` so only
    recent events come back; the overlap re-fetch dedups via the versioned
    external_id.

An independent `figma_file_snapshot` shard reads a shallow version probe, then
downloads `GET /v1/files/{key}` only when the file has changed.  It writes the
complete response to the durable artifact bucket and emits exactly one
`file_snapshot` record.  It runs even when the file has no comments or named
versions.

============================================================
FAN-OUT: ONE FILE -> N EVENT RECORDS + ONE SNAPSHOT RECORD
============================================================
The `figma:event` handler produces ONE observation per record. Each record is
tagged with a private `_fyralis_record_type="event"` the handler branches on.
external_id parity (set by the handler) collapses a backfilled event and its
live-webhook twin to one observation. Because an event's payload can be
re-published with a new `version`, its external_id is versioned by `version`.

Figma has no `/events` endpoint. ``FigmaClient.list_events`` is a local
compatibility method that derives this stream from `/versions` and `/comments`.
The page size is therefore a local fan-out/page boundary and is overridable via
`FIGMA_BACKFILL_PAGE_SIZE`.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingest.integrations.figma.client import FigmaApiError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_FILE_EVENTS = "figma_file_events"
SHARD_KIND_FILE_SNAPSHOT = "figma_file_snapshot"
_DEFAULT_PAGE_SIZE = 100


def _page_size() -> int:
    try:
        return min(500, int(os.environ.get("FIGMA_BACKFILL_PAGE_SIZE", "100")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class FigmaCursor(BaseModel):
    """Cursor for one file shard. Round-trips through the opaque dict in
    workflow_states.state_data.

    - offset            : the list-events pagination offset within a run.
    - high_water_created : max event `createdAt` (ISO) observed — the
                           warm-start / incremental lower bound AND the
                           reconciler's gap reference point.
    - incremental_floor : the `start=` lower bound frozen for this run (None in
                          FULL mode).
    - events_seen       : diagnostic.
    - seeded            : whether the first-call setup ran.
    """

    model_config = ConfigDict(extra="forbid")

    offset: int = 0
    high_water_created: str | None = None
    incremental_floor: str | None = None
    events_seen: int = 0
    seeded: bool = False


class FigmaSnapshotCursor(BaseModel):
    """Terminal cursor for one durable file-snapshot shard."""

    model_config = ConfigDict(extra="forbid")

    version: str | None = None
    content_hash: str | None = None
    fetched: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> FigmaCursor:
    if c is None:
        return FigmaCursor()
    return FigmaCursor.model_validate(c)


def _encode_cursor(c: FigmaCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real FigmaClient against the install's auth;
# the mock harness / tests rebind this symbol to inject a fake.
async def _open_figma_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_figma_client
    return await open_figma_client(install)


def _team_id_of(install: Any, shard_identifier: dict[str, Any]) -> str:
    """Resolve the Figma team id that namespaces every external_id.

    Prefers the install row's `team_id`; falls back to the shard_identifier
    (which the planner/tests may carry it on) so a unit test can drive the
    fetcher without a full install row.
    """
    try:
        if install is not None and "team_id" in install:
            tid = install["team_id"]
            if isinstance(tid, str) and tid:
                return tid
    except (KeyError, TypeError):
        pass
    tid = shard_identifier.get("team_id")
    return tid if isinstance(tid, str) and tid else ""


def _iso_date(iso: str | None) -> str | None:
    """The date portion of an ISO timestamp (Figma `start` is date-granular)."""
    if not isinstance(iso, str) or not iso:
        return None
    return iso[:10]


def _bump_high_water(cur: FigmaCursor, created: Any) -> None:
    if isinstance(created, str) and (
        cur.high_water_created is None or created > cur.high_water_created
    ):
        cur.high_water_created = created


def _record_value(record: Any, key: str) -> Any:
    try:
        return record[key] if key in record else None
    except (KeyError, TypeError):
        return None


def _installation_id_of(install: Any, shard_identifier: dict[str, Any]) -> str:
    value = shard_identifier.get("installation_id") or _record_value(install, "id")
    return str(value) if value is not None and str(value) else ""


def _tenant_id_of(install: Any) -> UUID:
    value = _record_value(install, "tenant_id")
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError("figma snapshot requires an installation tenant_id") from exc


def _snapshot_version(document: dict[str, Any]) -> str | None:
    """Figma's GET /files response version, with safe metadata fallbacks."""
    for key in ("version", "lastModified", "last_modified", "modifiedAt"):
        value = document.get(key)
        if value is not None and str(value):
            return str(value)
    return None


async def _get_file(
    client: Any, file_key: str, *, depth: int | None,
) -> dict[str, Any]:
    """Call newer Figma clients with depth while keeping old fakes compatible."""
    if depth is None:
        result = await client.get_file(file_key)
    else:
        try:
            result = await client.get_file(file_key, depth=depth)
        except TypeError:
            # Existing tests/third-party client wrappers may not yet expose
            # the optional depth kwarg.  A full response is correct, merely
            # less efficient for the version probe.
            result = await client.get_file(file_key)
    if not isinstance(result, dict):
        raise ValueError("figma GET /files response must be an object")
    return result


def _document_projection(document: dict[str, Any]) -> dict[str, Any]:
    """Bounded, searchable projection; full design remains in the artifact."""
    root = document.get("document")
    if not isinstance(root, dict):
        return {"page_names": [], "node_count": 0, "text_preview": ""}

    page_names: list[str] = []
    text_parts: list[str] = []
    node_count = 0
    stack: list[dict[str, Any]] = [root]
    while stack:
        node = stack.pop()
        node_count += 1
        node_type = node.get("type")
        name = node.get("name")
        if node_type == "CANVAS" and isinstance(name, str) and name:
            if len(page_names) < 64:
                page_names.append(name[:300])
        characters = node.get("characters")
        if isinstance(characters, str) and characters.strip() and len(text_parts) < 250:
            text_parts.append(characters.strip()[:500])
        children = node.get("children")
        if isinstance(children, list):
            stack.extend(child for child in reversed(children) if isinstance(child, dict))

    text_preview = " ".join(text_parts)
    if len(text_preview) > 4_000:
        text_preview = text_preview[:3_999] + "…"
    return {
        "page_names": page_names,
        "node_count": node_count,
        "text_preview": text_preview,
    }


async def _store_figma_design_snapshot(
    document: dict[str, Any], *, tenant_id: UUID,
) -> Any:
    """Test seam for the durable S3 write; returns ``StoredArtifact``."""
    from services.ingest.ingestion.artifacts import store_json_artifact

    return await store_json_artifact(
        document,
        tenant_id=tenant_id,
        source="figma",
        kind="figma_document_json",
    )


async def _fetch_snapshot_page(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """Fetch one file design, write it durably, and emit one snapshot record."""
    file_key = shard_identifier.get("file_key")
    if not isinstance(file_key, str) or not file_key:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    tenant_id = _tenant_id_of(install)
    team_id = _team_id_of(install, shard_identifier)
    installation_id = _installation_id_of(install, shard_identifier)
    previous_version = shard_identifier.get("snapshot_version")
    if not isinstance(previous_version, str) or not previous_version:
        previous_version = None

    client, close = await _open_figma_client(install)
    try:
        # A shallow probe avoids fetching a potentially large JSON tree after
        # a scheduled reconciliation when Figma reports the same version.
        metadata = await _get_file(client, file_key, depth=1)
        probed_version = _snapshot_version(metadata)
        if previous_version is not None and probed_version == previous_version:
            terminal = FigmaSnapshotCursor(
                version=probed_version,
                fetched=False,
            )
            return FetchResult(
                records=[], next_cursor=terminal.model_dump(mode="json"),
                end_of_data=True,
            )

        document = await _get_file(client, file_key, depth=None)
        artifact = await _store_figma_design_snapshot(document, tenant_id=tenant_id)
        version = _snapshot_version(document) or probed_version or artifact.content_hash
        last_modified = (
            document.get("lastModified")
            or document.get("last_modified")
            or metadata.get("lastModified")
            or metadata.get("last_modified")
        )
        file_name = document.get("name") or metadata.get("name") or shard_identifier.get("file_name")
        if not isinstance(file_name, str) or not file_name:
            file_name = file_key
        captured_at = datetime.now(timezone.utc).isoformat()
        record = {
            "_fyralis_record_type": "file_snapshot",
            "_fyralis_file_key": file_key,
            "_fyralis_team_id": team_id,
            "_fyralis_installation_id": installation_id,
            "file": {
                "key": file_key,
                "name": file_name,
                "project_name": shard_identifier.get("project_name"),
            },
            "snapshot": {
                "version": version,
                "last_modified": last_modified,
                "captured_at": captured_at,
                "projection": _document_projection(document),
            },
            # This private descriptor never enters observations.content.  The
            # handler exposes only its public blob reference and the writer
            # creates blobs/observation_artifacts in the observation tx.
            "artifact": artifact.private_descriptor(),
        }
        terminal = FigmaSnapshotCursor(
            version=version,
            content_hash=artifact.content_hash,
            fetched=True,
        )
        return FetchResult(
            records=[record],
            next_cursor=terminal.model_dump(mode="json"),
            end_of_data=True,
        )
    finally:
        await close()


async def fetch_page_figma(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of Figma events, or one terminal durable snapshot."""
    if shard_identifier.get("shard_kind") == SHARD_KIND_FILE_SNAPSHOT:
        return await _fetch_snapshot_page(install, shard_identifier, cursor)
    file_key = shard_identifier.get("file_key")
    if not isinstance(file_key, str) or not file_key:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    # The team_id namespaces every external_id (figma:{team_id}:event:…) so two
    # tenants' identical synthetic event ids never collapse on the global
    # observations UNIQUE(source_channel, external_id, occurred_at). It rides on
    # the install row; the shard_identifier carries it as a fallback for tests.
    team_id = _team_id_of(install, shard_identifier)

    cur = _decode_cursor(cursor)
    records: list[dict[str, Any]] = []

    client, close = await _open_figma_client(install)
    try:
        # First-call setup: warm-start mode (no snapshot record — figma is a
        # pure event stream, so the per-tenant observation count equals the
        # event count).
        if not cur.seeded:
            warm = shard_identifier.get("event_cursor")
            if isinstance(warm, str) and warm:
                cur.incremental_floor = warm  # warm start -> incremental
                cur.high_water_created = warm
            cur.seeded = True

        try:
            events, next_offset, total = await client.list_events(
                file_key,
                limit=_page_size(),
                offset=cur.offset,
                start=_iso_date(cur.incremental_floor),
            )
        except FigmaApiError as exc:
            if (exc.context or {}).get("code") == "figma_api_rate_limited" or \
               getattr(exc, "_code", None) == "figma_api_rate_limited":
                log.info("figma_backfill_rate_limited",
                         extra={"file_key": file_key})
                return FetchResult(
                    records=records, next_cursor=_encode_cursor(cur),
                    end_of_data=False,
                )
            raise

        for event in events:
            records.append({
                "_fyralis_record_type": "event",
                "_fyralis_file_key": file_key,
                "_fyralis_team_id": team_id,
                "event": event,
            })
            _bump_high_water(cur, event.get("createdAt") or event.get("created_at"))

        cur.events_seen += len(events)
        is_last = next_offset is None
        cur.offset = next_offset if next_offset is not None else cur.offset

        log.info(
            "figma_backfill_page",
            extra={"file_key": file_key, "events": len(events),
                   "records": len(records), "is_last": is_last},
        )
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=is_last,
        )
    finally:
        await close()


FETCHER_DISPATCH["figma"] = fetch_page_figma


__all__ = [
    "SHARD_KIND_FILE_EVENTS",
    "SHARD_KIND_FILE_SNAPSHOT",
    "FigmaCursor",
    "FigmaSnapshotCursor",
    "fetch_page_figma",
]
