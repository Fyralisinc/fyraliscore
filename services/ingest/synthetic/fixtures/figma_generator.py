"""Figma files/events fixture generator (design).

`make_figma(*, team_id, events=N, ...)` produces a deterministic Figma install
fixture shaped to feed `MockFigmaClient`. It mirrors the brex/github/gmail
generators: every field is derived via `hashlib` (stable across runs),
timestamps land in 2026-01, and the shape is exactly what the mock client
paginates over.

Fixture shape (consumed by `MockFigmaClient(fixture=...)`):
    {
      "team_id": "<team_id>",
      "files": {
        "<file_key>": {
          # the `GET /v1/files/{key}/meta` body (recency probe).
          "key": "<file_key>",
          "name": "...", "lastModified": "2026-01-05T00:00:00Z", ...,
          # newest-first event list paginated by the mock client.
          "events": [ {<event>}, ... ],
        },
        ...
      },
      "file_order": ["<file_key>", ...],   # planner shard order
      "page_size": 100,
    }

The fetcher (services/ingest/ingestion/fetchers/figma.py) emits ONE `event`
record per event (NO snapshot, unlike Brex) — so the observation count per
tenant is exactly the total event count across files. With a single file and
`events=4`, that is exactly 4 backfill observations per tenant.
"""
from __future__ import annotations

import hashlib
from typing import Any


# Default per-event Figma Webhooks V2 event types cycled across a file's stream.
# None map to the handler's `_STATE_CHANGE_EVENTS`, so the happy path is all
# signals; the handler still versions external_id by `version` either way.
_EVENT_TYPES = (
    "FILE_VERSION_UPDATE",
    "FILE_COMMENT",
    "LIBRARY_PUBLISH",
    "DEV_MODE_STATUS_UPDATE",
)


def make_figma(
    *,
    team_id: str,
    events: int = 4,
    files: int = 1,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 100,
    seed: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic Figma install fixture.

    Args:
      team_id: The Figma team id. Namespaces every external_id
        (`figma:{team_id}:event:…`) so two tenants' identical synthetic event
        ids never collapse on the global observations UNIQUE(source_channel,
        external_id, occurred_at) index — this is why the gate passes a
        per-tenant team_id.
      events: Number of events per file (each yields exactly one observation).
      files: Number of files (one shard each in the planner). Default 1 so
        `events=4` gives exactly 4 observations/tenant.
      base_iso: Anchor timestamp (2026-01); events are spaced backwards from it
        so the list is newest-first.
      page_size: The mock client's per-page cap for `list_events`.
      seed: Optional namespacing salt mixed into the synthetic ids in ADDITION
        to team_id. team_id already makes ids tenant-unique; `seed` is an extra
        salt for parametrized runs. Default None preserves team_id-only ids.

    Returns:
      Fixture dict consumable by `MockFigmaClient(fixture=...)`.
    """
    salt = seed or team_id
    base_date = base_iso[:10]  # YYYY-MM-DD anchor for spacing.

    files_map: dict[str, dict[str, Any]] = {}
    file_order: list[str] = []
    for fi in range(files):
        file_key = _digest("figma-file", salt, fi)[:22]
        file_order.append(file_key)
        evs = [
            _event(team_id, file_key, salt, idx, base_date)
            for idx in range(events)
        ]
        files_map[file_key] = {
            "key": file_key,
            "name": f"Design File {fi + 1}",
            "editorType": "figma",
            "role": "owner",
            "lastModified": f"{base_date}T00:00:00Z",
            "version": _digest(file_key, "version")[:12],
            # Newest-first event stream (the mock paginates this slice).
            "events": evs,
        }

    return {
        "team_id": team_id,
        "files": files_map,
        "file_order": file_order,
        "page_size": page_size,
    }


def _event(
    team_id: str, file_key: str, salt: str, idx: int, base_date: str,
) -> dict[str, Any]:
    """One deterministic Figma event, newest-first by `idx`.

    idx=0 is the newest; later indices are older. `createdAt` lands in 2026-01 so
    the handler's occurred_at is always a 2026 timestamp. Each event id is
    DISTINCT (digest over idx) so N events yield N distinct external_ids ->
    N observations.
    """
    event_id = f"evt_{_digest(salt, file_key, 'event', idx)[:20]}"
    event_type = _EVENT_TYPES[idx % len(_EVENT_TYPES)]
    # Space events one hour apart, newest first: idx 0 -> 23:00, etc.
    hour = 23 - (idx % 24)
    iso = f"{base_date}T{hour:02d}:00:00Z"
    actor = f"user_{_digest(event_id, 'actor')[:8]}"
    # version discriminator — a stable per-event token so re-fetch dedups but a
    # re-publish (new version) re-observes.
    version = _digest(event_id, "v")[:12]
    return {
        "id": event_id,
        "event_id": event_id,
        "event_type": event_type,
        "type": event_type,
        "team_id": team_id,
        "file_key": file_key,
        "fileKey": file_key,
        "version": version,
        "label": f"{event_type.replace('_', ' ').title()} {idx}",
        "description": f"event {idx} on {file_key}",
        "message": f"comment {idx}" if event_type == "FILE_COMMENT" else None,
        "status": "ready_for_dev" if event_type == "DEV_MODE_STATUS_UPDATE" else None,
        "triggered_by": {"id": actor, "handle": actor},
        "user": actor,
        "createdAt": iso,
        "created_at": iso,
    }


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_figma"]
