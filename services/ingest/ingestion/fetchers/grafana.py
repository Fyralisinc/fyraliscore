"""services/ingest/ingestion/fetchers/grafana.py — Grafana annotations backfill/poll fetcher (IN-GRAFANA).

Per A18 (per-source backfill = net-new code) + A16/N1 (cursor advanced by
ShardFetch, opaque to it) + A27.3 (records shaped for the handler).

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `grafana_org_annotations` shard streams one Grafana org's annotations (alerts +
annotation state are org-wide in Grafana, so there is ONE shard per install, not
one-per-resource like Jira's per-project shards). ShardFetch calls this fetcher
in a loop, persisting the returned cursor between calls. Two modes share it:

  - FULL (initial backfill): walk `GET /api/annotations` newest-first in pages of
    `limit`, bounded below by a window floor (GRAFANA_BACKFILL_WINDOW_DAYS, default
    90 days; 0 = all time). The walk advances the UPPER bound backward (`to = min
    time seen - 1ms`) until a short page signals the floor.
  - INCREMENTAL (poll / reconciler reshare): warm-started with an `updated_cursor`
    (the prior run's high-water annotation `time` in epoch ms). The floor is set to
    that high-water so only newer annotations come back; the boundary annotation
    re-fetches and dedups via the versioned external_id.

`end_of_data=True` when a page returns fewer than `limit` rows.

============================================================
WHY ANNOTATIONS (and not the alert state-history API) — v1 scope
============================================================
Grafana auto-creates an annotation for every alert state transition, so the
annotations stream already carries historical alert transitions (tagged with
`alertId` / `newState` / `prevState`) alongside deploy markers and manual notes.
This is the always-available backfill (core Grafana, no Loki backend required).
Live go-forward ALERTS arrive on the separate `grafana:alert` webhook channel;
this fetcher feeds the `grafana:annotation` channel. The full Loki-backed alert
state-history timeline is a documented v2 enhancement.

============================================================
external_id (set by the handler)
============================================================
`grafana:{instance}:annotation:{id}:{time}` — `id` is the stable annotation id,
versioned by `time` so a re-fetched annotation dedups. Each record is tagged with
a private `_fyralis_record_type="annotation"` + `_fyralis_instance` (the instance
host) for external_id namespacing, matching the live webhook's externalURL host.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingest.ingestion.fetchers import FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_ORG_ANNOTATIONS = "grafana_org_annotations"
_DEFAULT_PAGE_SIZE = 100
_MS_PER_DAY = 86_400_000


def _page_size() -> int:
    try:
        return max(1, min(100, int(os.environ.get("GRAFANA_ANNOTATIONS_PAGE_SIZE", "100"))))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


def _backfill_window_ms() -> int:
    """Lower-bound window for the FULL walk, in ms. 0 (env) = all time (no floor)."""
    try:
        days = int(os.environ.get("GRAFANA_BACKFILL_WINDOW_DAYS", "90"))
    except ValueError:
        days = 90
    return max(0, days) * _MS_PER_DAY


def _now_ms() -> int:
    return int(time.time() * 1000)


class GrafanaCursor(BaseModel):
    """Cursor for one org-annotations shard. Round-trips through the opaque dict
    in workflow_states.state_data per the M6.2a contract.

    - high_water_time_ms : max annotation `time` (epoch ms) observed — the
                           warm-start / incremental lower bound AND the
                           reconciler's gap reference point.
    - page_to_ms         : the UPPER bound (epoch ms) for the NEXT page; the walk
                           advances it backward. None on the first page (== now).
    - floor_ms           : the lower `from` bound frozen for this run (None in a
                           full all-time walk).
    - annotations_seen   : diagnostic.
    - seeded             : whether the first-call setup has run.
    """

    model_config = ConfigDict(extra="forbid")

    high_water_time_ms: int | None = None
    page_to_ms: int | None = None
    floor_ms: int | None = None
    annotations_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> GrafanaCursor:
    if c is None:
        return GrafanaCursor()
    return GrafanaCursor.model_validate(c)


def _encode_cursor(c: GrafanaCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real GrafanaClient against the install's auth;
# the mock harness / tests rebind this symbol to inject a fake.
async def _open_grafana_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_grafana_client
    return await open_grafana_client(install)


def _instance_of(install: asyncpg.Record) -> str:
    """The instance host used in external_id. MUST match what the live webhook
    handler derives from the payload `externalURL` host, so a backfilled
    annotation and any live twin share the namespace."""
    base = str(install["base_url"]) if "base_url" in install else ""
    return base.replace("https://", "").replace("http://", "").rstrip("/").split("/")[0]


def _ann_time_ms(ann: dict[str, Any]) -> int | None:
    t = ann.get("time")
    if isinstance(t, bool):
        return None
    if isinstance(t, (int, float)):
        return int(t)
    return None


async def fetch_page_grafana(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of annotations (tagged as records) + next cursor."""
    cur = _decode_cursor(cursor)

    if not cur.seeded:
        warm = shard_identifier.get("updated_cursor")
        if isinstance(warm, (int, float)) and warm:
            # Warm start -> incremental: floor at the prior high-water.
            cur.floor_ms = int(warm)
            cur.high_water_time_ms = int(warm)
        else:
            window = _backfill_window_ms()
            cur.floor_ms = (_now_ms() - window) if window > 0 else None
        cur.seeded = True

    instance = _instance_of(install)
    page_size = _page_size()

    client, close = await _open_grafana_client(install)
    try:
        annotations = await client.list_annotations(
            from_ms=cur.floor_ms,
            to_ms=cur.page_to_ms,
            limit=page_size,
        )

        records: list[dict[str, Any]] = []
        min_time: int | None = None
        for ann in annotations:
            rec = dict(ann)
            rec["_fyralis_record_type"] = "annotation"
            rec["_fyralis_instance"] = instance
            records.append(rec)
            t = _ann_time_ms(ann)
            if t is not None:
                if cur.high_water_time_ms is None or t > cur.high_water_time_ms:
                    cur.high_water_time_ms = t
                if min_time is None or t < min_time:
                    min_time = t

        cur.annotations_seen += len(annotations)
        # Walk backward: the next page's upper bound is just below the oldest
        # `time` seen this page. A page shorter than the limit is the last one.
        is_last = len(annotations) < page_size
        if not is_last and min_time is not None:
            cur.page_to_ms = min_time - 1

        log.info(
            "grafana_backfill_page",
            extra={
                "annotations": len(annotations),
                "is_last": is_last,
                "high_water_time_ms": cur.high_water_time_ms,
            },
        )
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=is_last,
        )
    finally:
        await close()




__all__ = [
    "SHARD_KIND_ORG_ANNOTATIONS",
    "GrafanaCursor",
    "fetch_page_grafana",
]
