"""services/ingest/ingestion/reconcilers/google_calendar.py — gap detection (IN-15).

Per A17 + A18 + A18.3 (reconciler pool-provider seam).

============================================================
GAP DETECTION ALGORITHM
============================================================
After a calendar shard completes, its cursor carries `high_water_updated` —
the max event `updated` timestamp the fetcher walked. The reconciler probes
the LIVE calendar with `events.list?updatedMin=<high_water+1ms>
&showDeleted=true&maxResults=1`: if anything changed STRICTLY AFTER the
high-water, a reshare is emitted for that calendar. A cancellation counts
(showDeleted=true).

EXCLUSIVE FLOOR (convergence — load-bearing). Calendar's `updatedMin` is an
INCLUSIVE lower bound (`updated >= updatedMin`), and `high_water` is by
construction the max `updated` the fetcher already walked — so a probe at
`updatedMin=high_water` ALWAYS re-matches that same boundary event and reports
a phantom gap, re-sharing forever (nothing here caps the re-share cycle;
`gap_baseline_updated` below is written but no fetcher consumes it, so a
re-walk reproduces the identical high-water). We therefore probe at
`high_water + 1ms` so the boundary event is excluded and only genuinely newer
edits trip a reshare. This mirrors the exclusive-floor technique
reconcilers/notion.py (`latest <= high_water` settles) and reconcilers/jira.py
(`_to_jql_minute_after`) already use; Calendar was the lone source still using
a raw inclusive boolean probe.

A reshared shard re-runs the walk with a boosted recency. `external_id`
parity means re-walked events dedup against what backfill already wrote —
only genuinely new/changed events produce new observations. Pragmatic v1:
the probe is one cheap 1-row query per calendar; it can over-reshare but
never under-reshares, and dedup makes re-walks idempotent.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import orjson

from lib.shared.provider_transport import ProviderTransportError
from services.ingest.ingestion.installations import load_source_installation
from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.reconcilers import (
    ReconciliationDecision,
    ResharedShard,
)
from services.ingest.ingestion.workflows.state import load_state
from services.ingest.integrations.google_calendar import metrics


log = logging.getLogger(__name__)


SHARD_KIND_EVENTS = "google_calendar_events"
RESHARE_RECENCY_SCORE = 1.5


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.google_calendar: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


async def _open_calendar_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers.google_calendar import _open_calendar_client
    return await _open_calendar_client(install)


def _decode_identifier(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, (str, bytes)):
        return orjson.loads(raw)
    return dict(raw)


def _exclusive_updated_floor(high_water: str) -> str | None:
    """RFC3339 timestamp 1ms after `high_water`, for use as an EXCLUSIVE
    `updatedMin` floor against Calendar's inclusive lower bound. Returns None
    if `high_water` can't be parsed (caller then skips the probe rather than
    risk a runaway). See the module docstring's EXCLUSIVE FLOOR note."""
    try:
        parsed = datetime.fromisoformat(high_water.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    nxt = (parsed + timedelta(milliseconds=1)).astimezone(timezone.utc)
    return nxt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


async def _load_shard_high_water(pool: Any, shard_id: Any) -> str | None:
    state = await load_state(pool, "shard_fetch", str(shard_id))
    if state is None or not state.state_data:
        return None
    cursor = state.state_data.get("cursor")
    if isinstance(cursor, dict):
        hw = cursor.get("high_water_updated")
        return hw if isinstance(hw, str) else None
    return None


async def _check_one_shard_for_gap(
    *, pool: Any, client: Any, shard: asyncpg.Record,
) -> ResharedShard | None:
    identifier = _decode_identifier(shard["shard_identifier"])
    if identifier.get("shard_kind") != SHARD_KIND_EVENTS:
        return None
    calendar_id = identifier.get("calendar_id")
    owner_email = identifier.get("owner_email") or calendar_id
    if not calendar_id:
        return None

    high_water = await _load_shard_high_water(pool, shard["id"])
    if high_water is None:
        # No reference point (empty calendar / cursor); nothing to compare.
        return None

    # EXCLUSIVE floor = high_water + 1ms. With Calendar's inclusive `updatedMin`
    # this excludes the high-water's own boundary event, so a calendar that
    # hasn't changed since the walk reports no gap and the reconciler converges
    # (a plain `updatedMin=high_water` re-matches the boundary forever).
    floor = _exclusive_updated_floor(high_water)
    if floor is None:
        return None

    try:
        has_updates = await client.has_updates_since(
            calendar_id=calendar_id,
            user_email=owner_email,
            updated_min=floor,
        )
    except ProviderTransportError:
        # A transport outcome is not evidence that the shard is clean.
        # Preserve the typed signal for the workflow scheduler.
        raise
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.google_calendar.probe_failed",
            extra={"shard_id": str(shard["id"]), "error": str(exc)[:200]},
        )
        return None

    if not has_updates:
        return None

    metrics.record_fetch_event("reconcile_gap")
    gap_identifier = dict(identifier)
    gap_identifier["parent_shard_id"] = str(shard["id"])
    gap_identifier["gap_baseline_updated"] = high_water
    return ResharedShard(
        shard=Shard(
            shard_kind=SHARD_KIND_EVENTS,
            shard_identifier=gap_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )


async def reconcile_google_calendar(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await load_source_installation(
        pool,
        source="google_calendar",
        tenant_id=run["tenant_id"],
        installation_id=run["installation_row_id"],
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_calendar_client(install)
    try:
        new_shards: list[ResharedShard] = []
        for shard in active:
            reshared = await _check_one_shard_for_gap(
                pool=pool, client=client, shard=shard,
            )
            if reshared is not None:
                new_shards.append(reshared)
    finally:
        await close()

    if new_shards:
        return ReconciliationDecision(
            has_gaps=True, new_shards=new_shards,
            message=f"google_calendar reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)




__all__ = [
    "RESHARE_RECENCY_SCORE",
    "SHARD_KIND_EVENTS",
    "reconcile_google_calendar",
    "set_pool_provider",
]
