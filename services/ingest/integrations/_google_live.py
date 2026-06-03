"""services/ingest/integrations/_google_live.py — shared live delta-drain.

Calendar and Drive both ingest near-real-time deltas the same way: drive the
EXISTING per-source fetcher incrementally from the stored cursor
(`sync_token` / `start_page_token`), feed every record through `core.ingest()`
(same channel + dedup as backfill), and capture the advanced cursor. Both the
live poller (the `gmail_history` analog) and the push handler (the `gmail_watch`
notification path) call this — so an observation written from a push, a poll,
or a backfill is indistinguishable downstream and dedups at the
`observations.UNIQUE` key.

This file owns NO leasing / persistence SQL — that differs per source
(calendar keys on calendar_id; drive on drive_kind+drive_id+owner_email) and
lives in each source's `live_poller`.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable

import structlog


log = structlog.get_logger("integrations.google_live")


# Safety bound: a single live drain should never page forever (a runaway token
# loop or a clock-skewed delta). 200 pages * the fetcher's page size is far
# above any real per-resource delta between two short poll ticks.
_MAX_PAGES = 200


async def drain_live(
    *,
    pool: Any,
    tenant_id: Any,
    scope: str,
    channel: str,
    fetcher: Callable[[Any, dict[str, Any], dict[str, Any] | None], Awaitable[Any]],
    shard_identifier: dict[str, Any],
    cursor_next_key: str,
    warm_token: str | None,
) -> tuple[int, str | None]:
    """Run the real fetch loop for one resource shard incrementally, ingesting
    each record. Returns ``(ingested_count, advanced_token)``.

    `install` is reconstructed as a minimal mapping — the Calendar/Drive
    fetchers' `_open_*_client` only read `install["scope"]`. `shard_identifier`
    must already carry the warm cursor (sync_token / start_page_token) so the
    fetcher starts in incremental mode. `advanced_token` is the new cursor to
    persist; it falls back to `warm_token` when the delta produced no new token
    (e.g. a rate-limited empty round), so a transient failure never erases the
    bookmark.
    """
    from services.ingest.ingestion.core import ingest

    install = {"scope": scope}
    cursor: dict[str, Any] | None = None
    advanced = warm_token
    ingested = 0
    pages = 0
    while True:
        pages += 1
        if pages > _MAX_PAGES:
            log.warning(
                "google_live.drain.page_cap",
                channel=channel, shard=shard_identifier.get("calendar_id")
                or shard_identifier.get("drive_id"),
            )
            break
        result = await fetcher(install, shard_identifier, cursor)
        for record in result.records:
            res = await ingest(channel, record, pool=pool, tenant_id=tenant_id)
            if not res.deduped:
                ingested += 1
        cursor = result.next_cursor
        if cursor:
            tok = cursor.get(cursor_next_key)
            if isinstance(tok, str) and tok:
                advanced = tok
        if result.end_of_data:
            break
    return ingested, advanced


__all__ = ["drain_live"]
