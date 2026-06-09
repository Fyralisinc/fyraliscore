"""services/ingest/ingestion/fetchers/ashby.py — Ashby backfill/poll fetcher.

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
An `ashby_entity` shard streams one recruiting entity type (candidate /
application / job / interview / offer) for the org.

  - FULL (initial backfill): `POST /<Category>.list` walked by the response
    CURSOR — each call carries the prior `nextCursor`, terminating when
    `moreDataAvailable` is false (`nextCursor is None`).
  - INCREMENTAL (poll): when warm-started with a `sync_token` (the persisted
    syncToken from a prior backfill/poll), the `.list` call passes it so only
    entities changed since the token are returned. The refreshed syncToken in
    the response is captured into the cursor to persist for the NEXT poll.

============================================================
RECORDS
============================================================
Each entity row is emitted as one record tagged with the private
`_fyralis_record_type` = the entity type (lowercased), plus `_fyralis_org_id`.
The `ashby:object` handler builds ONE observation per record. Ashby entities
MUTATE (a candidate advances stages, an offer is sent/accepted), but the
external_id is NOT version-suffixed (per the CONTRACT: `ashby:{org}:{entity}:{id}`),
so the handler relies on occurred_at + the per-entity updated timestamp to
represent the latest state; a re-walk of an unchanged entity dedups.

CONFIRMED (Ashby first-party docs): cursor pagination — request body `cursor`,
response `nextCursor` + `moreDataAvailable` bool — and an incremental `syncToken`
that `.list` accepts (request) and refreshes (response). The auth is API-key
Basic with an EMPTY password (see the client). Page size is env-overridable via
ASHBY_BACKFILL_PAGE_SIZE.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import AshbyApiError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "ashby_entity"
_DEFAULT_PAGE_SIZE = 100


def _page_size() -> int:
    try:
        return min(1000, int(os.environ.get("ASHBY_BACKFILL_PAGE_SIZE", "100")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class AshbyCursor(BaseModel):
    """Cursor for one entity shard.

    - cursor             : the Ashby `nextCursor` page token to resume the walk
                           (None at the start / when terminal).
    - sync_token         : the incremental syncToken. On warm-start this is the
                           floor (passed to `.list`); it is refreshed from each
                           response so the NEXT poll resumes incrementally.
    - high_water_updated : max entity updated timestamp (ISO) observed — the
                           reconciler's gap reference point.
    - rows_seen          : diagnostic.
    - seeded             : whether the first-call setup ran.
    """

    model_config = ConfigDict(extra="forbid")

    cursor: str | None = None
    sync_token: str | None = None
    high_water_updated: str | None = None
    rows_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> AshbyCursor:
    if c is None:
        return AshbyCursor()
    return AshbyCursor.model_validate(c)


def _encode_cursor(c: AshbyCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real AshbyClient; the synthetic harness
# monkeypatches THIS function. Keep the name + signature stable.
async def _open_ashby_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_ashby_client
    return await open_ashby_client(install)


def _entity_updated(row: dict[str, Any]) -> str | None:
    """Ashby entities carry an `updatedAt` (ISO8601). Fall back to `createdAt`."""
    for key in ("updatedAt", "updated_at", "createdAt", "created_at"):
        v = row.get(key)
        if isinstance(v, str) and v:
            return v
    return None


def _bump_high_water(cur: AshbyCursor, updated: str | None) -> None:
    if isinstance(updated, str) and (
        cur.high_water_updated is None or updated > cur.high_water_updated
    ):
        cur.high_water_updated = updated


def _org_id_of(install: asyncpg.Record) -> str:
    return str(install["org_id"]) if "org_id" in install else ""


async def fetch_page_ashby(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One cursor page of entity rows + next cursor."""
    entity_type = shard_identifier.get("entity_type")
    if not isinstance(entity_type, str) or not entity_type:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    cur = _decode_cursor(cursor)
    if not cur.seeded:
        # Warm-start an incremental poll from the persisted syncToken (the
        # planner threads it in as `sync_cursor`).
        warm = shard_identifier.get("sync_cursor")
        if isinstance(warm, str) and warm:
            cur.sync_token = warm
        cur.seeded = True

    org_id = _org_id_of(install)

    client, close = await _open_ashby_client(install)
    try:
        try:
            rows, next_cursor, next_sync_token = await client.list_entities(
                entity_type,
                cursor=cur.cursor,
                sync_token=cur.sync_token,
                limit=_page_size(),
            )
        except AshbyApiError as exc:
            code = (exc.context or {}).get("code") or getattr(exc, "_code", None)
            if code == "ashby_api_rate_limited":
                log.info("ashby_backfill_rate_limited",
                         extra={"entity_type": entity_type})
                return FetchResult(
                    records=[], next_cursor=_encode_cursor(cur),
                    end_of_data=False,
                )
            raise

        records: list[dict[str, Any]] = []
        for row in rows:
            records.append({
                "_fyralis_record_type": entity_type.lower(),
                "_fyralis_org_id": org_id,
                "entity": row,
            })
            _bump_high_water(cur, _entity_updated(row))

        cur.rows_seen += len(rows)
        # Persist the refreshed syncToken so the NEXT incremental poll resumes
        # from it; keep the prior one if the response didn't return a new token.
        if next_sync_token is not None:
            cur.sync_token = next_sync_token
        cur.cursor = next_cursor
        is_last = next_cursor is None

        log.info(
            "ashby_backfill_page",
            extra={"entity_type": entity_type, "rows": len(rows),
                   "is_last": is_last},
        )
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=is_last,
        )
    finally:
        await close()


FETCHER_DISPATCH["ashby"] = fetch_page_ashby


__all__ = [
    "SHARD_KIND_ENTITY",
    "AshbyCursor",
    "fetch_page_ashby",
    "_open_ashby_client",
]
