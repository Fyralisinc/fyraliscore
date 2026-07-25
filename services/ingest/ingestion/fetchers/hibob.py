"""services/ingest/ingestion/fetchers/hibob.py — HiBob backfill/poll fetcher (People/HR).

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `hibob_entity` shard streams one entity type (employee / lifecycle / timeoff
/ payroll) for the company.

  - FULL (initial backfill): walk `client.list_entities(<type>)` from offset 0
    or an opaque page cursor, depending on the HiBob endpoint.
  - INCREMENTAL (poll): when warm-started with an `updated_cursor` (the
    high-water `modified` timestamp), the fetcher passes `modified_since=<cursor>`
    so only changed entities come back; the overlap re-fetch dedups via the
    versioned external_id.

============================================================
RECORDS
============================================================
Each entity row is emitted as one record tagged with the private
`_fyralis_record_type` = the entity type, plus `_fyralis_company_id`. The
`hibob:object` handler builds ONE observation per record. Because People/HR
records MUTATE (an employee's lifecycle state, a time-off approval), the
external_id is versioned by the row's modified/version field so a change lands as
a NEW observation.

The client maps employee search to local offset pagination and Bob bulk tables
to their real opaque cursor (`response_metadata.next_cursor`). Page size is
env-overridable via HIBOB_BACKFILL_PAGE_SIZE.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingest.ingestion.fetchers import FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "hibob_entity"
_DEFAULT_PAGE_SIZE = 100


def _page_size() -> int:
    try:
        return min(1000, int(os.environ.get("HIBOB_BACKFILL_PAGE_SIZE", "100")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class HibobCursor(BaseModel):
    """Cursor for one entity shard. Round-trips through the opaque dict in
    workflow_states.state_data.

    - offset            : local offset pagination within this run.
    - page_cursor       : opaque HiBob cursor for bulk endpoints.
    - high_water_updated : max row `modified`/version (ISO) observed — the
                           warm-start / incremental lower bound AND the
                           reconciler's gap reference point.
    - incremental_floor : the `modified_since` lower bound frozen for this run
                          (None in FULL mode).
    - rows_seen         : diagnostic.
    - seeded            : whether the first-call setup ran.
    """

    model_config = ConfigDict(extra="forbid")

    offset: int = 0
    page_cursor: str | None = None
    high_water_updated: str | None = None
    incremental_floor: str | None = None
    rows_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> HibobCursor:
    if c is None:
        return HibobCursor()
    return HibobCursor.model_validate(c)


def _encode_cursor(c: HibobCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real HibobClient against the install's auth;
# the synthetic harness / tests rebind this symbol to inject a fake.
async def _open_hibob_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_hibob_client
    return await open_hibob_client(install)


def _row_modified(row: dict[str, Any]) -> str | None:
    """The row's high-water field across HiBob's varying entity shapes."""
    for key in ("modified", "modifiedAt", "lastModified", "updatedAt", "updated"):
        v = row.get(key)
        if isinstance(v, str) and v:
            return v
    return None


def _bump_high_water(cur: HibobCursor, updated: str | None) -> None:
    if isinstance(updated, str) and (
        cur.high_water_updated is None or updated > cur.high_water_updated
    ):
        cur.high_water_updated = updated


def _company_id_of(install: asyncpg.Record) -> str:
    return str(install["company_id"]) if "company_id" in install else ""


async def fetch_page_hibob(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of entity rows + next cursor."""
    entity_type = shard_identifier.get("entity_type")
    if not isinstance(entity_type, str) or not entity_type:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    cur = _decode_cursor(cursor)
    if not cur.seeded:
        warm = shard_identifier.get("updated_cursor")
        if isinstance(warm, str) and warm:
            cur.incremental_floor = warm
            cur.high_water_updated = warm
        cur.seeded = True

    company_id = _company_id_of(install)

    client, close = await _open_hibob_client(install)
    try:
        rows, next_page = await client.list_entities(
            entity_type,
            limit=_page_size(),
            offset=cur.offset,
            page_cursor=cur.page_cursor,
            modified_since=cur.incremental_floor,
        )

        records: list[dict[str, Any]] = []
        for row in rows:
            records.append({
                "_fyralis_record_type": entity_type,
                "_fyralis_company_id": company_id,
                "entity": row,
            })
            _bump_high_water(cur, _row_modified(row))

        cur.rows_seen += len(rows)
        is_last = next_page is None
        if isinstance(next_page, int):
            cur.offset = next_page
            cur.page_cursor = None
        elif isinstance(next_page, str) and next_page.isdigit():
            cur.offset = int(next_page)
            cur.page_cursor = None
        elif isinstance(next_page, str) and next_page:
            cur.page_cursor = next_page

        log.info(
            "hibob_backfill_page",
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




__all__ = [
    "SHARD_KIND_ENTITY",
    "HibobCursor",
    "fetch_page_hibob",
]
