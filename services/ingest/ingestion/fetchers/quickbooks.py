"""services/ingest/ingestion/fetchers/quickbooks.py — QuickBooks backfill/poll fetcher.

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `quickbooks_entity` shard streams one entity type (Invoice / Bill /
BillPayment / Payment) for the realm.

  - FULL (initial backfill): `SELECT * FROM <Entity> ORDERBY
    Metadata.LastUpdatedTime STARTPOSITION n MAXRESULTS m`, offset-paginated.
  - INCREMENTAL (poll): when warm-started with an `updated_cursor` (the
    LastUpdatedTime high-water), the WHERE clause adds
    `Metadata.LastUpdatedTime > '<cursor>'` so only changed entities come back.

============================================================
RECORDS
============================================================
Each entity row is emitted as one record tagged with the private
`_fyralis_record_type` = the entity type (lowercased), plus `_fyralis_realm_id`.
The `quickbooks:object` handler builds ONE observation per record. Because
invoices/bills MUTATE (draft -> sent -> paid -> overdue), the external_id is
versioned by `SyncToken` so a status change lands as a NEW observation.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import QuickBooksApiError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "quickbooks_entity"
_DEFAULT_PAGE_SIZE = 100


def _page_size() -> int:
    try:
        return min(1000, int(os.environ.get("QUICKBOOKS_BACKFILL_PAGE_SIZE", "100")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class QuickBooksCursor(BaseModel):
    """Cursor for one entity shard.

    - start_position    : the QBO STARTPOSITION offset within this run (1-based).
    - high_water_updated : max Metadata.LastUpdatedTime (ISO) observed — the
                           warm-start / incremental lower bound AND the
                           reconciler's gap reference point.
    - incremental_floor : the `LastUpdatedTime >` lower bound frozen for this run
                          (None in FULL mode).
    - rows_seen         : diagnostic.
    - seeded            : whether the first-call setup ran.
    """

    model_config = ConfigDict(extra="forbid")

    start_position: int = 1
    high_water_updated: str | None = None
    incremental_floor: str | None = None
    rows_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> QuickBooksCursor:
    if c is None:
        return QuickBooksCursor()
    return QuickBooksCursor.model_validate(c)


def _encode_cursor(c: QuickBooksCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real QuickBooksClient; tests rebind this.
async def _open_quickbooks_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_quickbooks_client
    return await open_quickbooks_client(install)


def _last_updated(row: dict[str, Any]) -> str | None:
    meta = row.get("MetaData") or row.get("Metadata") or {}
    if isinstance(meta, dict):
        v = meta.get("LastUpdatedTime")
        return v if isinstance(v, str) else None
    return None


def _bump_high_water(cur: QuickBooksCursor, updated: str | None) -> None:
    if isinstance(updated, str) and (
        cur.high_water_updated is None or updated > cur.high_water_updated
    ):
        cur.high_water_updated = updated


def _realm_id_of(install: asyncpg.Record) -> str:
    return str(install["realm_id"]) if "realm_id" in install else ""


async def fetch_page_quickbooks(
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

    realm_id = _realm_id_of(install)
    where = (
        f"Metadata.LastUpdatedTime > '{cur.incremental_floor}'"
        if cur.incremental_floor else None
    )

    client, close = await _open_quickbooks_client(install)
    try:
        try:
            rows, next_start = await client.query(
                entity_type,
                where=where,
                start_position=cur.start_position,
                max_results=_page_size(),
            )
        except QuickBooksApiError as exc:
            code = (exc.context or {}).get("code") or getattr(exc, "_code", None)
            if code == "quickbooks_api_rate_limited":
                log.info("quickbooks_backfill_rate_limited",
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
                "_fyralis_realm_id": realm_id,
                "entity": row,
            })
            _bump_high_water(cur, _last_updated(row))

        cur.rows_seen += len(rows)
        is_last = next_start is None
        if next_start is not None:
            cur.start_position = next_start

        log.info(
            "quickbooks_backfill_page",
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


FETCHER_DISPATCH["quickbooks"] = fetch_page_quickbooks


__all__ = [
    "SHARD_KIND_ENTITY",
    "QuickBooksCursor",
    "fetch_page_quickbooks",
]
