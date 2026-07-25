"""services/ingest/ingestion/fetchers/gusto.py — Gusto backfill/poll fetcher.

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `gusto_entity` shard streams one entity kind (`employee` | `payroll`) for
the company. The wire contract (VERIFIED against docs.gusto.com): bare-array
list endpoints under `/v1/companies/{company_uuid}/...`, offset pagination via
`page`/`per` query params with `X-Total-Count`/`X-Page`/`X-Per-Page` response
headers; a short/empty page is terminal.

  - `payroll`: `GET .../payrolls`. FULL walks the whole collection;
    INCREMENTAL (warm-started with an `updated_cursor` — the max `check_date`
    high-water) passes `start_date=<cursor>&date_filter_by=check_date` so only
    payrolls paid on/after the high-water come back (dedup absorbs the
    boundary re-fetch). `payroll_types=regular,off_cycle` widens past the
    server default (regular only); `processing_statuses` keeps the server
    default (processed).
  - `employee`: `GET .../employees`. The endpoint has NO updated-since filter,
    so every sync is a full re-walk; the `version`-discriminated external_id
    makes re-walks idempotent (only a changed employee lands as a new
    observation). The `terminated` query param is left unset (it is a FILTER —
    sending it would narrow the walk to one sub-population).

============================================================
RECORDS
============================================================
Each row is emitted as one record tagged with the private
`_fyralis_record_type` = the entity kind, plus `_fyralis_company_uuid`. The
`gusto:object` handler builds ONE observation per record; mutations land as
NEW observations because the external_id is versioned (employee `version`,
payroll processed-state).

Page size is env-overridable via GUSTO_BACKFILL_PAGE_SIZE (API max 100).
"""
from __future__ import annotations

import logging
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingest.ingestion.fetchers import FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "gusto_entity"
_DEFAULT_PAGE_SIZE = 100

ENTITY_EMPLOYEE = "employee"
ENTITY_PAYROLL = "payroll"


def _page_size() -> int:
    try:
        # docs.gusto.com: `per` max is 100.
        return min(100, int(os.environ.get("GUSTO_BACKFILL_PAGE_SIZE", "100")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class GustoCursor(BaseModel):
    """Cursor for one entity shard.

    - page              : the Gusto `page` offset within this run (1-based).
    - high_water        : max payroll `check_date` (YYYY-MM-DD) observed — the
                          warm-start / incremental lower bound AND the
                          reconciler's gap reference point. Stays None on
                          employee shards (no updated-since semantics).
    - incremental_floor : the `start_date` lower bound frozen for this run
                          (None in FULL mode / on employee shards).
    - rows_seen         : diagnostic.
    - seeded            : whether the first-call setup ran.
    """

    model_config = ConfigDict(extra="forbid")

    page: int = 1
    high_water: str | None = None
    incremental_floor: str | None = None
    rows_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> GustoCursor:
    if c is None:
        return GustoCursor()
    return GustoCursor.model_validate(c)


def _encode_cursor(c: GustoCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real GustoClient; tests rebind this.
async def _open_gusto_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_gusto_client
    return await open_gusto_client(install)


def _check_date(row: dict[str, Any]) -> str | None:
    v = row.get("check_date")
    return v if isinstance(v, str) and v else None


def _bump_high_water(cur: GustoCursor, check_date: str | None) -> None:
    # YYYY-MM-DD strings compare lexicographically == chronologically.
    if isinstance(check_date, str) and (
        cur.high_water is None or check_date > cur.high_water
    ):
        cur.high_water = check_date


def _company_uuid_of(install: asyncpg.Record) -> str:
    return str(install["company_uuid"]) if "company_uuid" in install else ""


async def fetch_page_gusto(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of entity rows + next cursor."""
    entity_type = shard_identifier.get("entity_type")
    if entity_type not in (ENTITY_EMPLOYEE, ENTITY_PAYROLL):
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    cur = _decode_cursor(cursor)
    if not cur.seeded:
        warm = shard_identifier.get("updated_cursor")
        # Only payrolls have a server-side date filter; employee shards always
        # full re-walk (dedup makes that idempotent).
        if entity_type == ENTITY_PAYROLL and isinstance(warm, str) and warm:
            cur.incremental_floor = warm
            cur.high_water = warm
        cur.seeded = True

    company_uuid = _company_uuid_of(install)

    client, close = await _open_gusto_client(install)
    try:
        # RetryLater must reach shard_fetch so it persists next_attempt_at
        # instead of hot-looping an empty page with an unchanged cursor.
        if entity_type == ENTITY_EMPLOYEE:
            rows, next_page = await client.list_employees(
                page=cur.page, per=_page_size(),
            )
        else:
            rows, next_page = await client.list_payrolls(
                page=cur.page,
                per=_page_size(),
                start_date=cur.incremental_floor,
                date_filter_by=(
                    "check_date" if cur.incremental_floor else None
                ),
                payroll_types=("regular", "off_cycle"),
            )

        records: list[dict[str, Any]] = []
        for row in rows:
            records.append({
                "_fyralis_record_type": entity_type,
                "_fyralis_company_uuid": company_uuid,
                "entity": row,
            })
            if entity_type == ENTITY_PAYROLL:
                _bump_high_water(cur, _check_date(row))

        cur.rows_seen += len(rows)
        is_last = next_page is None
        if next_page is not None:
            cur.page = next_page

        log.info(
            "gusto_backfill_page",
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
    "ENTITY_EMPLOYEE",
    "ENTITY_PAYROLL",
    "GustoCursor",
    "fetch_page_gusto",
]
