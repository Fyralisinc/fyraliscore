"""services/ingest/ingestion/fetchers/ramp.py — Ramp backfill/poll fetcher.

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `ramp_entity` shard streams one entity type (`transaction` / `reimbursement`
/ `card` / `user` — the VERIFIED Ramp Developer API taxonomy, docs.ramp.com).

  - FULL (initial backfill): walk the REST collection
    (`GET /developer/v1/<resource>?page_size=…`) following the KEYSET
    `page.next` URL (`{"data": [...], "page": {"next": <url|null>}}`) until
    `page.next` is null.
  - INCREMENTAL (poll): when warm-started with an `updated_cursor` (the
    high-water timestamp), the server-side window param narrows the walk:
      * transaction    — `from_date` (filters `user_transaction_time`)
      * reimbursement  — `updated_after` (filters `updated_at`)
      * card / user    — NO server-side incremental filter (verified): fall
        back to a full idempotent re-walk; the deterministic state-versioned
        external_id dedups unchanged rows.

============================================================
RECORDS
============================================================
Each entity row is emitted as one record tagged with the private
`_fyralis_record_type` = the entity type, plus `_fyralis_business_id`.
The `ramp:transaction` handler builds ONE observation per record. Because
Ramp resources MUTATE (PENDING → CLEARED / DECLINED, reimbursements walk a
state machine, cards suspend/terminate), the external_id is versioned by state
so a state change lands as a NEW observation.

Pagination cursor: the `page.next` URL is persisted verbatim between calls
(KEYSET — `start=<last entity id>` is embedded in it). Page size is capped at
the documented max 100 and env-knobbed via RAMP_BACKFILL_PAGE_SIZE.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import RampApiError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "ramp_entity"
_DEFAULT_PAGE_SIZE = 100

# Per-stream high-water timestamp field (the incremental cursor reference).
# `user` has no usable timestamp → no high-water (always full re-walk).
_HIGH_WATER_FIELD = {
    "transaction": "user_transaction_time",
    "reimbursement": "updated_at",
    "card": "created_at",
}


def _page_size() -> int:
    try:
        # docs.ramp.com: page_size must be between 2 and 100.
        return max(2, min(100, int(os.environ.get("RAMP_BACKFILL_PAGE_SIZE", "100"))))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class RampCursor(BaseModel):
    """Cursor for one entity shard.

    - next_page_url     : the keyset `page.next` URL to resume at (None = next
                          call starts a fresh window / is terminal).
    - high_water_updated : max per-stream timestamp observed (ISO; see
                          _HIGH_WATER_FIELD) — the warm-start / incremental
                          lower bound AND the reconciler's gap reference point.
    - incremental_floor : the server-side window floor frozen for this run
                          (None in FULL mode or for streams with no filter).
    - rows_seen         : diagnostic.
    - seeded            : whether the first-call setup ran.
    """

    model_config = ConfigDict(extra="forbid")

    next_page_url: str | None = None
    high_water_updated: str | None = None
    incremental_floor: str | None = None
    rows_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> RampCursor:
    if c is None:
        return RampCursor()
    return RampCursor.model_validate(c)


def _encode_cursor(c: RampCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real RampClient; tests rebind this.
async def _open_ramp_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_ramp_client
    return await open_ramp_client(install)


def _row_timestamp(entity_type: str, row: dict[str, Any]) -> str | None:
    field = _HIGH_WATER_FIELD.get(entity_type)
    if field is None:
        return None
    v = row.get(field)
    return v if isinstance(v, str) else None


def _bump_high_water(cur: RampCursor, updated: str | None) -> None:
    if isinstance(updated, str) and (
        cur.high_water_updated is None or updated > cur.high_water_updated
    ):
        cur.high_water_updated = updated


def _business_id_of(install: asyncpg.Record) -> str:
    return str(install["business_id"]) if "business_id" in install else ""


async def _fetch_entity_page(
    client: Any, entity_type: str, cur: RampCursor,
) -> tuple[list[dict[str, Any]], str | None]:
    """Dispatch one keyset page for the shard's stream. Returns
    `(rows, next_page_url)`."""
    size = _page_size()
    if entity_type == "transaction":
        return await client.list_transactions(
            from_date=cur.incremental_floor,
            page_size=size,
            page_url=cur.next_page_url,
        )
    if entity_type == "reimbursement":
        return await client.list_reimbursements(
            updated_after=cur.incremental_floor,
            page_size=size,
            page_url=cur.next_page_url,
        )
    if entity_type == "card":
        # No server-side incremental filter — full idempotent re-walk.
        return await client.list_cards(
            page_size=size, page_url=cur.next_page_url,
        )
    if entity_type == "user":
        return await client.list_users(
            page_size=size, page_url=cur.next_page_url,
        )
    raise RampApiError(
        f"unknown ramp entity_type {entity_type!r}",
        code="ramp_api_error",
        context={"entity_type": entity_type},
    )


async def fetch_page_ramp(
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
            # Streams without a server-side filter (card/user) keep the
            # high-water for the reconciler but re-walk in full.
            if entity_type in ("transaction", "reimbursement"):
                cur.incremental_floor = warm
            cur.high_water_updated = warm
        cur.seeded = True

    business_id = _business_id_of(install)

    client, close = await _open_ramp_client(install)
    try:
        try:
            rows, next_url = await _fetch_entity_page(client, entity_type, cur)
        except RampApiError as exc:
            code = (exc.context or {}).get("code") or getattr(exc, "_code", None)
            if code == "ramp_api_rate_limited":
                log.info("ramp_backfill_rate_limited",
                         extra={"entity_type": entity_type})
                return FetchResult(
                    records=[], next_cursor=_encode_cursor(cur),
                    end_of_data=False,
                )
            raise

        records: list[dict[str, Any]] = []
        for row in rows:
            records.append({
                "_fyralis_record_type": entity_type,
                "_fyralis_business_id": business_id,
                "entity": row,
            })
            _bump_high_water(cur, _row_timestamp(entity_type, row))

        cur.rows_seen += len(rows)
        is_last = next_url is None
        cur.next_page_url = next_url

        log.info(
            "ramp_backfill_page",
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


FETCHER_DISPATCH["ramp"] = fetch_page_ramp


__all__ = [
    "SHARD_KIND_ENTITY",
    "RampCursor",
    "fetch_page_ramp",
]
