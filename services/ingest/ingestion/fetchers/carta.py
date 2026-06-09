"""services/ingest/ingestion/fetchers/carta.py — Carta backfill/poll fetcher.

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `carta_entity` shard streams one cap-table entity type (Shareholder /
ShareClass / SafeNote / OptionGrant) for the firm.

  - FULL (initial backfill): `SELECT * FROM <Entity> ORDERBY
    Metadata.LastUpdatedTime STARTPOSITION n MAXRESULTS m`, offset-paginated.
  - INCREMENTAL (poll): when warm-started with an `updated_cursor` (the
    LastUpdatedTime high-water), the WHERE clause adds
    `Metadata.LastUpdatedTime > '<cursor>'` so only changed entities come back.

============================================================
RECORDS
============================================================
Each entity row is emitted as one record tagged with the private
`_fyralis_record_type` = the entity type (lowercased), plus `_fyralis_firm_id`.
The `carta:object` handler builds ONE observation per record. Because cap-table
objects MUTATE (a SAFE converts, an option grant vests), the external_id is
versioned by `SyncToken` so a state change lands as a NEW observation.

CONFIRMED (docs.carta.com/api-platform): Carta's API is `v1alpha1` (alpha —
expect breaking changes) and uses Google-AIP-style TOKEN pagination — `pageSize`
(int) + `pageToken` (string; response returns the next page token), NOT the
offset/STARTPOSITION the Gusto clone uses. Swap `client.query(...)`'s offset
bookkeeping to `pageSize`/`pageToken`. Rate-limit signal: 429 (burst 10/s,
sustained 300/min). Page size is env-overridable via CARTA_BACKFILL_PAGE_SIZE.
TODO(human): (1) ACCESS IS PARTNER-GATED (invite-only + SOC 2) — wire against the
    approved prod host (dev via https://mock-api.carta.com) once entitled.
    (2) confirm the per-entity "updated since" filter field name; Carta is
    poll-only (no webhook) and most tables refresh ~daily, so a full re-walk
    (idempotent via the versioned external_id) is an acceptable fallback if no
    incremental filter exists.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingest.integrations.carta.client import CartaApiError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "carta_entity"
_DEFAULT_PAGE_SIZE = 100


def _page_size() -> int:
    try:
        return min(1000, int(os.environ.get("CARTA_BACKFILL_PAGE_SIZE", "100")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class CartaCursor(BaseModel):
    """Cursor for one entity shard.

    - start_position    : the CARTA STARTPOSITION offset within this run (1-based).
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


def _decode_cursor(c: dict[str, Any] | None) -> CartaCursor:
    if c is None:
        return CartaCursor()
    return CartaCursor.model_validate(c)


def _encode_cursor(c: CartaCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real CartaClient; tests rebind this.
async def _open_carta_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_carta_client
    return await open_carta_client(install)


def _last_updated(row: dict[str, Any]) -> str | None:
    meta = row.get("MetaData") or row.get("Metadata") or {}
    if isinstance(meta, dict):
        v = meta.get("LastUpdatedTime")
        return v if isinstance(v, str) else None
    return None


def _bump_high_water(cur: CartaCursor, updated: str | None) -> None:
    if isinstance(updated, str) and (
        cur.high_water_updated is None or updated > cur.high_water_updated
    ):
        cur.high_water_updated = updated


def _firm_id_of(install: asyncpg.Record) -> str:
    return str(install["firm_id"]) if "firm_id" in install else ""


async def fetch_page_carta(
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

    firm_id = _firm_id_of(install)
    where = (
        f"Metadata.LastUpdatedTime > '{cur.incremental_floor}'"
        if cur.incremental_floor else None
    )

    client, close = await _open_carta_client(install)
    try:
        try:
            rows, next_start = await client.query(
                entity_type,
                where=where,
                start_position=cur.start_position,
                max_results=_page_size(),
            )
        except CartaApiError as exc:
            code = (exc.context or {}).get("code") or getattr(exc, "_code", None)
            if code == "carta_api_rate_limited":
                log.info("carta_backfill_rate_limited",
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
                "_fyralis_firm_id": firm_id,
                "entity": row,
            })
            _bump_high_water(cur, _last_updated(row))

        cur.rows_seen += len(rows)
        is_last = next_start is None
        if next_start is not None:
            cur.start_position = next_start

        log.info(
            "carta_backfill_page",
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


FETCHER_DISPATCH["carta"] = fetch_page_carta


__all__ = [
    "SHARD_KIND_ENTITY",
    "CartaCursor",
    "fetch_page_carta",
]
