"""services/ingest/ingestion/fetchers/carta.py — Carta backfill/poll fetcher.

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `carta_entity` shard streams one cap-table entity type for the issuer
(`install.firm_id` holds the Carta issuer id). Entity taxonomy (CONFIRMED
against the Issuer v1alpha1 OpenAPI — see integrations/carta/client.py):
`stakeholder` / `shareClass` / `optionGrant` / `convertibleNote`, each a
`GET /v1alpha1/issuers/{issuer}/{collection}` list.

  - FULL (initial backfill): walk the collection with AIP-158 token pagination
    (`pageSize` + opaque `pageToken`; the response's `nextPageToken` is the
    cursor; absent at EOF).
  - INCREMENTAL (poll): ONLY `optionGrant` has a server-side delta filter
    (`lastModifiedDatetimeAfter`, ISO 8601 UTC, inclusive "on or after"); when
    warm-started with an `updated_cursor` the fetcher passes it through and
    advances the `lastModifiedDatetime` high-water. The other three collections
    have NO modified-since filter, so an incremental run is a FULL idempotent
    re-walk — the content-digest-versioned external_id
    (`carta:{issuer}:{kind}:{id}:{version}`) dedups unchanged rows and
    re-observes mutations, so re-walks are safe and cheap on writes.

============================================================
RECORDS
============================================================
Each entity row is emitted as one record tagged with the private
`_fyralis_record_type` = the entity type (lowercased: "stakeholder" /
"shareclass" / "optiongrant" / "convertiblenote"), plus `_fyralis_firm_id`
(the issuer id). The `carta:object` handler builds ONE observation per record.

Rate limit: 429 surfaces as `carta_api_rate_limited`; the shard yields (same
cursor, end_of_data=False) so ShardFetch retries later. Page size is
env-overridable via CARTA_BACKFILL_PAGE_SIZE (server caps at 50; 100 for
stakeholders — values above the cap are coerced server-side).
"""
from __future__ import annotations

import logging
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingest.integrations.carta.client import (
    ENTITY_COLLECTIONS,
    CartaApiError,
)
from services.ingest.ingestion.fetchers import FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "carta_entity"
_DEFAULT_PAGE_SIZE = 50


def _page_size() -> int:
    try:
        return min(100, int(os.environ.get("CARTA_BACKFILL_PAGE_SIZE", "50")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class CartaCursor(BaseModel):
    """Cursor for one entity shard.

    - page_token         : the opaque AIP-158 `nextPageToken` to resume from
                           (None = first page).
    - high_water_modified: max `lastModifiedDatetime` (ISO) observed — only
                           option grants carry it; the warm-start / incremental
                           lower bound AND the reconciler's gap reference point.
    - incremental_floor  : the `lastModifiedDatetimeAfter` bound frozen for this
                           run (None in FULL mode; only honoured for
                           optionGrant).
    - rows_seen          : diagnostic.
    - seeded             : whether the first-call setup ran.
    """

    model_config = ConfigDict(extra="forbid")

    page_token: str | None = None
    high_water_modified: str | None = None
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


def _last_modified(row: dict[str, Any]) -> str | None:
    """Decode the `lastModifiedDatetime` wrapper (`{"value": "<iso>"}`) — only
    option grants carry it."""
    wrapper = row.get("lastModifiedDatetime")
    if isinstance(wrapper, dict):
        v = wrapper.get("value")
        return v if isinstance(v, str) and v else None
    return None


def _bump_high_water(cur: CartaCursor, modified: str | None) -> None:
    if isinstance(modified, str) and (
        cur.high_water_modified is None or modified > cur.high_water_modified
    ):
        cur.high_water_modified = modified


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
    if entity_type not in ENTITY_COLLECTIONS:
        log.warning(
            "carta_backfill_unknown_entity_type",
            extra={"entity_type": entity_type},
        )
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    cur = _decode_cursor(cursor)
    if not cur.seeded:
        warm = shard_identifier.get("updated_cursor")
        if isinstance(warm, str) and warm:
            cur.incremental_floor = warm
            cur.high_water_modified = warm
        cur.seeded = True

    firm_id = _firm_id_of(install)
    # Only optionGrants has a server-side delta filter; the other collections
    # full-re-walk and rely on external_id dedup (see module docstring).
    modified_after = (
        cur.incremental_floor if entity_type == "optionGrant" else None
    )

    client, close = await _open_carta_client(install)
    try:
        try:
            rows, next_token = await client.list_entity(
                entity_type,
                page_size=_page_size(),
                page_token=cur.page_token,
                modified_after=modified_after,
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
            _bump_high_water(cur, _last_modified(row))

        cur.rows_seen += len(rows)
        cur.page_token = next_token
        is_last = next_token is None

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




__all__ = [
    "SHARD_KIND_ENTITY",
    "CartaCursor",
    "fetch_page_carta",
]
