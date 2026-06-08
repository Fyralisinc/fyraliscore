"""services/ingest/ingestion/fetchers/linkedin.py — LinkedIn backfill/poll fetcher.

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `linkedin_entity` shard streams one people/recruiting entity type (share /
social_action / follower_stat) for the organization.

  - FULL (initial backfill): `SELECT * FROM <Entity> ORDERBY
    Metadata.LastUpdatedTime STARTPOSITION n MAXRESULTS m`, offset-paginated.
  - INCREMENTAL (poll): when warm-started with an `updated_cursor` (the
    LastUpdatedTime high-water), the WHERE clause adds
    `Metadata.LastUpdatedTime > '<cursor>'` so only changed entities come back.

============================================================
RECORDS
============================================================
Each entity row is emitted as one record tagged with the private
`_fyralis_record_type` = the entity type (lowercased), plus `_fyralis_org_urn`.
The `linkedin:object` handler builds ONE observation per record. The external_id
is `linkedin:{org}:{kind}:{id}` (NOT versioned by a sync token — LinkedIn
organization objects are append/stat-shaped, so a fresh id per object suffices).

TODO(human): confirm LinkedIn's real list/pagination shape + the per-entity
    "updated since" filter field name. LinkedIn's REST surface is page/cursor
    based collections scoped by an `organization` URN (shares/posts,
    organizationalEntityShareStatistics / socialActions, followerStatistics);
    this clones the Carta/Gusto offset/STARTPOSITION placeholder. ACCESS IS
    PARTNER-GATED (Marketing Developer Platform / Talent Solutions, invite-only)
    — wire against the approved prod host once entitled. Page size is
    env-overridable via LINKEDIN_BACKFILL_PAGE_SIZE.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingest.integrations.linkedin.client import LinkedinApiError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "linkedin_entity"
_DEFAULT_PAGE_SIZE = 100


def _page_size() -> int:
    try:
        return min(1000, int(os.environ.get("LINKEDIN_BACKFILL_PAGE_SIZE", "100")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class LinkedinCursor(BaseModel):
    """Cursor for one entity shard.

    - start_position    : the STARTPOSITION offset within this run (1-based).
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


def _decode_cursor(c: dict[str, Any] | None) -> LinkedinCursor:
    if c is None:
        return LinkedinCursor()
    return LinkedinCursor.model_validate(c)


def _encode_cursor(c: LinkedinCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real LinkedinClient; tests rebind this.
async def _open_linkedin_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_linkedin_client
    return await open_linkedin_client(install)


def _last_updated(row: dict[str, Any]) -> str | None:
    meta = row.get("MetaData") or row.get("Metadata") or {}
    if isinstance(meta, dict):
        v = meta.get("LastUpdatedTime")
        return v if isinstance(v, str) else None
    return None


def _bump_high_water(cur: LinkedinCursor, updated: str | None) -> None:
    if isinstance(updated, str) and (
        cur.high_water_updated is None or updated > cur.high_water_updated
    ):
        cur.high_water_updated = updated


def _org_urn_of(install: asyncpg.Record) -> str:
    return str(install["organization_urn"]) if "organization_urn" in install else ""


async def fetch_page_linkedin(
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

    organization_urn = _org_urn_of(install)
    where = (
        f"Metadata.LastUpdatedTime > '{cur.incremental_floor}'"
        if cur.incremental_floor else None
    )

    client, close = await _open_linkedin_client(install)
    try:
        try:
            rows, next_start = await client.query(
                entity_type,
                where=where,
                start_position=cur.start_position,
                max_results=_page_size(),
            )
        except LinkedinApiError as exc:
            code = (exc.context or {}).get("code") or getattr(exc, "_code", None)
            if code == "linkedin_api_rate_limited":
                log.info("linkedin_backfill_rate_limited",
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
                "_fyralis_org_urn": organization_urn,
                "entity": row,
            })
            _bump_high_water(cur, _last_updated(row))

        cur.rows_seen += len(rows)
        is_last = next_start is None
        if next_start is not None:
            cur.start_position = next_start

        log.info(
            "linkedin_backfill_page",
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


FETCHER_DISPATCH["linkedin"] = fetch_page_linkedin


__all__ = [
    "SHARD_KIND_ENTITY",
    "LinkedinCursor",
    "fetch_page_linkedin",
]
