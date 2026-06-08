"""services/ingest/ingestion/fetchers/brex.py — Brex backfill/poll fetcher (finance).

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `brex_account_txns` shard streams one account's transactions.

  - FULL (initial backfill): walk `GET /account/{id}/transactions` from
    offset 0, paginated, newest-first. On the FIRST page the fetcher also emits
    one `account_snapshot` record (the current balance) so the cash-position
    signal lands alongside the transaction history.
  - INCREMENTAL (poll): when the shard is warm-started with a `txn_cursor`
    (the high-water transaction `createdAt`), the fetcher passes `start=<date>`
    so only recent transactions come back; the overlap re-fetch dedups via the
    versioned external_id.

============================================================
FAN-OUT: ONE ACCOUNT -> N RECORDS
============================================================
The `brex:transaction` handler produces ONE observation per record. The
fetcher emits:
  - "account_snapshot" : one balance snapshot per shard run (cash position).
  - "transaction"      : one per transaction on the account.

Each record is tagged with a private `_fyralis_record_type` the handler branches
on. external_id parity (set by the handler) collapses a backfilled record and
its live-webhook twin to one observation. Because a transaction's STATUS mutates
(pending -> sent -> failed), its external_id is versioned by status.

TODO(human): confirm Brex transactions API pagination (offset vs cursor) +
created/posted filter. This fetcher clones the Mercury offset/limit + `start=`
date-filter contract (UNVERIFIED for Brex — blueprint §5 #3/#4). Brex v2 may be
cursor-token based; if so, swap the offset bookkeeping in `BrexCursor` for an
opaque page token and replace `start=` with whatever "created/posted since"
filter the API exposes. Page size is overridable via `BREX_BACKFILL_PAGE_SIZE`.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import BrexApiError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_ACCOUNT_TXNS = "brex_account_txns"
_DEFAULT_PAGE_SIZE = 100


def _page_size() -> int:
    try:
        return min(500, int(os.environ.get("BREX_BACKFILL_PAGE_SIZE", "100")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class BrexCursor(BaseModel):
    """Cursor for one account shard. Round-trips through the opaque dict in
    workflow_states.state_data.

    - offset            : the list-transactions pagination offset within a run.
    - high_water_created : max transaction `createdAt` (ISO) observed — the
                           warm-start / incremental lower bound AND the
                           reconciler's gap reference point.
    - incremental_floor : the `start=` lower bound frozen for this run (None in
                          FULL mode).
    - txns_seen         : diagnostic.
    - seeded            : whether the first-call setup (snapshot emit) ran.
    """

    model_config = ConfigDict(extra="forbid")

    offset: int = 0
    high_water_created: str | None = None
    incremental_floor: str | None = None
    txns_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> BrexCursor:
    if c is None:
        return BrexCursor()
    return BrexCursor.model_validate(c)


def _encode_cursor(c: BrexCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real BrexClient against the install's auth;
# the mock harness / tests rebind this symbol to inject a fake.
async def _open_brex_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_brex_client
    return await open_brex_client(install)


def _iso_date(iso: str | None) -> str | None:
    """The date portion of an ISO timestamp (Brex `start` is date-granular)."""
    if not isinstance(iso, str) or not iso:
        return None
    return iso[:10]


def _bump_high_water(cur: BrexCursor, created: Any) -> None:
    if isinstance(created, str) and (
        cur.high_water_created is None or created > cur.high_water_created
    ):
        cur.high_water_created = created


async def fetch_page_brex(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of transactions (+ a balance snapshot on the first page) + cursor."""
    account_id = shard_identifier.get("account_id")
    if not isinstance(account_id, str) or not account_id:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    cur = _decode_cursor(cursor)
    records: list[dict[str, Any]] = []

    client, close = await _open_brex_client(install)
    try:
        # First-call setup: warm-start mode + emit the balance snapshot.
        if not cur.seeded:
            warm = shard_identifier.get("txn_cursor")
            if isinstance(warm, str) and warm:
                cur.incremental_floor = warm  # warm start -> incremental
                cur.high_water_created = warm
            cur.seeded = True
            # Balance snapshot (cash-position signal) — one per shard run.
            try:
                account = await client.get_account(account_id)
            except BrexApiError:
                account = None
            if isinstance(account, dict):
                now_iso = datetime.now(timezone.utc).isoformat()
                records.append({
                    "_fyralis_record_type": "account_snapshot",
                    "_fyralis_account_id": account_id,
                    "account": account,
                    "as_of": now_iso,
                })

        try:
            txns, next_offset, total = await client.list_transactions(
                account_id,
                limit=_page_size(),
                offset=cur.offset,
                start=_iso_date(cur.incremental_floor),
            )
        except BrexApiError as exc:
            if (exc.context or {}).get("code") == "brex_api_rate_limited" or \
               getattr(exc, "_code", None) == "brex_api_rate_limited":
                log.info("brex_backfill_rate_limited",
                         extra={"account_id": account_id})
                return FetchResult(
                    records=records, next_cursor=_encode_cursor(cur),
                    end_of_data=False,
                )
            raise

        for txn in txns:
            records.append({
                "_fyralis_record_type": "transaction",
                "_fyralis_account_id": account_id,
                "transaction": txn,
            })
            _bump_high_water(cur, txn.get("createdAt") or txn.get("postedAt"))

        cur.txns_seen += len(txns)
        is_last = next_offset is None
        cur.offset = next_offset if next_offset is not None else cur.offset

        log.info(
            "brex_backfill_page",
            extra={"account_id": account_id, "txns": len(txns),
                   "records": len(records), "is_last": is_last},
        )
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=is_last,
        )
    finally:
        await close()


FETCHER_DISPATCH["brex"] = fetch_page_brex


__all__ = [
    "SHARD_KIND_ACCOUNT_TXNS",
    "BrexCursor",
    "fetch_page_brex",
]
