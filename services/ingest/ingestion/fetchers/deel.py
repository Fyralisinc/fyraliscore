"""services/ingest/ingestion/fetchers/deel.py — Deel backfill/poll fetcher (finance).

Deel's real REST v2 API exposes contracts under `/contracts` and payment-like
cash movement via the org invoice stream (`/invoices`). The client keeps the
existing `list_payments` seam and maps the fetcher's start floor to the invoice
created filter.

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `deel_contract_payments` shard streams one contract's payments.

  - FULL (initial backfill): walk invoices for the contract from
    offset 0, paginated, newest-first. On the FIRST page the fetcher also emits
    one `contract_snapshot` record (the current contract state) so the
    contract-position signal lands alongside the payment history.
  - INCREMENTAL (poll): when the shard is warm-started with a `payment_cursor`
    (the high-water payment `createdAt`), the fetcher passes `start=<date>`
    so only recent payments come back; the overlap re-fetch dedups via the
    versioned external_id.

============================================================
FAN-OUT: ONE CONTRACT -> N RECORDS
============================================================
The `deel:payment` handler produces ONE observation per record. The
fetcher emits:
  - "contract_snapshot" : one contract-state snapshot per shard run.
  - "payment"           : one per payment on the contract.

Each record is tagged with a private `_fyralis_record_type` the handler branches
on. external_id parity (set by the handler) collapses a backfilled record and
its live-webhook twin to one observation. Because a payment's STATUS mutates
(pending -> sent -> failed), its external_id is versioned by status.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import DeelApiError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_CONTRACT_PAYMENTS = "deel_contract_payments"
_DEFAULT_PAGE_SIZE = 100


def _page_size() -> int:
    try:
        return min(500, int(os.environ.get("DEEL_BACKFILL_PAGE_SIZE", "100")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class DeelCursor(BaseModel):
    """Cursor for one contract shard. Round-trips through the opaque dict in
    workflow_states.state_data.

    - offset            : the list-payments pagination offset within a run.
    - high_water_created : max payment `createdAt` (ISO) observed — the
                           warm-start / incremental lower bound AND the
                           reconciler's gap reference point.
    - incremental_floor : the `start=` lower bound frozen for this run (None in
                          FULL mode).
    - payments_seen     : diagnostic.
    - seeded            : whether the first-call setup (snapshot emit) ran.
    """

    model_config = ConfigDict(extra="forbid")

    offset: int = 0
    high_water_created: str | None = None
    incremental_floor: str | None = None
    payments_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> DeelCursor:
    if c is None:
        return DeelCursor()
    return DeelCursor.model_validate(c)


def _encode_cursor(c: DeelCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real DeelClient against the install's auth;
# the mock harness / tests rebind this symbol to inject a fake.
async def _open_deel_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_deel_client
    return await open_deel_client(install)


def _payment_high_water(payment: dict[str, Any]) -> Any:
    return (
        payment.get("createdAt")
        or payment.get("created_at")
        or payment.get("postedAt")
        or payment.get("posted_at")
        or payment.get("issued_at")
        or payment.get("invoice_date")
        or payment.get("updated_at")
        or payment.get("updatedAt")
    )


def _bump_high_water(cur: DeelCursor, created: Any) -> None:
    if isinstance(created, str) and (
        cur.high_water_created is None or created > cur.high_water_created
    ):
        cur.high_water_created = created


async def fetch_page_deel(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of payments (+ a contract snapshot on the first page) + cursor."""
    contract_id = shard_identifier.get("contract_id")
    if not isinstance(contract_id, str) or not contract_id:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    cur = _decode_cursor(cursor)
    records: list[dict[str, Any]] = []

    client, close = await _open_deel_client(install)
    try:
        # First-call setup: warm-start mode + emit the contract snapshot.
        if not cur.seeded:
            warm = shard_identifier.get("payment_cursor")
            if isinstance(warm, str) and warm:
                cur.incremental_floor = warm  # warm start -> incremental
                cur.high_water_created = warm
            cur.seeded = True
            # Contract snapshot (contract-state signal) — one per shard run.
            try:
                contract = await client.get_contract(contract_id)
            except DeelApiError:
                contract = None
            if isinstance(contract, dict):
                now_iso = datetime.now(timezone.utc).isoformat()
                records.append({
                    "_fyralis_record_type": "contract_snapshot",
                    "_fyralis_contract_id": contract_id,
                    "contract": contract,
                    "updated": now_iso,
                })

        try:
            payments, next_offset, total = await client.list_payments(
                contract_id,
                limit=_page_size(),
                offset=cur.offset,
                start=cur.incremental_floor,
            )
        except DeelApiError as exc:
            if (exc.context or {}).get("code") == "deel_api_rate_limited" or \
               getattr(exc, "_code", None) == "deel_api_rate_limited":
                log.info("deel_backfill_rate_limited",
                         extra={"contract_id": contract_id})
                return FetchResult(
                    records=records, next_cursor=_encode_cursor(cur),
                    end_of_data=False,
                )
            raise

        for payment in payments:
            records.append({
                "_fyralis_record_type": "payment",
                "_fyralis_contract_id": contract_id,
                "payment": payment,
            })
            _bump_high_water(cur, _payment_high_water(payment))

        cur.payments_seen += len(payments)
        is_last = next_offset is None
        cur.offset = next_offset if next_offset is not None else cur.offset

        log.info(
            "deel_backfill_page",
            extra={"contract_id": contract_id, "payments": len(payments),
                   "records": len(records), "is_last": is_last},
        )
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=is_last,
        )
    finally:
        await close()


FETCHER_DISPATCH["deel"] = fetch_page_deel


__all__ = [
    "SHARD_KIND_CONTRACT_PAYMENTS",
    "DeelCursor",
    "fetch_page_deel",
]
