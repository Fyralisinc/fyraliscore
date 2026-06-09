"""Self-verifying Mercury synthetic backfill test (NO DB, NO network).

Drives the REAL Mercury backfill fetcher (`fetch_page_mercury`) against the
synthetic `MockMercuryClient` + `make_mercury` fixture, then feeds every emitted
record through the REAL handler (`mercury:transaction`). Proves the
mock/fixture/fetcher/handler quartet is internally consistent — the same
contract the X3 harness relies on, but in-process and assertable.

What it locks down:
  - fan-out: each account -> 1 account_snapshot (first page only) + N transactions
  - handler parity: every record yields a non-null external_id + a 2026 occurred_at
  - pagination: transactions_per_account > page-size -> multiple fetcher pages
  - faults: a rate-limit profile surfaces MercuryApiError, and the fetcher's
    documented rate-limit handling (swallow page, end_of_data=False) holds.
"""
from __future__ import annotations

import asyncio

import pytest

from lib.shared.errors import MercuryApiError
from services.ingest.ingestion.fetchers import mercury as mercury_fetcher
from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.synthetic.fault_profiles import FaultProfile
from services.ingest.synthetic.fixtures.mercury_generator import make_mercury
from services.ingest.synthetic.mock_clients.mercury import MockMercuryClient


# The fetcher passes `install` straight to `_open_mercury_client` (which we
# monkeypatch to return the mock), so a bare dict stands in for the install
# Record. The shard_identifier mirrors what the planner emits.
_INSTALL = {"id": "00000000-0000-0000-0000-000000000001", "tenant_id": "t-1"}


def _shard_for(account_id: str, *, txn_cursor: str | None = None) -> dict:
    return {
        "shard_kind": mercury_fetcher.SHARD_KIND_ACCOUNT_TXNS,
        "account_id": account_id,
        "account_name": "Operating Checking 1",
        "installation_id": _INSTALL["id"],
        "txn_cursor": txn_cursor,
    }


def _bind_mock(monkeypatch, mock: MockMercuryClient) -> None:
    """Rebind the fetcher's client seam to yield `mock` + a no-op close."""
    async def _open(_install):  # noqa: ANN202
        async def _close() -> None:
            return None
        return mock, _close

    monkeypatch.setattr(mercury_fetcher, "_open_mercury_client", _open)


async def _drive_to_end(install: dict, shard: dict) -> list[dict]:
    """Run `fetch_page_mercury` in a loop until end_of_data; return all records."""
    records: list[dict] = []
    cursor: dict | None = None
    # Generous page cap so a buggy never-terminating loop fails loudly instead
    # of hanging the suite.
    for _ in range(50):
        result = await mercury_fetcher.fetch_page_mercury(install, shard, cursor)
        records.extend(result.records)
        cursor = result.next_cursor
        if result.end_of_data:
            return records
    raise AssertionError("fetch loop did not reach end_of_data within 50 pages")


def test_mercury_backfill_fetch_to_handler(monkeypatch):
    """Happy path: 1 account, 4 txns -> 5 records, all handler-valid."""
    accounts = 1
    transactions_per_account = 4
    fixture = make_mercury(
        accounts=accounts, transactions_per_account=transactions_per_account,
    )
    account_id = fixture["account_order"][0]
    _bind_mock(monkeypatch, MockMercuryClient(fixture=fixture))

    records = asyncio.run(_drive_to_end(_INSTALL, _shard_for(account_id)))

    # Fan-out: 1 account_snapshot (first page only) + N transactions.
    expected_observation_count = accounts * (1 + transactions_per_account)
    assert len(records) == expected_observation_count == 5

    snapshots = [r for r in records if r["_fyralis_record_type"] == "account_snapshot"]
    txns = [r for r in records if r["_fyralis_record_type"] == "transaction"]
    assert len(snapshots) == 1
    assert len(txns) == transactions_per_account

    # Every record is tagged with the account id and survives the REAL handler.
    channel = resolve_channel("mercury", "backfill")
    assert channel == "mercury:transaction"
    handler = get_handler(channel)
    for rec in records:
        assert rec["_fyralis_account_id"] == account_id
        draft = asyncio.run(handler(rec, {}))
        assert draft.source_channel == "mercury:transaction"
        assert draft.external_id  # non-null dedup key
        assert draft.occurred_at is not None
        assert draft.occurred_at.year == 2026


def test_mercury_backfill_pagination(monkeypatch):
    """transactions_per_account > page-size limit -> multiple fetcher pages."""
    page_size = 3
    transactions_per_account = 7  # 3 + 3 + 1 -> 3 pages
    fixture = make_mercury(
        accounts=1,
        transactions_per_account=transactions_per_account,
        page_size=page_size,
    )
    account_id = fixture["account_order"][0]
    _bind_mock(monkeypatch, MockMercuryClient(fixture=fixture))

    # Count pages explicitly to prove multi-page walk (not just final totals).
    pages = 0
    cursor: dict | None = None
    txn_records: list[dict] = []
    snapshot_records: list[dict] = []
    for _ in range(50):
        result = asyncio.run(
            mercury_fetcher.fetch_page_mercury(_INSTALL, _shard_for(account_id), cursor)
        )
        pages += 1
        txn_records += [
            r for r in result.records if r["_fyralis_record_type"] == "transaction"
        ]
        snapshot_records += [
            r for r in result.records if r["_fyralis_record_type"] == "account_snapshot"
        ]
        cursor = result.next_cursor
        if result.end_of_data:
            break

    assert pages == 3, f"expected 3 pages for {transactions_per_account} txns @ {page_size}/page"
    assert len(txn_records) == transactions_per_account
    # The balance snapshot is emitted on the FIRST page only, never re-emitted.
    assert len(snapshot_records) == 1


def test_mercury_rate_limit_surfaces_and_fetcher_swallows(monkeypatch):
    """A rate-limit fault raises MercuryApiError(mercury_api_rate_limited); the
    fetcher's documented handling returns the snapshot page with
    end_of_data=False (non-terminal) rather than raising."""
    fixture = make_mercury(accounts=1, transactions_per_account=4)
    account_id = fixture["account_order"][0]

    # 1) The mock itself raises the right typed error with the right code.
    rl_mock = MockMercuryClient(
        fixture=fixture,
        profile=FaultProfile(rate_limit_after_n_requests=0),  # fire immediately
    )
    with pytest.raises(MercuryApiError) as exc_info:
        asyncio.run(rl_mock.get_account(account_id))
    assert getattr(exc_info.value, "_code", None) == "mercury_api_rate_limited"

    # 2) Drive the fetcher: get_account succeeds (request 1), list_transactions
    #    trips the rate limit (request 2). The fetcher swallows the rate-limit on
    #    the transaction call, returning the already-built snapshot record with
    #    end_of_data=False (it will be retried on the next claim).
    fetch_mock = MockMercuryClient(
        fixture=fixture,
        profile=FaultProfile(rate_limit_after_n_requests=1),
    )
    _bind_mock(monkeypatch, fetch_mock)

    result = asyncio.run(
        mercury_fetcher.fetch_page_mercury(_INSTALL, _shard_for(account_id), None)
    )
    assert result.end_of_data is False  # non-terminal: shard re-fetches
    # The snapshot was built before list_transactions tripped the limit.
    assert len(result.records) == 1
    assert result.records[0]["_fyralis_record_type"] == "account_snapshot"
