"""Synthetic backfill test for QuickBooks (IN-FIN) — NO DB.

Drives the REAL `fetch_page_quickbooks` fetcher against `MockQuickBooksClient`
(rebound at the `_open_quickbooks_client` seam) over `make_quickbooks` fixtures,
then runs every emitted record through the REAL `quickbooks:object` handler.

What it proves end-to-end (fixture -> mock client -> real fetcher -> real
handler), per entity type:
  - the fetcher paginates each `quickbooks_entity` shard to `end_of_data`,
  - every record carries the `_fyralis_record_type` + `_fyralis_realm_id` tags
    the handler needs,
  - the handler emits a draft with a non-null `external_id` and an `occurred_at`
    in 2026 (the fixture's base year),
  - the total observation count matches `len(entities) * rows_per_entity`,
  - multi-page offset pagination works (rows_per_entity > page_size),
  - a rate-limit fault surfaces the production `QuickBooksApiError` shape and the
    fetcher's rate-limit branch handles it gracefully (no crash, not terminal).
"""
from __future__ import annotations

import pytest

from lib.shared.errors import QuickBooksApiError
from services.ingest.ingestion.fetchers import quickbooks as qb_fetcher
from services.ingest.ingestion.fetchers.quickbooks import (
    SHARD_KIND_ENTITY,
    fetch_page_quickbooks,
)
from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.fixtures.quickbooks_generator import make_quickbooks
from services.ingest.synthetic.mock_clients.quickbooks import MockQuickBooksClient


pytestmark = pytest.mark.asyncio


_REALM = "9341452000000001"
_ENTITIES = ["Invoice", "Bill", "BillPayment", "Payment"]


# ---------------------------------------------------------------------
# Test wiring
# ---------------------------------------------------------------------

def _install() -> dict[str, str]:
    """The fetcher reads `install["realm_id"]` (and `"realm_id" in install`); a
    plain dict satisfies both."""
    return {"id": "00000000-0000-0000-0000-000000000001", "realm_id": _REALM}


def _shard_for(entity_type: str, *, updated_cursor: str | None = None) -> dict:
    """Mirror the planner's shard_identifier (planners/quickbooks.py)."""
    return {
        "shard_kind": SHARD_KIND_ENTITY,
        "entity_type": entity_type,
        "realm_id": _REALM,
        "installation_id": _install()["id"],
        "updated_cursor": updated_cursor,
    }


def _patch_client(monkeypatch, client: MockQuickBooksClient) -> None:
    async def _open(_install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(qb_fetcher, "_open_quickbooks_client", _open)


async def _drain_shard(install, shard) -> list[dict]:
    """Drive one shard to end_of_data; return all records (raw fetcher tags)."""
    records: list[dict] = []
    cursor = None
    guard = 0
    while True:
        guard += 1
        assert guard < 1000, "pagination did not terminate"
        res = await fetch_page_quickbooks(install, shard, cursor)
        records.extend(res.records)
        cursor = res.next_cursor
        if res.end_of_data:
            break
    return records


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------

async def test_dispatch_and_channel_wired():
    from services.ingest.source_contract.runtime import resolve_fetcher
    assert resolve_fetcher("quickbooks") is fetch_page_quickbooks
    assert resolve_channel("quickbooks", "backfill") == "quickbooks:object"


async def test_backfill_all_entities_through_handler(monkeypatch):
    rows_per_entity = 2
    fixture = make_quickbooks(
        realm_id=_REALM, entities=_ENTITIES, rows_per_entity=rows_per_entity,
    )
    client = MockQuickBooksClient(fixture=fixture, profile=HAPPY_PATH)
    _patch_client(monkeypatch, client)

    handler = get_handler(resolve_channel("quickbooks", "backfill"))

    total = 0
    for entity_type in _ENTITIES:
        records = await _drain_shard(_install(), _shard_for(entity_type))
        assert len(records) == rows_per_entity, entity_type

        for rec in records:
            # Fetcher tagging contract.
            assert rec["_fyralis_record_type"] == entity_type.lower()
            assert rec["_fyralis_realm_id"] == _REALM
            assert isinstance(rec["entity"], dict)

            draft = await handler(rec, {})
            assert draft.source_channel == "quickbooks:object"
            assert draft.external_id, "external_id must be non-null"
            # external_id is `qbo:{realm}:{normalized_kind}:{id}:{SyncToken}`
            # (the handler normalises billpayment -> bill_payment).
            assert draft.external_id.startswith(f"qbo:{_REALM}:")
            assert draft.occurred_at.year == 2026
            total += 1

    # expected_observation_count = len(entities) * rows_per_entity = 4 * 2 = 8.
    assert total == len(_ENTITIES) * rows_per_entity == 8


async def test_paid_and_unpaid_states_emerge(monkeypatch):
    """The AR/AP fixture alternates zero-balance / open-balance rows; the handler
    must reflect both so we know the Balance signal is wired (not just
    count-checked). The open-balance row's DueDate (base+30d, early 2026) is in
    the past relative to the test clock -> the handler classifies it 'overdue';
    the zero-balance row is 'paid'. Both are AR/AP state_change signals."""
    fixture = make_quickbooks(realm_id=_REALM, entities=["Invoice"], rows_per_entity=2)
    client = MockQuickBooksClient(fixture=fixture, profile=HAPPY_PATH)
    _patch_client(monkeypatch, client)
    handler = get_handler(resolve_channel("quickbooks", "backfill"))

    records = await _drain_shard(_install(), _shard_for("Invoice"))
    statuses = set()
    kinds = set()
    for rec in records:
        draft = await handler(rec, {})
        statuses.add(draft.content["status"])
        kinds.add(draft.kind)
    # Zero-balance -> paid; open-balance past-due -> overdue. Both balance-driven.
    assert statuses == {"paid", "overdue"}
    assert kinds == {"state_change"}


async def test_multi_page_pagination(monkeypatch):
    """rows_per_entity > page_size -> the fetcher walks multiple offset pages."""
    page_size = 2
    rows_per_entity = 5  # -> pages of 2, 2, 1
    fixture = make_quickbooks(
        realm_id=_REALM, entities=["Invoice"],
        rows_per_entity=rows_per_entity, page_size=page_size,
    )
    client = MockQuickBooksClient(fixture=fixture, profile=HAPPY_PATH)
    _patch_client(monkeypatch, client)

    install = _install()
    shard = _shard_for("Invoice")

    pages: list[int] = []
    cursor = None
    all_records: list[dict] = []
    while True:
        res = await fetch_page_quickbooks(install, shard, cursor)
        pages.append(len(res.records))
        all_records.extend(res.records)
        cursor = res.next_cursor
        if res.end_of_data:
            break

    assert pages == [2, 2, 1], pages
    assert len(all_records) == rows_per_entity
    # Every Id is distinct -> offset advanced correctly, no dupes/skips.
    ids = [r["entity"]["Id"] for r in all_records]
    assert len(set(ids)) == rows_per_entity


async def test_incremental_where_filters_old_rows(monkeypatch):
    """A warm-started shard (updated_cursor) -> the mock honours
    `Metadata.LastUpdatedTime > '<cursor>'`, dropping rows at/below the floor."""
    fixture = make_quickbooks(realm_id=_REALM, entities=["Invoice"], rows_per_entity=3)
    # Rows are 1 min apart from base; floor at row 0's timestamp drops it, keeps 1,2.
    floor = fixture["entities"]["Invoice"][0]["MetaData"]["LastUpdatedTime"]
    client = MockQuickBooksClient(fixture=fixture, profile=HAPPY_PATH)
    _patch_client(monkeypatch, client)

    records = await _drain_shard(
        _install(), _shard_for("Invoice", updated_cursor=floor),
    )
    assert len(records) == 2
    kept = {r["entity"]["MetaData"]["LastUpdatedTime"] for r in records}
    assert floor not in kept


async def test_rate_limit_fault_raises_quickbooks_error():
    """The mock's rate-limit raiser surfaces the production exception shape
    (QuickBooksApiError with code=quickbooks_api_rate_limited)."""
    fixture = make_quickbooks(realm_id=_REALM, entities=["Invoice"], rows_per_entity=1)
    profile = FaultProfile(rate_limit_after_n_requests=0)  # fire on first call
    client = MockQuickBooksClient(fixture=fixture, profile=profile)

    with pytest.raises(QuickBooksApiError) as ei:
        await client.query("Invoice", start_position=1, max_results=100)
    assert getattr(ei.value, "_code", None) == "quickbooks_api_rate_limited"


async def test_fetcher_handles_rate_limit_gracefully(monkeypatch):
    """When the client raises a rate-limit, the fetcher swallows it and returns
    a non-terminal empty page (the backfill loop retries next tick)."""
    fixture = make_quickbooks(realm_id=_REALM, entities=["Invoice"], rows_per_entity=1)
    profile = FaultProfile(rate_limit_after_n_requests=0)
    client = MockQuickBooksClient(fixture=fixture, profile=profile)
    _patch_client(monkeypatch, client)

    res = await fetch_page_quickbooks(_install(), _shard_for("Invoice"), None)
    assert res.records == []
    assert res.end_of_data is False
