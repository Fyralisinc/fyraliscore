"""Tests for services/ingest/ingestion/fetchers/gusto.py (finance/payroll)."""
from __future__ import annotations

import pytest

from lib.shared.errors import GustoApiError
from services.ingest.ingestion.fetchers import gusto as gusto_fetcher
from services.ingest.ingestion.fetchers.gusto import (
    GustoCursor,
    SHARD_KIND_ENTITY,
    fetch_page_gusto,
)


pytestmark = pytest.mark.asyncio


_COMPANY = "8b342a55-907e-4ba8-a95d-d29fbf95d6e1"


class _FakeClient:
    """Implements the GustoClient list surface the fetcher uses (real
    `page`/`per` offset semantics: a short or empty page is terminal ->
    `next_page is None`, mirroring the X-Total-Count/X-Page/X-Per-Page
    header computation)."""

    def __init__(self, employees=None, payrolls=None, page_size=100):
        self._employees = employees or []
        self._payrolls = payrolls or []
        self._page_size = page_size
        self.calls: list[dict] = []

    def _page(self, pool, page, per):
        per_page = min(per, self._page_size)
        rows = pool[(page - 1) * per_page: page * per_page]
        if not rows or len(rows) < per_page:
            return rows, None
        if page * per_page >= len(pool):
            return rows, None
        return rows, page + 1

    async def list_employees(self, *, page=1, per=100, terminated=None):
        self.calls.append({"resource": "employees", "page": page,
                           "terminated": terminated})
        return self._page(self._employees, page, per)

    async def list_payrolls(self, *, page=1, per=100, start_date=None,
                            end_date=None, date_filter_by=None,
                            processing_statuses=None, payroll_types=None,
                            sort_order=None):
        self.calls.append({"resource": "payrolls", "page": page,
                           "start_date": start_date,
                           "date_filter_by": date_filter_by,
                           "payroll_types": payroll_types})
        pool = self._payrolls
        if start_date:
            # Inclusive, day-granular check_date window (real API semantics).
            pool = [r for r in pool
                    if (r.get("check_date") or "") >= start_date]
        return self._page(pool, page, per)


class _FakeInst:
    _d = {"company_uuid": _COMPANY, "base_url": "https://api.gusto.com",
          "tenant_id": None, "secret_ref": None}

    def __getitem__(self, k): return self._d[k]
    def __contains__(self, k): return k in self._d


def _wire(monkeypatch, client):
    async def _open(install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(gusto_fetcher, "_open_gusto_client", _open)


def _employee(uuid, version, terminated=False):
    return {"uuid": uuid, "version": version, "first_name": "Ava",
            "last_name": "Reyes", "terminated": terminated,
            "onboarded": True}


def _payroll(uuid, check_date, processed=True):
    return {"payroll_uuid": uuid, "uuid": uuid, "check_date": check_date,
            "processed": processed,
            "totals": {"gross_pay": "10000.00", "net_pay": "8200.00"}}


async def test_employee_backfill_tags_records_with_entity_type(monkeypatch):
    rows = [_employee("e-1", "v1"), _employee("e-2", "v2")]
    client = _FakeClient(employees=rows)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "employee",
             "company_uuid": _COMPANY}
    res = await fetch_page_gusto(_FakeInst(), shard, None)

    assert len(res.records) == 2
    assert all(r["_fyralis_record_type"] == "employee" for r in res.records)
    assert all(r["_fyralis_company_uuid"] == _COMPANY for r in res.records)
    assert res.end_of_data is True
    cur = GustoCursor.model_validate(res.next_cursor)
    # Employees carry no check_date — the high-water stays unset.
    assert cur.high_water is None
    # The terminated FILTER is never sent (it would narrow the walk).
    assert client.calls[0]["terminated"] is None


async def test_payroll_backfill_tracks_check_date_high_water(monkeypatch):
    rows = [_payroll("p-1", "2026-05-01"), _payroll("p-2", "2026-05-15")]
    client = _FakeClient(payrolls=rows)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "payroll",
             "company_uuid": _COMPANY}
    res = await fetch_page_gusto(_FakeInst(), shard, None)

    assert len(res.records) == 2
    assert all(r["_fyralis_record_type"] == "payroll" for r in res.records)
    assert res.end_of_data is True
    cur = GustoCursor.model_validate(res.next_cursor)
    assert cur.high_water == "2026-05-15"
    # FULL mode: no server-side date window.
    assert client.calls[0]["start_date"] is None
    assert client.calls[0]["date_filter_by"] is None
    # The walk widens past the server default (regular only).
    assert tuple(client.calls[0]["payroll_types"]) == ("regular", "off_cycle")


async def test_page_pagination_persists_and_advances(monkeypatch):
    rows = [_employee(f"e-{i}", f"v{i}") for i in range(1, 6)]
    client = _FakeClient(employees=rows, page_size=2)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "employee",
             "company_uuid": _COMPANY}

    # Page 1: full page -> next_page=2 persisted in the cursor.
    res1 = await fetch_page_gusto(_FakeInst(), shard, None)
    assert [r["entity"]["uuid"] for r in res1.records] == ["e-1", "e-2"]
    assert res1.end_of_data is False
    cur1 = GustoCursor.model_validate(res1.next_cursor)
    assert cur1.page == 2

    # Page 2: resumes FROM the persisted page number.
    res2 = await fetch_page_gusto(_FakeInst(), shard, res1.next_cursor)
    assert client.calls[-1]["page"] == 2
    assert [r["entity"]["uuid"] for r in res2.records] == ["e-3", "e-4"]
    assert res2.end_of_data is False

    # Page 3: short page -> terminal.
    res3 = await fetch_page_gusto(_FakeInst(), shard, res2.next_cursor)
    assert [r["entity"]["uuid"] for r in res3.records] == ["e-5"]
    assert res3.end_of_data is True
    cur3 = GustoCursor.model_validate(res3.next_cursor)
    assert cur3.rows_seen == 5


async def test_payroll_warm_start_passes_check_date_window(monkeypatch):
    payrolls = [_payroll("p-1", "2026-05-01"), _payroll("p-9", "2026-05-10")]
    client = _FakeClient(payrolls=payrolls)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "payroll",
             "company_uuid": _COMPANY, "updated_cursor": "2026-05-09"}
    res = await fetch_page_gusto(_FakeInst(), shard, None)

    # INCREMENTAL: start_date=<high-water> + date_filter_by=check_date.
    assert client.calls[0]["start_date"] == "2026-05-09"
    assert client.calls[0]["date_filter_by"] == "check_date"
    assert len(res.records) == 1
    assert res.records[0]["entity"]["payroll_uuid"] == "p-9"
    cur = GustoCursor.model_validate(res.next_cursor)
    assert cur.high_water == "2026-05-10"
    assert cur.incremental_floor == "2026-05-09"


async def test_employee_warm_start_is_full_rewalk(monkeypatch):
    # Warm-started employee shard: the endpoint has NO updated-since filter —
    # full re-walk (idempotent via the version-discriminated external_id).
    rows = [_employee("e-1", "v2")]
    client = _FakeClient(employees=rows)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "employee",
             "company_uuid": _COMPANY, "updated_cursor": "2026-05-09"}
    res = await fetch_page_gusto(_FakeInst(), shard, None)

    assert client.calls[0]["resource"] == "employees"
    assert len(res.records) == 1
    assert res.end_of_data is True
    cur = GustoCursor.model_validate(res.next_cursor)
    # The payroll-only warm start never leaks onto employee shards.
    assert cur.incremental_floor is None
    assert cur.high_water is None


async def test_rate_limited_returns_same_cursor_not_terminal(monkeypatch):
    class _Limited:
        async def list_employees(self, **kwargs):
            raise GustoApiError("429", code="gusto_api_rate_limited",
                                context={"http_status": 429})

    _wire(monkeypatch, _Limited())
    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "employee",
             "company_uuid": _COMPANY}
    res = await fetch_page_gusto(_FakeInst(), shard, None)
    assert res.records == []
    assert res.end_of_data is False  # retried later from the same cursor
    cur = GustoCursor.model_validate(res.next_cursor)
    assert cur.page == 1


async def test_empty_entity_terminates(monkeypatch):
    client = _FakeClient()
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "payroll",
             "company_uuid": _COMPANY}
    res = await fetch_page_gusto(_FakeInst(), shard, None)
    assert res.records == []
    assert res.end_of_data is True


async def test_missing_entity_type_is_noop(monkeypatch):
    client = _FakeClient()
    _wire(monkeypatch, client)
    res = await fetch_page_gusto(_FakeInst(), {"shard_kind": SHARD_KIND_ENTITY}, None)
    assert res.records == []
    assert res.end_of_data is True


async def test_legacy_qbo_entity_type_is_noop(monkeypatch):
    # The QBO-clone taxonomy ("Invoice"/"Bill"/...) must no longer fetch.
    client = _FakeClient(employees=[_employee("e-1", "v1")])
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "Invoice",
             "company_uuid": _COMPANY}
    res = await fetch_page_gusto(_FakeInst(), shard, None)
    assert res.records == []
    assert res.end_of_data is True
    assert client.calls == []
