"""MockGustoClient — Gusto read surface used by IN-FIN backfill.

In-process replacement for `GustoClient` at the `_open_gusto_client`
fetcher seam (services/ingest/ingestion/fetchers/gusto.py). Implements ONLY
what the backfill/poll fetcher + reconciler + onboarding probe call:

  - list_employees(page=..., per=..., terminated=None)
        -> (rows, next_page | None)
  - list_payrolls(page=..., per=..., start_date=..., date_filter_by=...,
                  payroll_types=..., ...)
        -> (rows, next_page | None)
  - company()  -> connectivity probe (single company object)

Pagination mirrors the real client's REAL-wire semantics exactly so the
fetcher's cursor loop behaves identically: `page` is 1-based, the slice is
`rows[(page - 1) * per : page * per]`, and the next page exists only while
`page * per < total` — a short or empty page is terminal -> `next_page is
None` (matches `GustoClient._next_page` computed from the X-Total-Count /
X-Page / X-Per-Page headers).

Incremental filter: the fetcher passes `start_date=<YYYY-MM-DD>` +
`date_filter_by="check_date"` in poll mode. The mock honours it the way the
real API documents the window (inclusive, day-granular): rows whose
`check_date` is >= the bound are returned.

Fault injection: `self._check_fault()` runs first on every public method and
the four raisers surface `GustoApiError` with the same stable `code`s the real
client emits, so the fetcher's rate-limit branch (and reconciler error
mapping) see exactly the production exception shape.
"""
from __future__ import annotations

from typing import Any, NoReturn, Sequence

from lib.shared.errors import GustoApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockGustoClient(_MockBase):
    """Stateful in-process replacement for `GustoClient`.

    `fixture` shape (per `make_gusto`):
        {
          "company_uuid": "8b342a55-...",
          "page_size": 100,
          "entities": {
            "employee": [ {<real-shaped employee>}, ... ],
            "payroll":  [ {<real-shaped payroll>}, ... ],
          },
        }
    """

    def __init__(
        self,
        *,
        fixture: dict[str, Any],
        profile: FaultProfile = HAPPY_PATH,
    ) -> None:
        super().__init__(profile=profile)
        self._fixture = fixture
        self._company_uuid = str(fixture.get("company_uuid", ""))
        self._default_page_size = int(fixture.get("page_size", 100))

    # ---- IN-FIN read surface ----
    async def list_employees(
        self,
        *,
        page: int = 1,
        per: int = 100,
        terminated: bool | None = None,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """One `page`/`per` page of employee rows + the next page (or None)."""
        self._check_fault()
        rows_all = self._entities_for("employee")
        if terminated is not None:
            rows_all = [
                r for r in rows_all if bool(r.get("terminated")) is terminated
            ]
        return self._paginate(rows_all, page, per)

    async def list_payrolls(
        self,
        *,
        page: int = 1,
        per: int = 100,
        start_date: str | None = None,
        end_date: str | None = None,
        date_filter_by: str | None = None,
        processing_statuses: Sequence[str] | None = None,
        payroll_types: Sequence[str] | None = None,
        sort_order: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """One `page`/`per` page of payroll rows + the next page (or None).

        Honours the inclusive `start_date`/`end_date` check_date window the
        fetcher/reconciler drive (day-granular, like the real API).
        """
        self._check_fault()
        rows_all = self._entities_for("payroll")
        if start_date:
            rows_all = [
                r for r in rows_all
                if (r.get("check_date") or "") >= start_date
            ]
        if end_date:
            rows_all = [
                r for r in rows_all
                if (r.get("check_date") or "") <= end_date
            ]
        return self._paginate(rows_all, page, per)

    async def company(self) -> dict[str, Any]:
        """`GET /v1/companies/{company_uuid}` analogue — the connectivity
        probe. Returns a minimal real-shaped company object."""
        self._check_fault()
        return {
            "uuid": self._company_uuid or "00000000-0000-0000-0000-000000000000",
            "name": f"Synthetic Co {self._company_uuid}",
            "trade_name": None,
            "company_status": "Approved",
        }

    # ---- Helpers ----
    def _entities_for(self, entity: str) -> list[dict[str, Any]]:
        ents = self._fixture.get("entities", {})
        rows = ents.get(entity, [])
        return list(rows) if isinstance(rows, list) else []

    def _paginate(
        self, rows_all: list[dict[str, Any]], page: int, per: int,
    ) -> tuple[list[dict[str, Any]], int | None]:
        # The page cap the fetcher actually honours is min(requested, fixture).
        per_page = min(per, self._default_page_size)
        start = max(0, (page - 1) * per_page)
        rows = rows_all[start:start + per_page]
        # Terminal exactly as the real client computes it from the count
        # headers: a short (or empty) page ends the stream.
        if not rows or len(rows) < per_page:
            return rows, None
        if page * per_page >= len(rows_all):
            return rows, None
        return rows, page + 1

    # ---- Fault raisers (production exception parity) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise GustoApiError(
            "MockGustoClient: rate limit 429 (X2 fault)",
            code="gusto_api_rate_limited",
            context={"http_status": 429, "retry_after": "1"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise GustoApiError(
            "MockGustoClient: 503 (X2 fault)",
            code="gusto_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise GustoApiError(
            "MockGustoClient: 401 access token rejected (X2 fault)",
            code="gusto_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise GustoApiError(
            "MockGustoClient: transient transport error (X2 fault)",
            code="gusto_api_error",
            context={"error_type": "TransportError"},
        )


__all__ = ["MockGustoClient"]
