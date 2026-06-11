"""MockRampClient — Ramp read surface used by IN-FIN backfill.

In-process replacement for `RampClient` at the `_open_ramp_client` fetcher seam
(services/ingest/ingestion/fetchers/ramp.py). Mirrors the REAL client's public
method surface exactly (verified docs.ramp.com Developer API):

  - list_transactions(from_date=…, to_date=…, state=…, page_size=…, start=…,
        page_url=…)   -> (rows, next_page_url | None)
  - list_reimbursements(updated_after=…, from_date=…, page_size=…, start=…,
        page_url=…)   -> (rows, next_page_url | None)
  - list_cards(page_size=…, start=…, page_url=…)
  - list_users(page_size=…, start=…, page_url=…)
  - business()        -> connectivity probe body (`id` = the business_id)

Pagination mirrors the real KEYSET semantics exactly so the fetcher's cursor
loop behaves identically:
  - a full page yields a `page.next`-style absolute URL embedding
    `start=<last entity id>` (+ the original window params), exactly like the
    real envelope's `{"page": {"next": …}}`;
  - the fetcher persists that URL and passes it back via `page_url=`; the mock
    parses `start`/`page_size`/filters back out of it;
  - a short (or empty) page is terminal -> next is None.

Incremental filters honoured: `from_date` (transactions, on
`user_transaction_time`) and `updated_after` (reimbursements, on `updated_at`)
keep rows strictly greater than the bound. Cards/users have no filter
(matches the real API).

Fault injection: `self._check_fault()` runs first on every public method and the
four raisers surface `RampApiError` with the same stable `code`s the real
client emits, so the fetcher's rate-limit branch (and reconciler error mapping)
see exactly the production exception shape.
"""
from __future__ import annotations

from typing import Any, NoReturn
from urllib.parse import parse_qs, urlencode, urlsplit

from lib.shared.errors import RampApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


_BASE = "https://api.ramp.com/developer/v1"


class MockRampClient(_MockBase):
    """Stateful in-process replacement for `RampClient`.

    `fixture` shape (per `make_ramp`):
        {
          "business_id": "bus-…",
          "page_size": 100,
          "entities": {
            "transaction":   [ {<real-shaped Ramp transaction>}, ... ],
            "reimbursement": [ ... ],
            "card":          [ ... ],
            "user":          [ ... ],
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
        self._business_id = str(fixture.get("business_id", ""))
        self._default_page_size = int(fixture.get("page_size", 100))

    # ---- IN-FIN read surface ----
    async def list_transactions(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        state: str | None = None,
        page_size: int = 100,
        start: str | None = None,
        page_url: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        self._check_fault()
        return self._page(
            "transactions", "transaction", "user_transaction_time",
            gt=from_date, page_size=page_size, start=start, page_url=page_url,
            extra_params={"from_date": from_date, "to_date": to_date,
                          "state": state},
            gt_param="from_date",
        )

    async def list_reimbursements(
        self,
        *,
        updated_after: str | None = None,
        from_date: str | None = None,
        page_size: int = 100,
        start: str | None = None,
        page_url: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        self._check_fault()
        return self._page(
            "reimbursements", "reimbursement", "updated_at",
            gt=updated_after, page_size=page_size, start=start,
            page_url=page_url,
            extra_params={"updated_after": updated_after,
                          "from_date": from_date},
            gt_param="updated_after",
        )

    async def list_cards(
        self,
        *,
        page_size: int = 100,
        start: str | None = None,
        page_url: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        self._check_fault()
        return self._page(
            "cards", "card", None,
            gt=None, page_size=page_size, start=start, page_url=page_url,
            extra_params={}, gt_param=None,
        )

    async def list_users(
        self,
        *,
        page_size: int = 100,
        start: str | None = None,
        page_url: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        self._check_fault()
        return self._page(
            "users", "user", None,
            gt=None, page_size=page_size, start=start, page_url=page_url,
            extra_params={}, gt_param=None,
        )

    async def business(self) -> dict[str, Any]:
        """`GET /business` analogue — the connectivity probe."""
        self._check_fault()
        return {
            "id": self._business_id or "mock-ramp-business",
            "business_name_legal": f"Synthetic Co {self._business_id}",
            "business_name_on_card": f"Synthetic Co {self._business_id}",
            "active": True,
        }

    # ---- Keyset paging core ----
    def _page(
        self,
        resource: str,
        entity_type: str,
        ts_field: str | None,
        *,
        gt: str | None,
        page_size: int,
        start: str | None,
        page_url: str | None,
        extra_params: dict[str, Any],
        gt_param: str | None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        # A follow-up call carries the previous `page.next` URL — recover the
        # keyset position + window from it (exactly what the real API does).
        if page_url:
            q = parse_qs(urlsplit(page_url).query)
            start = (q.get("start") or [None])[0]
            page_size = int((q.get("page_size") or [page_size])[0])
            if gt_param:
                gt = (q.get(gt_param) or [gt])[0]

        rows_all = self._entities_for(entity_type)
        if gt and ts_field:
            rows_all = [
                r for r in rows_all
                if isinstance(r.get(ts_field), str) and r[ts_field] > gt
            ]

        per_page = max(2, min(self._default_page_size, int(page_size)))

        pos = 0
        if start:
            for i, r in enumerate(rows_all):
                if str(r.get("id")) == start:
                    pos = i + 1
                    break
        page = rows_all[pos:pos + per_page]

        # Terminal exactly as the real envelope signals it: `page.next` is
        # null when the page is short/empty.
        if len(page) < per_page or not page:
            return page, None
        params: dict[str, Any] = {
            k: v for k, v in extra_params.items() if v is not None
        }
        if gt_param and gt:
            params[gt_param] = gt
        params["start"] = str(page[-1].get("id"))
        params["page_size"] = per_page
        next_url = f"{_BASE}/{resource}?{urlencode(params)}"
        return page, next_url

    # ---- Helpers ----
    def _entities_for(self, entity_type: str) -> list[dict[str, Any]]:
        ents = self._fixture.get("entities", {})
        rows = ents.get(entity_type, [])
        return list(rows) if isinstance(rows, list) else []

    # ---- Fault raisers (production exception parity) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise RampApiError(
            "MockRampClient: rate limit 429 (X2 fault)",
            code="ramp_api_rate_limited",
            context={"http_status": 429, "retry_after": "1"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise RampApiError(
            "MockRampClient: 503 (X2 fault)",
            code="ramp_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise RampApiError(
            "MockRampClient: 401 access token rejected (X2 fault)",
            code="ramp_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise RampApiError(
            "MockRampClient: transient transport error (X2 fault)",
            code="ramp_api_error",
            context={"error_type": "TransportError"},
        )


__all__ = ["MockRampClient"]
