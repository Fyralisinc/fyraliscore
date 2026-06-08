"""MockDeelClient — Deel API surface used by the finance backfill.

In-process replacement for `DeelClient` (services/ingest/integrations/deel/
client.py). Implements the read methods the planner-less Deel chain calls:

  - get_contract(contract_id) -> dict
      `GET /contract/{id}` body (the fetcher's first-page state snapshot probe).
  - list_payments(contract_id, *, limit, offset, start)
      -> (payments, next_offset, total)
      `GET /contract/{id}/payments`, offset-paginated, honouring `limit`.
      `next_offset is None` is terminal — exactly the real client's contract
      (`is_last = next_offset >= total or not payments`).
  - has_payments_since(contract_id, since)  [reconciler probe convenience]
      A thin wrapper over `list_payments(limit=1, start=since[:10])` mirroring
      what reconcilers/deel.py does to detect a gap.

Every public method calls `self._check_fault()` first (A21), so the fetcher /
reconciler see real `DeelApiError` types with the right `code` on a configured
fault — same as production.

`fixture` shape: see fixtures/deel_generator.py::make_deel.
"""
from __future__ import annotations

from typing import Any, NoReturn

from lib.shared.errors import DeelApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockDeelClient(_MockBase):
    """Stateful in-process replacement for `DeelClient`.

    Pagination is stateless (offset-based, like the real client): `offset`
    indexes into the contract's newest-first payment list and `limit` caps the
    page. The `start` (ISO date) lower bound filters by `postedAt`/`createdAt`
    date — modelling the incremental-poll window the fetcher passes after a warm
    start.
    """

    def __init__(
        self,
        *,
        fixture: dict[str, Any],
        profile: FaultProfile = HAPPY_PATH,
    ) -> None:
        super().__init__(profile=profile)
        self._fixture = fixture

    # ---- Read surface ----
    async def get_contract(self, contract_id: str) -> dict[str, Any]:
        """`GET /contract/{id}` — the state-snapshot probe body."""
        self._check_fault()
        con = self._fixture.get("contracts", {}).get(contract_id)
        if not isinstance(con, dict):
            raise DeelApiError(
                f"MockDeelClient: contract {contract_id!r} not found",
                code="deel_api_not_found",
                context={"contract_id": contract_id},
            )
        # Return the contract body WITHOUT the embedded payment list (the real
        # `GET /contract/{id}` does not inline payments).
        return {k: v for k, v in con.items() if k != "payments"}

    async def list_payments(
        self,
        contract_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
        start: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        """`GET /contract/{id}/payments` — offset-paginated, newest-first.

        Returns `(payments, next_offset, total)`. `next_offset is None`
        signals the last page (matching the real client's terminal contract).
        """
        self._check_fault()
        payments = self._payments_for(contract_id)
        if start:
            payments = [p for p in payments if _payment_date(p) >= start[:10]]
        total = len(payments)
        # Honour the fixture's page-size cap the same way the real client bounds
        # the page (and how the github mock caps per_page).
        page_cap = int(self._fixture.get("page_size", limit) or limit)
        eff_limit = min(limit, page_cap)
        page = payments[offset:offset + eff_limit]
        next_offset = offset + len(page)
        is_last = next_offset >= total or not page
        return page, (None if is_last else next_offset), total

    async def has_payments_since(
        self, contract_id: str, since: str,
    ) -> bool:
        """Reconciler probe convenience: True if any payment is dated on/after
        `since`. Mirrors reconcilers/deel.py's `list_payments(limit=1,
        start=high_water[:10])` gap check."""
        self._check_fault()
        payments = self._payments_for(contract_id)
        floor = since[:10]
        return any(_payment_date(p) >= floor for p in payments)

    # ---- Helpers ----
    def _payments_for(self, contract_id: str) -> list[dict[str, Any]]:
        con = self._fixture.get("contracts", {}).get(contract_id)
        if not isinstance(con, dict):
            return []
        payments = con.get("payments")
        return list(payments) if isinstance(payments, list) else []

    # ---- Fault raisers (surface the real DeelApiError + codes) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise DeelApiError(
            "MockDeelClient: rate limit (X2 fault)",
            code="deel_api_rate_limited",
            context={"http_status": 429, "retry_after": "1"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise DeelApiError(
            "MockDeelClient: 503 (X2 fault)",
            code="deel_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise DeelApiError(
            "MockDeelClient: 401 token rejected (X2 fault)",
            code="deel_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise DeelApiError(
            "MockDeelClient: transient transport error (X2 fault)",
            code="deel_api_error",
            context={"error_type": "TransportError"},
        )


def _payment_date(payment: dict[str, Any]) -> str:
    """Date portion of a payment's posted/created timestamp (for `start` filtering)."""
    iso = payment.get("postedAt") or payment.get("createdAt") or ""
    return iso[:10] if isinstance(iso, str) else ""


__all__ = ["MockDeelClient"]
