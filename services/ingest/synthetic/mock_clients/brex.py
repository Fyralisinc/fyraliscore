"""MockBrexClient — Brex banking API surface used by the finance backfill.

In-process replacement for `BrexClient` (services/ingest/integrations/brex/
client.py). Implements the read methods the planner-less Brex chain calls:

  - get_account(account_id) -> dict
      Real client enumerates Brex v2 cash/card account lists and returns one
      matching account (the fetcher's first-page balance snapshot probe).
  - list_transactions(account_id, *, limit, offset, start, account_kind)
      -> (txns, next_offset, total)
      Brex v2 cash/card transaction stream, offset-paginated inside the mock,
      honouring `limit`.
      `next_offset is None` is terminal — exactly the real client's contract
      (`is_last = next_offset >= total or not txns`).
  - has_transactions_since(account_id, since)  [reconciler probe convenience]
      A thin wrapper over `list_transactions(limit=1, start=since[:10])` mirroring
      what reconcilers/brex.py does to detect a gap.

Every public method calls `self._check_fault()` first (A21), so the fetcher /
reconciler see real `BrexApiError` types with the right `code` on a configured
fault — same as production.

`fixture` shape: see fixtures/brex_generator.py::make_brex.
"""
from __future__ import annotations

from typing import Any, NoReturn

from lib.shared.errors import BrexApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockBrexClient(_MockBase):
    """Stateful in-process replacement for `BrexClient`.

    Pagination is stateless (offset-based, like the real client): `offset`
    indexes into the account's newest-first transaction list and `limit` caps the
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
    async def get_account(self, account_id: str) -> dict[str, Any]:
        """`GET /account/{id}` — the balance-snapshot probe body."""
        self._check_fault()
        acct = self._fixture.get("accounts", {}).get(account_id)
        if not isinstance(acct, dict):
            raise BrexApiError(
                f"MockBrexClient: account {account_id!r} not found",
                code="brex_api_not_found",
                context={"account_id": account_id},
            )
        # Return the account body WITHOUT the embedded transaction list (the real
        # `GET /account/{id}` does not inline transactions).
        return {k: v for k, v in acct.items() if k != "transactions"}

    async def list_transactions(
        self,
        account_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
        start: str | None = None,
        account_kind: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        """`GET /account/{id}/transactions` — offset-paginated, newest-first.

        Returns `(transactions, next_offset, total)`. `next_offset is None`
        signals the last page (matching the real client's terminal contract).
        """
        self._check_fault()
        txns = self._transactions_for(account_id)
        if start:
            txns = [t for t in txns if _txn_date(t) >= start[:10]]
        total = len(txns)
        # Honour the fixture's page-size cap the same way the real client bounds
        # the page (and how the github mock caps per_page).
        page_cap = int(self._fixture.get("page_size", limit) or limit)
        eff_limit = min(limit, page_cap)
        page = txns[offset:offset + eff_limit]
        next_offset = offset + len(page)
        is_last = next_offset >= total or not page
        return page, (None if is_last else next_offset), total

    async def has_transactions_since(
        self, account_id: str, since: str,
    ) -> bool:
        """Reconciler probe convenience: True if any transaction is dated on/after
        `since`. Mirrors reconcilers/brex.py's `list_transactions(limit=1,
        start=high_water[:10])` gap check."""
        self._check_fault()
        txns = self._transactions_for(account_id)
        floor = since[:10]
        return any(_txn_date(t) >= floor for t in txns)

    # ---- Helpers ----
    def _transactions_for(self, account_id: str) -> list[dict[str, Any]]:
        acct = self._fixture.get("accounts", {}).get(account_id)
        if not isinstance(acct, dict):
            return []
        txns = acct.get("transactions")
        return list(txns) if isinstance(txns, list) else []

    # ---- Fault raisers (surface the real BrexApiError + codes) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise BrexApiError(
            "MockBrexClient: rate limit (X2 fault)",
            code="brex_api_rate_limited",
            context={"http_status": 429, "retry_after": "1"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise BrexApiError(
            "MockBrexClient: 503 (X2 fault)",
            code="brex_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise BrexApiError(
            "MockBrexClient: 401 token rejected (X2 fault)",
            code="brex_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise BrexApiError(
            "MockBrexClient: transient transport error (X2 fault)",
            code="brex_api_error",
            context={"error_type": "TransportError"},
        )


def _txn_date(txn: dict[str, Any]) -> str:
    """Date portion of a txn's posted/created timestamp (for `start` filtering)."""
    iso = (
        txn.get("postedAt")
        or txn.get("posted_at")
        or txn.get("createdAt")
        or txn.get("created_at")
        or ""
    )
    return iso[:10] if isinstance(iso, str) else ""


__all__ = ["MockBrexClient"]
