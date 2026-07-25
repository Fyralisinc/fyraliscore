"""MockAshbyClient — Ashby recruiting-ATS RPC surface used by IN-PEOPLE backfill.

In-process replacement for `AshbyClient` at the `_open_ashby_client` fetcher seam
(services/ingest/ingestion/fetchers/ashby.py). Implements ONLY what the
backfill/poll fetcher + reconciler call:

  - list_entities(category, cursor=..., sync_token=..., limit=...)
        -> (results, next_cursor | None, next_sync_token | None)
  - get_entity(category, entity_id)  -> one entity by id (detail / probe)

Pagination mirrors the real client's CURSOR semantics so the fetcher's loop
behaves identically:
  - the request `cursor` is an opaque page token; the mock encodes the offset as a
    decimal string. The slice is `rows[offset : offset + limit]`.
  - the response `nextCursor` is the next offset token when more rows remain (the
    real client only surfaces it when `moreDataAvailable` is true); a terminal page
    returns `next_cursor is None` (matches the fetcher's `is_last = next_cursor is
    None`).

Incremental sync: the fetcher passes `sync_token=<iso>` on a warm-started poll.
The mock treats the syncToken as an ISO floor (the high-water it was minted at) and
returns only entities with `updatedAt` strictly greater. Every `.list` response
also returns a refreshed `next_sync_token` (the max `updatedAt` it walked) so the
NEXT incremental poll resumes from it — the production round-trip the reconciler's
gap probe relies on.

Fault injection: `self._check_fault()` runs first on every public method. This
legacy in-process mock surfaces pre-transport `AshbyApiError` values;
production retryable responses instead emerge from `ProviderTransport` as
typed retry outcomes.
"""
from __future__ import annotations

from typing import Any, NoReturn

from lib.shared.errors import AshbyApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockAshbyClient(_MockBase):
    """Stateful in-process replacement for `AshbyClient`.

    `fixture` shape (per `make_ashby`):
        {
          "org_id": "ashby-org-0001",
          "page_size": 100,
          "entities": {
            "candidate":   [ {<full Ashby entity>}, ... ],
            "application": [ ... ],
            "job":         [ ... ],
            "interview":   [ ... ],
            "offer":       [ ... ],
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
        self._org_id = str(fixture.get("org_id", ""))
        self._default_page_size = int(fixture.get("page_size", 100))

    # ---- IN-PEOPLE read surface ----
    async def list_entities(
        self,
        category: str,
        *,
        cursor: str | None = None,
        sync_token: str | None = None,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], str | None, str | None]:
        """One cursor page of one entity category.

        Returns `(results, next_cursor, next_sync_token)`:
          - `next_cursor is None` (no more rows) is terminal for the walk.
          - `next_sync_token` is the refreshed syncToken to PERSIST for the next
            incremental poll (the max `updatedAt` walked, or the prior token when
            the page was empty).
        """
        self._check_fault()

        rows_all = self._entities_for(category)
        rows_all = self._apply_sync_token(rows_all, sync_token)

        per_page = min(limit, self._default_page_size)
        offset = _decode_cursor(cursor)
        page = rows_all[offset:offset + per_page]

        more = (offset + len(page)) < len(rows_all) and bool(page)
        next_cursor = str(offset + len(page)) if more else None

        # Refresh the syncToken to the max updatedAt the listing covers, so the
        # next incremental poll resumes from it. Keep the supplied token when the
        # page is empty (nothing new to advance past).
        max_updated = self._max_updated(rows_all)
        next_sync_token = max_updated if max_updated is not None else sync_token

        return page, next_cursor, next_sync_token

    async def get_entity(self, category: str, entity_id: str) -> dict[str, Any]:
        """One entity by id (detail / probe). Returns the bare entity object."""
        self._check_fault()
        for row in self._entities_for(category):
            if str(row.get("id") or row.get("Id") or "") == str(entity_id):
                return dict(row)
        return {}

    async def aclose(self) -> None:
        return None

    # ---- Helpers ----
    def _entities_for(self, category: str) -> list[dict[str, Any]]:
        ents = self._fixture.get("entities", {})
        rows = ents.get(category, [])
        return list(rows) if isinstance(rows, list) else []

    def _apply_sync_token(
        self, rows: list[dict[str, Any]], sync_token: str | None,
    ) -> list[dict[str, Any]]:
        if not sync_token:
            return rows
        return [r for r in rows if (self._updated(r) or "") > sync_token]

    def _max_updated(self, rows: list[dict[str, Any]]) -> str | None:
        best: str | None = None
        for r in rows:
            u = self._updated(r)
            if u is not None and (best is None or u > best):
                best = u
        return best

    @staticmethod
    def _updated(row: dict[str, Any]) -> str | None:
        for key in ("updatedAt", "updated_at", "createdAt", "created_at"):
            v = row.get(key)
            if isinstance(v, str) and v:
                return v
        return None

    # ---- Fault raisers (production exception parity) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise AshbyApiError(
            "MockAshbyClient: rate limit 429 (X2 fault)",
            code="ashby_api_rate_limited",
            context={"http_status": 429, "retry_after": "1"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise AshbyApiError(
            "MockAshbyClient: 503 (X2 fault)",
            code="ashby_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise AshbyApiError(
            "MockAshbyClient: 401 API key rejected (X2 fault)",
            code="ashby_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise AshbyApiError(
            "MockAshbyClient: transient transport error (X2 fault)",
            code="ashby_api_error",
            context={"error_type": "TransportError"},
        )


def _decode_cursor(cursor: str | None) -> int:
    """Decode the opaque page cursor (a decimal offset token). A missing /
    malformed cursor starts the walk at offset 0."""
    if not cursor:
        return 0
    try:
        return max(0, int(cursor))
    except (TypeError, ValueError):
        return 0


__all__ = ["MockAshbyClient"]
