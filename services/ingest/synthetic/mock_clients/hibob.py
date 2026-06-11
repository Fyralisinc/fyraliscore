"""MockHibobClient — HiBob People/HR read surface used by IN-PEOPLE backfill.

In-process replacement for `HibobClient` at the `_open_hibob_client` fetcher seam
(services/ingest/ingestion/fetchers/hibob.py). Implements ONLY what the
backfill/poll fetcher + reconciler call:

  - list_entities(entity_type, limit=..., offset=..., page_cursor=...,
        modified_since=...) -> (rows, next_page | None)
  - company_info()  -> connectivity probe (the reconciler does not call it, but
    parity with the real client surface keeps the seam swap total)

Pagination mirrors the real client's offset/limit semantics exactly so the
fetcher's cursor loop behaves identically:
  - the slice is `rows[offset : offset + limit]`,
  - `next_offset = offset + len(page)`; a page shorter than `limit` (or empty) is
    terminal -> `next_offset is None` (matches client.list_entities's
    `is_last = len(rows) < limit or not rows`).

Incremental filter: the fetcher passes `modified_since=<iso>` in poll mode. The
mock honours it by dropping entities whose `modified` field is not strictly
greater (matching the handler's `_modified_of` field precedence).

Fault injection: `self._check_fault()` runs first on every public method and the
four raisers surface `HibobApiError` with the same stable `code`s the real client
emits, so the fetcher's rate-limit branch (and reconciler error mapping) see
exactly the production exception shape.
"""
from __future__ import annotations

from typing import Any, NoReturn

from lib.shared.errors import HibobApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockHibobClient(_MockBase):
    """Stateful in-process replacement for `HibobClient`.

    `fixture` shape (per `make_hibob`):
        {
          "company_id": "hibob-co-0001",
          "page_size": 100,
          "entities": {
            "employee":  [ {<full HiBob entity>}, ... ],
            "lifecycle": [ ... ],
            "timeoff":   [ ... ],
            "payroll":   [ ... ],
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
        self._company_id = str(fixture.get("company_id", ""))
        self._default_page_size = int(fixture.get("page_size", 100))

    # ---- IN-PEOPLE read surface ----
    async def list_entities(
        self,
        entity_type: str,
        *,
        limit: int = 100,
        offset: int = 0,
        page_cursor: str | None = None,
        modified_since: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | str | None]:
        """One page of `entity_type` rows + the next offset/cursor (or None).

        Returns `(rows, next_offset)`; `next_offset is None` is terminal. Honours
        the `modified_since` incremental filter when present.
        """
        self._check_fault()

        rows_all = self._entities_for(entity_type)
        rows_all = self._apply_modified_since(rows_all, modified_since)

        # The page cap the fetcher actually honours is min(requested, fixture).
        per_page = min(limit, self._default_page_size)
        start = int(page_cursor) if isinstance(page_cursor, str) and page_cursor.isdigit() else max(0, offset)
        page = rows_all[start:start + per_page]

        # Terminal exactly as the real client computes it: a short (or empty)
        # page ends the stream.
        is_last = len(page) < per_page or not page
        next_offset = None if is_last else offset + len(page)
        return page, next_offset

    async def company_info(self) -> dict[str, Any]:
        """Connectivity probe analogue. Returns a minimal company body."""
        self._check_fault()
        return {
            "id": self._company_id or "1",
            "name": f"Synthetic Co {self._company_id}",
        }

    async def aclose(self) -> None:
        return None

    # ---- Helpers ----
    def _entities_for(self, entity_type: str) -> list[dict[str, Any]]:
        ents = self._fixture.get("entities", {})
        rows = ents.get(entity_type, [])
        return list(rows) if isinstance(rows, list) else []

    def _apply_modified_since(
        self, rows: list[dict[str, Any]], modified_since: str | None,
    ) -> list[dict[str, Any]]:
        if not modified_since:
            return rows
        return [r for r in rows if (self._modified(r) or "") > modified_since]

    @staticmethod
    def _modified(row: dict[str, Any]) -> str | None:
        for key in ("modified", "modifiedAt", "lastModified", "updatedAt", "updated"):
            v = row.get(key)
            if isinstance(v, str) and v:
                return v
        return None

    # ---- Fault raisers (production exception parity) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise HibobApiError(
            "MockHibobClient: rate limit 429 (X2 fault)",
            code="hibob_api_rate_limited",
            context={"http_status": 429, "retry_after": "1"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise HibobApiError(
            "MockHibobClient: 503 (X2 fault)",
            code="hibob_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise HibobApiError(
            "MockHibobClient: 401 service-user token rejected (X2 fault)",
            code="hibob_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise HibobApiError(
            "MockHibobClient: transient transport error (X2 fault)",
            code="hibob_api_error",
            context={"error_type": "TransportError"},
        )


__all__ = ["MockHibobClient"]
