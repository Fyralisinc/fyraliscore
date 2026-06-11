"""MockLinkedinClient — LinkedIn Community-Management read surface (IN-PEOPLE).

In-process replacement for `LinkedinClient` at the `_open_linkedin_client`
fetcher seam (services/ingest/ingestion/fetchers/linkedin.py). Implements ONLY
what the backfill/poll fetcher + reconciler + onboarding probe call:

  - list_posts(start=..., count=..., sort_by=...)
        -> (elements, next_start | None)   # the /rest/posts?q=author finder
  - share_statistics(start_ms=..., end_ms=..., granularity=...)
        -> [elements]                      # organizationalEntityShareStatistics
  - follower_statistics(start_ms=..., end_ms=..., granularity=...)
        -> [elements]                      # organizationalEntityFollowerStatistics
  - get_organization() -> connectivity probe (GET /rest/organizations/{id})

Pagination mirrors the real client's Rest.li offset semantics exactly so the
fetcher's cursor loop behaves identically:
  - posts are served DESC by `lastModifiedAt` (the finder's `sortBy=
    LAST_MODIFIED` default); `start` is 0-based; the slice is
    `rows[start : start + count]` (count capped at 100 like the wire).
  - `next_start = start + len(page)`; a page shorter than `count` (or empty)
    is terminal -> `next_start is None` (the mock never emits `paging.links`,
    matching the real client's `is_last` computation).
  - the statistics finders are NOT paginated: one call returns the whole
    window, filtered by `timeRange.start >= start_ms` (inclusive, like the
    wire) and `< end_ms` (exclusive) when bounds are given. `granularity` is
    accepted but ignored — fixture buckets are pre-shaped.

Fault injection: `self._check_fault()` runs first on every public method and the
four raisers surface `LinkedinApiError` with the same stable `code`s the real
client emits, so the fetcher's rate-limit branch (and reconciler error mapping)
see exactly the production exception shape.
"""
from __future__ import annotations

from typing import Any, NoReturn

from services.ingest.integrations.linkedin.client import (
    LinkedinApiError,
    organization_id_of,
)
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockLinkedinClient(_MockBase):
    """Stateful in-process replacement for `LinkedinClient`.

    `fixture` shape (per `make_linkedin`):
        {
          "organization_urn": "li-org-0001",
          "page_size": 100,
          "entities": {
            "post":                [ {<post element>}, ... ],
            "share_statistics":    [ {<time-bound element>}, ... ],
            "follower_statistics": [ {<time-bound element>}, ... ],
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
        self._organization_urn = str(fixture.get("organization_urn", ""))
        self._default_page_size = int(fixture.get("page_size", 100))

    # ---- Read surface ----
    async def list_posts(
        self,
        *,
        start: int = 0,
        count: int = 100,
        sort_by: str = "LAST_MODIFIED",
    ) -> tuple[list[dict[str, Any]], int | None]:
        """One offset page of the org's posts (DESC by lastModifiedAt) + the
        next `start` (or None when terminal)."""
        self._check_fault()

        rows_all = sorted(
            self._entities_for("post"),
            key=lambda r: (
                r.get("lastModifiedAt") or r.get("createdAt") or 0
            ),
            reverse=True,
        )
        per_page = max(1, min(count, self._default_page_size, 100))
        start = max(0, int(start))
        page = rows_all[start:start + per_page]

        # Terminal exactly as the real client computes it (no `next` link is
        # ever emitted by the mock): a short (or empty) page ends the stream.
        is_last = len(page) < per_page or not page
        next_start = None if is_last else start + len(page)
        return page, next_start

    async def share_statistics(
        self,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
        granularity: str = "DAY",
    ) -> list[dict[str, Any]]:
        """The whole (filtered) share-statistics window — NOT paginated."""
        self._check_fault()
        return self._window(self._entities_for("share_statistics"), start_ms, end_ms)

    async def follower_statistics(
        self,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
        granularity: str = "DAY",
    ) -> list[dict[str, Any]]:
        """The whole (filtered) follower-statistics window — NOT paginated."""
        self._check_fault()
        return self._window(
            self._entities_for("follower_statistics"), start_ms, end_ms,
        )

    async def get_organization(self) -> dict[str, Any]:
        """Connectivity probe analogue — a minimal organization body."""
        self._check_fault()
        org_id = organization_id_of(self._organization_urn) or "1"
        return {
            "id": org_id,
            "localizedName": f"Synthetic Org {self._organization_urn}",
            "vanityName": f"synthetic-org-{org_id}",
            "$URN": f"urn:li:organization:{org_id}",
        }

    async def aclose(self) -> None:
        return None

    # ---- Helpers ----
    def _entities_for(self, entity: str) -> list[dict[str, Any]]:
        ents = self._fixture.get("entities", {})
        rows = ents.get(entity, [])
        return list(rows) if isinstance(rows, list) else []

    @staticmethod
    def _window(
        rows: list[dict[str, Any]], start_ms: int | None, end_ms: int | None,
    ) -> list[dict[str, Any]]:
        def _start_of(row: dict[str, Any]) -> int | None:
            tr = row.get("timeRange")
            if isinstance(tr, dict) and isinstance(tr.get("start"), int):
                return tr["start"]
            return None

        out: list[dict[str, Any]] = []
        for row in rows:
            bucket = _start_of(row)
            if start_ms is not None and (bucket is None or bucket < start_ms):
                continue
            if end_ms is not None and bucket is not None and bucket >= end_ms:
                continue
            out.append(row)
        return out

    # ---- Fault raisers (production exception parity) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise LinkedinApiError(
            "MockLinkedinClient: rate limit 429 (X2 fault)",
            code="linkedin_api_rate_limited",
            context={"http_status": 429, "retry_after": "1"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise LinkedinApiError(
            "MockLinkedinClient: 503 (X2 fault)",
            code="linkedin_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise LinkedinApiError(
            "MockLinkedinClient: 401 access token rejected (X2 fault)",
            code="linkedin_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise LinkedinApiError(
            "MockLinkedinClient: transient transport error (X2 fault)",
            code="linkedin_api_error",
            context={"error_type": "TransportError"},
        )


__all__ = ["MockLinkedinClient"]
