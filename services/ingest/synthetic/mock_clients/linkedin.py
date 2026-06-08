"""MockLinkedinClient — LinkedIn organization query surface (IN-PEOPLE backfill).

In-process replacement for `LinkedinClient` at the `_open_linkedin_client` fetcher
seam (services/ingest/ingestion/fetchers/linkedin.py). Implements ONLY what the
backfill/poll fetcher + reconciler call:

  - query(entity, where=..., start_position=..., max_results=...)
        -> (rows, next_start_position | None)
  - org_info()  -> connectivity probe (the reconciler's gap reference call)

Pagination mirrors the real client's offset semantics exactly so the fetcher's
cursor loop behaves identically (cloned from the Carta/Gusto query shape):
  - `start_position` is 1-based; the slice is
    `rows[start_position - 1 : start_position - 1 + max_results]`.
  - `next_start_position = start_position + len(page)`; a page shorter than
    `max_results` (or empty) is terminal -> `next_start_position is None`
    (matches client.query's `is_last = returned < max_results or not rows`).

Incremental filter: the fetcher passes `where="Metadata.LastUpdatedTime > '<iso>'"`
in poll mode. The mock honours it minimally by parsing out the ISO bound and
dropping entities whose `MetaData.LastUpdatedTime` is not strictly greater.

Fault injection: `self._check_fault()` runs first on every public method and the
four raisers surface `LinkedinApiError` with the same stable `code`s the real
client emits, so the fetcher's rate-limit branch (and reconciler error mapping)
see exactly the production exception shape.

NOTE: LinkedIn recruitment APIs are PARTNER-GATED (Marketing Developer Platform /
Talent Solutions, invite-only). This mock mirrors the placeholder read surface the
production client clones from Carta/Gusto; the real REST/pagination shapes are
UNVERIFIED pending partner entitlement.
"""
from __future__ import annotations

import re
from typing import Any, NoReturn

from services.ingest.integrations.linkedin.client import LinkedinApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


_WHERE_GT = re.compile(
    r"Metadata\.LastUpdatedTime\s*>\s*'([^']*)'", re.IGNORECASE,
)


class MockLinkedinClient(_MockBase):
    """Stateful in-process replacement for `LinkedinClient`.

    `fixture` shape (per `make_linkedin`):
        {
          "organization_urn": "li-org-0001",
          "page_size": 100,
          "entities": {
            "share":         [ {<full LinkedIn entity>}, ... ],
            "social_action": [ ... ],
            "follower_stat": [ ... ],
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

    # ---- IN-PEOPLE read surface ----
    async def query(
        self,
        entity: str,
        *,
        where: str | None = None,
        order_by: str = "Metadata.LastUpdatedTime",
        start_position: int = 1,
        max_results: int = 100,
    ) -> tuple[list[dict[str, Any]], int | None]:
        """One offset page of `entity` rows + the next STARTPOSITION (or None).

        Returns `(rows, next_start_position)`; `next_start_position is None` is
        terminal. Honours the `Metadata.LastUpdatedTime > '...'` incremental
        WHERE filter when present.
        """
        self._check_fault()

        rows_all = self._entities_for(entity)
        rows_all = self._apply_where(rows_all, where)

        # The page cap the fetcher actually honours is min(requested, fixture).
        per_page = min(max_results, self._default_page_size)
        # STARTPOSITION is 1-based.
        start = max(0, start_position - 1)
        page = rows_all[start:start + per_page]

        # Terminal exactly as the real client computes it: a short (or empty)
        # page ends the stream.
        is_last = len(page) < per_page or not page
        next_start = None if is_last else start_position + len(page)
        return page, next_start

    async def org_info(self) -> dict[str, Any]:
        """Connectivity probe analogue — a minimal OrgInfo body."""
        self._check_fault()
        return {
            "OrgInfo": {
                "Id": self._organization_urn or "1",
                "OrgName": f"Synthetic Org {self._organization_urn}",
            },
        }

    async def aclose(self) -> None:
        return None

    # ---- Helpers ----
    def _entities_for(self, entity: str) -> list[dict[str, Any]]:
        ents = self._fixture.get("entities", {})
        rows = ents.get(entity, [])
        return list(rows) if isinstance(rows, list) else []

    def _apply_where(
        self, rows: list[dict[str, Any]], where: str | None,
    ) -> list[dict[str, Any]]:
        if not where:
            return rows
        m = _WHERE_GT.search(where)
        if not m:
            return rows
        bound = m.group(1)
        return [r for r in rows if (self._last_updated(r) or "") > bound]

    @staticmethod
    def _last_updated(row: dict[str, Any]) -> str | None:
        meta = row.get("MetaData") or row.get("Metadata") or {}
        if isinstance(meta, dict):
            v = meta.get("LastUpdatedTime")
            return v if isinstance(v, str) else None
        return None

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
