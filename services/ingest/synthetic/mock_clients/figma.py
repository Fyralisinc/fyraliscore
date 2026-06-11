"""MockFigmaClient — Figma design API surface used by the design backfill.

In-process replacement for `FigmaClient` (services/ingest/integrations/figma/
client.py). Implements the read methods the Figma chain calls:

  - get_file(file_key) -> dict
      `GET /v1/files/{key}` body (the fetcher's recency probe).
  - list_events(file_key, *, limit, offset, start)
      -> (events, next_offset, total)
      Derived versions/comments event stream, offset-paginated inside the mock.
      `next_offset is None` is terminal — exactly the real client's contract
      (`is_last = next_offset >= total or not events`).
  - has_events_since(file_key, since)  [reconciler probe convenience]
      A thin wrapper over `list_events(limit=1, start=since[:10])` mirroring what
      reconcilers/figma.py does to detect a gap.

Every public method calls `self._check_fault()` first (A21), so the fetcher /
reconciler see real `FigmaApiError` types with the right `code` on a configured
fault — same as production.

`fixture` shape: see fixtures/figma_generator.py::make_figma.
"""
from __future__ import annotations

from typing import Any, NoReturn

from services.ingest.integrations.figma.client import FigmaApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockFigmaClient(_MockBase):
    """Stateful in-process replacement for `FigmaClient`.

    Pagination is stateless (offset-based, like the real client): `offset`
    indexes into the file's newest-first event list and `limit` caps the page.
    The `start` (ISO date) lower bound filters by `createdAt` date — modelling
    the incremental-poll window the fetcher passes after a warm start.
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
    async def list_files(self, team_id: str | None = None) -> list[dict[str, Any]]:
        """`GET /v1/teams/{id}/projects` + project file enumeration."""
        self._check_fault()
        files = self._fixture.get("files", {})
        out: list[dict[str, Any]] = []
        for key in self._fixture.get("file_order", list(files.keys())):
            fx = files.get(key)
            if isinstance(fx, dict):
                meta = {k: v for k, v in fx.items() if k != "events"}
                meta.setdefault("file_key", key)
                out.append(meta)
        return out

    async def get_file(self, file_key: str) -> dict[str, Any]:
        """`GET /v1/files/{key}` — the recency-probe body."""
        self._check_fault()
        fx = self._fixture.get("files", {}).get(file_key)
        if not isinstance(fx, dict):
            raise FigmaApiError(
                f"MockFigmaClient: file {file_key!r} not found",
                code="figma_api_not_found",
                context={"file_key": file_key},
            )
        # Return the file body WITHOUT the embedded event list (the real
        # `GET /v1/files/{key}/meta` does not inline events).
        return {k: v for k, v in fx.items() if k != "events"}

    async def list_events(
        self,
        file_key: str,
        *,
        limit: int = 100,
        offset: int = 0,
        start: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        """`GET /v1/files/{key}/events` — offset-paginated, newest-first.

        Returns `(events, next_offset, total)`. `next_offset is None` signals
        the last page (matching the real client's terminal contract).
        """
        self._check_fault()
        events = self._events_for(file_key)
        if start:
            events = [e for e in events if _event_date(e) >= start[:10]]
        total = len(events)
        # Honour the fixture's page-size cap the same way the real client bounds
        # the page (and how the github mock caps per_page).
        page_cap = int(self._fixture.get("page_size", limit) or limit)
        eff_limit = min(limit, page_cap)
        page = events[offset:offset + eff_limit]
        next_offset = offset + len(page)
        is_last = next_offset >= total or not page
        return page, (None if is_last else next_offset), total

    async def has_events_since(
        self, file_key: str, since: str,
    ) -> bool:
        """Reconciler probe convenience: True if any event is dated on/after
        `since`. Mirrors reconcilers/figma.py's `list_events(limit=1,
        start=high_water[:10])` gap check."""
        self._check_fault()
        events = self._events_for(file_key)
        floor = since[:10]
        return any(_event_date(e) >= floor for e in events)

    # ---- Helpers ----
    def _events_for(self, file_key: str) -> list[dict[str, Any]]:
        fx = self._fixture.get("files", {}).get(file_key)
        if not isinstance(fx, dict):
            return []
        events = fx.get("events")
        return list(events) if isinstance(events, list) else []

    # ---- Fault raisers (surface the real FigmaApiError + codes) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise FigmaApiError(
            "MockFigmaClient: rate limit (X2 fault)",
            code="figma_api_rate_limited",
            context={"http_status": 429, "retry_after": "1"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise FigmaApiError(
            "MockFigmaClient: 503 (X2 fault)",
            code="figma_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise FigmaApiError(
            "MockFigmaClient: 401 token rejected (X2 fault)",
            code="figma_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise FigmaApiError(
            "MockFigmaClient: transient transport error (X2 fault)",
            code="figma_api_error",
            context={"error_type": "TransportError"},
        )


def _event_date(event: dict[str, Any]) -> str:
    """Date portion of an event's created timestamp (for `start` filtering)."""
    iso = event.get("createdAt") or event.get("created_at") or ""
    return iso[:10] if isinstance(iso, str) else ""


__all__ = ["MockFigmaClient"]
