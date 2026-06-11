"""MockFirefliesClient — Fireflies transcript API surface for the backfill.

In-process replacement for `FirefliesClient` (services/ingest/integrations/
fireflies/client.py). Implements the read methods the Fireflies chain calls:

  - get_workspace() -> dict
      `GET /workspace` body (the install-time workspace probe).
  - get_transcript(transcript_id) -> dict
      `GET /transcript/{id}` body (hydrate probe).
  - list_transcripts(*, limit, offset, start)
      -> (transcripts, next_offset, total)
      `GET /transcripts`, offset-paginated, honouring `limit`. `next_offset is
      None` is terminal — exactly the real client's contract
      (`is_last = next_offset >= total or not items`).
  - has_transcripts_since(since)  [reconciler probe convenience]
      A thin wrapper over `list_transcripts(limit=1, start=since[:10])` mirroring
      what reconcilers/fireflies.py does to detect a gap.

Every public method calls `self._check_fault()` first (A21), so the fetcher /
reconciler see real `FirefliesApiError` types with the right `code` on a
configured fault — same as production.

`fixture` shape: see fixtures/fireflies_generator.py::make_fireflies.
"""
from __future__ import annotations

from typing import Any, NoReturn

from lib.shared.errors import FirefliesApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockFirefliesClient(_MockBase):
    """Stateful in-process replacement for `FirefliesClient`.

    Pagination is stateless (offset-based, like the real client): `offset`
    indexes into the workspace's newest-first transcript list and `limit` caps
    the page. The `start` (ISO date) lower bound filters by `dateTime`/`date`
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
    async def get_workspace(self) -> dict[str, Any]:
        """`GET /workspace` — the workspace probe body."""
        self._check_fault()
        return {
            "workspace_id": self._fixture.get("workspace_id"),
            "id": self._fixture.get("workspace_id"),
            "name": self._fixture.get("workspace_name") or "Synthetic Workspace",
        }

    async def get_transcript(self, transcript_id: str) -> dict[str, Any]:
        """`GET /transcript/{id}` — one transcript body."""
        self._check_fault()
        for t in self._transcripts():
            if _transcript_id(t) == transcript_id:
                return dict(t)
        raise FirefliesApiError(
            f"MockFirefliesClient: transcript {transcript_id!r} not found",
            code="fireflies_api_not_found",
            context={"transcript_id": transcript_id},
        )

    async def list_transcripts(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        start: str | None = None,
    ) -> tuple[list[dict[str, Any]], int | None, int]:
        """`GET /transcripts` — offset-paginated, newest-first.

        Returns `(transcripts, next_offset, total)`. `next_offset is None`
        signals the last page (matching the real client's terminal contract).
        """
        self._check_fault()
        items = self._transcripts()
        if start:
            items = [t for t in items if _transcript_date(t) >= start[:10]]
        total = len(items)
        # Honour the fixture's page-size cap the same way the real client bounds
        # the page (and how the brex mock caps per page).
        page_cap = int(self._fixture.get("page_size", limit) or limit)
        eff_limit = min(limit, page_cap)
        page = items[offset:offset + eff_limit]
        next_offset = offset + len(page)
        is_last = next_offset >= total or not page
        return page, (None if is_last else next_offset), total

    async def has_transcripts_since(self, since: str) -> bool:
        """Reconciler probe convenience: True if any transcript is dated on/after
        `since`. Mirrors reconcilers/fireflies.py's `list_transcripts(limit=1,
        start=high_water[:10])` gap check."""
        self._check_fault()
        floor = since[:10]
        return any(_transcript_date(t) >= floor for t in self._transcripts())

    # ---- Helpers ----
    def _transcripts(self) -> list[dict[str, Any]]:
        txns = self._fixture.get("transcripts")
        return list(txns) if isinstance(txns, list) else []

    # ---- Fault raisers (surface the real FirefliesApiError + codes) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise FirefliesApiError(
            "MockFirefliesClient: rate limit (X2 fault)",
            code="fireflies_api_rate_limited",
            context={"http_status": 429, "retry_after": "1"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise FirefliesApiError(
            "MockFirefliesClient: 503 (X2 fault)",
            code="fireflies_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise FirefliesApiError(
            "MockFirefliesClient: 401 token rejected (X2 fault)",
            code="fireflies_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise FirefliesApiError(
            "MockFirefliesClient: transient transport error (X2 fault)",
            code="fireflies_api_error",
            context={"error_type": "TransportError"},
        )


def _transcript_id(t: dict[str, Any]) -> str:
    return str(t.get("id") or t.get("transcript_id") or t.get("transcriptId") or "")


def _transcript_date(t: dict[str, Any]) -> str:
    """Date portion of a transcript's date/dateTime (for `start` filtering)."""
    iso = t.get("dateTime") or t.get("date") or t.get("createdAt") or ""
    if isinstance(iso, (int, float)):
        from datetime import datetime, timezone
        return datetime.fromtimestamp(iso / 1000.0, tz=timezone.utc).date().isoformat()
    return iso[:10] if isinstance(iso, str) else ""


__all__ = ["MockFirefliesClient"]
