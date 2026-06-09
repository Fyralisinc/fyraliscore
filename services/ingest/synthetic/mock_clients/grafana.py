"""MockGrafanaClient — Grafana HTTP API surface used by IN-GRAFANA backfill/poll.

Stateless in-process replacement for `GrafanaClient`
(`services/ingest/integrations/grafana/client.py`). Implements the methods the
production fetcher (`fetchers/grafana.py`) and reconciler (`reconcilers/grafana.py`)
call against the `_open_grafana_client` seam:

  - list_annotations(from_ms=..., to_ms=..., limit=100) -> list[dict]
      Returns the annotation objects whose `time` (epoch ms) is within the
      [from_ms, to_ms] window, NEWEST-FIRST, capped at min(limit, per_page).
      Mirrors the real `GET /api/annotations` contract: a bare array, newest
      first, `from`/`to` in epoch MILLISECONDS. The fetcher's backward walk
      lowers `to_ms` to `min(time seen) - 1` each page and stops when a page
      comes back shorter than `limit` (end-of-data).
  - has_annotations_since(from_ms=...) -> bool
      Reconciler gap probe: is there >=1 annotation at/after `from_ms`?

Faults: every public method calls `self._check_fault()` first (A21). The four
raisers surface `GrafanaApiError` with the production `code` values so the
fetcher branches exactly as it would against the real client (the fetcher keys
its rate-limit fallback on `code == "grafana_api_rate_limited"`).
"""
from __future__ import annotations

from typing import Any, NoReturn

from lib.shared.errors import GrafanaApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockGrafanaClient(_MockBase):
    """In-process replacement for `GrafanaClient`, driven by a `make_grafana`
    fixture.

    `fixture` shape (per `make_grafana`):
        {
          "base_url": "https://acme.grafana.net",
          "per_page": 100,
          # newest-first, like the real GET /api/annotations array.
          "annotations": [ <annotation dict with id/time/text/tags/...>, ... ],
          "alert_webhook": { ... },   # template only; unused here.
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
        self._per_page = int(fixture.get("per_page", 100)) or 100
        # Keep the fixture's newest-first order; the fetcher relies on it.
        self._annotations: list[dict[str, Any]] = list(
            fixture.get("annotations", [])
        )

    # ---------------------------------------------------------------
    # Public read surface (mirrors GrafanaClient)
    # ---------------------------------------------------------------
    async def list_annotations(
        self,
        *,
        from_ms: int | None = None,
        to_ms: int | None = None,
        limit: int = 100,
        type_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """`GET /api/annotations` — annotations in the [from_ms, to_ms] window,
        NEWEST-FIRST, capped at min(limit, per_page).

        `from_ms` / `to_ms` are epoch MILLISECONDS (inclusive bounds, either
        optional). Matches the real client's filtering + ordering so the
        fetcher's backward `to_ms` walk terminates correctly.
        """
        self._check_fault()

        cap = min(int(limit or self._per_page), self._per_page)
        window: list[dict[str, Any]] = []
        for ann in self._annotations:
            t = _ann_time_ms(ann)
            if t is None:
                continue
            if from_ms is not None and t < int(from_ms):
                continue
            if to_ms is not None and t > int(to_ms):
                continue
            window.append(ann)

        # Defensive: enforce newest-first even if the fixture is unsorted.
        window.sort(key=lambda a: _ann_time_ms(a) or 0, reverse=True)
        return window[:cap]

    async def has_annotations_since(self, *, from_ms: int) -> bool:
        """Reconciler gap probe: any annotation with `time` at/after `from_ms`
        (epoch ms)? The caller passes an EXCLUSIVE floor (high-water + 1 ms)."""
        rows = await self.list_annotations(from_ms=from_ms, limit=1)
        return len(rows) > 0

    async def get_org(self) -> dict[str, Any]:
        """`GET /api/org` — connectivity/credential probe used by the seed."""
        self._check_fault()
        return {"id": 1, "name": "Mock Grafana Org"}

    async def aclose(self) -> None:
        """No-op (mock holds no httpx client); present for surface parity."""
        return None

    # ---------------------------------------------------------------
    # Fault raisers (production GrafanaApiError codes — A21)
    # ---------------------------------------------------------------
    def _raise_rate_limit(self) -> NoReturn:
        raise GrafanaApiError(
            "MockGrafanaClient: rate limit (429), retry budget exhausted "
            "(X2 fault)",
            code="grafana_api_rate_limited",
            context={"http_status": 429},
        )

    def _raise_5xx(self) -> NoReturn:
        raise GrafanaApiError(
            "MockGrafanaClient: 503 (X2 fault)",
            code="grafana_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise GrafanaApiError(
            "MockGrafanaClient: 401 service-account token rejected (X2 fault)",
            code="grafana_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise GrafanaApiError(
            "MockGrafanaClient: transient transport error (X2 fault)",
            code="grafana_api_error",
            context={"error_type": "TransportError"},
        )


def _ann_time_ms(ann: dict[str, Any]) -> int | None:
    t = ann.get("time")
    if isinstance(t, bool):
        return None
    if isinstance(t, (int, float)):
        return int(t)
    return None


__all__ = ["MockGrafanaClient"]
