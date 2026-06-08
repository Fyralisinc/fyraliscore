"""MockAwsClient — AWS CloudTrail API surface used by IN-AWS backfill/poll.

Stateless in-process replacement for `AwsClient`
(`services/ingest/integrations/aws/client.py`). Implements the methods the
production fetcher (`fetchers/aws.py`) and reconciler (`reconcilers/aws.py`) call
against the `_open_aws_client` seam:

  - list_events(account_id, region, from_ms, to_ms, cursor, limit) -> dict
      Returns `{"events": [...], "next_cursor": str | None}` — the event objects
      whose `eventTime` (epoch ms) is within the [from_ms, to_ms] window,
      NEWEST-FIRST, paged by an opaque `"off:<n>"` token capped at
      min(limit, per_page). Mirrors the real `LookupEvents` contract: newest
      first, `StartTime`/`EndTime` filtering, an opaque `NextToken` continuation.
      The fetcher threads the token back verbatim and stops when `next_cursor is
      None` (end-of-data).
  - has_events_since(account_id, region, from_ms) -> bool
      Reconciler gap probe: is there >=1 event at/after `from_ms`?

Faults: every public method calls `self._check_fault()` first (A21). The four
raisers surface `AwsApiError` with the production `code` values so the fetcher
branches exactly as it would against the real client (the fetcher keys its
throttle fallback on `code == "aws_api_throttled"`).
"""
from __future__ import annotations

from typing import Any, NoReturn

from services.ingest.integrations.aws.client import AwsApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockAwsClient(_MockBase):
    """In-process replacement for `AwsClient`, driven by a `make_aws` fixture.

    `fixture` shape (per `make_aws`):
        {
          "account_id": "123456789012",
          "region": "us-east-1",
          "per_page": 50,
          # newest-first, like the real LookupEvents array.
          "events": [ <event dict with eventId/eventTime/...>, ... ],
          "poll_event": { ... },   # template only; unused here.
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
        self._per_page = int(fixture.get("per_page", 50)) or 50
        # Keep the fixture's newest-first order; the fetcher relies on it.
        self._events: list[dict[str, Any]] = list(fixture.get("events", []))

    # ---------------------------------------------------------------
    # Public read surface (mirrors AwsClient)
    # ---------------------------------------------------------------
    async def list_events(
        self,
        *,
        account_id: str,
        region: str,
        from_ms: int | None = None,
        to_ms: int | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        """`CloudTrail:LookupEvents` — events in the [from_ms, to_ms] window,
        NEWEST-FIRST, paged by an opaque `"off:<n>"` token capped at
        min(limit, per_page).

        `from_ms` / `to_ms` are epoch MILLISECONDS (inclusive bounds, either
        optional). `cursor` is the opaque continuation token returned by the
        prior call. Matches the real client's filtering + ordering + token
        contract so the fetcher's window walk terminates correctly.
        """
        self._check_fault()

        window: list[dict[str, Any]] = []
        for event in self._events:
            t = _event_time_ms(event)
            if t is None:
                continue
            if from_ms is not None and t < int(from_ms):
                continue
            if to_ms is not None and t > int(to_ms):
                continue
            window.append(event)

        # Defensive: enforce newest-first even if the fixture is unsorted.
        window.sort(key=lambda e: _event_time_ms(e) or 0, reverse=True)

        start = _decode_token(cursor)
        per_page = min(int(limit or self._per_page), self._per_page)
        end = start + per_page
        page = window[start:end]

        is_last = end >= len(window)
        next_cursor = None if is_last else _encode_token(end)
        return {"events": page, "next_cursor": next_cursor}

    async def has_events_since(
        self, *, account_id: str, region: str, from_ms: int,
    ) -> bool:
        """Reconciler gap probe: any event with `eventTime` at/after `from_ms`
        (epoch ms)? The caller passes an EXCLUSIVE floor (high-water + 1 ms)."""
        page = await self.list_events(
            account_id=account_id, region=region, from_ms=from_ms, limit=1,
        )
        return len(page.get("events") or []) > 0

    async def describe_account(self) -> dict[str, Any]:
        """`STS:GetCallerIdentity`-style connectivity/credential probe."""
        self._check_fault()
        return {
            "Account": str(self._fixture.get("account_id", "000000000000")),
            "Arn": f"arn:aws:sts::{self._fixture.get('account_id', '000000000000')}:assumed-role/mock",
        }

    async def aclose(self) -> None:
        """No-op (mock holds no httpx client); present for surface parity."""
        return None

    # ---------------------------------------------------------------
    # Fault raisers (production AwsApiError codes — A21)
    # ---------------------------------------------------------------
    def _raise_rate_limit(self) -> NoReturn:
        raise AwsApiError(
            "MockAwsClient: throttled (RequestLimitExceeded), retry budget "
            "exhausted (X2 fault)",
            code="aws_api_throttled",
            context={"http_status": 400},
        )

    def _raise_5xx(self) -> NoReturn:
        raise AwsApiError(
            "MockAwsClient: 503 (X2 fault)",
            code="aws_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise AwsApiError(
            "MockAwsClient: 403 AccessDenied / signature mismatch (X2 fault)",
            code="aws_api_unauthorized",
            context={"http_status": 403},
        )

    def _raise_transient(self) -> NoReturn:
        raise AwsApiError(
            "MockAwsClient: transient transport error (X2 fault)",
            code="aws_api_error",
            context={"error_type": "TransportError"},
        )


def _event_time_ms(event: dict[str, Any]) -> int | None:
    t = event.get("eventTime")
    if isinstance(t, bool):
        return None
    if isinstance(t, (int, float)):
        return int(t)
    return None


def _decode_token(token: str | None) -> int:
    """Opaque `"off:<n>"` continuation token -> start offset (0 when absent)."""
    if not token:
        return 0
    if token.startswith("off:"):
        try:
            return max(0, int(token[4:]))
        except ValueError:
            return 0
    return 0


def _encode_token(offset: int) -> str:
    return f"off:{int(offset)}"


__all__ = ["MockAwsClient"]
