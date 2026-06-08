"""MockSignalClient — Signal backfill surface used by IN-SIGNAL.

Stateless in-process replacement for `SignalClient`
(`services/ingest/integrations/signal/client.py`). Implements the methods the
production fetcher (`fetchers/signal.py`) and reconciler (`reconcilers/signal.py`)
call against the `_open_signal_client` seam:

  - get_history(thread_id=..., thread_kind=..., offset_id=..., min_id=...,
                limit=...)
      -> (messages: list[dict], next_offset_id: int|None, is_last: bool)
  - has_history_since(thread_id=..., thread_kind=..., min_id=...)
      -> bool
  - iter_threads(limit=...) -> list[dict]    (onboarding enumeration / parity)
  - me() -> dict                             (connectivity probe)

`get_history` mirrors the REAL thread-history contract (the Telegram archetype):
it pages BACKWARD from the newest message. `offset_id=0` starts at the newest;
each page returns up to `limit` messages OLDER than `offset_id` (and newer than
the incremental `min_id` floor), newest-first; the oldest id in the page is the
next page's `offset_id`. `is_last` is True once the page exhausts the matching
messages.

The LIVE path does NOT use this mock — the synthetic gateway generator drives the
production `gateway/dispatch.handle_update` directly (Telegram-gateway style).

Faults: every public method calls `self._check_fault()` first (A21). The four
raisers surface `SignalApiError` with the production `code` values so the fetcher
branches exactly as it would against the real client (its rate-limit fallback
keys on `code == "signal_api_rate_limited"`).
"""
from __future__ import annotations

from typing import Any, NoReturn

from lib.shared.errors import SignalApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockSignalClient(_MockBase):
    """In-process replacement for `SignalClient`, driven by a `make_signal`
    fixture (see `fixtures/signal_generator.py`)."""

    def __init__(
        self,
        *,
        fixture: dict[str, Any],
        profile: FaultProfile = HAPPY_PATH,
    ) -> None:
        super().__init__(profile=profile)
        self._fixture = fixture
        self._page_size = int(fixture.get("page_size", 100)) or 100

    # ---------------------------------------------------------------
    # Public read surface (mirrors SignalClient)
    # ---------------------------------------------------------------
    async def get_history(
        self,
        *,
        thread_id: int,
        thread_kind: str = "direct",
        offset_id: int = 0,
        min_id: int = 0,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], int | None, bool]:
        """One backward page of `thread_id`'s history. See module docstring."""
        self._check_fault()
        messages = self._messages_for(thread_id)
        per_page = min(int(limit or self._page_size), self._page_size)

        # Candidates: older than offset_id (0 = newest), newer than min_id floor.
        candidates = [
            m for m in messages
            if (offset_id == 0 or int(m["id"]) < offset_id)
            and int(m["id"]) > (min_id or 0)
        ]
        # Newest-first, like thread history.
        candidates.sort(key=lambda m: int(m["id"]), reverse=True)
        page = candidates[:per_page]
        next_offset_id = int(page[-1]["id"]) if page else None
        is_last = len(candidates) <= per_page
        return page, next_offset_id, is_last

    async def has_history_since(
        self,
        *,
        thread_id: int,
        thread_kind: str = "direct",
        min_id: int,
    ) -> bool:
        """Reconciler gap probe: any message with id > `min_id`?"""
        self._check_fault()
        return any(int(m["id"]) > (min_id or 0) for m in self._messages_for(thread_id))

    async def iter_threads(self, *, limit: int = 200) -> list[dict[str, Any]]:
        """Enumerate threads for onboarding (surface parity)."""
        self._check_fault()
        out: list[dict[str, Any]] = []
        for tid in self._fixture.get("thread_order", []):
            t = self._fixture.get("threads", {}).get(str(tid))
            if t is None:
                continue
            out.append({
                "thread_id": t["thread_id"],
                "thread_kind": t.get("thread_kind", "direct"),
                "title": t.get("title"),
            })
        return out[:limit]

    async def me(self) -> dict[str, Any]:
        """Connectivity/credential probe."""
        self._check_fault()
        return {"id": 1, "username": "mock_signal", "number": None}

    async def aclose(self) -> None:
        """No-op (mock holds no connection); present for surface parity."""
        return None

    # ---------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------
    def _messages_for(self, thread_id: int) -> list[dict[str, Any]]:
        t = self._fixture.get("threads", {}).get(str(thread_id))
        if t is None:
            return []
        return list(t.get("messages", []))

    # ---------------------------------------------------------------
    # Fault raisers (production SignalApiError codes — A21)
    # ---------------------------------------------------------------
    def _raise_rate_limit(self) -> NoReturn:
        raise SignalApiError(
            "MockSignalClient: rate limited (X2 fault)",
            code="signal_api_rate_limited",
            context={"retry_after": 5, "method": "get_history"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise SignalApiError(
            "MockSignalClient: internal error (X2 fault)",
            code="signal_api_error",
            context={"method": "get_history"},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise SignalApiError(
            "MockSignalClient: linked device unlinked (X2 fault)",
            code="signal_api_unauthorized",
            context={"method": "get_history"},
        )

    def _raise_transient(self) -> NoReturn:
        raise SignalApiError(
            "MockSignalClient: transient transport error (X2 fault)",
            code="signal_api_error",
            context={"method": "get_history", "error_type": "TransportError"},
        )


__all__ = ["MockSignalClient"]
