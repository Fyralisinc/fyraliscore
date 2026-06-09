"""MockTelegramClient — MTProto backfill surface used by IN-TELEGRAM.

Stateless in-process replacement for `TelegramClient`
(`services/ingest/integrations/telegram/client.py`). Implements the methods the
production fetcher (`fetchers/telegram.py`) and reconciler
(`reconcilers/telegram.py`) call against the `_open_telegram_client` seam:

  - get_history(dialog_id=..., access_hash=..., dialog_kind=..., offset_id=...,
                min_id=..., limit=...)
      -> (messages: list[dict], next_offset_id: int|None, is_last: bool)
  - has_history_since(dialog_id=..., access_hash=..., dialog_kind=..., min_id=...)
      -> bool
  - iter_dialogs(limit=...) -> list[dict]   (onboarding enumeration / parity)
  - me() -> dict                            (connectivity probe)

`get_history` mirrors the REAL `messages.getHistory` contract: it pages BACKWARD
from the newest message. `offset_id=0` starts at the newest; each page returns up
to `limit` messages OLDER than `offset_id` (and newer than the incremental
`min_id` floor), newest-first; the oldest id in the page is the next page's
`offset_id`. `is_last` is True once the page exhausts the matching messages.

The LIVE path does NOT use this mock — the synthetic gateway generator drives the
production `gateway/dispatch.handle_update` directly (Discord-gateway style).

Faults: every public method calls `self._check_fault()` first (A21). The four
raisers surface `TelegramApiError` with the production `code` values so the
fetcher branches exactly as it would against the real client (its flood-wait
fallback keys on `code == "telegram_api_flood_wait"`).
"""
from __future__ import annotations

from typing import Any, NoReturn

from lib.shared.errors import TelegramApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockTelegramClient(_MockBase):
    """In-process replacement for `TelegramClient`, driven by a `make_telegram`
    fixture (see `fixtures/telegram_generator.py`)."""

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
    # Public read surface (mirrors TelegramClient)
    # ---------------------------------------------------------------
    async def get_history(
        self,
        *,
        dialog_id: int,
        access_hash: int | None = None,
        dialog_kind: str = "chat",
        offset_id: int = 0,
        min_id: int = 0,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], int | None, bool]:
        """One backward page of `dialog_id`'s history. See module docstring."""
        self._check_fault()
        messages = self._messages_for(dialog_id)
        per_page = min(int(limit or self._page_size), self._page_size)

        # Candidates: older than offset_id (0 = newest), newer than min_id floor.
        candidates = [
            m for m in messages
            if (offset_id == 0 or int(m["id"]) < offset_id)
            and int(m["id"]) > (min_id or 0)
        ]
        # Newest-first, like messages.getHistory.
        candidates.sort(key=lambda m: int(m["id"]), reverse=True)
        page = candidates[:per_page]
        next_offset_id = int(page[-1]["id"]) if page else None
        is_last = len(candidates) <= per_page
        return page, next_offset_id, is_last

    async def has_history_since(
        self,
        *,
        dialog_id: int,
        access_hash: int | None = None,
        dialog_kind: str = "chat",
        min_id: int,
    ) -> bool:
        """Reconciler gap probe: any message with id > `min_id`?"""
        self._check_fault()
        return any(int(m["id"]) > (min_id or 0) for m in self._messages_for(dialog_id))

    async def iter_dialogs(self, *, limit: int = 200) -> list[dict[str, Any]]:
        """Enumerate dialogs for onboarding (surface parity)."""
        self._check_fault()
        out: list[dict[str, Any]] = []
        for did in self._fixture.get("dialog_order", []):
            d = self._fixture.get("dialogs", {}).get(str(did))
            if d is None:
                continue
            out.append({
                "dialog_id": d["dialog_id"],
                "dialog_kind": d.get("dialog_kind", "chat"),
                "access_hash": d.get("access_hash"),
                "title": d.get("title"),
            })
        return out[:limit]

    async def me(self) -> dict[str, Any]:
        """Connectivity/credential probe."""
        self._check_fault()
        return {"id": 1, "username": "mock_bot", "phone": None}

    async def aclose(self) -> None:
        """No-op (mock holds no connection); present for surface parity."""
        return None

    # ---------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------
    def _messages_for(self, dialog_id: int) -> list[dict[str, Any]]:
        d = self._fixture.get("dialogs", {}).get(str(dialog_id))
        if d is None:
            return []
        return list(d.get("messages", []))

    # ---------------------------------------------------------------
    # Fault raisers (production TelegramApiError codes — A21)
    # ---------------------------------------------------------------
    def _raise_rate_limit(self) -> NoReturn:
        raise TelegramApiError(
            "MockTelegramClient: FLOOD_WAIT (X2 fault)",
            code="telegram_api_flood_wait",
            context={"retry_after": 5, "method": "get_history"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise TelegramApiError(
            "MockTelegramClient: internal RPC error (X2 fault)",
            code="telegram_api_error",
            context={"method": "get_history"},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise TelegramApiError(
            "MockTelegramClient: AUTH_KEY revoked (X2 fault)",
            code="telegram_api_unauthorized",
            context={"method": "get_history"},
        )

    def _raise_transient(self) -> NoReturn:
        raise TelegramApiError(
            "MockTelegramClient: transient transport error (X2 fault)",
            code="telegram_api_error",
            context={"method": "get_history", "error_type": "TransportError"},
        )


__all__ = ["MockTelegramClient"]
