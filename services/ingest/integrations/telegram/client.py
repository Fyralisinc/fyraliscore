"""services/ingest/integrations/telegram/client.py — outbound MTProto client.

A thin async wrapper over **Telethon** (the pure-Python MTProto user-account
client — ADR-0003). It is the single outbound surface for:

  - history BACKFILL  (`get_history` → messages.getHistory, paged on offset_id),
  - dialog ENUMERATION at onboarding (`iter_dialogs`),
  - the reconciler GAP PROBE (`has_history_since`),
  - a connectivity/credential probe (`me`).

The durable credential is a persisted Telethon `StringSession` (the auth_key),
resolved once from the secret store via `session_secret_ref` (or preset for the
local spammer / tests). `api_id`/`api_hash` are the MTProto application
credentials. Telethon is an OPTIONAL dependency: the import is deferred to
`connect()` so importing this module (and the whole ingestion package) never
requires Telethon — the synthetic harness monkeypatches the `_open_telegram_client`
seam and never connects.

All read methods return RAW message dicts (Telethon `Message` → dict via
`_message_to_dict`) of the shape `integrations/telegram/records.build_message_record`
consumes, so the backfill fetcher and the live worker share one record contract.

FLOOD_WAIT (RPC error 420) surfaces as `TelegramApiError(telegram_api_flood_wait)`
carrying the server-returned `seconds` on `context["retry_after"]` — the canonical
Telegram backoff (the caller waits the server's value; it is NOT client-chosen).
"""
from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import structlog

from lib.shared.errors import TelegramApiError


log = structlog.get_logger("integrations.telegram.client")

_DEFAULT_PAGE_SIZE = 100  # messages.getHistory caps the limit at 100.


def _message_to_dict(msg: Any) -> dict[str, Any]:
    """Telethon `Message` → the raw dict `records.build_message_record` expects.

    Telethon exposes `.id`, `.date` (aware datetime), `.edit_date`, `.message`
    (text), `.out`, and `.from_id` (a Peer). We flatten `from_id` to
    `{"user_id": int}` (the only sender shape we resolve in v1)."""
    from_id = getattr(msg, "from_id", None)
    sender: dict[str, Any] | None = None
    user_id = getattr(from_id, "user_id", None)
    if isinstance(user_id, int):
        sender = {"user_id": user_id}
    date = getattr(msg, "date", None)
    edit_date = getattr(msg, "edit_date", None)
    return {
        "id": int(getattr(msg, "id", 0) or 0),
        "date": int(date.timestamp()) if date is not None else None,
        "edit_date": int(edit_date.timestamp()) if edit_date is not None else None,
        "message": getattr(msg, "message", None) or "",
        "out": bool(getattr(msg, "out", False)),
        "from_id": sender,
    }


class TelegramClient:
    """Outbound MTProto client for one install's BACKFILL session.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_telegram_client`.
    Holds a persistent Telethon connection for the life of the shard sweep; the
    process-wide reuse is handled by the opener (`_noop` close) like the other
    sources.
    """

    def __init__(
        self,
        *,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        api_id: str | None = None,
        api_hash_secret_ref: str | None = None,
        session_secret_ref: str | None = None,
        session: str | None = None,
        api_hash: str | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._api_id = api_id
        self._api_hash_secret_ref = api_hash_secret_ref
        self._session_secret_ref = session_secret_ref
        # Preset session / api_hash (spammer/test); else resolved from secrets.
        self._session: str | None = session
        self._api_hash: str | None = api_hash
        self._client: Any | None = None
        self._connect_lock = asyncio.Lock()

    async def _resolve_secret(self, ref: str | None) -> str | None:
        if ref is None or self._secret_store is None or self._tenant_id is None:
            return None
        raw = await self._secret_store.get(ref, tenant_id=self._tenant_id)
        return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)

    async def _connect(self) -> Any:
        if self._client is not None:
            return self._client
        async with self._connect_lock:
            if self._client is not None:
                return self._client
            try:
                from telethon import TelegramClient as _TLClient  # type: ignore
                from telethon.sessions import StringSession  # type: ignore
            except ImportError as exc:  # pragma: no cover - optional dep
                raise TelegramApiError(
                    "telegram integration requires telethon "
                    "(pip install 'fyraliscore[telegram]')",
                    code="telegram_api_error",
                ) from exc

            session = self._session or await self._resolve_secret(
                self._session_secret_ref,
            )
            api_hash = self._api_hash or await self._resolve_secret(
                self._api_hash_secret_ref,
            )
            if not (session and self._api_id and api_hash):
                raise TelegramApiError(
                    "telegram client missing session / api_id / api_hash",
                    code="telegram_api_unauthorized",
                )
            client = _TLClient(StringSession(session), int(self._api_id), api_hash)
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                raise TelegramApiError(
                    "telegram session is not authorized (revoked?)",
                    code="telegram_api_unauthorized",
                )
            self._client = client
            return client

    def _input_peer(self, *, dialog_id: int, access_hash: int | None, dialog_kind: str) -> Any:
        from telethon.tl import types as _t  # type: ignore
        if dialog_kind == "channel":
            return _t.InputPeerChannel(channel_id=dialog_id, access_hash=access_hash or 0)
        if dialog_kind == "user":
            return _t.InputPeerUser(user_id=dialog_id, access_hash=access_hash or 0)
        return _t.InputPeerChat(chat_id=dialog_id)

    async def _guard_flood(self, coro_factory: Any, *, method: str) -> Any:
        """Run a Telethon coroutine, mapping FloodWaitError → TelegramApiError
        with the server-returned wait on context (the caller decides to sleep)."""
        try:
            from telethon.errors import FloodWaitError  # type: ignore
        except ImportError:  # pragma: no cover
            FloodWaitError = ()  # type: ignore
        try:
            return await coro_factory()
        except FloodWaitError as exc:  # type: ignore[misc]
            raise TelegramApiError(
                "telegram FLOOD_WAIT",
                code="telegram_api_flood_wait",
                context={"retry_after": getattr(exc, "seconds", None), "method": method},
            ) from exc

    async def get_history(
        self,
        *,
        dialog_id: int,
        access_hash: int | None,
        dialog_kind: str,
        offset_id: int = 0,
        min_id: int = 0,
        limit: int = _DEFAULT_PAGE_SIZE,
    ) -> tuple[list[dict[str, Any]], int | None, bool]:
        """One page of history older than `offset_id` (0 = newest), bounded below
        by `min_id` (0 = no floor; >0 = incremental re-walk above a high-water).

        Returns `(messages, next_offset_id, is_last)`. `next_offset_id` is the
        MIN message id in this page (the cursor for the next, older page);
        `is_last` is True when the page came back short (no older history).
        """
        client = await self._connect()
        peer = self._input_peer(
            dialog_id=dialog_id, access_hash=access_hash, dialog_kind=dialog_kind,
        )
        limit = min(_DEFAULT_PAGE_SIZE, max(1, limit))
        result = await self._guard_flood(
            lambda: client.get_messages(
                peer, limit=limit, offset_id=offset_id or 0, min_id=min_id or 0,
            ),
            method="get_history",
        )
        messages = [_message_to_dict(m) for m in result]
        ids = [m["id"] for m in messages if isinstance(m.get("id"), int) and m["id"] > 0]
        next_offset_id = min(ids) if ids else None
        is_last = len(messages) < limit or next_offset_id is None
        return messages, next_offset_id, is_last

    async def iter_dialogs(self, *, limit: int = 200) -> list[dict[str, Any]]:
        """Enumerate the account's dialogs for onboarding shard planning.

        Returns `[{dialog_id, dialog_kind, access_hash, title}]`."""
        client = await self._connect()
        out: list[dict[str, Any]] = []
        dialogs = await self._guard_flood(
            lambda: client.get_dialogs(limit=limit), method="iter_dialogs",
        )
        for d in dialogs:
            entity = getattr(d, "entity", None)
            ent_id = getattr(entity, "id", None)
            if not isinstance(ent_id, int):
                continue
            kind = "channel" if getattr(d, "is_channel", False) else (
                "user" if getattr(d, "is_user", False) else "chat"
            )
            out.append({
                "dialog_id": ent_id,
                "dialog_kind": kind,
                "access_hash": getattr(entity, "access_hash", None),
                "title": getattr(d, "name", None) or getattr(d, "title", None),
            })
        return out

    async def has_history_since(
        self, *, dialog_id: int, access_hash: int | None, dialog_kind: str,
        min_id: int,
    ) -> bool:
        """Reconciler gap probe: is there any message with id > `min_id`?

        `get_messages(..., min_id=min_id, limit=1)` returns the newest message
        above the high-water; non-empty ⇒ a gap to re-walk."""
        client = await self._connect()
        peer = self._input_peer(
            dialog_id=dialog_id, access_hash=access_hash, dialog_kind=dialog_kind,
        )
        result = await self._guard_flood(
            lambda: client.get_messages(peer, limit=1, min_id=min_id),
            method="has_history_since",
        )
        return len(result) > 0

    async def me(self) -> dict[str, Any]:
        """Cheap connectivity + auth probe (the authenticated account)."""
        client = await self._connect()
        who = await client.get_me()
        return {
            "id": getattr(who, "id", None),
            "username": getattr(who, "username", None),
            "phone": getattr(who, "phone", None),
        }

    async def aclose(self) -> None:
        if self._client is not None:
            try:
                await self._client.disconnect()
            finally:
                self._client = None


__all__ = ["TelegramClient"]
