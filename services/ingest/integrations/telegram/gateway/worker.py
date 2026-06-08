"""services/ingest/integrations/telegram/gateway/worker.py — persistent updates worker.

Holds ONE install's LIVE MTProto connection (the Discord-gateway analog) and
drives `dispatch.handle_update` for every incoming message update. Telethon is an
OPTIONAL dependency, imported lazily in `run_forever` — importing this module
never requires it (the synthetic harness drives `handle_update` directly and
never instantiates the worker).

Gap recovery: on startup the worker loads the persisted `pts/qts/seq/date` from
`telegram_update_state` and calls Telethon's `catch_up()` (which issues
`updates.getDifference` / `updates.getChannelDifference`) so any update missed
while the connection was down — including during a long backfill sweep on the
separate backfill session — is reconciled. It then runs the live event loop and
periodically persists the advancing state.

Single-instance safety: a Telegram authorization may be driven by only one live
connection at a time, so the launcher acquires the `gateway:telegram:leader_lock`
Redis lease BEFORE constructing the worker (mirrors Discord).
"""
from __future__ import annotations

from typing import Any

import structlog

from services.ingest.integrations.telegram.client import _message_to_dict
from services.ingest.integrations.telegram.gateway.dispatch import (
    DispatchDeps,
    handle_update,
)


log = structlog.get_logger("integrations.telegram.gateway.worker")


class TelegramGatewayWorker:
    """One live MTProto updates connection for a single install/tenant."""

    def __init__(
        self,
        *,
        deps: DispatchDeps,
        session: str,
        api_id: int,
        api_hash: str,
        dialog_index: dict[int, dict[str, Any]],
        save_state: Any = None,  # async callable(pts, qts, seq, date) | None
    ) -> None:
        self._deps = deps
        self._session = session
        self._api_id = api_id
        self._api_hash = api_hash
        # dialog_id -> {"dialog_kind": str, "title": str|None}
        self._dialog_index = dialog_index
        self._save_state = save_state
        self._client: Any | None = None

    def _dialog_context(self, peer_id: int) -> dict[str, Any]:
        meta = self._dialog_index.get(peer_id) or {}
        return {
            "dialog_kind": meta.get("dialog_kind") or "chat",
            "dialog_title": meta.get("title"),
        }

    async def run_forever(self) -> None:
        """Connect, recover gaps, and run the live event loop until disconnect."""
        try:
            from telethon import TelegramClient as _TLClient, events  # type: ignore
            from telethon.sessions import StringSession  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional dep
            raise RuntimeError(
                "telegram gateway worker requires telethon "
                "(pip install 'fyraliscore[telegram]')"
            ) from exc

        client = _TLClient(StringSession(self._session), self._api_id, self._api_hash)
        self._client = client

        @client.on(events.NewMessage)
        async def _on_new_message(event: Any) -> None:  # noqa: ANN401
            try:
                peer_id = int(getattr(event, "chat_id", 0) or 0)
                ctx = self._dialog_context(peer_id)
                update = {
                    "event": "new_message",
                    "message": _message_to_dict(event.message),
                    "dialog_id": peer_id,
                    "dialog_kind": ctx["dialog_kind"],
                    "dialog_title": ctx["dialog_title"],
                }
                await handle_update(update, self._deps)
                await self._persist_state(client)
            except Exception:  # noqa: BLE001
                log.exception("telegram_gateway.update_handler_failed")

        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError("telegram live session is not authorized (revoked?)")

        log.info("telegram_gateway.connected", tenant_id=str(self._deps.tenant_id))
        # Gap recovery: reconcile updates missed while disconnected / during the
        # backfill sweep (updates.getDifference under the hood).
        try:
            await client.catch_up()
        except Exception:  # noqa: BLE001
            log.warning("telegram_gateway.catch_up_failed")

        await client.run_until_disconnected()

    async def _persist_state(self, client: Any) -> None:
        if self._save_state is None:
            return
        try:
            state = await client.get_state()  # telethon updates.State
            await self._save_state(
                getattr(state, "pts", None),
                getattr(state, "qts", None),
                getattr(state, "seq", None),
                getattr(state, "date", None),
            )
        except Exception:  # noqa: BLE001
            log.debug("telegram_gateway.persist_state_failed")

    async def aclose(self) -> None:
        if self._client is not None:
            try:
                await self._client.disconnect()
            finally:
                self._client = None


def build_dialog_index(rows: list[Any]) -> dict[int, dict[str, Any]]:
    """telegram_dialogs rows -> {dialog_id: {dialog_kind, title}} for the worker."""
    index: dict[int, dict[str, Any]] = {}
    for r in rows:
        did = r["dialog_id"]
        if isinstance(did, int):
            index[did] = {"dialog_kind": r["dialog_kind"], "title": r["title"]}
    return index


__all__ = ["TelegramGatewayWorker", "build_dialog_index"]
