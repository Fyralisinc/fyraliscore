"""services/ingest/integrations/signal/gateway/worker.py — persistent receive worker.

Holds ONE install's LIVE Signal linked-device session (the Telegram-gateway
analog) and drives `dispatch.handle_update` for every incoming message. The real
Signal transport (signal-cli daemon / libsignal) is an OPTIONAL dependency,
imported lazily in `run_forever` — importing this module never requires it (the
synthetic harness drives `handle_update` directly and never instantiates the
worker).

Gap recovery: on startup the worker loads the persisted sync cursor from
`signal_update_state` and requests a sync replay so any message missed while the
session was down — including during a long backfill sweep on the separate
backfill linked device — is reconciled. It then runs the live receive loop and
periodically persists the advancing cursor.

Single-instance safety: a Signal linked device should be driven by only one live
receive loop at a time, so the launcher acquires the `gateway:signal:leader_lock`
Redis lease BEFORE constructing the worker (mirrors Telegram / Discord).

NOTE: the real Signal receive/sync transport is NOT verified — the lazy import
and the receive-loop body are TODO(human) shells. The synthetic path never
instantiates this worker.
"""
from __future__ import annotations

from typing import Any

import structlog

from services.ingest.integrations.signal.client import _message_to_dict
from services.ingest.integrations.signal.gateway.dispatch import (
    DispatchDeps,
    handle_update,
)


log = structlog.get_logger("integrations.signal.gateway.worker")


class SignalGatewayWorker:
    """One live Signal linked-device session for a single install/tenant."""

    def __init__(
        self,
        *,
        deps: DispatchDeps,
        session: str,
        account_label: str,
        thread_index: dict[int, dict[str, Any]],
        save_state: Any = None,  # async callable(cursor) | None
    ) -> None:
        self._deps = deps
        self._session = session
        self._account_label = account_label
        # thread_id -> {"thread_kind": str, "title": str|None}
        self._thread_index = thread_index
        self._save_state = save_state
        self._client: Any | None = None

    def _thread_context(self, peer_id: int) -> dict[str, Any]:
        meta = self._thread_index.get(peer_id) or {}
        return {
            "thread_kind": meta.get("thread_kind") or "direct",
            "thread_title": meta.get("title"),
        }

    async def run_forever(self) -> None:
        """Connect, recover gaps, and run the live receive loop until disconnect.

        TODO(human): wire the real Signal linked-device transport. The synthetic
        harness drives `handle_update` directly and never reaches this method, so
        it stays a TODO shell.
        """
        raise RuntimeError(
            "signal gateway worker transport is not configured "
            "(real signal-cli/libsignal receive loop is a TODO)"
        )

    async def _on_new_message(self, event: Any) -> None:
        """Receive-callback shape mirroring Telegram's NewMessage handler. The
        real transport invokes this per incoming message once wired."""
        try:
            peer_id = int(getattr(event, "thread_id", 0) or 0)
            ctx = self._thread_context(peer_id)
            update = {
                "event": "new_message",
                "message": _message_to_dict(getattr(event, "message", event)),
                "thread_id": peer_id,
                "thread_kind": ctx["thread_kind"],
                "thread_title": ctx["thread_title"],
            }
            await handle_update(update, self._deps)
            await self._persist_state(self._client)
        except Exception:  # noqa: BLE001
            log.exception("signal_gateway.update_handler_failed")

    async def _persist_state(self, client: Any) -> None:
        if self._save_state is None or client is None:
            return
        try:
            # TODO(human): read the real advancing sync cursor from the transport.
            cursor = getattr(client, "sync_cursor", None)
            await self._save_state(cursor)
        except Exception:  # noqa: BLE001
            log.debug("signal_gateway.persist_state_failed")

    async def aclose(self) -> None:
        if self._client is not None:
            try:
                await self._client.disconnect()
            finally:
                self._client = None


def build_thread_index(rows: list[Any]) -> dict[int, dict[str, Any]]:
    """signal_threads rows -> {thread_id: {thread_kind, title}} for the worker."""
    index: dict[int, dict[str, Any]] = {}
    for r in rows:
        tid = r["thread_id"]
        if isinstance(tid, int):
            index[tid] = {"thread_kind": r["thread_kind"], "title": r["title"]}
    return index


__all__ = ["SignalGatewayWorker", "build_thread_index"]
