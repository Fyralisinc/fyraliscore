"""services/ingest/integrations/signal/client.py — outbound Signal client.

A thin async wrapper over the **signal-cli / libsignal** linked-device surface
(the real Signal client for a non-phone integration — ADR-0003 archetype). It is
the single outbound surface for:

  - history BACKFILL  (`get_history` → paged thread message history),
  - thread ENUMERATION at onboarding (`iter_threads`),
  - the reconciler GAP PROBE (`has_history_since`),
  - a connectivity/credential probe (`me`).

The durable credential is a persisted LINKED-DEVICE registration (the libsignal
identity/session store), resolved once from the secret store via
`session_secret_ref` (or preset for the local spammer / tests). Signal's
underlying transport is OPTIONAL: the import is deferred to `connect()` so
importing this module (and the whole ingestion package) never requires it — the
synthetic harness monkeypatches the `_open_signal_client` seam and never
connects.

All read methods return RAW message dicts of the shape
`integrations/signal/records.build_message_record` consumes, so the backfill
fetcher and the live worker share one record contract.

NOTE: the real Signal client surface (signal-cli JSON-RPC vs. libsignal direct,
the exact pagination cursor, the linked-device auth handshake, rate-limit
semantics) is NOT verified against vendor docs. The concrete transport calls are
left as TODO(human) markers; the synthetic gate drives MockSignalClient, so this
stub only needs the method signatures to be correct.
"""
from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import structlog

from lib.shared.errors import SignalApiError


log = structlog.get_logger("integrations.signal.client")

_DEFAULT_PAGE_SIZE = 100  # TODO(human): confirm Signal history page cap vs. vendor docs.

# TODO(human): confirm the real Signal client transport (signal-cli JSON-RPC
# daemon vs. an in-process libsignal binding) and the import path below.
_SIGNAL_TRANSPORT_IMPORT = "signalcli"  # placeholder module name; not verified.


def _message_to_dict(msg: Any) -> dict[str, Any]:
    """Real Signal message → the raw dict `records.build_message_record` expects.

    Mirrors the Telegram client's flattening: a stable integer message id, an
    epoch-second `date`, an optional `edit_date` (edits unsupported in v1 → None),
    the text body, an `out` flag for our own sends, and `from_id={"user_id": int}`
    for the sender (the only sender shape resolved in v1).

    TODO(human): map the real signal-cli/libsignal envelope fields
    (timestamp, sourceUuid, dataMessage.body, …) once the transport is confirmed.
    """
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


class SignalClient:
    """Outbound Signal client for one install's BACKFILL session.

    Built by `services/ingest/ingestion/fetchers/_clients.py::build_signal_client`
    (added by the wiring phase). Holds a persistent linked-device connection for
    the life of the shard sweep; the process-wide reuse is handled by the opener
    (`_noop` close) like the other sources.
    """

    def __init__(
        self,
        *,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        account_label: str | None = None,
        session_secret_ref: str | None = None,
        session: str | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._account_label = account_label
        self._session_secret_ref = session_secret_ref
        # Preset session (spammer/test); else resolved from secrets.
        self._session: str | None = session
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
            session = self._session or await self._resolve_secret(
                self._session_secret_ref,
            )
            if not session:
                raise SignalApiError(
                    "signal client missing linked-device session",
                    code="signal_api_unauthorized",
                )
            # TODO(human): instantiate + connect the real Signal transport here
            # (signal-cli JSON-RPC daemon attach, or libsignal store load), and
            # verify the linked device is still authorized (not unlinked). The
            # synthetic path never reaches this method (it patches the opener
            # seam), so this stays a TODO shell.
            raise SignalApiError(
                "signal integration transport is not configured "
                "(real signal-cli/libsignal wiring is a TODO)",
                code="signal_api_error",
            )

    async def _guard_rate_limit(self, coro_factory: Any, *, method: str) -> Any:
        """Run a Signal coroutine, mapping a server rate-limit to SignalApiError
        with the server-returned wait on context (the caller decides to sleep).

        TODO(human): map the real Signal rate-limit signal (HTTP 429 from the
        service, or signal-cli's RateLimitException) → signal_api_rate_limited
        with context['retry_after'] from the server's value.
        """
        return await coro_factory()

    async def get_history(
        self,
        *,
        thread_id: int,
        thread_kind: str,
        offset_id: int = 0,
        min_id: int = 0,
        limit: int = _DEFAULT_PAGE_SIZE,
    ) -> tuple[list[dict[str, Any]], int | None, bool]:
        """One page of history older than `offset_id` (0 = newest), bounded below
        by `min_id` (0 = no floor; >0 = incremental re-walk above a high-water).

        Returns `(messages, next_offset_id, is_last)`. `next_offset_id` is the
        MIN message id in this page (the cursor for the next, older page);
        `is_last` is True when the page came back short (no older history).

        TODO(human): wire to the real thread-history call once the transport is
        confirmed (signal-cli has no historical fetch for non-linked threads; a
        linked device replays from the message store / sync).
        """
        client = await self._connect()  # raises until transport is wired
        limit = min(_DEFAULT_PAGE_SIZE, max(1, limit))
        result = await self._guard_rate_limit(
            lambda: client.get_messages(  # TODO(human): real call signature
                thread_id, limit=limit, offset_id=offset_id or 0, min_id=min_id or 0,
            ),
            method="get_history",
        )
        messages = [_message_to_dict(m) for m in result]
        ids = [m["id"] for m in messages if isinstance(m.get("id"), int) and m["id"] > 0]
        next_offset_id = min(ids) if ids else None
        is_last = len(messages) < limit or next_offset_id is None
        return messages, next_offset_id, is_last

    async def iter_threads(self, *, limit: int = 200) -> list[dict[str, Any]]:
        """Enumerate the linked account's threads for onboarding shard planning.

        Returns `[{thread_id, thread_kind, title}]`.

        TODO(human): wire to the real thread/group listing once confirmed.
        """
        client = await self._connect()
        out: list[dict[str, Any]] = []
        threads = await self._guard_rate_limit(
            lambda: client.list_threads(limit=limit), method="iter_threads",
        )
        for t in threads:
            tid = getattr(t, "thread_id", None)
            if not isinstance(tid, int):
                continue
            out.append({
                "thread_id": tid,
                "thread_kind": getattr(t, "thread_kind", None) or "direct",
                "title": getattr(t, "title", None) or getattr(t, "name", None),
            })
        return out

    async def has_history_since(
        self, *, thread_id: int, thread_kind: str, min_id: int,
    ) -> bool:
        """Reconciler gap probe: is there any message with id > `min_id`?

        TODO(human): wire to a cheap 1-row newest-above-high-water call.
        """
        client = await self._connect()
        result = await self._guard_rate_limit(
            lambda: client.get_messages(thread_id, limit=1, min_id=min_id),
            method="has_history_since",
        )
        return len(result) > 0

    async def me(self) -> dict[str, Any]:
        """Cheap connectivity + auth probe (the linked account).

        TODO(human): return the real linked-account identity (number / uuid).
        """
        client = await self._connect()
        who = await client.whoami()
        return {
            "id": getattr(who, "id", None),
            "username": getattr(who, "username", None),
            "number": getattr(who, "number", None),
        }

    async def aclose(self) -> None:
        if self._client is not None:
            try:
                # TODO(human): real transport teardown.
                await self._client.disconnect()
            finally:
                self._client = None


__all__ = ["SignalClient"]
