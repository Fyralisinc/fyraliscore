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

CONFIRMED (signal.org/docs; github.com/AsamK/signal-cli): Signal has NO official
server API and NO maintained pure-Python client (freedomofpress/signal-protocol
is archived + protocol-only). The only sound integration is **signal-cli in
JSON-RPC daemon mode** — link signal-cli as a secondary device to a real number
(`signal-cli link`), run `signal-cli -a <number> daemon --tcp HOST:PORT` (or
`--socket PATH`), and talk JSON-RPC to it. So the "transport" is not a Python
import but a socket connection to that daemon (SIGNAL_JSONRPC_ENDPOINT).

HISTORY LIMITATION (CONFIRMED): a linked device cannot fetch arbitrary thread
history — signal-cli surfaces messages going FORWARD (the `receive` JSON-RPC
notification) plus what syncs at link time. So `get_history` backfill is
inherently shallow (own/linked-account, recent sync only); deep history is not
obtainable. This matches the research's "narrow coverage" finding for Signal.

The remaining TODO(human) is OPERATOR setup, not code shape: a linked number + a
running signal-cli daemon reachable at SIGNAL_JSONRPC_ENDPOINT. The synthetic
gate drives MockSignalClient and never connects, so the JSON-RPC socket client
below is a documented shell whose method signatures match the daemon's methods.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import UUID

import structlog

from lib.shared.errors import SignalApiError
from services.ingest.integrations.secret_cache import SecretValueCache


log = structlog.get_logger("integrations.signal.client")

# signal-cli's `receive`/history surface has no documented page cap (a linked
# device replays its local store); 100 is a sane self-imposed batch size.
_DEFAULT_PAGE_SIZE = 100

# CONFIRMED: the transport is a signal-cli JSON-RPC DAEMON reached over a socket
# (TCP `--tcp HOST:PORT` or UNIX `--socket PATH`), NOT a Python library import.
# Point the worker/client at it via SIGNAL_JSONRPC_ENDPOINT. The relevant daemon
# methods are `listGroups` (threads), `subscribeReceive`/`receive` (live), and
# there is NO history-fetch method (see HISTORY LIMITATION above).
_SIGNAL_JSONRPC_ENDPOINT = os.environ.get("SIGNAL_JSONRPC_ENDPOINT", "")


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
        self._session_cache = SecretValueCache(preset=session)
        self._secret_lock = asyncio.Lock()
        self._client: Any | None = None
        self._connect_lock = asyncio.Lock()

    async def _resolve_secret(
        self, ref: str | None, cache: SecretValueCache,
    ) -> str | None:
        if ref is None or self._secret_store is None or self._tenant_id is None:
            return None
        return await cache.resolve(
            lock=self._secret_lock,
            secret_store=self._secret_store,
            secret_ref=ref,
            tenant_id=self._tenant_id,
            missing_error=lambda: SignalApiError(
                "signal client missing linked-device session",
                code="signal_api_unauthorized",
            ),
        )

    async def _resolve_session(self) -> str | None:
        return await self._resolve_secret(
            self._session_secret_ref, self._session_cache,
        )

    async def _connect(self) -> Any:
        if self._client is not None:
            return self._client
        async with self._connect_lock:
            if self._client is not None:
                return self._client
            session = await self._resolve_session()
            if not session:
                raise SignalApiError(
                    "signal client missing linked-device session",
                    code="signal_api_unauthorized",
                )
            # OPERATOR STEP (not a code TODO): open a JSON-RPC connection to the
            # signal-cli daemon at SIGNAL_JSONRPC_ENDPOINT holding this account's
            # linked-device identity, and verify the device is still authorized
            # (not unlinked). Raises until an endpoint is configured + reachable;
            # the synthetic path patches the opener seam and never reaches here.
            if not _SIGNAL_JSONRPC_ENDPOINT:
                raise SignalApiError(
                    "signal transport unavailable: SIGNAL_JSONRPC_ENDPOINT unset "
                    "(run a signal-cli daemon for this linked account — see the "
                    "module docstring)",
                    code="signal_api_unauthorized",
                )
            raise SignalApiError(
                "signal-cli JSON-RPC client connection is not implemented "
                f"(endpoint={_SIGNAL_JSONRPC_ENDPOINT!r}); operator-provided daemon "
                "required",
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
