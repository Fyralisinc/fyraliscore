"""services/ingest/integrations/telegram/client.py — outbound MTProto client.

A thin async wrapper over **Telethon** (the pure-Python MTProto user-account
client — ADR-0003). It is the single outbound surface for:

  - history BACKFILL  (`get_history` → messages.getHistory, paged on offset_id),
  - dialog ENUMERATION at onboarding (`iter_dialogs`),
  - the reconciler GAP PROBE (`has_history_since`),
  - a connectivity/credential probe (`me`).

The durable credential is a persisted Telethon `StringSession` (the auth_key),
resolved once from the secret store via `session_secret_ref` (or preset for the
Provider Lab / tests). `api_id`/`api_hash` are the MTProto application
credentials. Telethon is an OPTIONAL dependency: the import is deferred to
`connect()` so importing this module (and the whole ingestion package) never
requires Telethon — the synthetic harness monkeypatches the `_open_telegram_client`
seam and never connects.

All read methods return RAW message dicts (Telethon `Message` → dict via
`_message_to_dict`) of the shape `integrations/telegram/records.build_message_record`
consumes, so the backfill fetcher and the live worker share one record contract.

Every explicit Fyralis/Telethon operation runs through ProviderTransport with
the exact tenant + installation identity. FLOOD_WAIT becomes
``ProviderRateLimited`` and therefore a durable ``RetryLater`` when it cannot be
retried inside the bounded request budget.
"""
from __future__ import annotations

import asyncio
from typing import Any, Protocol
from uuid import UUID

import structlog

from lib.shared.errors import TelegramApiError
from lib.shared.provider_transport import (
    ProviderPermanentError,
    ProviderRateLimited,
    ProviderTimeoutError,
    ProviderTransientError,
    RequestPolicy,
)
from services.ingest.integrations.provider_transport import (
    PolicyResolver,
    ProviderExecutor,
    ProviderRequestBinding,
    QuotaResolver,
    explicit_local_transport,
)
from services.ingest.integrations.secret_cache import SecretValueCache


log = structlog.get_logger("integrations.telegram.client")

_DEFAULT_PAGE_SIZE = 100  # messages.getHistory caps the limit at 100.


class TelegramTransport(Protocol):
    """Injectable high-level boundary emulated by Provider Lab.

    The production implementation is Telethon inside ``TelegramClient``. Tests
    and Provider Lab inject only this finite used surface; Fyralis does not
    attempt to emulate or implement a general-purpose MTProto server.
    """

    async def connect(self) -> None: ...

    async def is_user_authorized(self) -> bool: ...

    async def get_history(
        self,
        *,
        dialog_id: int,
        access_hash: int | None,
        dialog_kind: str,
        offset_id: int,
        min_id: int,
        limit: int,
    ) -> tuple[list[dict[str, Any]], int | None, bool]: ...

    async def iter_dialogs(self, *, limit: int) -> list[dict[str, Any]]: ...

    async def has_history_since(
        self,
        *,
        dialog_id: int,
        access_hash: int | None,
        dialog_kind: str,
        min_id: int,
    ) -> bool: ...

    async def me(self) -> dict[str, Any]: ...

    async def disconnect(self) -> None: ...


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
        installation_id: UUID | str | None = None,
        telegram_transport: TelegramTransport | None = None,
        provider_transport: ProviderExecutor | None = None,
        request_policy: RequestPolicy | PolicyResolver | None = None,
        quota_resolver: QuotaResolver | None = None,
        allow_unlimited_local: bool | None = None,
        require_tenant_installation: bool = True,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._api_id = api_id
        self._api_hash_secret_ref = api_hash_secret_ref
        self._session_secret_ref = session_secret_ref
        # Preset session / api_hash (Provider Lab/test); else resolved from secrets.
        self._session_cache = SecretValueCache(preset=session)
        self._api_hash_cache = SecretValueCache(preset=api_hash)
        self._secret_lock = asyncio.Lock()
        self._client: Any | None = None
        self._injected_transport = telegram_transport
        self._connect_lock = asyncio.Lock()
        self._installation_id = (
            str(installation_id) if installation_id is not None else None
        )
        local_unlimited = explicit_local_transport(
            requested=allow_unlimited_local,
            has_local_injection=(
                telegram_transport is not None
                or session is not None
                or api_hash is not None
            ),
        )
        self._provider = ProviderRequestBinding(
            source="telegram",
            tenant_id=str(tenant_id) if tenant_id is not None else None,
            installation_id=self._installation_id,
            transport=provider_transport,
            request_policy=request_policy,
            quota_resolver=quota_resolver,
            allow_unlimited_local=local_unlimited,
            require_tenant_installation=require_tenant_installation,
        )

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
            missing_error=lambda: TelegramApiError(
                "telegram client missing secret material",
                code="telegram_api_unauthorized",
            ),
        )

    async def _resolve_session(self) -> str | None:
        return await self._resolve_secret(
            self._session_secret_ref, self._session_cache,
        )

    async def _resolve_api_hash(self) -> str | None:
        return await self._resolve_secret(
            self._api_hash_secret_ref, self._api_hash_cache,
        )

    async def _connect(self) -> Any:
        if self._client is not None:
            return self._client
        async with self._connect_lock:
            if self._client is not None:
                return self._client
            if self._injected_transport is not None:
                client = self._injected_transport
            else:
                try:
                    from telethon import TelegramClient as _TLClient  # type: ignore
                    from telethon.sessions import StringSession  # type: ignore
                except ImportError as exc:  # pragma: no cover - optional dep
                    raise TelegramApiError(
                        "telegram integration requires telethon "
                        "(pip install 'fyraliscore[telegram]')",
                        code="telegram_api_error",
                    ) from exc

                session = await self._resolve_session()
                api_hash = await self._resolve_api_hash()
                if not (session and self._api_id and api_hash):
                    raise TelegramApiError(
                        "telegram client missing session / api_id / api_hash",
                        code="telegram_api_unauthorized",
                    )
                client = _TLClient(
                    StringSession(session),
                    int(self._api_id),
                    api_hash,
                )

            await self._execute_telethon(
                "session.connect",
                client.connect,
            )
            authorized = await self._execute_telethon(
                "session.is_user_authorized",
                client.is_user_authorized,
            )
            if not authorized:
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

    async def _execute_telethon(
        self,
        operation: str,
        coro_factory: Any,
    ) -> Any:
        """Execute one explicit MTProto boundary under ProviderTransport."""

        async def _once() -> Any:
            try:
                return await coro_factory()
            except (
                ProviderPermanentError,
                ProviderRateLimited,
                ProviderTransientError,
            ):
                raise
            except Exception as exc:  # noqa: BLE001 — Telethon is optional.
                raise _map_telethon_provider_error(
                    exc,
                    operation=operation,
                ) from exc

        try:
            return await self._provider.execute(
                operation,
                _once,
                concurrency_key=(
                    f"telegram:{self._installation_id}:{operation}"
                    if self._installation_id is not None
                    else None
                ),
            )
        except ProviderPermanentError as exc:
            error_kind = exc.context.get("telegram_error_kind")
            code = (
                "telegram_api_unauthorized"
                if error_kind == "unauthorized"
                else (
                    "telegram_api_not_found"
                    if error_kind == "not_found"
                    else "telegram_api_error"
                )
            )
            raise TelegramApiError(
                exc.message,
                code=code,
                context={"method": operation},
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
        limit = min(_DEFAULT_PAGE_SIZE, max(1, limit))
        if self._injected_transport is not None:
            return await self._execute_telethon(
                "get_history",
                lambda: client.get_history(
                    dialog_id=dialog_id,
                    access_hash=access_hash,
                    dialog_kind=dialog_kind,
                    offset_id=offset_id,
                    min_id=min_id,
                    limit=limit,
                ),
            )
        peer = self._input_peer(
            dialog_id=dialog_id,
            access_hash=access_hash,
            dialog_kind=dialog_kind,
        )
        result = await self._execute_telethon(
            "get_history",
            lambda: client.get_messages(
                peer,
                limit=limit,
                offset_id=offset_id or 0,
                min_id=min_id or 0,
            ),
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
        if self._injected_transport is not None:
            return await self._execute_telethon(
                "iter_dialogs",
                lambda: client.iter_dialogs(limit=limit),
            )
        dialogs = await self._execute_telethon(
            "iter_dialogs",
            lambda: client.get_dialogs(limit=limit),
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
        if self._injected_transport is not None:
            return await self._execute_telethon(
                "has_history_since",
                lambda: client.has_history_since(
                    dialog_id=dialog_id,
                    access_hash=access_hash,
                    dialog_kind=dialog_kind,
                    min_id=min_id,
                ),
            )
        peer = self._input_peer(
            dialog_id=dialog_id,
            access_hash=access_hash,
            dialog_kind=dialog_kind,
        )
        result = await self._execute_telethon(
            "has_history_since",
            lambda: client.get_messages(peer, limit=1, min_id=min_id),
        )
        return len(result) > 0

    async def me(self) -> dict[str, Any]:
        """Cheap connectivity + auth probe (the authenticated account)."""
        client = await self._connect()
        if self._injected_transport is not None:
            return await self._execute_telethon("me", client.me)
        who = await self._execute_telethon("me", client.get_me)
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


def _map_telethon_provider_error(
    exc: Exception,
    *,
    operation: str,
) -> Exception:
    """Map FloodWait and the finite Telethon failure families Fyralis handles."""
    class_name = type(exc).__name__
    seconds = getattr(exc, "seconds", None)
    if class_name in {"FloodWait", "FloodWaitError"} or (
        isinstance(seconds, (int, float))
        and "flood" in class_name.casefold()
    ):
        return ProviderRateLimited(
            "telegram FLOOD_WAIT",
            retry_after_seconds=(
                max(0.0, float(seconds))
                if isinstance(seconds, (int, float))
                else None
            ),
            status_code=None,
            header_parser_id="telegram.flood_wait",
            source="telegram",
            operation=operation,
        )
    if isinstance(exc, asyncio.TimeoutError) or class_name in {
        "TimeoutError",
        "TimedOutError",
    }:
        return ProviderTimeoutError(
            "telegram operation timed out",
            source="telegram",
            operation=operation,
        )
    if class_name in {
        "AuthKeyError",
        "AuthKeyNotFound",
        "AuthKeyUnregisteredError",
        "SessionExpiredError",
        "SessionPasswordNeededError",
        "SessionRevokedError",
        "UnauthorizedError",
    }:
        return ProviderPermanentError(
            "telegram session is unauthorized",
            source="telegram",
            operation=operation,
            telegram_error_kind="unauthorized",
            error_type=class_name,
        )
    if class_name in {
        "ChannelInvalidError",
        "ChannelPrivateError",
        "ChatIdInvalidError",
        "PeerIdInvalidError",
        "UserIdInvalidError",
    }:
        return ProviderPermanentError(
            "telegram peer is unavailable",
            source="telegram",
            operation=operation,
            telegram_error_kind="not_found",
            error_type=class_name,
        )
    if isinstance(exc, (ConnectionError, OSError)) or class_name in {
        "ServerError",
        "RpcCallFailError",
    }:
        return ProviderTransientError(
            "telegram transport unavailable",
            source="telegram",
            operation=operation,
            error_type=class_name,
        )
    return ProviderPermanentError(
        "telegram RPC request failed",
        source="telegram",
        operation=operation,
        telegram_error_kind="permanent",
        error_type=class_name,
    )


__all__ = ["TelegramClient", "TelegramTransport"]
