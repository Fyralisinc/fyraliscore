"""Pinned signal-cli JSON-RPC/SSE transport and Fyralis read client.

Signal does not expose an official server API. Fyralis therefore targets the
HTTP daemon surface of signal-cli 0.14.4.1 and only the operations it needs:

* ``listGroups`` for installation-scoped group discovery;
* ``receive`` for the daemon's finite, forward-only local replay;
* ``subscribeReceive`` / ``unsubscribeReceive`` plus SSE for live delivery.

The daemon is started with ``--http``. Its native endpoints are
``/api/v1/rpc`` and ``/api/v1/events``. Endpoint overrides also let the exact
same client run against Provider Lab's ``/signal/jsonrpc`` and subscription
SSE route.

There is deliberately no invented deep-history API. ``get_history`` pages the
messages available in signal-cli's local receive queue/cache, so Signal
backfill remains shallow and forward-only. The persistent gateway is the
primary completeness path.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import AsyncIterator, Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

import httpx
import structlog

from lib.shared.errors import SignalApiError
from lib.shared.provider_transport import (
    ProviderPermanentError,
    ProviderRateLimited,
    ProviderTimeoutError,
    ProviderTransientError,
    RequestPolicy,
    parse_retry_after,
)
from services.ingest.integrations.provider_transport import (
    PolicyResolver,
    ProviderExecutor,
    ProviderRequestBinding,
    QuotaResolver,
    explicit_local_transport,
)
from services.ingest.integrations.secret_cache import SecretValueCache


log = structlog.get_logger("integrations.signal.client")

PINNED_SIGNAL_CLI_VERSION = "0.14.4.1"
_DEFAULT_PAGE_SIZE = 100
_DEFAULT_RPC_PATH = "/api/v1/rpc"
_DEFAULT_EVENTS_PATH = "/api/v1/events"


def _positive_stable_id(value: object) -> int:
    """Map provider string identities into a stable positive BIGINT."""

    text = str(value or "").strip()
    try:
        numeric = int(text)
    except ValueError:
        numeric = 0
    if numeric > 0:
        return numeric
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1) or 1


def _integer(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _message_to_dict(msg: Any) -> dict[str, Any]:
    """Coerce a canonical dict or legacy message object into record input."""

    if isinstance(msg, Mapping):
        sender = msg.get("from_id")
        return {
            "id": _integer(msg.get("id")),
            "date": (
                _integer(msg.get("date"))
                if msg.get("date") is not None
                else None
            ),
            "edit_date": (
                _integer(msg.get("edit_date"))
                if msg.get("edit_date") is not None
                else None
            ),
            "message": str(msg.get("message") or ""),
            "out": bool(msg.get("out", False)),
            "from_id": dict(sender) if isinstance(sender, Mapping) else None,
            **(
                {"sender_username": str(msg["sender_username"])}
                if msg.get("sender_username")
                else {}
            ),
        }

    from_id = getattr(msg, "from_id", None)
    user_id = getattr(from_id, "user_id", None)
    sender = {"user_id": user_id} if isinstance(user_id, int) else None
    date = getattr(msg, "date", None)
    edit_date = getattr(msg, "edit_date", None)
    return {
        "id": _integer(getattr(msg, "id", 0)),
        "date": int(date.timestamp()) if date is not None else None,
        "edit_date": (
            int(edit_date.timestamp()) if edit_date is not None else None
        ),
        "message": str(getattr(msg, "message", None) or ""),
        "out": bool(getattr(msg, "out", False)),
        "from_id": sender,
    }


def _rpc_and_events_urls(
    endpoint: str,
    events_endpoint: str | None,
) -> tuple[str, str]:
    raw = endpoint.strip()
    if not raw:
        raise SignalApiError(
            "SIGNAL_JSONRPC_ENDPOINT is not configured",
            code="signal_api_unauthorized",
        )
    split = urlsplit(raw)
    if split.scheme not in {"http", "https"} or not split.netloc:
        raise SignalApiError(
            "Signal requires a signal-cli HTTP daemon endpoint",
            code="signal_api_error",
            context={"required_transport": "signal-cli daemon --http"},
        )
    path = split.path.rstrip("/")
    if not path:
        path = _DEFAULT_RPC_PATH
        raw = urlunsplit((split.scheme, split.netloc, path, "", ""))

    if events_endpoint:
        events = events_endpoint.strip()
    elif path.endswith(_DEFAULT_RPC_PATH):
        events = urlunsplit(
            (
                split.scheme,
                split.netloc,
                path[: -len(_DEFAULT_RPC_PATH)] + _DEFAULT_EVENTS_PATH,
                "",
                "",
            )
        )
    elif path.endswith("/jsonrpc"):
        events = urlunsplit(
            (
                split.scheme,
                split.netloc,
                path[: -len("/jsonrpc")] + "/events/{subscription_id}",
                "",
                "",
            )
        )
    else:
        raise SignalApiError(
            "SIGNAL_SSE_ENDPOINT is required for a custom JSON-RPC path",
            code="signal_api_error",
        )
    return raw, events


def _unwrap_receive(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    current: object = payload
    if payload.get("method") == "receive":
        current = payload.get("params")
    if not isinstance(current, Mapping):
        return None
    result = current.get("result")
    if isinstance(result, Mapping):
        current = result
    if not isinstance(current, Mapping):
        return None
    return current if isinstance(current.get("envelope"), Mapping) else None


def _receive_to_update(
    payload: Mapping[str, Any],
    *,
    group_titles: Mapping[int, str | None] | None = None,
) -> dict[str, Any] | None:
    """Convert one signal-cli receive payload into the canonical live update."""

    received = _unwrap_receive(payload)
    if received is None:
        return None
    envelope = received["envelope"]
    if not isinstance(envelope, Mapping):
        return None

    data_message = envelope.get("dataMessage")
    outgoing = False
    peer: object = (
        envelope.get("sourceUuid")
        or envelope.get("sourceNumber")
        or envelope.get("source")
    )
    if not isinstance(data_message, Mapping):
        sync = envelope.get("syncMessage")
        sent = sync.get("sentMessage") if isinstance(sync, Mapping) else None
        if not isinstance(sent, Mapping):
            return None
        data_message = sent
        outgoing = True
        peer = (
            sent.get("destinationUuid")
            or sent.get("destinationNumber")
            or sent.get("destination")
            or peer
        )

    stamp_ms = _integer(
        data_message.get("timestamp") or envelope.get("timestamp")
    )
    if stamp_ms <= 0:
        return None
    group_info = data_message.get("groupInfo")
    group_id = (
        group_info.get("groupId")
        if isinstance(group_info, Mapping)
        else None
    )
    if not group_id and not peer:
        return None
    thread_kind = "group" if group_id else "direct"
    thread_id = _positive_stable_id(group_id or peer)
    sender_id = _positive_stable_id(
        envelope.get("sourceUuid")
        or envelope.get("sourceNumber")
        or envelope.get("source")
        or peer
    )
    source_name = envelope.get("sourceName")
    title = (
        (group_titles or {}).get(thread_id)
        if thread_kind == "group"
        else (str(source_name) if source_name else None)
    )
    message: dict[str, Any] = {
        "id": stamp_ms,
        "date": stamp_ms // 1000,
        "edit_date": None,
        "message": str(
            data_message.get("message")
            or data_message.get("body")
            or ""
        ),
        "out": outgoing,
        "from_id": {"user_id": sender_id},
    }
    if source_name:
        message["sender_username"] = str(source_name)
    return {
        "event": "new_message",
        "message": message,
        "thread_id": thread_id,
        "thread_kind": thread_kind,
        "thread_title": title,
    }


class SignalJsonRpcTransport:
    """Exact installation-scoped signal-cli HTTP JSON-RPC/SSE boundary."""

    def __init__(
        self,
        *,
        session: str,
        account_label: str,
        tenant_id: UUID | str,
        installation_id: UUID | str,
        jsonrpc_endpoint: str | None = None,
        sse_endpoint: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        provider_transport: ProviderExecutor | None = None,
        request_policy: RequestPolicy | PolicyResolver | None = None,
        quota_resolver: QuotaResolver | None = None,
        allow_unlimited_local: bool | None = None,
        signal_cli_version: str | None = None,
        multi_account: bool | None = None,
    ) -> None:
        session = session.strip()
        account_label = account_label.strip()
        if not session or not account_label:
            raise SignalApiError(
                "Signal transport requires session and account identity",
                code="signal_api_unauthorized",
            )
        configured_version = (
            signal_cli_version
            or os.environ.get("SIGNAL_CLI_VERSION")
            or PINNED_SIGNAL_CLI_VERSION
        )
        if configured_version != PINNED_SIGNAL_CLI_VERSION:
            raise SignalApiError(
                "unsupported signal-cli version",
                code="signal_api_error",
                context={
                    "configured_version": configured_version,
                    "pinned_version": PINNED_SIGNAL_CLI_VERSION,
                },
            )

        explicit_endpoint = jsonrpc_endpoint
        endpoint = (
            explicit_endpoint
            or os.environ.get("SIGNAL_JSONRPC_ENDPOINT")
            or ""
        )
        events = sse_endpoint or os.environ.get("SIGNAL_SSE_ENDPOINT")
        self._rpc_url, self._events_url = _rpc_and_events_urls(
            endpoint,
            events,
        )
        self._native_http_daemon = urlsplit(self._rpc_url).path.endswith(
            _DEFAULT_RPC_PATH
        )
        self._send_session_header = (
            not self._native_http_daemon
            or os.environ.get(
                "SIGNAL_CLI_SESSION_AUTH",
                "",
            ).strip().casefold()
            in {"1", "true", "yes", "on"}
        )
        self._session = session
        self._account_label = account_label
        self._multi_account = (
            multi_account
            if multi_account is not None
            else os.environ.get(
                "SIGNAL_CLI_MULTI_ACCOUNT",
                "",
            ).strip().casefold()
            in {"1", "true", "yes", "on"}
        )
        self._tenant_id = str(tenant_id)
        self._installation_id = str(installation_id)
        self._owns_http = http_client is None
        self._http = http_client or httpx.AsyncClient(timeout=30.0)
        if provider_transport is None or quota_resolver is None:
            from services.ingest.integrations.provider_transport_runtime import (
                get_provider_transport_runtime,
            )

            runtime = get_provider_transport_runtime()
            if runtime is not None:
                provider_transport = provider_transport or runtime.transport
                quota_resolver = quota_resolver or runtime.quota_resolver
        local_unlimited = explicit_local_transport(
            requested=allow_unlimited_local,
            has_local_injection=(
                http_client is not None
                or explicit_endpoint is not None
                or bool(endpoint)
            ),
        )
        self._provider = ProviderRequestBinding(
            source="signal",
            tenant_id=self._tenant_id,
            installation_id=self._installation_id,
            transport=provider_transport,
            request_policy=request_policy,
            quota_resolver=quota_resolver,
            allow_unlimited_local=local_unlimited,
        )
        self._request_sequence = 0
        self._subscription_id: str | int | None = None
        self._group_titles: dict[int, str | None] = {}
        self._history: dict[int, dict[int, dict[str, Any]]] = {}
        self.sync_cursor: int | None = None
        self._closed = False

    @property
    def account_label(self) -> str:
        return self._account_label

    def _headers(self, *, sse: bool = False) -> dict[str, str]:
        headers = {
            "Accept": "text/event-stream" if sse else "application/json",
        }
        if self._send_session_header:
            headers["Authorization"] = f"Session {self._session}"
        return headers

    def _params(self, params: Mapping[str, Any] | None) -> dict[str, Any]:
        result = dict(params or {})
        if self._multi_account:
            result.setdefault("account", self._account_label)
        return result

    async def _rpc(
        self,
        method: str,
        *,
        operation: str,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        self._request_sequence += 1
        request_id = (
            f"{self._installation_id}:{self._request_sequence}:{uuid4().hex[:8]}"
        )
        body = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": self._params(params),
        }

        async def _once() -> Any:
            try:
                response = await self._http.post(
                    self._rpc_url,
                    headers=self._headers(),
                    json=body,
                )
            except httpx.TimeoutException as exc:
                raise ProviderTimeoutError(
                    "signal-cli JSON-RPC timed out",
                    source="signal",
                    operation=operation,
                ) from exc
            except httpx.TransportError as exc:
                raise ProviderTransientError(
                    "signal-cli JSON-RPC transport error",
                    source="signal",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc
            self._raise_http_error(response, operation=operation)
            try:
                payload = response.json()
            except ValueError as exc:
                raise ProviderTransientError(
                    "signal-cli returned invalid JSON",
                    source="signal",
                    operation=operation,
                ) from exc
            if not isinstance(payload, Mapping):
                raise ProviderTransientError(
                    "signal-cli returned a non-object JSON-RPC response",
                    source="signal",
                    operation=operation,
                )
            error = payload.get("error")
            if isinstance(error, Mapping):
                self._raise_rpc_error(error, operation=operation)
            if "result" not in payload:
                raise ProviderTransientError(
                    "signal-cli JSON-RPC response omitted result",
                    source="signal",
                    operation=operation,
                )
            return payload["result"]

        try:
            return await self._provider.execute(operation, _once)
        except ProviderPermanentError as exc:
            status = _integer(exc.context.get("status_code"))
            code = (
                "signal_api_unauthorized"
                if status in {401, 403}
                else "signal_api_error"
            )
            raise SignalApiError(
                exc.message,
                code=code,
                context={
                    "operation": operation,
                    **(
                        {"http_status": status}
                        if status
                        else {}
                    ),
                },
            ) from exc

    @staticmethod
    def _raise_http_error(
        response: httpx.Response,
        *,
        operation: str,
    ) -> None:
        if response.status_code == 429:
            retry_after = parse_retry_after(
                response.headers.get("Retry-After")
            )
            raise ProviderRateLimited(
                "signal-cli rate limited",
                retry_after_seconds=retry_after,
                header_parser_id="signal.retry_after",
                source="signal",
                operation=operation,
            )
        if response.status_code >= 500:
            raise ProviderTransientError(
                "signal-cli daemon unavailable",
                source="signal",
                operation=operation,
                status_code=response.status_code,
            )
        if response.status_code >= 400:
            raise ProviderPermanentError(
                "signal-cli request rejected",
                source="signal",
                operation=operation,
                status_code=response.status_code,
            )

    @staticmethod
    def _raise_rpc_error(
        error: Mapping[str, Any],
        *,
        operation: str,
    ) -> None:
        code = _integer(error.get("code"))
        message = str(error.get("message") or "signal-cli JSON-RPC error")
        data = error.get("data")
        data_map = data if isinstance(data, Mapping) else {}
        retry_after = parse_retry_after(
            data_map.get("retryAfter")
            or data_map.get("retry_after")
        )
        lowered = message.casefold()
        if retry_after is not None or "rate limit" in lowered:
            raise ProviderRateLimited(
                "signal-cli rate limited",
                retry_after_seconds=retry_after,
                header_parser_id="signal.retry_after",
                source="signal",
                operation=operation,
                rpc_code=code,
            )
        raise ProviderPermanentError(
            message,
            source="signal",
            operation=operation,
            rpc_code=code,
        )

    async def list_threads(
        self,
        *,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        groups = await self._rpc(
            "listGroups",
            operation="list_groups",
        )
        if not isinstance(groups, list):
            raise SignalApiError(
                "signal-cli listGroups returned a non-list",
                code="signal_api_error",
            )
        threads: list[dict[str, Any]] = []
        for group in groups:
            if not isinstance(group, Mapping):
                continue
            native_id = group.get("id")
            if native_id in {None, ""}:
                continue
            thread_id = _positive_stable_id(native_id)
            title = str(group["name"]) if group.get("name") else None
            self._group_titles[thread_id] = title
            threads.append(
                {
                    "thread_id": thread_id,
                    "thread_kind": "group",
                    "title": title,
                }
            )
        return threads[: max(0, int(limit))]

    async def whoami(self) -> dict[str, Any]:
        # listGroups is a read-only authenticated connectivity probe supported
        # in both single-account and multi-account signal-cli daemon modes.
        await self.list_threads(limit=1)
        return {
            "id": _positive_stable_id(self._account_label),
            "username": self._account_label,
            "number": (
                self._account_label
                if self._account_label.startswith("+")
                else None
            ),
        }

    async def _receive_once(self) -> list[dict[str, Any]]:
        result = await self._rpc("receive", operation="receive_poll")
        payloads = result if isinstance(result, list) else [result]
        updates: list[dict[str, Any]] = []
        for payload in payloads:
            if not isinstance(payload, Mapping):
                continue
            update = _receive_to_update(
                payload,
                group_titles=self._group_titles,
            )
            if update is None:
                continue
            updates.append(update)
            self._remember(update)
        return updates

    def _remember(self, update: Mapping[str, Any]) -> None:
        thread_id = update.get("thread_id")
        message = update.get("message")
        if not isinstance(thread_id, int) or not isinstance(message, Mapping):
            return
        canonical = _message_to_dict(message)
        message_id = canonical["id"]
        if not isinstance(message_id, int) or message_id <= 0:
            return
        self._history.setdefault(thread_id, {})[message_id] = canonical
        self.sync_cursor = max(self.sync_cursor or 0, message_id)

    def acknowledge_update(self, update: Mapping[str, Any]) -> None:
        """Advance the live cursor only after downstream durability succeeds."""

        self._remember(update)

    async def get_messages(
        self,
        thread_id: int,
        *,
        limit: int,
        offset_id: int = 0,
        min_id: int = 0,
    ) -> list[dict[str, Any]]:
        await self._receive_once()
        messages = list(self._history.get(thread_id, {}).values())
        candidates = [
            item
            for item in messages
            if (offset_id == 0 or item["id"] < offset_id)
            and item["id"] > min_id
        ]
        candidates.sort(key=lambda item: item["id"], reverse=True)
        return candidates[: max(1, min(_DEFAULT_PAGE_SIZE, limit))]

    async def _subscribe(self) -> str | int:
        result = await self._rpc(
            "subscribeReceive",
            operation="subscribe_receive",
        )
        subscription: object = result
        if isinstance(result, Mapping):
            subscription = (
                result.get("subscription")
                or result.get("subscriptionId")
            )
        if not isinstance(subscription, (str, int)) or str(subscription) == "":
            raise SignalApiError(
                "signal-cli returned an invalid receive subscription",
                code="signal_api_error",
            )
        self._subscription_id = subscription
        return subscription

    async def _unsubscribe(self) -> None:
        subscription = self._subscription_id
        self._subscription_id = None
        if subscription is None:
            return
        try:
            await self._rpc(
                "unsubscribeReceive",
                operation="unsubscribe_receive",
                params={"subscription": subscription},
            )
        except Exception:  # noqa: BLE001 - cleanup is best effort
            log.warning(
                "signal.unsubscribe_failed",
                tenant_id=self._tenant_id,
                installation_id=self._installation_id,
            )

    def _event_url(self, subscription: str | int) -> str:
        return self._events_url.format(subscription_id=subscription)

    async def _open_event_response(
        self,
        subscription: str | int,
    ) -> httpx.Response:
        operation = "events_stream"

        async def _once() -> httpx.Response:
            request = self._http.build_request(
                "GET",
                self._event_url(subscription),
                headers=self._headers(sse=True),
            )
            try:
                response = await self._http.send(request, stream=True)
            except httpx.TimeoutException as exc:
                raise ProviderTimeoutError(
                    "signal-cli SSE connect timed out",
                    source="signal",
                    operation=operation,
                ) from exc
            except httpx.TransportError as exc:
                raise ProviderTransientError(
                    "signal-cli SSE transport error",
                    source="signal",
                    operation=operation,
                    error_type=type(exc).__name__,
                ) from exc
            try:
                self._raise_http_error(response, operation=operation)
            except Exception:
                await response.aclose()
                raise
            content_type = response.headers.get("Content-Type", "")
            if not content_type.casefold().startswith("text/event-stream"):
                await response.aclose()
                raise ProviderPermanentError(
                    "signal-cli events endpoint is not SSE",
                    source="signal",
                    operation=operation,
                    content_type=content_type,
                )
            return response

        try:
            return await self._provider.execute(operation, _once)
        except ProviderPermanentError as exc:
            status = _integer(exc.context.get("status_code"))
            raise SignalApiError(
                exc.message,
                code=(
                    "signal_api_unauthorized"
                    if status in {401, 403}
                    else "signal_api_error"
                ),
                context={"operation": operation, "http_status": status},
            ) from exc

    async def iter_updates(self) -> AsyncIterator[dict[str, Any]]:
        """Yield one normalized update per SSE ``receive`` event."""

        # Native signal-cli HTTP exposes a process-wide /api/v1/events stream.
        # Provider Lab's finite conformance stream is subscription-scoped so it
        # exercises subscribe/unsubscribe ownership explicitly.
        subscription: str | int = (
            0 if self._native_http_daemon else await self._subscribe()
        )
        response: httpx.Response | None = None
        event_name = ""
        data_lines: list[str] = []
        try:
            response = await self._open_event_response(subscription)
            async for line in response.aiter_lines():
                if line == "":
                    update = self._decode_sse_event(event_name, data_lines)
                    event_name = ""
                    data_lines = []
                    if update is not None:
                        yield update
                    continue
                if line.startswith(":"):
                    continue
                field, _, value = line.partition(":")
                value = value[1:] if value.startswith(" ") else value
                if field == "event":
                    event_name = value
                elif field == "data":
                    data_lines.append(value)
            update = self._decode_sse_event(event_name, data_lines)
            if update is not None:
                yield update
        finally:
            if response is not None:
                await response.aclose()
            if not self._native_http_daemon:
                await self._unsubscribe()

    def _decode_sse_event(
        self,
        event_name: str,
        data_lines: list[str],
    ) -> dict[str, Any] | None:
        if event_name and event_name != "receive":
            return None
        if not data_lines:
            return None
        try:
            payload = json.loads("\n".join(data_lines))
        except json.JSONDecodeError:
            log.warning(
                "signal.invalid_sse_event",
                tenant_id=self._tenant_id,
                installation_id=self._installation_id,
            )
            return None
        if not isinstance(payload, Mapping):
            return None
        return _receive_to_update(
            payload,
            group_titles=self._group_titles,
        )

    async def disconnect(self) -> None:
        await self._unsubscribe()
        if self._owns_http and not self._closed:
            await self._http.aclose()
        self._closed = True


class SignalClient:
    """Fyralis fetcher/reconciler facade for one exact Signal installation."""

    def __init__(
        self,
        *,
        pool: Any | None = None,
        secret_store: Any | None = None,
        tenant_id: UUID | None = None,
        installation_id: UUID | str | None = None,
        account_label: str | None = None,
        session_secret_ref: str | None = None,
        session: str | None = None,
        jsonrpc_endpoint: str | None = None,
        sse_endpoint: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        provider_transport: ProviderExecutor | None = None,
        request_policy: RequestPolicy | PolicyResolver | None = None,
        quota_resolver: QuotaResolver | None = None,
        allow_unlimited_local: bool | None = None,
        signal_cli_version: str | None = None,
        multi_account: bool | None = None,
    ) -> None:
        self._pool = pool
        self._secret_store = secret_store
        self._tenant_id = tenant_id
        self._installation_id = (
            str(installation_id) if installation_id is not None else None
        )
        self._account_label = str(account_label or "").strip()
        self._session_secret_ref = session_secret_ref
        self._session_cache = SecretValueCache(preset=session)
        self._secret_lock = asyncio.Lock()
        self._client: SignalJsonRpcTransport | None = None
        self._connect_lock = asyncio.Lock()
        self._jsonrpc_endpoint = jsonrpc_endpoint
        self._sse_endpoint = sse_endpoint
        self._http_client = http_client
        self._provider_transport = provider_transport
        self._request_policy = request_policy
        self._quota_resolver = quota_resolver
        self._allow_unlimited_local = allow_unlimited_local
        self._signal_cli_version = signal_cli_version
        self._multi_account = multi_account

    async def _resolve_secret(
        self,
        ref: str | None,
        cache: SecretValueCache,
    ) -> str | None:
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
            self._session_secret_ref,
            self._session_cache,
        )

    async def _resolve_installation_id(self) -> str:
        if self._installation_id:
            return self._installation_id
        if (
            self._pool is None
            or self._tenant_id is None
            or not self._session_secret_ref
        ):
            raise SignalApiError(
                "signal client missing exact installation identity",
                code="signal_api_unauthorized",
            )
        rows = await self._pool.fetch(
            """
            SELECT id
            FROM signal_installations
            WHERE tenant_id = $1
              AND disabled_at IS NULL
              AND (
                    session_secret_ref = $2
                 OR backfill_session_secret_ref = $2
              )
            """,
            self._tenant_id,
            self._session_secret_ref,
        )
        if len(rows) != 1:
            raise SignalApiError(
                "signal session does not resolve to exactly one installation",
                code="signal_api_unauthorized",
                context={"match_count": len(rows)},
            )
        self._installation_id = str(rows[0]["id"])
        return self._installation_id

    async def _connect(self) -> SignalJsonRpcTransport:
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
            if not self._account_label:
                raise SignalApiError(
                    "signal client missing account identity",
                    code="signal_api_unauthorized",
                )
            installation_id = await self._resolve_installation_id()
            self._client = SignalJsonRpcTransport(
                session=session,
                account_label=self._account_label,
                tenant_id=self._tenant_id or "",
                installation_id=installation_id,
                jsonrpc_endpoint=self._jsonrpc_endpoint,
                sse_endpoint=self._sse_endpoint,
                http_client=self._http_client,
                provider_transport=self._provider_transport,
                request_policy=self._request_policy,
                quota_resolver=self._quota_resolver,
                allow_unlimited_local=self._allow_unlimited_local,
                signal_cli_version=self._signal_cli_version,
                multi_account=self._multi_account,
            )
            return self._client

    async def get_history(
        self,
        *,
        thread_id: int,
        thread_kind: str,
        offset_id: int = 0,
        min_id: int = 0,
        limit: int = _DEFAULT_PAGE_SIZE,
    ) -> tuple[list[dict[str, Any]], int | None, bool]:
        del thread_kind
        client = await self._connect()
        limit = min(_DEFAULT_PAGE_SIZE, max(1, limit))
        messages = await client.get_messages(
            thread_id,
            limit=limit,
            offset_id=offset_id,
            min_id=min_id,
        )
        ids = [
            item["id"]
            for item in messages
            if isinstance(item.get("id"), int) and item["id"] > 0
        ]
        next_offset_id = min(ids) if ids else None
        is_last = len(messages) < limit or next_offset_id is None
        return messages, next_offset_id, is_last

    async def iter_threads(self, *, limit: int = 200) -> list[dict[str, Any]]:
        return await (await self._connect()).list_threads(limit=limit)

    async def has_history_since(
        self,
        *,
        thread_id: int,
        thread_kind: str,
        min_id: int,
    ) -> bool:
        del thread_kind
        messages = await (await self._connect()).get_messages(
            thread_id,
            limit=1,
            min_id=min_id,
        )
        return bool(messages)

    async def me(self) -> dict[str, Any]:
        return await (await self._connect()).whoami()

    async def aclose(self) -> None:
        if self._client is not None:
            try:
                await self._client.disconnect()
            finally:
                self._client = None


__all__ = [
    "PINNED_SIGNAL_CLI_VERSION",
    "SignalClient",
    "SignalJsonRpcTransport",
    "_message_to_dict",
]
