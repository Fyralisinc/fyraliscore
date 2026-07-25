"""Persistent installation-scoped signal-cli receive worker.

Holds one installation's linked-device session and drives
``dispatch.handle_update`` for every normalized SSE receive event. A finite
stream (Provider Lab or daemon restart) is resubscribed after a bounded delay;
typed ``RetryLater`` outcomes escape so the process supervisor can relinquish
the worker instead of hot-looping through a provider cooldown.

Durability checkpointing: the worker acknowledges each SSE update only after it
crosses the ingestion durability boundary, then persists the advancing message
timestamp in ``signal_update_state``. signal-cli owns its local receive queue;
the Fyralis cursor is an audit/reconciliation checkpoint, not an invented
server-side Signal replay token.

Installation-scoped safety: a Signal linked device should be driven by only one
live receive loop at a time, so the launcher acquires
``gateway:signal:{tenant_id}:{installation_id}:leader_lock`` before constructing
the worker. Different installations use different leases and transport
contexts.

"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import structlog

from lib.shared.errors import SignalApiError
from lib.shared.provider_transport import RequestPolicy, RetryLater
from services.ingest.integrations.provider_transport import (
    PolicyResolver,
    ProviderExecutor,
    QuotaResolver,
)
from services.ingest.integrations.signal.client import _message_to_dict
from services.ingest.integrations.signal.client import SignalJsonRpcTransport
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
        jsonrpc_endpoint: str | None = None,
        sse_endpoint: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        provider_transport: ProviderExecutor | None = None,
        request_policy: RequestPolicy | PolicyResolver | None = None,
        quota_resolver: QuotaResolver | None = None,
        allow_unlimited_local: bool | None = None,
        signal_cli_version: str | None = None,
        multi_account: bool | None = None,
        reconnect_delay_seconds: float = 1.0,
    ) -> None:
        self._deps = deps
        self._session = session
        self._account_label = account_label
        # thread_id -> {"thread_kind": str, "title": str|None}
        self._thread_index = thread_index
        self._save_state = save_state
        self._client: SignalJsonRpcTransport | None = None
        self._jsonrpc_endpoint = jsonrpc_endpoint
        self._sse_endpoint = sse_endpoint
        self._http_client = http_client
        self._provider_transport = provider_transport
        self._request_policy = request_policy
        self._quota_resolver = quota_resolver
        self._allow_unlimited_local = allow_unlimited_local
        self._signal_cli_version = signal_cli_version
        self._multi_account = multi_account
        if reconnect_delay_seconds < 0:
            raise ValueError("reconnect_delay_seconds must be non-negative")
        self._reconnect_delay_seconds = reconnect_delay_seconds

    def _thread_context(
        self,
        peer_id: int,
        *,
        default_kind: str = "direct",
        default_title: str | None = None,
    ) -> dict[str, Any]:
        meta = self._thread_index.get(peer_id) or {}
        return {
            "thread_kind": meta.get("thread_kind") or default_kind,
            "thread_title": meta.get("title") or default_title,
        }

    async def run_forever(self) -> None:
        """Receive indefinitely, reconnecting only after finite/transient EOF."""

        while True:
            try:
                delivered = await self.run_once()
                log.info(
                    "signal_gateway.stream_ended",
                    delivered=delivered,
                    tenant_id=str(self._deps.tenant_id),
                    installation_id=self._deps.installation_id,
                )
            except asyncio.CancelledError:
                raise
            except RetryLater:
                # ProviderTransport selected a durable cooldown. Let the
                # supervisor release the installation worker rather than sleep
                # while holding its leader lease.
                raise
            except SignalApiError as exc:
                if not exc.recoverable:
                    raise
                log.warning(
                    "signal_gateway.recoverable_error",
                    code=exc.code,
                    tenant_id=str(self._deps.tenant_id),
                    installation_id=self._deps.installation_id,
                )
            except httpx.TransportError as exc:
                log.warning(
                    "signal_gateway.stream_transport_error",
                    error_type=type(exc).__name__,
                    tenant_id=str(self._deps.tenant_id),
                    installation_id=self._deps.installation_id,
                )

            if self._reconnect_delay_seconds:
                await asyncio.sleep(self._reconnect_delay_seconds)

    async def run_once(self) -> int:
        """Consume one subscription stream, returning its delivery count."""

        client = self._client
        if client is None:
            client = SignalJsonRpcTransport(
                session=self._session,
                account_label=self._account_label,
                tenant_id=self._deps.tenant_id,
                installation_id=self._deps.installation_id,
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
            self._client = client

        delivered = 0
        async for update in client.iter_updates():
            thread_id = update.get("thread_id")
            if isinstance(thread_id, int):
                context = self._thread_context(
                    thread_id,
                    default_kind=str(update.get("thread_kind") or "direct"),
                    default_title=update.get("thread_title"),
                )
                update["thread_kind"] = context["thread_kind"]
                if context["thread_title"] is not None:
                    update["thread_title"] = context["thread_title"]
            durable = await handle_update(update, self._deps)
            if durable is False:
                raise SignalApiError(
                    "Signal update missed the ingestion durability boundary",
                    code="signal_api_error",
                    context={
                        "error_type": "IngestionDurabilityFailure",
                        "thread_id": thread_id,
                    },
                )
            client.acknowledge_update(update)
            delivered += 1
            await self._persist_state(client)
        return delivered

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
