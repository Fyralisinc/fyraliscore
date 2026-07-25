from __future__ import annotations

from typing import Any
from uuid import uuid4

import httpx
import pytest

from lib.shared.errors import SignalApiError
from services.ingest.integrations.signal.client import (
    PINNED_SIGNAL_CLI_VERSION,
    SignalClient,
    SignalJsonRpcTransport,
)
from services.ingest.integrations.signal.gateway import worker as worker_module
from services.ingest.integrations.signal.gateway.dispatch import DispatchDeps
from services.ingest.integrations.signal.gateway.worker import SignalGatewayWorker
from services.ingest.synthetic.fixtures.signal_generator import make_signal
from services.ingest.synthetic.provider_lab.app import build_provider_lab_app


pytestmark = pytest.mark.asyncio


class _RecordingProviderTransport:
    def __init__(self) -> None:
        self.contexts: list[Any] = []

    async def execute(self, request_context, policy, call):  # noqa: ANN001
        self.contexts.append(request_context)
        return await call()


def _quota(*args: Any, **kwargs: Any) -> tuple[()]:
    del args, kwargs
    return ()


async def test_signal_read_client_uses_provider_lab_json_rpc_surface() -> None:
    fixture = make_signal(threads=2, messages_per_thread=3)
    app = build_provider_lab_app(fixtures={"signal": [fixture]})
    tenant_id = uuid4()
    installation_id = uuid4()
    provider = _RecordingProviderTransport()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://provider-lab",
    ) as http:
        client = SignalClient(
            session="lab-signal::account",
            account_label="account",
            tenant_id=tenant_id,
            installation_id=installation_id,
            jsonrpc_endpoint="http://provider-lab/signal/jsonrpc",
            http_client=http,
            provider_transport=provider,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )

        threads = await client.iter_threads()
        identity = await client.me()
        group_thread_id = fixture["thread_order"][1]
        first, next_offset, is_last = await client.get_history(
            thread_id=group_thread_id,
            thread_kind="group",
            limit=2,
        )
        second, _, second_is_last = await client.get_history(
            thread_id=group_thread_id,
            thread_kind="group",
            offset_id=next_offset or 0,
            limit=2,
        )
        has_newer = await client.has_history_since(
            thread_id=group_thread_id,
            thread_kind="group",
            min_id=first[-1]["id"],
        )
        await client.aclose()

    assert threads == [
        {
            "thread_id": group_thread_id,
            "thread_kind": "group",
            "title": "Thread 2",
        }
    ]
    assert identity["username"] == "account"
    assert [message["message"] for message in first] == [
        f"message 3 in thread {group_thread_id}",
        f"message 2 in thread {group_thread_id}",
    ]
    assert is_last is False
    assert len(second) == 1
    assert second_is_last is True
    assert has_newer is True
    assert {context.operation for context in provider.contexts} == {
        "list_groups",
        "receive_poll",
    }
    assert all(context.tenant_id == str(tenant_id) for context in provider.contexts)
    assert all(
        context.installation_id == str(installation_id)
        for context in provider.contexts
    )


async def test_signal_gateway_consumes_sse_and_persists_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = make_signal(threads=2, messages_per_thread=2)
    app = build_provider_lab_app(fixtures={"signal": [fixture]})
    provider = _RecordingProviderTransport()
    delivered: list[dict[str, Any]] = []
    cursors: list[int | None] = []
    tenant_id = uuid4()
    installation_id = str(uuid4())

    async def _capture(update, deps):  # noqa: ANN001
        assert deps.tenant_id == tenant_id
        assert deps.installation_id == installation_id
        delivered.append(update)

    async def _save(cursor: int | None) -> None:
        cursors.append(cursor)

    monkeypatch.setattr(worker_module, "handle_update", _capture)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://provider-lab",
    ) as http:
        worker = SignalGatewayWorker(
            deps=DispatchDeps(
                pool=None,
                tenant_id=tenant_id,
                installation_id=installation_id,
            ),
            session="lab-signal::account",
            account_label="account",
            thread_index={},
            save_state=_save,
            jsonrpc_endpoint="http://provider-lab/signal/jsonrpc",
            http_client=http,
            provider_transport=provider,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        count = await worker.run_once()
        await worker.aclose()

    assert count == 4
    assert len(delivered) == 4
    assert all(update["event"] == "new_message" for update in delivered)
    assert cursors[-1] == max(
        update["message"]["id"] for update in delivered
    )
    assert {context.operation for context in provider.contexts} == {
        "subscribe_receive",
        "events_stream",
        "unsubscribe_receive",
    }
    assert all(context.tenant_id == str(tenant_id) for context in provider.contexts)
    assert all(
        context.installation_id == installation_id
        for context in provider.contexts
    )


async def test_signal_gateway_does_not_advance_cursor_before_durability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = make_signal(threads=1, messages_per_thread=1)
    app = build_provider_lab_app(fixtures={"signal": [fixture]})
    persisted: list[int | None] = []

    async def _not_durable(update, deps):  # noqa: ANN001
        del update, deps
        return False

    async def _save(cursor: int | None) -> None:
        persisted.append(cursor)

    monkeypatch.setattr(worker_module, "handle_update", _not_durable)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://provider-lab",
    ) as http:
        worker = SignalGatewayWorker(
            deps=DispatchDeps(
                pool=None,
                tenant_id=uuid4(),
                installation_id=str(uuid4()),
            ),
            session="lab-signal::account",
            account_label="account",
            thread_index={},
            save_state=_save,
            jsonrpc_endpoint="http://provider-lab/signal/jsonrpc",
            http_client=http,
            allow_unlimited_local=True,
        )
        with pytest.raises(SignalApiError, match="durability boundary"):
            await worker.run_once()
        assert worker._client is not None
        assert worker._client.sync_cursor is None
        await worker.aclose()

    assert persisted == []


async def test_signal_client_fails_closed_without_exact_installation() -> None:
    client = SignalClient(
        session="local-session",
        account_label="account",
        tenant_id=uuid4(),
        jsonrpc_endpoint="http://127.0.0.1:8080/api/v1/rpc",
        allow_unlimited_local=True,
    )

    with pytest.raises(SignalApiError, match="exact installation"):
        await client.me()


async def test_signal_transport_rejects_unpinned_signal_cli_version() -> None:
    with pytest.raises(SignalApiError, match="unsupported signal-cli version"):
        SignalJsonRpcTransport(
            session="session",
            account_label="account",
            tenant_id=uuid4(),
            installation_id=uuid4(),
            jsonrpc_endpoint="http://127.0.0.1:8080/api/v1/rpc",
            allow_unlimited_local=True,
            signal_cli_version="0.13.0",
        )

    assert PINNED_SIGNAL_CLI_VERSION == "0.14.4.1"


async def test_native_signal_cli_http_uses_rpc_and_process_sse_paths() -> None:
    requests: list[httpx.Request] = []
    event = (
        'event: receive\n'
        'data: {"envelope":{"timestamp":1000,"sourceUuid":"sender",'
        '"dataMessage":{"timestamp":1000,"message":"hello"}}}\n\n'
    )

    def _handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/events":
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                text=event,
            )
        raise AssertionError(f"unexpected native request {request.url}")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(_handler),
    ) as http:
        transport = SignalJsonRpcTransport(
            session="linked-device-material",
            account_label="+15551234567",
            tenant_id=uuid4(),
            installation_id=uuid4(),
            jsonrpc_endpoint="http://signal-cli:8080",
            http_client=http,
            allow_unlimited_local=True,
        )
        updates = [update async for update in transport.iter_updates()]
        await transport.disconnect()

    assert len(updates) == 1
    assert updates[0]["message"]["id"] == 1000
    assert [request.url.path for request in requests] == ["/api/v1/events"]
    assert "Authorization" not in requests[0].headers
