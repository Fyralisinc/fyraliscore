"""Production AWS and Telegram-boundary conformance against Provider Lab."""
from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import uvicorn

from lib.shared.provider_transport import RequestContext, RequestPolicy
from services.ingest.integrations.aws.client import AwsClient
from services.ingest.integrations.telegram.client import TelegramClient
from services.ingest.synthetic.fixtures.aws_generator import make_aws
from services.ingest.synthetic.fixtures.telegram_generator import make_telegram
from services.ingest.synthetic.provider_lab.app import build_provider_lab_app


pytestmark = pytest.mark.asyncio


class _Recorder:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []

    async def execute(self, context, policy, call):  # noqa: ANN001, ANN202
        assert isinstance(policy, RequestPolicy)
        self.contexts.append(context)
        return await call()


def _quota(*_args: object) -> tuple[()]:
    return ()


class _Store:
    def __init__(self, material: dict[str, object]) -> None:
        self.material = material

    async def get(self, _ref: str, *, tenant_id: UUID) -> str:
        del tenant_id
        return json.dumps(self.material)


class _LabTelegramTransport:
    """The deliberately finite TelegramTransport Provider Lab adapter."""

    def __init__(self, http: httpx.AsyncClient) -> None:
        self.http = http
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def is_user_authorized(self) -> bool:
        return True

    async def _post(self, operation: str, body: dict[str, Any]) -> dict[str, Any]:
        response = await self.http.post(
            f"/telegram/transport/{operation}",
            headers={"Authorization": "Session lab-telegram::account"},
            json=body,
        )
        response.raise_for_status()
        return response.json()

    async def get_history(self, **kwargs: object):
        payload = await self._post("get_history", dict(kwargs))
        return (
            payload["messages"],
            payload.get("next_offset_id"),
            payload["is_last"],
        )

    async def iter_dialogs(self, *, limit: int):
        return (await self._post("iter_dialogs", {"limit": limit}))["dialogs"]

    async def has_history_since(self, **kwargs: object) -> bool:
        return bool(
            (
                await self._post(
                    "has_history_since",
                    dict(kwargs),
                )
            )["has_history"]
        )

    async def me(self) -> dict[str, Any]:
        return await self._post("me", {})

    async def disconnect(self) -> None:
        self.connected = False


@pytest.mark.timeout(15)
async def test_real_aws_client_and_telegram_boundary_run_against_lab(
    unused_tcp_port: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    aws_fixture = make_aws(
        account_id="123456789012",
        events=3,
        per_page=2,
    )
    telegram_fixture = make_telegram(
        dialogs=1,
        messages_per_dialog=3,
        page_size=2,
    )
    dialog_id = telegram_fixture["dialog_order"][0]
    app = build_provider_lab_app(
        fixtures={
            "aws": [aws_fixture],
            "telegram": [telegram_fixture],
        }
    )
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=unused_tcp_port,
            log_level="error",
            lifespan="off",
            ws="websockets-sansio",
        )
    )
    server_task = asyncio.create_task(server.serve())
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.01)
    assert server.started

    tenant_id, aws_installation_id, telegram_installation_id = (
        uuid4(),
        uuid4(),
        uuid4(),
    )
    aws_recorder = _Recorder()
    telegram_recorder = _Recorder()
    endpoint = f"http://127.0.0.1:{unused_tcp_port}/aws"
    static_client = AwsClient(
        account_id="123456789012",
        region="us-east-1",
        tenant_id=tenant_id,
        installation_row_id=aws_installation_id,
        credential_kind="static_keys",
        secret_ref="static",
        secret_store=_Store(
            {
                "access_key_id": "AKIDLAB",
                "secret_access_key": "provider-lab-secret",
            }
        ),
        endpoint_override=endpoint,
        provider_transport=aws_recorder,
        quota_resolver=_quota,
        allow_unlimited_local=False,
    )
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "BASELABKEY")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "provider-lab-base-secret")
    assumed_client = AwsClient(
        account_id="123456789012",
        region="us-east-1",
        tenant_id=tenant_id,
        installation_row_id=aws_installation_id,
        credential_kind="assume_role",
        secret_ref="role",
        secret_store=_Store(
            {"role_arn": "arn:aws:iam::123456789012:role/Fyralis"}
        ),
        endpoint_override=endpoint,
        provider_transport=aws_recorder,
        quota_resolver=_quota,
        allow_unlimited_local=False,
    )
    http = httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{unused_tcp_port}"
    )
    telegram_transport = _LabTelegramTransport(http)
    telegram_client = TelegramClient(
        tenant_id=tenant_id,
        installation_id=telegram_installation_id,
        telegram_transport=telegram_transport,
        provider_transport=telegram_recorder,
        quota_resolver=_quota,
        allow_unlimited_local=False,
    )

    try:
        page = await static_client.list_events(
            account_id="123456789012",
            region="us-east-1",
            limit=2,
        )
        identity = await static_client.describe_account()
        assumed_identity = await assumed_client.describe_account()
        messages, next_offset, is_last = await telegram_client.get_history(
            dialog_id=dialog_id,
            access_hash=None,
            dialog_kind="chat",
            limit=2,
        )
        dialogs = await telegram_client.iter_dialogs()
        has_history = await telegram_client.has_history_since(
            dialog_id=dialog_id,
            access_hash=None,
            dialog_kind="chat",
            min_id=1,
        )
        telegram_identity = await telegram_client.me()
    finally:
        await static_client.aclose()
        await assumed_client.aclose()
        await telegram_client.aclose()
        await http.aclose()
        server.should_exit = True
        await server_task

    assert len(page["events"]) == 2
    assert page["next_cursor"] == "off:2"
    assert identity["account_id"] == "123456789012"
    assert assumed_identity["account_id"] == "123456789012"
    assert [context.operation for context in aws_recorder.contexts] == [
        "cloudtrail.lookup_events",
        "sts.get_caller_identity",
        "sts.assume_role",
        "sts.get_caller_identity",
    ]
    assert len(messages) == 2
    assert next_offset == min(message["id"] for message in messages)
    assert is_last is False
    assert dialogs[0]["dialog_id"] == dialog_id
    assert has_history is True
    assert telegram_identity["username"] == "provider_lab"
    assert [context.operation for context in telegram_recorder.contexts] == [
        "session.connect",
        "session.is_user_authorized",
        "get_history",
        "iter_dialogs",
        "has_history_since",
        "me",
    ]
    assert all(
        context.tenant_id == str(tenant_id)
        and context.installation_id
        in {str(aws_installation_id), str(telegram_installation_id)}
        for context in aws_recorder.contexts + telegram_recorder.contexts
    )
