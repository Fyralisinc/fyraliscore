"""ProviderTransport contracts for AWS and the Telegram MTProto exception."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from lib.shared.provider_transport import (
    ProviderTransport,
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
    RetryLater,
    RetryReason,
)
from services.ingest.integrations.aws.client import AwsClient
from services.ingest.integrations.aws.credentials import AwsCredentials
from services.ingest.integrations.aws.live_poll import PollDeps, _resolve_install
from services.ingest.integrations.telegram.client import TelegramClient
from services.ingest.integrations.telegram.gateway.worker import (
    TelegramGatewayWorker,
)


pytestmark = pytest.mark.asyncio


class _Recorder:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []

    async def execute(self, context, policy, call):  # noqa: ANN001, ANN202
        assert isinstance(policy, RequestPolicy)
        self.contexts.append(context)
        return await call()


def _quota(
    source: str,
    operation: str,
    tenant_id: str | None,
    installation_id: str | None,
    dimensions: dict[str, str],
) -> tuple[QuotaRequirement, ...]:
    assert tenant_id is not None
    assert installation_id is not None
    assert dimensions == {}
    return (
        QuotaRequirement(
            scope="installation",
            bucket_key=f"{source}:{operation}:{installation_id}",
            capacity=10,
            refill_per_second=10.0,
        ),
    )


class _ServiceContext:
    def __init__(self, service: object) -> None:
        self.service = service

    async def __aenter__(self) -> object:
        return self.service

    async def __aexit__(self, *_args: object) -> None:
        return None


class _CloudTrail:
    async def lookup_events(self, **_kwargs: object) -> dict[str, object]:
        return {"Events": [], "NextToken": None}


class _StsIdentity:
    async def get_caller_identity(self) -> dict[str, str]:
        return {
            "Account": "123456789012",
            "Arn": "arn:aws:sts::123456789012:assumed-role/test",
            "UserId": "test",
        }


async def test_aws_calls_use_exact_context_and_finite_operations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id, installation_id = uuid4(), uuid4()
    recorder = _Recorder()
    client = AwsClient(
        account_id="123456789012",
        region="us-east-1",
        tenant_id=tenant_id,
        installation_row_id=installation_id,
        provider_transport=recorder,
        quota_resolver=_quota,
        allow_unlimited_local=False,
    )
    client._creds = AwsCredentials("access", "secret")

    async def _service_client(service: str) -> _ServiceContext:
        return _ServiceContext(
            _CloudTrail() if service == "cloudtrail" else _StsIdentity()
        )

    monkeypatch.setattr(client, "_service_client", _service_client)
    await client.list_events(
        account_id="123456789012",
        region="us-east-1",
    )
    await client.describe_account()

    assert [context.operation for context in recorder.contexts] == [
        "cloudtrail.lookup_events",
        "sts.get_caller_identity",
    ]
    assert all(
        context.source == "aws"
        and context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )
    assert client._retry_config().retries["total_max_attempts"] == 1


class _SecretStore:
    def __init__(self, material: dict[str, object]) -> None:
        self.material = material

    async def get(self, _ref: str, *, tenant_id: UUID) -> str:
        del tenant_id
        return json.dumps(self.material)


class _AssumeRole:
    async def assume_role(self, **_kwargs: object) -> dict[str, object]:
        return {
            "Credentials": {
                "AccessKeyId": "ASIALAB",
                "SecretAccessKey": "secret",
                "SessionToken": "session",
                "Expiration": datetime(2030, 1, 1, tzinfo=timezone.utc),
            }
        }


class _Session:
    def client(self, service: str, **kwargs: object) -> _ServiceContext:
        assert service == "sts"
        config = kwargs["config"]
        assert config.retries["total_max_attempts"] == 1
        return _ServiceContext(_AssumeRole())


async def test_aws_assume_role_credential_acquisition_is_transport_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import aioboto3

    monkeypatch.setattr(aioboto3, "Session", _Session)
    tenant_id, installation_id = uuid4(), uuid4()
    recorder = _Recorder()
    client = AwsClient(
        account_id="123456789012",
        region="us-east-1",
        tenant_id=tenant_id,
        installation_row_id=installation_id,
        credential_kind="assume_role",
        secret_ref="role-ref",
        secret_store=_SecretStore(
            {"role_arn": "arn:aws:iam::123456789012:role/Fyralis"}
        ),
        provider_transport=recorder,
        quota_resolver=_quota,
        allow_unlimited_local=False,
    )

    credentials = await client._credentials()

    assert credentials.access_key_id == "ASIALAB"
    assert [context.operation for context in recorder.contexts] == [
        "sts.assume_role"
    ]
    context = recorder.contexts[0]
    assert context.tenant_id == str(tenant_id)
    assert context.installation_id == str(installation_id)


class _ThrottleError(Exception):
    def __init__(self) -> None:
        self.response = {
            "Error": {"Code": "ThrottlingException"},
            "ResponseMetadata": {
                "HTTPStatusCode": 429,
                "HTTPHeaders": {"retry-after": "45"},
            },
        }


class _ThrottledCloudTrail:
    def __init__(self) -> None:
        self.calls = 0

    async def lookup_events(self, **_kwargs: object) -> dict[str, object]:
        self.calls += 1
        raise _ThrottleError


async def test_aws_throttle_becomes_durable_retry_without_botocore_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _ThrottledCloudTrail()
    transport = ProviderTransport(
        now=lambda: datetime(2026, 7, 25, tzinfo=timezone.utc)
    )
    client = AwsClient(
        account_id="123456789012",
        region="us-east-1",
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        provider_transport=transport,
        request_policy=RequestPolicy(
            max_attempts=1,
            max_inline_retry_after_seconds=0,
        ),
        quota_resolver=_quota,
        allow_unlimited_local=False,
    )
    client._creds = AwsCredentials("access", "secret")

    async def _service_client(_service: str) -> _ServiceContext:
        return _ServiceContext(service)

    monkeypatch.setattr(client, "_service_client", _service_client)
    with pytest.raises(RetryLater) as raised:
        await client.list_events(
            account_id="123456789012",
            region="us-east-1",
        )

    assert service.calls == 1
    assert raised.value.context["operation"] == "cloudtrail.lookup_events"
    assert raised.value.context["reason"] == RetryReason.RATE_LIMIT.value
    assert raised.value.context["retry_after_seconds"] == 45.0


async def test_aws_live_poll_resolves_only_exact_tenant_installation() -> None:
    tenant_id, installation_id = uuid4(), uuid4()

    class _Pool:
        def __init__(self) -> None:
            self.call: tuple[str, tuple[object, ...]] | None = None

        async def fetchrow(self, query, *args):  # noqa: ANN001, ANN202
            self.call = query, args
            return {"id": installation_id, "tenant_id": tenant_id}

    pool = _Pool()
    resolved = await _resolve_install(
        PollDeps(
            pool=pool,
            tenant_id=tenant_id,
            installation_id=str(installation_id),
        ),
        account_id="123456789012",
        region="us-east-1",
    )

    assert resolved is not None
    assert resolved.tenant_id == tenant_id
    assert resolved.installation_id == str(installation_id)
    assert pool.call is not None
    query, args = pool.call
    assert "LIMIT 1" not in query.upper()
    assert args == (
        tenant_id,
        str(installation_id),
        "123456789012",
        "us-east-1",
    )


class _TelegramBoundary:
    def __init__(self) -> None:
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def is_user_authorized(self) -> bool:
        return True

    async def get_history(self, **_kwargs: object):
        return ([{"id": 2}, {"id": 1}], 1, False)

    async def iter_dialogs(self, *, limit: int):
        return [{"dialog_id": 7, "dialog_kind": "chat"}][:limit]

    async def has_history_since(self, **_kwargs: object) -> bool:
        return True

    async def me(self) -> dict[str, object]:
        return {"id": 9, "username": "fyralis"}

    async def disconnect(self) -> None:
        self.connected = False


async def test_telegram_injected_boundary_uses_exact_installation_operations() -> None:
    tenant_id, installation_id = uuid4(), uuid4()
    recorder = _Recorder()
    boundary = _TelegramBoundary()
    client = TelegramClient(
        tenant_id=tenant_id,
        installation_id=installation_id,
        telegram_transport=boundary,
        provider_transport=recorder,
        quota_resolver=_quota,
        allow_unlimited_local=False,
    )

    await client.get_history(
        dialog_id=7,
        access_hash=None,
        dialog_kind="chat",
    )
    await client.iter_dialogs()
    await client.has_history_since(
        dialog_id=7,
        access_hash=None,
        dialog_kind="chat",
        min_id=1,
    )
    await client.me()
    await client.aclose()

    assert [context.operation for context in recorder.contexts] == [
        "session.connect",
        "session.is_user_authorized",
        "get_history",
        "iter_dialogs",
        "has_history_since",
        "me",
    ]
    assert all(
        context.source == "telegram"
        and context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )
    assert boundary.connected is False


class FloodWaitError(Exception):
    def __init__(self, seconds: int) -> None:
        self.seconds = seconds


class _FloodingTelegramBoundary(_TelegramBoundary):
    async def get_history(self, **_kwargs: object):
        raise FloodWaitError(90)


async def test_telegram_flood_wait_becomes_exact_durable_retry_later() -> None:
    fixed_now = datetime(2026, 7, 25, tzinfo=timezone.utc)
    client = TelegramClient(
        tenant_id=uuid4(),
        installation_id=uuid4(),
        telegram_transport=_FloodingTelegramBoundary(),
        provider_transport=ProviderTransport(now=lambda: fixed_now),
        request_policy=RequestPolicy(
            max_attempts=1,
            max_inline_retry_after_seconds=0,
        ),
        quota_resolver=_quota,
        allow_unlimited_local=False,
    )

    with pytest.raises(RetryLater) as raised:
        await client.get_history(
            dialog_id=7,
            access_hash=None,
            dialog_kind="chat",
        )

    assert raised.value.context["operation"] == "get_history"
    assert raised.value.context["reason"] == RetryReason.RATE_LIMIT.value
    assert raised.value.context["retry_after_seconds"] == 90.0


async def test_telegram_gateway_provider_context_isolated_per_installation() -> None:
    tenant_id, first_id, second_id = uuid4(), uuid4(), uuid4()
    recorder = _Recorder()
    first = TelegramGatewayWorker(
        deps=SimpleNamespace(
            tenant_id=tenant_id,
            installation_id=str(first_id),
        ),
        session="first",
        api_id=1,
        api_hash="hash",
        dialog_index={},
        provider_transport=recorder,
        quota_resolver=_quota,
        allow_unlimited_local=False,
    )
    second = TelegramGatewayWorker(
        deps=SimpleNamespace(
            tenant_id=tenant_id,
            installation_id=str(second_id),
        ),
        session="second",
        api_id=1,
        api_hash="hash",
        dialog_index={},
        provider_transport=recorder,
        quota_resolver=_quota,
        allow_unlimited_local=False,
    )

    async def _done() -> None:
        return None

    await first._execute_telethon("updates.catch_up", _done)
    await second._execute_telethon("updates.catch_up", _done)

    assert [context.operation for context in recorder.contexts] == [
        "updates.catch_up",
        "updates.catch_up",
    ]
    assert [context.installation_id for context in recorder.contexts] == [
        str(first_id),
        str(second_id),
    ]
