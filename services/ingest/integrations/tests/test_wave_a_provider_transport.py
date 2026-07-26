"""Contract tests for the Wave-A provider-client transport boundary."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx
import pytest

from lib.shared.provider_transport import (
    ProviderTransport,
    QuotaDecision,
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
    RetryLater,
)
from services.ingest.integrations.discord.client import DiscordClient
from services.ingest.integrations.discord.commands import (
    register_fyralis_command,
)
from services.ingest.integrations.discord.gateway.client import (
    DiscordGatewayClient,
)
from services.ingest.integrations.discord.oauth import (
    _exchange_code_for_tokens as exchange_discord_code,
)
from services.ingest.integrations.gmail.client import (
    GMAIL_METADATA_SCOPE,
    GmailClient,
    GoogleHttpClient,
)
from services.ingest.integrations.gmail.dwd import (
    DwdTokenMinter,
    ServiceAccountKey,
)
from services.ingest.integrations.gmail.pubsub import PubsubAdmin
from services.ingest.integrations.github.client import GithubClient
from services.ingest.integrations.slack.client import SlackClient
from services.ingest.integrations.slack.oauth import (
    _exchange_code_for_tokens as exchange_slack_code,
)
from services.ingest.integrations.provider_transport_runtime import (
    get_provider_transport_runtime,
    reset_provider_transport_runtime_for_tests,
)
from services.ingest.source_contract.quota_contract import (
    PROVIDER_QUOTA_CONFIG_SCHEMA_VERSION,
    PROVIDER_QUOTA_CONTRACT,
)


def _quota(
    source: str,
    operation: str,
    tenant_id: str | None,
    installation_id: str | None,
    dimensions,
) -> tuple[QuotaRequirement, ...]:
    del dimensions
    assert tenant_id is not None
    assert installation_id is not None
    return (
        QuotaRequirement(
            scope="installation",
            bucket_key=f"{source}:{operation}:{installation_id}",
            capacity=10,
            refill_per_second=1.0,
        ),
    )


def _runtime_quota_payload(
    rule: dict[str, object],
) -> dict[str, object]:
    return {
        "schema_version": PROVIDER_QUOTA_CONFIG_SCHEMA_VERSION,
        "catalog_sha256": PROVIDER_QUOTA_CONTRACT.catalog_sha256,
        "limits": {
            identity.reference: [dict(rule)]
            for identity in PROVIDER_QUOTA_CONTRACT.operations
        },
    }


def _tenant_quota(
    source: str,
    operation: str,
    tenant_id: str | None,
    installation_id: str | None,
    dimensions,
) -> tuple[QuotaRequirement, ...]:
    del dimensions
    assert tenant_id is not None
    assert installation_id is None
    return (
        QuotaRequirement(
            scope="tenant",
            bucket_key=f"{source}:{operation}:{tenant_id}",
            capacity=10,
            refill_per_second=1.0,
        ),
    )


class _RecordingTransport:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []

    async def execute(self, context, policy, call):
        assert isinstance(policy, RequestPolicy)
        self.contexts.append(context)
        return await call()


class _Minter:
    async def mint(
        self,
        *,
        user_email: str,
        scopes: list[str],
        **_context,
    ) -> str:
        del user_email, scopes
        return "google-token"

    def invalidate(self, *, user_email: str, scopes: list[str]) -> None:
        del user_email, scopes


async def test_slack_context_is_exact_and_operation_is_method() -> None:
    tenant_id = uuid4()
    row_id = uuid4()
    recorder = _RecordingTransport()
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json={"ok": True}),
        ),
    )
    client = SlackClient(
        pool=None,  # type: ignore[arg-type]
        secret_store=None,
        tenant_id=tenant_id,
        installation_row_id=row_id,
        team_id="T1",
        base_url="https://slack.test/api",
        http_client=http,
        provider_transport=recorder,
        quota_resolver=_quota,
    )
    client._bot_token_cache.set("xoxb-test", ttl_seconds=float("inf"))
    try:
        await client.users_info("U1")
    finally:
        await client.aclose()

    [context] = recorder.contexts
    assert (
        context.source,
        context.operation,
        context.tenant_id,
        context.installation_id,
    ) == ("slack", "users.info", str(tenant_id), str(row_id))
    assert context.quota_requirements[0].cost == 1


async def test_gmail_list_and_child_hydration_are_charged_separately() -> None:
    tenant_id = str(uuid4())
    row_id = str(uuid4())
    recorder = _RecordingTransport()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/messages"):
            return httpx.Response(200, json={"messages": [{"id": "m1"}]})
        return httpx.Response(200, json={"id": "m1", "payload": {}})

    httpx_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    google = GoogleHttpClient(
        _Minter(),  # type: ignore[arg-type]
        http_client=httpx_client,
        tenant_id=tenant_id,
        installation_id=row_id,
        provider_transport=recorder,
        quota_resolver=_quota,
    )
    gmail = GmailClient(google, base_url="https://gmail.test/gmail/v1")
    try:
        await gmail.messages_list(
            user_email="person@example.test",
            scope=GMAIL_METADATA_SCOPE,
        )
        await gmail.get_message(
            user_email="person@example.test",
            scope=GMAIL_METADATA_SCOPE,
            message_id="m1",
        )
    finally:
        await httpx_client.aclose()

    assert [item.operation for item in recorder.contexts] == [
        "messages.list",
        "messages.get",
    ]
    assert all(item.source == "gmail" for item in recorder.contexts)
    assert all(item.tenant_id == tenant_id for item in recorder.contexts)
    assert all(item.installation_id == row_id for item in recorder.contexts)
    assert (
        recorder.contexts[0].quota_requirements[0].bucket_key
        != recorder.contexts[1].quota_requirements[0].bucket_key
    )


async def test_github_token_mint_has_exact_registered_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    row_id = uuid4()
    recorder = _RecordingTransport()
    monkeypatch.setattr(
        "services.ingest.integrations.github.client.mint_app_jwt",
        lambda: "app-jwt",
    )
    response = {
        "token": "installation-token",
        "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
    }
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(201, json=response),
        ),
    )
    client = GithubClient(
        pool=None,  # type: ignore[arg-type]
        http_client=http,
        api_base_url="https://github.test",
        provider_transport=recorder,
        quota_resolver=_quota,
    )
    await client.register_installation_context(
        "42",
        tenant_id=tenant_id,
        installation_row_id=row_id,
    )
    try:
        assert await client.mint_installation_token("42") == "installation-token"
    finally:
        await client.aclose()

    [context] = recorder.contexts
    assert (
        context.source,
        context.operation,
        context.tenant_id,
        context.installation_id,
    ) == ("github", "installation_token.mint", str(tenant_id), str(row_id))


async def test_discord_route_context_is_exact() -> None:
    tenant_id = uuid4()
    row_id = uuid4()
    recorder = _RecordingTransport()
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json={"id": "C1"}),
        ),
    )
    client = DiscordClient(
        pool=None,  # type: ignore[arg-type]
        secret_store=None,
        tenant_id=tenant_id,
        installation_row_id=row_id,
        guild_id="G1",
        bot_token="bot",
        base_url="https://discord.test/api/v10",
        http_client=http,
        provider_transport=recorder,
        quota_resolver=_quota,
    )
    try:
        await client.get_channel("C1")
    finally:
        await client.aclose()

    [context] = recorder.contexts
    assert (
        context.source,
        context.operation,
        context.tenant_id,
        context.installation_id,
    ) == ("discord", "/channels/{channel_id}", str(tenant_id), str(row_id))


async def test_discord_gateway_discovery_is_transport_charged() -> None:
    recorder = _RecordingTransport()
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={"url": "wss://gateway.discord.test"},
            ),
        ),
    )
    client = DiscordGatewayClient(
        bot_token="bot",
        application_id="app-1",
        dispatch_handler=lambda _frame: None,  # type: ignore[arg-type]
        http_client=http,
        gateway_bot_url="https://discord.test/api/v10/gateway/bot",
        provider_transport=recorder,
        quota_resolver=lambda source, operation, tenant, installation, dimensions: (
            QuotaRequirement(
                scope="app",
                bucket_key=f"{source}:{operation}:{dimensions['app']}",
                capacity=10,
                refill_per_second=1,
            ),
        ),
    )
    try:
        url = await client._fetch_gateway_url()
    finally:
        await client.aclose()

    assert url == "wss://gateway.discord.test?v=10&encoding=json"
    [context] = recorder.contexts
    assert (context.source, context.operation) == ("discord", "/gateway/bot")


async def test_slack_oauth_exchange_is_honestly_tenant_scoped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    recorder = _RecordingTransport()
    monkeypatch.setenv("SLACK_CLIENT_ID", "slack-app")
    monkeypatch.setenv("SLACK_CLIENT_SECRET", "slack-secret")
    monkeypatch.setenv(
        "SLACK_REDIRECT_URI",
        "https://fyralis.test/integrations/slack/callback",
    )
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={"ok": True, "team": {"id": "T1"}},
            ),
        ),
    )
    try:
        response = await exchange_slack_code(
            "oauth-code",
            tenant_id=tenant_id,
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_tenant_quota,
        )
    finally:
        await http.aclose()

    assert response["ok"] is True
    [context] = recorder.contexts
    assert (
        context.source,
        context.operation,
        context.tenant_id,
        context.installation_id,
    ) == ("slack", "oauth.v2.access", str(tenant_id), None)


async def test_discord_oauth_and_command_registration_bind_at_correct_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    installation_id = uuid4()
    recorder = _RecordingTransport()
    monkeypatch.setenv("DISCORD_CLIENT_ID", "discord-app")
    monkeypatch.setenv("DISCORD_CLIENT_SECRET", "discord-secret")
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "discord-bot")
    monkeypatch.setenv(
        "DISCORD_REDIRECT_URI",
        "https://fyralis.test/integrations/discord/callback",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth2/token"):
            return httpx.Response(
                200,
                json={
                    "access_token": "token",
                    "guild": {"id": "G1"},
                },
            )
        return httpx.Response(201, json={"id": "command-1"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        await exchange_discord_code(
            "oauth-code",
            tenant_id=tenant_id,
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_tenant_quota,
        )
        await register_fyralis_command(
            "discord-app",
            tenant_id=tenant_id,
            installation_id=installation_id,
            guild_id="G1",
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_quota,
        )
    finally:
        await http.aclose()

    oauth_context, command_context = recorder.contexts
    assert (
        oauth_context.operation,
        oauth_context.tenant_id,
        oauth_context.installation_id,
    ) == ("/oauth2/token", str(tenant_id), None)
    assert (
        command_context.operation,
        command_context.tenant_id,
        command_context.installation_id,
    ) == (
        "/applications/{application_id}/commands",
        str(tenant_id),
        str(installation_id),
    )


async def test_gmail_dwd_exchange_has_exact_installation_context() -> None:
    tenant_id = str(uuid4())
    installation_id = str(uuid4())
    recorder = _RecordingTransport()
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={"access_token": "google-token", "expires_in": 3600},
            ),
        ),
    )
    minter = DwdTokenMinter(
        ServiceAccountKey(
            client_email="svc@project.test",
            private_key_pem="unused",
            private_key_id="key-1",
            token_uri="https://oauth.test/token",
        ),
        http_client=http,
        provider_transport=recorder,
        quota_resolver=_quota,
    )
    try:
        token, ttl = await minter._exchange(
            "signed-assertion",
            source="gmail",
            tenant_id=tenant_id,
            installation_id=installation_id,
            user_email="admin@example.test",
            quota_dimensions={"project": "project-1"},
            require_tenant_installation=True,
        )
    finally:
        await http.aclose()

    assert (token, ttl) == ("google-token", 3600)
    [context] = recorder.contexts
    assert (
        context.source,
        context.operation,
        context.tenant_id,
        context.installation_id,
    ) == ("gmail", "dwd.token.exchange", tenant_id, installation_id)


async def test_gmail_pubsub_crud_is_transport_charged_per_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    installation_id = uuid4()
    recorder = _RecordingTransport()
    monkeypatch.setenv("GMAIL_PUBSUB_PROJECT_ID", "project-1")
    monkeypatch.setenv(
        "GMAIL_PUBSUB_PUSH_ENDPOINT",
        "https://fyralis.test/webhooks/gmail",
    )
    monkeypatch.setenv(
        "GMAIL_PUBSUB_PUSH_OIDC_SA",
        "push@project-1.iam.gserviceaccount.com",
    )

    class _PubsubMinter:
        service_account_email = "svc@project-1.iam.gserviceaccount.com"

        async def mint(self, **kwargs) -> str:
            assert kwargs["tenant_id"] == str(tenant_id)
            assert kwargs["installation_id"] == str(installation_id)
            return "pubsub-token"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(":getIamPolicy"):
            return httpx.Response(200, json={"bindings": []})
        return httpx.Response(200, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    admin = PubsubAdmin(
        _PubsubMinter(),  # type: ignore[arg-type]
        http_client=http,
        provider_transport=recorder,
        quota_resolver=_quota,
    )
    try:
        await admin.provision(
            tenant_id,
            installation_id=installation_id,
        )
        await admin.teardown(
            tenant_id,
            installation_id=installation_id,
        )
    finally:
        await http.aclose()

    assert [context.operation for context in recorder.contexts] == [
        "pubsub.topic.create",
        "pubsub.iam.get",
        "pubsub.iam.set",
        "pubsub.subscription.create",
        "pubsub.subscription.delete",
        "pubsub.topic.delete",
    ]
    assert all(
        context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )


class _CooldownCoordinator:
    def __init__(self) -> None:
        self.cooldown = False
        self.reported: list[float] = []

    async def acquire_many(self, requirements):
        assert requirements
        if self.cooldown:
            return QuotaDecision.deny(
                retry_after_seconds=5.0,
                blocked_scope="installation",
            )
        return QuotaDecision.allow()

    async def report_cooldown(
        self,
        requirements,
        *,
        retry_after_seconds: float,
    ) -> None:
        assert requirements
        self.cooldown = True
        self.reported.append(retry_after_seconds)

    async def report_success(self, requirements) -> None:
        assert requirements

    async def report_failure(self, requirements) -> None:
        assert requirements


async def test_shared_cooldown_blocks_next_http_attempt_and_retry_later_propagates() -> (
    None
):
    coordinator = _CooldownCoordinator()
    transport = ProviderTransport(quota_coordinator=coordinator)
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, headers={"Retry-After": "5"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = SlackClient(
        pool=None,  # type: ignore[arg-type]
        secret_store=None,
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        team_id="T1",
        base_url="https://slack.test/api",
        http_client=http,
        provider_transport=transport,
        request_policy=RequestPolicy(
            max_attempts=1,
            timeout_seconds=1,
            max_elapsed_seconds=1,
            max_quota_wait_seconds=0,
        ),
        quota_resolver=_quota,
    )
    client._bot_token_cache.set("xoxb-test", ttl_seconds=float("inf"))
    try:
        with pytest.raises(RetryLater) as first:
            await client.users_info("U1")
        with pytest.raises(RetryLater) as second:
            await client.users_info("U1")
    finally:
        await client.aclose()

    assert calls == 1, "shared cooldown must block before a second HTTP attempt"
    assert coordinator.reported == [5.0]
    assert first.value.request_context.operation == "users.info"
    assert second.value.blocked_scope == "installation"


async def test_production_runtime_uses_only_declared_quota_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_provider_transport_runtime_for_tests()
    monkeypatch.setenv("REDIS_URL", "redis://provider-transport.invalid/0")
    payload = _runtime_quota_payload(
        {
            "scope": "global",
            "identity": "global",
            "capacity": 17,
            "refill_per_second": 2.5,
            "cost": 3,
            "evidence_ref": "https://provider.test/docs/quota-policy",
            "verified_on": "2025-01-01",
        }
    )
    limits = payload["limits"]
    assert isinstance(limits, dict)
    gmail_reference = PROVIDER_QUOTA_CONTRACT.reference_for(
        "gmail",
        "messages.get",
    )
    limits[gmail_reference][0].update(
        {"scope": "user", "identity": "user"},
    )
    monkeypatch.setenv("FYRALIS_PROVIDER_QUOTAS_JSON", json.dumps(payload))
    runtime = get_provider_transport_runtime(required=True)
    assert runtime is not None
    try:
        [requirement] = runtime.quota_resolver(
            "gmail",
            "messages.get",
            "tenant-1",
            "install-1",
            {"user": "person@example.test"},
        )
        assert (
            requirement.scope,
            requirement.capacity,
            requirement.refill_per_second,
            requirement.cost,
        ) == ("user", 17, 2.5, 3)
    finally:
        await runtime.aclose()
        reset_provider_transport_runtime_for_tests()


def test_runtime_rejects_undeclared_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_provider_transport_runtime_for_tests()
    payload = _runtime_quota_payload(
        {
            "scope": "global",
            "identity": "global",
            "capacity": 1,
            "refill_per_second": 1,
            "evidence_ref": "https://provider.test/docs/quota-policy",
            "verified_on": "2025-01-01",
        }
    )
    limits = payload["limits"]
    assert isinstance(limits, dict)
    limits[f"qop_v1_{'0' * 64}"] = next(iter(limits.values()))
    monkeypatch.setenv("REDIS_URL", "redis://provider-transport.invalid/0")
    monkeypatch.setenv("FYRALIS_PROVIDER_QUOTAS_JSON", json.dumps(payload))
    with pytest.raises(RuntimeError, match="undeclared operation references"):
        get_provider_transport_runtime(required=True)


def test_runtime_rejects_missing_required_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_provider_transport_runtime_for_tests()
    payload = _runtime_quota_payload(
        {
            "scope": "global",
            "identity": "global",
            "capacity": 1,
            "refill_per_second": 1,
            "evidence_ref": "https://provider.test/docs/quota-policy",
            "verified_on": "2025-01-01",
        }
    )
    limits = payload["limits"]
    assert isinstance(limits, dict)
    del limits[PROVIDER_QUOTA_CONTRACT.reference_for("discord", "/gateway/bot")]
    monkeypatch.setenv("REDIS_URL", "redis://provider-transport.invalid/0")
    monkeypatch.setenv("FYRALIS_PROVIDER_QUOTAS_JSON", json.dumps(payload))
    with pytest.raises(
        RuntimeError,
        match="missing required contract operations",
    ):
        get_provider_transport_runtime(required=True)


def test_production_client_fails_closed_without_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "production")
    with pytest.raises(RuntimeError, match="requires ProviderTransport"):
        SlackClient(
            pool=None,  # type: ignore[arg-type]
            secret_store=None,
            tenant_id=uuid4(),
            installation_row_id=uuid4(),
            team_id="T1",
        )
