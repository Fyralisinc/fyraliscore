from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from services.ingest.connector_runtime.host_services import HostServicesFactory
from services.ingest.source_contract.connector import GrantedAuthority
from services.ingest.source_contract.errors import PermissionDeniedError
from services.ingest.source_contract.host_services import (
    GovernedHttpRequest,
    SecretValue,
)
from services.ingest.source_contract.identity import SlotId
from services.ingest.source_contract.models import SourceRecord


@pytest.mark.asyncio
async def test_host_services_enforce_secret_and_http_grants() -> None:
    calls: list[tuple[object, object]] = []

    async def secret_reader(installation_id, slot):
        calls.append((installation_id, slot))
        return SecretValue.from_text("credential")

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        installation_id = uuid4()
        services = HostServicesFactory(
            http_client=client,
            secret_reader=secret_reader,
        ).build(
            installation_id,
            GrantedAuthority(
                secret_slots=frozenset({"oauth.access_token"}),
                outbound_hosts=frozenset({"api.slack.com"}),
            ),
            connector_id="fyralis/slack",
        )

        value = await services.secrets.resolve(SlotId("oauth.access_token"))
        assert value.reveal_text() == "credential"
        assert calls == [(installation_id, "oauth.access_token")]

        response = await services.http.send(
            GovernedHttpRequest(method="GET", url="https://api.slack.com/test")
        )
        assert response.status_code == 200

        with pytest.raises(PermissionDeniedError):
            await services.secrets.resolve(SlotId("webhook.signing_secret"))
        with pytest.raises(PermissionDeniedError):
            await services.http.send(
                GovernedHttpRequest(method="GET", url="https://example.com/")
            )
        with pytest.raises(PermissionDeniedError):
            await services.http.send(
                GovernedHttpRequest(method="GET", url="http://api.slack.com/")
            )


@pytest.mark.asyncio
async def test_default_mutating_ports_fail_closed() -> None:
    async with httpx.AsyncClient() as client:
        services = HostServicesFactory(http_client=client).build(
            uuid4(), GrantedAuthority(), connector_id="fyralis/example"
        )

        with pytest.raises(PermissionDeniedError):
            await services.raw_emission.emit(
                SourceRecord(native_type="test", payload={})
            )
