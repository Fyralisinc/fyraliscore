from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from services.ingest.connector_conformance.fakes import FakeHostEnvironment
from services.ingest.connector_platform.execution import LegacyExecutionRouter
from services.ingest.connector_platform.legacy_capabilities import LegacyGatewayStream
from services.ingest.connector_platform.legacy_context import (
    LegacyBindingPayload,
    legacy_binding_scope,
    require_legacy_binding,
)
from services.ingest.connector_platform.pilots import build_pilot_composition
from services.ingest.connector_runtime.composition import build_runtime_composition
from services.ingest.connector_runtime.execution import (
    CapabilityExecutionRequest,
    ConnectorCapabilityExecutor,
)
from services.ingest.connector_runtime.host_services import HostServicesFactory
from services.ingest.connector_runtime.legacy import LegacyConnectorAdapter
from services.ingest.connector_runtime.policy import ExecutionMode, RoutingPolicy
from services.ingest.connector_runtime.shadow import InMemoryShadowReportSink
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingest.ingestion.handlers import get_handler
from services.ingest.source_contract.capabilities import GATEWAY_STREAM_V1
from services.ingest.source_contract.capabilities.ingestion import (
    GatewayBatch,
    GatewayOpenRequest,
    GatewayReceiveRequest,
    GatewaySession,
)
from services.ingest.source_contract.connector import GrantedAuthority
from services.ingest.source_contract.host_services import SecretValue
from services.ingest.source_contract.manifest import ConnectorManifest
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    CursorState,
    IdentityInput,
    InstallationRef,
    NormalizationInput,
    SourceRecord,
)


def _install(source: str) -> dict:
    return {
        "id": uuid4(),
        "tenant_id": uuid4(),
        "installation_id": f"{source}-workspace",
        "secret_ref": str(uuid4()),
        "enabled": True,
    }


@pytest.mark.asyncio
async def test_slack_webhook_bridge_uses_existing_hmac_verifier() -> None:
    secret = "signing-secret"
    received_at = datetime.now(timezone.utc).replace(microsecond=0)
    timestamp = str(int(received_at.timestamp()))
    payload = {
        "type": "event_callback",
        "team_id": "T1",
        "event": {
            "type": "message",
            "channel": "C1",
            "ts": "1700000000.000001",
            "text": "hello",
        },
    }
    body = json.dumps(payload, separators=(",", ":")).encode()
    signature = (
        "v0="
        + hmac.new(
            secret.encode(), f"v0:{timestamp}:".encode() + body, hashlib.sha256
        ).hexdigest()
    )

    async def read_secret(_installation_id, slot):
        assert slot == "webhook_signing_secret"
        return SecretValue.from_text(secret)

    async with httpx.AsyncClient() as client:
        router = LegacyExecutionRouter(
            build_pilot_composition(RoutingPolicy(global_mode=ExecutionMode.CONNECTOR)),
            HostServicesFactory(http_client=client, secret_reader=read_secret),
        )

        async def legacy_call():
            raise AssertionError("legacy webhook should not be authoritative")

        result = await router.webhook(
            "slack",
            _install("slack"),
            BoundedWebhookRequest(
                body=body,
                headers={
                    "X-Slack-Request-Timestamp": timestamp,
                    "X-Slack-Signature": signature,
                },
                received_at=received_at,
            ),
            legacy_call,
        )

    assert result.events[0].external_installation_id == "T1"
    assert result.events[0].record.payload == payload


@pytest.mark.asyncio
async def test_identity_normalization_and_poll_have_shadow_parity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install = _install("notion")
    record = SourceRecord(
        native_type="page",
        payload={
            "object": "page",
            "id": "page-1",
            "created_time": "2025-01-01T00:00:00Z",
            "last_edited_time": "2025-01-01T00:00:00Z",
            "properties": {},
        },
    )
    input_value = IdentityInput(
        record=record,
        external_installation_id="workspace-1",
        ingress_kind="poll",
    )
    sink = InMemoryShadowReportSink()
    calls = 0

    async def fetcher(_install, _shard, _cursor):
        nonlocal calls
        calls += 1
        return FetchResult(
            records=[],
            next_cursor={
                "stack": [],
                "items_seen": 0,
                "last_edited_at": None,
                "seeded": True,
            },
            end_of_data=True,
        )

    monkeypatch.setitem(FETCHER_DISPATCH, "notion", fetcher)

    async def provider(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"results": [], "has_more": False, "next_cursor": None},
        )

    async def read_secret(_installation, _slot):
        return SecretValue.from_text("notion-token")

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        router = LegacyExecutionRouter(
            build_pilot_composition(RoutingPolicy(global_mode=ExecutionMode.SHADOW)),
            HostServicesFactory(http_client=client, secret_reader=read_secret),
            shadow_sink=sink,
        )

        async def legacy_identity() -> str:
            return "notion:page:page-1"

        identity = await router.identity(
            "notion", install, input_value, legacy_identity
        )

        normalization_input = NormalizationInput(
            record=record,
            ingress_kind="poll",
        )

        async def legacy_normalize():
            return await get_handler("notion:object")(record.payload, {})

        draft = await router.normalize(
            "notion", install, normalization_input, legacy_normalize
        )
        page = await router.poll(
            "notion",
            install,
            {"shard_kind": "notion_page_tree", "workspace_id": "workspace-1"},
            None,
            shadow_safe=True,
        )

    assert identity == "notion:page:page-1"
    assert draft.external_id == identity
    assert page.next_cursor == {
        "stack": [],
        "items_seen": 0,
        "last_edited_at": None,
        "seeded": True,
    }
    assert calls == 1
    assert len(sink.reports) == 3
    assert all(report.matches for report in sink.reports)


class _GatewayDriver:
    def __init__(self) -> None:
        self.closed = False

    async def open(self, request: GatewayOpenRequest) -> GatewaySession:
        return GatewaySession(
            session_id="legacy-session", resume_state=request.resume_state
        )

    async def receive(self, request: GatewayReceiveRequest) -> GatewayBatch:
        return GatewayBatch(
            records=(SourceRecord(native_type="message", payload={"id": "1"}),),
            resume_state=CursorState(schema_version=1, payload={"sequence": 1}),
        )

    async def close(self, session: GatewaySession) -> None:
        assert session.session_id == "legacy-session"
        self.closed = True


def _gateway_manifest() -> ConnectorManifest:
    return ConnectorManifest.model_validate(
        {
            "apiVersion": "sources.fyralis.io/v1alpha1",
            "kind": "SourceConnector",
            "metadata": {
                "id": "fyralis/gateway-test",
                "source": "telegram",
                "displayName": "Gateway test",
                "version": "0.1.0",
                "owner": "ingestion",
            },
            "spec": {
                "contract": ">=1.0,<2.0",
                "implementation": "tests.gateway:create",
                "capabilities": [{"id": GATEWAY_STREAM_V1.ref.id, "version": 1}],
                "ingressKinds": ["gateway"],
                "permissions": {},
                "trust": {"maximumTier": "attested_agent"},
            },
        }
    )


@pytest.mark.asyncio
async def test_gateway_driver_is_resolved_through_registry_capability() -> None:
    manifest = _gateway_manifest()
    adapter = LegacyConnectorAdapter(
        manifest,
        {
            GATEWAY_STREAM_V1.ref: lambda _context: LegacyGatewayStream(
                require_legacy_binding()
            )
        },
    )
    composition = build_runtime_composition(
        (adapter.candidate((GATEWAY_STREAM_V1,)),),
        policy=RoutingPolicy(global_mode=ExecutionMode.CONNECTOR),
    )
    environment = FakeHostEnvironment()
    installation = InstallationRef(
        id=uuid4(),
        tenant_id=uuid4(),
        connector_id=manifest.connector_id,
        generation=1,
    )
    driver = _GatewayDriver()

    async def legacy_call() -> GatewayBatch:
        raise AssertionError("legacy call should not be authoritative")

    async def connector_call(capability, operation) -> GatewayBatch:
        session = await capability.open(GatewayOpenRequest(), operation)
        batch = await capability.receive(
            GatewayReceiveRequest(session=session), operation
        )
        await capability.close(session, operation)
        return batch

    request = CapabilityExecutionRequest(
        installation=installation,
        source="telegram",
        authority=GrantedAuthority(),
        services=environment.services,
        capability=GATEWAY_STREAM_V1,
        connector_call=connector_call,
        legacy_call=legacy_call,
        deadline=datetime.now(timezone.utc) + timedelta(seconds=5),
    )
    with legacy_binding_scope(
        LegacyBindingPayload(
            install=SimpleNamespace(),
            external_installation_id="telegram-1",
            gateway_driver=driver,
        )
    ):
        batch = await ConnectorCapabilityExecutor(
            composition.registry, composition.routing
        ).execute(request)

    assert batch.records[0].payload == {"id": "1"}
    assert driver.closed
