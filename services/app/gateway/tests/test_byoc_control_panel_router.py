from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI

from services.app.gateway.byoc_control_panel_router import (
    build_byoc_control_panel_router,
)
from services.platform.runtime.byoc_control_panel_access import (
    ByocControlPanelAccessGrant,
)
from services.platform.runtime.byoc_control_panel_contract import (
    EXAMPLE_CUSTOMER_ID,
    EXAMPLE_DEPLOYMENT_ID,
    build_example_control_panel_state,
)


TENANT_ID = UUID("11111111-1111-4111-8111-111111111111")
ACTOR_ID = UUID("22222222-2222-4222-8222-222222222222")
GENERATED_AT = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)


class _AgentStore:
    def __init__(self):
        self.state = build_example_control_panel_state(generated_at=GENERATED_AT)

    async def list_agents(self, query):
        return self.state.agent_fleet


class _ReceiptStore:
    def __init__(self, name: str):
        self.state = build_example_control_panel_state(generated_at=GENERATED_AT)
        self.name = name

    async def list_receipts(self, query):
        return getattr(self.state, self.name)


def _grant(
    *,
    deployment_id: str = EXAMPLE_DEPLOYMENT_ID,
    customer_id: str = EXAMPLE_CUSTOMER_ID,
    enabled: bool = True,
) -> ByocControlPanelAccessGrant:
    return ByocControlPanelAccessGrant(
        schema_version="fyralis.byoc.control_panel_access_grant.v1",
        tenant_id=TENANT_ID,
        customer_id=customer_id,
        deployment_ids=(deployment_id,),
        role="viewer",
        enabled=enabled,
        granted_at=GENERATED_AT,
        stored_scope="sanitized_control_panel_access_metadata_only",
    )


def _app(
    *,
    inject_auth: bool = True,
    grants: tuple[ByocControlPanelAccessGrant, ...] = (),
) -> FastAPI:
    app = FastAPI()
    app.state.byoc_control_panel_access_grants = grants
    app.state.byoc_agent_registry_store = _AgentStore()
    app.state.byoc_evidence_intake_store = _ReceiptStore("evidence_packages")
    app.state.byoc_preflight_report_intake_store = _ReceiptStore("preflight_reports")
    app.state.byoc_runner_evidence_intake_store = _ReceiptStore("runner_evidence")

    if inject_auth:

        @app.middleware("http")
        async def _inject_auth(request, call_next):
            request.state.auth = SimpleNamespace(
                tenant_id=TENANT_ID,
                actor_id=ACTOR_ID,
            )
            return await call_next(request)

    app.include_router(build_byoc_control_panel_router())
    return app


@pytest.mark.asyncio
async def test_byoc_control_panel_proxy_serves_authorized_state() -> None:
    app = _app(grants=(_grant(),))
    query = (
        f"deployment_id={EXAMPLE_DEPLOYMENT_ID}"
        f"&customer_id={EXAMPLE_CUSTOMER_ID}&recent_limit=5"
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/byoc/control-panel/state?{query}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == "fyralis.byoc.control_panel_state.v1"
    assert payload["stored_scope"] == "sanitized_control_panel_metadata_only"
    assert payload["deployment_id"] == EXAMPLE_DEPLOYMENT_ID
    assert payload["customer_id"] == EXAMPLE_CUSTOMER_ID
    assert payload["agent_fleet"]["result_count"] == 1
    assert "x-fyralis-byoc-read-signature" not in response.text.lower()
    assert "secret_ref" not in response.text.lower()
    assert "install_token" not in response.text.lower()
    assert "payload" not in response.text.lower()


@pytest.mark.asyncio
async def test_byoc_control_panel_proxy_requires_gateway_auth() -> None:
    app = _app(inject_auth=False, grants=(_grant(),))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            f"/byoc/control-panel/state?deployment_id={EXAMPLE_DEPLOYMENT_ID}"
        )

    assert response.status_code == 401
    assert response.json()["detail"]["error"] == "missing_gateway_bearer_auth"


@pytest.mark.asyncio
async def test_byoc_control_panel_proxy_rejects_missing_or_mismatched_grant() -> None:
    missing_app = _app(grants=())
    mismatched_app = _app(grants=(_grant(deployment_id="dep_control02"),))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=missing_app),
        base_url="http://test",
    ) as client:
        missing = await client.get(
            f"/byoc/control-panel/state?deployment_id={EXAMPLE_DEPLOYMENT_ID}"
        )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=mismatched_app),
        base_url="http://test",
    ) as client:
        mismatched = await client.get(
            f"/byoc/control-panel/state?deployment_id={EXAMPLE_DEPLOYMENT_ID}"
        )

    assert missing.status_code == 403
    assert missing.json()["detail"]["errors"] == [
        "control_panel_access: grant_missing"
    ]
    assert mismatched.status_code == 403
    assert mismatched.json()["detail"]["errors"] == [
        "control_panel_access: deployment_not_allowed"
    ]


@pytest.mark.asyncio
async def test_byoc_control_panel_proxy_rejects_disabled_grant() -> None:
    app = _app(grants=(_grant(enabled=False),))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            f"/byoc/control-panel/state?deployment_id={EXAMPLE_DEPLOYMENT_ID}"
        )

    assert response.status_code == 403
    assert response.json()["detail"]["errors"] == [
        "control_panel_access: grant_disabled"
    ]
