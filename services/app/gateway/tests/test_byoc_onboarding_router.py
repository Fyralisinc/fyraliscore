from __future__ import annotations

import re
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI

from services.app.gateway.byoc_onboarding_router import (
    _ensure_rehearsal_actor,
    build_byoc_onboarding_router,
)
from services.platform.runtime.byoc_onboarding_intents import (
    InMemoryOnboardingIntentStore,
)


class _RecordingSecretStore:
    def __init__(self) -> None:
        self.values: list[tuple[str, str]] = []

    async def put(self, plaintext, *, label, tenant_id):
        self.values.append((label, str(plaintext)))
        return f"secret-ref:{label}"


def _app() -> tuple[FastAPI, InMemoryOnboardingIntentStore]:
    store = InMemoryOnboardingIntentStore()
    app = FastAPI()
    app.include_router(build_byoc_onboarding_router(store=store))
    return app, store


def _gateway_app(gateway_pool, secret_store=None) -> FastAPI:
    app = FastAPI()
    app.state.pool = gateway_pool
    if secret_store is not None:
        app.state.secret_store = secret_store
    app.include_router(build_byoc_onboarding_router(store=InMemoryOnboardingIntentStore()))
    return app


@pytest.mark.asyncio
async def test_design_partner_plan_selection_creates_sanitized_intent() -> None:
    app, store = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/platform/onboarding/intents",
            json={
                "plan_code": "design_partner_byoc_pilot",
                "procurement_channel": "design_partner",
                "entrypoint": "get_fyralis",
            },
        )

    assert response.status_code == 201
    payload = response.json()
    rendered = response.text.lower()
    assert payload["schema_version"] == "fyralis.platform.onboarding_intent.v1"
    assert re.match(r"^ofi_[0-9a-f]{32}$", payload["intent_id"])
    assert payload["plan_code"] == "design_partner_byoc_pilot"
    assert payload["status"] == "draft"
    assert payload["customer_id"] is None
    assert payload["deployment_id"] is None
    assert payload["stored_scope"] == "sanitized_onboarding_metadata_only"
    assert store.events[0]["event_type"] == "plan_selected"
    assert "secret" not in rendered
    assert "token" not in rendered
    assert "credential" not in rendered


@pytest.mark.asyncio
async def test_enterprise_plan_selection_is_not_implemented_yet() -> None:
    app, _store = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/platform/onboarding/intents",
            json={
                "plan_code": "enterprise_byoc",
                "procurement_channel": "sales",
                "entrypoint": "get_fyralis",
            },
        )

    assert response.status_code == 501
    assert response.json()["detail"]["error"] == "unsupported_onboarding_plan"


@pytest.mark.asyncio
async def test_slack_rehearsal_is_not_enabled_by_default() -> None:
    app, _store = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/platform/onboarding/slack/rehearsal/prepare"
        )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "source_rehearsal_not_enabled"


@pytest.mark.asyncio
async def test_generic_source_prepare_returns_actionable_inputs(
    gateway_pool,
    monkeypatch,
) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    monkeypatch.setenv("FYRALIS_SOURCE_REHEARSAL_ENABLED", "1")
    monkeypatch.setenv("COMPANY_OS_TENANT_ID", str(tenant_id))
    monkeypatch.setenv("COMPANY_OS_CEO_ACTOR_ID", str(actor_id))

    app = _gateway_app(gateway_pool, _RecordingSecretStore())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/platform/onboarding/sources/hibob/rehearsal/prepare"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "hibob"
    assert payload["authorization_mode"] == "customer_local_provider_refs"
    assert payload["required_inputs"] == ["company_id", "service_user_token"]
    assert "webhook_secret" in payload["optional_inputs"]
    assert payload["status"]["next_action"] == (
        "Submit the required HiBob connection details."
    )


@pytest.mark.asyncio
async def test_generic_source_finalize_stores_refs_and_emits_trigger(
    gateway_pool,
    monkeypatch,
) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    secret_store = _RecordingSecretStore()
    monkeypatch.setenv("FYRALIS_SOURCE_REHEARSAL_ENABLED", "1")
    monkeypatch.setenv("COMPANY_OS_TENANT_ID", str(tenant_id))
    monkeypatch.setenv("COMPANY_OS_CEO_ACTOR_ID", str(actor_id))

    app = _gateway_app(gateway_pool, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/platform/onboarding/sources/hibob/rehearsal/finalize",
            json={
                "inputs": {
                    "company_id": "hibob-company-1",
                    "service_user_token": "super-secret-token",
                    "webhook_secret": "webhook-super-secret",
                }
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["installation_id"] == "hibob-company-1"
    assert payload["status"]["installed"] is True
    assert payload["status"]["trigger_count"] == 1
    assert sorted(label for label, _value in secret_store.values) == [
        "hibob_service_user_token",
        "hibob_webhook_secret",
    ]

    install = await gateway_pool.fetchrow(
        """
        SELECT installation_id, secret_ref, enabled
          FROM provider_installations
         WHERE tenant_id = $1 AND provider = 'hibob'
        """,
        tenant_id,
    )
    assert install["installation_id"] == "hibob-company-1"
    assert install["secret_ref"] == "secret-ref:hibob_webhook_secret"
    assert install["enabled"] is True

    trigger_payload = await gateway_pool.fetchval(
        """
        SELECT payload::text
          FROM onboarding_triggers
         WHERE tenant_id = $1 AND source = 'hibob'
        """,
        tenant_id,
    )
    assert "hibob-company-1" in trigger_payload
    assert "secret-ref:hibob_service_user_token" in trigger_payload
    assert "super-secret-token" not in trigger_payload
    assert "webhook-super-secret" not in trigger_payload


@pytest.mark.asyncio
async def test_rehearsal_actor_gets_tenant_admin_grant(gateway_pool) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()

    await _ensure_rehearsal_actor(
        gateway_pool,
        tenant_id=tenant_id,
        actor_id=actor_id,
    )

    row = await gateway_pool.fetchrow(
        """
        SELECT role, entity_type, entity_id, revoked_at
          FROM actor_roles
         WHERE tenant_id = $1
           AND actor_id = $2
           AND role = 'admin'
        """,
        tenant_id,
        actor_id,
    )

    assert row is not None
    assert row["entity_type"] == "tenant"
    assert row["entity_id"] is None
    assert row["revoked_at"] is None


@pytest.mark.asyncio
async def test_design_partner_intake_mints_customer_tenant_and_deployment_ids() -> None:
    app, store = _app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post(
            "/platform/onboarding/intents",
            json={
                "plan_code": "design_partner_byoc_pilot",
                "procurement_channel": "design_partner",
                "entrypoint": "get_fyralis",
            },
        )
        intent_id = created.json()["intent_id"]

        response = await client.post(
            f"/platform/onboarding/intents/{intent_id}/design-partner-intake",
            json={
                "company_name": "Acme Finance",
                "setup_owner_email": "Platform-Owner@Acme.Example",
                "target_cloud": "aws",
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "workspace_created"
    assert re.match(r"^cus_[0-9a-f]{16}$", payload["customer_id"])
    assert re.match(r"^dep_[0-9a-f]{16}$", payload["deployment_id"])
    assert payload["tenant_id"]
    assert payload["company_name"] == "Acme Finance"
    assert payload["setup_owner_email"] == "platform-owner@acme.example"
    assert payload["target_cloud"] == "aws"
    assert [event["event_type"] for event in store.events] == [
        "plan_selected",
        "design_partner_intake_submitted",
        "workspace_created",
    ]


@pytest.mark.asyncio
async def test_design_partner_intake_requires_existing_intent() -> None:
    app, _store = _app()
    missing_id = "ofi_00000000000000000000000000000000"
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            f"/platform/onboarding/intents/{missing_id}/design-partner-intake",
            json={
                "company_name": "Acme Finance",
                "setup_owner_email": "platform-owner@acme.example",
                "target_cloud": "aws",
            },
        )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "onboarding_intent_not_found"
