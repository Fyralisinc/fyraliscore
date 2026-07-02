from __future__ import annotations

import re

import httpx
import pytest
from fastapi import FastAPI

from services.app.gateway.byoc_onboarding_router import build_byoc_onboarding_router
from services.platform.runtime.byoc_onboarding_intents import (
    InMemoryOnboardingIntentStore,
)


def _app() -> tuple[FastAPI, InMemoryOnboardingIntentStore]:
    store = InMemoryOnboardingIntentStore()
    app = FastAPI()
    app.include_router(build_byoc_onboarding_router(store=store))
    return app, store


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
