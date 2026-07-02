"""Gateway-route wiring tests for the Gmail management endpoints.

`gmail/status_api.py` (`get_gmail_status`) and `gmail/uninstall.py`
(`uninstall_install` / `stop_mailbox`) were fully implemented but
orphaned — no router mounted them (their docstrings named routes that
didn't exist). `gmail/oauth.py` now exposes them on the already-mounted
Gmail connect router:

    GET  /integrations/gmail/status        -> get_gmail_status
    POST /integrations/gmail/uninstall      -> uninstall_install
    POST /integrations/gmail/mailbox/stop   -> stop_mailbox

These tests pin that wiring: auth is enforced, request bodies are
validated, and each route delegates to the right function with the
parsed args. The underlying functions touch RLS-scoped tables + Google
APIs, so they're stubbed here — this is a route-wiring guard, not a
re-test of their internals.
"""
from __future__ import annotations

from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI

from lib.observability import counter, reset_default_for_tests
from lib.shared.product_workflow_metrics import (
    PRODUCT_WORKFLOW_EVENT_OUTCOMES,
    PRODUCT_WORKFLOW_EVENTS,
    PRODUCT_WORKFLOWS,
)


pytestmark = pytest.mark.asyncio


def _events():
    return counter(
        "product_workflow_events_total",
        "lookup",
        ("workflow", "event", "outcome"),
        allowed_label_values={
            "workflow": PRODUCT_WORKFLOWS,
            "event": PRODUCT_WORKFLOW_EVENTS,
            "outcome": PRODUCT_WORKFLOW_EVENT_OUTCOMES,
        },
    )


@pytest.fixture(autouse=True)
def _clean_metrics():
    reset_default_for_tests()
    yield
    reset_default_for_tests()


def _make_app(*, with_auth: bool, tenant_id: UUID | None = None) -> FastAPI:
    from services.ingest.integrations.gmail.oauth import router

    app = FastAPI()
    if with_auth:
        @app.middleware("http")
        async def _inject_auth(request, call_next):  # type: ignore[no-untyped-def]
            class _A:
                pass

            a = _A()
            a.tenant_id = tenant_id
            request.state.auth = a
            return await call_next(request)

    app.include_router(router)
    return app


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    )


# ---------------------------------------------------------------------
# GET /integrations/gmail/status
# ---------------------------------------------------------------------
async def test_status_requires_auth() -> None:
    app = _make_app(with_auth=False)
    async with _client(app) as c:
        r = await c.get("/integrations/gmail/status")
    assert r.status_code == 401


async def test_status_returns_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.ingest.integrations.gmail import oauth as gmail_oauth

    tenant = uuid4()
    seen: dict = {}

    async def _fake_status(*, tenant_id):
        seen["tenant_id"] = tenant_id
        return {"connected": True, "installation_id": "inst-1", "watches": {"total": 3}}

    monkeypatch.setattr(gmail_oauth, "get_gmail_status", _fake_status)

    app = _make_app(with_auth=True, tenant_id=tenant)
    async with _client(app) as c:
        r = await c.get("/integrations/gmail/status")

    assert r.status_code == 200, r.text
    assert r.json() == {"connected": True, "installation_id": "inst-1", "watches": {"total": 3}}
    assert seen["tenant_id"] == tenant  # tenant came from request.state.auth
    assert (
        _events().get(
            workflow="source_onboarding",
            event="source_status_checked",
            outcome="success",
        )
        == 1
    )


async def test_status_records_not_found_for_disconnected_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.gmail import oauth as gmail_oauth

    async def _fake_status(*, tenant_id):
        return {"connected": False}

    monkeypatch.setattr(gmail_oauth, "get_gmail_status", _fake_status)

    app = _make_app(with_auth=True, tenant_id=uuid4())
    async with _client(app) as c:
        r = await c.get("/integrations/gmail/status")

    assert r.status_code == 200, r.text
    assert r.json() == {"connected": False}
    assert (
        _events().get(
            workflow="source_onboarding",
            event="source_status_checked",
            outcome="not_found",
        )
        == 1
    )


# ---------------------------------------------------------------------
# POST /integrations/gmail/uninstall
# ---------------------------------------------------------------------
async def test_uninstall_requires_installation_id(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _make_app(with_auth=True, tenant_id=uuid4())
    async with _client(app) as c:
        r = await c.post("/integrations/gmail/uninstall", json={})
    assert r.status_code == 400
    assert "gmail_installation_id" in r.text


async def test_uninstall_rejects_non_uuid(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _make_app(with_auth=True, tenant_id=uuid4())
    async with _client(app) as c:
        r = await c.post(
            "/integrations/gmail/uninstall",
            json={"gmail_installation_id": "not-a-uuid"},
        )
    assert r.status_code == 400
    assert "must be a UUID" in r.text


async def test_uninstall_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.ingest.integrations.gmail import oauth as gmail_oauth

    tenant = uuid4()
    install_id = uuid4()
    seen: dict = {}

    async def _fake_uninstall(*, tenant_id, gmail_installation_id, actor_email=None):
        seen.update(
            tenant_id=tenant_id,
            gmail_installation_id=gmail_installation_id,
            actor_email=actor_email,
        )

    monkeypatch.setattr(gmail_oauth, "uninstall_install", _fake_uninstall)

    app = _make_app(with_auth=True, tenant_id=tenant)
    async with _client(app) as c:
        r = await c.post(
            "/integrations/gmail/uninstall",
            json={"gmail_installation_id": str(install_id), "actor_email": "ops@acme.com"},
        )

    assert r.status_code == 200, r.text
    assert r.json()["status"] == "uninstalled"
    assert seen == {
        "tenant_id": tenant,
        "gmail_installation_id": install_id,
        "actor_email": "ops@acme.com",
    }
    assert (
        _events().get(
            workflow="source_onboarding",
            event="source_uninstalled",
            outcome="success",
        )
        == 1
    )


# ---------------------------------------------------------------------
# POST /integrations/gmail/mailbox/stop
# ---------------------------------------------------------------------
async def test_mailbox_stop_requires_email(monkeypatch: pytest.MonkeyPatch) -> None:
    app = _make_app(with_auth=True, tenant_id=uuid4())
    async with _client(app) as c:
        r = await c.post(
            "/integrations/gmail/mailbox/stop",
            json={"gmail_installation_id": str(uuid4())},
        )
    assert r.status_code == 400
    assert "email_address" in r.text


async def test_mailbox_stop_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    from services.ingest.integrations.gmail import oauth as gmail_oauth

    tenant = uuid4()
    install_id = uuid4()
    seen: dict = {}

    async def _fake_stop(*, tenant_id, gmail_installation_id, email_address, actor_email=None):
        seen.update(
            tenant_id=tenant_id,
            gmail_installation_id=gmail_installation_id,
            email_address=email_address,
            actor_email=actor_email,
        )

    monkeypatch.setattr(gmail_oauth, "stop_mailbox", _fake_stop)

    app = _make_app(with_auth=True, tenant_id=tenant)
    async with _client(app) as c:
        r = await c.post(
            "/integrations/gmail/mailbox/stop",
            json={
                "gmail_installation_id": str(install_id),
                "email_address": "Alice@Acme.com",  # normalized to lower
            },
        )

    assert r.status_code == 200, r.text
    assert r.json()["status"] == "stopped"
    assert seen["email_address"] == "alice@acme.com"
    assert seen["gmail_installation_id"] == install_id
    assert seen["tenant_id"] == tenant
