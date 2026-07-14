from __future__ import annotations

from uuid import uuid4

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from services.product.ask.api import build_router
from services.product.ask.orchestrator import AskOrchestrator
from services.product.ask.schemas import AskScope
from services.product.ask.store import InMemoryAskStore
from services.product.ask.tests.test_orchestrator import _ConnProvider, _FakeReader
from services.platform.access_control.authority import AuthorityDecision


TENANT = uuid4()
VIEWER = uuid4()


class _ProductionSettings:
    is_production = True


async def test_api_session_turn_and_evidence_expand(monkeypatch):
    async def fake_authorize_read(*args, **kwargs):
        return AuthorityDecision(True, "authorized")

    monkeypatch.setattr(
        "services.product.ask.orchestrator.authorize_read",
        fake_authorize_read,
        raising=True,
    )
    store = InMemoryAskStore()
    orch = AskOrchestrator(
        store=store,
        conn_provider=_ConnProvider(),
        reader=_FakeReader(),
    )
    app = FastAPI()
    app.include_router(
        build_router(
            orch,
            default_tenant_id=TENANT,
            default_viewer_id=VIEWER,
        )
    )

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        created = await client.post(
            "/v1/ask/sessions",
            json={
                "initial_scope": AskScope(
                    type="current_page",
                    label="Today",
                ).model_dump(mode="json"),
                "source_route": "/today",
            },
        )
        assert created.status_code == 200
        session_id = created.json()["session"]["id"]

        turn = await client.post(
            f"/v1/ask/sessions/{session_id}/messages",
            json={"query": "What is blocked?"},
        )
        assert turn.status_code == 200
        run_id = turn.json()["retrieval_run_id"]
        assert turn.json()["payload"]["answer"]

        expanded = await client.post(
            "/v1/ask/evidence/expand",
            json={"retrieval_run_id": run_id},
        )
        assert expanded.status_code == 200
        assert len(expanded.json()["evidence"]) == 1
        assert len(expanded.json()["omitted"]) == 1


async def test_api_rejects_default_identity_in_production():
    store = InMemoryAskStore()
    orch = AskOrchestrator(
        store=store,
        conn_provider=_ConnProvider(),
        reader=_FakeReader(),
    )
    app = FastAPI()
    app.state.gateway_settings = _ProductionSettings()
    app.include_router(
        build_router(
            orch,
            default_tenant_id=TENANT,
            default_viewer_id=VIEWER,
        )
    )

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        created = await client.post(
            "/v1/ask/sessions",
            json={
                "initial_scope": AskScope(
                    type="current_page",
                    label="Today",
                ).model_dump(mode="json"),
                "source_route": "/today",
            },
        )
        header_created = await client.post(
            "/v1/ask/sessions",
            headers={
                "x-tenant-id": str(TENANT),
                "x-actor-id": str(VIEWER),
            },
            json={
                "initial_scope": AskScope(
                    type="current_page",
                    label="Today",
                ).model_dump(mode="json"),
                "source_route": "/today",
            },
        )

    assert created.status_code == 401
    assert header_created.status_code == 401


async def test_api_accepts_gateway_auth_in_production():
    store = InMemoryAskStore()
    orch = AskOrchestrator(
        store=store,
        conn_provider=_ConnProvider(),
        reader=_FakeReader(),
    )
    app = FastAPI()
    app.state.gateway_settings = _ProductionSettings()

    @app.middleware("http")
    async def _inject_auth(request, call_next):
        request.state.auth = type(
            "Auth",
            (),
            {"tenant_id": TENANT, "actor_id": VIEWER},
        )()
        return await call_next(request)

    app.include_router(
        build_router(
            orch,
            default_tenant_id=uuid4(),
            default_viewer_id=uuid4(),
        )
    )

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        created = await client.post(
            "/v1/ask/sessions",
            json={
                "initial_scope": AskScope(
                    type="current_page",
                    label="Today",
                ).model_dump(mode="json"),
                "source_route": "/today",
            },
        )

    assert created.status_code == 200
    assert created.json()["session"]["tenant_id"] == str(TENANT)


async def test_api_validation_errors_use_bounded_codes(monkeypatch):
    async def fake_can_read(*args, **kwargs):
        return type("Decision", (), {"allowed": True})()

    monkeypatch.setattr(
        "services.product.ask.orchestrator.can_read",
        fake_can_read,
        raising=True,
    )
    store = InMemoryAskStore()
    orch = AskOrchestrator(
        store=store,
        conn_provider=_ConnProvider(),
        reader=_FakeReader(),
    )
    app = FastAPI()
    app.include_router(
        build_router(
            orch,
            default_tenant_id=TENANT,
            default_viewer_id=VIEWER,
        )
    )

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        created = await client.post(
            "/v1/ask/sessions",
            json={
                "initial_scope": AskScope(
                    type="current_page",
                    label="Today",
                ).model_dump(mode="json"),
                "source_route": "/today",
            },
        )
        session_id = created.json()["session"]["id"]
        empty_query = await client.post(
            f"/v1/ask/sessions/{session_id}/messages",
            json={"query": "   "},
        )
        missing_session = await client.post(
            f"/v1/ask/sessions/{uuid4()}/messages",
            json={"query": "What changed?"},
        )

    assert empty_query.status_code == 400
    assert empty_query.json() == {"detail": "invalid_query"}
    assert missing_session.status_code == 404
    assert missing_session.json() == {"detail": "ask_session_not_found"}
