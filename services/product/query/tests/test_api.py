"""Tests for services/product/query/api.py.

Exercises:
  - POST /view/ceo/ask happy path
  - prefetch fast-path via query_id
  - POST /view/ceo/turn-action (save / done / followup)
  - tenant resolution
"""
from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from lib.shared.errors import DependencyUnavailableError
from services.product.query import strategies as strat_pkg
from services.product.query.adapters import InMemoryCacheAdapter
from services.product.query.api import build_router
from services.product.query.core import QueryHandler
from services.product.query.tests._helpers import (
    FakeRenderingAdapter,
    FakeStrategy,
    ScriptedClassifier,
    fake_conn_provider,
)


TENANT = uuid4()


class _ProductionSettings:
    is_production = True


class UnavailableRenderingAdapter(FakeRenderingAdapter):
    async def render_conversation_turn(self, req):
        raise DependencyUnavailableError(
            "rendering",
            "conversation-turn",
            attempts=3,
            status_code=503,
        )


@pytest.fixture
def fake_strategies(monkeypatch):
    replacements = {
        cat: FakeStrategy(category=cat)
        for cat in strat_pkg.STRATEGIES.keys()
    }
    monkeypatch.setattr(strat_pkg, "STRATEGIES", replacements, raising=True)
    from services.product.query import strategies as strategies_mod
    monkeypatch.setattr(strategies_mod, "STRATEGIES", replacements, raising=True)
    yield


@pytest.fixture
def app(fake_strategies):
    cache = InMemoryCacheAdapter()
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier("arbitrary"),
        rendering_adapter=FakeRenderingAdapter(),
        cache_adapter=cache,
    )
    app = FastAPI()
    app.include_router(
        build_router(handler, default_tenant_id=TENANT),
    )
    app.state.handler = handler
    app.state.cache = cache
    return app


async def test_ask_happy_path(app):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.post(
            "/view/ceo/ask",
            json={"query": "why is Acme at risk?"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["query_echo"] == "why is Acme at risk?"
    assert body["response_html"]
    assert {"id": "followup", "label": "Follow up"} in body["verbs"]
    assert body["latency_ms"] >= 0


async def test_ask_rejects_empty_query(app):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.post("/view/ceo/ask", json={"query": ""})
    assert r.status_code == 400


async def test_ask_maps_rendering_unavailable_to_503(fake_strategies):
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier("arbitrary"),
        rendering_adapter=UnavailableRenderingAdapter(),
        cache_adapter=InMemoryCacheAdapter(),
    )
    app = FastAPI()
    app.include_router(build_router(handler, default_tenant_id=TENANT))

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.post(
            "/view/ceo/ask",
            json={"query": "why is Acme at risk?"},
        )

    assert r.status_code == 503
    assert r.json()["detail"] == {
        "error": "dependency_unavailable",
        "dependency": "rendering",
        "operation": "conversation-turn",
    }


async def test_ask_hits_prefetch_cache(app):
    """Warm the cache via handler directly, then hit /ask with
    query_id to test the fast-path."""
    handler: QueryHandler = app.state.handler
    from services.product.query.core import AnswerQueryRequest
    await handler.answer_query(
        AnswerQueryRequest(
            tenant_id=TENANT,
            query="preloaded: why is Acme at risk?",
            query_id="chip_preloaded",
        )
    )

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.post(
            "/view/ceo/ask",
            json={
                "query": "preloaded: why is Acme at risk?",
                "query_id": "chip_preloaded",
            },
        )
    assert r.status_code == 200
    body = r.json()
    assert body["query_echo"] == "preloaded: why is Acme at risk?"


async def test_turn_action_save(app):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.post(
            "/view/ceo/turn-action",
            json={"turn_id": str(uuid4()), "action": "save"},
        )
    assert r.status_code == 200
    assert r.json() == {"ok": True, "new_turn_id": None}


async def test_turn_action_done(app):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.post(
            "/view/ceo/turn-action",
            json={"turn_id": str(uuid4()), "action": "done"},
        )
    assert r.status_code == 200
    assert r.json()["ok"] is True


async def test_turn_action_followup_requires_query(app):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.post(
            "/view/ceo/turn-action",
            json={"turn_id": str(uuid4()), "action": "followup"},
        )
    assert r.status_code == 400


async def test_turn_action_followup_success(app):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.post(
            "/view/ceo/turn-action",
            json={
                "turn_id": str(uuid4()),
                "action": "followup",
                "follow_up_query": "and why?",
            },
        )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["new_turn_id"] is not None


async def test_ask_accepts_inline_card_context(app):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.post(
            "/view/ceo/ask",
            json={
                "query": "draft a reply",
                "inline_card_context": {
                    "card_id": str(uuid4()),
                    "subject": "Acme renewal",
                    "recipient": "monica",
                    "kind": "observation",
                },
            },
        )
    assert r.status_code == 200


async def test_ask_rejects_header_and_default_tenant_in_production(fake_strategies):
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier("arbitrary"),
        rendering_adapter=FakeRenderingAdapter(),
        cache_adapter=InMemoryCacheAdapter(),
    )
    app = FastAPI()
    app.state.gateway_settings = _ProductionSettings()
    app.include_router(build_router(handler, default_tenant_id=TENANT))

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        missing_auth = await client.post(
            "/view/ceo/ask",
            json={"query": "why is Acme at risk?"},
        )
        header_auth = await client.post(
            "/view/ceo/ask",
            headers={"x-tenant-id": str(TENANT)},
            json={"query": "why is Acme at risk?"},
        )

    assert missing_auth.status_code == 401
    assert header_auth.status_code == 401


async def test_ask_accepts_gateway_auth_in_production(fake_strategies):
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier("arbitrary"),
        rendering_adapter=FakeRenderingAdapter(),
        cache_adapter=InMemoryCacheAdapter(),
    )
    app = FastAPI()
    app.state.gateway_settings = _ProductionSettings()

    @app.middleware("http")
    async def _inject_auth(request, call_next):
        request.state.auth = type(
            "Auth",
            (),
            {"tenant_id": TENANT, "actor_id": uuid4()},
        )()
        return await call_next(request)

    app.include_router(build_router(handler, default_tenant_id=uuid4()))

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        ok = await client.post(
            "/view/ceo/ask",
            json={"query": "why is Acme at risk?"},
        )
        mismatch = await client.post(
            "/view/ceo/ask",
            headers={"x-tenant-id": str(uuid4())},
            json={"query": "why is Acme at risk?"},
        )

    assert ok.status_code == 200
    assert mismatch.status_code == 403
    assert mismatch.json()["detail"] == "tenant_mismatch"
