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
VIEWER = uuid4()


@pytest.fixture
def fake_strategies(monkeypatch):
    replacements = {
        cat: FakeStrategy(category=cat)
        for cat in strat_pkg.STRATEGIES.keys()
    }
    monkeypatch.setattr(strat_pkg, "STRATEGIES", replacements, raising=True)
    from services.product.query import strategies as strategies_mod
    monkeypatch.setattr(strategies_mod, "STRATEGIES", replacements, raising=True)
    yield replacements


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
        build_router(
            handler,
            default_tenant_id=TENANT,
            default_viewer_id=VIEWER,
        ),
    )
    app.state.handler = handler
    app.state.cache = cache
    app.state.strategies = fake_strategies
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
    access = app.state.strategies["arbitrary"].last_access_context
    assert access.requestor_actor_id == VIEWER


async def test_ask_requires_viewer_when_no_auth_default(fake_strategies):
    handler = QueryHandler(
        conn_provider=fake_conn_provider(),
        classifier=ScriptedClassifier("arbitrary"),
        rendering_adapter=FakeRenderingAdapter(),
        cache_adapter=InMemoryCacheAdapter(),
    )
    no_viewer_app = FastAPI()
    no_viewer_app.include_router(
        build_router(handler, default_tenant_id=TENANT),
    )

    async with AsyncClient(
        transport=ASGITransport(app=no_viewer_app), base_url="http://test"
    ) as client:
        r = await client.post(
            "/view/ceo/ask",
            json={"query": "why is Acme at risk?"},
        )
    assert r.status_code == 401


async def test_ask_rejects_empty_query(app):
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.post("/view/ceo/ask", json={"query": ""})
    assert r.status_code == 400


async def test_ask_hits_prefetch_cache(app):
    """Warm the cache via handler directly, then hit /ask with
    query_id to test the fast-path."""
    handler: QueryHandler = app.state.handler
    from services.product.query.core import AnswerQueryRequest
    await handler.answer_query(
        AnswerQueryRequest(
            tenant_id=TENANT,
            viewer_id=VIEWER,
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
