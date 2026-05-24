"""services/gateway/tests/test_spec_routes.py — light smoke tests for
the spec-aligned product routes. These do NOT touch Postgres; they
mount the router on a bare FastAPI app and simulate the
BearerAuthMiddleware by stuffing an `auth` object onto request.state.
The shape-matching here is the contract between the backend and the
ui/src/api/spec-mocks.ts fixtures.
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.testclient import TestClient

from services.gateway.spec_routes import build_spec_router


class _StubAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request.state.auth = type("Auth", (), {"tenant_id": "demo", "actor_id": "actor-ceo"})
        return await call_next(request)


@pytest.fixture()
def client() -> TestClient:
    app = FastAPI()
    app.add_middleware(_StubAuthMiddleware)
    app.include_router(build_spec_router())
    return TestClient(app)


def test_list_threads_returns_grouped_payload(client: TestClient) -> None:
    r = client.get("/v1/spec/operating_threads/")
    assert r.status_code == 200
    body = r.json()
    assert "groups" in body
    assert "compressionSentence" in body
    assert {"changedToday", "contested", "blockedCommitments", "arrAtRisk"} <= set(body["statusCounters"])
    titles = {t["title"] for g in body["groups"] for t in g["threads"]}
    assert "Customer Reliability" in titles


def test_get_thread_404s_for_unknown_id(client: TestClient) -> None:
    assert client.get("/v1/spec/operating_threads/nope").status_code == 404


def test_recent_changes_returns_items(client: TestClient) -> None:
    r = client.get("/v1/spec/operating_threads/recent_changes")
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) >= 1
    assert items[0]["kind"] in {
        "state_change",
        "forecast_created",
        "commitment_flagged",
        "delta_proposed",
        "delta_accepted",
        "contestation",
    }


def test_list_spec_deltas_includes_queue_sections(client: TestClient) -> None:
    r = client.get("/v1/spec/decision_deltas/")
    assert r.status_code == 200
    body = r.json()
    sections = {d["queueSection"] for d in body["deltas"]}
    assert {"requires_authority", "delegatable", "needs_context"} <= sections
    assert "sinceLastReview" in body


def test_list_spec_forecasts(client: TestClient) -> None:
    r = client.get("/v1/spec/forecasts/")
    assert r.status_code == 200
    body = r.json()
    assert len(body["items"]) >= 1
    assert "leadingIndicators" in body["items"][0]


def test_ledger_events_supports_category_filter(client: TestClient) -> None:
    r = client.get("/v1/spec/ledger_events/?categories=forecast")
    assert r.status_code == 200
    body = r.json()
    assert all(e["category"] == "forecast" for e in body["events"])


def test_unauth_request_is_rejected() -> None:
    app = FastAPI()
    app.include_router(build_spec_router())
    cli = TestClient(app)
    assert cli.get("/v1/spec/operating_threads/").status_code == 401
