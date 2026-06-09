from __future__ import annotations

from uuid import uuid4

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from services.product.ask.api import build_router
from services.product.ask.orchestrator import AskOrchestrator
from services.product.ask.schemas import AskScope
from services.product.ask.store import InMemoryAskStore
from services.product.ask.tests.test_orchestrator import _ConnProvider, _FakeReader


TENANT = uuid4()
VIEWER = uuid4()


async def test_api_session_turn_and_evidence_expand(monkeypatch):
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
