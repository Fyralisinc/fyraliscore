from __future__ import annotations

from fastapi import FastAPI
from starlette.testclient import TestClient

from services.app.gateway.finance_router import build_finance_router


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(build_finance_router())
    return TestClient(app)


def test_sources_listing_is_available_without_gateway_deps() -> None:
    response = _client().get("/finance/sources")

    assert response.status_code == 200
    sources = {source["source"] for source in response.json()["sources"]}
    assert sources == {"mercury", "quickbooks", "brex", "ramp", "gusto", "deel"}


def test_unknown_source_is_rejected_before_gateway_deps() -> None:
    response = _client().post("/finance/stripe/install")

    assert response.status_code == 404
    assert response.json()["detail"] == "unknown finance source 'stripe'"


def test_missing_tenant_is_rejected_before_gateway_deps(monkeypatch) -> None:
    monkeypatch.delenv("DEFAULT_TENANT_ID", raising=False)
    monkeypatch.delenv("COMPANY_OS_TENANT_ID", raising=False)

    response = _client().post("/finance/mercury/install")

    assert response.status_code == 400
    assert response.json()["detail"] == "tenant_id missing"
