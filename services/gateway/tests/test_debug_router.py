from __future__ import annotations

from uuid import uuid4

from fastapi import FastAPI
from starlette.testclient import TestClient

from services.gateway.debug_router import build_debug_router


class _Acquire:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        return None


class _Pool:
    def acquire(self):
        return _Acquire()


def test_think_quality_endpoint_delegates_to_report_builder(monkeypatch):
    tenant_id = uuid4()
    captured = {}

    async def fake_report(conn, *, tenant_id, since_hours, limit, low_context_ratio):
        captured.update(
            {
                "tenant_id": str(tenant_id),
                "since_hours": since_hours,
                "limit": limit,
                "low_context_ratio": low_context_ratio,
            }
        )
        return {"summary": {"total_runs": 0}, "flagged_runs": []}

    monkeypatch.setattr(
        "services.think.quality_report.build_think_quality_report",
        fake_report,
    )

    app = FastAPI()
    app.state.pool = _Pool()
    app.include_router(build_debug_router())
    client = TestClient(app)

    response = client.get(
        "/debug/think-quality?since_hours=12&limit=25&low_context_ratio=0.4",
        headers={"X-Tenant-Id": str(tenant_id)},
    )

    assert response.status_code == 200
    assert response.json()["summary"]["total_runs"] == 0
    assert captured == {
        "tenant_id": str(tenant_id),
        "since_hours": 12,
        "limit": 25,
        "low_context_ratio": 0.4,
    }


def test_think_quality_cases_endpoint_delegates_to_case_builder(monkeypatch):
    tenant_id = uuid4()
    captured = {}

    async def fake_cases(
        conn,
        *,
        tenant_id,
        since_hours,
        limit,
        low_context_ratio,
        include_artifacts,
    ):
        captured.update(
            {
                "tenant_id": str(tenant_id),
                "since_hours": since_hours,
                "limit": limit,
                "low_context_ratio": low_context_ratio,
                "include_artifacts": include_artifacts,
            }
        )
        return {"cases": [{"case_id": "think-quality:test"}]}

    monkeypatch.setattr(
        "services.think.quality_report.build_think_quality_cases",
        fake_cases,
    )

    app = FastAPI()
    app.state.pool = _Pool()
    app.include_router(build_debug_router())
    client = TestClient(app)

    response = client.get(
        "/debug/think-quality/cases?"
        "since_hours=6&limit=7&low_context_ratio=0.3&include_artifacts=false",
        headers={"X-Tenant-Id": str(tenant_id)},
    )

    assert response.status_code == 200
    assert response.json()["cases"][0]["case_id"] == "think-quality:test"
    assert captured == {
        "tenant_id": str(tenant_id),
        "since_hours": 6,
        "limit": 7,
        "low_context_ratio": 0.3,
        "include_artifacts": False,
    }
