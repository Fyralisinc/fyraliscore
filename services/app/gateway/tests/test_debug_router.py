from __future__ import annotations

from uuid import uuid4

from fastapi import FastAPI
from starlette.testclient import TestClient

from services.app.gateway.debug_router import build_debug_router


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
        "services.reasoning.think.quality_report.build_think_quality_report",
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
        "services.reasoning.think.quality_report.build_think_quality_cases",
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


def test_sage_health_endpoint_delegates_to_report_builder(monkeypatch):
    tenant_id = uuid4()
    captured = {}

    async def fake_health(
        conn,
        *,
        tenant_id,
        structural_freshness_hours,
        optimizer_lag_minutes,
    ):
        captured.update(
            {
                "tenant_id": str(tenant_id),
                "structural_freshness_hours": structural_freshness_hours,
                "optimizer_lag_minutes": optimizer_lag_minutes,
            }
        )
        return {"status": "ok", "findings": []}

    monkeypatch.setattr(
        "services.reasoning.sage.health.build_sage_health_report",
        fake_health,
    )

    app = FastAPI()
    app.state.pool = _Pool()
    app.include_router(build_debug_router())
    client = TestClient(app)

    response = client.get(
        "/debug/sage-health?structural_freshness_hours=12&optimizer_lag_minutes=45",
        headers={"X-Tenant-Id": str(tenant_id)},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert captured == {
        "tenant_id": str(tenant_id),
        "structural_freshness_hours": 12,
        "optimizer_lag_minutes": 45,
    }


def test_relationship_ontology_review_endpoint_delegates(monkeypatch):
    tenant_id = uuid4()
    proposal_id = uuid4()
    captured = {}

    async def fake_review(
        self,
        conn,
        *,
        tenant_id,
        proposal_id,
        status,
        reviewed_by,
        note,
    ):
        captured.update(
            {
                "tenant_id": str(tenant_id),
                "proposal_id": str(proposal_id),
                "status": status,
                "reviewed_by": reviewed_by,
                "note": note,
            }
        )
        return {
            "id": proposal_id,
            "tenant_id": tenant_id,
            "proposed_edge_kind": "gated_by_decision",
            "status": status,
        }

    monkeypatch.setattr(
        "services.reasoning.relationships.ontology_proposals."
        "RelationshipOntologyProposalsRepo.review",
        fake_review,
    )

    app = FastAPI()
    app.state.pool = _Pool()
    app.include_router(build_debug_router())
    client = TestClient(app)

    response = client.post(
        f"/debug/relationship-ontology-proposals/{proposal_id}/review",
        headers={"X-Tenant-Id": str(tenant_id)},
        json={
            "status": "accepted",
            "reviewed_by": "operator",
            "note": "Repeated decision-gate evidence.",
        },
    )

    assert response.status_code == 200
    assert response.json()["proposal"]["status"] == "accepted"
    assert captured == {
        "tenant_id": str(tenant_id),
        "proposal_id": str(proposal_id),
        "status": "accepted",
        "reviewed_by": "operator",
        "note": "Repeated decision-gate evidence.",
    }
