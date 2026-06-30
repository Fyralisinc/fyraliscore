from __future__ import annotations

from uuid import uuid4

from fastapi import FastAPI
from starlette.testclient import TestClient

from services.app.gateway.debug_router import build_debug_router
from services.platform.access_control.authority import AuthorityDecision, Principal


class _Acquire:
    def __init__(self, conn=None):
        self.conn = conn or object()

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return None


class _Pool:
    def __init__(self, conn=None):
        self.conn = conn

    def acquire(self):
        return _Acquire(self.conn)


class _DebugConn:
    def __init__(self, rows):
        self.rows = rows

    async def fetch(self, *args, **kwargs):
        return self.rows


def _app_with_auth(tenant_id, actor_id, pool):
    app = FastAPI()
    app.state.pool = pool

    @app.middleware("http")
    async def _auth(request, call_next):
        request.state.auth = type(
            "Auth",
            (),
            {"tenant_id": tenant_id, "actor_id": actor_id},
        )()
        return await call_next(request)

    app.include_router(build_debug_router())
    return app


def test_debug_models_requires_actor_auth():
    tenant_id = uuid4()
    app = FastAPI()
    app.state.pool = _Pool(_DebugConn([]))
    app.include_router(build_debug_router())
    client = TestClient(app)

    response = client.get(
        "/debug/models",
        headers={"X-Tenant-Id": str(tenant_id)},
    )

    assert response.status_code == 401


def test_debug_models_filters_rows_through_authority(monkeypatch):
    tenant_id = uuid4()
    actor_id = uuid4()
    visible = uuid4()
    secret = uuid4()

    async def fake_principal_for_actor(actor_id, *, conn, tenant_id):
        return Principal(tenant_id=tenant_id, actor_id=actor_id)

    async def fake_authorize_read(principal, purpose, object_ref, *, conn):
        if object_ref.object_id == secret:
            return AuthorityDecision(False, "model_out_of_scope")
        return AuthorityDecision(True, "authorized")

    monkeypatch.setattr(
        "services.app.gateway.debug_router.principal_for_actor",
        fake_principal_for_actor,
    )
    monkeypatch.setattr(
        "services.app.gateway.debug_router.authorize_read",
        fake_authorize_read,
    )

    app = _app_with_auth(
        tenant_id,
        actor_id,
        _Pool(
            _DebugConn([
                {
                    "id": secret,
                    "proposition_kind": "belief",
                    "status": "active",
                    "confidence": 0.9,
                    "confidence_at_assertion": 0.9,
                    "confirmed_count": 0,
                    "contested_count": 0,
                    "proposition": {},
                    "born_from_event_id": uuid4(),
                    "last_confirmed_at": None,
                    "created_at": None,
                },
                {
                    "id": visible,
                    "proposition_kind": "belief",
                    "status": "active",
                    "confidence": 0.8,
                    "confidence_at_assertion": 0.8,
                    "confirmed_count": 0,
                    "contested_count": 0,
                    "proposition": {},
                    "born_from_event_id": uuid4(),
                    "last_confirmed_at": None,
                    "created_at": None,
                },
            ])
        ),
    )
    client = TestClient(app)

    response = client.get(
        "/debug/models",
        headers={"X-Tenant-Id": str(tenant_id)},
    )

    assert response.status_code == 200
    assert [row["id"] for row in response.json()["models"]] == [str(visible)]


def test_debug_cache_requires_admin_or_leadership(monkeypatch):
    tenant_id = uuid4()
    actor_id = uuid4()

    async def fake_principal_for_actor(actor_id, *, conn, tenant_id):
        return Principal(tenant_id=tenant_id, actor_id=actor_id)

    monkeypatch.setattr(
        "services.app.gateway.debug_router.principal_for_actor",
        fake_principal_for_actor,
    )

    app = _app_with_auth(
        tenant_id,
        actor_id,
        _Pool(_DebugConn([])),
    )
    client = TestClient(app)

    response = client.get(
        "/debug/cache",
        headers={"X-Tenant-Id": str(tenant_id)},
    )

    assert response.status_code == 403


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
