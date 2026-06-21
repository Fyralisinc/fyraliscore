from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.testclient import TestClient

from services.app.gateway.clarifications_router import build_clarifications_router


class _Acquire:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    async def __aenter__(self) -> object:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _Pool:
    def __init__(self, conn: object) -> None:
        self.conn = conn

    def acquire(self) -> _Acquire:
        return _Acquire(self.conn)


class _Conn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []
        self.fetchvals: list[tuple[str, tuple]] = []

    async def execute(self, query: str, *args):
        self.executed.append((query, args))
        return "UPDATE 1"

    async def fetchval(self, query: str, *args):
        self.fetchvals.append((query, args))
        return False


class _Row:
    def __init__(self, payload):
        self._data = payload

    @property
    def payload(self):
        return self._data.get("payload", {})

    def __getattr__(self, name):
        return self._data.get(name)

    def to_dict(self):
        return self._data


def _client(
    *,
    tenant_id=None,
    actor_id=None,
    authenticated: bool = True,
    conn: object | None = None,
) -> TestClient:
    tenant_id = tenant_id or uuid4()
    actor_id = actor_id or uuid4()
    app = FastAPI()
    app.state.deps = SimpleNamespace(pool=_Pool(conn or object()))

    if authenticated:

        class _StubAuthMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                request.state.auth = SimpleNamespace(
                    tenant_id=tenant_id,
                    actor_id=actor_id,
                )
                return await call_next(request)

        app.add_middleware(_StubAuthMiddleware)

    app.include_router(build_clarifications_router())
    return TestClient(app)


def test_clarifications_list_requires_authentication() -> None:
    client = _client(authenticated=False)

    response = client.get("/v1/clarifications")

    assert response.status_code == 401
    assert response.json() == {
        "error": "unauthorized",
        "reason": "missing_bearer",
    }


def test_clarifications_list_delegates(monkeypatch) -> None:
    tenant_id = uuid4()
    conn = object()
    captured = {}

    async def fake_list(acquired_conn, *, tenant_id, status, limit):
        captured.update(
            {
                "conn": acquired_conn,
                "tenant_id": str(tenant_id),
                "status": status,
                "limit": limit,
            }
        )
        return [
            _Row(
                {
                    "id": str(uuid4()),
                    "kind": "actor_identity",
                    "question": "Who is github:alice?",
                }
            )
        ]

    monkeypatch.setattr(
        "services.app.gateway.clarifications_router.list_clarification_requests",
        fake_list,
    )
    client = _client(tenant_id=tenant_id, conn=conn)

    response = client.get("/v1/clarifications?status=open&limit=5")

    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["items"][0]["kind"] == "actor_identity"
    assert captured == {
        "conn": conn,
        "tenant_id": str(tenant_id),
        "status": "open",
        "limit": 5,
    }


def test_clarification_answer_delegates(monkeypatch) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    request_id = uuid4()
    conn = object()
    captured = {}

    async def fake_answer(acquired_conn, *, tenant_id, request_id, answer, answered_by):
        captured.update(
            {
                "conn": acquired_conn,
                "tenant_id": str(tenant_id),
                "request_id": str(request_id),
                "answer": answer,
                "answered_by": str(answered_by),
            }
        )
        return _Row({"id": str(request_id), "status": "answered", "answer": answer})

    monkeypatch.setattr(
        "services.app.gateway.clarifications_router.answer_clarification_request",
        fake_answer,
    )
    client = _client(tenant_id=tenant_id, actor_id=actor_id, conn=conn)

    response = client.post(
        f"/v1/clarifications/{request_id}/answer",
        json={"answer": {"action": "create_internal_actor"}},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "answered"
    assert captured == {
        "conn": conn,
        "tenant_id": str(tenant_id),
        "request_id": str(request_id),
        "answer": {"action": "create_internal_actor"},
        "answered_by": str(actor_id),
    }


def test_clarification_answer_resolves_substrate_candidate(monkeypatch) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    request_id = uuid4()
    candidate_id = uuid4()
    conn = object()
    captured = {}

    async def fake_answer(acquired_conn, *, tenant_id, request_id, answer, answered_by):
        return _Row(
            {
                "id": str(request_id),
                "kind": "substrate_candidate_resolution",
                "status": "answered",
                "object_kind": "substrate_candidate",
                "object_id": str(candidate_id),
                "answer": answer,
            }
        )

    async def fake_get(acquired_conn, *, tenant_id, candidate_id):
        return SimpleNamespace(id=candidate_id, label="Alpen Ops")

    async def fake_apply(acquired_conn, *, candidate, answer):
        captured.update(
            {
                "conn": acquired_conn,
                "candidate_id": str(candidate.id),
                "answer": answer,
            }
        )
        return SimpleNamespace(action="promote_actor")

    monkeypatch.setattr(
        "services.app.gateway.clarifications_router.answer_clarification_request",
        fake_answer,
    )
    monkeypatch.setattr(
        "services.app.gateway.clarifications_router.get_substrate_candidate",
        fake_get,
    )
    monkeypatch.setattr(
        "services.app.gateway.clarifications_router.apply_candidate_resolution_answer",
        fake_apply,
    )
    client = _client(tenant_id=tenant_id, actor_id=actor_id, conn=conn)

    response = client.post(
        f"/v1/clarifications/{request_id}/answer",
        json={"answer": {"action": "promote_actor"}},
    )

    assert response.status_code == 200
    assert captured == {
        "conn": conn,
        "candidate_id": str(candidate_id),
        "answer": {"action": "promote_actor"},
    }


def test_clarification_answer_accepts_entity_resolution_candidate(monkeypatch) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    request_id = uuid4()
    review_id = uuid4()
    observation_id = uuid4()
    conn = _Conn()
    canonical_ref = {"type": "customer", "id": str(uuid4())}

    async def fake_answer(acquired_conn, *, tenant_id, request_id, answer, answered_by):
        return _Row(
            {
                "id": str(request_id),
                "kind": "entity_resolution",
                "status": "answered",
                "object_kind": "entity_review",
                "object_id": str(review_id),
                "source_observation_id": str(observation_id),
                "payload": {
                    "phrase": "Alpen",
                    "candidates": [
                        {
                            "canonical_ref": canonical_ref,
                            "confidence": 0.76,
                        }
                    ],
                },
                "answer": answer,
            }
        )

    monkeypatch.setattr(
        "services.app.gateway.clarifications_router.answer_clarification_request",
        fake_answer,
    )
    client = _client(tenant_id=tenant_id, actor_id=actor_id, conn=conn)

    response = client.post(
        f"/v1/clarifications/{request_id}/answer",
        json={"answer": {"action": "accept_candidate"}},
    )

    assert response.status_code == 200
    assert any("INSERT INTO entity_aliases" in query for query, _args in conn.executed)
    assert any("UPDATE entity_review_queue" in query for query, _args in conn.executed)
    assert any("UPDATE observations" in query for query, _args in conn.executed)
    assert any("INSERT INTO observations" in query for query, _args in conn.executed)


def test_clarification_answer_creates_new_customer_entity(monkeypatch) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    request_id = uuid4()
    review_id = uuid4()
    observation_id = uuid4()
    resource_id = uuid4()
    conn = _Conn()
    created_calls: list[dict] = []

    async def fake_answer(acquired_conn, *, tenant_id, request_id, answer, answered_by):
        return _Row(
            {
                "id": str(request_id),
                "kind": "entity_resolution",
                "status": "answered",
                "object_kind": "entity_review",
                "object_id": str(review_id),
                "source_observation_id": str(observation_id),
                "payload": {"phrase": "Beta Corp", "candidates": []},
                "answer": answer,
            }
        )

    async def fake_create(**kwargs):
        created_calls.append(kwargs)
        return SimpleNamespace(id=resource_id)

    monkeypatch.setattr(
        "services.app.gateway.clarifications_router.answer_clarification_request",
        fake_answer,
    )
    monkeypatch.setattr(
        "services.app.gateway.clarifications_router.resources_repo.create",
        fake_create,
    )
    client = _client(tenant_id=tenant_id, actor_id=actor_id, conn=conn)

    response = client.post(
        f"/v1/clarifications/{request_id}/answer",
        json={
            "answer": {
                "action": "create_new_entity",
                "entity_type": "customer",
                "label": "Beta Corp",
            }
        },
    )

    assert response.status_code == 200
    assert created_calls
    assert created_calls[0]["kind"] == "relational"
    assert created_calls[0]["identity"] == "Beta Corp"
    assert created_calls[0]["created_by_event_id"] == observation_id
    alias_args = next(
        args for query, args in conn.executed if "INSERT INTO entity_aliases" in query
    )
    assert '"type": "customer"' in alias_args[3]
    assert str(resource_id) in alias_args[3]
    assert any("UPDATE entity_review_queue" in query for query, _args in conn.executed)


def test_clarification_answer_rejects_entity_resolution_candidate(monkeypatch) -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    request_id = uuid4()
    review_id = uuid4()
    conn = _Conn()

    async def fake_answer(acquired_conn, *, tenant_id, request_id, answer, answered_by):
        return _Row(
            {
                "id": str(request_id),
                "kind": "entity_resolution",
                "status": "answered",
                "object_kind": "entity_review",
                "object_id": str(review_id),
                "payload": {"phrase": "the project", "candidates": []},
                "answer": answer,
            }
        )

    monkeypatch.setattr(
        "services.app.gateway.clarifications_router.answer_clarification_request",
        fake_answer,
    )
    client = _client(tenant_id=tenant_id, actor_id=actor_id, conn=conn)

    response = client.post(
        f"/v1/clarifications/{request_id}/answer",
        json={"answer": {"action": "reject_candidate"}},
    )

    assert response.status_code == 200
    assert any(
        "UPDATE entity_review_queue" in query and args[3] == "reject_candidate"
        for query, args in conn.executed
    )
    assert not any("INSERT INTO entity_aliases" in query for query, _args in conn.executed)


def test_clarification_answer_rejects_invalid_id() -> None:
    client = _client()

    response = client.post(
        "/v1/clarifications/not-a-uuid/answer",
        json={"answer": {"action": "x"}},
    )

    assert response.status_code == 400
    assert response.json() == {"error": "invalid_clarification_id"}
