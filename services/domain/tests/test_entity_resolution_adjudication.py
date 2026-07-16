from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from lib.shared.errors import ValidationError
from services.domain.clarifications import ClarificationRequest
from services.domain.entity_resolution_adjudication import (
    adjudicate_entity_resolution_clarification,
)


class _Conn:
    def __init__(self, *, authorized: bool = False) -> None:
        self.authorized = authorized
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.fetchvals: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.executed.append((query, args))
        return "UPDATE 1"

    async def fetchval(self, query: str, *args: object) -> bool:
        self.fetchvals.append((query, args))
        return self.authorized


def _clarification(
    *,
    tenant_id: UUID,
    request_id: UUID,
    object_kind: str = "grounding_trace",
    object_id: UUID | None = None,
    observation_id: UUID | None = None,
    payload: dict | None = None,
) -> ClarificationRequest:
    now = datetime.now(timezone.utc)
    return ClarificationRequest(
        id=request_id,
        tenant_id=tenant_id,
        kind="entity_resolution",
        status="answered",
        priority="normal",
        question="What does this entity mention refer to?",
        explanation="",
        object_kind=object_kind,
        object_id=object_id,
        object_key=None,
        source_observation_id=observation_id,
        model_id=None,
        options=[],
        payload=payload or {},
        answer={"action": "accept_candidate"},
        answered_by=None,
        answered_at=now,
        dismissed_reason=None,
        expires_at=None,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.asyncio
async def test_accept_candidate_persists_adjudicated_alias_and_successor(
    monkeypatch,
) -> None:
    tenant_id = uuid4()
    answered_by = uuid4()
    request_id = uuid4()
    observation_id = uuid4()
    grounding_trace_id = uuid4()
    successor_trace_id = uuid4()
    canonical_ref = {"type": "customer", "id": str(uuid4())}
    clarification = _clarification(
        tenant_id=tenant_id,
        request_id=request_id,
        observation_id=observation_id,
        payload={
            "phrase": "Alpen",
            "feedback_lineage": {
                "grounding_trace_id": str(grounding_trace_id),
                "resolution_assessment_id": str(uuid4()),
            },
            "candidates": [
                {
                    "canonical_ref": canonical_ref,
                    "confidence": 0.76,
                }
            ],
        },
    )
    conn = _Conn()
    alias_calls: list[dict] = []
    successor_calls: list[dict] = []
    semantic_calls: list[dict] = []

    async def fake_insert_alias(acquired_conn, **kwargs):
        alias_calls.append({"conn": acquired_conn, **kwargs})
        return SimpleNamespace(id=uuid4())

    async def fake_append_successor(acquired_conn, **kwargs):
        successor_calls.append({"conn": acquired_conn, **kwargs})
        return successor_trace_id

    async def fake_enqueue_work(_repo, acquired_conn, **kwargs):
        semantic_calls.append({"conn": acquired_conn, **kwargs})
        return SimpleNamespace(id=uuid4())

    monkeypatch.setattr(
        "services.domain.entity_resolution_adjudication.insert_alias_with_connection",
        fake_insert_alias,
    )
    monkeypatch.setattr(
        "services.domain.entity_resolution_adjudication.EntityGroundingRepo.append_adjudicated_successor",
        fake_append_successor,
    )
    monkeypatch.setattr(
        "services.domain.entity_resolution_adjudication.SourceSemanticRepo.enqueue_work",
        fake_enqueue_work,
    )

    await adjudicate_entity_resolution_clarification(
        conn,
        clarification=clarification,
        answer={"action": "accept_candidate"},
        tenant_id=tenant_id,
        answered_by=answered_by,
    )

    assert len(alias_calls) == 1
    metadata = alias_calls[0]["extra_metadata"]
    assert alias_calls[0]["conn"] is conn
    assert alias_calls[0]["resolved_entity_ref"] == canonical_ref
    assert alias_calls[0]["adjudicated"] is True
    assert metadata["identity_basis_ref"] == f"clarification-request:{request_id}"
    assert metadata["adjudicated_by"] == str(answered_by)
    assert metadata["resolution_scope"] == "source_context_only"
    assert metadata["autonomous_replay_eligible"] is False
    assert successor_calls[0]["original_trace_id"] == grounding_trace_id
    assert successor_calls[0]["source_observation_id"] == observation_id
    assert successor_calls[0]["canonical_ref"] == canonical_ref
    assert semantic_calls[0]["grounding_trace_id"] == successor_trace_id


@pytest.mark.asyncio
async def test_tenant_global_adjudication_requires_privileged_confirmation() -> None:
    tenant_id = uuid4()
    clarification = _clarification(
        tenant_id=tenant_id,
        request_id=uuid4(),
        payload={
            "phrase": "NBI",
            "candidates": [
                {
                    "canonical_ref": {"type": "customer", "id": str(uuid4())},
                    "confidence": 0.99,
                }
            ],
        },
    )

    with pytest.raises(
        ValidationError,
        match="explicit privileged confirmation",
    ):
        await adjudicate_entity_resolution_clarification(
            _Conn(),
            clarification=clarification,
            answer={
                "action": "accept_candidate",
                "resolution_scope": "tenant_global_exact",
            },
            tenant_id=tenant_id,
            answered_by=uuid4(),
        )


@pytest.mark.asyncio
async def test_reject_candidate_dismisses_entity_review() -> None:
    tenant_id = uuid4()
    review_id = uuid4()
    answered_by = uuid4()
    conn = _Conn()
    clarification = _clarification(
        tenant_id=tenant_id,
        request_id=uuid4(),
        object_kind="entity_review",
        object_id=review_id,
        payload={"phrase": "the project", "candidates": []},
    )

    await adjudicate_entity_resolution_clarification(
        conn,
        clarification=clarification,
        answer={"action": "reject_candidate"},
        tenant_id=tenant_id,
        answered_by=answered_by,
    )

    query, args = conn.executed[0]
    assert "UPDATE entity_review_queue" in query
    assert args == (review_id, tenant_id, answered_by, "reject_candidate")


@pytest.mark.asyncio
async def test_rejects_non_entity_resolution_clarification() -> None:
    tenant_id = uuid4()
    clarification = _clarification(
        tenant_id=tenant_id,
        request_id=uuid4(),
    )
    object.__setattr__(clarification, "kind", "actor_identity")

    with pytest.raises(
        ValidationError,
        match="not an entity-resolution adjudication",
    ):
        await adjudicate_entity_resolution_clarification(
            _Conn(),
            clarification=clarification,
            answer={"action": "reject_candidate"},
            tenant_id=tenant_id,
            answered_by=uuid4(),
        )
