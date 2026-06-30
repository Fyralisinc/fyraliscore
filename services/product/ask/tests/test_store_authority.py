from __future__ import annotations

from uuid import UUID

import pytest

from lib.shared.ids import uuid7
from services.platform.access_control.authority import (
    AuthorityDecision,
    Principal,
)
from services.product.ask import store as store_module
from services.product.ask.schemas import AskEvidenceItem
from services.product.ask.store import (
    _enqueue_accepted_answer_writeback,
    _record_ask_evidence_authority,
)


pytestmark = pytest.mark.asyncio


class _FakeConn:
    def __init__(
        self,
        *,
        answer_row: dict | None = None,
        evidence_rows: list[dict] | None = None,
    ) -> None:
        self.answer_row = answer_row
        self.evidence_rows = evidence_rows or []
        self.executed: list[tuple[str, tuple]] = []

    async def fetchrow(self, query: str, *args):
        if "FROM ask_answers" in query:
            return self.answer_row
        return None

    async def fetch(self, query: str, *args):
        if "FROM ask_evidence_items" in query:
            return self.evidence_rows
        return []

    async def execute(self, query: str, *args):
        self.executed.append((query, args))
        return "INSERT 0 1"


def _answer_row(tenant_id: UUID, retrieval_run_id: UUID) -> dict:
    return {
        "tenant_id": tenant_id,
        "current_scope": {"type": "whole_company"},
        "answer_payload": {"answer": "Budget is tight.", "confidence": 0.91},
        "confidence": 0.91,
        "retrieval_run_id": retrieval_run_id,
    }


def _evidence_row(
    *,
    source_kind: str,
    source_ref: UUID | None,
    summary: str,
    raw_payload: dict | None = None,
) -> dict:
    return {
        "source_ref": source_ref,
        "source_kind": source_kind,
        "summary": summary,
        "strength": "supporting",
        "supports_answer": True,
        "is_counterevidence": False,
        "raw_payload": raw_payload or {},
    }


async def test_accepted_answer_writeback_filters_evidence_by_live_authority(
    monkeypatch,
):
    tenant = uuid7()
    viewer = uuid7()
    session_id = uuid7()
    answer_id = uuid7()
    retrieval_run_id = uuid7()
    allowed_obs = uuid7()
    secret_model = uuid7()
    conn = _FakeConn(
        answer_row=_answer_row(tenant, retrieval_run_id),
        evidence_rows=[
            _evidence_row(
                source_kind="observation",
                source_ref=allowed_obs,
                summary="Allowed support.",
            ),
            _evidence_row(
                source_kind="model",
                source_ref=secret_model,
                summary="Secret finance ARR is $10M.",
            ),
            _evidence_row(
                source_kind="composed_chain",
                source_ref=None,
                summary="Mixed chain.",
                raw_payload={
                    "source_observation_ids": [str(allowed_obs), str(secret_model)]
                },
            ),
        ],
    )
    enqueued: list[dict] = []

    async def fake_principal_for_actor(actor_id, *, conn, tenant_id):
        return Principal(tenant_id=tenant_id, actor_id=actor_id)

    async def fake_authorize_read(principal, purpose, object_ref, *, conn):
        if object_ref.object_id == secret_model:
            return AuthorityDecision(False, "model_out_of_scope")
        return AuthorityDecision(True, "authorized")

    async def fake_enqueue_trigger(conn, *, tenant_id, trigger_kind, trigger_subkind, payload):
        enqueued.append(payload)

    monkeypatch.setattr(store_module, "principal_for_actor", fake_principal_for_actor)
    monkeypatch.setattr(store_module, "authorize_read", fake_authorize_read)
    monkeypatch.setattr(store_module, "enqueue_trigger", fake_enqueue_trigger)

    await _enqueue_accepted_answer_writeback(
        conn,  # type: ignore[arg-type]
        feedback_id=uuid7(),
        session_id=session_id,
        answer_id=answer_id,
        viewer_id=viewer,
        payload={"rating": "helpful"},
    )

    assert len(enqueued) == 1
    assert [row["summary"] for row in enqueued[0]["provenance"]] == [
        "Allowed support."
    ]


async def test_accepted_answer_writeback_suppresses_when_all_evidence_denied(
    monkeypatch,
):
    tenant = uuid7()
    viewer = uuid7()
    retrieval_run_id = uuid7()
    secret_model = uuid7()
    conn = _FakeConn(
        answer_row=_answer_row(tenant, retrieval_run_id),
        evidence_rows=[
            _evidence_row(
                source_kind="model",
                source_ref=secret_model,
                summary="Secret finance ARR is $10M.",
            ),
        ],
    )
    enqueued: list[dict] = []

    async def fake_principal_for_actor(actor_id, *, conn, tenant_id):
        return Principal(tenant_id=tenant_id, actor_id=actor_id)

    async def fake_authorize_read(principal, purpose, object_ref, *, conn):
        return AuthorityDecision(False, "model_out_of_scope")

    async def fake_enqueue_trigger(conn, *, tenant_id, trigger_kind, trigger_subkind, payload):
        enqueued.append(payload)

    monkeypatch.setattr(store_module, "principal_for_actor", fake_principal_for_actor)
    monkeypatch.setattr(store_module, "authorize_read", fake_authorize_read)
    monkeypatch.setattr(store_module, "enqueue_trigger", fake_enqueue_trigger)

    await _enqueue_accepted_answer_writeback(
        conn,  # type: ignore[arg-type]
        feedback_id=uuid7(),
        session_id=uuid7(),
        answer_id=uuid7(),
        viewer_id=viewer,
        payload={"rating": "helpful"},
    )

    assert enqueued == []


async def test_ask_evidence_records_provenance_and_inherited_labels():
    tenant = uuid7()
    model = uuid7()
    evidence = AskEvidenceItem(
        id=uuid7(),
        source_ref=model,
        source_kind="omitted_model",
        summary="Omitted evidence.",
        omitted_reason="budget_exhausted",
    )
    conn = _FakeConn()

    await _record_ask_evidence_authority(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant,
        item=evidence,
    )

    assert len(conn.executed) == 2
    provenance_args = conn.executed[0][1]
    label_args = conn.executed[1][1]
    assert provenance_args[0:6] == (
        tenant,
        "evidence",
        evidence.id,
        "model",
        model,
        "ask_evidence_source",
    )
    assert label_args[0:4] == (
        tenant,
        "evidence",
        evidence.id,
        "ask_evidence_source",
    )
    assert label_args[4] == ["model"]
    assert label_args[5] == [model]
