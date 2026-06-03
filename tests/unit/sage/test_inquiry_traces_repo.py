"""tests/unit/sage/test_inquiry_traces_repo.py — Phase 1 trace repos.

Direct repo tests for the three gap-filler tables introduced in
migration 0084 (retrieval_plans, omitted_evidence, inquiry_outcome_events).

Despite living under tests/unit, these tests touch a real Postgres
because the repos are thin wrappers over SQL — there is no business
logic worth mocking. They use the same `gateway_pool` fixture as
services/product/decision_deltas/tests (per-test fresh DB via TRUNCATE),
re-exported through services/app/gateway/tests/conftest.py. The
`pytest.mark.integration` marker keeps them out of any "pure unit"
selection that runs without a database.
"""
from __future__ import annotations

from uuid import UUID

import asyncpg
import pytest

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.reasoning.sage.inquiry_traces import (
    OmittedEvidenceRepo,
    OmittedEvidenceRow,
    OutcomeEventsRepo,
    RetrievalPlanRow,
    RetrievalPlansRepo,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


async def _seed_inquiry_session(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> UUID:
    """Insert a minimal inquiry_sessions row so FK references resolve.

    The schema (migration 0046) has a wide CHECK on `status` and
    `stop_status`; we pick the most innocuous values. The
    auto-register tenant trigger handles the parent `tenants` row.
    """
    session_id = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO inquiry_sessions (
              id, tenant_id, signal_ref_type, signal_ref_id,
              route, status, stop_status
            ) VALUES (
              $1, $2, 'internal', NULL,
              'DEEP_INQUIRY_PATH', 'running', 'insufficient_continue'
            )
            """,
            session_id,
            tenant_id,
        )
    return session_id


# ---------------------------------------------------------------------
# RetrievalPlansRepo
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retrieval_plans_insert_and_list_for_session(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    repo = RetrievalPlansRepo(gateway_pool, tenant_id=tenant_id)

    plan_a = RetrievalPlanRow(
        inquiry_session_id=session_id,
        question_id="q1",
        plan_revision=0,
        intents=[{"intent": "find_active_commitment", "target": "Acme"}],
        paths=[{"path": "exact"}, {"path": "structural"}],
        budgets={"max_evidence": 8, "max_seconds": 4.0},
        success_conditions=[{"kind": "has_status_node"}],
        notes={"reason": "initial plan"},
    )
    inserted_a = await repo.insert(plan_a)
    assert inserted_a.id is not None
    assert inserted_a.tenant_id == tenant_id
    assert inserted_a.question_id == "q1"
    assert inserted_a.plan_revision == 0
    assert inserted_a.intents == [
        {"intent": "find_active_commitment", "target": "Acme"},
    ]
    assert inserted_a.budgets["max_evidence"] == 8

    # Second plan for the same question (revision 1) + a plan for q2.
    plan_a_v2 = plan_a.model_copy(update={"plan_revision": 1, "intents": []})
    await repo.insert(plan_a_v2)
    plan_b = plan_a.model_copy(update={
        "question_id": "q2",
        "plan_revision": 0,
        "intents": [{"intent": "find_counterevidence"}],
    })
    await repo.insert(plan_b)

    listed = await repo.list_for_session(session_id)
    assert len(listed) == 3
    # ORDER BY question_id ASC, plan_revision ASC.
    assert [(p.question_id, p.plan_revision) for p in listed] == [
        ("q1", 0), ("q1", 1), ("q2", 0),
    ]


@pytest.mark.asyncio
async def test_retrieval_plans_unique_revision_per_question(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """The UNIQUE (session, question, revision) constraint is enforced
    at the DB layer. A duplicate insert should fail."""
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    repo = RetrievalPlansRepo(gateway_pool, tenant_id=tenant_id)

    plan = RetrievalPlanRow(
        inquiry_session_id=session_id,
        question_id="q1",
        plan_revision=0,
    )
    await repo.insert(plan)

    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        await repo.insert(plan.model_copy())


@pytest.mark.asyncio
async def test_retrieval_plans_rejects_empty_question_id(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    repo = RetrievalPlansRepo(gateway_pool, tenant_id=tenant_id)
    with pytest.raises(ValidationError):
        await repo.insert(
            RetrievalPlanRow(
                inquiry_session_id=session_id,
                question_id="   ",
            )
        )


# ---------------------------------------------------------------------
# OmittedEvidenceRepo
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_omitted_evidence_insert_and_list_for_session(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    repo = OmittedEvidenceRepo(gateway_pool, tenant_id=tenant_id)

    item_a = OmittedEvidenceRow(
        inquiry_session_id=session_id,
        question_id="q1",
        source_type="observation",
        source_ref="obs:1",
        source_ref_id=uuid7(),
        retrieval_paths=[{"path": "semantic"}],
        omission_reason="redundant",
        reason_detail="duplicate of obs:0",
        score=0.42,
        metadata={"covered_by": "obs:0"},
    )
    inserted_a = await repo.insert(item_a)
    assert inserted_a.id is not None
    assert inserted_a.tenant_id == tenant_id
    assert inserted_a.omission_reason == "redundant"
    assert inserted_a.score == pytest.approx(0.42)
    assert inserted_a.retrieval_paths == [{"path": "semantic"}]

    item_b = item_a.model_copy(update={
        "source_ref": "obs:2",
        "omission_reason": "generic_hub",
        "metadata": {},
    })
    await repo.insert(item_b)

    listed = await repo.list_for_session(session_id)
    assert len(listed) == 2
    assert {r.omission_reason for r in listed} == {"redundant", "generic_hub"}


@pytest.mark.asyncio
async def test_omitted_evidence_rejects_invalid_reason(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    repo = OmittedEvidenceRepo(gateway_pool, tenant_id=tenant_id)
    with pytest.raises(ValidationError):
        await repo.insert(
            OmittedEvidenceRow(
                inquiry_session_id=session_id,
                source_type="observation",
                source_ref="obs:1",
                omission_reason="not_a_real_reason",
            )
        )


# ---------------------------------------------------------------------
# OutcomeEventsRepo
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outcome_events_append_and_list_for_session(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    repo = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)

    e1 = await repo.append(
        session_id,
        "retrieved_evidence_used_in_packet",
        {"evidence_id": str(uuid7())},
    )
    assert e1.id is not None
    assert e1.event_type == "retrieved_evidence_used_in_packet"
    assert e1.tenant_id == tenant_id
    assert "evidence_id" in e1.payload

    await repo.append(session_id, "user_accepted_node", {"node_id": "n1"})
    await repo.append(session_id, "user_accepted_node", {"node_id": "n2"})
    await repo.append(session_id, "recommendation_ignored", {})

    all_events = await repo.list_for_session(session_id)
    assert len(all_events) == 4
    # Filter to just one type.
    accepted = await repo.list_for_session(
        session_id, event_type="user_accepted_node",
    )
    assert len(accepted) == 2
    assert all(e.event_type == "user_accepted_node" for e in accepted)


@pytest.mark.asyncio
async def test_outcome_events_aggregate_by_type(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    repo = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)

    await repo.append(session_id, "user_accepted_node", {})
    await repo.append(session_id, "user_accepted_node", {})
    await repo.append(session_id, "user_contested_node", {})
    await repo.append(session_id, "recommendation_acted_on", {})

    agg = await repo.aggregate_by_type(session_id)
    assert agg == {
        "user_accepted_node": 2,
        "user_contested_node": 1,
        "recommendation_acted_on": 1,
    }
    # An empty session aggregates to {} (not a dense zero-filled map).
    empty_session_id = await _seed_inquiry_session(
        gateway_pool, tenant_id=tenant_id,
    )
    assert await repo.aggregate_by_type(empty_session_id) == {}


@pytest.mark.asyncio
async def test_outcome_events_rejects_invalid_event_type(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    session_id = await _seed_inquiry_session(gateway_pool, tenant_id=tenant_id)
    repo = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    with pytest.raises(ValidationError):
        await repo.append(session_id, "not_a_real_event", {})
