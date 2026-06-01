"""tests/unit/sage/test_phase1_wiring.py — Phase 1 outcome-event wiring.

Drives the existing inquiry/validator/applier pipeline through their
SAGE trace integration points and asserts that the right rows land in
`retrieval_plans` / `omitted_evidence` / `inquiry_outcome_events`.

These tests share Wave 1's `gateway_pool` fixture (per-test fresh DB
via TRUNCATE) and are marked `pytest.mark.integration` because every
case hits real Postgres. Driving a full Think run end-to-end would
require an LLM stack, so we instead:

  * Hand-craft a synthetic `InquiryResult` and feed it through the
    real `_emit_phase1_traces` helper in `services/execution/inquiry`.
    This is exactly the code path `_persist_inquiry` reaches after the
    inquiry runs.
  * Install a `TraceContext` directly and call the public `emit_event`
    surface that validator/applier use, mirroring what the real
    pipeline does when it drops an op or applies a diff.

Pattern cribbed from `tests/unit/sage/test_inquiry_traces_repo.py`.
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from services.execution.inquiry import (
    EvidenceCard,
    Hypothesis,
    InquiryQuestion,
    InquiryResult,
    RetrievalAction,
    SufficiencyVerdict,
    _emit_phase1_traces,
)
from services.retrieval.primary import RetrievalResult, TriggerContext
from services.sage.inquiry_traces import (
    OmittedEvidenceRepo,
    OutcomeEventsRepo,
    RetrievalPlansRepo,
    TraceContext,
    emit_event,
    emit_events_batch,
    emit_omitted_evidence,
    emit_retrieval_plan,
    emission_enabled,
    reset_trace_context,
    set_trace_context,
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


def _make_trigger(tenant_id: UUID) -> TriggerContext:
    """Minimal TriggerContext for the synthetic inquiry result."""
    return TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=None,
        seed_natural_text="phase 1 wiring synthetic trigger",
        seed_occurred_at=datetime.now(timezone.utc),
        seed_entity_ids=[],
        scope_actors=[],
        seed_signature=None,
        precomputed_seed_vector=None,
    )


def _make_evidence_card(
    *,
    source_type: str = "observation",
    summary: str = "synthetic evidence",
    supports: tuple[str, ...] = (),
    score: float = 0.5,
    paths: tuple[str, ...] = ("semantic",),
    questions: tuple[str, ...] = ("Q1",),
) -> EvidenceCard:
    ev_id = uuid7()
    return EvidenceCard(
        evidence_id=ev_id,
        source_type=source_type,
        source_ref=f"{source_type}:{ev_id}",
        source_ref_id=ev_id,
        summary=summary,
        trust_tier="authoritative",
        timestamp=datetime.now(timezone.utc),
        retrieval_paths=set(paths),
        retrieved_for_questions=set(questions),
        supports_hypotheses=set(supports),
        weakens_hypotheses=set(),
        contradicts_hypotheses=set(),
        raw_content_ref=f"{source_type}:{ev_id}",
        token_estimate=10,
        access_scope="tenant",
        sensitivity="normal",
        score=score,
    )


def _make_inquiry_result(
    *,
    tenant_id: UUID,
    session_id: UUID,
    used_cards: list[EvidenceCard],
    omitted_cards: list[EvidenceCard],
) -> InquiryResult:
    question = InquiryQuestion(
        question_id="Q1",
        question="What is the dependency for Acme?",
        primitive="DEPENDENCY",
        tests_hypotheses=("H0",),
        expected_value=0.6,
        expected_cost=0.3,
        retrieval_target="dependency_graph",
        stop_condition="dependency_resolved",
        score=0.7,
        round_index=1,
    )
    action = RetrievalAction(
        question_id="Q1",
        path="semantic",
        target="dependency_evidence",
        query="dep query",
        filters={},
        budget=25,
    )
    sufficiency = SufficiencyVerdict(
        status="sufficient_for_reasoning",
        reason="testing",
        evidence_count=len(used_cards) + len(omitted_cards),
        answered_questions=1,
        remaining_unknowns=(),
    )
    decisive_items = [
        {
            "evidence_id": str(c.evidence_id),
            "source_type": c.source_type,
            "source_ref": c.source_ref,
            "summary": c.summary,
            "token_estimate": c.token_estimate,
        }
        for c in used_cards
    ]
    packet = {
        "signal_summary": "synthetic",
        "tiers": {
            "decisive_evidence": decisive_items,
            "supporting_evidence_groups": [],
            "background_summaries": [],
            "omission_ledger": [],
        },
        "budget": {
            "token_budget": 1000,
            "estimated_tokens_used": 100,
            "reservoir_evidence_count": len(used_cards) + len(omitted_cards),
        },
    }
    trigger = _make_trigger(tenant_id)
    combined = RetrievalResult(
        trigger=trigger,
        models=[],
        observations=[],
        acts={},
        resources=[],
        model_scores={},
        notes={},
    )
    return InquiryResult(
        session_id=session_id,
        route="DEEP_INQUIRY_PATH",
        retrieval_result=combined,
        hypotheses=(Hypothesis("H0", "claim", 0.5, "ship"),),
        questions=(question,),
        retrieval_actions=(action,),
        question_answers=(),
        evidence_cards=tuple(used_cards + omitted_cards),
        sufficiency=sufficiency,
        context_packet=packet,
        notes={},
    )


@pytest_asyncio.fixture(autouse=True)
async def _clear_sage_trace_env(monkeypatch):
    """Make sure SAGE_TRACE_EMIT defaults to ON for every test in this
    module unless a specific test overrides it. We pop any operator-set
    value to keep determinism across machines."""
    monkeypatch.delenv("SAGE_TRACE_EMIT", raising=False)
    yield


# ---------------------------------------------------------------------
# emit_event — best-effort, gated, context-aware
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_emit_event_no_op_without_context(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """emit_event must silently do nothing when no TraceContext is
    installed. This is the safety net for unit tests and code paths
    that aren't inside a Think run."""
    # Sanity: no context.
    await emit_event(
        "retrieved_evidence_used_in_packet",
        {"evidence_id": str(uuid7())},
    )
    # If the line above writes anything, the next aggregate would show
    # rows. Use a fresh inquiry session as the join key.
    session_id = await _seed_inquiry_session(
        gateway_pool, tenant_id=tenant_id,
    )
    repo = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    assert await repo.aggregate_by_type(session_id) == {}


@pytest.mark.asyncio
async def test_emit_event_writes_with_context(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    session_id = await _seed_inquiry_session(
        gateway_pool, tenant_id=tenant_id,
    )
    ctx = TraceContext(
        tenant_id=tenant_id,
        inquiry_session_id=session_id,
        pool=gateway_pool,
    )
    token = set_trace_context(ctx)
    try:
        await emit_event(
            "retrieved_evidence_used_in_packet",
            {"evidence_id": str(uuid7())},
        )
        await emit_event(
            "node_used_in_valid_diff",
            {"model_id": str(uuid7())},
        )
    finally:
        reset_trace_context(token)

    repo = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    agg = await repo.aggregate_by_type(session_id)
    assert agg == {
        "retrieved_evidence_used_in_packet": 1,
        "node_used_in_valid_diff": 1,
    }


@pytest.mark.asyncio
async def test_sage_trace_emit_env_disables_emission(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    monkeypatch,
):
    """Setting SAGE_TRACE_EMIT=0 must short-circuit every emit helper."""
    session_id = await _seed_inquiry_session(
        gateway_pool, tenant_id=tenant_id,
    )
    ctx = TraceContext(
        tenant_id=tenant_id,
        inquiry_session_id=session_id,
        pool=gateway_pool,
    )

    monkeypatch.setenv("SAGE_TRACE_EMIT", "0")
    assert emission_enabled() is False

    token = set_trace_context(ctx)
    try:
        await emit_event(
            "retrieved_evidence_used_in_packet",
            {"evidence_id": str(uuid7())},
        )
        await emit_retrieval_plan(
            question_id="Q1",
            plan_revision=0,
            intents=[{"primitive": "DEPENDENCY"}],
        )
        await emit_omitted_evidence(
            source_type="observation",
            source_ref="obs:1",
            omission_reason="redundant",
        )
        await emit_events_batch([
            ("user_accepted_node", {"node_id": "n1"}),
        ])
    finally:
        reset_trace_context(token)

    plans = await RetrievalPlansRepo(
        gateway_pool, tenant_id=tenant_id,
    ).list_for_session(session_id)
    omitted = await OmittedEvidenceRepo(
        gateway_pool, tenant_id=tenant_id,
    ).list_for_session(session_id)
    events = await OutcomeEventsRepo(
        gateway_pool, tenant_id=tenant_id,
    ).aggregate_by_type(session_id)
    assert plans == []
    assert omitted == []
    assert events == {}


@pytest.mark.asyncio
async def test_emit_swallows_repo_failure_without_crashing(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    monkeypatch,
):
    """A forced exception inside the OutcomeEventsRepo append must be
    swallowed (warning-logged) so the pipeline keeps running."""
    session_id = await _seed_inquiry_session(
        gateway_pool, tenant_id=tenant_id,
    )

    async def _boom(self, *args, **kwargs):
        raise RuntimeError("forced repo failure")

    monkeypatch.setattr(OutcomeEventsRepo, "append", _boom)

    ctx = TraceContext(
        tenant_id=tenant_id,
        inquiry_session_id=session_id,
        pool=gateway_pool,
    )
    token = set_trace_context(ctx)
    try:
        # Must NOT raise.
        await emit_event(
            "retrieved_evidence_used_in_packet",
            {"evidence_id": str(uuid7())},
        )
    finally:
        reset_trace_context(token)

    # And no row landed (the forced exception ate it).
    rows = await OutcomeEventsRepo(
        gateway_pool, tenant_id=tenant_id,
    ).list_for_session(session_id)
    assert rows == []


@pytest.mark.asyncio
async def test_conn_backed_emit_sql_failure_does_not_abort_outer_transaction(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
):
    """A real SQL failure during best-effort emission must roll back to
    a savepoint, not poison the caller's transaction."""
    session_id = await _seed_inquiry_session(
        gateway_pool, tenant_id=tenant_id,
    )

    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            ctx = TraceContext(
                tenant_id=tenant_id,
                inquiry_session_id=session_id,
                conn=conn,
            )
            token = set_trace_context(ctx)
            try:
                await emit_retrieval_plan(
                    question_id="Q1",
                    plan_revision=0,
                    intents=[{"primitive": "DEPENDENCY"}],
                )
                # Same (session, question_id, revision) violates the
                # retrieval_plans UNIQUE constraint. emit_retrieval_plan
                # must swallow it and leave this transaction usable.
                await emit_retrieval_plan(
                    question_id="Q1",
                    plan_revision=0,
                    intents=[{"primitive": "DEPENDENCY"}],
                )
                assert await conn.fetchval("SELECT 1") == 1
            finally:
                reset_trace_context(token)

    plans = await RetrievalPlansRepo(
        gateway_pool, tenant_id=tenant_id,
    ).list_for_session(session_id)
    assert len(plans) == 1


# ---------------------------------------------------------------------
# _emit_phase1_traces — drives the inquiry-side wiring end-to-end
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_emit_phase1_traces_writes_plans_omissions_and_events(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """Drive the production helper that `_persist_inquiry` calls. The
    helper should land:
      * one `retrieval_plans` row per question
      * one `omitted_evidence` row per packet-omitted evidence card
      * `retrieved_evidence_used_in_packet` for each card in the packet
      * `retrieved_evidence_omitted` for each excluded card
    """
    session_id = await _seed_inquiry_session(
        gateway_pool, tenant_id=tenant_id,
    )

    used_a = _make_evidence_card(
        source_type="observation",
        summary="this evidence makes the packet",
        supports=("H0",),
        score=0.92,
    )
    used_b = _make_evidence_card(
        source_type="commitment",
        summary="commitment used in packet",
        supports=("H0",),
        score=0.81,
    )
    omitted_noise = _make_evidence_card(
        source_type="model",
        summary="generic hub model with no hypothesis link",
        supports=(),
        score=0.04,
    )
    omitted_dup = _make_evidence_card(
        source_type="observation",
        summary="redundant supporting evidence",
        supports=("H0",),
        score=0.30,
    )

    result = _make_inquiry_result(
        tenant_id=tenant_id,
        session_id=session_id,
        used_cards=[used_a, used_b],
        omitted_cards=[omitted_noise, omitted_dup],
    )

    async with gateway_pool.acquire() as conn:
        await _emit_phase1_traces(conn, result, _make_trigger(tenant_id))

    plans = await RetrievalPlansRepo(
        gateway_pool, tenant_id=tenant_id,
    ).list_for_session(session_id)
    assert [p.question_id for p in plans] == ["Q1"]
    assert plans[0].plan_revision == 0
    # The plan should remember the planning context.
    intents = plans[0].intents
    assert intents and intents[0]["primitive"] == "DEPENDENCY"
    paths = plans[0].paths
    assert paths and paths[0]["path"] == "semantic"

    omitted = await OmittedEvidenceRepo(
        gateway_pool, tenant_id=tenant_id,
    ).list_for_session(session_id)
    assert len(omitted) == 2
    reasons = {o.omission_reason for o in omitted}
    # generic_hub for the noise model; the other goes to redundant
    # since the packet budget is not near the cap.
    assert "generic_hub" in reasons
    assert "redundant" in reasons

    events_repo = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    agg = await events_repo.aggregate_by_type(session_id)
    # Two used, two omitted.
    assert agg.get("retrieved_evidence_used_in_packet") == 2
    assert agg.get("retrieved_evidence_omitted") == 2


@pytest.mark.asyncio
async def test_emit_phase1_traces_disabled_by_env(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    monkeypatch,
):
    """SAGE_TRACE_EMIT=0 must skip the entire Phase 1 emission block
    (no plan rows, no omitted-evidence rows, no events)."""
    monkeypatch.setenv("SAGE_TRACE_EMIT", "0")
    session_id = await _seed_inquiry_session(
        gateway_pool, tenant_id=tenant_id,
    )

    used = _make_evidence_card(source_type="observation")
    omitted = _make_evidence_card(source_type="model")

    result = _make_inquiry_result(
        tenant_id=tenant_id,
        session_id=session_id,
        used_cards=[used],
        omitted_cards=[omitted],
    )

    async with gateway_pool.acquire() as conn:
        await _emit_phase1_traces(conn, result, _make_trigger(tenant_id))

    plans = await RetrievalPlansRepo(
        gateway_pool, tenant_id=tenant_id,
    ).list_for_session(session_id)
    omits = await OmittedEvidenceRepo(
        gateway_pool, tenant_id=tenant_id,
    ).list_for_session(session_id)
    agg = await OutcomeEventsRepo(
        gateway_pool, tenant_id=tenant_id,
    ).aggregate_by_type(session_id)
    assert plans == []
    assert omits == []
    assert agg == {}


@pytest.mark.asyncio
async def test_emit_phase1_traces_handles_failing_event_writes(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    monkeypatch,
):
    """Force the outcome-event append to fail mid-batch and confirm
    _emit_phase1_traces does NOT raise (the pipeline must complete).
    Plans and omissions should still land because those use different
    repos that we leave intact."""
    session_id = await _seed_inquiry_session(
        gateway_pool, tenant_id=tenant_id,
    )

    async def _boom(self, *args, **kwargs):
        raise RuntimeError("induced outcome-event failure")

    monkeypatch.setattr(OutcomeEventsRepo, "append", _boom)

    used = _make_evidence_card(source_type="observation")
    omitted = _make_evidence_card(
        source_type="model", summary="generic hub", supports=(),
    )
    result = _make_inquiry_result(
        tenant_id=tenant_id,
        session_id=session_id,
        used_cards=[used],
        omitted_cards=[omitted],
    )

    async with gateway_pool.acquire() as conn:
        # Must NOT raise.
        await _emit_phase1_traces(conn, result, _make_trigger(tenant_id))

    plans = await RetrievalPlansRepo(
        gateway_pool, tenant_id=tenant_id,
    ).list_for_session(session_id)
    omits = await OmittedEvidenceRepo(
        gateway_pool, tenant_id=tenant_id,
    ).list_for_session(session_id)
    # Plans + omissions repos still work; events were swallowed.
    assert len(plans) == 1
    assert len(omits) == 1


# ---------------------------------------------------------------------
# Validator → outcome-event mapping
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validation_drop_event_type_mapping():
    """The internal classifier should map bad-reference reasons to
    validation_failed_due_to_bad_reference and everything else to
    validation_failed_due_to_missing_evidence."""
    from services.think.validator import _outcome_event_for_drop_reason

    assert _outcome_event_for_drop_reason("missing_model_reference") == (
        "validation_failed_due_to_bad_reference"
    )
    assert _outcome_event_for_drop_reason("invalid_entity_reference") == (
        "validation_failed_due_to_bad_reference"
    )
    assert _outcome_event_for_drop_reason("missing_entity_reference") == (
        "validation_failed_due_to_bad_reference"
    )
    assert _outcome_event_for_drop_reason("inadequate_falsifier") == (
        "validation_failed_due_to_missing_evidence"
    )
    assert _outcome_event_for_drop_reason("unclassified") == (
        "validation_failed_due_to_missing_evidence"
    )


@pytest.mark.asyncio
async def test_validator_emit_helper_writes_event_under_context(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """The validator's `_emit_validation_drop_event` private helper is
    the call site that fires on every dropped op; assert it threads
    through the public emitter and writes the correct event type."""
    from services.think.validator import _emit_validation_drop_event

    session_id = await _seed_inquiry_session(
        gateway_pool, tenant_id=tenant_id,
    )
    ctx = TraceContext(
        tenant_id=tenant_id,
        inquiry_session_id=session_id,
        pool=gateway_pool,
    )
    token = set_trace_context(ctx)
    try:
        await _emit_validation_drop_event(
            op_type="claim",
            op_kind="insert",
            reason="inadequate_falsifier",
            error_message="missing falsifier",
        )
        await _emit_validation_drop_event(
            op_type="claim",
            op_kind="update",
            reason="missing_model_reference",
            error_message="model abc not found",
        )
    finally:
        reset_trace_context(token)

    repo = OutcomeEventsRepo(gateway_pool, tenant_id=tenant_id)
    agg = await repo.aggregate_by_type(session_id)
    assert agg == {
        "validation_failed_due_to_missing_evidence": 1,
        "validation_failed_due_to_bad_reference": 1,
    }
