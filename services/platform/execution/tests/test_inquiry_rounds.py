from __future__ import annotations

import asyncio
from datetime import datetime
from uuid import UUID, uuid4

import pytest

from services.platform.execution import inquiry, inquiry_bootstrap, inquiry_rounds
from services.platform.execution.config import InquiryConfig
from services.platform.execution.types import EvidenceCard, InquiryQuestion, RetrievalAction
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext


class _NoPolicyConn:
    async def fetchval(self, *_args: object, **_kwargs: object) -> None:
        return None

    async def fetch(self, *_args: object, **_kwargs: object) -> list[object]:
        return []


class _AcquireConn:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    async def __aenter__(self) -> object:
        return self._conn

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        tb: object,
    ) -> None:
        return None


class _ReadPool:
    def __init__(self, conn: object) -> None:
        self.conn = conn
        self.acquires = 0

    def acquire(self) -> _AcquireConn:
        self.acquires += 1
        return _AcquireConn(self.conn)


def _weak_noop_trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
        seed_entity_ids=[],
        seed_natural_text=(
            "Workspace chatter: lunch notes, travel plans, and general team "
            "coordination. No blocker, no owner change, no decision."
        ),
        seed_occurred_at=None,
        scope_actors=[],
    )


def _question(question_id: str, primitive: str = "DEPENDENCY") -> InquiryQuestion:
    return InquiryQuestion(
        question_id=question_id,
        question=f"What evidence answers {question_id}?",
        primitive=primitive,
        tests_hypotheses=("H1",),
        expected_value=0.8,
        expected_cost=0.2,
        retrieval_target=f"target {question_id}",
        stop_condition="find evidence",
        score=0.7,
    )


def _model_evidence(index: int) -> EvidenceCard:
    return EvidenceCard(
        evidence_id=uuid4(),
        source_type="model",
        source_ref=f"model:{index}",
        source_ref_id=uuid4(),
        summary=f"Relevant model evidence {index}",
        trust_tier="model",
        timestamp=datetime(2026, 1, 1),
        retrieval_paths={"baseline"},
        retrieved_for_questions={"Q0"},
        supports_hypotheses={"H1"},
        score=0.8,
    )


def test_inquiry_imports_round_phase_from_canonical_module() -> None:
    assert inquiry._execute_inquiry_rounds is inquiry_rounds._execute_inquiry_rounds


def test_broad_action_gate_keeps_nearby_temporal_but_gates_broad_temporal() -> None:
    nearby = RetrievalAction(
        "Q1",
        "temporal",
        "nearby_counterevidence",
        filters={"_temporal_lane": "nearby"},
    )
    broad = RetrievalAction(
        "Q1",
        "temporal",
        "recent_counterevidence",
        filters={"_temporal_lane": "broad"},
    )

    actions, skipped = inquiry_rounds._split_gated_actions(
        _question("Q1", "COUNTEREVIDENCE"),
        [nearby, broad],
        action_gate_scope="broad",
        action_gate_reason="sage_reader_focused_route",
    )

    assert actions == [nearby]
    assert [note["target"] for note in skipped] == ["recent_counterevidence"]


def _bootstrap_state(
    trigger: TriggerContext,
    *,
    cfg: InquiryConfig | None = None,
) -> inquiry_bootstrap._InquiryBootstrapState:
    return inquiry_bootstrap._InquiryBootstrapState(
        cfg=cfg or InquiryConfig(max_rounds=1),
        route="DEEP_INQUIRY_PATH",
        session_id=UUID("00000000-0000-0000-0000-000000000012"),
        candidate_top_n=20,
        effective_top_n=10,
        signal_class="material",
        weak_signal=False,
        cold_weak_noop_gate={"used": False},
        baseline_top_n=20,
        stage_timing_notes=[],
        baseline=RetrievalResult(trigger),
        hypotheses=(),
        evidence_by_key={},
        all_questions=[],
        all_actions=[],
        action_cache={},
        baseline_action_cache_notes={},
        action_timing_notes=[],
        answers=[],
        retrieval_results=[],
        unknowns=set(),
        question_planning_notes=[],
        reconstruction_notes=[],
        question_policy={},
        reflective_rules=(),
        sage_reader_notes={"questions": {}, "signatures": []},
        sage_reader_runtime=None,
        sage_reader_substrate=None,
        max_rounds=1,
    )


def test_build_question_read_plan_records_sage_policy_action_stages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trigger = _weak_noop_trigger()
    question = _question("Q1", primitive="CONSTRAINT")

    def fake_compile_retrieval_plan(*_args: object, **_kwargs: object):
        return [
            RetrievalAction(
                "Q1",
                "focused_index",
                "question_answerability_scope",
                budget=40,
            ),
            RetrievalAction("Q1", "semantic", "constraint_evidence", budget=30),
        ]

    monkeypatch.setattr(
        inquiry_rounds,
        "compile_retrieval_plan",
        fake_compile_retrieval_plan,
    )
    state = _bootstrap_state(
        trigger,
        cfg=InquiryConfig(
            sage_retrieval_policy_enabled=True,
            sage_retrieval_policy_semantic_budget_floor=8,
        ),
    )

    plan = inquiry_rounds._build_question_read_plan(
        state,
        trigger=trigger,
        question=question,
        sage_result=None,
        learned_motif=None,
        reconstruction_state=None,
    )

    assert [action.filters["_sage_policy_stage"] for action in plan.actions_to_run] == [
        1,
        2,
    ]
    assert plan.actions_to_run[1].filters["_sage_policy_mode"] == "probe"
    assert plan.actions_to_run[1].budget == 18
    assert state.sage_reader_notes["retrieval_policy_actions"]["Q1"][1] == {
        "path": "semantic",
        "target": "constraint_evidence",
        "mode": "probe",
        "stage": 2,
        "budget": 18,
        "reason": "semantic_probe_after_structural_first_actions",
    }


@pytest.mark.asyncio
async def test_select_questions_uses_smaller_budget_when_primary_context_is_rich(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trigger = _weak_noop_trigger()
    state = _bootstrap_state(
        trigger,
        cfg=InquiryConfig(
            questions_per_round=3,
            adaptive_question_budget_enabled=True,
            adaptive_strong_context_question_limit=2,
            adaptive_strong_context_min_evidence=3,
            adaptive_strong_context_min_models=3,
        ),
    )
    state.baseline.models = [object(), object(), object()]  # type: ignore[list-item]
    for index in range(6):
        card = _model_evidence(index)
        state.evidence_by_key[(card.source_type, str(card.evidence_id))] = card

    async def fake_candidates(*_args: object, **_kwargs: object):
        return (
            [
                _question("Q1", "DEPENDENCY"),
                _question("Q2", "OWNERSHIP"),
                _question("Q3", "COUNTEREVIDENCE"),
            ],
            {"mode": "deterministic_fallback"},
        )

    monkeypatch.setattr(
        inquiry_rounds,
        "candidate_questions_for_round",
        fake_candidates,
    )

    selected = await inquiry_rounds._select_questions_for_round(
        state,
        trigger=trigger,
        llm_provider=None,
        round_index=1,
        reconstruction_state=None,
    )

    assert len(selected) == 2
    budget_note = state.question_planning_notes[-1]["adaptive_question_budget"]
    assert budget_note["applied"] is True
    assert budget_note["reason"] == "primary_context_already_rich"


@pytest.mark.asyncio
async def test_execute_inquiry_rounds_preserves_no_round_default_status() -> None:
    trigger = _weak_noop_trigger()
    state = await inquiry_bootstrap._bootstrap_inquiry_run(
        trigger=trigger,
        conn=_NoPolicyConn(),
        embedder=None,
        read_pool=None,
        route=None,
        mode="deep",
        top_n=64,
        config=InquiryConfig(max_rounds=2),
    )

    status = await inquiry_rounds._execute_inquiry_rounds(
        state,
        trigger=trigger,
        conn=_NoPolicyConn(),
        embedder=None,
        llm_provider=None,
        read_pool=None,
    )

    assert state.max_rounds == 0
    assert status.stop_status == "insufficient_continue"
    assert status.stop_reason == "inquiry has not run"
    assert state.all_questions == []
    assert state.answers == []


@pytest.mark.asyncio
async def test_execute_inquiry_round_overlaps_sage_and_motif_read_prep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trigger = _weak_noop_trigger()
    selected = [_question("Q1"), _question("Q2", "OWNERSHIP")]
    sage_started = asyncio.Event()
    motifs_started = asyncio.Event()
    events: list[str] = []

    async def fake_select(*_args: object, **_kwargs: object):
        return selected

    async def fake_sage(*_args: object, **_kwargs: object):
        events.append("sage_started")
        sage_started.set()
        await motifs_started.wait()
        events.append("sage_finished")
        return {}, {"used": True, "parallel": True}

    async def fake_motifs(*_args: object, **_kwargs: object):
        await sage_started.wait()
        events.append("motifs_started")
        motifs_started.set()
        return {}

    def fake_build_plans(*_args: object, **_kwargs: object):
        return [
            inquiry_rounds._QuestionRetrievalPlan(question=question)
            for question in selected
        ]

    async def fake_execute_actions(*_args: object, **_kwargs: object):
        return {}

    def fake_apply_results(*_args: object, **_kwargs: object):
        return inquiry_rounds._InquiryRoundStatus(
            "sufficient_for_reasoning",
            "test complete",
        )

    monkeypatch.setattr(inquiry_rounds, "_select_questions_for_round", fake_select)
    monkeypatch.setattr(
        inquiry_rounds,
        "_execute_sage_reader_actions_for_round",
        fake_sage,
    )
    monkeypatch.setattr(
        inquiry_rounds,
        "load_retrieval_motifs_for_questions",
        fake_motifs,
    )
    monkeypatch.setattr(inquiry_rounds, "_build_question_read_plans", fake_build_plans)
    monkeypatch.setattr(
        inquiry_rounds,
        "_execute_question_retrieval_actions",
        fake_execute_actions,
    )
    monkeypatch.setattr(
        inquiry_rounds,
        "_apply_question_read_plan_results",
        fake_apply_results,
    )
    state = inquiry_bootstrap._InquiryBootstrapState(
        cfg=InquiryConfig(
            max_rounds=1,
            read_prep_parallel_enabled=True,
            round_action_pipeline_enabled=False,
            sage_reader_enabled=True,
            sage_reader_parallel_enabled=True,
            sage_reader_parallelism=2,
        ),
        route="DEEP_INQUIRY_PATH",
        session_id=UUID("00000000-0000-0000-0000-000000000010"),
        candidate_top_n=20,
        effective_top_n=10,
        signal_class="material",
        weak_signal=False,
        cold_weak_noop_gate={"used": False},
        baseline_top_n=20,
        stage_timing_notes=[],
        baseline=RetrievalResult(trigger),
        hypotheses=(),
        evidence_by_key={},
        all_questions=[],
        all_actions=[],
        action_cache={},
        baseline_action_cache_notes={},
        action_timing_notes=[],
        answers=[],
        retrieval_results=[],
        unknowns=set(),
        question_planning_notes=[],
        reconstruction_notes=[],
        question_policy={},
        reflective_rules=(),
        sage_reader_notes={"questions": {}, "signatures": []},
        sage_reader_runtime=object(),
        sage_reader_substrate=None,
        max_rounds=1,
    )

    status = await asyncio.wait_for(
        inquiry_rounds._execute_inquiry_round(
            state,
            trigger=trigger,
            conn=_NoPolicyConn(),
            embedder=None,
            llm_provider=None,
            read_pool=object(),
            round_index=1,
        ),
        timeout=1.0,
    )

    assert status.stop_status == "sufficient_for_reasoning"
    assert events == ["sage_started", "motifs_started", "sage_finished"]
    assert state.stage_timing_notes[-1]["stage"] == "round_read_prep_parallel"


@pytest.mark.asyncio
async def test_execute_inquiry_round_overlaps_single_question_sage_and_motifs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trigger = _weak_noop_trigger()
    selected = [_question("Q1")]
    motif_conn = object()
    read_pool = _ReadPool(motif_conn)
    sage_started = asyncio.Event()
    motifs_started = asyncio.Event()
    events: list[str] = []

    async def fake_select(*_args: object, **_kwargs: object):
        return selected

    async def fake_sage(*_args: object, **_kwargs: object):
        events.append("sage_started")
        sage_started.set()
        await asyncio.wait_for(motifs_started.wait(), timeout=0.5)
        events.append("sage_finished")
        return {}, {"used": True, "parallel": False}

    async def fake_motifs(conn: object, *_args: object, **_kwargs: object):
        assert conn is motif_conn
        await asyncio.wait_for(sage_started.wait(), timeout=0.5)
        events.append("motifs_started")
        motifs_started.set()
        return {}

    def fake_build_plans(*_args: object, **_kwargs: object):
        return [inquiry_rounds._QuestionRetrievalPlan(question=selected[0])]

    async def fake_execute_actions(*_args: object, **_kwargs: object):
        return {}

    def fake_apply_results(*_args: object, **_kwargs: object):
        return inquiry_rounds._InquiryRoundStatus(
            "sufficient_for_reasoning",
            "test complete",
        )

    monkeypatch.setattr(inquiry_rounds, "_select_questions_for_round", fake_select)
    monkeypatch.setattr(
        inquiry_rounds,
        "_execute_sage_reader_actions_for_round",
        fake_sage,
    )
    monkeypatch.setattr(
        inquiry_rounds,
        "load_retrieval_motifs_for_questions",
        fake_motifs,
    )
    monkeypatch.setattr(inquiry_rounds, "_build_question_read_plans", fake_build_plans)
    monkeypatch.setattr(
        inquiry_rounds,
        "_execute_question_retrieval_actions",
        fake_execute_actions,
    )
    monkeypatch.setattr(
        inquiry_rounds,
        "_apply_question_read_plan_results",
        fake_apply_results,
    )
    state = inquiry_bootstrap._InquiryBootstrapState(
        cfg=InquiryConfig(
            max_rounds=1,
            read_prep_parallel_enabled=True,
            round_action_pipeline_enabled=False,
            sage_reader_enabled=True,
            sage_reader_parallel_enabled=False,
        ),
        route="DEEP_INQUIRY_PATH",
        session_id=UUID("00000000-0000-0000-0000-000000000012"),
        candidate_top_n=20,
        effective_top_n=10,
        signal_class="material",
        weak_signal=False,
        cold_weak_noop_gate={"used": False},
        baseline_top_n=20,
        stage_timing_notes=[],
        baseline=RetrievalResult(trigger),
        hypotheses=(),
        evidence_by_key={},
        all_questions=[],
        all_actions=[],
        action_cache={},
        baseline_action_cache_notes={},
        action_timing_notes=[],
        answers=[],
        retrieval_results=[],
        unknowns=set(),
        question_planning_notes=[],
        reconstruction_notes=[],
        question_policy={},
        reflective_rules=(),
        sage_reader_notes={"questions": {}, "signatures": []},
        sage_reader_runtime=object(),
        sage_reader_substrate=None,
        max_rounds=1,
    )

    status = await asyncio.wait_for(
        inquiry_rounds._execute_inquiry_round(
            state,
            trigger=trigger,
            conn=_NoPolicyConn(),
            embedder=None,
            llm_provider=None,
            read_pool=read_pool,  # type: ignore[arg-type]
            round_index=1,
        ),
        timeout=1.0,
    )

    assert status.stop_status == "sufficient_for_reasoning"
    assert events == ["sage_started", "motifs_started", "sage_finished"]
    assert read_pool.acquires == 1
    assert state.stage_timing_notes[-1]["stage"] == "round_read_prep_parallel"
    assert state.stage_timing_notes[-1]["motif_read_pool"] is True


@pytest.mark.asyncio
async def test_execute_inquiry_round_pipelines_actions_after_each_sage_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trigger = _weak_noop_trigger()
    selected = [_question("Q1"), _question("Q2", "OWNERSHIP")]
    q1_actions_started = asyncio.Event()
    events: list[str] = []
    applied: list[str] = []

    async def fake_select(*_args: object, **_kwargs: object):
        return selected

    async def fake_sage(
        questions: list[InquiryQuestion],
        *_args: object,
        on_question_result=None,
        **_kwargs: object,
    ):
        events.append("sage_started")
        assert on_question_result is not None
        await on_question_result(questions[0], None)
        events.append("sage_q1_ready")
        await asyncio.wait_for(q1_actions_started.wait(), timeout=0.5)
        events.append("sage_q2_ready")
        await on_question_result(questions[1], None)
        return {}, {"used": True, "parallel": True}

    async def fake_motifs(*_args: object, **_kwargs: object):
        events.append("motifs_loaded")
        return {}

    async def fake_execute_actions(plans, *_args: object, **_kwargs: object):
        qid = plans[0].question.question_id
        events.append(f"actions_{qid}_started")
        if qid == "Q1":
            q1_actions_started.set()
        await asyncio.sleep(0)
        return {qid: []}

    def fake_apply_results(*_args: object, **kwargs: object):
        plan = kwargs["plan"]
        applied.append(plan.question.question_id)
        if plan.question.question_id == "Q2":
            return inquiry_rounds._InquiryRoundStatus(
                "sufficient_for_reasoning",
                "test complete",
            )
        return inquiry_rounds._InquiryRoundStatus(
            "insufficient_continue",
            "continue",
        )

    monkeypatch.setattr(inquiry_rounds, "_select_questions_for_round", fake_select)
    monkeypatch.setattr(
        inquiry_rounds,
        "_execute_sage_reader_actions_for_round",
        fake_sage,
    )
    monkeypatch.setattr(
        inquiry_rounds,
        "load_retrieval_motifs_for_questions",
        fake_motifs,
    )
    monkeypatch.setattr(
        inquiry_rounds,
        "_execute_question_retrieval_actions",
        fake_execute_actions,
    )
    monkeypatch.setattr(
        inquiry_rounds,
        "_apply_question_read_plan_results",
        fake_apply_results,
    )
    state = inquiry_bootstrap._InquiryBootstrapState(
        cfg=InquiryConfig(
            max_rounds=1,
            read_prep_parallel_enabled=True,
            round_action_pipeline_enabled=True,
            sage_reader_enabled=True,
            sage_reader_parallel_enabled=True,
            sage_reader_parallelism=2,
            question_action_parallel_enabled=True,
            question_action_parallelism=2,
        ),
        route="DEEP_INQUIRY_PATH",
        session_id=UUID("00000000-0000-0000-0000-000000000011"),
        candidate_top_n=20,
        effective_top_n=10,
        signal_class="material",
        weak_signal=False,
        cold_weak_noop_gate={"used": False},
        baseline_top_n=20,
        stage_timing_notes=[],
        baseline=RetrievalResult(trigger),
        hypotheses=(),
        evidence_by_key={},
        all_questions=[],
        all_actions=[],
        action_cache={},
        baseline_action_cache_notes={},
        action_timing_notes=[],
        answers=[],
        retrieval_results=[],
        unknowns=set(),
        question_planning_notes=[],
        reconstruction_notes=[],
        question_policy={},
        reflective_rules=(),
        sage_reader_notes={"questions": {}, "signatures": []},
        sage_reader_runtime=object(),
        sage_reader_substrate=None,
        max_rounds=1,
    )

    status = await asyncio.wait_for(
        inquiry_rounds._execute_inquiry_round(
            state,
            trigger=trigger,
            conn=_NoPolicyConn(),
            embedder=None,
            llm_provider=None,
            read_pool=object(),
            round_index=1,
        ),
        timeout=1.0,
    )

    assert status.stop_status == "sufficient_for_reasoning"
    assert events.index("actions_Q1_started") < events.index("sage_q2_ready")
    assert applied == ["Q1", "Q2"]
    assert [question.question_id for question in state.all_questions] == ["Q1", "Q2"]
    assert state.stage_timing_notes[-1]["stage"] == "round_read_action_pipeline"
    assert state.sage_reader_notes["batches"][0]["pipelined_actions"] is True
