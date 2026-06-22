from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

from services.platform.execution import inquiry, inquiry_bootstrap, inquiry_rounds
from services.platform.execution.config import InquiryConfig
from services.platform.execution.types import InquiryQuestion
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext


class _NoPolicyConn:
    async def fetchval(self, *_args: object, **_kwargs: object) -> None:
        return None

    async def fetch(self, *_args: object, **_kwargs: object) -> list[object]:
        return []


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


def test_inquiry_imports_round_phase_from_canonical_module() -> None:
    assert inquiry._execute_inquiry_rounds is inquiry_rounds._execute_inquiry_rounds


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
