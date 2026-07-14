from __future__ import annotations

import asyncio
from types import SimpleNamespace
from uuid import UUID

import pytest

from services.platform.execution import inquiry, sage_reader_execution
from services.platform.execution.config import InquiryConfig
from services.platform.execution.types import InquiryQuestion
from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.sage.reader import ReaderActivationTrace


def _trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
        seed_entity_ids=[],
        seed_natural_text="Does the launch have a blocker?",
        seed_occurred_at=None,
        scope_actors=[],
    )


def _question(
    question_id: str = "Q1",
    question: str = "What blocks the launch?",
) -> InquiryQuestion:
    return InquiryQuestion(
        question_id=question_id,
        question=question,
        primitive="DEPENDENCY",
        tests_hypotheses=("H1",),
        expected_value=0.8,
        expected_cost=0.2,
        retrieval_target="blocker",
        stop_condition="find blocker",
        score=0.7,
    )


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
        self._conn = conn
        self.acquires = 0

    def acquire(self) -> _AcquireConn:
        self.acquires += 1
        return _AcquireConn(self._conn)


def test_inquiry_private_aliases_point_to_sage_reader_execution_module() -> None:
    assert inquiry._build_sage_reader is sage_reader_execution._build_sage_reader
    assert (
        inquiry._execute_sage_reader_action
        is sage_reader_execution._execute_sage_reader_action
    )
    assert (
        inquiry._execute_sage_reader_actions_for_round
        is sage_reader_execution._execute_sage_reader_actions_for_round
    )


@pytest.mark.asyncio
async def test_execute_sage_reader_action_returns_none_when_disabled() -> None:
    result = await sage_reader_execution._execute_sage_reader_action(
        _question(),
        _trigger(),
        None,
        InquiryConfig(sage_reader_enabled=False),
        hypotheses=(),
        reader=object(),
    )

    assert result is None


@pytest.mark.asyncio
async def test_execute_sage_reader_action_wraps_reader_result() -> None:
    class Reader:
        def __init__(self) -> None:
            self.kwargs = {}

        async def read(self, **kwargs):
            self.kwargs = kwargs
            return SimpleNamespace(
                observations=[],
                models=[],
                pathway_result=PathwayResult(source_pathway="sage_reader"),
                question_primitive=kwargs["question_primitive"],
                signature={"question_id": kwargs["question_id"]},
                projected_evidence=[],
                activations=[],
                debug={"ok": True},
                model_scores={},
            )

    reader = Reader()
    evidence_state = {
        "summary": "counterevidence unresolved",
        "known_model_ids": ["m1"],
    }
    result = await sage_reader_execution._execute_sage_reader_action(
        _question(),
        _trigger(),
        None,
        InquiryConfig(sage_reader_enabled=True, result_model_limit=7),
        hypotheses=(),
        reader=reader,
        evidence_state=evidence_state,
    )

    assert result is not None
    assert reader.kwargs["evidence_state"] == evidence_state
    assert result.notes["pathways_run"] == ["sage_reader"]
    assert result.notes["action"]["path"] == "sage_reader"
    assert result.notes["action"]["budget"] == 7
    assert result.notes["sage_reader"]["question_primitive"] == "DEPENDENCY"
    assert result.notes["sage_reader"]["reconstruction_state"] == evidence_state


@pytest.mark.asyncio
async def test_sage_reader_round_cache_reuses_duplicate_serial_questions() -> None:
    class Reader:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def read(self, **kwargs):
            self.calls.append(kwargs["question_id"])
            return SimpleNamespace(
                observations=[],
                models=[],
                pathway_result=PathwayResult(
                    source_pathway="sage_reader",
                    notes={
                        "question_id": kwargs["question_id"],
                        "question_primitive": kwargs["question_primitive"],
                    },
                ),
                question_primitive=kwargs["question_primitive"],
                signature={"question": kwargs["question"]},
                projected_evidence=[],
                activations=[
                    ReaderActivationTrace(
                        question_id=kwargs["question_id"],
                        model_id=UUID("00000000-0000-0000-0000-000000000101"),
                        activation_score=0.8,
                        activation_reasons=("explicit_seed",),
                        selected=True,
                        selection_rank=0,
                    )
                ],
                debug={"ok": True},
                model_scores={},
            )

    reader = Reader()
    results, note = await sage_reader_execution._execute_sage_reader_actions_for_round(
        [
            _question("Q1", "What blocks the launch?"),
            _question("Q2", "  what   blocks THE launch? "),
        ],
        _trigger(),
        object(),  # type: ignore[arg-type]
        InquiryConfig(
            sage_reader_enabled=True,
            sage_reader_parallel_enabled=False,
        ),
        reader=reader,
        substrate=None,
        hypotheses=(),
        read_pool=None,
    )

    assert reader.calls == ["Q1"]
    assert note["cache_hits"] == 1
    assert note["cache_waits"] == 0
    assert note["cache_wait_elapsed_ms"] == 0
    assert set(results) == {"Q1", "Q2"}
    assert results["Q1"].notes["sage_reader"]["cache_hit"] is False
    assert results["Q1"].notes["sage_reader"]["cache_wait"] is False
    assert results["Q1"].notes["sage_reader"]["timing_kind"] == "owner_work"
    assert results["Q2"].notes["sage_reader"]["cache_hit"] is True
    assert results["Q2"].notes["sage_reader"]["cache_wait"] is False
    assert results["Q2"].notes["sage_reader"]["timing_kind"] == "cache_hit"
    assert results["Q2"].notes["sage_reader"]["question_id"] == "Q2"
    assert results["Q2"].notes["sage_reader"]["activations"][0]["question_id"] == "Q2"
    assert results["Q2"].notes["action"]["question_id"] == "Q2"
    assert results["Q2"].pathway_results[0].notes["question_id"] == "Q2"
    assert results["Q2"].pathway_results[0].notes["question_primitive"] == "DEPENDENCY"


@pytest.mark.asyncio
async def test_sage_reader_round_cache_coalesces_parallel_in_flight_duplicates() -> (
    None
):
    class Reader:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def read(self, **kwargs):
            self.calls.append(kwargs["question_id"])
            await asyncio.sleep(0)
            return SimpleNamespace(
                observations=[],
                models=[],
                pathway_result=PathwayResult(
                    source_pathway="sage_reader",
                    notes={
                        "question_id": kwargs["question_id"],
                        "question_primitive": kwargs["question_primitive"],
                    },
                ),
                question_primitive=kwargs["question_primitive"],
                signature={"question": kwargs["question"]},
                projected_evidence=[],
                activations=[],
                debug={"ok": True},
                model_scores={},
            )

    reader = Reader()
    read_pool = _ReadPool(object())
    results, note = await sage_reader_execution._execute_sage_reader_actions_for_round(
        [
            _question("Q1", "What blocks the launch?"),
            _question("Q2", "What blocks the launch?"),
            _question("Q3", "What blocks the launch?"),
        ],
        _trigger(),
        object(),  # type: ignore[arg-type]
        InquiryConfig(
            sage_reader_enabled=True,
            sage_reader_parallel_enabled=True,
            sage_reader_parallelism=3,
        ),
        reader=reader,
        substrate=None,
        hypotheses=(),
        read_pool=read_pool,  # type: ignore[arg-type]
    )

    assert reader.calls == ["Q1"]
    assert read_pool.acquires == 1
    assert note["parallel"] is True
    assert note["cache_hits"] == 0
    assert note["cache_waits"] == 2
    assert note["cache_wait_elapsed_ms"] >= 0
    assert set(results) == {"Q1", "Q2", "Q3"}
    assert results["Q2"].notes["sage_reader"]["cache_hit"] is True
    assert results["Q2"].notes["sage_reader"]["cache_wait"] is True
    assert results["Q2"].notes["sage_reader"]["timing_kind"] == "in_flight_wait"
    assert results["Q2"].notes["sage_reader"]["cache_source_question_id"] == "Q1"
    assert results["Q3"].pathway_results[0].notes["question_id"] == "Q3"


@pytest.mark.asyncio
async def test_sage_reader_round_notifies_each_question_as_results_complete() -> None:
    class Reader:
        async def read(self, **kwargs):
            if kwargs["question_id"] == "Q1":
                await asyncio.sleep(0)
            return SimpleNamespace(
                observations=[],
                models=[],
                pathway_result=PathwayResult(
                    source_pathway="sage_reader",
                    notes={
                        "question_id": kwargs["question_id"],
                        "question_primitive": kwargs["question_primitive"],
                    },
                ),
                question_primitive=kwargs["question_primitive"],
                signature={"question": kwargs["question"]},
                projected_evidence=[],
                activations=[],
                debug={"ok": True},
                model_scores={},
            )

    notified: list[str] = []

    async def on_question_result(
        question: InquiryQuestion,
        result: object,
    ) -> None:
        assert result is not None
        notified.append(question.question_id)

    results, note = await sage_reader_execution._execute_sage_reader_actions_for_round(
        [
            _question("Q1", "What blocks the launch?"),
            _question("Q2", "Who owns the launch?"),
        ],
        _trigger(),
        object(),  # type: ignore[arg-type]
        InquiryConfig(
            sage_reader_enabled=True,
            sage_reader_parallel_enabled=True,
            sage_reader_parallelism=2,
        ),
        reader=Reader(),
        substrate=None,
        hypotheses=(),
        read_pool=_ReadPool(object()),  # type: ignore[arg-type]
        on_question_result=on_question_result,
    )

    assert set(notified) == {"Q1", "Q2"}
    assert set(results) == {"Q1", "Q2"}
    assert note["parallel"] is True
