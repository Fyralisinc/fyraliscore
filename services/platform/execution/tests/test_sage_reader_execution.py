from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

import pytest

from services.platform.execution import inquiry, sage_reader_execution
from services.platform.execution.config import InquiryConfig
from services.platform.execution.types import InquiryQuestion
from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.retrieval.primary import TriggerContext


def _trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
        seed_entity_ids=[],
        seed_natural_text="Does the launch have a blocker?",
        seed_occurred_at=None,
        scope_actors=[],
    )


def _question() -> InquiryQuestion:
    return InquiryQuestion(
        question_id="Q1",
        question="What blocks the launch?",
        primitive="DEPENDENCY",
        tests_hypotheses=("H1",),
        expected_value=0.8,
        expected_cost=0.2,
        retrieval_target="blocker",
        stop_condition="find blocker",
        score=0.7,
    )


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
        async def read(self, **kwargs):
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

    result = await sage_reader_execution._execute_sage_reader_action(
        _question(),
        _trigger(),
        None,
        InquiryConfig(sage_reader_enabled=True, result_model_limit=7),
        hypotheses=(),
        reader=Reader(),
    )

    assert result is not None
    assert result.notes["pathways_run"] == ["sage_reader"]
    assert result.notes["action"]["path"] == "sage_reader"
    assert result.notes["action"]["budget"] == 7
    assert result.notes["sage_reader"]["question_primitive"] == "DEPENDENCY"
