from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.platform.execution import (
    inquiry,
    question_planning,
    question_planning_provider,
)
from services.platform.execution.config import InquiryConfig
from services.platform.execution.question_planning_schemas import (
    LLMBeliefDeltaSpec,
    LLMCompactBeliefDeltaSpec,
    LLMCompactQuestionPlan,
    LLMCompactQuestionSpec,
    LLMInquiryQuestionPlan,
    LLMInquiryQuestionSpec,
)
from services.platform.execution.types import (
    EvidenceCard,
    Hypothesis,
    InquiryQuestion,
    ReconstructionState,
)
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext


@pytest.fixture(autouse=True)
def _reset_question_planning_provider_health():
    question_planning_provider.reset_question_planning_provider_health()
    yield
    question_planning_provider.reset_question_planning_provider_health()


def _trigger(
    text: str = "HarborRail procurement audit evidence blocker",
) -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        observation_id=uuid4(),
        seed_natural_text=text,
        seed_entity_ids=["HarborRail"],
    )


def _hypothesis(
    *,
    hypothesis_id: str = "H1",
    claim: str = "HarborRail procurement depends on audit evidence",
) -> Hypothesis:
    return Hypothesis(
        id=hypothesis_id,
        claim=claim,
        confidence=0.72,
        impact_if_true="high",
        delta_type="update",
        affected_entities=("HarborRail",),
        uncertainty_slots=("who owns audit evidence",),
        evidence_needed=("procurement thread",),
    )


def _question(
    *,
    primitive: str,
    question_id: str,
    question: str,
    expected_value: float,
    expected_cost: float,
    score: float,
) -> InquiryQuestion:
    return InquiryQuestion(
        question_id=question_id,
        question=question,
        primitive=primitive,
        tests_hypotheses=("H1",),
        expected_value=expected_value,
        expected_cost=expected_cost,
        retrieval_target="test_target",
        stop_condition="test_stop",
        score=score,
    )


def test_question_planning_helpers_keep_legacy_inquiry_identity() -> None:
    assert (
        inquiry._candidate_questions_for_round
        is question_planning.candidate_questions_for_round
    )
    assert (
        inquiry._generate_llm_question_plan
        is question_planning.generate_llm_question_plan
    )
    assert (
        inquiry._candidate_questions_from_belief_deltas
        is question_planning.candidate_questions_from_belief_deltas
    )
    assert (
        inquiry._quality_control_question_text
        is question_planning.quality_control_question_text
    )
    assert (
        inquiry._question_from_delta_slot is question_planning.question_from_delta_slot
    )
    assert (
        inquiry._merge_llm_and_safety_questions
        is question_planning.merge_llm_and_safety_questions
    )
    assert (
        inquiry._ALLOWED_QUESTION_PRIMITIVES
        is question_planning.ALLOWED_QUESTION_PRIMITIVES
    )


def test_expand_compact_question_plan_normalizes_to_full_schema() -> None:
    compact = LLMCompactQuestionPlan(
        r="Ownership and counterevidence decide the update.",
        d=[
            LLMCompactBeliefDeltaSpec(
                i="D_AUDIT",
                claim="HarborRail procurement depends on audit evidence",
                type="update",
                entities=["HarborRail"],
                slots=["who owns audit evidence"],
                evidence=["procurement thread"],
                impact="high",
                conf=0.82,
            )
        ],
        q=[
            LLMCompactQuestionSpec(
                p="OWNERSHIP",
                q="Who owns audit evidence for HarborRail procurement?",
                v=0.91,
                c=0.21,
            )
        ],
    )

    plan = question_planning.expand_compact_question_plan(compact)

    assert plan.rationale == "Ownership and counterevidence decide the update."
    assert plan.belief_deltas[0].delta_id == "D_AUDIT"
    assert plan.belief_deltas[0].claim_atom == (
        "HarborRail procurement depends on audit evidence"
    )
    assert plan.questions[0].primitive == "OWNERSHIP"
    assert plan.questions[0].retrieval_target is None


def test_normalize_belief_delta_hypotheses_dedupes_and_cleans() -> None:
    trigger = _trigger("HarborRail procurement audit evidence requirement")
    specs = [
        LLMBeliefDeltaSpec(
            delta_id="D Audit",
            claim_atom="HarborRail procurement depends on audit evidence",
            delta_type="modify",
            affected_entities=["HarborRail", "HarborRail"],
            uncertainty_slots=[],
            evidence_needed=["procurement thread", "procurement thread"],
            impact_if_true="critical",
            confidence=0.87,
        ),
        LLMBeliefDeltaSpec(
            delta_id="D Duplicate",
            claim_atom="HarborRail procurement depends on audit evidence",
            delta_type="create_new",
            affected_entities=["Ignored"],
            uncertainty_slots=["ignored"],
            confidence=0.2,
        ),
    ]

    hypotheses = question_planning.normalize_llm_belief_delta_hypotheses(
        specs,
        trigger=trigger,
    )

    assert len(hypotheses) == 1
    delta = hypotheses[0]
    assert delta.id == "D_Audit"
    assert delta.delta_type == "update"
    assert delta.affected_entities == ("HarborRail",)
    assert delta.evidence_needed == ("procurement thread",)
    assert delta.impact_if_true == "high"
    assert "who owns the next action" in delta.uncertainty_slots


def test_normalize_llm_questions_repairs_and_scores_specific_questions() -> None:
    trigger = _trigger("HarborRail renewal evidence dependency")
    notes: list[dict[str, object]] = []
    specs = [
        LLMInquiryQuestionSpec(
            primitive="DEPENDENCY",
            question=(
                "Is Enterprise controls should be represented as the dominant "
                "constrained factor actually on the critical path?"
            ),
            expected_value=0.92,
            expected_cost=0.24,
            tests_hypotheses=["H1", "missing"],
        ),
        LLMInquiryQuestionSpec(
            primitive="DEPENDENCY",
            question="Duplicate primitive should be ignored?",
            expected_value=0.4,
            expected_cost=0.2,
            tests_hypotheses=["H1"],
        ),
        LLMInquiryQuestionSpec(
            primitive="NOT_ALLOWED",
            question="Should invalid primitive be ignored?",
            expected_value=0.9,
            expected_cost=0.1,
        ),
    ]

    questions = question_planning.normalize_llm_questions(
        specs,
        (_hypothesis(),),
        trigger=trigger,
        quality_notes=notes,
    )

    assert len(questions) == 1
    question = questions[0]
    assert question.primitive == "DEPENDENCY"
    assert question.tests_hypotheses == ("H1",)
    assert question.score == pytest.approx(0.8)
    assert question.question.endswith("?")
    assert notes[0]["repair_reason"] == "dependency_template_clause_leak"


def test_merge_llm_and_safety_questions_keeps_counterevidence_and_stronger_scores() -> (
    None
):
    llm_goal = _question(
        primitive="GOAL_IMPACT",
        question_id="Q_GOAL_IMPACT",
        question="Which goal is affected?",
        expected_value=0.42,
        expected_cost=0.32,
        score=0.22,
    )
    safety_goal = _question(
        primitive="GOAL_IMPACT",
        question_id="Q_GOAL_IMPACT",
        question="Which customer goal is blocked?",
        expected_value=0.91,
        expected_cost=0.20,
        score=0.83,
    )
    safety_counterevidence = _question(
        primitive="COUNTEREVIDENCE",
        question_id="Q_COUNTEREVIDENCE",
        question="What evidence weakens the blocker interpretation?",
        expected_value=0.78,
        expected_cost=0.30,
        score=0.60,
    )

    merged, added = question_planning.merge_llm_and_safety_questions(
        [llm_goal],
        [safety_goal, safety_counterevidence],
    )

    assert added == 1
    assert [question.primitive for question in merged] == [
        "COUNTEREVIDENCE",
        "GOAL_IMPACT",
    ]
    goal = next(question for question in merged if question.primitive == "GOAL_IMPACT")
    assert goal.question == "Which goal is affected?"
    assert goal.expected_value == pytest.approx(0.91)
    assert goal.expected_cost == pytest.approx(0.20)
    assert goal.score == pytest.approx(0.83)


def test_merge_llm_and_safety_questions_force_includes_high_value_ownership() -> None:
    llm_questions = [
        _question(
            primitive="CONSTRAINT",
            question_id="Q_CONSTRAINT",
            question="Which constraint is binding?",
            expected_value=0.90,
            expected_cost=0.24,
            score=0.78,
        ),
        _question(
            primitive="COUNTEREVIDENCE",
            question_id="Q_COUNTEREVIDENCE",
            question="What counterevidence exists?",
            expected_value=0.84,
            expected_cost=0.30,
            score=0.66,
        ),
        _question(
            primitive="DEPENDENCY",
            question_id="Q_CRITICAL_PATH",
            question="Is this a dependency?",
            expected_value=0.88,
            expected_cost=0.24,
            score=0.76,
        ),
        _question(
            primitive="COMMITMENT",
            question_id="Q_ACTIVE_COMMITMENT",
            question="Which commitment changes?",
            expected_value=0.78,
            expected_cost=0.18,
            score=0.72,
        ),
    ]
    owner = _question(
        primitive="OWNERSHIP",
        question_id="Q_OWNER",
        question="Who owns the next action?",
        expected_value=0.72,
        expected_cost=0.22,
        score=0.65,
    )

    merged, added = question_planning.merge_llm_and_safety_questions(
        llm_questions,
        [owner],
    )

    assert added == 1
    assert "OWNERSHIP" in {question.primitive for question in merged}


@pytest.mark.asyncio
async def test_candidate_questions_for_round_falls_back_when_disabled() -> None:
    trigger = _trigger("HarborRail renewal blocker has owner risk")

    questions, note = await question_planning.candidate_questions_for_round(
        trigger,
        RetrievalResult(trigger=trigger),
        (_hypothesis(),),
        {},
        {"responsible owner", "counterevidence"},
        llm_provider=None,
        config=InquiryConfig(llm_question_planning_enabled=False),
        round_index=2,
    )

    assert note == {
        "round": 2,
        "mode": "deterministic_fallback",
        "reason": "disabled_by_config",
        "candidate_count": len(questions),
    }
    assert {question.primitive for question in questions} >= {
        "COUNTEREVIDENCE",
        "DEPENDENCY",
        "OWNERSHIP",
    }


@pytest.mark.asyncio
async def test_candidate_questions_governor_skips_llm_when_context_is_sufficient(
    monkeypatch,
) -> None:
    trigger = _trigger("HarborRail renewal risk has enough audit evidence")
    evidence_by_key = {
        ("model", str(index)): EvidenceCard(
            evidence_id=uuid4(),
            source_type="model",
            source_ref=f"model-{index}",
            source_ref_id=uuid4(),
            summary="Durable evidence already covers ownership and counterevidence.",
            trust_tier=None,
            timestamp=None,
        )
        for index in range(16)
    }
    baseline = RetrievalResult(trigger=trigger)
    baseline.models = [object() for _ in range(7)]  # type: ignore[list-item]

    monkeypatch.setattr(
        question_planning,
        "select_question_planning_provider",
        lambda _provider: pytest.fail("governor should skip provider selection"),
    )

    questions, note = await question_planning.candidate_questions_for_round(
        trigger,
        baseline,
        (_hypothesis(),),
        evidence_by_key,
        {"counterevidence"},
        llm_provider=SimpleNamespace(config=SimpleNamespace(provider="codex")),  # type: ignore[arg-type]
        config=InquiryConfig(),
        round_index=2,
    )

    assert questions
    assert note["mode"] == "deterministic_fallback"
    assert note["reason"] == "execution_utility_governor"
    assert note["utility_governor"]["decision"] == "suppress"


@pytest.mark.asyncio
async def test_candidate_questions_retries_codex_question_planner_quota(
    monkeypatch,
) -> None:
    class UsageLimitedProvider:
        config = SimpleNamespace(
            provider="codex",
            model="gpt-5.3-codex-spark",
            reasoning_effort="low",
            timeout_s=24,
        )

        def __init__(self) -> None:
            self.calls = 0

        async def structured(self, **_kwargs: object):
            self.calls += 1
            raise RuntimeError("You've hit your usage limit for GPT-5.3-Codex-Spark")

    class FallbackProvider:
        config = SimpleNamespace(
            provider="codex",
            model="gpt-5.5",
            reasoning_effort="low",
            timeout_s=60,
        )

        def __init__(self) -> None:
            self.calls = 0

        async def structured(self, **_kwargs: object):
            self.calls += 1
            return LLMInquiryQuestionPlan(
                rationale="Counterevidence still decides the update.",
                belief_deltas=[],
                questions=[
                    LLMInquiryQuestionSpec(
                        primitive="COUNTEREVIDENCE",
                        question="What evidence weakens the HarborRail audit blocker?",
                        retrieval_target="semantic_counterevidence",
                        expected_value=0.91,
                        expected_cost=0.22,
                        tests_hypotheses=["H1"],
                        stop_condition="counterevidence found or ruled out",
                    )
                ],
            )

    failed = UsageLimitedProvider()
    fallback = FallbackProvider()
    source = SimpleNamespace(config=SimpleNamespace(provider="codex"))

    monkeypatch.setattr(
        question_planning,
        "select_question_planning_provider",
        lambda provider: failed,
    )
    monkeypatch.setattr(
        question_planning,
        "select_question_planning_fallback_provider",
        lambda provider, failed_provider: fallback,
    )

    questions, note = await question_planning.candidate_questions_for_round(
        _trigger("HarborRail audit evidence is still blocking procurement."),
        RetrievalResult(trigger=_trigger()),
        (_hypothesis(),),
        {},
        {"counterevidence"},
        llm_provider=source,  # type: ignore[arg-type]
        config=InquiryConfig(),
        round_index=1,
    )

    assert failed.calls == 1
    assert fallback.calls == 1
    assert note["mode"] == "llm"
    assert note["llm_model"] == "gpt-5.5"
    assert note["planner_retry_count"] == 1
    assert note["planner_retry_errors"][0]["llm_model"] == "gpt-5.3-codex-spark"
    assert questions[0].primitive == "COUNTEREVIDENCE"


@pytest.mark.asyncio
async def test_candidate_questions_records_fallback_planner_failure(
    monkeypatch,
) -> None:
    class UsageLimitedProvider:
        config = SimpleNamespace(
            provider="codex",
            model="gpt-5.3-codex-spark",
            reasoning_effort="low",
            timeout_s=24,
        )

        async def structured(self, **_kwargs: object):
            raise RuntimeError(
                "codex cli exited 1: :loader: ignoring interface.icon_small\n"
                "ERROR: You've hit your usage limit for GPT-5.3-Codex-Spark"
            )

    class FailingFallbackProvider:
        config = SimpleNamespace(
            provider="codex",
            model="gpt-5.5",
            reasoning_effort="low",
            timeout_s=60,
        )

        async def structured(self, **_kwargs: object):
            raise TimeoutError("fallback planner exceeded 60s")

    failed = UsageLimitedProvider()
    fallback = FailingFallbackProvider()
    source = SimpleNamespace(config=SimpleNamespace(provider="codex"))

    monkeypatch.setattr(
        question_planning,
        "select_question_planning_provider",
        lambda provider: failed,
    )
    monkeypatch.setattr(
        question_planning,
        "select_question_planning_fallback_provider",
        lambda provider, failed_provider: fallback,
    )

    questions, note = await question_planning.candidate_questions_for_round(
        _trigger("HarborRail audit evidence is still blocking procurement."),
        RetrievalResult(trigger=_trigger()),
        (_hypothesis(),),
        {},
        {"counterevidence"},
        llm_provider=source,  # type: ignore[arg-type]
        config=InquiryConfig(),
        round_index=1,
    )

    assert questions
    assert note["mode"] == "deterministic_fallback"
    assert note["llm_model"] == "gpt-5.5"
    assert note["reason"] == "TimeoutError"
    assert "fallback planner exceeded 60s" in note["detail"]
    assert note["planner_retry_count"] == 2
    assert [error["llm_model"] for error in note["planner_retry_errors"]] == [
        "gpt-5.3-codex-spark",
        "gpt-5.5",
    ]
    assert ":loader:" not in note["planner_retry_errors"][0]["detail"]
    assert "usage limit" in note["planner_retry_errors"][0]["detail"]


@pytest.mark.asyncio
async def test_candidate_questions_skips_planner_when_primary_and_fallback_are_unhealthy(
    monkeypatch,
) -> None:
    class UsageLimitedProvider:
        config = SimpleNamespace(
            provider="codex",
            api_key="test-key",
            model="gpt-5.3-codex-spark",
            reasoning_effort="low",
            timeout_s=24,
            max_retries=0,
        )

        def __init__(self) -> None:
            self.calls = 0

        async def structured(self, **_kwargs: object):
            self.calls += 1
            raise RuntimeError("You've hit your usage limit for GPT-5.3-Codex-Spark")

    class TimeoutFallbackProvider:
        config = SimpleNamespace(
            provider="codex",
            api_key="test-key",
            model="gpt-5.4-mini",
            reasoning_effort="low",
            timeout_s=36,
            max_retries=0,
        )

        def __init__(self) -> None:
            self.calls = 0

        async def structured(self, **_kwargs: object):
            self.calls += 1
            raise TimeoutError("fallback planner exceeded 36s")

    primary = UsageLimitedProvider()
    fallback = TimeoutFallbackProvider()
    source = SimpleNamespace(config=SimpleNamespace(provider="codex"))

    monkeypatch.setattr(
        question_planning,
        "select_question_planning_provider",
        lambda provider: primary,
    )
    monkeypatch.setattr(
        question_planning,
        "select_question_planning_fallback_provider",
        lambda provider, failed_provider: fallback,
    )

    trigger = _trigger("HarborRail audit evidence still has ambiguous ownership.")
    first_questions, first_note = await question_planning.candidate_questions_for_round(
        trigger,
        RetrievalResult(trigger=trigger),
        (_hypothesis(),),
        {},
        {"owner"},
        llm_provider=source,  # type: ignore[arg-type]
        config=InquiryConfig(),
        round_index=1,
    )
    second_questions, second_note = (
        await question_planning.candidate_questions_for_round(
            trigger,
            RetrievalResult(trigger=trigger),
            (_hypothesis(),),
            {},
            {"owner"},
            llm_provider=source,  # type: ignore[arg-type]
            config=InquiryConfig(),
            round_index=2,
        )
    )

    assert first_questions
    assert second_questions
    assert primary.calls == 1
    assert fallback.calls == 1
    assert first_note["mode"] == "deterministic_fallback"
    assert first_note["planner_retry_count"] == 2
    assert first_note["planner_provider_backoffs"][0]["backoff_kind"] == "quota"
    assert first_note["planner_provider_backoffs"][1]["backoff_kind"] == "timeout"
    assert second_note["mode"] == "deterministic_fallback"
    assert second_note["reason"] == "question_planning_provider_in_backoff"
    assert [note["llm_model"] for note in second_note["planner_provider_backoffs"]] == [
        "gpt-5.3-codex-spark",
        "gpt-5.4-mini",
    ]


@pytest.mark.asyncio
async def test_candidate_questions_skips_unhealthy_primary_planner(
    monkeypatch,
) -> None:
    class PrimaryProvider:
        config = SimpleNamespace(
            provider="codex",
            api_key="test-key",
            model="gpt-5.3-codex-spark",
            reasoning_effort="low",
            timeout_s=24,
            max_retries=0,
        )

        def __init__(self) -> None:
            self.calls = 0

        async def structured(self, **_kwargs: object):
            self.calls += 1
            raise RuntimeError("codex app-server turn ended with status 'failed'")

    class FallbackProvider:
        config = SimpleNamespace(
            provider="codex",
            api_key="test-key",
            model="gpt-5.4-mini",
            reasoning_effort="low",
            timeout_s=36,
            max_retries=0,
        )

        def __init__(self) -> None:
            self.calls = 0

        async def structured(self, **_kwargs: object):
            self.calls += 1
            return LLMInquiryQuestionPlan(
                rationale="Fallback planner stayed available.",
                belief_deltas=[],
                questions=[
                    LLMInquiryQuestionSpec(
                        primitive="OWNERSHIP",
                        question="Who owns the HarborRail audit evidence?",
                        retrieval_target="owner_evidence",
                        expected_value=0.9,
                        expected_cost=0.2,
                        tests_hypotheses=["H1"],
                        stop_condition="owner identified",
                    )
                ],
            )

    primary = PrimaryProvider()
    fallback = FallbackProvider()
    source = SimpleNamespace(config=SimpleNamespace(provider="codex"))

    monkeypatch.setattr(
        question_planning,
        "select_question_planning_provider",
        lambda provider: primary,
    )
    monkeypatch.setattr(
        question_planning,
        "select_question_planning_fallback_provider",
        lambda provider, failed_provider: fallback,
    )

    trigger = _trigger("HarborRail audit evidence needs an owner.")
    first_questions, first_note = await question_planning.candidate_questions_for_round(
        trigger,
        RetrievalResult(trigger=trigger),
        (_hypothesis(),),
        {},
        {"owner"},
        llm_provider=source,  # type: ignore[arg-type]
        config=InquiryConfig(),
        round_index=1,
    )
    second_questions, second_note = (
        await question_planning.candidate_questions_for_round(
            trigger,
            RetrievalResult(trigger=trigger),
            (_hypothesis(),),
            {},
            {"owner"},
            llm_provider=source,  # type: ignore[arg-type]
            config=InquiryConfig(),
            round_index=2,
        )
    )

    assert primary.calls == 1
    assert fallback.calls == 2
    assert first_note["mode"] == "llm"
    assert second_note["mode"] == "llm"
    assert first_note["planner_retry_count"] == 1
    assert "planner_provider_backoffs" in first_note
    assert "planner_provider_backoffs" in second_note
    assert second_note["planner_provider_backoffs"][0]["llm_model"] == (
        "gpt-5.3-codex-spark"
    )
    assert "OWNERSHIP" in {question.primitive for question in first_questions}
    assert "OWNERSHIP" in {question.primitive for question in second_questions}


@pytest.mark.asyncio
async def test_generate_llm_question_plan_includes_reconstruction_state() -> None:
    class Provider:
        config = SimpleNamespace(provider="codex", model="gpt-5.3-codex-spark")

        def __init__(self) -> None:
            self.payload: dict[str, object] = {}

        async def structured(self, *, user: str, schema: object, **_kwargs: object):
            self.payload = json.loads(user)
            return LLMCompactQuestionPlan(
                r="Counterevidence is still open.",
                d=[
                    LLMCompactBeliefDeltaSpec(
                        i="D1",
                        claim="HarborRail launch may still be blocked",
                        type="update",
                    )
                ],
                q=[
                    LLMCompactQuestionSpec(
                        p="COUNTEREVIDENCE",
                        q="What evidence weakens the HarborRail blocker premise?",
                        v=0.9,
                        c=0.2,
                    )
                ],
            )

    provider = Provider()
    trigger = _trigger("HarborRail launch blocker")
    state = ReconstructionState(
        round_index=2,
        summary="counterevidence unresolved",
        active_cues=("HarborRail", "counterevidence"),
        unresolved_slots=("counterevidence",),
        known_model_ids=("model-1",),
        operator_bias=("semantic:counterevidence",),
    )

    await question_planning.generate_llm_question_plan(
        trigger,
        RetrievalResult(trigger=trigger),
        (_hypothesis(),),
        {},
        {"counterevidence"},
        llm_provider=provider,
        config=InquiryConfig(),
        reconstruction_state=state,
    )

    assert provider.payload["recon"]["round_index"] == 2
    assert provider.payload["recon"]["active_cues"] == [
        "HarborRail",
        "counterevidence",
    ]
    assert provider.payload["recon"]["known_model_count"] == 1
