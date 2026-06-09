from __future__ import annotations

import json
from uuid import uuid4

import pytest

from lib.llm.provider import LLMConfig, LLMProvider
from services.execution.inquiry import (
    Hypothesis,
    InquiryConfig,
    _candidate_questions_from_belief_deltas,
    _generate_llm_question_plan,
    _quality_control_question_text,
    _question_from_delta_slot,
)
from services.retrieval.primary import RetrievalResult, TriggerContext


class _SparkQuestionProvider(LLMProvider):
    def __init__(self) -> None:
        super().__init__(
            LLMConfig(
                provider="codex",
                api_key="test",
                model="gpt-5.3-codex-spark",
                reasoning_effort="low",
                max_retries=0,
            )
        )
        self.calls: list[dict[str, object]] = []

    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        self.calls.append(
            {
                "system": system,
                "user": user,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "schema_hint": schema_hint,
            }
        )
        return json.dumps(
            {
                "r": "The signal needs ownership and counterevidence checks.",
                "d": [
                    {
                        "i": "D_AUDIT",
                        "claim": "HarborRail procurement depends on customer-visible audit evidence",
                        "type": "update",
                        "entities": ["HarborRail"],
                        "slots": ["who owns customer-visible audit evidence"],
                        "evidence": ["procurement thread"],
                        "impact": "high",
                        "conf": 0.72,
                    }
                ],
                "q": [
                    {
                        "p": "OWNERSHIP",
                        "q": "Who owns customer-visible audit evidence for HarborRail procurement?",
                        "v": 0.91,
                        "c": 0.21,
                    }
                ],
            }
        )


def _trigger(text: str = "Atlas Retail Group renewal blocker signal") -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        observation_id=uuid4(),
        seed_natural_text=text,
    )


@pytest.mark.asyncio
async def test_spark_question_planning_uses_compact_schema(monkeypatch):
    monkeypatch.delenv("INQUIRY_CODEX_COMPACT_QUESTION_SCHEMA", raising=False)
    trigger = _trigger("HarborRail procurement audit evidence requirement")
    provider = _SparkQuestionProvider()

    plan = await _generate_llm_question_plan(
        trigger,
        RetrievalResult(trigger=trigger),
        (),
        {},
        {"ownership"},
        llm_provider=provider,
        config=InquiryConfig(),
        max_tokens=420,
    )

    assert provider.calls
    assert '"d"' in str(provider.calls[0]["schema_hint"])
    assert '"belief_deltas"' not in str(provider.calls[0]["schema_hint"])
    assert plan.belief_deltas[0].claim_atom == (
        "HarborRail procurement depends on customer-visible audit evidence"
    )
    assert plan.questions[0].primitive == "OWNERSHIP"


def test_delta_question_compiler_does_not_embed_full_sentence_subjects():
    trigger = _trigger("HarborRail Transit repeated P1 incident in ingestion freshness")
    delta = Hypothesis(
        id="D1",
        claim=(
            "HarborRail Transit is experiencing a repeated P1 incident in the "
            "ingestion freshness pipeline"
        ),
        confidence=0.72,
        impact_if_true="high",
        delta_type="create",
        affected_entities=("HarborRail Transit",),
        uncertainty_slots=("whether this appeared before",),
        source="llm_delta",
    )

    question = _question_from_delta_slot(
        delta,
        "whether this appeared before",
        "RECURRENCE",
        trigger,
    )

    assert question.endswith("?")
    assert "Has HarborRail Transit is" not in question
    assert "this appeared before pattern" not in question
    assert "HarborRail Transit" in question


def test_quality_control_repairs_known_malformed_dependency_template():
    trigger = _trigger("Atlas Retail Group enterprise controls renewal blocker")
    malformed = (
        "Is Enterprise controls should be represented as the dominant "
        "constrained factor actually on the critical path?"
    )
    notes: list[dict[str, object]] = []

    repaired = _quality_control_question_text(
        malformed,
        "DEPENDENCY",
        trigger,
        source="llm_question",
        quality_notes=notes,
    )

    assert repaired != malformed
    assert repaired.endswith("?")
    assert notes
    assert notes[0]["repair_reason"] == "dependency_template_clause_leak"


def test_quality_control_preserves_specific_imperatives_with_punctuation_repair():
    trigger = _trigger("HarborRail Salesforce renewal update")
    original = "Pull Salesforce history for HarborRail to confirm renewal-stage changes."
    notes: list[dict[str, object]] = []

    repaired = _quality_control_question_text(
        original,
        "COUNTEREVIDENCE",
        trigger,
        source="llm_question",
        quality_notes=notes,
    )

    assert repaired == "Pull Salesforce history for HarborRail to confirm renewal-stage changes?"
    assert notes[0]["repair_reason"] == "punctuation_added"


def test_recurrence_delta_question_does_not_duplicate_pattern_word():
    trigger = _trigger("Atlas SAML permission pattern frequency")
    delta = Hypothesis(
        id="D3",
        claim="Atlas indicates a recurring pattern of SAML permission edge cases",
        confidence=0.62,
        impact_if_true="medium",
        delta_type="update",
        affected_entities=("Atlas Retail Group",),
        uncertainty_slots=("pattern frequency",),
        source="llm_delta",
    )

    question = _question_from_delta_slot(
        delta,
        "pattern frequency",
        "RECURRENCE",
        trigger,
    )

    assert "pattern pattern" not in question
    assert "pattern frequency pattern" not in question


def test_belief_delta_candidate_questions_report_quality_repairs():
    trigger = _trigger("Atlas Retail Group forecast confidence evidence diversity")
    delta = Hypothesis(
        id="D2",
        claim=(
            "Decision 'Forecast confidence requires evidence diversity' should "
            "be modeled as a hard constraint before advancing renewal basis"
        ),
        confidence=0.7,
        impact_if_true="high",
        delta_type="update",
        affected_entities=("Atlas Retail Group",),
        uncertainty_slots=(
            "is policy in effect for Decision Forecast confidence requires evidence diversity",
        ),
        source="llm_delta",
    )
    notes: list[dict[str, object]] = []

    questions = _candidate_questions_from_belief_deltas(
        trigger,
        [delta],
        hypotheses=(),
        quality_notes=notes,
    )

    assert questions
    assert all("determines is policy" not in q.question for q in questions)
    assert all(q.question.endswith("?") for q in questions)


def test_generic_delta_slots_fall_back_to_domain_focus():
    trigger = _trigger("Atlas Retail security review evidence gate")
    delta = Hypothesis(
        id="D4",
        claim=(
            "Atlas Retail renewal approval depends on SOC2, audit export, "
            "SAML mapping, and data residency evidence"
        ),
        confidence=0.76,
        impact_if_true="high",
        delta_type="update",
        affected_entities=("Atlas Retail",),
        uncertainty_slots=("constraint",),
        evidence_needed=("security review evidence artifacts",),
        source="llm_delta",
    )

    question = _question_from_delta_slot(delta, "constraint", "CONSTRAINT", trigger)

    assert "blocking constraint for" not in question
    assert "security review evidence artifacts" in question


def test_primitive_label_slots_are_not_treated_as_question_focus():
    trigger = _trigger("Atlas SAML permission blocker after setup")
    delta = Hypothesis(
        id="D5",
        claim="Atlas SAML permission failure threatens renewal onboarding progress",
        confidence=0.66,
        impact_if_true="high",
        delta_type="update",
        affected_entities=("Atlas",),
        uncertainty_slots=("GOAL_IMPACT",),
        evidence_needed=("SAML permission failure",),
        source="llm_delta",
    )

    question = _question_from_delta_slot(delta, "GOAL_IMPACT", "CONSTRAINT", trigger)

    assert "GOAL IMPACT" not in question
    assert "SAML permission failure" in question


def test_prefixed_generic_slots_strip_planner_label():
    trigger = _trigger("HarborRail customer-visible audit trail procurement gate")
    delta = Hypothesis(
        id="D6",
        claim="HarborRail procurement acceptance requires a customer-visible audit trail",
        confidence=0.75,
        impact_if_true="high",
        delta_type="update",
        affected_entities=("HarborRail",),
        uncertainty_slots=("constraint:customer-visible audit trail",),
        source="llm_delta",
    )

    question = _question_from_delta_slot(
        delta,
        "constraint:customer-visible audit trail",
        "CONSTRAINT",
        trigger,
    )

    assert "constraint:customer-visible" not in question
    assert "customer-visible audit trail" in question
