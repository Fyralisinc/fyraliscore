from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from services.platform.execution import answer_evaluation, inquiry
from services.platform.execution.types import (
    EvidenceCard,
    Hypothesis,
    InquiryQuestion,
    QuestionAnswer,
)


def _hypotheses() -> tuple[Hypothesis, ...]:
    return (
        Hypothesis("H1", "The signal is a real blocker.", 0.7, "high"),
        Hypothesis("H2", "An owner or commitment is affected.", 0.4, "medium"),
        Hypothesis("H3", "The signal may recur.", 0.3, "medium"),
        Hypothesis("H0", "The signal is stale or already resolved.", 0.2, "low"),
    )


def _question(
    *,
    question_id: str = "Q_COUNTEREVIDENCE",
    primitive: str = "COUNTEREVIDENCE",
) -> InquiryQuestion:
    return InquiryQuestion(
        question_id=question_id,
        question="What evidence weakens this interpretation?",
        primitive=primitive,
        tests_hypotheses=("H1", "H0"),
        expected_value=0.8,
        expected_cost=0.2,
        retrieval_target="counterevidence",
        stop_condition="premise checked",
        score=0.7,
    )


def _card(
    *,
    question_id: str = "Q_COUNTEREVIDENCE",
    summary: str = "Customer says the blocker is no longer active.",
    source_type: str = "observation",
    timestamp: datetime | None = None,
    supports: set[str] | None = None,
    weakens: set[str] | None = None,
    contradicts: set[str] | None = None,
) -> EvidenceCard:
    return EvidenceCard(
        evidence_id=uuid4(),
        source_type=source_type,
        source_ref=f"{source_type}:{uuid4()}",
        source_ref_id=uuid4(),
        summary=summary,
        trust_tier="authoritative",
        timestamp=timestamp,
        retrieval_paths={"semantic"},
        retrieved_for_questions={question_id},
        supports_hypotheses=supports or set(),
        weakens_hypotheses=weakens or set(),
        contradicts_hypotheses=contradicts or set(),
        raw_content_ref=f"{source_type}:{uuid4()}",
        token_estimate=12,
        score=0.8,
    )


def test_answer_evaluation_helpers_keep_legacy_inquiry_identity() -> None:
    assert inquiry._classify_hypothesis_links is (
        answer_evaluation.classify_hypothesis_links
    )
    assert inquiry._has_premise_challenge_language is (
        answer_evaluation.has_premise_challenge_language
    )
    assert inquiry._has_missing_owner_language is (
        answer_evaluation.has_missing_owner_language
    )
    assert inquiry._answer_question is answer_evaluation.answer_question
    assert inquiry._resolved_unknowns_for_answer is (
        answer_evaluation.resolved_unknowns_for_answer
    )
    assert inquiry._sufficiency_gate is answer_evaluation.sufficiency_gate


def test_classify_hypothesis_links_detects_premise_and_owner_challenges() -> None:
    supports, weakens, contradicts = answer_evaluation.classify_hypothesis_links(
        (
            "Acme SSO is one blocker, but data migration is also active, "
            "and no explicit owner is represented."
        ),
        _hypotheses(),
        trigger_text="Why is Acme blocked by SSO?",
    )

    assert {"H1", "H2"} <= supports
    assert {"H1", "H2"} <= weakens
    assert contradicts == set()


def test_classify_hypothesis_links_ignores_declared_unrelated_evidence() -> None:
    supports, weakens, contradicts = answer_evaluation.classify_hypothesis_links(
        "This is unrelated to the current trigger, although the blocker is active.",
        _hypotheses(),
        trigger_text="Current blocker is active",
    )

    assert supports == set()
    assert weakens == set()
    assert contradicts == set()


def test_counterevidence_answer_distinguishes_stale_and_fresh_counterevidence() -> None:
    now = datetime(2026, 6, 13, tzinfo=timezone.utc)
    stale = _card(
        timestamp=now - timedelta(days=90),
        weakens={"H1"},
    )
    fresh = _card(
        timestamp=now - timedelta(days=1),
        weakens={"H1"},
    )
    question = _question()

    stale_answer = answer_evaluation.answer_question(
        question,
        {("observation", "stale"): stale},
        trigger_occurred_at=now,
        stale_after_days=30,
    )
    fresh_answer = answer_evaluation.answer_question(
        question,
        {("observation", "fresh"): fresh},
        trigger_occurred_at=now,
        stale_after_days=30,
    )

    assert stale_answer.answer_status == "inconclusive"
    assert stale_answer.new_uncertainties == ("fresh counterevidence",)
    assert fresh_answer.answer_status == "supported"
    assert fresh_answer.counterevidence == (str(fresh.evidence_id),)


def test_ownership_answer_requires_positive_owner_signal() -> None:
    question = _question(question_id="Q_OWNER", primitive="OWNERSHIP")
    missing_owner = _card(
        question_id="Q_OWNER",
        summary="The launch has owner=unassigned and no explicit owner.",
        source_type="commitment",
        supports={"H2"},
    )
    assigned_owner = _card(
        question_id="Q_OWNER",
        summary=f"The launch owner={uuid4()} is accountable for mitigation.",
        source_type="commitment",
        supports={"H2"},
    )

    missing_answer = answer_evaluation.answer_question(
        question,
        {("commitment", "missing"): missing_owner},
    )
    assigned_answer = answer_evaluation.answer_question(
        question,
        {("commitment", "assigned"): assigned_owner},
    )

    assert missing_answer.answer_status == "inconclusive"
    assert missing_answer.new_uncertainties == ("responsible owner",)
    assert assigned_answer.answer_status == "supported"
    assert assigned_answer.supporting_evidence == (str(assigned_owner.evidence_id),)


def test_resolved_unknowns_and_sufficiency_gate_follow_answer_outcomes() -> None:
    dependency = _question(question_id="Q_CRITICAL_PATH", primitive="DEPENDENCY")
    counter = _question()
    dependency_answer = QuestionAnswer(
        "Q_CRITICAL_PATH",
        "supported",
        "Dependency answered.",
    )
    counter_answer = QuestionAnswer(
        "Q_COUNTEREVIDENCE",
        "supported",
        "Counterevidence checked.",
    )
    support = _card(
        question_id="Q_CRITICAL_PATH",
        supports={"H1"},
        source_type="commitment",
    )
    counter_card = _card(
        question_id="Q_COUNTEREVIDENCE",
        weakens={"H1"},
    )

    assert answer_evaluation.resolved_unknowns_for_answer(
        dependency, dependency_answer
    ) == {"whether the blocker is on the critical path"}
    assert answer_evaluation.resolved_unknowns_for_answer(counter, counter_answer) == {
        "counterevidence"
    }

    verdict = answer_evaluation.sufficiency_gate(
        "DEEP_INQUIRY_PATH",
        _hypotheses(),
        [support, counter_card],
        [dependency_answer, counter_answer],
        round_index=0,
        max_rounds=1,
        unknowns=set(),
    )

    assert verdict.status == "sufficient_for_reasoning"
    assert verdict.answered_questions == 2
