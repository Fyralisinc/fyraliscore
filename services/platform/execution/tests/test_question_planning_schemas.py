from __future__ import annotations

from services.platform.execution import inquiry
from services.platform.execution import question_planning_schemas as schemas


def test_question_planning_schemas_keep_legacy_inquiry_identity() -> None:
    assert inquiry.LLMBeliefDeltaSpec is schemas.LLMBeliefDeltaSpec
    assert inquiry.LLMCompactBeliefDeltaSpec is schemas.LLMCompactBeliefDeltaSpec
    assert inquiry.LLMCompactQuestionPlan is schemas.LLMCompactQuestionPlan
    assert inquiry.LLMCompactQuestionSpec is schemas.LLMCompactQuestionSpec
    assert inquiry.LLMInquiryQuestionPlan is schemas.LLMInquiryQuestionPlan
    assert inquiry.LLMInquiryQuestionSpec is schemas.LLMInquiryQuestionSpec


def test_question_planning_schema_validation_is_unchanged() -> None:
    plan = schemas.LLMCompactQuestionPlan(
        r="Need a focused follow-up",
        d=[
            {
                "claim": "The renewal blocker may be a security audit dependency.",
                "entities": ["HarborRail"],
                "slots": ["who owns the audit dependency"],
            }
        ],
        q=[
            {
                "p": "OWNERSHIP",
                "q": "Who owns resolving the security audit dependency?",
            }
        ],
    )

    assert plan.d[0].claim.startswith("The renewal blocker")
    assert plan.q[0].p == "OWNERSHIP"
