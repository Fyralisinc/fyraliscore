from __future__ import annotations

from uuid import uuid4

import pytest

from services.platform.execution import reflective_rules
from services.platform.execution.config import InquiryConfig
from services.platform.execution.types import InquiryQuestion, RetrievalAction
from services.reasoning.retrieval.primary import TriggerContext


def _trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_entity_ids=[{"type": "customer", "label": "DeltaCo"}],
        seed_natural_text=(
            "DeltaCo SAML launch is blocked again. PR #847 may not make deploy."
        ),
    )


def _question(
    primitive: str,
    *,
    question_id: str,
    score: float,
) -> InquiryQuestion:
    return InquiryQuestion(
        question_id=question_id,
        question=f"Fallback {primitive.lower()} question?",
        primitive=primitive,
        tests_hypotheses=("H1",),
        expected_value=0.7,
        expected_cost=0.2,
        retrieval_target="test",
        stop_condition="done",
        score=score,
    )


def _rule() -> reflective_rules.ReflectiveRetrievalRule:
    return reflective_rules.ReflectiveRetrievalRule(
        id=uuid4(),
        signature={
            "signal_type": "T1",
            "signal_class": "material",
            "domain_terms": ["blocked"],
            "entity_types": ["customer"],
        },
        rule_pack={
            "question_rules": [
                {
                    "prefer_primitive": "COUNTEREVIDENCE",
                    "when": "current_status_unknown",
                    "question_template": (
                        "What fresh evidence shows whether {subject} is still "
                        "blocked or already resolved?"
                    ),
                }
            ],
            "avoid_rules": [
                {
                    "primitive": "OWNERSHIP",
                    "when": "current_status_unknown",
                }
            ],
            "action_rules": [
                {
                    "primitive": "COUNTEREVIDENCE",
                    "prefer_paths": ["temporal", "semantic"],
                    "semantic_terms": ["merged", "deployed", "rollback"],
                }
            ],
        },
        utility_score=2.0,
        success_count=3,
        match_score=0.9,
    )


def test_reflective_signature_scores_matching_domain_and_scope() -> None:
    current = reflective_rules.reflective_signature_for(_trigger())
    stored = {
        "signal_type": "T1",
        "signal_class": current["signal_class"],
        "entity_types": ["customer"],
        "domain_terms": current["domain_terms"],
    }

    assert reflective_rules.reflective_signature_match_score(stored, current) > 0.7


def test_reflective_question_rules_boost_template_and_avoid_owner() -> None:
    questions = [
        _question("OWNERSHIP", question_id="Q_OWNER", score=0.75),
        _question("COUNTEREVIDENCE", question_id="Q_COUNTEREVIDENCE", score=0.62),
    ]

    shaped = reflective_rules.apply_reflective_rules_to_questions(
        questions,
        _trigger(),
        unknowns={"current status", "counterevidence"},
        rules=(_rule(),),
        score_boost=0.12,
    )

    by_primitive = {question.primitive: question for question in shaped}
    assert by_primitive["COUNTEREVIDENCE"].question.startswith(
        "What fresh evidence shows whether DeltaCo"
    )
    assert by_primitive["COUNTEREVIDENCE"].score > 0.62
    assert by_primitive["OWNERSHIP"].score < 0.75
    assert shaped[0].primitive == "COUNTEREVIDENCE"


def test_reflective_action_rules_reorder_append_terms_and_add_attribution() -> None:
    rule = _rule()
    actions = [
        RetrievalAction("Q_COUNTEREVIDENCE", "semantic", "counter", query="blocked"),
        RetrievalAction("Q_COUNTEREVIDENCE", "temporal", "recent", query="blocked"),
        RetrievalAction("Q_COUNTEREVIDENCE", "structural", "graph"),
    ]

    shaped = reflective_rules.apply_reflective_rules_to_actions(
        _question("COUNTEREVIDENCE", question_id="Q_COUNTEREVIDENCE", score=0.6),
        actions,
        rules=(rule,),
    )

    assert [action.path for action in shaped[:2]] == ["temporal", "semantic"]
    assert "rollback" in (shaped[0].query or "")
    assert shaped[0].filters["_reflective_rule_ids"] == [str(rule.id)]


@pytest.mark.asyncio
async def test_load_reflective_rules_returns_empty_when_table_missing() -> None:
    class FakeConn:
        async def fetchval(self, *_args: object) -> None:
            return None

    loaded = await reflective_rules.load_reflective_retrieval_rules(
        FakeConn(),  # type: ignore[arg-type]
        _trigger(),
        enabled=True,
        limit=5,
        match_threshold=0.42,
    )

    assert loaded == ()


def test_reflective_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INQUIRY_REFLECTIVE_RULES_ENABLED", "0")
    monkeypatch.setenv("INQUIRY_REFLECTIVE_RULE_LIMIT", "0")

    cfg = InquiryConfig.from_env()

    assert cfg.reflective_rules_enabled is False
    assert cfg.reflective_rule_limit == 0
