from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from services.platform.execution import inquiry, question_generation
from services.platform.execution.types import Hypothesis
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext


def _trigger(
    text: str,
    *,
    seed_entity_ids: list[dict[str, object]] | None = None,
) -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_natural_text=text,
        seed_entity_ids=seed_entity_ids or [],
    )


def _baseline(
    *,
    commitments: int = 0,
    goals: int = 0,
    models: int = 0,
) -> RetrievalResult:
    trigger = _trigger("baseline")
    return RetrievalResult(
        trigger=trigger,
        models=[SimpleNamespace(id=uuid4()) for _ in range(models)],
        acts={
            "commitments": [SimpleNamespace(id=uuid4()) for _ in range(commitments)],
            "goals": [SimpleNamespace(id=uuid4()) for _ in range(goals)],
            "decisions": [],
        },
    )


def _hypotheses() -> tuple[Hypothesis, ...]:
    return (
        Hypothesis(
            id="H1",
            claim="The launch blocker is material.",
            confidence=0.7,
            impact_if_true="high",
        ),
        Hypothesis(
            id="H2",
            claim="A commitment is affected.",
            confidence=0.6,
            impact_if_true="medium",
        ),
        Hypothesis(
            id="H0",
            claim="No update is needed.",
            confidence=0.2,
            impact_if_true="low",
        ),
    )


def test_question_generation_helpers_keep_legacy_inquiry_identity() -> None:
    assert inquiry._candidate_questions is question_generation.candidate_questions
    assert inquiry._dedupe_unknowns is question_generation.dedupe_unknowns
    assert (
        inquiry._deterministic_delta_uncertainties
        is question_generation.deterministic_delta_uncertainties
    )
    assert inquiry._generate_hypotheses is question_generation.generate_hypotheses
    assert inquiry._initial_unknowns is question_generation.initial_unknowns


def test_generate_hypotheses_anchors_risk_commitment_and_no_op() -> None:
    trigger = _trigger(
        "AcmeAtlas launch is blocked by security review capacity and SSO is at risk.",
        seed_entity_ids=[
            {"type": "customer", "label": "AcmeAtlas"},
            {"type": "system", "name": "Enterprise SSO"},
        ],
    )

    hypotheses = question_generation.generate_hypotheses(
        trigger,
        _baseline(commitments=1),
    )

    by_id = {hypothesis.id: hypothesis for hypothesis in hypotheses}
    assert set(by_id) == {"H1", "H2", "H0"}
    assert by_id["H1"].impact_if_true == "high"
    assert "AcmeAtlas" in by_id["H1"].affected_entities
    assert "which active commitment is affected" in by_id["H2"].uncertainty_slots
    assert by_id["H0"].delta_type == "no_op"


def test_initial_unknowns_and_deterministic_uncertainties_are_deduped() -> None:
    trigger = _trigger("Who owns the renewal blocker that repeats every quarter?")

    unknowns = question_generation.initial_unknowns(trigger, _baseline(goals=1))
    uncertainties = question_generation.deterministic_delta_uncertainties(
        "who owns the renewal blocker that repeats every quarter?"
    )

    assert "responsible owner" in unknowns
    assert "affected commitment" in unknowns
    assert "affected goal" not in unknowns
    assert "who owns the next action" in uncertainties
    assert question_generation.dedupe_unknowns(["Owner", "owner", "", None]) == [
        "Owner"
    ]


def test_candidate_questions_preserve_specific_signal_focus_and_scores() -> None:
    trigger = _trigger(
        "AcmeAtlas launch is blocked by security review capacity.",
        seed_entity_ids=[{"type": "customer", "label": "AcmeAtlas"}],
    )

    questions = question_generation.candidate_questions(
        trigger,
        _hypotheses(),
        evidence_by_key={},
        unknowns={"responsible owner", "affected commitment", "counterevidence"},
    )

    by_primitive = {question.primitive: question for question in questions}
    assert by_primitive["DEPENDENCY"].question == (
        "Is security review capacity the dependency that puts AcmeAtlas "
        "on the critical path?"
    )
    assert "security review capacity" in by_primitive["OWNERSHIP"].question
    assert by_primitive["OWNERSHIP"].score == (round(0.72 - 0.22 + 0.15, 4))

    with_existing_evidence = question_generation.candidate_questions(
        trigger,
        _hypotheses(),
        evidence_by_key={("model", str(uuid4())): object() for _ in range(5)},  # type: ignore[dict-item]
        unknowns=set(),
    )
    assert with_existing_evidence[0].score == questions[0].score - 0.15
