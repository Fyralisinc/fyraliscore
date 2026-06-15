from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from services.platform.execution import context_packet, inquiry
from services.platform.execution.types import (
    EvidenceCard,
    Hypothesis,
    InquiryQuestion,
    QuestionAnswer,
    SufficiencyVerdict,
)
from services.reasoning.retrieval.primary import TriggerContext


def _trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_natural_text="HarborRail procurement blocker needs audit evidence",
    )


def _question(
    *,
    question_id: str = "Q_CONSTRAINT",
    primitive: str = "CONSTRAINT",
) -> InquiryQuestion:
    return InquiryQuestion(
        question_id=question_id,
        question="Which procurement constraint blocks HarborRail renewal?",
        primitive=primitive,
        tests_hypotheses=("H1",),
        expected_value=0.9,
        expected_cost=0.2,
        retrieval_target="constraint_evidence",
        stop_condition="constraint found",
        score=0.7,
    )


def _card(
    summary: str,
    *,
    source_type: str = "model",
    raw_content_ref: str | None = None,
    score: float = 0.7,
    trust_tier: str | None = "model",
    questions: set[str] | None = None,
    supports: set[str] | None = None,
    weakens: set[str] | None = None,
    contradicts: set[str] | None = None,
    paths: set[str] | None = None,
    source_ref_id=None,
) -> EvidenceCard:
    source_ref = raw_content_ref or f"{source_type}:{uuid4()}"
    return EvidenceCard(
        evidence_id=uuid4(),
        source_type=source_type,
        source_ref=source_ref,
        source_ref_id=source_ref_id or uuid4(),
        summary=summary,
        trust_tier=trust_tier,
        timestamp=datetime(2026, 6, 13, tzinfo=timezone.utc),
        retrieval_paths=paths or {"semantic"},
        retrieved_for_questions=questions or {"Q_CONSTRAINT"},
        supports_hypotheses=supports or set(),
        weakens_hypotheses=weakens or set(),
        contradicts_hypotheses=contradicts or set(),
        raw_content_ref=source_ref,
        token_estimate=12,
        score=score,
    )


def test_context_packet_helpers_keep_legacy_inquiry_identity() -> None:
    assert inquiry._rank_evidence is context_packet.rank_evidence
    assert (
        inquiry._select_minimal_sufficient_evidence
        is context_packet.select_minimal_sufficient_evidence
    )
    assert inquiry._compile_context_packet is context_packet.compile_context_packet
    assert inquiry._evidence_value is context_packet.evidence_value
    assert inquiry._filter_context_packet_evidence is (
        context_packet.filter_context_packet_evidence
    )
    assert inquiry._candidate_state_changes is context_packet.candidate_state_changes
    assert (
        inquiry._memory_decision_candidates
        is context_packet.memory_decision_candidates
    )


def test_rank_evidence_prefers_useful_and_recent_cards() -> None:
    weak = _card("Generic unlinked model", score=0.9)
    support = _card(
        "Procurement blocker has audit evidence",
        score=0.6,
        supports={"H1"},
        trust_tier="authoritative",
    )
    counter = _card(
        "Customer accepted the mitigation",
        score=0.7,
        weakens={"H1"},
        trust_tier="authoritative",
    )

    ranked = context_packet.rank_evidence([weak, support, counter], limit=2)

    assert ranked == [counter, support]
    assert weak not in ranked


def test_select_minimal_sufficient_evidence_protects_answers_and_anchors() -> None:
    support = _card(
        "Audit evidence blocks procurement approval",
        supports={"H1"},
        score=1.0,
    )
    counter = _card(
        "Signed exception weakens the blocker",
        questions={"Q_COUNTEREVIDENCE"},
        weakens={"H1"},
        score=0.95,
    )
    owner = _card(
        "commitment launch owner=platform enablement",
        source_type="commitment",
        questions={"Q_OWNER"},
        supports={"H2"},
        score=0.8,
    )
    noise = [
        _card(f"Generic duplicate dashboard noise {idx}", score=0.3)
        for idx in range(10)
    ]

    selected, report = context_packet.select_minimal_sufficient_evidence(
        [support, counter, owner, *noise],
        hypotheses=(
            Hypothesis("H1", "The blocker is real.", 0.7, "high"),
            Hypothesis("H2", "An owner exists.", 0.5, "medium"),
        ),
        questions=[
            _question(),
            _question(question_id="Q_COUNTEREVIDENCE", primitive="COUNTEREVIDENCE"),
            _question(question_id="Q_OWNER", primitive="OWNERSHIP"),
        ],
        answers=[
            QuestionAnswer(
                "Q_CONSTRAINT",
                "supported",
                "Constraint answered.",
                supporting_evidence=(str(support.evidence_id),),
            ),
            QuestionAnswer(
                "Q_COUNTEREVIDENCE",
                "supported",
                "Counter evidence answered.",
                counterevidence=(str(counter.evidence_id),),
            ),
        ],
        route="DEEP_INQUIRY_PATH",
        mode="deep",
        evidence_limit=8,
    )

    selected_ids = {card.evidence_id for card in selected}
    assert {support.evidence_id, counter.evidence_id, owner.evidence_id} <= selected_ids
    assert report["coverage"]["questions"] == 1.0
    assert report["protected_count"] >= 3


def test_compile_context_packet_model_first_suppresses_redundant_observations() -> None:
    trigger = _trigger()
    model_card = _card(
        "Model says procurement risk increased because audit export is blocked.",
        raw_content_ref="model:risk",
        supports={"H1"},
        score=0.9,
    )
    redundant_observation = _card(
        "Raw message repeats that procurement risk increased.",
        source_type="observation",
        raw_content_ref="observation:duplicate",
        supports={"H1"},
        paths={"sage_reader"},
        score=0.7,
        trust_tier="authoritative",
    )
    counter_observation = _card(
        "Latest note says the customer accepted the mitigation.",
        source_type="observation",
        raw_content_ref="observation:counter",
        weakens={"H1"},
        paths={"sage_reader"},
        score=0.8,
        trust_tier="authoritative",
    )
    answer = QuestionAnswer(
        "Q_CONSTRAINT",
        "supported",
        "Model-backed risk evidence was found.",
        supporting_evidence=(str(model_card.evidence_id),),
        counterevidence=(str(counter_observation.evidence_id),),
    )

    packet = context_packet.compile_context_packet(
        trigger,
        "DEEP_INQUIRY_PATH",
        (Hypothesis("H1", "Procurement risk increased.", 0.7, "high"),),
        [_question()],
        [answer],
        [model_card, redundant_observation, counter_observation],
        SufficiencyVerdict(
            "sufficient_for_reasoning",
            "model-first evidence test",
            3,
            1,
            (),
        ),
        token_budget=4000,
        evidence_mode="model_first",
    )

    decisive_refs = {
        item["raw_content_ref"] for item in packet["tiers"]["decisive_evidence"]
    }
    supporting_refs = {
        ref
        for group in packet["tiers"]["supporting_evidence_groups"]
        for ref in group["source_refs"]
    }
    assert "model:risk" in supporting_refs
    assert "observation:counter" in decisive_refs
    assert "observation:duplicate" not in decisive_refs | supporting_refs
    assert packet["budget"]["evidence_policy"]["suppressed_observation_count"] == 1


def test_candidate_state_changes_names_act_and_pattern_hints() -> None:
    changes = context_packet.candidate_state_changes(
        (
            Hypothesis("H1", "Commitment blocker is real.", 0.7, "high"),
            Hypothesis("H3", "This may recur.", 0.4, "medium"),
        ),
        [
            _card(
                "Commitment evidence supports blocker.",
                source_type="commitment",
                supports={"H1"},
            ),
            _card(
                "Model evidence supports recurrence.",
                source_type="model",
                supports={"H3"},
            ),
        ],
        SufficiencyVerdict(
            "sufficient_for_reasoning",
            "ready",
            2,
            1,
            (),
        ),
    )

    assert {change["kind"] for change in changes} == {
        "possible_act_update",
        "possible_model",
    }


def test_compile_context_packet_emits_memory_decision_candidates() -> None:
    observation_id = uuid4()
    model_id = uuid4()
    commitment_id = uuid4()
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=uuid4(),
        observation_id=observation_id,
        observation_ids=[observation_id, uuid4()],
        seed_natural_text="HarborRail launch is blocked by audit evidence.",
    )
    model_card = _card(
        "Existing model says HarborRail launch risk is active.",
        raw_content_ref=f"model:{model_id}",
        source_type="model",
        supports={"H1"},
        score=0.9,
    )
    commitment_card = _card(
        "Active commitment depends on audit evidence.",
        raw_content_ref=f"commitment:{commitment_id}",
        source_type="commitment",
        source_ref_id=commitment_id,
        supports={"H1"},
        score=0.85,
    )
    counter_card = _card(
        "Customer may accept a temporary exception.",
        raw_content_ref="observation:counter",
        source_type="observation",
        weakens={"H1"},
        score=0.8,
        trust_tier="authoritative",
    )

    packet = context_packet.compile_context_packet(
        trigger,
        "DEEP_INQUIRY_PATH",
        (
            Hypothesis(
                id="H1",
                claim="HarborRail launch is blocked by audit evidence.",
                confidence=0.74,
                impact_if_true="high",
                delta_type="update",
                target_model_ids=(str(model_id),),
                uncertainty_slots=("whether audit evidence is on the critical path",),
                evidence_needed=("dependency_evidence",),
            ),
            Hypothesis(
                id="H0",
                claim="The batch is already captured or background only.",
                confidence=0.22,
                impact_if_true="low",
                delta_type="no_op",
            ),
        ),
        [
            _question(primitive="DEPENDENCY", question_id="Q_CRITICAL_PATH"),
            _question(primitive="OWNERSHIP", question_id="Q_OWNER"),
        ],
        [
            QuestionAnswer(
                "Q_CRITICAL_PATH",
                "supported",
                "Dependency evidence found.",
                supporting_evidence=(str(model_card.evidence_id),),
                counterevidence=(str(counter_card.evidence_id),),
            )
        ],
        [model_card, commitment_card, counter_card],
        SufficiencyVerdict(
            "sufficient_for_reasoning",
            "ready",
            3,
            1,
            (),
        ),
        token_budget=4000,
        evidence_mode="all",
    )

    candidates = packet["memory_decision_candidates"]
    by_family = {candidate["op_family"]: candidate for candidate in candidates}
    assert {"claim_update", "edge_insert", "act_update", "no_op"} <= set(by_family)
    assert str(model_id) in by_family["claim_update"]["target_model_ids"]
    assert str(observation_id) in by_family["claim_update"]["source_observation_ids"]
    assert str(model_id) in by_family["edge_insert"]["target_model_ids"]
    assert "blocks" in by_family["edge_insert"]["suggested_edge_kinds"]
    assert "explains" in by_family["edge_insert"]["suggested_edge_kinds"]
    assert "Q_CRITICAL_PATH:DEPENDENCY=supported" in by_family["edge_insert"][
        "answer_summary"
    ]
    assert any(
        "Use blocks only" in item
        for item in by_family["edge_insert"]["write_preconditions"]
    )
    assert str(commitment_id) in by_family["act_update"]["target_act_ids"]
    assert by_family["no_op"]["reason"].startswith("Batch may contain")


def test_memory_decision_candidates_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setenv("INQUIRY_MEMORY_DECISION_CANDIDATES", "0")
    packet = context_packet.compile_context_packet(
        _trigger(),
        "DEEP_INQUIRY_PATH",
        (Hypothesis("H1", "Procurement risk increased.", 0.7, "high"),),
        [_question()],
        [],
        [_card("Procurement risk evidence.", supports={"H1"})],
        SufficiencyVerdict(
            "sufficient_for_reasoning",
            "ready",
            1,
            0,
            (),
        ),
        token_budget=4000,
    )

    assert packet["memory_decision_candidates"] == []
