from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from services.platform.execution import context_packet, inquiry
from services.platform.execution.types import (
    EvidenceCard,
    Hypothesis,
    InquiryQuestion,
    QuestionAnswer,
    ResidualDebtCard,
    SufficiencyVerdict,
)
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.think.compiled_reasoning import (
    build_compiled_batch_memory_decision_request,
)


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
        inquiry._memory_decision_candidates is context_packet.memory_decision_candidates
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
    recon = packet["reconstruction_state"]
    assert recon["round_index"] == 0
    assert recon["known_model_count"] == 1
    assert recon["known_observation_count"] == 1
    assert recon["hypothesis_status"]["H1"]["support"] == 1
    assert recon["hypothesis_status"]["H1"]["weakens"] == 1


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
    assert "blocks" in by_family["claim_update"]["suggested_edge_kinds"]
    assert "explains" in by_family["claim_update"]["suggested_edge_kinds"]
    assert (
        "Q_CRITICAL_PATH:DEPENDENCY=supported"
        in by_family["claim_update"]["answer_summary"]
    )
    assert any(
        "same_issue_as or analogous_to only as candidate/review similarity" in item
        for item in by_family["claim_update"]["write_preconditions"]
    )
    assert str(model_id) in by_family["edge_insert"]["target_model_ids"]
    assert "blocks" in by_family["edge_insert"]["suggested_edge_kinds"]
    assert "explains" in by_family["edge_insert"]["suggested_edge_kinds"]
    assert (
        "Q_CRITICAL_PATH:DEPENDENCY=supported"
        in by_family["edge_insert"]["answer_summary"]
    )
    assert any(
        "Use blocks only" in item
        for item in by_family["edge_insert"]["write_preconditions"]
    )
    assert str(commitment_id) in by_family["act_update"]["target_act_ids"]
    assert by_family["no_op"]["reason"].startswith("Batch may contain")


def test_batch_fragments_compile_closed_local_atomics_without_distractors() -> None:
    storylines = {
        "Atlas release": (
            "The release certificate still has no clearly recorded owner.",
            "A late reply asks whether the certificate ownership handoff happened.",
            "The release dashboard remains optimistic while its record is incomplete.",
            "Someone says they have it now without naming the infrastructure owner.",
            "The rollout window moved after the ownership question resurfaced.",
        ),
        "Beacon migration": (
            "The privileged access review still has no clearly recorded owner.",
            "A late reply asks whether the security approval transition happened.",
            "The deployment dashboard remains optimistic while its record is incomplete.",
            "Someone says they have it now without naming the identity reviewer.",
            "Migration completion moved after the ownership question resurfaced.",
        ),
        "Cobalt renewal": (
            "The customer approval email still has no clearly recorded owner.",
            "A late reply asks whether the renewal approval transition happened.",
            "The CRM health field remains optimistic while its record is incomplete.",
            "Someone says they have it now without naming the procurement lead.",
            "The renewal signature moved after the ownership question resurfaced.",
        ),
        "Delta handoff": (
            "The named support owner still has no clearly recorded owner.",
            "A late reply asks whether the support-to-operations handoff happened.",
            "The handoff checklist remains optimistic while its record is incomplete.",
            "Someone says they have it now without naming the incident commander.",
            "The repeat incident rate moved after the ownership question resurfaced.",
        ),
    }
    fragments = []
    expected: dict[str, set[str]] = {}
    for scope, bodies in storylines.items():
        expected[scope] = set()
        for body in bodies:
            observation_id = str(uuid4())
            expected[scope].add(observation_id)
            fragments.append(
                {
                    "observation_id": observation_id,
                    "source_channel": "slack:message",
                    "text": f"{scope}, update 1: {body}",
                }
            )
    distractors = (
        "Week 1: Facilities changed the lunch delivery entrance.",
        "Week 1: The book club moved its informal discussion.",
        "Week 1: A test calendar received a new color label.",
        "Week 1: The Atlas certificate training example uses a handoff checklist.",
        "Week 1: Cobalt paint approval is listed in the Beacon office ticket.",
    )
    distractor_ids = set()
    for text in distractors:
        observation_id = str(uuid4())
        distractor_ids.add(observation_id)
        fragments.append(
            {
                "observation_id": observation_id,
                "source_channel": "slack:message",
                "text": text,
            }
        )
    all_ids = [uuid4() for _ in fragments]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=uuid4(),
        observation_id=all_ids[0],
        observation_ids=all_ids,
        seed_natural_text="Evidence window containing 25 source signals.",
        seed_signature={"batch_signal_fragments": fragments},
    )

    candidates = context_packet.memory_decision_candidates(
        trigger,
        (
            Hypothesis(
                "H2",
                "An active commitment, owner, or promised outcome is affected by the batch.",
                0.7,
                "high",
                affected_entities=("batch",),
            ),
        ),
        [],
        [],
        [],
        SufficiencyVerdict("sufficient_for_reasoning", "ready", 0, 0, ()),
    )

    # Only directly assertable facets enter the closed truth-candidate plane.
    # Questions and unresolved pronouns remain explicitly represented below,
    # but cannot be promoted as facts by the compiled writer.
    assert len(candidates) == 12
    assert {candidate.semantic_scope[0] for candidate in candidates} == set(storylines)
    for candidate in candidates:
        scope = candidate.semantic_scope[0]
        assert len(candidate.member_observation_ids) == 1
        assert set(candidate.member_observation_ids) <= expected[scope]
        assert candidate.source_observation_ids == candidate.member_observation_ids
        assert len(candidate.observation_evidence) == 1
        assert {
            row["observation_id"] for row in candidate.observation_evidence
        } == set(candidate.member_observation_ids)
        assert all(
            row["body"].startswith(f"{scope}, update 1:")
            for row in candidate.observation_evidence
        )
        assert all(
            row["source_channel"] == "slack:message"
            for row in candidate.observation_evidence
        )
        assert candidate.entailed_claim_text == candidate.proposed_text
        assert candidate.entailed_claim_text.startswith(f"{scope}, update 1: ")
        assert "churn" not in candidate.entailed_claim_text.casefold()
        assert "delay" not in candidate.entailed_claim_text.casefold()
        assert not (set(candidate.member_observation_ids) & distractor_ids)
    assert all(candidate.candidate_id != "MDC_H2" for candidate in candidates)

    uncertainty = context_packet.batch_fragment_uncertainty_signals(trigger)
    assert len(uncertainty) == 8
    assert {row["kind"] for row in uncertainty} == {
        "open_question",
        "clarification_required",
    }
    assert {row["routing"] for row in uncertainty} == {
        "open_question",
        "clarification_residual",
    }
    assert {row["observation_id"] for row in uncertainty}.isdisjoint({
        observation_id
        for candidate in candidates
        for observation_id in candidate.member_observation_ids
    })
    assert {
        row["observation_id"] for row in uncertainty
    } | {
        observation_id
        for candidate in candidates
        for observation_id in candidate.member_observation_ids
    } == set().union(*expected.values())

    compiled_packet = context_packet.compile_context_packet(
        trigger,
        "DEEP_INQUIRY_PATH",
        (),
        [],
        [],
        [],
        SufficiencyVerdict("sufficient_for_reasoning", "ready", 20, 0, ()),
        token_budget=4000,
        evidence_mode="all",
    )
    assert len(compiled_packet["memory_decision_candidates"]) == 12
    assert compiled_packet["uncertainty_signals"] == uncertainty
    request = build_compiled_batch_memory_decision_request(
        trigger,
        ContextBundle(notes={"inquiry_context_packet": compiled_packet}),
    )
    assert request is not None
    assert "<uncertainty_signals>" in request.user
    assert "Do not emit a claim or decision for them" in request.user
    assert request.user.count("<candidate>") == 12

    reordered = context_packet.memory_decision_candidates(
        TriggerContext(
            kind="T1",
            subkind="event_batch",
            tenant_id=trigger.tenant_id,
            observation_id=trigger.observation_id,
            observation_ids=list(reversed(trigger.observation_ids)),
            seed_natural_text=trigger.seed_natural_text,
            seed_signature={"batch_signal_fragments": list(reversed(fragments))},
        ),
        (),
        [],
        [],
        [],
        SufficiencyVerdict("sufficient_for_reasoning", "ready", 0, 0, ()),
    )
    def atom_projection(values):
        return {
            (
                item.candidate_id,
                item.entailed_claim_text,
                item.member_observation_ids,
            )
            for item in values
        }
    assert atom_projection(reordered) == atom_projection(candidates)

    duplicate = dict(fragments[0])
    duplicate["observation_id"] = str(uuid4())
    duplicated = context_packet.memory_decision_candidates(
        TriggerContext(
            kind="T1",
            subkind="event_batch",
            tenant_id=trigger.tenant_id,
            observation_id=trigger.observation_id,
            observation_ids=[*trigger.observation_ids, uuid4()],
            seed_natural_text=trigger.seed_natural_text,
            seed_signature={"batch_signal_fragments": [*fragments, duplicate]},
        ),
        (),
        [],
        [],
        [],
        SufficiencyVerdict("sufficient_for_reasoning", "ready", 0, 0, ()),
    )
    assert atom_projection(candidates) <= atom_projection(duplicated)
    assert all(len(item.member_observation_ids) == 1 for item in duplicated)
    assert all(len(item.observation_evidence) == 1 for item in duplicated)


def test_compile_context_packet_carries_bounded_residual_spine() -> None:
    trigger = _trigger()
    residuals = [
        ResidualDebtCard(
            residual_id=uuid4(),
            residual_kind="compression_uncertain",
            source_observation_id=uuid4(),
            compact_summary=(
                "Think succeeded but no durable model-layer fate represented "
                f"customer blocker {idx}. " + ("detail " * 120)
            ),
            reason=f"think_success_without_durable_fate:{idx}",
        )
        for idx in range(8)
    ]
    residuals.append(
        ResidualDebtCard(
            residual_id=uuid4(),
            residual_kind="validation_dropped_value",
            compact_summary="Already absorbed residual should not enter the packet.",
            reason="absorbed",
            status="absorbed",
        )
    )

    packet = context_packet.compile_context_packet(
        trigger,
        "DEEP_INQUIRY_PATH",
        (Hypothesis("H1", "Procurement risk increased.", 0.7, "high"),),
        [_question()],
        [],
        [_card("Model evidence supports procurement risk.", supports={"H1"})],
        SufficiencyVerdict(
            "sufficient_for_reasoning",
            "ready",
            1,
            0,
            (),
        ),
        token_budget=30000,
        residuals=residuals,
    )

    spine = packet["model_residual_spine"]
    policy = packet["budget"]["residual_spine"]
    assert len(spine) == 5
    assert policy["non_canonical"] is True
    assert policy["open_residual_count"] == 8
    assert policy["packet_residual_count"] == 5
    assert policy["suppressed_residual_count"] == 3
    assert all(item["non_canonical"] is True for item in spine)
    assert all("ordinary model-layer evidence" in item["use"] for item in spine)
    assert "Already absorbed" not in str(spine)
    assert max(len(item["compact_summary"]) for item in spine) <= 360


def test_memory_decision_candidates_emit_bounded_relation_slot_candidates() -> None:
    target_model_id = uuid4()
    weakener_model_id = uuid4()
    resolution_model_id = uuid4()
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=uuid4(),
        observation_id=uuid4(),
        seed_natural_text=(
            "Incident opacity contradicts launch confidence, while audit export "
            "unblocks renewal approval."
        ),
    )
    target_card = _card(
        "Existing model says launch confidence remains high.",
        raw_content_ref=f"model:{target_model_id}",
        source_type="model",
        source_ref_id=target_model_id,
        supports={"H1"},
        score=0.9,
    )
    weakener_card = _card(
        "Incident opacity contradicts launch confidence.",
        raw_content_ref=f"model:{weakener_model_id}",
        source_type="model",
        source_ref_id=weakener_model_id,
        weakens={"H1"},
        score=0.95,
    )
    resolution_card = _card(
        "Audit export unblocks renewal approval.",
        raw_content_ref=f"model:{resolution_model_id}",
        source_type="model",
        source_ref_id=resolution_model_id,
        supports={"H1"},
        score=0.88,
    )

    candidates = context_packet.memory_decision_candidates(
        trigger,
        (
            Hypothesis(
                id="H1",
                claim="Launch confidence and renewal approval changed.",
                confidence=0.78,
                impact_if_true="high",
                delta_type="update",
                target_model_ids=(str(target_model_id),),
            ),
        ),
        [_question(primitive="COUNTEREVIDENCE", question_id="Q_COUNTER")],
        [
            QuestionAnswer(
                "Q_COUNTER",
                "supported",
                "Counterevidence and resolution evidence found.",
                supporting_evidence=(str(resolution_card.evidence_id),),
                counterevidence=(str(weakener_card.evidence_id),),
            )
        ],
        [target_card, weakener_card, resolution_card],
        SufficiencyVerdict(
            "sufficient_for_reasoning",
            "ready",
            3,
            0,
            (),
        ),
    )

    slot_candidates = {
        candidate.suggested_edge_kinds[0]: candidate
        for candidate in candidates
        if candidate.candidate_id.startswith("MDC_SLOT_")
    }
    assert set(slot_candidates) == {"weakens", "contributes_to_resolution"}
    weakens = slot_candidates["weakens"]
    assert weakens.target_model_ids == (str(target_model_id),)
    assert weakens.evidence_model_ids[:2] == (
        str(weakener_model_id),
        str(target_model_id),
    )
    assert "weakens" in weakens.proposed_text
    resolution = slot_candidates["contributes_to_resolution"]
    assert resolution.suggested_edge_kinds == (
        "contributes_to_resolution",
        "supports",
    )
    assert "unblocks" in resolution.proposed_text
    assert len(slot_candidates) == 2


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
