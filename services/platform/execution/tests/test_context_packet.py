from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import asyncpg
import pytest
from pgvector.asyncpg import register_vector

from lib.shared.types import ModelCreate
from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from services.domain.models.repo import ModelsRepo
from services.domain.entity_grounding.learned_discovery import PersistedSignalText
from services.evaluation.epistemic_repair.p6_think_runner import (
    _p6_simulation_mention_adapter,
)
from services.platform.execution import context_packet, inquiry
from services.platform.execution.types import (
    EvidenceCard,
    Hypothesis,
    InquiryQuestion,
    MemoryDecisionCandidate,
    QuestionAnswer,
    ResidualDebtCard,
    SufficiencyVerdict,
)
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryCandidateDecision,
    BatchMemoryDecisionSet,
    build_compiled_batch_memory_decision_request,
)
from services.reasoning.think.applier import apply_diff
from services.reasoning.think.diff_schema import ValidatedDiff
from services.reasoning.think.truth_admission import admit_validated_think_claim


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


def _grounded_scope(
    scope: str,
    canonical_ref: str | None = None,
) -> list[dict[str, str]]:
    return [{
        "surface": scope,
        "canonical_ref": canonical_ref or (
            "workstream:" + "-".join(scope.casefold().split())
        ),
    }]


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


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("Who owns the release certificate?", "open_question"),
        ("A reviewer asks if approval happened", "open_question"),
        ("Is the migration complete", "open_question"),
        ("The approval status is still unclear", "clarification_required"),
        ("The handoff might have happened", "clarification_required"),
        ("The dashboard remains incomplete", None),
    ],
)
def test_batch_uncertainty_boundary_covers_question_and_ambiguity_paraphrases(
    body: str,
    expected: str | None,
) -> None:
    assert context_packet._batch_fragment_uncertainty_kind(body) == expected


def test_unprefixed_recognized_scope_assertion_becomes_closed_atomic() -> None:
    prefixed_one = str(uuid4())
    prefixed_two = str(uuid4())
    conclusion = str(uuid4())
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=uuid4(),
        observation_id=uuid4(),
        observation_ids=[uuid4(), uuid4(), uuid4()],
        seed_natural_text="Three Orion delivery signals.",
        seed_signature={"batch_signal_fragments": [
            {
                "observation_id": prefixed_one,
                "source_channel": "slack:message",
                "text": "Orion delivery, update 4: The record links pending approval to rollout delay.",
                "grounded_mentions": _grounded_scope("Orion delivery"),
            },
            {
                "observation_id": prefixed_two,
                "source_channel": "slack:message",
                "text": "Orion delivery, update 4: The rollout window moved.",
                "grounded_mentions": _grounded_scope("Orion delivery"),
            },
            {
                "observation_id": conclusion,
                "source_channel": "slack:message",
                "text": "Orion delivery is ready.",
                "grounded_mentions": _grounded_scope("Orion delivery"),
            },
        ]},
    )

    candidates, material = context_packet._batch_fragment_candidates(trigger)

    matching = [
        candidate for candidate in candidates
        if candidate.member_observation_ids == (conclusion,)
    ]
    assert material
    assert len(matching) == 1
    assert matching[0].semantic_scope == ("Orion delivery",)
    assert matching[0].entailed_claim_text == "Orion delivery is ready."
    assert context_packet._is_scope_level_synthesis_assertion(
        matching[0], "Orion delivery"
    )
    synthesis = context_packet._scoped_synthesis_candidates(
        candidates,
        [
            _card("Accepted Model for Orion delivery records approval state."),
            _card("Accepted Model for Orion delivery records schedule impact."),
        ],
    )
    assert len(synthesis) == 1
    assert synthesis[0].member_observation_ids == (conclusion,)
    assert synthesis[0].source_observation_ids == (conclusion,)
    # Synthesis is an additional candidate; it never consumes its direct
    # conclusion atomic.
    assert matching[0] in candidates


def test_batch_fragments_compile_closed_local_atomics_without_distractors() -> None:
    storylines = {
        "Atlas release": (
            "A source links the open release certificate to the moved rollout window.",
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
                    "grounded_mentions": _grounded_scope(scope),
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

    with_prior_model = context_packet.memory_decision_candidates(
        trigger,
        (),
        [],
        [],
        [_card(
            "Accepted Model for Atlas release records earlier ownership state.",
            raw_content_ref=f"model:{uuid4()}",
        ), _card(
            "Accepted Model for Atlas release records a separate schedule impact.",
            raw_content_ref=f"model:{uuid4()}",
        )],
        SufficiencyVerdict("sufficient_for_reasoning", "ready", 1, 0, ()),
    )
    synthesis = [
        item for item in with_prior_model
        if item.candidate_id.startswith("MDC_SYNTH_")
    ]
    assert synthesis == []

    synthesis_fragments = [dict(fragment) for fragment in fragments]
    atlas_fragment = next(
        fragment
        for fragment in synthesis_fragments
        if fragment["text"].startswith("Atlas release, update 1: Someone says")
    )
    atlas_fragment["text"] = "Atlas release, update 4: Atlas release is ready."
    synthesis_trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=trigger.tenant_id,
        observation_id=all_ids[0],
        observation_ids=all_ids,
        seed_natural_text="Evidence window containing 25 source signals.",
        seed_signature={"batch_signal_fragments": synthesis_fragments},
    )
    with_synthesis_opportunity = context_packet.memory_decision_candidates(
        synthesis_trigger,
        (),
        [],
        [],
        [_card(
            "Accepted Model for Atlas release records earlier ownership state.",
            raw_content_ref=f"model:{uuid4()}",
        ), _card(
            "Accepted Model for Atlas release records a separate schedule impact.",
            raw_content_ref=f"model:{uuid4()}",
        )],
        SufficiencyVerdict("sufficient_for_reasoning", "ready", 1, 0, ()),
    )
    synthesis = [
        item for item in with_synthesis_opportunity
        if item.candidate_id.startswith("MDC_SYNTH_")
    ]
    assert len(synthesis) == 1
    assert synthesis[0].semantic_scope == ("Atlas release",)
    assert synthesis[0].candidate_kind == "synthesis"
    assert synthesis[0].allowed_operations == ("situation_and_edge", "no_op")
    assert synthesis[0].evidence_model_ids
    assert synthesis[0].proposed_text == "Atlas release, update 4: Atlas release is ready."
    assert synthesis[0].observation_evidence == ({
        "observation_id": atlas_fragment["observation_id"],
        "source_channel": atlas_fragment["source_channel"],
        "body": atlas_fragment["text"],
        "canonical_ref": "workstream:atlas-release",
    },)
    assert len(with_synthesis_opportunity) == 14
    synthesis_request = build_compiled_batch_memory_decision_request(
        synthesis_trigger,
        ContextBundle(notes={"inquiry_context_packet": {
            "signal_summary": "Entity-scoped mixed batch",
            "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
            "memory_decision_candidates": [
                asdict(candidate) for candidate in with_synthesis_opportunity
            ],
            "important_unknowns": [],
            "tiers": {},
        }}),
    )
    assert synthesis_request is not None
    assert len(synthesis_request.candidates) == 14
    assert "MDC_SYNTH_" in synthesis_request.user
    assert not any(
        obligation.candidate_id == synthesis[0].candidate_id
        and obligation.source_model_id is not None
        and obligation.target_model_id is not None
        for obligation in synthesis_request.relation_obligations
    )
    no_write = synthesis_request.to_raw_diff(
        BatchMemoryDecisionSet(decisions=[BatchMemoryCandidateDecision(
            candidate_id=synthesis[0].candidate_id,
            decision="accept", operation="situation_and_edge", confidence=0.8,
            claim_text="Atlas release is ready.",
            edge_kind="blocks", reason="A relation was not explicitly bound.",
        )]),
        trigger=synthesis_trigger,
        trigger_ref=uuid4(),
    )
    assert not any(
        (op.entry or {}).get("proposition", {}).get("claim_role") == "situation"
        for op in no_write.claim_ops
    )
    assert no_write.relation_claim_ops == []
    assert "invalid or stale closed-set relation binding" in no_write.reasoning_trace

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


def test_scoped_synthesis_requires_conclusion_and_diverse_prior_models() -> None:
    scope = "Orion rollout"
    scope_ref = "workstream:orion-rollout"
    ordinary_id = str(uuid4())
    adverse_id = str(uuid4())
    ordinary = MemoryDecisionCandidate(
        candidate_id="ordinary", op_family="claim_insert", candidate_kind="atomic",
        proposed_text="A record links the open owner handoff to the delayed rollout.",
        entailed_claim_text="A record links the open owner handoff to the delayed rollout.",
        member_observation_ids=(ordinary_id,), semantic_scope=(scope,),
        canonical_scope_ref=scope_ref,
        observation_evidence=({"observation_id": ordinary_id, "body":
            "A record links the open owner handoff to the delayed rollout."},),
    )
    adverse = MemoryDecisionCandidate(
        candidate_id="adverse", op_family="claim_insert", candidate_kind="atomic",
        proposed_text="The rollout window moved after ownership became unclear.",
        entailed_claim_text="The rollout window moved after ownership became unclear.",
        member_observation_ids=(adverse_id,), semantic_scope=(scope,),
        canonical_scope_ref=scope_ref,
        observation_evidence=({"observation_id": adverse_id, "body":
            "The rollout window moved after ownership became unclear."},),
    )
    conclusion = MemoryDecisionCandidate(
        candidate_id="conclusion", op_family="claim_insert", candidate_kind="atomic",
        proposed_text=f"{scope} is blocked.",
        entailed_claim_text=f"{scope} is blocked.",
        member_observation_ids=(str(uuid4()),), semantic_scope=(scope,),
        canonical_scope_ref=scope_ref,
    )
    diverse_memory = [
        _card(f"{scope} previously lacked an owner."),
        _card(f"{scope} previously missed a delivery window."),
    ]

    assert context_packet._scoped_synthesis_candidates(
        [ordinary], diverse_memory,
    ) == []
    assert context_packet._scoped_synthesis_candidates(
        [conclusion], diverse_memory[:1],
    ) == []
    candidates = context_packet._scoped_synthesis_candidates(
        [ordinary, adverse, conclusion], diverse_memory,
    )
    assert len(candidates) == 1
    assert candidates[0].semantic_scope == (scope,)
    assert len(candidates[0].evidence_model_ids) == 2
    assert candidates[0].allowed_operations == ("situation_and_edge", "no_op")


def test_scoped_synthesis_rejects_missing_or_colliding_scope_refs() -> None:
    scope = "Orion rollout"
    evidence = [_card(f"{scope} model one"), _card(f"{scope} model two")]

    def candidates(refs: tuple[str | None, str | None, str | None]):
        texts = (
            "A record links the open owner handoff to the delayed rollout.",
            "The rollout moved after ownership became unclear.",
            f"{scope} is blocked.",
        )
        return [MemoryDecisionCandidate(
            candidate_id=f"c-{index}", op_family="claim_insert",
            candidate_kind="atomic", proposed_text=text,
            entailed_claim_text=text, member_observation_ids=(str(uuid4()),),
            semantic_scope=(scope,), canonical_scope_ref=ref,
            observation_evidence=({"observation_id": str(uuid4()), "body": text},),
        ) for index, (text, ref) in enumerate(zip(texts, refs, strict=True))]

    assert context_packet._scoped_synthesis_candidates(
        candidates((None, None, None)), evidence,
    ) == []
    assert context_packet._scoped_synthesis_candidates(
        candidates(("workstream:orion-rollout", "workstream:other",
                    "workstream:orion-rollout")), evidence,
    ) == []


def test_scope_synthesis_rejects_unmatched_typed_entity_fallback() -> None:
    scope = "Atlas release"
    fragments = [
        {
            "observation_id": str(uuid4()),
            "source_channel": "test",
            "text": text,
            "canonical_ref": "customer:acme",
            "entities_mentioned": [{"type": "customer", "id": "acme"}],
        }
        for text in (
            f"{scope}, update 4: A record links the open owner to delay.",
            f"{scope}, update 4: Completion moved after ownership became unclear.",
            f"{scope} is blocked.",
        )
    ]
    trigger = TriggerContext(
        kind="T1", subkind="event_batch", tenant_id=uuid4(),
        observation_ids=[uuid4() for _ in fragments],
        seed_signature={"batch_signal_fragments": fragments},
    )
    candidates, _ = context_packet._batch_fragment_candidates(trigger)
    assert candidates
    assert all(candidate.canonical_scope_ref is None for candidate in candidates)
    assert context_packet._scoped_synthesis_candidates(
        candidates, [_card(f"{scope} model one"), _card(f"{scope} model two")],
    ) == []


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ({"kind": "belief", "subject": "Atlas"},
         {"kind": "belief", "subject": "Atlas"}),
        ('{"kind":"belief","subject":"Atlas"}',
         {"kind": "belief", "subject": "Atlas"}),
        ('["not", "an", "object"]', None),
        ("not-json", None),
    ],
)
def test_hydrated_model_proposition_accepts_only_json_objects(
    raw: object, expected: dict[str, object] | None,
) -> None:
    assert context_packet._json_object_or_none(raw) == expected


def test_actual_p6_batch4_opens_grounded_synthesis_without_seed_components() -> None:
    batch = build_p6_population().batches[3]
    observation_ids = {
        signal.signal_id: uuid5(NAMESPACE_URL, signal.signal_id)
        for signal in batch.signals
    }
    adapter_candidates = _p6_simulation_mention_adapter(tuple(
        PersistedSignalText(
            signal_id=observation_ids[signal.signal_id],
            source_channel=signal.source_channel,
            content_text=signal.text,
        )
        for signal in batch.signals
    ))
    mentions_by_observation: dict[UUID, list[dict[str, str]]] = {}
    for mention in adapter_candidates:
        mentions_by_observation.setdefault(mention.signal_id, []).append({
            "surface": mention.surface,
            "canonical_ref": str(mention.provisional_canonical_ref or ""),
        })
    trigger = TriggerContext(
        kind="T1", subkind="event_batch", tenant_id=uuid4(),
        observation_ids=list(observation_ids.values()),
        seed_signature={"batch_signal_fragments": [
            {"observation_id": str(observation_ids[signal.signal_id]),
             "source_channel": signal.source_channel, "text": signal.text,
             "grounded_mentions": mentions_by_observation.get(
                 observation_ids[signal.signal_id], []
             )}
            for signal in batch.signals
        ]},
    )
    model_ids = (uuid4(), uuid4())
    versions = (uuid4(), uuid4())
    candidates = context_packet.memory_decision_candidates(
        trigger, (), [], [], [],
        SufficiencyVerdict("sufficient_for_reasoning", "ready", 0, 0, ()),
        synthesis_scope_models={"Atlas release": tuple(map(str, model_ids))},
    )
    synthesis = [item for item in candidates if item.candidate_kind == "synthesis"]
    assert len(synthesis) == 1
    candidate = synthesis[0]
    assert candidate.member_observation_ids == (
        str(observation_ids["p6-b04-s09"]),
    )
    assert set(candidate.relation_evidence_observation_ids) == {
        str(observation_ids[key])
        for key in ("p6-b04-s01", "p6-b04-s05", "p6-b04-s13")
    }
    assert str(observation_ids["p6-b04-s17"]) not in (
        candidate.relation_evidence_observation_ids
    )
    packet = {
        "memory_decision_candidates": [asdict(item) for item in candidates],
        "signal_summary": "P6 sealed batch 4",
        "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
        "synthesis_scope_hydration": {
            "endpoint_model_versions": dict(zip(
                map(str, model_ids), map(str, versions), strict=True,
            )),
            "endpoint_model_cards": {
                str(model_id): {
                    "id": str(model_id), "version_id": str(version),
                    "natural": f"Accepted Atlas model {index}",
                    "proposition": {"kind": "belief", "subject": "Atlas release"},
                    "canonical_scope": {
                        "label": "Atlas release", "ref": "workstream:atlas-release",
                    },
                }
                for index, (model_id, version) in enumerate(
                    zip(model_ids, versions, strict=True), start=1,
                )
            },
        },
    }
    request = build_compiled_batch_memory_decision_request(
        trigger, ContextBundle(notes={"inquiry_context_packet": packet}),
    )
    assert request is not None
    obligation = next(
        item for item in request.relation_obligations
        if item.candidate_id == candidate.candidate_id
    )
    assert obligation.edge_kind == "blocks"
    assert set(obligation.evidence_event_ids) == {
        observation_ids[key]
        for key in ("p6-b04-s01", "p6-b04-s05", "p6-b04-s13")
    }
    assert obligation.source_model_id is None
    assert obligation.target_model_id is None
    compiled = request.to_raw_diff(
        BatchMemoryDecisionSet(decisions=[BatchMemoryCandidateDecision(
            candidate_id=candidate.candidate_id, decision="accept",
            operation="situation_and_edge", confidence=0.8,
            source_model_id=model_ids[0], target_model_id=model_ids[1],
            situation_member_model_ids=list(model_ids),
            claim_text="Atlas readiness is blocked by certificate ownership.",
            reason="The exact auxiliary evidence establishes the dependency.",
        )]),
        trigger=trigger, trigger_ref=uuid4(),
    )
    assert len(compiled.relation_claim_ops) == 1
    assert compiled.relation_claim_ops[0].metadata["atomic_with_synthesis"] is True


@pytest.mark.asyncio
async def test_conclusion_hydrates_scope_complete_memory_outside_selected_retrieval() -> None:
    scopes = ("Atlas release", "Beacon migration", "Cobalt renewal", "Delta handoff")
    fragments = []
    for scope in scopes:
        fragments.extend([
            {"observation_id": str(uuid4()), "source_channel": "test",
             "text": f"{scope}, update 4: A record links the open owner handoff to delayed completion.",
             "grounded_mentions": _grounded_scope(scope)},
            {"observation_id": str(uuid4()), "source_channel": "test",
             "text": f"{scope}, update 4: Completion moved again.",
             "grounded_mentions": _grounded_scope(scope)},
        ])
    conclusion_id = str(uuid4())
    fragments.append({
        "observation_id": conclusion_id, "source_channel": "test",
        "text": "Delta handoff is blocked.",
        "grounded_mentions": _grounded_scope("Delta handoff"),
    })
    trigger = TriggerContext(
        kind="T1", subkind="event_batch", tenant_id=uuid4(),
        observation_ids=[uuid4() for _ in fragments],
        seed_natural_text="Four independent company scopes",
        seed_signature={"batch_signal_fragments": fragments},
    )
    delta_ids = [uuid4(), uuid4(), uuid4()]
    delta_versions = [uuid4(), uuid4(), uuid4()]

    class Connection:
        calls = 0

        async def fetch(self, _query, tenant_id, labels, limit):
            self.calls += 1
            assert tenant_id == trigger.tenant_id
            assert labels == ["Delta handoff"]
            assert limit == 8
            return [
                {"scope_label": "Delta handoff",
                 "scope_ref": "workstream:delta-handoff", "id": model_id,
                 "truth_version_id": version_id,
                 "natural_text": f"Accepted Delta handoff model {model_id}",
                 "proposition": {"kind": "belief", "subject": "Delta handoff"}}
                for model_id, version_id in zip(delta_ids, delta_versions, strict=True)
            ]

    conn = Connection()
    hydrated, receipt = await context_packet.hydrate_synthesis_scope_models(
        conn, trigger
    )
    # Arbitrary selected retrieval contains three other scopes and omits Delta.
    selected = [_card(f"Accepted Model for {scope}") for scope in scopes[:3]]
    candidates = context_packet.memory_decision_candidates(
        trigger, (), [], [], selected,
        SufficiencyVerdict("sufficient_for_reasoning", "ready", 0, 0, ()),
        synthesis_scope_models=hydrated,
    )
    synthesis = [item for item in candidates if item.candidate_kind == "synthesis"]
    assert conn.calls == 1
    assert receipt["returned_model_count"] == 3
    assert receipt["endpoint_model_versions"] == {
        str(model_id): str(version_id)
        for model_id, version_id in zip(delta_ids, delta_versions, strict=True)
    }
    assert len(synthesis) == 1
    assert synthesis[0].semantic_scope == ("Delta handoff",)
    assert synthesis[0].evidence_model_ids == tuple(map(str, delta_ids))
    assert synthesis[0].member_observation_ids == (conclusion_id,)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_conclusion_hydrates_nonempty_accepted_current_models(
) -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for the PostgreSQL hydration proof")
    tenant_id = uuid4()
    scope_label = "Orion delivery"
    scope_ref = "workstream:orion-delivery"
    model_ids = []
    conn = await asyncpg.connect(dsn)
    await register_vector(conn)
    transaction = conn.transaction()
    await transaction.start()
    try:
        await conn.execute(
            "INSERT INTO tenants (id,name) VALUES ($1,$2)",
            tenant_id,
            "scope-hydration-proof",
        )
        for index in range(3):
            observation_id = uuid4()
            await conn.execute(
                """
                INSERT INTO observations
                  (id,tenant_id,occurred_at,kind,source_channel,content,content_text,
                   embedding,embedding_pending,trust_tier)
                VALUES ($1,$2,now(),'signal','test','{}'::jsonb,$3,$4,FALSE,'authoritative')
                """,
                observation_id,
                tenant_id,
                f"Orion delivery accepted evidence {index}",
                [0.0] * 768,
            )
            row = await admit_validated_think_claim(
                conn,
                proposed=ModelCreate(
                    tenant_id=tenant_id,
                    born_from_event_id=observation_id,
                    proposition={
                        "kind": "state",
                        "subject": "Orion delivery",
                        "assertion": f"accepted state {index}",
                        "scope_label": scope_label,
                        "scope_ref": scope_ref,
                    },
                    natural=f"Orion delivery accepted state {index}",
                    embedding=[0.0] * 768,
                    scope_entities=[{
                        "type": "workstream",
                        "id": scope_ref,
                        "canonical_ref": scope_ref,
                        "display_label": scope_label,
                    }, *([{
                        "type": "workstream",
                        "id": "workstream:shared-platform",
                        "canonical_ref": "workstream:shared-platform",
                        "display_label": "Shared platform",
                    }] if index == 0 else [])],
                    scope_temporal={},
                    confidence=0.7,
                    confidence_at_assertion=0.7,
                    supporting_event_ids=[observation_id],
                ),
                evidence_observation_ids=(observation_id,),
                models_repo=ModelsRepo(None, embedder=None),
            )
            model_ids.append(row.id)

        conclusion_id, link_id, state_id = uuid4(), uuid4(), uuid4()
        signal_rows = (
            (link_id, "Orion delivery, update 4: A record links the open ownership handoff to delayed completion."),
            (state_id, "Orion delivery, update 4: Ownership remains pending while rollout completion is delayed."),
            (conclusion_id, "Orion delivery is blocked."),
        )
        for observation_id, text in signal_rows:
            await conn.execute(
                """
                INSERT INTO observations
                  (id,tenant_id,occurred_at,kind,source_channel,content,content_text,
                   embedding,embedding_pending,trust_tier)
                VALUES ($1,$2,now(),'signal','test','{}'::jsonb,$3,$4,FALSE,'authoritative')
                """,
                observation_id, tenant_id, text, [0.0] * 768,
            )
        trigger = TriggerContext(
            kind="T1",
            subkind="event_batch",
            tenant_id=tenant_id,
            observation_ids=[link_id, state_id, conclusion_id],
            seed_natural_text="Orion delivery batch",
            seed_signature={"batch_signal_fragments": [
                {"observation_id": str(link_id), "source_channel": "test",
                 "text": signal_rows[0][1],
                 "grounded_mentions": _grounded_scope(scope_label, scope_ref)},
                {"observation_id": str(state_id), "source_channel": "test",
                 "text": signal_rows[1][1],
                 "grounded_mentions": _grounded_scope(scope_label, scope_ref)},
                {"observation_id": str(conclusion_id), "source_channel": "test",
                 "text": "Orion delivery is blocked.",
                 "grounded_mentions": _grounded_scope(scope_label, scope_ref)},
            ]},
        )
        hydrated, receipt = await context_packet.hydrate_synthesis_scope_models(
            conn, trigger,
        )
        hydrated_model_ids = [UUID(value) for value in hydrated[scope_label]]
        candidates = context_packet.memory_decision_candidates(
            trigger, (), [], [], [],
            SufficiencyVerdict("sufficient_for_reasoning", "ready", 0, 0, ()),
            synthesis_scope_models=hydrated,
        )
        assert any(item.candidate_kind == "synthesis" for item in candidates), (
            hydrated, receipt, [
                (item.entailed_claim_text, item.semantic_scope) for item in candidates
            ]
        )
        packet = {
            "memory_decision_candidates": [asdict(item) for item in candidates],
            "signal_summary": "Orion delivery batch",
            "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
            "synthesis_scope_hydration": receipt,
        }
        request = build_compiled_batch_memory_decision_request(
            trigger, ContextBundle(notes={"inquiry_context_packet": packet}),
        )
        assert request is not None
        synthesis_candidate = next(
            item for item in request.candidates if item["candidate_kind"] == "synthesis"
        )
        compiled = request.to_raw_diff(
            BatchMemoryDecisionSet(decisions=[BatchMemoryCandidateDecision(
                candidate_id=synthesis_candidate["candidate_id"], decision="accept",
                operation="situation_and_edge", confidence=0.78,
                source_model_id=hydrated_model_ids[0],
                target_model_id=hydrated_model_ids[1],
                situation_member_model_ids=hydrated_model_ids,
                claim_text="Orion delivery is blocked by a persistent ownership gap.",
                reason="Ownership ambiguity repeatedly delays delivery completion.",
            )]),
            trigger=trigger,
            trigger_ref=uuid4(),
        )
        assert sum(
            (op.entry or {}).get("proposition", {}).get("claim_role") == "situation"
            for op in compiled.claim_ops
        ) == 1
        assert len(compiled.relation_claim_ops) == 1
        validated = ValidatedDiff(**compiled.model_dump())
        apply_result = await apply_diff(
            validated,
            conn,
            trigger_kind="T1:event_batch",
            trigger_cause_event_id=conclusion_id,
            models_repo=ModelsRepo(None, embedder=None),
        )
        synthesis_id = await conn.fetchval(
            """
            SELECT id FROM accepted_current_models
            WHERE tenant_id=$1 AND proposition->>'claim_role'='situation'
            ORDER BY created_at DESC LIMIT 1
            """,
            tenant_id,
        )
        assert synthesis_id in apply_result["applied_model_ids"]
        synthesis_evidence_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM model_truth_evidence_references evidence
            JOIN model_truth_heads head
              ON head.tenant_id=evidence.tenant_id
             AND head.version_id=evidence.model_version_id
            WHERE head.tenant_id=$1 AND head.model_id=$2
              AND evidence.evidence_kind='observation'
            """,
            tenant_id,
            synthesis_id,
        )
        accepted_relation_count = await conn.fetchval(
            "SELECT count(*) FROM accepted_current_relations WHERE tenant_id=$1",
            tenant_id,
        )
        relation_version_count = await conn.fetchval(
            "SELECT count(*) FROM relation_truth_versions WHERE tenant_id=$1",
            tenant_id,
        )
        projected_edge_count = await conn.fetchval(
            "SELECT count(*) FROM model_edges WHERE tenant_id=$1",
            tenant_id,
        )
        relation_evidence = await conn.fetchval(
            "SELECT evidence_event_ids FROM relation_claims WHERE tenant_id=$1",
            tenant_id,
        )
    finally:
        await transaction.rollback()
        await conn.close()

    assert set(hydrated[scope_label]) == {str(model_id) for model_id in model_ids[1:]}
    assert synthesis_evidence_count == 1
    assert accepted_relation_count == 1
    assert relation_version_count == 1
    assert projected_edge_count == 1
    assert set(relation_evidence) == {link_id, state_id}
    assert set(receipt.pop("endpoint_model_versions")) == {
        str(model_id) for model_id in model_ids[1:]
    }
    assert set(receipt.pop("endpoint_model_cards")) == {
        str(model_id) for model_id in model_ids[1:]
    }
    assert receipt == {
        "queried": True,
        "scope_count": 1,
        "limit_per_scope": 8,
        "returned_model_count": 2,
        "ambiguous_scope_count": 0,
        "scopes": [scope_label],
    }


@pytest.mark.asyncio
async def test_no_conclusion_performs_no_scope_memory_query() -> None:
    trigger = TriggerContext(
        kind="T1", subkind="event_batch", tenant_id=uuid4(),
        observation_ids=[uuid4()],
        seed_natural_text="ordinary update",
        seed_signature={"batch_signal_fragments": [{
            "observation_id": str(uuid4()), "source_channel": "test",
            "text": "Atlas release, update 3: Ownership remains unclear.",
        }]},
    )

    class Connection:
        async def fetch(self, *_args):
            raise AssertionError("no conclusion must not query accepted memory")

    hydrated, receipt = await context_packet.hydrate_synthesis_scope_models(
        Connection(), trigger
    )
    assert hydrated == {}
    assert receipt == {"queried": False, "reason": "no_scope_level_conclusion"}


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
