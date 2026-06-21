from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from services.platform.execution import inquiry, reconstruction_state
from services.platform.execution.reconstruction_state import (
    apply_reconstruction_to_actions,
    build_reconstruction_state,
    evidence_state_for_reader,
    planner_reconstruction_payload,
    reconstruction_gate_decision,
    reconstruction_state_for_purpose,
    reconstruction_state_payload,
    serialized_payload_size,
)
from services.platform.execution.types import (
    EvidenceCard,
    Hypothesis,
    QuestionAnswer,
    RetrievalAction,
)
from services.reasoning.retrieval.primary import TriggerContext


def _trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
        seed_entity_ids=[{"type": "customer", "label": "HarborRail"}],
        seed_natural_text="HarborRail launch is blocked by audit evidence.",
    )


def _card(
    *,
    source_type: str,
    summary: str,
    supports: set[str] | None = None,
    weakens: set[str] | None = None,
) -> EvidenceCard:
    return EvidenceCard(
        evidence_id=uuid4(),
        source_type=source_type,
        source_ref=f"{source_type}:{uuid4()}",
        source_ref_id=uuid4(),
        summary=summary,
        trust_tier="authoritative",
        timestamp=datetime(2026, 6, 17, tzinfo=timezone.utc),
        retrieval_paths={"semantic"},
        retrieved_for_questions={"Q_COUNTEREVIDENCE"},
        supports_hypotheses=supports or set(),
        weakens_hypotheses=weakens or set(),
        raw_content_ref=f"{source_type}:raw",
        score=0.8,
    )


def test_inquiry_private_aliases_point_to_reconstruction_module() -> None:
    assert (
        inquiry._build_reconstruction_state
        is reconstruction_state.build_reconstruction_state
    )
    assert (
        inquiry._apply_reconstruction_to_actions
        is reconstruction_state.apply_reconstruction_to_actions
    )
    assert (
        inquiry._evidence_state_for_reader
        is reconstruction_state.evidence_state_for_reader
    )
    assert (
        inquiry._reconstruction_gate_decision
        is reconstruction_state.reconstruction_gate_decision
    )
    assert (
        inquiry._planner_reconstruction_payload
        is reconstruction_state.planner_reconstruction_payload
    )


def test_build_reconstruction_state_compacts_frontier_for_next_read() -> None:
    support = _card(
        source_type="model",
        summary="HarborRail launch blocker depends on procurement audit evidence.",
        supports={"H1"},
    )
    counter = _card(
        source_type="observation",
        summary="A Slack reply says the premise is incomplete and owner is unclear.",
        weakens={"H1"},
    )

    state = build_reconstruction_state(
        trigger=_trigger(),
        hypotheses=(
            Hypothesis(
                id="H1",
                claim="HarborRail launch is blocked",
                confidence=0.8,
                impact_if_true="delay",
                evidence_needed=("owner thread",),
            ),
        ),
        evidence=[support, counter],
        answers=[
            QuestionAnswer(
                question_id="Q_COUNTEREVIDENCE",
                answer_status="inconclusive",
                summary="Owner remains unresolved.",
            )
        ],
        unknowns={"responsible owner", "counterevidence"},
        round_index=2,
    )

    assert state.round_index == 2
    assert "HarborRail" in state.active_cues
    assert "responsible owner" in state.unresolved_slots
    assert state.known_model_ids == (str(support.source_ref_id),)
    assert state.known_observation_ids == (str(counter.source_ref_id),)
    assert state.hypothesis_status["H1"]["support"] == 1
    assert state.hypothesis_status["H1"]["weakens"] == 1
    assert "semantic:counterevidence" in state.operator_bias
    assert "structural:ownership_graph" in state.operator_bias

    reader_state = evidence_state_for_reader(state)
    assert reader_state is not None
    assert reader_state["known_model_ids"] == [str(support.source_ref_id)]
    assert reader_state["reconstruction_state"]["round_index"] == 2
    assert reader_state["payload_kind"] == "reader_compact"

    planner_state = planner_reconstruction_payload(state)
    assert planner_state["round_index"] == 2
    assert planner_state["known_model_count"] == 1
    assert serialized_payload_size(planner_state) < serialized_payload_size(
        reconstruction_state_payload(state)
    )


def test_reconstruction_gate_skips_low_value_first_round_planner_and_actions() -> None:
    state = build_reconstruction_state(
        trigger=_trigger(),
        hypotheses=(),
        evidence=[],
        answers=[],
        unknowns={"counterevidence"},
        round_index=1,
    )

    gates = reconstruction_gate_decision(state, trigger=_trigger())

    assert gates["planner"]["enabled"] is False
    assert gates["actions"]["enabled"] is False
    assert gates["actions"]["reason"] == "first_round_actions_stay_parallel"
    assert reconstruction_state_for_purpose(
        state,
        trigger=_trigger(),
        purpose="planner",
    ) is None
    assert reconstruction_state_for_purpose(
        state,
        trigger=_trigger(),
        purpose="actions",
    ) is None


def test_reconstruction_gate_enables_reader_when_prior_scope_exists() -> None:
    state = build_reconstruction_state(
        trigger=_trigger(),
        hypotheses=(),
        evidence=[
            _card(
                source_type="model",
                summary="HarborRail launch blocker depends on procurement audit evidence.",
            )
        ],
        answers=[],
        unknowns={"counterevidence"},
        round_index=1,
    )

    gates = reconstruction_gate_decision(state, trigger=_trigger())

    assert gates["reader"]["enabled"] is True
    assert reconstruction_state_for_purpose(
        state,
        trigger=_trigger(),
        purpose="reader",
    ) is state


def test_apply_reconstruction_to_actions_stages_and_binds_later_round_actions() -> None:
    state = build_reconstruction_state(
        trigger=_trigger(),
        hypotheses=(),
        evidence=[
            _card(
                source_type="model",
                summary="Audit evidence procurement owner blocker",
                supports={"H1"},
            )
        ],
        answers=[],
        unknowns={"responsible owner"},
        round_index=2,
    )
    actions = [
        RetrievalAction(
            "Q1", "focused_index", "answerability", filters={"terms": ["audit"]}
        ),
        RetrievalAction("Q1", "semantic", "owner_evidence", query="owner lookup"),
    ]

    staged = apply_reconstruction_to_actions(actions, state=state)

    assert staged[0].filters["_reconstruction_stage"] == 1
    assert staged[0].filters["_reconstruction_cue_count"] <= 4
    assert "responsible owner" in staged[0].filters["terms"]
    assert staged[1].filters["_reconstruction_stage"] == 2
    assert staged[1].filters["_bind_previous_scope"] is True
    assert "responsible owner" in staged[1].query


def test_apply_reconstruction_to_actions_caps_terms_and_ignores_generic_cues() -> None:
    state = build_reconstruction_state(
        trigger=_trigger(),
        hypotheses=(),
        evidence=[
            _card(
                source_type="model",
                summary=(
                    "risk owner dependency evidence constraint impact "
                    "HarborRail audit procurement launch"
                ),
            )
        ],
        answers=[],
        unknowns={"risk", "owner", "counterevidence"},
        round_index=2,
    )
    action = RetrievalAction(
        "Q1",
        "focused_index",
        "answerability",
        filters={"terms": ["existing", "term", "slots", "kept"]},
    )

    staged = apply_reconstruction_to_actions([action], state=state)

    assert len(staged[0].filters["terms"]) <= 8
    assert "risk" not in staged[0].filters["_reconstruction_active_cues"]
    assert "owner" not in staged[0].filters["_reconstruction_active_cues"]


def test_apply_reconstruction_to_actions_leaves_first_round_parallel() -> None:
    state = build_reconstruction_state(
        trigger=_trigger(),
        hypotheses=(),
        evidence=[],
        answers=[],
        unknowns={"counterevidence"},
        round_index=1,
    )
    actions = [RetrievalAction("Q1", "semantic", "counter", query="counter")]

    assert apply_reconstruction_to_actions(actions, state=state) == actions
