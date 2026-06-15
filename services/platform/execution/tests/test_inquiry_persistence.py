from __future__ import annotations

from uuid import uuid4

from services.platform.execution import inquiry, inquiry_persistence
from services.platform.execution.types import EvidenceCard


def _card(
    *,
    source_type: str = "observation",
    supports: set[str] | None = None,
    questions: set[str] | None = None,
    score: float = 0.5,
) -> EvidenceCard:
    card = EvidenceCard(
        evidence_id=uuid4(),
        source_type=source_type,
        source_ref=f"{source_type}:ref",
        source_ref_id=uuid4(),
        summary="evidence summary",
        trust_tier="model" if source_type == "model" else "authoritative",
        timestamp=None,
        score=score,
    )
    card.supports_hypotheses.update(supports or set())
    card.retrieved_for_questions.update(questions or set())
    return card


def test_inquiry_private_aliases_point_to_inquiry_persistence_module() -> None:
    assert inquiry._persist_inquiry is inquiry_persistence._persist_inquiry
    assert inquiry._emit_phase1_traces is inquiry_persistence._emit_phase1_traces
    assert (
        inquiry._packet_evidence_refs_by_question
        is inquiry_persistence._packet_evidence_refs_by_question
    )
    assert (
        inquiry._reader_attribution_nonselected_limit
        is inquiry_persistence._reader_attribution_nonselected_limit
    )


def test_packet_evidence_refs_are_grouped_by_question() -> None:
    first = _card(questions={"Q1", "Q2"}, score=0.7)
    second = _card(source_type="model", questions={"Q2"}, score=0.2)

    refs = inquiry_persistence._packet_evidence_refs_by_question((first, second))

    assert [ref["evidence_id"] for ref in refs["Q1"]] == [str(first.evidence_id)]
    assert {ref["source_type"] for ref in refs["Q2"]} == {"observation", "model"}
    assert refs["Q2"][0]["score"] == 0.7


def test_classify_omission_reason_prefers_specific_reasons() -> None:
    model_noise = _card(source_type="model")
    relevant = _card(supports={"H1"})

    assert (
        inquiry_persistence._classify_omission_reason(
            model_noise,
            packet_budget_cap=1000,
            packet_budget_used=10,
        )
        == "generic_hub"
    )
    assert (
        inquiry_persistence._classify_omission_reason(
            relevant,
            packet_budget_cap=1000,
            packet_budget_used=960,
        )
        == "budget_exhausted"
    )
    assert (
        inquiry_persistence._classify_omission_reason(
            relevant,
            packet_budget_cap=1000,
            packet_budget_used=100,
        )
        == "redundant"
    )
