from __future__ import annotations

from uuid import uuid4

from services.platform.execution import inquiry, question_text
from services.reasoning.retrieval.primary import TriggerContext


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


def test_question_text_helpers_keep_legacy_inquiry_identity() -> None:
    assert inquiry._QuestionAnchors is question_text.QuestionAnchors
    assert inquiry._capitalized_anchor_spans is question_text.capitalized_anchor_spans
    assert inquiry._claim_from_text is question_text.claim_from_text
    assert inquiry._clean_question_anchor is question_text.clean_question_anchor
    assert (
        inquiry._clean_question_focus_phrase
        is question_text.clean_question_focus_phrase
    )
    assert inquiry._counterevidence_focus is question_text.counterevidence_focus
    assert inquiry._domain_keyword_focus is question_text.domain_keyword_focus
    assert inquiry._entity_label_from_seed is question_text.entity_label_from_seed
    assert (
        inquiry._fallback_focus_from_delta_claim
        is question_text.fallback_focus_from_delta_claim
    )
    assert inquiry._focus_from_preface is question_text.focus_from_preface
    assert inquiry._focus_sentence_score is question_text.focus_sentence_score
    assert inquiry._focus_sentences is question_text.focus_sentences
    assert inquiry._is_specific_focus_phrase is question_text.is_specific_focus_phrase
    assert (
        inquiry._looks_like_company_overview
        is question_text.looks_like_company_overview
    )
    assert (
        inquiry._looks_like_machine_identifier
        is question_text.looks_like_machine_identifier
    )
    assert inquiry._question_anchors is question_text.question_anchors
    assert (
        inquiry._question_constraint_phrase is question_text.question_constraint_phrase
    )
    assert inquiry._question_entity_labels is question_text.question_entity_labels
    assert inquiry._question_focus_phrase is question_text.question_focus_phrase
    assert inquiry._question_subject is question_text.question_subject
    assert inquiry._safe_question_focus is question_text.safe_question_focus
    assert inquiry._specific_question is question_text.specific_question
    assert inquiry._truncate_text is question_text.truncate_text


def test_question_anchors_prefer_entity_labels_and_constraint_focus() -> None:
    trigger = _trigger(
        "AcmeAtlas launch is blocked by security review capacity and SSO is at risk.",
        seed_entity_ids=[
            {"type": "customer", "id": "019ea807-48e2-7000-b96f-b0a86bf8256f"},
            {"type": "customer", "label": "AcmeAtlas"},
            {"type": "system", "name": "Enterprise SSO"},
        ],
    )

    anchors = question_text.question_anchors(trigger)

    assert anchors.subject == "AcmeAtlas, Enterprise SSO"
    assert anchors.constraint == "security review capacity"
    assert "blocked by security review capacity" in anchors.focus
    assert not question_text.looks_like_machine_identifier(anchors.subject)


def test_specific_questions_use_signal_anchors_without_uuid_leakage() -> None:
    trigger = _trigger(
        "Customer launch is waiting on legal approval.",
        seed_entity_ids=[
            {"type": "customer", "id": "019ea807-48e2-7000-b96f-b0a86bf8256f"}
        ],
    )
    anchors = question_text.question_anchors(trigger)

    owner = question_text.specific_question("OWNERSHIP", anchors)
    dependency = question_text.specific_question("DEPENDENCY", anchors)

    assert "019ea807" not in owner + dependency
    assert "legal approval" in owner
    assert "legal approval" in dependency
    assert owner.endswith("?")
    assert dependency.endswith("?")


def test_focus_phrase_cleanup_rejects_generic_slots_but_keeps_domain_terms() -> None:
    assert (
        question_text.clean_question_focus_phrase("whether blocker status")
        == "blocker status"
    )
    assert not question_text.is_specific_focus_phrase("blocker status")
    assert question_text.is_specific_focus_phrase("SOC2 audit trail blocker")
    assert (
        question_text.domain_keyword_focus(
            "The new SOC2 audit trail blocker delayed procurement approval."
        )
        == "audit trail blocker delayed procurement approval"
    )


def test_claim_and_truncate_text_are_stable_and_compact() -> None:
    assert question_text.claim_from_text("", fallback="fallback") == "fallback"
    long_text = " ".join(["launch"] * 80)

    assert question_text.claim_from_text(long_text, fallback="fallback").endswith("...")
    assert len(question_text.truncate_text(long_text, 40)) <= 40
