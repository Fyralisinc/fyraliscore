from __future__ import annotations

from services.reasoning.edge_intelligence.relation_extractor import (
    extract_relation_evidence,
)


def test_extracts_forward_block_relation() -> None:
    relations = extract_relation_evidence(
        "DPA approval blocks HubSpot import. Priya owns the legal review."
    )

    assert len(relations) == 1
    assert relations[0].subject_text == "DPA approval"
    assert relations[0].predicate == "blocks"
    assert relations[0].object_text == "HubSpot import"
    assert relations[0].edge_kind_hint == "blocks"


def test_extracts_blocked_by_relation_with_correct_direction() -> None:
    relations = extract_relation_evidence("HubSpot import is blocked by DPA approval.")

    assert len(relations) == 1
    assert relations[0].subject_text == "DPA approval"
    assert relations[0].object_text == "HubSpot import"
    assert relations[0].predicate == "blocks"


def test_extracts_multiple_relation_kinds_without_duplicates() -> None:
    relations = extract_relation_evidence(
        "Capacity gap weakens launch confidence. Capacity gap weakens launch confidence. "
        "SOC2 approval enables enterprise rollout."
    )

    assert [(r.predicate, r.subject_text, r.object_text) for r in relations] == [
        ("enables", "SOC2 approval", "enterprise rollout"),
        ("weakens", "Capacity gap", "launch confidence"),
    ]


def test_ignores_empty_and_tiny_fragments() -> None:
    assert extract_relation_evidence("") == []
    assert extract_relation_evidence("A blocks B.") == []
