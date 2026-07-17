"""Integrity checks for inspected boundary/type development corpus v1."""

from collections import Counter

from tests.evaluation.learned_entity_discovery_boundary_type_development_v1 import (
    DEVELOPMENT_CORPUS,
    DEVELOPMENT_ONLY,
    EVIDENCE_CLASS,
    VERSION,
)


def test_boundary_type_v1_is_explicitly_development_only_and_batched() -> None:
    assert DEVELOPMENT_ONLY is True
    assert EVIDENCE_CLASS == "development_feedback_only_not_generalization_evidence"
    assert VERSION == "boundary-type-development-v1"
    assert Counter(row["batch_id"] for row in DEVELOPMENT_CORPUS) == {
        "boundary-type-v1-batch-1": 10,
        "boundary-type-v1-batch-2": 10,
    }
    assert {row["source_type"] for row in DEVELOPMENT_CORPUS} == {
        "slack", "email", "jira",
    }


def test_boundary_type_v1_gold_is_exact_and_focuses_workstream_and_codes() -> None:
    mentions = [mention for row in DEVELOPMENT_CORPUS for mention in row["gold"]]
    for row in DEVELOPMENT_CORPUS:
        for mention in row["gold"]:
            assert row["text"][mention["start"]:mention["end"]] == mention["surface"]
            assert mention["canonical_referent"] is None
    workstreams = [item for item in mentions if item["entity_type"] == "workstream"]
    ambiguous = [item for item in mentions if item["entity_type"] == "other"]
    assert len(workstreams) >= 9
    assert sum(item["surface"].endswith(" workstream") for item in workstreams) >= 2
    assert {item["surface"] for item in ambiguous} == {"RUNE-310", "AX-19"}
    negative_text = "\n".join(row["text"] for row in DEVELOPMENT_CORPUS if not row["gold"])
    assert "generic planning text" in negative_text
    assert "transport coordinates" in negative_text
