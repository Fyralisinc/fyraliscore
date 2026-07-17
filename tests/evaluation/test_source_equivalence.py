from copy import deepcopy

from lib.evaluation.source_equivalence import evaluate_normalized_source_equivalence


def _rows():
    rows = []
    boundaries = {
        "slack": ["channel:eng", "thread:launch-42", "message:17"],
        "email": ["mailbox:ops", "thread:launch", "message:m-17"],
        "jira": ["project:ENG", "issue:ENG-42", "comment:17"],
        "document_meeting": ["meeting:weekly-7", "transcript:segment-17"],
    }
    authorities = {
        "slack": "slack:user:alice", "email": "email:alice@example.test",
        "jira": "jira:account:alice", "document_meeting": "meeting:speaker:alice",
    }
    for case_id, models, relations in (
        ("launch-blocked", ["belief:atlas:blocked"], ["atlas|blocks|launch"]),
        ("launch-ready", ["belief:atlas:ready"], ["approval|enables|launch"]),
    ):
        for source in boundaries:
            rows.append({
                "semantic_case_id": case_id, "source_kind": source,
                "batch_signal_count": 3,
                "entity_lineage_complete": True,
                "model_lineage_complete": True,
                "relation_lineage_complete": True,
                "entity_refs": ["project:atlas", "goal:launch"],
                "model_signatures": models, "relation_signatures": relations,
                "authority_ref": authorities[source],
                "expected_authority_ref": authorities[source],
                "assertion_source_system": source.split("_", 1)[0],
                "expected_source_system": source.split("_", 1)[0],
                "boundary_refs": boundaries[source],
                "expected_boundary_refs": boundaries[source],
            })
    return rows


def test_equivalent_persisted_source_batches_preserve_semantics_and_provenance():
    report = evaluate_normalized_source_equivalence(_rows())

    assert report["verdict"] == "meets_policy"
    assert report["population"] == {"cases": 2, "source_batches": 8}
    assert report["continuous_score"] == 1.0
    assert all(value == 1.0 for value in report["measurements"].values())
    assert all(report["checks"].values())


def test_semantic_collapse_and_boundary_loss_are_measured_separately():
    rows = deepcopy(_rows())
    jira = next(row for row in rows if row["source_kind"] == "jira")
    jira["model_signatures"] = ["belief:atlas:delayed"]
    jira["boundary_refs"] = ["issue:ENG-42"]

    report = evaluate_normalized_source_equivalence(rows)

    assert report["verdict"] == "below_policy"
    assert report["measurements"]["model_outcome_similarity"] < 1.0
    assert report["measurements"]["conversational_boundary_fidelity"] < 1.0
    assert report["measurements"]["source_authority_fidelity"] == 1.0
    assert 0.0 < report["continuous_score"] < 1.0
