from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

from lib.evaluation.source_equivalence import evaluate_normalized_source_equivalence
from lib.contracts.perception import MentionAnchorKind
from services.domain.source_semantics.extractor import DeterministicSourceSemanticExtractor


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
    extractor = DeterministicSourceSemanticExtractor()
    tenant_id = uuid4()
    for case_id, state in (
        ("launch-blocked", "blocked"),
        ("launch-ready", "ready"),
    ):
        for source in boundaries:
            observation_id = uuid4()
            text = f"Atlas is {state}."
            observation_ref = f"observation:{observation_id}"
            grounding = SimpleNamespace(
                tenant_id=tenant_id,
                trace_id=uuid4(),
                source_observation_id=observation_id,
                content_text=text,
                source_channel=f"{source}:normalized",
                source_author_ref=authorities[source],
                context_snapshot_id=uuid4(),
                mention=SimpleNamespace(
                    detection_confidence=0.95,
                    primary_anchor=SimpleNamespace(
                        anchor_id=f"mention-anchor:{observation_id}",
                        kind=MentionAnchorKind.EXPLICIT,
                        surface_form="Atlas",
                        coordinate=SimpleNamespace(
                            evidence_record_id=observation_ref,
                            source_object_id=observation_ref,
                            field_path="content_text",
                            span_start=0,
                            span_end=5,
                        ),
                    ),
                ),
            )
            semantic = extractor.extract(grounding)
            predicate = semantic.semantic_frame.predicate_or_event_type
            assertion_source = semantic.source_assertion.coordinates[0].source_system
            rows.append({
                "semantic_case_id": case_id, "source_kind": source,
                "batch_signal_count": 3,
                "entity_lineage_complete": True,
                "model_lineage_complete": True,
                "relation_lineage_complete": True,
                "entity_refs": ["project:atlas", "goal:launch"],
                "model_signatures": [f"belief:project:atlas:{predicate}"],
                "relation_signatures": [f"project:atlas|state|{predicate}"],
                "authority_ref": semantic.source_assertion.current_speaker_or_author,
                "expected_authority_ref": authorities[source],
                "assertion_source_system": assertion_source,
                "expected_source_system": source,
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
