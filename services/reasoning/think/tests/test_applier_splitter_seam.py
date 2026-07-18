from __future__ import annotations

from uuid import uuid4

from services.reasoning.think.applier import _expand_claim_ops_for_splitter
from services.reasoning.think.diff_schema import ClaimOp


def _manifest_op(natural: str, bodies: list[str]) -> tuple[ClaimOp, list[str]]:
    observation_ids = [str(uuid4()) for _ in bodies]
    return (
        ClaimOp(
            op="insert",
            entry={
                "born_from_event_id": str(uuid4()),
                "natural": natural,
                "proposition": {
                    "kind": "belief",
                    "claim_role": "fact",
                    "assertion": natural,
                    "evidence_event_ids": observation_ids,
                },
                "supporting_event_ids": observation_ids,
                "evidence_observation_manifest": [
                    {
                        "observation_id": observation_id,
                        "body": body,
                        "source_channel": "slack:message",
                    }
                    for observation_id, body in zip(
                        observation_ids, bodies, strict=True
                    )
                ],
            },
        ),
        observation_ids,
    )


def test_applier_seam_replaces_unsplit_delta_atomic_with_filtered_result():
    op, observation_ids = _manifest_op(
        "Delta handoff ownership remains unresolved.",
        [
            "Delta handoff: The support owner has no clearly recorded owner.",
            "Delta handoff: A reply asks whether the handoff happened.",
            "Delta handoff: The checklist record remains incomplete.",
            "Delta handoff: The incident rate moved again.",
        ],
    )

    expanded, summary = _expand_claim_ops_for_splitter(
        [op], trigger_cause_event_id=None, trigger_evidence_ids=[]
    )

    assert len(expanded) == 1
    source, filtered, group_id = expanded[0]
    assert source is not filtered
    assert group_id is None
    assert filtered.entry["supporting_event_ids"] == [observation_ids[0]]
    assert summary == {
        "compound_inputs": 0,
        "atomic_outputs": 0,
        "synthesized_situations": 0,
    }


def test_applier_seam_drops_unsupported_unsplit_atomic():
    op, _ = _manifest_op(
        "Delta executive sentiment is worsening.",
        ["Delta handoff: The checklist record remains incomplete."],
    )

    expanded, summary = _expand_claim_ops_for_splitter(
        [op], trigger_cause_event_id=None, trigger_evidence_ids=[]
    )

    assert expanded == []
    assert summary["atomic_outputs"] == 0
    assert summary["synthesized_situations"] == 0


def test_split_telemetry_classifies_outputs_by_proposition_role():
    natural = "The owner is missing and the deadline is delayed."
    op = ClaimOp(
        op="insert",
        entry={
            "born_from_event_id": str(uuid4()),
            "natural": natural,
            "proposition": {"kind": "state", "assertion": natural},
        },
    )

    expanded, summary = _expand_claim_ops_for_splitter(
        [op], trigger_cause_event_id=None, trigger_evidence_ids=[]
    )

    assert len(expanded) == 2
    assert summary == {
        "compound_inputs": 1,
        "atomic_outputs": 2,
        "synthesized_situations": 0,
    }
