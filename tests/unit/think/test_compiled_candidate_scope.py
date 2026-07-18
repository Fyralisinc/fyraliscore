from __future__ import annotations

from uuid import uuid4

from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryCandidateDecision,
    BatchMemoryDecisionSet,
    _claim_op_from_batch_decision,
    build_compiled_batch_memory_decision_request,
)
from services.reasoning.think.applier import _prepare_claim_insert_model
from services.reasoning.think.diff_schema import ClaimOp
from services.reasoning.think.reconciler import (
    _business_scope_refs,
    _generic_model_scope_compatible,
)
from services.reasoning.think.evidence_manifest import (
    authorize_compiler_evidence_manifest,
)
from services.reasoning.think.splitter import split_compound_claim_op


def _compiled_claim(scope: str, *, role: str = "concern"):
    observation_id = uuid4()
    candidate = {
        "candidate_id": f"MDC_{scope}",
        "semantic_scope": [scope],
        "member_observation_ids": [str(observation_id)],
        "source_observation_ids": [str(observation_id)],
        "proposed_text": f"{scope} is materially blocked.",
        "target_model_ids": [str(uuid4()), str(uuid4())],
    }
    decision = BatchMemoryCandidateDecision(
        candidate_id=candidate["candidate_id"],
        decision="accept",
        operation="claim",
        confidence=0.7,
        claim_role=role,
        claim_text=candidate["proposed_text"],
        reason="Candidate-local evidence supports the claim.",
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        observation_id=observation_id,
        observation_ids=[observation_id],
        seed_natural_text="One physical mixed-workstream batch.",
    )
    op, _, error = _claim_op_from_batch_decision(candidate, decision, trigger)
    assert error == ""
    assert op is not None and op.entry is not None
    return op.entry


def test_compiled_atomic_claim_propagates_workstream_scope_and_subject() -> None:
    atlas = _compiled_claim("Atlas release")

    assert atlas["scope_entities"] == [
        {"type": "workstream", "id": "workstream:atlas-release"}
    ]
    assert atlas["proposition"]["about"] == "Atlas release"
    assert atlas["proposition"]["scope_label"] == "Atlas release"
    assert atlas["proposition"]["scope_ref"] == "workstream:atlas-release"
    assert atlas["proposition"]["about"] != "batch"


def test_compiled_composite_claim_inherits_workstream_scope() -> None:
    atlas = _compiled_claim("Atlas release", role="situation")

    assert atlas["scope_entities"] == [
        {"type": "workstream", "id": "workstream:atlas-release"}
    ]
    assert atlas["proposition"]["subject"] == "Atlas release"
    assert atlas["proposition"]["scope_ref"] == "workstream:atlas-release"


def test_compiled_renewal_scope_is_a_stable_commitment_coordinate() -> None:
    cobalt = _compiled_claim("Cobalt renewal")

    assert cobalt["scope_entities"] == [
        {"type": "commitment", "id": "commitment:cobalt-renewal"}
    ]
    assert cobalt["proposition"]["about"] == "Cobalt renewal"
    assert cobalt["proposition"]["scope_ref"] == "commitment:cobalt-renewal"


def test_compiled_workstreams_cannot_reconcile_across_scope() -> None:
    atlas = _compiled_claim("Atlas release")
    beacon = _compiled_claim("Beacon migration")
    for entry in (atlas, beacon):
        entry["domain_tags"] = ["source_digest"]

    assert _business_scope_refs(atlas) == {
        ("workstream", "workstream:atlas-release")
    }
    assert _business_scope_refs(beacon) == {
        ("workstream", "workstream:beacon-migration")
    }
    assert not _generic_model_scope_compatible(atlas, beacon)
    assert not _generic_model_scope_compatible(beacon, atlas)


def test_closed_atomic_candidate_ignores_provider_rewrite_and_keeps_one_ref() -> None:
    observation_id = uuid4()
    candidate = {
        "candidate_id": "MDC_ATOM_atlas",
        "proposed_text": "Atlas release: The certificate has no recorded owner.",
        "entailed_claim_text": (
            "Atlas release: The certificate has no recorded owner."
        ),
        "semantic_scope": ["Atlas release"],
        "source_observation_ids": [str(observation_id)],
        "member_observation_ids": [str(observation_id)],
        "observation_evidence": [
            {
                "observation_id": str(observation_id),
                "body": (
                    "Atlas release, update 1: The certificate has no recorded owner."
                ),
                "source_channel": "slack:message",
            }
        ],
    }
    decision = BatchMemoryCandidateDecision(
        candidate_id=candidate["candidate_id"],
        decision="accept",
        operation="claim",
        confidence=0.7,
        claim_role="concern",
        claim_text="Atlas release has unseen schedule churn and delay.",
        reason="Attempted broad rewrite.",
    )
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=uuid4(),
        observation_id=observation_id,
        observation_ids=[observation_id],
        seed_natural_text="One batch.",
    )

    op, _, error = _claim_op_from_batch_decision(
        candidate, decision, trigger, force_role="fact"
    )

    assert error == ""
    assert op is not None and op.entry is not None
    assert op.entry["natural"] == candidate["entailed_claim_text"]
    assert op.entry["supporting_event_ids"] == [str(observation_id)]
    assert op.entry["proposition"]["claim_role"] == "fact"
    assert "churn" not in op.entry["natural"]
    assert "delay" not in op.entry["natural"]


def test_closed_atomic_batch_is_not_truncated_to_six_candidates() -> None:
    observations = [uuid4() for _ in range(20)]

    def fact_text(index: int) -> str:
        if index == 0:
            return (
                "Atlas release, update 1: The release certificate still has "
                "no clearly recorded owner."
            )
        return f"Atlas release, update 1: exact operational fact {index}."

    candidates = [
        {
            "candidate_id": f"MDC_ATOM_{index}",
            "op_family": "claim_insert",
            "proposed_text": fact_text(index),
            "entailed_claim_text": fact_text(index),
            "semantic_scope": ["Atlas release"],
            "source_observation_ids": [str(observation_id)],
            "member_observation_ids": [str(observation_id)],
            "observation_evidence": [
                {
                    "observation_id": str(observation_id),
                    "body": fact_text(index),
                    "source_channel": "slack:message",
                }
            ],
        }
        for index, observation_id in enumerate(observations)
    ]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=uuid4(),
        observation_id=observations[0],
        observation_ids=[*observations, *(uuid4() for _ in range(5))],
        seed_natural_text="One physical 25-signal batch.",
    )
    bundle = ContextBundle(
        notes={
            "inquiry_context_packet": {
                "signal_summary": "Twenty exact business facts and five noise signals.",
                "memory_decision_candidates": candidates,
            }
        }
    )

    request = build_compiled_batch_memory_decision_request(trigger, bundle)

    assert request is not None
    assert len(request.candidates) == 20
    assert "immutable claim wording" in request.system
    decision = BatchMemoryCandidateDecision(
        candidate_id="MDC_ATOM_0",
        decision="accept",
        operation="claim",
        confidence=0.7,
        claim_role="concern",
        claim_text="Atlas release has unseen churn and delay.",
        reason="Attempted rewrite.",
    )
    diff = request.to_raw_diff(
        BatchMemoryDecisionSet(decisions=[decision]),
        trigger=trigger,
        trigger_ref=uuid4(),
    )
    assert len(diff.claim_ops) == 1
    assert diff.claim_ops[0].entry is not None
    assert (
        diff.claim_ops[0].entry["natural"]
        == fact_text(0)
    )
    assert diff.claim_ops[0].entry["proposition"]["claim_role"] == "fact"
    split_ops = split_compound_claim_op(diff.claim_ops[0])
    assert len(split_ops) == 1
    split_entry = split_ops[0].entry
    assert split_entry is not None
    selected = split_entry["supporting_event_ids"]
    assert selected == [str(observations[0])]
    authorize_compiler_evidence_manifest(
        selected_observation_ids=[observations[0]],
        manifest=split_entry["evidence_observation_manifest"],
        persisted_observations=[
            {
                "id": observations[0],
                "content_text": fact_text(0),
            }
        ],
    )


def test_compiler_evidence_manifest_is_consumed_before_model_create() -> None:
    entry = _compiled_claim("Atlas release")
    observation_id = entry["supporting_event_ids"][0]
    entry["evidence_observation_manifest"] = [
        {
            "observation_id": observation_id,
            "body": "Atlas release still has no clearly recorded owner.",
            "source_channel": "slack:message",
        }
    ]

    model = _prepare_claim_insert_model(
        ClaimOp(op="insert", entry=entry),
        entry["tenant_id"],
        cause_event_id=entry["born_from_event_id"],
        trigger_supporting_event_ids=[],
    )

    assert [str(value) for value in model.supporting_event_ids] == [observation_id]
    assert "evidence_observation_manifest" not in model.model_dump()
