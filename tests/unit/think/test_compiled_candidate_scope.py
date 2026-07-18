from __future__ import annotations

from uuid import uuid4

from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryCandidateDecision,
    _claim_op_from_batch_decision,
)
from services.reasoning.think.reconciler import (
    _business_scope_refs,
    _generic_model_scope_compatible,
)


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
    atlas = _compiled_claim("Atlas")

    assert atlas["scope_entities"] == [{"type": "workstream", "id": "Atlas"}]
    assert atlas["proposition"]["about"] == "Atlas"
    assert atlas["proposition"]["about"] != "batch"


def test_compiled_composite_claim_inherits_workstream_scope() -> None:
    atlas = _compiled_claim("Atlas", role="situation")

    assert atlas["scope_entities"] == [{"type": "workstream", "id": "Atlas"}]
    assert atlas["proposition"]["subject"] == "Atlas"


def test_compiled_workstreams_cannot_reconcile_across_scope() -> None:
    atlas = _compiled_claim("Atlas")
    beacon = _compiled_claim("Beacon")
    for entry in (atlas, beacon):
        entry["domain_tags"] = ["source_digest"]

    assert _business_scope_refs(atlas) == {("workstream", "Atlas")}
    assert _business_scope_refs(beacon) == {("workstream", "Beacon")}
    assert not _generic_model_scope_compatible(atlas, beacon)
    assert not _generic_model_scope_compatible(beacon, atlas)
