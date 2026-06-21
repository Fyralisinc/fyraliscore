from __future__ import annotations

from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.diff_schema import ClaimOp, RawDiff
from services.reasoning.think.run_pipeline import _drop_event_batch_wrapper_claims


def test_drop_event_batch_wrapper_claims_drops_batch_subject_insert():
    tenant_id = uuid7()
    obs_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=obs_id,
        member_trigger_ids=[uuid7()],
    )
    diff = RawDiff(
        trigger_ref=obs_id,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(obs_id),
                    "proposition": {
                        "kind": "belief",
                        "summary": "The batch combines unrelated GitHub activity.",
                    },
                    "natural": "The batch combines unrelated GitHub activity.",
                    "confidence": 0.55,
                },
            ),
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(obs_id),
                    "proposition": {
                        "kind": "belief",
                        "summary": "Checkpoint explorer incident response has an active owner.",
                    },
                    "natural": "Checkpoint explorer incident response has an active owner.",
                    "confidence": 0.66,
                },
            ),
        ],
    )

    out = _drop_event_batch_wrapper_claims(diff, trigger)

    assert len(out.claim_ops) == 1
    assert out.claim_ops[0].entry["natural"].startswith("Checkpoint explorer")
    assert out.reasoning_trace == "dropped 1 T1:event_batch wrapper claim(s)"


def test_drop_event_batch_wrapper_claims_drops_batch_level_insert():
    tenant_id = uuid7()
    obs_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=obs_id,
        member_trigger_ids=[uuid7()],
    )
    diff = RawDiff(
        trigger_ref=obs_id,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(obs_id),
                    "proposition": {
                        "kind": "belief",
                        "summary": "Batch-level planner hypothesis H1 with support.",
                    },
                    "natural": "Batch-level planner hypothesis H1 with support.",
                    "confidence": 0.55,
                },
            )
        ],
    )

    out = _drop_event_batch_wrapper_claims(diff, trigger)

    assert out.claim_ops == []
    assert out.reasoning_trace == "dropped 1 T1:event_batch wrapper claim(s)"


def test_drop_event_batch_wrapper_claims_drops_mid_sentence_batch_wrapper():
    tenant_id = uuid7()
    obs_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=obs_id,
        member_trigger_ids=[uuid7()],
    )
    diff = RawDiff(
        trigger_ref=obs_id,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(obs_id),
                    "proposition": {
                        "kind": "belief",
                        "summary": "Checkpoint response is moving, but the batch also preserves ambiguity.",
                    },
                    "natural": "Checkpoint response is moving, but the batch also preserves ambiguity.",
                    "confidence": 0.55,
                },
            )
        ],
    )

    out = _drop_event_batch_wrapper_claims(diff, trigger)

    assert out.claim_ops == []
    assert out.reasoning_trace == "dropped 1 T1:event_batch wrapper claim(s)"
