from __future__ import annotations

from uuid import uuid4

from services.reasoning.think.applier import _unsupported_claim_receipt
from services.reasoning.think.diff_schema import ClaimOp


def test_unsupported_claim_receipt_is_stable_and_inspectable() -> None:
    evidence_id = uuid4()
    entity_id = uuid4()
    op = ClaimOp(op="insert", entry={
        "natural": "Atlas release ownership is unresolved.",
        "proposition": {
            "kind": "belief", "claim_role": "concern",
            "evidence_event_ids": [str(evidence_id)],
        },
        "scope_entities": [{"type": "workstream", "id": str(entity_id)}],
    })

    first = _unsupported_claim_receipt(op)
    second = _unsupported_claim_receipt(op)

    assert first == second
    assert len(first["payload_digest"]) == 64
    assert first["proposed_evidence_ids"] == [str(evidence_id)]
    assert first["proposed_scope"] == {
        "actors": [], "entities": [("workstream", str(entity_id))],
    }


def test_unsupported_claim_receipt_distinguishes_claim_payloads() -> None:
    base = ClaimOp(op="insert", entry={
        "natural": "Atlas release ownership is unresolved.",
        "proposition": {"kind": "belief"},
    })
    changed = ClaimOp(op="insert", entry={
        "natural": "Beacon migration access review is unresolved.",
        "proposition": {"kind": "belief"},
    })
    assert (
        _unsupported_claim_receipt(base)["payload_digest"]
        != _unsupported_claim_receipt(changed)["payload_digest"]
    )
