from __future__ import annotations

from uuid import uuid4

from services.think.diff_schema import ClaimOp, EdgeOp, RawDiff
from services.think.validator import _iter_entity_ids_touched


def test_region_touch_ignores_same_diff_new_model_placeholders():
    tenant_id = uuid4()
    trigger_id = uuid4()
    observation_id = uuid4()
    existing_model_id = uuid4()
    customer_id = uuid4()

    diff = RawDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(observation_id),
                    "scope_entities": [
                        {"type": "customer", "id": str(customer_id)},
                    ],
                },
            )
        ],
        edge_ops=[
            EdgeOp(
                op="add",
                edge_kind="supports",
                source_model_id=observation_id,
                target_model_id=existing_model_id,
            )
        ],
        act_ops=[],
        resource_ops=[],
    )

    touched = set(_iter_entity_ids_touched(diff))
    assert ("customer", str(customer_id)) in touched
    assert ("model", str(existing_model_id)) in touched
    assert ("model", str(observation_id)) not in touched
