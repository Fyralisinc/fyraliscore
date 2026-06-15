from __future__ import annotations

from datetime import datetime, timezone

from lib.shared.ids import uuid7
from services.domain.models.propositions import validate_proposition
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.bridge_inference import maybe_inject_latent_bridge
from services.reasoning.think.diff_schema import ClaimOp, RawDiff


def test_maybe_inject_latent_bridge_from_structured_batch_fragments():
    tenant_id = uuid7()
    obs_ids = [uuid7() for _ in range(3)]
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_ids[0],
        observation_ids=obs_ids,
        subkind="event_batch",
        seed_occurred_at=datetime(2026, 6, 11, tzinfo=timezone.utc),
        seed_signature={
            "batch": True,
            "batch_signal_fragments": [
                {
                    "observation_id": str(obs_ids[0]),
                    "text": (
                        "Forecast checkpoint for Northstar Labs: the expansion "
                        "package is still blocked because discount exception "
                        "approval is absent."
                    ),
                },
                {
                    "observation_id": str(obs_ids[1]),
                    "text": (
                        "Pipeline export for Northstar Labs now shows "
                        "commit-stage expansion with exception pricing applied, "
                        "but no approval record is attached."
                    ),
                },
                {
                    "observation_id": str(obs_ids[2]),
                    "text": (
                        "Ops review asks how Northstar Labs moved from blocked "
                        "pricing to approved exception between checkpoints; "
                        "the sensor trail has a gap."
                    ),
                },
            ],
        },
    )
    diff = RawDiff(trigger_ref=uuid7(), tenant_id=tenant_id)

    out = maybe_inject_latent_bridge(diff, trigger)

    assert len(out.claim_ops) == 1
    entry = out.claim_ops[0].entry
    assert entry["born_from_event_id"] == str(obs_ids[0])
    assert entry["supporting_event_ids"] == [str(oid) for oid in obs_ids]
    assert entry["confidence"] == 0.58
    validate_proposition(entry["proposition"])
    text = f"{entry['natural']} {entry['proposition']}".lower()
    assert "northstar" in text
    assert "blocked" in text
    assert "exception-pricing" in text
    assert "off-sensor" in text
    assert "bounded" in text
    assert "deterministic_bridge_inference" in out.reasoning_trace


def test_maybe_inject_latent_bridge_does_not_duplicate_existing_claim():
    tenant_id = uuid7()
    obs_ids = [uuid7(), uuid7()]
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_ids[0],
        observation_ids=obs_ids,
        subkind="event_batch",
        seed_natural_text=(
            "Northstar pricing was blocked before. Northstar pricing has "
            "approved exception pricing after. The transition has a gap."
        ),
    )
    diff = RawDiff(
        trigger_ref=uuid7(),
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "natural": "Existing bounded inferred bridge for Northstar.",
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "hypothesis",
                        "abstraction_level": "atomic",
                        "hypothesis_text": "bounded inferred bridge",
                    },
                },
            )
        ],
    )

    out = maybe_inject_latent_bridge(diff, trigger)

    assert len(out.claim_ops) == 1
