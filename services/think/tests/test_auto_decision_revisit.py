from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from lib.shared.ids import uuid7
from services.retrieval.assembler import ContextBundle
from services.retrieval.primary import TriggerContext
from services.think.auto_create_commitment import (
    maybe_inject_customer_risk,
    maybe_inject_decision_revisit,
    maybe_inject_future_prediction,
)
from services.think.diff_schema import ClaimOp, RawDiff


def test_maybe_inject_decision_revisit_adds_scoped_claim_and_transition():
    tenant_id = uuid7()
    obs_id = uuid7()
    actor_id = uuid7()
    decision_id = uuid7()
    content = (
        "Review wrapped. Outcome: decision is being formally revisited. "
        "We're not reversing Kafka, but self-managed-vs-Confluent is reopened."
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        seed_natural_text=content,
        seed_occurred_at=datetime.now(timezone.utc),
    )
    bundle = ContextBundle(
        observations=[
            SimpleNamespace(id=obs_id, content_text=content, actor_id=actor_id)
        ],
        acts_summary={
            "goals": [],
            "commitments": [],
            "decisions": [
                SimpleNamespace(
                    id=decision_id,
                    title="Adopt Kafka as the company-wide event bus",
                    state="active",
                )
            ],
        },
    )
    diff = RawDiff(trigger_ref=obs_id, tenant_id=tenant_id)

    out = maybe_inject_decision_revisit(diff, trigger, bundle)

    assert len(out.claim_ops) == 1
    assert out.claim_ops[0].entry["proposition"]["kind"] == "concern"
    assert out.claim_ops[0].entry["scope_entities"] == [
        {"type": "decision", "id": str(decision_id)}
    ]
    assert len(out.act_ops) == 1
    assert out.act_ops[0].op == "transition_decision"
    assert out.act_ops[0].confidence_basis == obs_id
    assert out.act_ops[0].entity == {
        "id": str(decision_id),
        "new_state": "revisited",
    }


def test_maybe_inject_decision_revisit_ignores_unmatched_text():
    tenant_id = uuid7()
    obs_id = uuid7()
    content = "Review wrapped. No decision changed."
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        seed_natural_text=content,
    )
    bundle = ContextBundle()
    diff = RawDiff(trigger_ref=obs_id, tenant_id=tenant_id)

    out = maybe_inject_decision_revisit(diff, trigger, bundle)

    assert out.claim_ops == []
    assert out.act_ops == []


def test_maybe_inject_future_prediction_splits_explicit_plan():
    tenant_id = uuid7()
    obs_id = uuid7()
    actor_id = uuid7()
    seed_time = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    content = "Refund PR #847 is open; targeting ship tomorrow morning."
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        seed_natural_text=content,
        seed_occurred_at=seed_time,
    )
    bundle = ContextBundle(
        observations=[
            SimpleNamespace(id=obs_id, content_text=content, actor_id=actor_id)
        ],
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
                        "kind": "state",
                        "subject": "Refund PR #847",
                        "assertion": "open",
                    },
                    "natural": "Refund PR #847 is open.",
                    "confidence": 0.66,
                    "scope_actors": [str(actor_id)],
                    "scope_entities": [
                        {"type": "commitment", "id": str(uuid7())}
                    ],
                    "scope_temporal": {
                        "valid_from": seed_time.isoformat(),
                        "valid_until": None,
                    },
                    "falsifier": None,
                },
            )
        ],
    )

    out = maybe_inject_future_prediction(diff, trigger, bundle)

    assert len(out.claim_ops) == 2
    prediction = out.claim_ops[1].entry
    assert prediction["proposition"]["kind"] == "prediction"
    assert prediction["scope_actors"] == [str(actor_id)]
    assert prediction["scope_entities"] == diff.claim_ops[0].entry["scope_entities"]
    assert prediction["falsifier"]["kind"] == "prediction_deadline"
    assert prediction["falsifier"]["evaluate_at"].startswith("2026-05-19")
    assert prediction["scope_temporal"]["valid_until"].startswith("2026-05-19")


def test_maybe_inject_future_prediction_ignores_existing_prediction():
    tenant_id = uuid7()
    obs_id = uuid7()
    content = "Production deploy is scheduled for 3pm today."
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        seed_natural_text=content,
        seed_occurred_at=datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc),
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
                        "kind": "prediction",
                        "expected": "Production deploy happens at 3pm.",
                        "resolution": "The deploy occurs or is delayed.",
                    },
                    "natural": "Production deploy is scheduled for 3pm.",
                    "confidence": 0.62,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "falsifier": {
                        "kind": "prediction_deadline",
                        "evaluate_at": "2026-05-18T15:00:00+00:00",
                        "check": "Look for a deployment result.",
                    },
                },
            )
        ],
    )

    out = maybe_inject_future_prediction(diff, trigger, ContextBundle())

    assert len(out.claim_ops) == 1


def test_maybe_inject_customer_risk_adds_customer_scoped_concern():
    tenant_id = uuid7()
    obs_id = uuid7()
    actor_id = uuid7()
    customer_id = uuid7()
    seed_time = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    content = (
        "Globex just sent a churn-risk email — Marcus is talking renewal "
        "pushback and naming two competitors."
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        seed_natural_text=content,
        seed_occurred_at=seed_time,
        seed_entity_ids=[{"type": "customer", "id": str(customer_id)}],
        scope_actors=[actor_id],
    )
    bundle = ContextBundle(
        observations=[
            SimpleNamespace(
                id=obs_id,
                content_text=content,
                actor_id=actor_id,
                entities_mentioned=[
                    {"type": "customer", "id": str(customer_id)}
                ],
            )
        ],
    )
    diff = RawDiff(trigger_ref=obs_id, tenant_id=tenant_id)

    out = maybe_inject_customer_risk(diff, trigger, bundle)

    assert len(out.claim_ops) == 1
    entry = out.claim_ops[0].entry
    assert entry["proposition"]["kind"] == "concern"
    assert entry["scope_actors"] == [str(actor_id)]
    assert entry["scope_entities"] == [
        {"type": "customer", "id": str(customer_id)}
    ]
    assert entry["falsifier"]["kind"] == "observation_pattern"


def test_maybe_inject_customer_risk_does_not_duplicate_scoped_risk():
    tenant_id = uuid7()
    obs_id = uuid7()
    customer_id = uuid7()
    content = "Globex churn risk is elevated."
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        seed_natural_text=content,
        seed_entity_ids=[{"type": "customer", "id": str(customer_id)}],
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
                        "kind": "concern",
                        "about": "Globex renewal",
                        "nature": "churn risk",
                        "raised_by": "customer",
                    },
                    "natural": "Globex churn risk is elevated.",
                    "confidence": 0.74,
                    "scope_actors": [],
                    "scope_entities": [
                        {"type": "customer", "id": str(customer_id)}
                    ],
                    "scope_temporal": {},
                    "falsifier": None,
                },
            )
        ],
    )

    out = maybe_inject_customer_risk(diff, trigger, ContextBundle())

    assert len(out.claim_ops) == 1
