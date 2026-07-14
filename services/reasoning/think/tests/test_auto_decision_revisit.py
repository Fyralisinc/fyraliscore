from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from lib.shared.ids import uuid7
from services.domain.models.propositions import validate_proposition
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.auto_create_commitment import (
    maybe_inject_customer_risk,
    maybe_inject_decision_pressure_recommendation,
    maybe_inject_decision_revisit,
    maybe_inject_future_prediction,
)
from services.reasoning.think.diff_schema import ClaimOp, RawDiff


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


def test_maybe_inject_decision_revisit_ignores_drafted_decision():
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
                    state="drafted",
                )
            ],
        },
    )
    diff = RawDiff(trigger_ref=obs_id, tenant_id=tenant_id)

    out = maybe_inject_decision_revisit(diff, trigger, bundle)

    assert out.claim_ops == []
    assert out.act_ops == []


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


def test_maybe_inject_future_prediction_ignores_t1_event_batch():
    tenant_id = uuid7()
    obs_id = uuid7()
    content = (
        "Batch of 30 signals:\n"
        "- 019ed474-0000-7000-8000-000000000000: Production deploy is scheduled "
        "for 3pm today."
    )
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=obs_id,
        seed_natural_text=content,
        seed_occurred_at=datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc),
        member_trigger_ids=[uuid7()],
    )
    diff = RawDiff(trigger_ref=obs_id, tenant_id=tenant_id)

    out = maybe_inject_future_prediction(diff, trigger, ContextBundle())

    assert out.claim_ops == []


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


def test_maybe_inject_decision_pressure_recommendation_from_situation():
    tenant_id = uuid7()
    obs_id = uuid7()
    customer_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
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
                        "claim_role": "situation",
                        "abstraction_level": "composite",
                        "situation": "Foundry connector reliability is renewal risk",
                        "summary": (
                            "Connector reliability and stale freshness signals "
                            "are now creating renewal risk."
                        ),
                        "member_model_ids": [str(uuid7()), str(uuid7())],
                        "relationship_summary": "Reliability issues affect renewal.",
                        "status": "forming",
                        "pressure_type": "revenue",
                        "shared_mechanism": "Repeated connector failures.",
                        "judgment_change": "This needs owner review.",
                        "affected_customers": ["FoundryWorks"],
                        "affected_decisions": [],
                        "affected_teams": ["customer_success"],
                        "evidence_event_ids": [str(obs_id)],
                        "open_falsifier": (
                            "Connector freshness recovers and renewal risk clears."
                        ),
                    },
                    "natural": (
                        "Foundry connector reliability is now renewal risk."
                    ),
                    "confidence": 0.67,
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

    out = maybe_inject_decision_pressure_recommendation(
        diff, trigger, ContextBundle()
    )

    assert len(out.claim_ops) == 2
    recommendation = out.claim_ops[1].entry
    assert recommendation["proposition"]["kind"] == "norm"
    assert recommendation["proposition"]["claim_role"] == "recommendation"
    assert recommendation["proposition"]["target_act_ref"] is None
    assert recommendation["proposition"]["target_actor_id"] is None
    assert recommendation["proposition"]["proposed_change"] == {
        "operation": "create",
        "payload": {
            "title": (
                "Review next action: "
                "Foundry connector reliability is renewal risk"
            ),
            "description": (
                "Assign an accountable owner to decide the next step for this "
                "accepted operational pressure; do not mutate the Acts ledger "
                "automatically."
            ),
            "kind": "decision_pressure",
            "source_pressure_type": "revenue",
        },
    }
    assert recommendation["scope_entities"] == [
        {"type": "customer", "id": str(customer_id)}
    ]
    assert "owner review" in recommendation["semantic_terms"]
    assert out.act_ops == []
    parsed = validate_proposition(recommendation["proposition"])
    assert parsed.claim_role == "recommendation"


def test_maybe_inject_decision_pressure_recommendation_creates_owned_decision():
    tenant_id = uuid7()
    obs_id = uuid7()
    actor_id = uuid7()
    customer_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
    )
    bundle = ContextBundle(
        observations=[
            SimpleNamespace(id=obs_id, content_text="Review pressure", actor_id=actor_id)
        ]
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
                        "claim_role": "situation",
                        "abstraction_level": "composite",
                        "situation": "Foundry controls approval blocks renewal",
                        "summary": (
                            "Controls approval is blocking renewal confidence."
                        ),
                        "member_model_ids": [str(uuid7()), str(uuid7())],
                        "relationship_summary": (
                            "Security approval blocks renewal progress."
                        ),
                        "status": "forming",
                        "pressure_type": "revenue",
                        "shared_mechanism": "Approval owner is missing.",
                        "judgment_change": "A named owner must decide next action.",
                        "affected_customers": ["FoundryWorks"],
                        "affected_decisions": [],
                        "affected_teams": ["security", "customer_success"],
                        "evidence_event_ids": [str(obs_id)],
                        "open_falsifier": (
                            "Security owner signs off and renewal confidence "
                            "recovers."
                        ),
                    },
                    "natural": (
                        "Foundry controls approval is now blocking renewal."
                    ),
                    "confidence": 0.69,
                    "scope_actors": [str(actor_id)],
                    "scope_entities": [
                        {"type": "customer", "id": str(customer_id)}
                    ],
                    "scope_temporal": {},
                    "falsifier": None,
                },
            )
        ],
    )

    out = maybe_inject_decision_pressure_recommendation(diff, trigger, bundle)

    assert len(out.claim_ops) == 2
    pressure_entry = out.claim_ops[0].entry
    assert pressure_entry is not None
    pressure_model_id = pressure_entry["model_id"]
    assert len(out.act_ops) == 1
    act = out.act_ops[0]
    assert act.op == "create_decision"
    assert str(act.confidence_basis) == pressure_model_id
    assert act.entity["title"] == (
        "Decide next action: Foundry controls approval blocks renewal"
    )
    assert act.entity["scope"]["owner_actor_id"] == str(actor_id)
    assert act.entity["scope"]["entities"] == [
        {"type": "customer", "id": str(customer_id)}
    ]
    assert act.entity["revisit_triggers"] == {
        "owner_assigns_action": "Owner assigns action",
        "pressure_resolves": "Pressure resolves",
        "pressure_not_material": (
            "Later evidence shows the pressure is not material"
        ),
    }


def test_maybe_inject_decision_pressure_recommendation_keeps_existing_rec():
    tenant_id = uuid7()
    obs_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
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
                        "kind": "norm",
                        "claim_role": "recommendation",
                        "target_act_ref": None,
                        "proposed_change": {"operation": "create", "payload": {}},
                        "expected_impact": None,
                        "qualitative_impact": "Review the blocker.",
                        "target_actor_id": None,
                    },
                    "natural": "Review the blocker.",
                    "confidence": 0.64,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "falsifier": None,
                },
            )
        ],
    )

    out = maybe_inject_decision_pressure_recommendation(
        diff, trigger, ContextBundle()
    )

    assert len(out.claim_ops) == 1


def test_maybe_inject_decision_pressure_recommendation_ignores_noise():
    tenant_id = uuid7()
    obs_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
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
                        "claim_role": "situation",
                        "abstraction_level": "composite",
                        "situation": "Background noise window",
                        "summary": (
                            "Background noise and non-actionable chatter are "
                            "present in the source window."
                        ),
                        "member_model_ids": [str(uuid7()), str(uuid7())],
                        "relationship_summary": "No business action.",
                        "status": "forming",
                        "pressure_type": "execution",
                        "shared_mechanism": "Duplicated dashboard reminders.",
                        "judgment_change": "No business fact should be written.",
                        "affected_customers": [],
                        "affected_decisions": [],
                        "affected_teams": [],
                        "evidence_event_ids": [str(obs_id)],
                        "open_falsifier": "A later signal shows concrete work.",
                    },
                    "natural": "Background noise and non-actionable chatter.",
                    "confidence": 0.8,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "falsifier": None,
                },
            )
        ],
    )

    out = maybe_inject_decision_pressure_recommendation(
        diff, trigger, ContextBundle()
    )

    assert len(out.claim_ops) == 1


def test_maybe_inject_decision_pressure_recommendation_requires_event_id():
    tenant_id = uuid7()
    trigger = TriggerContext(kind="T1", tenant_id=tenant_id)
    diff = RawDiff(
        trigger_ref=uuid7(),
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "situation",
                        "abstraction_level": "composite",
                        "situation": "Revenue pressure needs review",
                        "summary": "Revenue pressure needs owner review.",
                        "member_model_ids": [str(uuid7()), str(uuid7())],
                        "relationship_summary": "Multiple risks interact.",
                        "status": "forming",
                        "pressure_type": "revenue",
                        "shared_mechanism": "Same renewal path.",
                        "judgment_change": "This needs owner review.",
                        "affected_customers": ["Acme"],
                        "affected_decisions": [],
                        "affected_teams": [],
                        "evidence_event_ids": [],
                        "open_falsifier": "The risk clears.",
                    },
                    "natural": "Revenue pressure needs owner review.",
                    "confidence": 0.8,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "falsifier": None,
                },
            )
        ],
    )

    out = maybe_inject_decision_pressure_recommendation(
        diff, trigger, ContextBundle()
    )

    assert len(out.claim_ops) == 1
