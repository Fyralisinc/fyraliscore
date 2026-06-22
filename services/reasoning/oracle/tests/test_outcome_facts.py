from __future__ import annotations

from datetime import datetime, timezone

from lib.shared.ids import uuid7
from services.reasoning.oracle import (
    AUTHORITATIVE_TRUST_TIER,
    human_correction_outcome_fact,
    representation_repair_payload_for_outcome,
)


def test_human_correction_outcome_fact_builds_authoritative_payload() -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    delta_id = uuid7()
    occurred_at = datetime(2026, 6, 20, 4, 5, tzinfo=timezone.utc)

    fact = human_correction_outcome_fact(
        tenant_id=tenant_id,
        delta_id=delta_id,
        actor_id=actor_id,
        correction_type="wrong_conclusion",
        explanation="We already remediated this last week.",
        supporting_link="https://example.test/thread/1",
        apply_to_related=True,
        occurred_at=occurred_at,
        main_assertion="Salesforce sync failures threaten anchor renewals.",
        target_node_kind="customer",
        target_node_id=uuid7(),
        evidence=[
            {
                "id": uuid7(),
                "source": "salesforce",
                "title": "Renewal health",
                "trust_tier": "verified",
                "ts": occurred_at,
            }
        ],
    )

    payload = fact.to_payload()

    assert payload["fact_kind"] == "human_correction"
    assert payload["subject_type"] == "decision_delta"
    assert payload["subject_id"] == str(delta_id)
    assert payload["source"] == "today_delta_correction"
    assert payload["trust_tier"] == AUTHORITATIVE_TRUST_TIER
    assert payload["actor_id"] == str(actor_id)
    assert payload["payload"]["correction"]["type"] == "wrong_conclusion"
    assert payload["payload"]["correction"]["apply_to_related"] is True
    assert payload["payload"]["decision_delta"]["main_assertion"].startswith("Salesforce")
    assert payload["payload"]["evidence"][0]["source"] == "salesforce"


def test_representation_repair_payload_for_human_correction_fact() -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    delta_id = uuid7()
    fact = human_correction_outcome_fact(
        tenant_id=tenant_id,
        delta_id=delta_id,
        actor_id=actor_id,
        correction_type="already_handled",
        explanation="The deployment completed and the customer confirmed.",
        main_assertion="Deployment is blocked.",
    )

    payload = representation_repair_payload_for_outcome(fact)

    assert payload["repair_key"] == (
        f"oracle:human_correction:decision_delta:{delta_id}:"
        "correction_submitted:already_handled"
    )
    assert payload["repair_intent"] == "apply_human_correction"
    assert payload["audit_warning_code"] == "human_correction_submitted"
    assert payload["source_delta_id"] == str(delta_id)
    assert payload["seed_entity_ids"] == [
        {"type": "decision_delta", "id": str(delta_id)}
    ]
    assert payload["scope_actors"] == [str(actor_id)]
    assert "Deployment is blocked" in payload["seed_natural_text"]
    assert payload["oracle_outcome_fact"]["trust_tier"] == AUTHORITATIVE_TRUST_TIER
