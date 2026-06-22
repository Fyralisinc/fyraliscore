from __future__ import annotations

from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval.projection_pathway import projection_subject_candidates


def test_projection_subject_candidates_use_text_and_entity_scope() -> None:
    tenant_id = uuid7()
    customer_id = uuid7()
    actor_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        seed_natural_text="Cash runway and hiring capacity affect customer renewal.",
    )

    subjects = projection_subject_candidates(
        trigger,
        effective_seed_entities=[
            {"type": "customer_resource", "id": str(customer_id)},
        ],
        effective_scope_actors=[actor_id],
    )

    assert ("constraints", "company:runway") in subjects
    assert ("constraints", "company:financial_capacity") in subjects
    assert ("constraints", "company:capacity") in subjects
    assert ("resources", "company:financial") in subjects
    assert ("resources", "company:capacity") in subjects
    assert ("resources", "company:relational") in subjects
    assert ("constraints", f"customer:{customer_id}:constraints") in subjects
    assert ("resources", f"customer:{customer_id}:resources") in subjects
    assert ("employee_profiles", f"employee:{actor_id}:profile") in subjects
