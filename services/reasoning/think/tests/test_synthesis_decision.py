from uuid import uuid4

from services.reasoning.think.diff_schema import ClaimOp, EdgeOp, ValidatedDiff
from services.reasoning.think.synthesis_decision import summarize_synthesis_decisions


def _diff(**kwargs) -> ValidatedDiff:
    return ValidatedDiff(
        trigger_ref=uuid4(),
        tenant_id=uuid4(),
        **kwargs,
    )


def test_empty_diff_is_discard_decision() -> None:
    decisions = summarize_synthesis_decisions(_diff())

    assert decisions == [{
        "bucket": "diff",
        "index": None,
        "decision": "discard_as_noise",
        "reason": "validated_diff_has_no_mutating_ops",
    }]


def test_claim_insert_carries_memory_grammar() -> None:
    decisions = summarize_synthesis_decisions(_diff(claim_ops=[
        ClaimOp(
            op="insert",
            entry={
                "proposition": {
                    "kind": "concern",
                    "about": "Beacon renewal",
                    "nature": "capacity risk",
                    "raised_by": "cs",
                },
                "natural": "Beacon renewal risk is blocked by platform capacity.",
                "scope_entities": [{"type": "customer", "id": str(uuid4())}],
            },
        )
    ]))

    assert decisions[0]["decision"] == "create_atomic_model"
    assert decisions[0]["proposition_kind"] == "concern"
    assert decisions[0]["claim_role"] == "concern"
    assert decisions[0]["polarity"] == "negative"
    assert decisions[0]["domain_tags"] == [
        "customers",
        "people",
        "systems",
        "execution",
        "risk",
    ]


def test_situation_recommendation_and_edge_are_distinct_decisions() -> None:
    source = uuid4()
    target = uuid4()
    decisions = summarize_synthesis_decisions(_diff(
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "proposition": {
                        "kind": "situation",
                        "situation": "Beacon renewal readiness gap",
                        "summary": "Multiple claims now form one condition.",
                        "member_model_ids": [str(source), str(target)],
                        "relationship_summary": "The claims interact.",
                        "pressure_type": "revenue",
                        "shared_mechanism": "Same renewal path.",
                    },
                    "natural": "Beacon renewal readiness gap.",
                },
            ),
            ClaimOp(
                op="insert",
                entry={
                    "proposition": {
                        "kind": "recommendation",
                        "target_act_ref": None,
                        "proposed_change": {"operation": "create", "payload": {}},
                        "qualitative_impact": "Reduce renewal risk.",
                        "target_actor_id": None,
                    },
                    "natural": "Create a recovery commitment.",
                },
            ),
        ],
        edge_ops=[
            EdgeOp(
                op="add",
                source_model_id=source,
                target_model_id=target,
                edge_kind="blocks",
            )
        ],
    ))

    assert [d["decision"] for d in decisions] == [
        "create_or_update_situation",
        "create_action_proposal",
        "create_or_update_edge",
    ]
