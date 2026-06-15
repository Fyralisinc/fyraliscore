from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from services.platform.execution import inquiry, motif_utils
from services.platform.execution.types import RetrievalAction
from services.reasoning.retrieval.primary import TriggerContext


def _trigger(
    text: str = "SOC2 security blocker risk for customer renewal",
) -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_entity_ids=[
            {"type": "customer", "id": str(uuid4())},
            {"type": "commitment", "id": str(uuid4())},
        ],
        scope_actors=[],
        seed_natural_text=text,
        seed_occurred_at=datetime(2026, 6, 13, 8, 0, tzinfo=timezone.utc),
    )


def test_motif_helpers_keep_legacy_inquiry_identity() -> None:
    assert inquiry._action_motif_uuid is motif_utils.action_motif_uuid
    assert inquiry._json_obj is motif_utils.json_obj
    assert inquiry._motif_domain_terms is motif_utils.motif_domain_terms
    assert inquiry._motif_plan_from_actions is motif_utils.motif_plan_from_actions
    assert inquiry._motif_signature_for is motif_utils.motif_signature_for
    assert (
        inquiry._motif_signature_match_score is motif_utils.motif_signature_match_score
    )
    assert inquiry._packet_used_evidence_ids is motif_utils.packet_used_evidence_ids
    assert inquiry._safe_int is motif_utils.safe_int
    assert inquiry._safe_uuid is motif_utils.safe_uuid
    assert inquiry._set_overlap_ratio is motif_utils.set_overlap_ratio


def test_motif_signature_and_match_score_are_stable() -> None:
    trigger = _trigger()
    signature = motif_utils.motif_signature_for(trigger, "DEPENDENCY")

    assert signature["signal_type"] == "T1"
    assert signature["signal_class"] == "material"
    assert signature["question_primitive"] == "DEPENDENCY"
    assert signature["entity_types"] == ["commitment", "customer"]
    assert signature["domain_terms"] == [
        "blocker",
        "customer",
        "renewal",
        "risk",
        "security",
        "soc2",
    ]
    assert motif_utils.motif_signature_match_score(signature, signature) == 1.0
    assert motif_utils.set_overlap_ratio(["a", "b"], ["b", "c"]) == 1 / 3


def test_json_safe_coercion_and_packet_ids() -> None:
    evidence_id = str(uuid4())

    assert motif_utils.json_obj(b'{"a": 1}') == {"a": 1}
    assert motif_utils.json_obj("not json") == {}
    assert motif_utils.safe_int("-2") == 0
    assert motif_utils.safe_int("3") == 3
    assert motif_utils.safe_uuid(evidence_id) is not None
    assert motif_utils.safe_uuid("not-a-uuid") is None
    assert motif_utils.packet_used_evidence_ids(
        {
            "tiers": {
                "decisive_evidence": [{"evidence_id": evidence_id}],
                "supporting_evidence_groups": [
                    {"evidence_ids": ["supporting-1", "supporting-2"]}
                ],
            }
        }
    ) == {evidence_id, "supporting-1", "supporting-2"}


def test_motif_plan_from_actions_creates_staged_deduped_recipe() -> None:
    motif_id = uuid4()
    actions = [
        RetrievalAction(
            "Q1",
            "focused_index",
            "answerability",
            filters={"_motif_id": str(motif_id)},
            budget=8,
        ),
        RetrievalAction("Q1", "focused_index", "answerability", budget=99),
        RetrievalAction("Q1", "semantic", "semantic_counterevidence", budget=5),
    ]

    plan = motif_utils.motif_plan_from_actions(actions)

    assert motif_utils.action_motif_uuid(actions[0]) == motif_id
    assert plan == {
        "version": 1,
        "execution": "staged",
        "actions": [
            {
                "path": "focused_index",
                "target": "answerability",
                "budget": 8,
                "stage": 1,
                "bind_previous_scope": False,
            },
            {
                "path": "semantic",
                "target": "semantic_counterevidence",
                "budget": 5,
                "stage": 2,
                "bind_previous_scope": True,
            },
        ],
    }
