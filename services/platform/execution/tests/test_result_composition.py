from __future__ import annotations

from uuid import UUID, uuid4

from services.platform.execution.types import ModelRelevance
from services.platform.execution import inquiry, result_composition
from services.platform.execution.types import RetrievalAction
from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.retrieval.primary import TriggerContext


def test_inquiry_private_aliases_point_to_result_composition_module() -> None:
    assert inquiry._merge_results is result_composition._merge_results
    assert inquiry._result_from_pathway is result_composition._result_from_pathway
    assert (
        inquiry._apply_relevance_diversity
        is result_composition._apply_relevance_diversity
    )
    assert inquiry._upsert_evidence is result_composition._upsert_evidence
    assert (
        inquiry._model_relevance_cluster_key
        is result_composition._model_relevance_cluster_key
    )


def test_canonical_entity_pairs_expands_customer_resource_aliases() -> None:
    entity_id = uuid4()

    pairs = result_composition._canonical_entity_pairs(
        [
            {"type": "customer", "id": str(entity_id)},
            {"type": "goal", "id": str(uuid4())},
            {"type": "bad"},
        ]
    )

    assert {
        ("customer", str(entity_id)),
        ("customer_resource", str(entity_id)),
        ("resource", str(entity_id)),
    } <= pairs


def test_result_from_pathway_preserves_pathway_action_notes() -> None:
    trigger = TriggerContext(
        kind="T1",
        tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
        seed_entity_ids=[],
        seed_natural_text="Does the launch have a blocker?",
        seed_occurred_at=None,
        scope_actors=[],
    )
    action = RetrievalAction(
        "Q1",
        "semantic",
        "evidence",
        query="launch blocker",
        budget=3,
    )
    pathway = PathwayResult(source_pathway="B")

    result = result_composition._result_from_pathway(trigger, pathway, action)

    assert result.trigger is trigger
    assert result.pathway_results == [pathway]
    assert result.notes["action"]["question_id"] == "Q1"
    assert result.notes["pathways_run"] == ["B"]


def test_material_canonical_sage_selection_survives_final_gate() -> None:
    relevance = ModelRelevance(
        model_id=uuid4(), final_score=0.01, base_score=0.0,
        lexical_score=0.1, scope_score=0.0, path_score=0.0,
        evidence_score=0.0, provenance_score=0.0, penalty=0.0, reasons=(),
    )

    assert result_composition._is_material_focused_sage_selection(
        relevance, {"SAGE"}
    )


def test_sage_selection_cannot_bypass_materiality_or_unrelated_fence() -> None:
    no_overlap = ModelRelevance(
        model_id=uuid4(), final_score=0.2, base_score=0.1,
        lexical_score=0.0, scope_score=0.0, path_score=0.1,
        evidence_score=0.0, provenance_score=0.0, penalty=0.0, reasons=(),
    )
    unrelated = ModelRelevance(
        model_id=uuid4(), final_score=0.2, base_score=0.0,
        lexical_score=0.2, scope_score=0.0, path_score=0.0,
        evidence_score=0.0, provenance_score=0.0, penalty=0.4,
        reasons=("declares unrelated to trigger",),
    )

    assert not result_composition._is_material_focused_sage_selection(
        no_overlap, {"SAGE"}
    )
    assert not result_composition._is_material_focused_sage_selection(
        unrelated, {"SAGE"}
    )
