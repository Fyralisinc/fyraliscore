from __future__ import annotations

import json
import math
from uuid import uuid4

import pytest

from lib.evaluation.entity_pipeline_gold import (
    GoldEntityPipelineCase,
    GoldRelationExpectation,
    analyze_entity_pipeline_rows,
    canonical_ref_key,
    evaluate_persisted_entity_pipeline,
)
from scripts.evaluate_entity_pipeline_gold import (
    load_gold_manifest,
    load_gold_manifest_bundle,
)


def _canonical(candidate_id: str, entity_type: str, referent_id: str):
    return {
        "candidate_id": candidate_id,
        "kind": "canonical_referent",
        "canonical_referent_id": referent_id,
        "canonical_referent_version": 1,
        "candidate_source": "tenant_aliases",
        "candidate_type": entity_type,
        "authorized_positive_evidence_refs": ["sealed-source-evidence"],
        "authorized_negative_evidence_refs": [],
    }


def _special(candidate_id: str, kind: str):
    return {
        "candidate_id": candidate_id,
        "kind": kind,
        "canonical_referent_id": None,
        "canonical_referent_version": None,
        "candidate_source": None,
        "candidate_type": None,
        "authorized_positive_evidence_refs": [],
        "authorized_negative_evidence_refs": [],
    }


def _pipeline_row(
    *, observation_id, surface, candidates, selected_id=None, selected_ref=None,
    current_fate="resolved_for_consumer", type_distribution=None,
    semantic_disposition=None, break_semantic_model_lineage=False,
):
    (
        context_id, detection_id, type_assessment_id, request_id,
        set_id, assessment_id, admission_id, trace_id,
    ) = (uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), uuid4())
    type_distribution = type_distribution or {"unknown": 1.0}
    semantic_interpretation_id = uuid4() if semantic_disposition else None
    semantic_admission_id = uuid4() if semantic_disposition else None
    semantic_grounding_admission_id = (
        uuid4() if semantic_disposition == "belief_applied"
        else (admission_id if semantic_disposition == "no_admission" else None)
    )
    semantic_model_id = uuid4() if semantic_disposition == "belief_applied" else None
    mention_ref = f"mention:{detection_id}"
    distribution = {
        item["candidate_id"]: max(0.01, 0.9 - index * 0.2)
        for index, item in enumerate(candidates)
    }
    total = sum(distribution.values())
    distribution = {key: value / total for key, value in distribution.items()}
    return {
        "source_observation_id": observation_id,
        "candidate_surface": surface,
        "detection_id": detection_id,
        "entity_mention_id": detection_id,
        "detection_fate": "detected",
        "context_snapshot_id": context_id,
        "detection_command": {
            "detection": {"entity_type_assessment": {
                "assessment_id": str(type_assessment_id),
                "type_distribution": type_distribution,
            }}
        },
        "candidate_request_id": request_id,
        "mention_ref": mention_ref,
        "candidate_request": {
            "entity_type_assessment_refs": [str(type_assessment_id)],
        },
        "candidate_set_id": set_id,
        "candidates": candidates,
        "assessment_id": assessment_id,
        "candidate_distribution": distribution,
        "selected_candidate_id": selected_id,
        "admission_id": admission_id,
        "trace_id": trace_id,
        "current_fate": current_fate,
        "selected_referent": selected_ref,
        "trace": {
            "context_snapshot": {"id": str(context_id)},
            "mention_detection": {"id": str(detection_id)},
            "candidate_request": {"id": str(request_id)},
            "candidate_set": {"id": str(set_id)},
            "assessment": {"id": str(assessment_id)},
            "admission": {"id": str(admission_id)},
        },
        "semantic_interpretation_id": semantic_interpretation_id,
        "semantic_grounding_trace_id": trace_id if semantic_disposition else None,
        "semantic_source_observation_id": observation_id if semantic_disposition else None,
        "semantic_context_snapshot_id": context_id if semantic_disposition else None,
        "semantic_entity_mention_id": detection_id if semantic_disposition else None,
        "semantic_resolution_assessment_id": assessment_id if semantic_disposition else None,
        "semantic_grounding_admission_id": semantic_grounding_admission_id,
        "semantic_grounding_admission_assessment_id": (
            assessment_id if semantic_disposition else None
        ),
        "semantic_grounding_admission_consumer": (
            "epistemic-applier" if semantic_disposition == "belief_applied"
            else ("entity-resolver" if semantic_disposition == "no_admission" else None)
        ),
        "semantic_grounding_admission_purpose": (
            "belief-admission" if semantic_disposition == "belief_applied"
            else ("entity-resolution" if semantic_disposition == "no_admission" else None)
        ),
        "semantic_grounding_admission_operation": (
            "create-grounded-belief" if semantic_disposition == "belief_applied"
            else ("resolve-mention" if semantic_disposition == "no_admission" else None)
        ),
        "semantic_grounding_continuity": (
            {
                "grounding_admission_ref": (
                    f"grounding-admission:{semantic_grounding_admission_id}"
                ),
                "resolution_assessment_ref": f"resolution-assessment:{assessment_id}",
                "mention_ref": mention_ref,
                "downstream_object_ref": (
                    f"model:{semantic_model_id}"
                    if semantic_disposition == "belief_applied"
                    else f"source-semantic-interpretation:{semantic_interpretation_id}"
                ),
            }
            if semantic_disposition else None
        ),
        "semantic_admission_id": semantic_admission_id,
        "semantic_disposition": semantic_disposition,
        "semantic_admitted_model_id": semantic_model_id,
        "downstream_model_id": semantic_model_id,
        "semantic_interpretation_model_count": int(semantic_model_id is not None),
        "downstream_model_proposition": (
            {"source_semantic_interpretation_id": str(
                uuid4() if break_semantic_model_lineage else semantic_interpretation_id
            )}
            if semantic_model_id else None
        ),
    }


class _FakeDb:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def fetch(self, query, *args):
        self.calls.append((query, args))
        return self.rows


@pytest.mark.asyncio
async def test_db_backed_entity_pipeline_scores_one_batched_population() -> None:
    tenant_id = uuid4()
    observation_ids = [uuid4() for _ in range(5)]
    common_specials = [
        _special("none", "none_of_the_above"),
        _special("novel", "novel_referent"),
        _special("unknown", "unknown"),
    ]
    nimbus_ref = {"type": "customer", "id": "customer:nimbus", "version": 1}
    jira_ref = {"type": "workstream", "id": "workstream:PAY-42", "version": 1}
    alex_ref = {"type": "person", "id": "person:alex-r", "version": 1}
    other_alex_ref = {"type": "person", "id": "person:alex-s", "version": 1}

    rows = [
        # Exact learned alias resolution.
        _pipeline_row(
            observation_id=observation_ids[0], surface="NBI",
            candidates=[_canonical("nimbus", "customer", "customer:nimbus"), *common_specials],
            selected_id="nimbus", selected_ref=nimbus_ref,
            type_distribution={"customer": 0.9, "unknown": 0.1},
            semantic_disposition="belief_applied",
        ),
        # Structured Jira source field provides a candidate without an alias.
        _pipeline_row(
            observation_id=observation_ids[1], surface="PAY-42",
            candidates=[_canonical("jira", "workstream", "workstream:PAY-42"), *common_specials],
            selected_id="jira", selected_ref=jira_ref,
            type_distribution={"workstream": 0.8, "unknown": 0.2},
            semantic_disposition="no_admission",
        ),
        # Open-world phrase safely abstains; no link accuracy is manufactured.
        _pipeline_row(
            observation_id=observation_ids[2], surface="Project Zephyr",
            candidates=[*common_specials], selected_id="unknown", selected_ref=None,
            current_fate="abstained",
            type_distribution={"workstream": 0.6, "unknown": 0.4},
        ),
        # Homonymous candidates remain in review rather than being called correct.
        _pipeline_row(
            observation_id=observation_ids[3], surface="Alex",
            candidates=[
                _canonical("alex-r", "person", "person:alex-r"),
                _canonical("alex-s", "person", "person:alex-s"),
                *common_specials,
            ], selected_id="alex-r", selected_ref=None, current_fate="review",
            type_distribution={"person": 0.7, "unknown": 0.3},
        ),
        # Rejected detections terminate before candidate generation.
        {
            "source_observation_id": observation_ids[4],
            "candidate_surface": "Friday", "detection_id": uuid4(),
            "detection_fate": "rejected_not_entity", "candidate_request_id": None,
            "context_snapshot_id": uuid4(), "detection_command": {},
            "candidate_request": None, "admission_id": None,
            "candidate_set_id": None, "candidates": None, "assessment_id": None,
            "candidate_distribution": None, "selected_candidate_id": None,
            "trace_id": None, "current_fate": None, "selected_referent": None,
            "trace": None,
        },
    ]
    cases = [
        GoldEntityPipelineCase(
            case_id=f"case-{index}", batch_id="batch-25-signals",
            source_observation_id=observation_ids[index], surface=surface,
            gold_entity_type=entity_type, gold_canonical_label=label,
            expected_detection_fate=("rejected" if surface == "Friday" else "detected"),
            acceptable_terminal_fates=(
                ("review",) if surface == "Alex"
                else (("abstained", "unresolved") if surface == "Project Zephyr" else ())
            ),
            expected_semantic_disposition=(
                "belief_applied" if surface == "NBI"
                else ("no_admission" if surface == "PAY-42" else None)
            ),
        )
        for index, (surface, entity_type, label) in enumerate(
            [
                ("NBI", "customer", "gold:nimbus"),
                ("PAY-42", "workstream", "gold:pay-42"),
                ("Project Zephyr", "workstream", None),
                ("Alex", "person", "gold:alex-r"),
                ("Friday", "date", None),
            ]
        )
    ]
    sealed_labels = {
        canonical_ref_key(nimbus_ref): "gold:nimbus",
        canonical_ref_key(jira_ref): "gold:pay-42",
        canonical_ref_key(alex_ref): "gold:alex-r",
        canonical_ref_key(other_alex_ref): "gold:alex-s",
    }
    db = _FakeDb(rows)

    report = await evaluate_persisted_entity_pipeline(
        db, tenant_id=tenant_id, gold_cases=cases,
        canonical_gold_labels=sealed_labels, ks=(1, 3),
    )

    assert len(db.calls) == 1
    assert "entity_mention_detection_heads" in db.calls[0][0]
    assert "agency_command_results" in db.calls[0][0]
    assert "source_semantic_interpretations" in db.calls[0][0]
    assert "source_semantic_admission_decisions" in db.calls[0][0]
    assert "semantic_interpretation_model_count" in db.calls[0][0]
    assert "FROM model_edges edge" in db.calls[0][0]
    assert "topology.downstream_relations" in db.calls[0][0]
    assert set(db.calls[0][1][1]) == set(observation_ids)
    metrics = report.overall
    assert metrics.gold_case_count == 5
    assert metrics.detected_case_count == 4
    assert metrics.candidate_population_count == 4
    assert metrics.assessed_case_count == 4
    assert metrics.type_assessed_case_count == 4
    assert metrics.terminal_case_count == 5
    assert metrics.governed_fate_coverage == 1.0
    assert metrics.candidate_population_coverage == 1.0
    assert metrics.resolution_assessment_coverage == 1.0
    assert metrics.detection_accuracy == 1.0
    assert metrics.type_assessment_coverage == 1.0
    assert metrics.type_assessment_accuracy == 1.0
    assert metrics.mean_gold_type_probability == pytest.approx(0.75)
    assert metrics.type_assessment_brier_score == pytest.approx(0.15)
    assert metrics.type_assessment_log_loss == pytest.approx(
        sum(-math.log(value) for value in (0.9, 0.8, 0.6, 0.7)) / 4
    )
    assert metrics.candidate_recall_at_k == {1: 1.0, 3: 1.0}
    assert metrics.gold_type_present_at_k == {1: 0.75, 3: 0.75}
    assert metrics.selected_type_accuracy == 1.0
    assert metrics.canonical_link_accuracy == 1.0
    assert metrics.canonical_link_coverage == pytest.approx(2 / 3)
    assert metrics.abstention_rate == 0.25
    assert metrics.review_rate == 0.25
    assert metrics.terminal_fate_accuracy == 1.0
    assert metrics.safe_decision_rate == 1.0
    assert metrics.harmful_false_link_rate == 0.0
    assert metrics.detection_to_terminal_coverage == 1.0
    assert metrics.lineage_integrity == 1.0
    assert metrics.rejected_detection_candidate_count == 0
    assert metrics.unknown_canonical_ref_count == 0
    assert metrics.invalid_type_assessment_count == 0
    assert metrics.type_assessment_lineage_integrity == 1.0
    assert metrics.semantic_expected_case_count == 2
    assert metrics.semantic_interpretation_count == 2
    assert metrics.semantic_decision_count == 2
    assert metrics.belief_applied_count == 1
    assert metrics.semantic_interpretation_coverage == 1.0
    assert metrics.semantic_decision_coverage == 1.0
    assert metrics.semantic_disposition_accuracy == 1.0
    assert metrics.semantic_lineage_integrity == 1.0
    assert metrics.belief_model_materialization_rate == 1.0
    assert metrics.belief_model_lineage_integrity == 1.0
    assert metrics.no_admission_no_model_safety_rate == 1.0
    assert metrics.harmful_semantic_propagation_rate == 0.0
    assert report.by_batch["batch-25-signals"] == metrics
    assert report.uncertainties == (
        "canonical_metrics_exclude_open_world_gold_cases",
        "terminal_fate_accuracy_excludes_unlabeled_cases",
        "semantic_impact_metrics_exclude_unlabeled_cases",
        "relation_topology_metrics_exclude_unlabeled_cases",
    )


@pytest.mark.asyncio
async def test_rejected_detection_with_candidate_population_is_reported_not_scored() -> None:
    tenant_id, observation_id = uuid4(), uuid4()
    row = {
        "source_observation_id": observation_id, "candidate_surface": "Friday",
        "detection_id": uuid4(), "detection_fate": "rejected_not_entity",
        "context_snapshot_id": uuid4(), "detection_command": {},
        "candidate_request_id": uuid4(), "candidate_set_id": uuid4(),
        "candidate_request": {}, "admission_id": None,
        "candidates": [_canonical("wrong", "person", "person:friday")],
        "assessment_id": None, "candidate_distribution": None,
        "selected_candidate_id": None, "trace_id": None, "current_fate": None,
        "selected_referent": None, "trace": None,
    }
    report = await evaluate_persisted_entity_pipeline(
        _FakeDb([row]), tenant_id=tenant_id,
        gold_cases=[GoldEntityPipelineCase(
            case_id="rejected", batch_id="batch", source_observation_id=observation_id,
            surface="Friday", gold_entity_type="date",
        )], canonical_gold_labels={},
    )
    assert report.overall.rejected_detection_candidate_count == 1
    assert report.overall.candidate_population_count == 0
    assert report.overall.assessed_case_count == 0
    assert report.overall.lineage_integrity == 0.0


@pytest.mark.asyncio
async def test_semantic_lineage_rejects_swapped_disposition_admission_contracts() -> None:
    tenant_id = uuid4()
    observation_ids = [uuid4(), uuid4()]
    referent = {"type": "customer", "id": "customer:nimbus", "version": 1}
    belief = _pipeline_row(
        observation_id=observation_ids[0], surface="NBI",
        candidates=[_canonical("nimbus", "customer", "customer:nimbus")],
        selected_id="nimbus", selected_ref=referent,
        semantic_disposition="belief_applied",
    )
    # Applied beliefs may not reuse the resolver's consumer admission.
    belief["semantic_grounding_admission_id"] = belief["admission_id"]
    belief["semantic_grounding_continuity"]["grounding_admission_ref"] = (
        f"grounding-admission:{belief['admission_id']}"
    )
    no_admission = _pipeline_row(
        observation_id=observation_ids[1], surface="NBI",
        candidates=[_canonical("nimbus", "customer", "customer:nimbus")],
        selected_id="nimbus", selected_ref=referent,
        semantic_disposition="no_admission",
    )
    # A non-admission may not manufacture a separate epistemic authorization.
    foreign_admission = uuid4()
    no_admission["semantic_grounding_admission_id"] = foreign_admission
    no_admission["semantic_grounding_continuity"]["grounding_admission_ref"] = (
        f"grounding-admission:{foreign_admission}"
    )
    cases = [
        GoldEntityPipelineCase(
            case_id=f"semantic-{index}", batch_id="batch",
            source_observation_id=observation_id, surface="NBI",
            gold_entity_type="customer", gold_canonical_label="gold:nimbus",
            expected_semantic_disposition=disposition,
        )
        for index, (observation_id, disposition) in enumerate(zip(
            observation_ids, ("belief_applied", "no_admission"), strict=True
        ))
    ]
    report = await evaluate_persisted_entity_pipeline(
        _FakeDb([belief, no_admission]), tenant_id=tenant_id, gold_cases=cases,
        canonical_gold_labels={canonical_ref_key(referent): "gold:nimbus"},
    )
    assert report.overall.semantic_lineage_integrity == 0.0


@pytest.mark.asyncio
async def test_pipeline_exposes_missing_stage_wrong_link_and_broken_lineage() -> None:
    tenant_id = uuid4()
    observation_ids = [uuid4(), uuid4()]
    wrong_ref = {"type": "customer", "id": "customer:wrong", "version": 1}
    row = _pipeline_row(
        observation_id=observation_ids[0],
        surface="NBI",
        candidates=[_canonical("wrong", "customer", "customer:wrong")],
        selected_id="wrong",
        selected_ref=wrong_ref,
        type_distribution={"person": 0.8, "customer": 0.1, "unknown": 0.1},
        semantic_disposition="belief_applied",
        break_semantic_model_lineage=True,
    )
    row["candidate_request"] = {"entity_type_assessment_refs": [str(uuid4())]}
    cases = [
        GoldEntityPipelineCase(
            case_id="wrong-link", batch_id="batch-a",
            source_observation_id=observation_ids[0], surface="NBI",
            gold_entity_type="customer", gold_canonical_label="gold:nimbus",
            expected_detection_fate="detected",
            acceptable_terminal_fates=("review", "unresolved", "abstained"),
            expected_semantic_disposition="no_admission",
        ),
        GoldEntityPipelineCase(
            case_id="missing-fate", batch_id="batch-b",
            source_observation_id=observation_ids[1], surface="Zephyr",
            gold_entity_type="workstream", expected_detection_fate="detected",
            acceptable_terminal_fates=("review", "unresolved", "abstained"),
            expected_semantic_disposition="no_admission",
        ),
    ]
    report = await evaluate_persisted_entity_pipeline(
        _FakeDb([row]), tenant_id=tenant_id, gold_cases=cases,
        canonical_gold_labels={canonical_ref_key(wrong_ref): "gold:wrong"}, ks=(1,),
    )

    metrics = report.overall
    assert metrics.governed_fate_coverage == 0.5
    assert metrics.detection_accuracy == 0.5
    assert metrics.type_assessment_accuracy == 0.0
    assert metrics.mean_gold_type_probability == pytest.approx(0.1)
    assert metrics.canonical_link_accuracy == 0.0
    assert metrics.canonical_link_coverage == 1.0
    assert metrics.safe_decision_rate == 0.0
    assert metrics.harmful_false_link_rate == 1.0
    assert metrics.terminal_fate_accuracy == 0.0
    assert metrics.type_assessment_lineage_integrity == 0.0
    assert metrics.semantic_interpretation_coverage == 0.5
    assert metrics.semantic_decision_coverage == 0.5
    assert metrics.semantic_disposition_accuracy == 0.0
    assert metrics.belief_model_materialization_rate == 1.0
    assert metrics.belief_model_lineage_integrity == 0.0
    assert metrics.harmful_semantic_propagation_rate == 1.0
    assert report.by_batch["batch-b"].governed_fate_coverage == 0.0


@pytest.mark.asyncio
async def test_invalid_type_distribution_is_exposed_not_scored() -> None:
    observation_id = uuid4()
    row = _pipeline_row(
        observation_id=observation_id, surface="NBI",
        candidates=[_special("unknown", "unknown")],
        selected_id="unknown", selected_ref=None, current_fate="abstained",
        type_distribution={"customer": 0.8, "unknown": 0.8},
    )
    report = await evaluate_persisted_entity_pipeline(
        _FakeDb([row]), tenant_id=uuid4(),
        gold_cases=[GoldEntityPipelineCase(
            case_id="invalid-type", batch_id="batch",
            source_observation_id=observation_id, surface="NBI",
            gold_entity_type="customer", expected_detection_fate="detected",
        )],
        canonical_gold_labels={},
    )
    assert report.overall.invalid_type_assessment_count == 1
    assert report.overall.type_assessed_case_count == 0
    assert report.overall.type_assessment_coverage == 0.0
    assert report.overall.type_assessment_accuracy is None


def test_gold_manifest_loader_seals_cases_and_canonical_labels(tmp_path) -> None:
    observation_id = uuid4()
    manifest = tmp_path / "entity-pipeline-gold.json"
    manifest.write_text(json.dumps({
        "schema_version": "gold-entity-pipeline-corpus-v1",
        "cases": [{
            "case_id": "case-1", "batch_id": "batch-1",
            "source_observation_id": str(observation_id), "surface": "NBI",
            "gold_entity_type": "customer",
            "gold_canonical_label": "gold:nimbus",
            "expected_detection_fate": "detected",
            "acceptable_terminal_fates": ["resolved_for_consumer"],
        }],
        "canonical_gold_labels": {
            "customer:customer:nimbus:v1": "gold:nimbus",
        },
    }), encoding="utf-8")

    cases, labels = load_gold_manifest(manifest)

    assert cases[0].source_observation_id == observation_id
    assert cases[0].acceptable_terminal_fates == ("resolved_for_consumer",)
    assert labels == {"customer:customer:nimbus:v1": "gold:nimbus"}


def test_v2_gold_manifest_loads_versioned_topology_labels(tmp_path) -> None:
    observation_id, source_model_id, target_model_id = uuid4(), uuid4(), uuid4()
    manifest = tmp_path / "entity-pipeline-topology-gold.json"
    manifest.write_text(json.dumps({
        "schema_version": "gold-entity-pipeline-corpus-v2",
        "topology_population_version": "sealed-topology-population-v1",
        "cases": [{
            "case_id": "source", "batch_id": "batch-1",
            "source_observation_id": str(observation_id), "surface": "Promise P-1",
            "gold_entity_type": "commitment",
            "expected_relations": [{
                "expectation_id": "promise-to-customer",
                "expected_admission": True,
                "source_model_gold_label": "gold:promise-model",
                "target_model_gold_label": "gold:customer-model",
                "relation_type": "committed_to",
                "source_mention_case_ids": ["source"],
            }],
        }],
        "canonical_gold_labels": {},
        "topology_model_gold_labels": {
            str(source_model_id): "gold:promise-model",
            str(target_model_id): "gold:customer-model",
        },
    }), encoding="utf-8")

    cases, labels, topology = load_gold_manifest_bundle(manifest)

    assert cases[0].expected_relations[0].relation_type == "committed_to"
    assert labels == {}
    assert topology == {
        str(source_model_id): "gold:promise-model",
        str(target_model_id): "gold:customer-model",
    }


@pytest.mark.asyncio
async def test_no_admission_with_stray_model_fails_downstream_safety() -> None:
    observation_id = uuid4()
    row = _pipeline_row(
        observation_id=observation_id, surface="NBI",
        candidates=[_special("unknown", "unknown")],
        selected_id="unknown", selected_ref=None, current_fate="abstained",
        type_distribution={"customer": 0.7, "unknown": 0.3},
        semantic_disposition="no_admission",
    )
    row["semantic_interpretation_model_count"] = 1
    report = await evaluate_persisted_entity_pipeline(
        _FakeDb([row]), tenant_id=uuid4(),
        gold_cases=[GoldEntityPipelineCase(
            case_id="unsafe-no-admission", batch_id="batch",
            source_observation_id=observation_id, surface="NBI",
            gold_entity_type="customer", expected_detection_fate="detected",
            expected_semantic_disposition="no_admission",
        )],
        canonical_gold_labels={},
    )

    assert report.overall.semantic_disposition_accuracy == 1.0
    assert report.overall.no_admission_no_model_safety_rate == 0.0
    assert report.overall.harmful_semantic_propagation_rate == 1.0


def test_relation_topology_scores_admission_endpoints_direction_and_lineage() -> None:
    observation_ids = [uuid4() for _ in range(3)]
    source_model, target_model, poison_model = uuid4(), uuid4(), uuid4()
    correct_ref = {"type": "customer", "id": "customer:known", "version": 1}
    promise_ref = {"type": "commitment", "id": "commitment:p-1", "version": 1}
    wrong_ref = {"type": "customer", "id": "customer:wrong", "version": 1}
    first = _pipeline_row(
        observation_id=observation_ids[0], surface="Known Co",
        candidates=[_canonical("known", "customer", "customer:known")],
        selected_id="known", selected_ref=correct_ref,
        type_distribution={"customer": 0.9, "unknown": 0.1},
    )
    second = _pipeline_row(
        observation_id=observation_ids[1], surface="Promise P-1",
        candidates=[_canonical("promise", "commitment", "commitment:p-1")],
        selected_id="promise", selected_ref=promise_ref,
        type_distribution={"commitment": 0.8, "unknown": 0.2},
    )
    open_case = _pipeline_row(
        observation_id=observation_ids[2], surface="Unnamed venture",
        candidates=[_canonical("wrong", "customer", "customer:wrong")],
        selected_id="wrong", selected_ref=wrong_ref,
        type_distribution={"customer": 0.6, "unknown": 0.4},
    )
    good_relation_id, harmful_relation_id = uuid4(), uuid4()
    good_relation = {
        "id": good_relation_id, "source_model_id": source_model,
        "target_model_id": target_model, "edge_kind": "committed_to",
        "status": "active", "metadata": {"source_entity_mention_ids": [
            str(first["entity_mention_id"]), str(second["entity_mention_id"]),
        ]},
    }
    harmful_relation = {
        "id": harmful_relation_id, "source_model_id": poison_model,
        "target_model_id": target_model, "edge_kind": "supports",
        "status": "active", "metadata": {"source_entity_mention_id": str(
            open_case["entity_mention_id"]
        )},
    }
    # The same durable edge can be visible from multiple originating rows; the
    # evaluator must deduplicate it while preserving both mention origins.
    first["downstream_relations"] = [good_relation]
    second["downstream_relations"] = [good_relation]
    open_case["downstream_relations"] = [harmful_relation]
    cases = [
        GoldEntityPipelineCase(
            case_id="customer", batch_id="batch", source_observation_id=observation_ids[0],
            surface="Known Co", gold_entity_type="customer",
            gold_canonical_label="gold:known",
            expected_relations=(GoldRelationExpectation(
                expectation_id="expected-edge", expected_admission=True,
                source_model_gold_label="gold:promise-model",
                target_model_gold_label="gold:customer-model",
                relation_type="committed_to",
                source_mention_case_ids=("customer", "promise"),
            ),),
        ),
        GoldEntityPipelineCase(
            case_id="promise", batch_id="batch", source_observation_id=observation_ids[1],
            surface="Promise P-1", gold_entity_type="commitment",
            gold_canonical_label="gold:promise",
        ),
        GoldEntityPipelineCase(
            case_id="open", batch_id="batch", source_observation_id=observation_ids[2],
            surface="Unnamed venture", gold_entity_type="customer",
            expected_relations=(GoldRelationExpectation(
                expectation_id="must-not-propagate", expected_admission=False,
                source_mention_case_ids=("open",),
            ),),
        ),
    ]

    report = analyze_entity_pipeline_rows(
        gold_cases=cases,
        canonical_gold_labels={
            canonical_ref_key(correct_ref): "gold:known",
            canonical_ref_key(promise_ref): "gold:promise",
            canonical_ref_key(wrong_ref): "gold:wrong",
        },
        topology_model_gold_labels={
            str(source_model): "gold:promise-model",
            str(target_model): "gold:customer-model",
            str(poison_model): "gold:poison-model",
        },
        rows=[first, second, open_case],
    )

    metrics = report.overall
    assert metrics.relation_expectation_count == 2
    assert metrics.expected_relation_admission_count == 1
    assert metrics.observed_active_relation_count == 2
    assert metrics.relation_admission_accuracy == 0.5
    assert metrics.expected_relation_recall == 1.0
    assert metrics.relation_non_admission_safety_rate == 0.0
    assert metrics.relation_endpoint_accuracy == 1.0
    assert metrics.relation_type_accuracy == 1.0
    assert metrics.relation_direction_accuracy == 1.0
    assert metrics.relation_lineage_coverage == 1.0
    assert metrics.relation_lineage_integrity == 1.0
    assert metrics.unexpected_relation_rate == 0.5
    assert metrics.harmful_topology_relation_count == 1
    assert metrics.harmful_topology_model_count == 2
    assert metrics.harmful_topology_propagation_rate == 0.5
    assert metrics.unknown_topology_endpoint_count == 0


def test_relation_topology_exposes_reversed_edge_and_wrong_type() -> None:
    observation_id, source_model, target_model = uuid4(), uuid4(), uuid4()
    row = _pipeline_row(
        observation_id=observation_id, surface="Entity A",
        candidates=[_special("unknown", "unknown")],
        selected_id="unknown", selected_ref=None, current_fate="review",
    )
    row["downstream_relations"] = [{
        "id": uuid4(), "source_model_id": target_model,
        "target_model_id": source_model, "edge_kind": "contradicts",
        "status": "active", "metadata": {"source_entity_mention_id": str(
            row["entity_mention_id"]
        )},
    }]
    case = GoldEntityPipelineCase(
        case_id="entity-a", batch_id="batch", source_observation_id=observation_id,
        surface="Entity A", gold_entity_type="customer",
        expected_relations=(GoldRelationExpectation(
            expectation_id="directed", expected_admission=True,
            source_model_gold_label="gold:a", target_model_gold_label="gold:b",
            relation_type="depends_on", source_mention_case_ids=("entity-a",),
        ),),
    )

    metrics = analyze_entity_pipeline_rows(
        gold_cases=[case], canonical_gold_labels={}, rows=[row],
        topology_model_gold_labels={
            str(source_model): "gold:a", str(target_model): "gold:b",
        },
    ).overall

    assert metrics.relation_admission_accuracy == 0.0
    assert metrics.expected_relation_recall == 0.0
    assert metrics.relation_endpoint_accuracy == 1.0
    assert metrics.relation_direction_accuracy == 0.0
    assert metrics.relation_type_accuracy == 0.0
    assert metrics.relation_lineage_coverage == 0.0
    assert metrics.relation_lineage_integrity is None
    assert metrics.unexpected_relation_rate == 1.0
    assert metrics.harmful_topology_propagation_rate == 1.0


def test_relation_expectations_reject_unknown_mention_lineage() -> None:
    case = GoldEntityPipelineCase(
        case_id="known", batch_id="batch", source_observation_id=uuid4(),
        surface="Known", gold_entity_type="team",
        expected_relations=(GoldRelationExpectation(
            expectation_id="bad-lineage", expected_admission=False,
            source_mention_case_ids=("missing",),
        ),),
    )
    with pytest.raises(ValueError, match="unknown mention case"):
        analyze_entity_pipeline_rows(
            gold_cases=[case], canonical_gold_labels={}, rows=[],
        )


def test_shared_endpoint_adjacency_does_not_manufacture_relation_origin() -> None:
    observations = [uuid4(), uuid4()]
    shared_model, target_model, orphan_model = uuid4(), uuid4(), uuid4()
    unrelated = _pipeline_row(
        observation_id=observations[0], surface="Unrelated Entity",
        candidates=[_special("unknown", "unknown")],
        selected_id="unknown", selected_ref=None, current_fate="review",
    )
    actual_ref = {"type": "team", "id": "team:actual", "version": 1}
    actual_origin = _pipeline_row(
        observation_id=observations[1], surface="Actual Origin",
        candidates=[_canonical("actual", "team", "team:actual")],
        selected_id="actual", selected_ref=actual_ref,
    )
    lineaged_edge = {
        "id": uuid4(), "source_model_id": shared_model,
        "target_model_id": target_model, "edge_kind": "supports",
        "status": "active", "metadata": {"source_entity_mention_id": str(
            actual_origin["entity_mention_id"]
        )},
    }
    unlineaged_edge = {
        "id": uuid4(), "source_model_id": shared_model,
        "target_model_id": orphan_model, "edge_kind": "depends_on",
        "status": "active", "metadata": {},
    }
    # Both edges are discovered only because they are adjacent to the unrelated
    # row's admitted model. Their metadata, not this placement, controls origin.
    unrelated["downstream_relations"] = [lineaged_edge, unlineaged_edge]
    actual_origin["downstream_relations"] = []
    cases = [
        GoldEntityPipelineCase(
            case_id="unrelated", batch_id="batch",
            source_observation_id=observations[0], surface="Unrelated Entity",
            gold_entity_type="customer",
            expected_relations=(GoldRelationExpectation(
                expectation_id="unrelated-must-not-propagate",
                expected_admission=False,
                source_mention_case_ids=("unrelated",),
            ),),
        ),
        GoldEntityPipelineCase(
            case_id="actual", batch_id="batch",
            source_observation_id=observations[1], surface="Actual Origin",
            gold_entity_type="team", gold_canonical_label="gold:actual",
            expected_relations=(GoldRelationExpectation(
                expectation_id="actual-edge", expected_admission=True,
                source_model_gold_label="gold:shared",
                target_model_gold_label="gold:target", relation_type="supports",
                source_mention_case_ids=("actual",),
            ),),
        ),
    ]

    metrics = analyze_entity_pipeline_rows(
        gold_cases=cases,
        canonical_gold_labels={canonical_ref_key(actual_ref): "gold:actual"},
        rows=[unrelated, actual_origin],
        topology_model_gold_labels={
            str(shared_model): "gold:shared", str(target_model): "gold:target",
            str(orphan_model): "gold:orphan",
        },
    ).overall

    assert metrics.observed_active_relation_count == 2
    assert metrics.relation_admission_accuracy == 1.0
    assert metrics.expected_relation_recall == 1.0
    assert metrics.relation_non_admission_safety_rate == 1.0
    assert metrics.relation_lineage_integrity == 1.0
    assert metrics.unlineaged_active_relation_count == 1
    assert metrics.unlineaged_active_relation_rate == 0.5
    assert metrics.unexpected_relation_rate == 0.5
    assert metrics.harmful_topology_relation_count == 1
