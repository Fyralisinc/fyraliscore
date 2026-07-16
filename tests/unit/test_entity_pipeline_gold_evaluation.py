from __future__ import annotations

import json
import math
from uuid import uuid4

import pytest

from lib.evaluation.entity_pipeline_gold import (
    GoldEntityPipelineCase,
    canonical_ref_key,
    evaluate_persisted_entity_pipeline,
)
from scripts.evaluate_entity_pipeline_gold import load_gold_manifest


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
):
    (
        context_id, detection_id, type_assessment_id, request_id,
        set_id, assessment_id, admission_id, trace_id,
    ) = (uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), uuid4())
    type_distribution = type_distribution or {"unknown": 1.0}
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
        "detection_fate": "detected",
        "context_snapshot_id": context_id,
        "detection_command": {
            "detection": {"entity_type_assessment": {
                "assessment_id": str(type_assessment_id),
                "type_distribution": type_distribution,
            }}
        },
        "candidate_request_id": request_id,
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
        ),
        # Structured Jira source field provides a candidate without an alias.
        _pipeline_row(
            observation_id=observation_ids[1], surface="PAY-42",
            candidates=[_canonical("jira", "workstream", "workstream:PAY-42"), *common_specials],
            selected_id="jira", selected_ref=jira_ref,
            type_distribution={"workstream": 0.8, "unknown": 0.2},
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
    assert report.by_batch["batch-25-signals"] == metrics
    assert report.uncertainties == (
        "canonical_metrics_exclude_open_world_gold_cases",
        "terminal_fate_accuracy_excludes_unlabeled_cases",
        "downstream_relation_and_model_impact_not_evaluated",
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
    )
    row["candidate_request"] = {"entity_type_assessment_refs": [str(uuid4())]}
    cases = [
        GoldEntityPipelineCase(
            case_id="wrong-link", batch_id="batch-a",
            source_observation_id=observation_ids[0], surface="NBI",
            gold_entity_type="customer", gold_canonical_label="gold:nimbus",
            expected_detection_fate="detected",
            acceptable_terminal_fates=("review", "unresolved", "abstained"),
        ),
        GoldEntityPipelineCase(
            case_id="missing-fate", batch_id="batch-b",
            source_observation_id=observation_ids[1], surface="Zephyr",
            gold_entity_type="workstream", expected_detection_fate="detected",
            acceptable_terminal_fates=("review", "unresolved", "abstained"),
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
