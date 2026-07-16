from __future__ import annotations

from uuid import uuid4

import pytest

from lib.evaluation.entity_pipeline_gold import (
    GoldEntityPipelineCase,
    canonical_ref_key,
    evaluate_persisted_entity_pipeline,
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
    current_fate="resolved_for_consumer",
):
    detection_id, request_id, set_id, assessment_id, trace_id = (
        uuid4(), uuid4(), uuid4(), uuid4(), uuid4()
    )
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
        "candidate_request_id": request_id,
        "candidate_set_id": set_id,
        "candidates": candidates,
        "assessment_id": assessment_id,
        "candidate_distribution": distribution,
        "selected_candidate_id": selected_id,
        "trace_id": trace_id,
        "current_fate": current_fate,
        "selected_referent": selected_ref,
        "trace": {
            "mention_detection": {"id": str(detection_id)},
            "candidate_request": {"id": str(request_id)},
            "candidate_set": {"id": str(set_id)},
            "assessment": {"id": str(assessment_id)},
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
        ),
        # Structured Jira source field provides a candidate without an alias.
        _pipeline_row(
            observation_id=observation_ids[1], surface="PAY-42",
            candidates=[_canonical("jira", "workstream", "workstream:PAY-42"), *common_specials],
            selected_id="jira", selected_ref=jira_ref,
        ),
        # Open-world phrase safely abstains; no link accuracy is manufactured.
        _pipeline_row(
            observation_id=observation_ids[2], surface="Project Zephyr",
            candidates=[*common_specials], selected_id="unknown", selected_ref=None,
            current_fate="abstained",
        ),
        # Homonymous candidates remain in review rather than being called correct.
        _pipeline_row(
            observation_id=observation_ids[3], surface="Alex",
            candidates=[
                _canonical("alex-r", "person", "person:alex-r"),
                _canonical("alex-s", "person", "person:alex-s"),
                *common_specials,
            ], selected_id="alex-r", selected_ref=None, current_fate="review",
        ),
        # Rejected detections terminate before candidate generation.
        {
            "source_observation_id": observation_ids[4],
            "candidate_surface": "Friday", "detection_id": uuid4(),
            "detection_fate": "rejected_not_entity", "candidate_request_id": None,
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
    assert set(db.calls[0][1][1]) == set(observation_ids)
    metrics = report.overall
    assert metrics.gold_case_count == 5
    assert metrics.detected_case_count == 4
    assert metrics.candidate_population_count == 4
    assert metrics.assessed_case_count == 4
    assert metrics.terminal_case_count == 5
    assert metrics.candidate_recall_at_k == {1: 1.0, 3: 1.0}
    assert metrics.gold_type_present_at_k == {1: 0.75, 3: 0.75}
    assert metrics.selected_type_accuracy == 1.0
    assert metrics.canonical_link_accuracy == 1.0
    assert metrics.canonical_link_coverage == pytest.approx(2 / 3)
    assert metrics.abstention_rate == 0.25
    assert metrics.review_rate == 0.25
    assert metrics.detection_to_terminal_coverage == 1.0
    assert metrics.lineage_integrity == 1.0
    assert metrics.rejected_detection_candidate_count == 0
    assert metrics.unknown_canonical_ref_count == 0
    assert report.by_batch["batch-25-signals"] == metrics
    assert report.uncertainties == ("canonical_metrics_exclude_open_world_gold_cases",)


@pytest.mark.asyncio
async def test_rejected_detection_with_candidate_population_is_reported_not_scored() -> None:
    tenant_id, observation_id = uuid4(), uuid4()
    row = {
        "source_observation_id": observation_id, "candidate_surface": "Friday",
        "detection_id": uuid4(), "detection_fate": "rejected_not_entity",
        "candidate_request_id": uuid4(), "candidate_set_id": uuid4(),
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
