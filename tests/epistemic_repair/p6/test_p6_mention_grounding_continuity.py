from __future__ import annotations

from uuid import uuid4
from uuid import NAMESPACE_URL, uuid5

import pytest

from lib.evaluation.epistemic_repair.p6_postfreeze_evidence import (
    _detected_mention_evidence,
    _mention_grounding_continuity,
    extract_p6_postfreeze_evidence,
)


def _row(
    *, trace_fate: str | None, provisional_fate: str | None = None,
    work_status: str | None = None, work_fate: dict | None = None,
) -> dict:
    return {
        "id": uuid4(),
        "candidate_surface": "Facilities",
        "fate": "detected",
        "grounding_fate": trace_fate,
        "grounding_work_status": work_status,
        "grounding_work_fate": work_fate,
        "selected_referent": {},
        "mention": {
            "surface": "Facilities",
            "grounding_fate": provisional_fate,
            "primary_anchor": {
                "coordinate": {"span_start": 8, "span_end": 18},
            },
        },
    }


def test_detected_mention_without_trace_exposes_incomplete_grounding_stage() -> None:
    mention = _detected_mention_evidence(
        _row(trace_fate=None), signal_id="p6-b01-s21",
    )

    assert mention["detection_fate"] == "detected"
    assert mention["grounding_fate"] is None
    assert mention["grounding_stage"] == "not_started"
    assert mention["grounding_complete"] is False


def test_detected_mention_keeps_trace_grounding_distinct_from_detection() -> None:
    mention = _detected_mention_evidence(
        _row(trace_fate="resolved_for_consumer"), signal_id="p6-b01-s21",
    )

    assert mention["detection_fate"] == "detected"
    assert mention["grounding_fate"] == "resolved_for_consumer"
    assert mention["grounding_stage"] == "trace_recorded"
    assert mention["grounding_complete"] is True


def test_provisional_grounding_fate_is_explicit_without_a_trace() -> None:
    mention = _detected_mention_evidence(
        _row(trace_fate=None, provisional_fate="extracted_unresolved"),
        signal_id="p6-b01-s21",
    )

    assert mention["detection_fate"] == "detected"
    assert mention["grounding_fate"] == "extracted_unresolved"
    assert mention["grounding_stage"] == "provisional_detection"
    assert mention["grounding_complete"] is True


def test_pending_work_is_scheduled_but_not_complete() -> None:
    mention = _detected_mention_evidence(
        _row(
            trace_fate=None,
            work_status="pending",
            work_fate={"fate_kind": "pending_grounding", "terminal": False},
        ),
        signal_id="p6-b01-s21",
    )

    assert mention["detection_fate"] == "detected"
    assert mention["grounding_fate"] == "pending_grounding"
    assert mention["grounding_stage"] == "work_scheduled"
    assert mention["grounding_complete"] is False


def test_cf3a_continuity_gate_fails_with_exact_incomplete_member() -> None:
    missing = _detected_mention_evidence(
        _row(trace_fate=None), signal_id="p6-b01-s21",
    )
    complete = _detected_mention_evidence(
        _row(trace_fate="unresolved"), signal_id="p6-b01-s01",
    )

    gate, report = _mention_grounding_continuity([complete, missing])

    assert gate is False
    assert report["detected"] == 2
    assert report["complete"] == 1
    assert report["incomplete"] == 1
    assert report["incomplete_mentions"] == [{
        "id": missing["id"],
        "signal_id": "p6-b01-s21",
        "surface": "Facilities",
    }]


class _ExtractorConnection:
    def __init__(self, *, tenant_id, observation_id) -> None:
        self.tenant_id = tenant_id
        self.observation_id = observation_id

    async def fetch(self, query, *_args):
        if "FROM observations WHERE" in query:
            return [{
                "id": self.observation_id,
                "occurred_at": "2026-07-18T00:00:00+00:00",
                "source_channel": "slack:message",
                "content_text": "Facilities",
                "entities_mentioned": [],
            }]
        if "FROM entity_mention_detections detection" in query:
            return [{
                **_row(
                    trace_fate=None,
                    work_status="pending",
                    work_fate={
                        "fate_kind": "pending_grounding", "terminal": False,
                    },
                ),
                "source_observation_id": self.observation_id,
            }]
        return []

    async def fetchrow(self, query, *_args):
        assert "accepted_models_without_evidence" in query
        return {
            "accepted_models_without_evidence": 0,
            "accepted_relations_without_participants": 0,
            "open_truth_repair_obligations": 0,
            "pending_truth_triggers": 0,
        }


@pytest.mark.asyncio
async def test_extractor_reports_pending_work_without_masking_it_as_complete() -> None:
    tenant_id = uuid4()
    signal_id = "p6-b01-s21"
    observation_id = uuid5(
        NAMESPACE_URL, f"p6-think:{tenant_id}:{signal_id}",
    )
    evidence = await extract_p6_postfreeze_evidence(
        _ExtractorConnection(tenant_id=tenant_id, observation_id=observation_id),
        tenant_id=tenant_id,
        signal_ids=(signal_id,),
    )

    mention = evidence["mentions"][0]
    assert mention["grounding_fate"] == "pending_grounding"
    assert mention["grounding_stage"] == "work_scheduled"
    assert mention["grounding_complete"] is False
    assert evidence["mention_grounding_continuity"]["incomplete"] == 1
    assert not evidence["hg_gates"][
        "complete_detected_mention_grounding_continuity"
    ]
