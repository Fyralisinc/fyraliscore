from __future__ import annotations

from uuid import uuid4

from lib.evaluation.epistemic_repair.p6_postfreeze_evidence import (
    _detected_mention_evidence,
    _mention_grounding_continuity,
)


def _row(*, trace_fate: str | None, provisional_fate: str | None = None) -> dict:
    return {
        "id": uuid4(),
        "candidate_surface": "Facilities",
        "fate": "detected",
        "grounding_fate": trace_fate,
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


def test_detected_mention_keeps_trace_grounding_distinct_from_detection() -> None:
    mention = _detected_mention_evidence(
        _row(trace_fate="resolved_for_consumer"), signal_id="p6-b01-s21",
    )

    assert mention["detection_fate"] == "detected"
    assert mention["grounding_fate"] == "resolved_for_consumer"
    assert mention["grounding_stage"] == "trace_recorded"


def test_provisional_grounding_fate_is_explicit_without_a_trace() -> None:
    mention = _detected_mention_evidence(
        _row(trace_fate=None, provisional_fate="extracted_unresolved"),
        signal_id="p6-b01-s21",
    )

    assert mention["detection_fate"] == "detected"
    assert mention["grounding_fate"] == "extracted_unresolved"
    assert mention["grounding_stage"] == "provisional_detection"


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
