from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from lib.architecture_registry import load_architecture_registry
from lib.evaluation.source_semantics import (
    SourceSemanticEvaluationScope,
    analyze_source_semantic_rows,
    build_source_semantic_invariant_evidence,
    render_source_semantic_markdown,
)


NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[2]


def _scope():
    return SourceSemanticEvaluationScope(
        tenant_id=uuid4(),
        start=NOW - timedelta(minutes=1),
        end=NOW + timedelta(minutes=1),
        run_id="source-semantic-fixture",
    )


def _row(*, applied: bool, text: str = "NBI is blocked"):
    observation_id = uuid4()
    trace_id = uuid4()
    assessment_id = uuid4()
    grounding_admission_id = uuid4()
    interpretation_id = uuid4()
    model_id = uuid4() if applied else None
    referent = {"type": "customer", "id": "customer-nimbus", "version": 1}
    grounded_referent = {
        "referent_id": "customer-nimbus",
        "referent_version": 1,
    }
    mention_id = uuid4()
    assertion_id = f"source-assertion:{uuid4()}"
    source_ref = f"observation:{observation_id}"
    mention = {
        "mention_id": str(mention_id),
        "mention_version": 1,
        "primary_anchor": {
            "anchor_id": f"mention-anchor:{mention_id}",
            "kind": "explicit",
            "surface_form": "NBI",
            "coordinate": {
                "evidence_record_id": source_ref,
                "source_object_id": source_ref,
                "source_revision": f"{source_ref}:v1",
                "field_path": "content_text",
                "span_start": 0,
                "span_end": 3,
            },
        },
    }
    continuity = {
        "downstream_object_ref": (
            f"model:{model_id}"
            if applied
            else f"source-semantic-interpretation:{interpretation_id}"
        ),
        "mention_ref": f"mention:{mention_id}:v1",
        "mention_version": 1,
        "resolution_assessment_ref": f"resolution-assessment:{assessment_id}",
        "resolution_assessment_version": 1,
        "grounding_admission_ref": f"grounding-admission:{grounding_admission_id}",
        "grounding_admission_version": 1,
        "selected_referent": grounded_referent,
    }
    proposition = {
        "kind": "belief",
        "source_semantic_interpretation_id": str(interpretation_id),
        "grounding_continuity": continuity,
    }
    return {
        "grounding_trace_id": trace_id,
        "source_observation_id": observation_id,
        "current_fate": "resolved_for_consumer",
        "resolution_assessment_id": assessment_id,
        "selected_referent": referent,
        "content_text": text,
        "mention_ref": f"mention:{mention_id}:v1",
        "mention": mention,
        "interpretation_id": interpretation_id,
        "interpretation_grounding_admission_id": grounding_admission_id,
        "source_assertion": {
            "assertion_id": assertion_id,
            "coordinates": [
                {
                    "evidence_record_id": source_ref,
                    "source_object_id": source_ref,
                    "source_revision": f"{source_ref}:v1",
                    "field_path": "content_text",
                    "span_start": 0,
                    "span_end": len(text),
                }
            ],
            "current_speaker_or_author": "slack:U-NORTHSTAR",
            "kind": "asserted" if applied else "asked",
            "expressed_content": text,
            "uncertainty": 0.1,
            "extractor_version": "fixture-v1",
        },
        "semantic_frame": {
            "source_assertion_id": assertion_id,
            "predicate_or_event_type": "blocked",
            "arguments": [{"role": "subject"}],
            "negated": False,
            "modality": "actual",
            "confidence": 0.9,
        },
        "speech_act": {
            "source_assertion_id": assertion_id,
            "distribution": {"report" if applied else "question": 1.0},
        },
        "grounding_continuity": continuity,
        "disposition": "belief_applied" if applied else "no_admission",
        "admitted_model_id": model_id,
        "interpretation_grounding_assessment_id": assessment_id,
        "interpretation_grounding_consumer": (
            "epistemic-applier" if applied else "observation-grounding-sidecar"
        ),
        "interpretation_grounding_purpose": (
            "belief-admission" if applied else "company-physics-grounding"
        ),
        "interpretation_grounding_operation": (
            "create-grounded-belief"
            if applied
            else "consume-resolution-assessment"
        ),
        "interpretation_grounding_admission": {
            "selected_referent": grounded_referent,
        },
        "model_id": model_id,
        "model_born_from_event_id": observation_id if applied else None,
        "model_proposition": proposition if applied else None,
        "model_scope_entities": [referent] if applied else None,
        "interpretation_model_count": 1 if applied else 0,
    }


def test_clean_rows_report_continuous_full_core_closure() -> None:
    applied = _row(applied=True)
    question = _row(applied=False, text="Is NBI blocked?")
    state = analyze_source_semantic_rows(
        scope=_scope(),
        rows=(applied, question),
        artifact_refs=("pytest://source-semantic-clean",),
    )

    assert state.eligible_grounding_interpretation_coverage == 1.0
    assert state.source_coordinate_reconstructability_rate == 1.0
    assert state.interpretation_structural_closure_rate == 1.0
    assert state.grounding_continuity_exactness_rate == 1.0
    assert state.explicit_admission_fate_coverage == 1.0
    assert state.supported_report_admission_precision == 1.0
    assert state.supported_report_admission_recall == 1.0
    assert state.epistemic_consumer_admission_continuity_rate == 1.0
    assert state.model_dependency_closure_rate == 1.0
    assert state.non_admitted_no_model_safety_rate == 1.0
    assert state.decision_fate_counts == {
        "belief_applied": 1,
        "no_admission": 1,
    }
    assert state.incident_counts == {}


def test_zero_exposure_is_unknown_not_perfect() -> None:
    state = analyze_source_semantic_rows(
        scope=_scope(),
        rows=(),
        artifact_refs=("pytest://source-semantic-empty",),
    )

    assert state.eligible_grounding_interpretation_coverage is None
    assert state.supported_report_admission_precision is None
    assert state.model_dependency_closure_rate is None
    assert state.non_admitted_no_model_safety_rate is None
    assert "unknown/not exposed" in render_source_semantic_markdown(state)


def test_tampering_is_localized_to_trace_linked_incidents() -> None:
    broken = deepcopy(_row(applied=True))
    broken["source_assertion"]["coordinates"][0]["span_end"] = 2
    broken["semantic_frame"]["source_assertion_id"] = "wrong"
    broken["grounding_continuity"]["grounding_admission_ref"] = "wrong"
    broken["interpretation_grounding_consumer"] = "wrong-consumer"
    broken["model_id"] = None
    broken["model_born_from_event_id"] = None
    broken["model_proposition"] = None
    broken["model_scope_entities"] = None
    broken["interpretation_model_count"] = 0

    state = analyze_source_semantic_rows(
        scope=_scope(),
        rows=(broken,),
        artifact_refs=("pytest://source-semantic-tampered",),
    )

    assert state.source_coordinate_reconstructability_rate == 0.0
    assert state.interpretation_structural_closure_rate == 0.0
    assert state.grounding_continuity_exactness_rate == 0.0
    assert state.epistemic_consumer_admission_continuity_rate == 0.0
    assert state.applied_decision_model_coverage == 0.0
    assert state.model_dependency_closure_rate == 0.0
    assert "source_coordinate_not_reconstructable" in state.incident_counts
    assert "source_semantic_structure_not_closed" in state.incident_counts
    assert "grounding_continuity_mismatch" in state.incident_counts
    assert "epistemic_consumer_admission_discontinuity" in state.incident_counts
    trace_ref = f"grounding-trace:{broken['grounding_trace_id']}"
    assert trace_ref in state.incident_trace_refs[
        "model_grounding_dependency_discontinuity"
    ]


def test_state_projects_to_inv_26_evidence_without_claiming_full_e4() -> None:
    state = analyze_source_semantic_rows(
        scope=_scope(),
        rows=(_row(applied=True),),
        artifact_refs=("pytest://source-semantic-evidence",),
    )
    registry = load_architecture_registry(ROOT / "architecture/registry.yaml")

    evidence = build_source_semantic_invariant_evidence(
        state,
        registry=registry,
        executed_scenario_ids=frozenset({"SOURCE-SEMANTICS"}),
    )

    assert len(evidence) == 1
    assert evidence[0].invariant_id == "INV-26"
    assert evidence[0].achieved_evidence_tier.value == "E3"
    assert evidence[0].denominator.complete is True
    assert "negation_modality_condition_quantity_and_time" not in (
        evidence[0].observed_trace_facts
    )


def test_missing_interpretation_remains_unknown_in_evidence_denominator() -> None:
    missing = _row(applied=True)
    missing["interpretation_id"] = None
    state = analyze_source_semantic_rows(
        scope=_scope(),
        rows=(missing,),
        artifact_refs=("pytest://source-semantic-missing",),
    )
    registry = load_architecture_registry(ROOT / "architecture/registry.yaml")

    evidence = build_source_semantic_invariant_evidence(
        state,
        registry=registry,
        executed_scenario_ids=frozenset(),
    )

    denominator = evidence[0].denominator
    assert denominator.eligible == 1
    assert denominator.attempted_or_committed == 1
    assert denominator.known_fate_count == 0
    assert denominator.complete is False
