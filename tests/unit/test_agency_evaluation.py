from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from lib.architecture_registry import load_architecture_registry
from lib.evaluation.agency import (
    AgencyEvaluationScope,
    analyze_agency_rows,
    build_agency_invariant_evidence,
)
from lib.evaluation.proof import EvidenceTier


ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)
IMMUTABLE_TABLES = {
    "consequential_intervention_specs",
    "consequential_predictions",
    "consequential_authorization_decisions",
    "consequential_outcomes",
    "consequential_settlements",
    "consequential_attributions",
}


def _scope() -> AgencyEvaluationScope:
    return AgencyEvaluationScope(
        tenant_id=uuid4(),
        start=NOW - timedelta(hours=1),
        end=NOW,
        run_id="agency-eval-unit",
    )


def test_empty_agency_population_is_unknown_e3_not_successful_full_system_proof() -> (
    None
):
    state = analyze_agency_rows(
        scope=_scope(),
        proposals=(),
        specs=(),
        predictions=(),
        authorizations=(),
        outcomes=(),
        settlements=(),
        attributions=(),
        episodes=(),
        commands=(),
        guarded_tables=IMMUTABLE_TABLES,
        artifact_refs=("pytest:agency-empty",),
    )
    assert state.incident_counts == {}
    assert state.prediction_preregistration_rate == 1.0
    registry = load_architecture_registry(ROOT / "architecture/registry.yaml")
    evidence = build_agency_invariant_evidence(
        state,
        registry=registry,
        executed_scenario_ids=frozenset(),
    )
    assert {item.invariant_id for item in evidence} == {
        "INV-09",
        "INV-10",
        "INV-11",
        "INV-16",
        "INV-22",
    }
    assert all(item.achieved_evidence_tier is EvidenceTier.E3 for item in evidence)
    assert all(item.metric_observations[0].point_estimate is None for item in evidence)


def test_agency_evaluator_localizes_orphan_manifest_command_and_storage_failures() -> (
    None
):
    object_id = uuid4()
    state = analyze_agency_rows(
        scope=_scope(),
        proposals=(),
        specs=(
            {
                "spec_id": object_id,
                "proposal_count": 0,
            },
        ),
        predictions=(),
        authorizations=(),
        outcomes=(),
        settlements=(),
        attributions=(),
        episodes=(
            {
                "episode_id": object_id,
                "episode": {},
                "episode_digest": "0" * 64,
            },
        ),
        commands=(
            {
                "command_kind": "register_prediction",
                "command": {},
                "request_digest": "1" * 64,
                "processing_authority_fingerprint": "2" * 64,
                "writer_scope_id": "prediction:test",
                "writer_epoch": 1,
                "event_count": 0,
                "outbox_count": 0,
            },
        ),
        guarded_tables=set(),
        artifact_refs=("pytest:agency-corrupt",),
    )
    assert state.incident_counts["orphan_intervention_spec"] == 1
    assert state.incident_counts["incomplete_or_invalid_episode_manifest"] == 1
    assert state.incident_counts["unreconstructable_agency_command"] == 1
    assert state.incident_counts["agency_command_without_event"] == 1
    assert state.incident_counts["unguarded_immutable_agency_table"] == 6
