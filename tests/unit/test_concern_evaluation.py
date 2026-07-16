from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from lib.architecture_registry import load_architecture_registry
from lib.evaluation.concern import (
    ConcernEvaluationScope,
    analyze_concern_rows,
    build_concern_invariant_evidence,
)
from lib.evaluation.proof import EvidenceTier


ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)


def _scope() -> ConcernEvaluationScope:
    return ConcernEvaluationScope(
        tenant_id=uuid4(),
        start=NOW - timedelta(hours=1),
        end=NOW,
        run_id="concern-eval-unit",
    )


def test_empty_concern_population_is_unknown_coverage_not_an_incident() -> None:
    state = analyze_concern_rows(
        scope=_scope(),
        bindings=(),
        heads=(),
        versions=(),
        commands=(),
        corrections=(),
        artifact_refs=("pytest:concern-empty",),
    )
    assert state.incident_counts == {}
    assert state.reducer_conformance_rate == 1.0
    registry = load_architecture_registry(ROOT / "architecture/registry.yaml")
    evidence = build_concern_invariant_evidence(
        state,
        registry=registry,
        executed_scenario_ids=frozenset(),
    )
    assert {item.invariant_id for item in evidence} == {"INV-20", "INV-23", "INV-37"}
    assert all(item.achieved_evidence_tier is EvidenceTier.E3 for item in evidence)
    assert all(item.metric_observations[0].point_estimate is None for item in evidence)


def test_concern_evaluator_localizes_corrupt_protocol_and_successor() -> None:
    concern_id = uuid4()
    successor_id = uuid4()
    command_id = uuid4()
    state = analyze_concern_rows(
        scope=_scope(),
        bindings=(
            {
                "binding_ref": "attention-binding:bad:v1",
                "binding": {},
                "binding_digest": "0" * 64,
                "attention_source_ref": "goal:bad",
            },
        ),
        heads=(
            {
                "concern_id": concern_id,
                "dedupe_key": "a" * 64,
                "current_version": 1,
                "current_state": "open",
                "predecessor_concern_id": None,
                "successor_concern_id": successor_id,
            },
            {
                "concern_id": successor_id,
                "dedupe_key": "b" * 64,
                "current_version": 1,
                "current_state": "candidate",
                "predecessor_concern_id": None,
                "successor_concern_id": None,
            },
        ),
        versions=(
            {
                "concern_id": concern_id,
                "aggregate_version": 1,
                "state": "open",
                "snapshot": {},
                "snapshot_digest": "0" * 64,
                "effective_binding_envelope": {},
                "effective_binding_digest": "1" * 64,
                "transition": {},
                "transitioned_at": NOW - timedelta(minutes=1),
                "command_kind": "evaluate",
                "event_count": 0,
                "outbox_count": 0,
            },
        ),
        commands=(
            {
                "id": command_id,
                "command_kind": "evaluate",
                "command": {},
                "request_digest": "2" * 64,
                "processing_authority_fingerprint": "3" * 64,
                "consumption_authority_fingerprint": "4" * 64,
                "version_count": 0,
                "event_count": 0,
                "outbox_count": 0,
            },
        ),
        corrections=(
            {
                "predecessor_concern_id": concern_id,
                "successor_concern_id": successor_id,
            },
        ),
        artifact_refs=("pytest:concern-corrupt",),
    )
    assert state.incident_counts["invalid_attention_binding_contract"] == 1
    assert state.incident_counts["invalid_concern_snapshot"] == 1
    assert state.incident_counts["unreconstructable_concern_command"] == 1
    assert state.incident_counts["concern_command_without_versions"] == 1
    assert state.incident_counts["concern_identity_correction_not_reciprocal"] == 1
