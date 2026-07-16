from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from lib.architecture_registry import load_architecture_registry
from lib.evaluation.closed_loop import (
    ClosedLoopEvaluationScope,
    ClosedLoopEvaluationState,
    build_closed_loop_invariant_evidence,
)
from lib.evaluation.proof import EvidenceTier
from lib.shared.ids import uuid7


REGISTRY = Path(__file__).resolve().parents[2] / "architecture/registry.yaml"


def _state(
    *,
    episode_count: int,
    complete_episode_count: int,
    component_violations: dict[str, int] | None = None,
) -> ClosedLoopEvaluationState:
    start = datetime(2026, 7, 16, tzinfo=timezone.utc)
    return ClosedLoopEvaluationState(
        scope=ClosedLoopEvaluationScope(
            tenant_id=uuid7(),
            start=start,
            end=start + timedelta(hours=1),
            run_id="pytest-closed-loop-evidence",
        ),
        episode_count=episode_count,
        complete_episode_count=complete_episode_count,
        closed_loop_completion_rate=(
            complete_episode_count / episode_count if episode_count else None
        ),
        stage_coverage_rates={},
        continuity_rates={},
        component_violation_counts=component_violations or {},
        component_key_rates={},
        incident_counts={},
        episode_reports=(),
        uncertainty=("test uncertainty",),
        artifact_refs=("pytest://closed-loop-evidence",),
    )


def test_zero_exposure_cannot_claim_e3_closed_loop_evidence() -> None:
    evidence = build_closed_loop_invariant_evidence(
        _state(episode_count=0, complete_episode_count=0),
        registry=load_architecture_registry(REGISTRY),
        executed_scenario_ids=frozenset(),
    )[0]

    assert evidence.applicable_exposures == 0
    assert evidence.achieved_evidence_tier is EvidenceTier.E0
    assert evidence.observed_trace_facts == frozenset()
    assert evidence.metric_observations[0].point_estimate is None


def test_component_violations_are_visible_in_joined_evidence() -> None:
    evidence = build_closed_loop_invariant_evidence(
        _state(
            episode_count=1,
            complete_episode_count=1,
            component_violations={"execution": 2},
        ),
        registry=load_architecture_registry(REGISTRY),
        executed_scenario_ids=frozenset(),
    )[0]

    assert evidence.achieved_evidence_tier is EvidenceTier.E3
    assert evidence.metric_observations[0].violation_count == 2
    assert [incident.incident_class for incident in evidence.incidents] == [
        "component:execution"
    ]
