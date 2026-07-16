from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from lib.architecture_registry import load_architecture_registry
from lib.evaluation.closed_loop import (
    ClosedLoopEvaluationScope,
    ClosedLoopEvaluationState,
    _summarize_activation_work_rows,
    _summarize_manifest_work_rows,
    _summarize_scheduling_work_rows,
    build_closed_loop_invariant_evidence,
    render_closed_loop_markdown,
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
        manifest_work_item_count=0,
        manifest_work_applied_count=0,
        manifest_work_incomplete_count=0,
        manifest_work_terminal_failure_count=0,
        manifest_work_completion_rate=None,
        manifest_work_fate_counts={},
        activation_work_item_count=0,
        activation_work_activated_count=0,
        activation_work_incomplete_count=0,
        activation_work_authorization_expired_count=0,
        activation_work_terminal_failure_count=0,
        activation_work_completion_rate=None,
        activation_work_fate_counts={},
        scheduling_work_item_count=0,
        scheduling_work_leased_count=0,
        scheduling_work_incomplete_count=0,
        scheduling_work_backlog_count=0,
        scheduling_work_work_expired_count=0,
        scheduling_work_authorization_expired_count=0,
        scheduling_work_terminal_failure_count=0,
        scheduling_work_completion_rate=None,
        scheduling_work_fate_counts={},
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


def test_manifest_queue_incidents_remain_visible_without_downgrading_e3() -> None:
    state = _state(episode_count=1, complete_episode_count=1).model_copy(
        update={
            "manifest_work_item_count": 4,
            "manifest_work_applied_count": 1,
            "manifest_work_incomplete_count": 2,
            "manifest_work_terminal_failure_count": 1,
            "manifest_work_completion_rate": 0.25,
            "manifest_work_fate_counts": {
                "applied": 1,
                "failed_terminal": 1,
                "pending": 1,
                "retry_scheduled": 1,
            },
            "incident_counts": {
                "episode_manifest_work_failed_terminal": 1,
                "episode_manifest_work_incomplete": 2,
            },
        }
    )

    evidence = build_closed_loop_invariant_evidence(
        state,
        registry=load_architecture_registry(REGISTRY),
        executed_scenario_ids=frozenset(),
    )[0]

    assert evidence.achieved_evidence_tier is EvidenceTier.E3
    assert evidence.metric_observations[0].violation_count == 3
    assert {incident.incident_class for incident in evidence.incidents} == {
        "episode_manifest_work_failed_terminal",
        "episode_manifest_work_incomplete",
    }
    by_class = {incident.incident_class: incident for incident in evidence.incidents}
    assert (
        by_class["episode_manifest_work_failed_terminal"].summary
        == "Observed 1 terminal episode-manifest work failures."
    )
    assert by_class["episode_manifest_work_failed_terminal"].severity == 5


def test_manifest_queue_fates_are_continuous_and_unknown_fates_are_rejected() -> None:
    summary = _summarize_manifest_work_rows(
        [
            {"status": "pending"},
            {"status": "processing"},
            {"status": "retry_scheduled"},
            {"status": "applied"},
            {"status": "applied"},
            {"status": "failed_terminal"},
        ]
    )

    assert summary == (
        6,
        2,
        3,
        1,
        {
            "applied": 2,
            "failed_terminal": 1,
            "pending": 1,
            "processing": 1,
            "retry_scheduled": 1,
        },
    )

    with pytest.raises(ValueError, match="unknown InterventionEpisode"):
        _summarize_manifest_work_rows([{"status": "silently_dropped"}])


def test_activation_queue_fates_are_continuous_and_unknown_fates_are_rejected() -> None:
    summary = _summarize_activation_work_rows(
        [
            {"status": "pending"},
            {"status": "processing"},
            {"status": "retry_scheduled"},
            {"status": "activated"},
            {"status": "activated"},
            {"status": "authorization_expired"},
            {"status": "failed_terminal"},
        ]
    )

    assert summary == (
        7,
        2,
        3,
        1,
        1,
        {
            "activated": 2,
            "authorization_expired": 1,
            "failed_terminal": 1,
            "pending": 1,
            "processing": 1,
            "retry_scheduled": 1,
        },
    )

    with pytest.raises(ValueError, match="unknown authorized agency activation"):
        _summarize_activation_work_rows([{"status": "silently_dropped"}])


def test_activation_queue_incidents_remain_visible_without_downgrading_e3() -> None:
    state = _state(episode_count=1, complete_episode_count=1).model_copy(
        update={
            "activation_work_item_count": 6,
            "activation_work_activated_count": 2,
            "activation_work_incomplete_count": 2,
            "activation_work_authorization_expired_count": 1,
            "activation_work_terminal_failure_count": 1,
            "activation_work_completion_rate": 2 / 6,
            "activation_work_fate_counts": {
                "activated": 2,
                "authorization_expired": 1,
                "failed_terminal": 1,
                "pending": 1,
                "retry_scheduled": 1,
            },
            "incident_counts": {
                "agency_activation_authorization_expired": 1,
                "agency_activation_work_failed_terminal": 1,
                "agency_activation_work_incomplete": 2,
            },
        }
    )

    evidence = build_closed_loop_invariant_evidence(
        state,
        registry=load_architecture_registry(REGISTRY),
        executed_scenario_ids=frozenset(),
    )[0]

    assert evidence.achieved_evidence_tier is EvidenceTier.E3
    assert evidence.metric_observations[0].violation_count == 4
    by_class = {incident.incident_class: incident for incident in evidence.incidents}
    assert set(by_class) == {
        "agency_activation_authorization_expired",
        "agency_activation_work_failed_terminal",
        "agency_activation_work_incomplete",
    }
    assert (
        by_class["agency_activation_authorization_expired"].summary
        == "Observed 1 authorized activations that expired before activation."
    )
    assert by_class["agency_activation_work_failed_terminal"].severity == 5


def test_markdown_reports_activation_queue_exposure_and_continuous_fates() -> None:
    state = _state(episode_count=1, complete_episode_count=1).model_copy(
        update={
            "activation_work_item_count": 4,
            "activation_work_activated_count": 2,
            "activation_work_incomplete_count": 1,
            "activation_work_authorization_expired_count": 1,
            "activation_work_terminal_failure_count": 0,
            "activation_work_completion_rate": 0.5,
            "activation_work_fate_counts": {
                "activated": 2,
                "authorization_expired": 1,
                "processing": 1,
            },
        }
    )

    markdown = render_closed_loop_markdown(state)

    assert "## Authorized Agency Activation Queue" in markdown
    assert "- Work items: 4" in markdown
    assert "- Activated: 2" in markdown
    assert "- Incomplete: 1" in markdown
    assert "- Authorization expired: 1" in markdown
    assert "- Completion rate: 50.0%" in markdown
    assert "| authorization_expired | 1 |" in markdown


def test_scheduling_queue_fates_are_continuous_and_unknown_fates_are_rejected() -> None:
    summary = _summarize_scheduling_work_rows(
        [
            {"status": "pending"},
            {"status": "processing"},
            {"status": "retry_scheduled"},
            {"status": "leased"},
            {"status": "leased"},
            {"status": "work_expired"},
            {"status": "authorization_expired"},
            {"status": "failed_terminal"},
        ]
    )

    assert summary == (
        8,
        2,
        3,
        1,
        1,
        1,
        {
            "authorization_expired": 1,
            "failed_terminal": 1,
            "leased": 2,
            "pending": 1,
            "processing": 1,
            "retry_scheduled": 1,
            "work_expired": 1,
        },
    )

    with pytest.raises(ValueError, match="unknown registered work scheduling"):
        _summarize_scheduling_work_rows([{"status": "silently_dropped"}])


def test_scheduling_queue_incidents_remain_visible_without_downgrading_e3() -> None:
    state = _state(episode_count=1, complete_episode_count=1).model_copy(
        update={
            "scheduling_work_item_count": 7,
            "scheduling_work_leased_count": 2,
            "scheduling_work_incomplete_count": 2,
            "scheduling_work_backlog_count": 2,
            "scheduling_work_work_expired_count": 1,
            "scheduling_work_authorization_expired_count": 1,
            "scheduling_work_terminal_failure_count": 1,
            "scheduling_work_completion_rate": 2 / 7,
            "scheduling_work_fate_counts": {
                "authorization_expired": 1,
                "failed_terminal": 1,
                "leased": 2,
                "pending": 1,
                "retry_scheduled": 1,
                "work_expired": 1,
            },
            "incident_counts": {
                "work_scheduling_authorization_expired": 1,
                "work_scheduling_failed_terminal": 1,
                "work_scheduling_incomplete": 2,
                "work_scheduling_work_expired": 1,
            },
        }
    )

    evidence = build_closed_loop_invariant_evidence(
        state,
        registry=load_architecture_registry(REGISTRY),
        executed_scenario_ids=frozenset(),
    )[0]

    assert evidence.achieved_evidence_tier is EvidenceTier.E3
    assert evidence.metric_observations[0].violation_count == 5
    by_class = {incident.incident_class: incident for incident in evidence.incidents}
    assert set(by_class) == {
        "work_scheduling_authorization_expired",
        "work_scheduling_failed_terminal",
        "work_scheduling_incomplete",
        "work_scheduling_work_expired",
    }
    assert (
        by_class["work_scheduling_work_expired"].summary
        == "Observed 1 registered work items that expired before leasing."
    )
    assert by_class["work_scheduling_failed_terminal"].severity == 5


def test_markdown_reports_scheduling_backlog_and_continuous_fates() -> None:
    state = _state(episode_count=1, complete_episode_count=1).model_copy(
        update={
            "scheduling_work_item_count": 5,
            "scheduling_work_leased_count": 2,
            "scheduling_work_incomplete_count": 1,
            "scheduling_work_backlog_count": 1,
            "scheduling_work_work_expired_count": 1,
            "scheduling_work_authorization_expired_count": 1,
            "scheduling_work_terminal_failure_count": 0,
            "scheduling_work_completion_rate": 0.4,
            "scheduling_work_fate_counts": {
                "authorization_expired": 1,
                "leased": 2,
                "processing": 1,
                "work_expired": 1,
            },
        }
    )

    markdown = render_closed_loop_markdown(state)

    assert "## Registered Work Scheduling Queue" in markdown
    assert "- Work items: 5" in markdown
    assert "- Leased: 2" in markdown
    assert "- Incomplete: 1" in markdown
    assert "- Backlog: 1" in markdown
    assert "- Work expired: 1" in markdown
    assert "- Authorization expired: 1" in markdown
    assert "- Completion rate: 40.0%" in markdown
    assert "| work_expired | 1 |" in markdown
