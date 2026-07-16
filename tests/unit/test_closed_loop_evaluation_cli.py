from __future__ import annotations

from dataclasses import dataclass, replace

from scripts.evaluate_closed_loop_state import _observed_loop_is_complete


@dataclass(frozen=True)
class _State:
    episode_count: int
    closed_loop_completion_rate: float | None
    stage_coverage_rates: dict[str, float | None]
    continuity_rates: dict[str, float | None]
    component_violation_counts: dict[str, int]
    violation_count: int


def test_complete_loop_gate_requires_exposure_and_every_join() -> None:
    complete = _State(
        episode_count=1,
        closed_loop_completion_rate=1.0,
        stage_coverage_rates={"belief": 1.0, "outcome": 1.0},
        continuity_rates={"belief_to_concern": 1.0},
        component_violation_counts={},
        violation_count=0,
    )

    assert _observed_loop_is_complete(complete)
    assert not _observed_loop_is_complete(
        replace(complete, episode_count=0, closed_loop_completion_rate=None)
    )
    assert not _observed_loop_is_complete(
        replace(complete, continuity_rates={"belief_to_concern": 0.5})
    )
    assert not _observed_loop_is_complete(
        replace(
            complete,
            component_violation_counts={"execution": 1},
            violation_count=1,
        )
    )
