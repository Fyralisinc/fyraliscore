"""services/integrations/slack/metrics.py — install / uninstall counters.

Bounded-cardinality metrics for the Slack OAuth flow. Label set is
intentionally small (≤8 distinct values per metric) so storage cost
stays proportional to provider count, not workspace count.

`tenant_id` and `team_id` are NOT label values — IN-07 SC-008 / IN-08
SC-007 (no enumeration via label cardinality).
"""
from __future__ import annotations

import threading


_lock = threading.Lock()
_install_outcomes: dict[str, int] = {}
_uninstall_outcomes: dict[str, int] = {}
_install_durations_s: list[float] = []
_DURATION_SAMPLE_CAP = 1024


def record_install_outcome(outcome: str) -> None:
    """slack_install_outcomes_total{outcome}.

    outcome ∈ {success, initiated, state_invalid, state_expired,
               state_consumed, slack_oauth_error, installation_collision,
               secret_store_unavailable}.
    """
    with _lock:
        _install_outcomes[outcome] = _install_outcomes.get(outcome, 0) + 1


def record_uninstall_outcome(outcome: str) -> None:
    """slack_uninstall_outcomes_total{outcome}.

    outcome ∈ {success, unknown_team, error}.
    """
    with _lock:
        _uninstall_outcomes[outcome] = _uninstall_outcomes.get(outcome, 0) + 1


def observe_install_duration(seconds: float) -> None:
    """slack_install_duration_seconds histogram (sample-based)."""
    with _lock:
        _install_durations_s.append(seconds)
        if len(_install_durations_s) > _DURATION_SAMPLE_CAP:
            del _install_durations_s[0]


def get_install_outcome_count(outcome: str) -> int:
    with _lock:
        return _install_outcomes.get(outcome, 0)


def get_uninstall_outcome_count(outcome: str) -> int:
    with _lock:
        return _uninstall_outcomes.get(outcome, 0)


def reset() -> None:
    """Test helper — clear all counters."""
    with _lock:
        _install_outcomes.clear()
        _uninstall_outcomes.clear()
        _install_durations_s.clear()


__all__ = [
    "record_install_outcome",
    "record_uninstall_outcome",
    "observe_install_duration",
    "get_install_outcome_count",
    "get_uninstall_outcome_count",
    "reset",
]
