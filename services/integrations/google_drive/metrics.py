"""services/integrations/google_drive/metrics.py — counters (IN-16).

Bounded-cardinality, mirroring the Calendar/Gmail/Notion metric posture.
Neither tenant_id nor drive_id is a label value.
"""
from __future__ import annotations

import threading


_lock = threading.Lock()
_provision_outcomes: dict[str, int] = {}
_fetch_counts: dict[str, int] = {}


def record_provision_outcome(outcome: str) -> None:
    """gdrive_provision_outcomes_total{outcome}.

    outcome in {success, no_targets, directory_error, install_error}.
    """
    with _lock:
        _provision_outcomes[outcome] = _provision_outcomes.get(outcome, 0) + 1


def record_fetch_event(event: str, by: int = 1) -> None:
    """gdrive_fetch_total{event}.

    event in {files, comments, revisions, extracted, extract_failed,
              rate_limited, start_token_expired, reconcile_gap}.
    """
    with _lock:
        _fetch_counts[event] = _fetch_counts.get(event, 0) + by


def get_provision_outcome_count(outcome: str) -> int:
    with _lock:
        return _provision_outcomes.get(outcome, 0)


def get_fetch_event_count(event: str) -> int:
    with _lock:
        return _fetch_counts.get(event, 0)


def reset() -> None:
    """Test helper — clear all counters."""
    with _lock:
        _provision_outcomes.clear()
        _fetch_counts.clear()


__all__ = [
    "record_provision_outcome",
    "record_fetch_event",
    "get_provision_outcome_count",
    "get_fetch_event_count",
    "reset",
]
