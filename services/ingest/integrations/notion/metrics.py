"""services/ingest/integrations/notion/metrics.py — install / fetch counters (IN-14).

Bounded-cardinality, mirroring the Slack/GitHub metric posture. Neither
`tenant_id` nor `workspace_id` is a label value (no enumeration via
label cardinality).
"""
from __future__ import annotations

import threading

from services.ingest.integrations.metrics_contract import (
    MetricSample,
    export_install_fetch_metrics,
)


_lock = threading.Lock()
_install_outcomes: dict[str, int] = {}
_fetch_counts: dict[str, int] = {}


def record_install_outcome(outcome: str) -> None:
    """notion_install_outcomes_total{outcome}.

    outcome ∈ {success, initiated, state_invalid, state_expired,
               state_consumed, notion_oauth_error, installation_collision,
               secret_store_unavailable, notion_unconfigured}.
    """
    with _lock:
        _install_outcomes[outcome] = _install_outcomes.get(outcome, 0) + 1


def record_fetch_event(event: str, by: int = 1) -> None:
    """notion_fetch_total{event}.

    event ∈ {pages, rate_limited, reconcile_gap, block_truncated}.
    """
    with _lock:
        _fetch_counts[event] = _fetch_counts.get(event, 0) + by


def get_install_outcome_count(outcome: str) -> int:
    with _lock:
        return _install_outcomes.get(outcome, 0)


def get_fetch_event_count(event: str) -> int:
    with _lock:
        return _fetch_counts.get(event, 0)


def export_metrics(source_id: str) -> tuple[MetricSample, ...]:
    """Copy and normalize the source-owned counters under their lock."""

    with _lock:
        installs = dict(_install_outcomes)
        fetches = dict(_fetch_counts)
    return export_install_fetch_metrics(
        source_id,
        install_outcomes=installs,
        fetch_counts=fetches,
    )


def reset() -> None:
    """Test helper — clear all counters."""
    with _lock:
        _install_outcomes.clear()
        _fetch_counts.clear()


__all__ = [
    "export_metrics",
    "record_install_outcome",
    "record_fetch_event",
    "get_install_outcome_count",
    "get_fetch_event_count",
    "reset",
]
