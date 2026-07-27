"""services/ingest/integrations/fireflies/metrics.py — in-process counters.

Mirrors the brex/google_drive/jira metrics shape: a module-level dict bumped by
the client + onboarding, readable by tests/diagnostics. No Prometheus dependency
at this layer (the workers expose their own /metrics).
"""
from __future__ import annotations

from services.ingest.integrations.metrics_contract import make_snapshot_exporter


_counters: dict[str, int] = {}


def record_request(outcome: str) -> None:
    """outcome ∈ {ok, rate_limited, error, unauthorized}."""
    key = f"fireflies.request.{outcome}"
    _counters[key] = _counters.get(key, 0) + 1


def record_provision_outcome(outcome: str) -> None:
    """outcome ∈ {success, no_transcripts, error}."""
    key = f"fireflies.provision.{outcome}"
    _counters[key] = _counters.get(key, 0) + 1


def snapshot() -> dict[str, int]:
    return dict(_counters)


def _reset_for_tests() -> None:
    _counters.clear()


export_metrics = make_snapshot_exporter(snapshot)


__all__ = [
    "export_metrics",
    "record_request",
    "record_provision_outcome",
    "snapshot",
]
