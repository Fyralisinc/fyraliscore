"""services/ingest/integrations/ramp/metrics.py — lightweight in-process counters."""
from __future__ import annotations

from services.ingest.integrations.metrics_contract import make_snapshot_exporter


_counters: dict[str, int] = {}


def record_request(outcome: str) -> None:
    """outcome ∈ {ok, rate_limited, error, unauthorized}."""
    key = f"ramp.request.{outcome}"
    _counters[key] = _counters.get(key, 0) + 1


def record_provision_outcome(outcome: str) -> None:
    """outcome ∈ {success, no_entities, error}."""
    key = f"ramp.provision.{outcome}"
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
